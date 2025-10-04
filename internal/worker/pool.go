package worker

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// Job wraps a Kafka message together with its consumer session.
type Job struct {
	Message  *sarama.ConsumerMessage
	Session  sarama.ConsumerGroupSession
	Attempts int
}

// Record is the payload handed to the Processor implementation.
type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   map[string][]byte
	Timestamp time.Time
}

// Processor writes records to durable storage.
type Processor interface {
	ProcessBatch(ctx context.Context, records []Record) error
}

// Options tune the worker pool behaviour.
type Options struct {
	WorkerCount int
	JobBuffer   int
	BatchSize   int
	FlushEvery  time.Duration
	MaxRetries  int
	OnError     func(error)
	OnSuccess   func(batchSize int)
}

// Pool fans out Kafka jobs to workers with batching support.
type Pool struct {
	processor Processor
	opts      Options
	jobs      chan Job
	wg        sync.WaitGroup
	mu        sync.RWMutex
	closed    bool
}

// NewPool allocates a pool ready to accept jobs.
func NewPool(processor Processor, opts Options) *Pool {
	if opts.WorkerCount <= 0 {
		opts.WorkerCount = 32
	}
	if opts.JobBuffer <= 0 {
		opts.JobBuffer = 4096
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = 100
	}
	if opts.FlushEvery <= 0 {
		opts.FlushEvery = 25 * time.Millisecond
	}
	if opts.MaxRetries <= 0 {
		opts.MaxRetries = 5
	}
	if opts.OnError == nil {
		opts.OnError = func(err error) { log.Printf("worker error: %v", err) }
	}
	if opts.OnSuccess == nil {
		opts.OnSuccess = func(int) {}
	}
	return &Pool{
		processor: processor,
		opts:      opts,
		jobs:      make(chan Job, opts.JobBuffer),
	}
}

// Start spins up the configured worker goroutines.
func (p *Pool) Start(ctx context.Context) {
	for i := 0; i < p.opts.WorkerCount; i++ {
		p.wg.Add(1)
		go p.runWorker(ctx, i)
	}
}

// Stop waits for workers to finish draining.
func (p *Pool) Stop() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	close(p.jobs)
	p.mu.Unlock()
	p.wg.Wait()
}

// Submit queues a job for processing, returning false when the pool is shutting down.
func (p *Pool) Submit(job Job) bool {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return false
	}
	p.mu.RUnlock()

	select {
	case p.jobs <- job:
		return true
	default:
		p.jobs <- job
		return true
	}
}

func (p *Pool) runWorker(ctx context.Context, id int) {
	defer p.wg.Done()

	ticker := time.NewTicker(p.opts.FlushEvery)
	defer ticker.Stop()

	var buffer []Job

	flush := func(force bool) {
		if len(buffer) == 0 {
			return
		}
		records := make([]Record, 0, len(buffer))
		for _, job := range buffer {
			headers := make(map[string][]byte, len(job.Message.Headers))
			for _, h := range job.Message.Headers {
				headers[string(h.Key)] = h.Value
			}
			records = append(records, Record{
				Topic:     job.Message.Topic,
				Partition: job.Message.Partition,
				Offset:    job.Message.Offset,
				Key:       job.Message.Key,
				Value:     job.Message.Value,
				Headers:   headers,
				Timestamp: job.Message.Timestamp,
			})
		}

		err := p.processor.ProcessBatch(ctx, records)
		if err != nil {
			for _, job := range buffer {
				p.handleFailure(ctx, job, err)
			}
		} else {
			for _, job := range buffer {
				job.Session.MarkMessage(job.Message, "")
			}
			p.opts.OnSuccess(len(buffer))
		}
		buffer = buffer[:0]
	}

	for {
		select {
		case <-ctx.Done():
			flush(true)
			return
		case <-ticker.C:
			flush(false)
		case job, ok := <-p.jobs:
			if !ok {
				flush(true)
				return
			}
			buffer = append(buffer, job)
			if len(buffer) >= p.opts.BatchSize {
				flush(true)
			}
		}
	}
}

func (p *Pool) handleFailure(ctx context.Context, job Job, cause error) {
	p.opts.OnError(cause)
	if job.Attempts >= p.opts.MaxRetries {
		log.Printf("dropping message offset=%d attempts=%d: %v", job.Message.Offset, job.Attempts, cause)
		return
	}
	job.Attempts++
	backoff := time.Duration(1<<uint(job.Attempts-1)) * 100 * time.Millisecond
	if backoff > 5*time.Second {
		backoff = 5 * time.Second
	}
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
			p.Submit(job)
		}
	}()
}
