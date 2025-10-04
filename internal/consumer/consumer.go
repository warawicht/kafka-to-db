package consumer

import (
	"context"
	"fmt"
	"log"

	"github.com/IBM/sarama"

	"demo/internal/config"
	"demo/internal/worker"
)

// Runner wires a Kafka consumer group to a worker pool.
type Runner struct {
	cfg    config.Config
	pool   *worker.Pool
	client sarama.ConsumerGroup
}

// NewRunner creates a consumer runner instance.
func NewRunner(ctx context.Context, cfg config.Config, pool *worker.Pool) (*Runner, error) {
	saramaCfg := sarama.NewConfig()
	version, err := sarama.ParseKafkaVersion(cfg.KafkaVersion)
	if err != nil {
		return nil, fmt.Errorf("parse kafka version: %w", err)
	}
	saramaCfg.Version = version
	saramaCfg.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	saramaCfg.Consumer.Return.Errors = true
	saramaCfg.Consumer.Offsets.AutoCommit.Enable = true
	saramaCfg.Consumer.Offsets.AutoCommit.Interval = cfg.KafkaHeartbeat
	saramaCfg.Consumer.Group.Session.Timeout = cfg.KafkaSessionTimeout
	saramaCfg.Consumer.Group.Heartbeat.Interval = cfg.KafkaHeartbeat
	saramaCfg.Metadata.RefreshFrequency = cfg.KafkaHeartbeat

	client, err := sarama.NewConsumerGroup(cfg.KafkaBrokers, cfg.KafkaGroup, saramaCfg)
	if err != nil {
		return nil, fmt.Errorf("create consumer group: %w", err)
	}

	r := &Runner{cfg: cfg, pool: pool, client: client}
	go func() {
		for err := range client.Errors() {
			log.Printf("consumer error: %v", err)
		}
	}()

	return r, nil
}

// Close releases client resources.
func (r *Runner) Close() error {
	return r.client.Close()
}

// Run starts consuming the configured topic until the context is cancelled.
func (r *Runner) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		handler := &groupHandler{pool: r.pool}
		if err := r.client.Consume(ctx, []string{r.cfg.KafkaTopic}, handler); err != nil {
			log.Printf("consume error: %v", err)
			// allow loop to retry on transient errors.
		}
	}
}

type groupHandler struct {
	pool *worker.Pool
}

func (h *groupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *groupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *groupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		job := worker.Job{Message: msg, Session: session}
		if ok := h.pool.Submit(job); !ok {
			log.Printf("worker pool rejected message offset=%d", msg.Offset)
		}
	}
	return nil
}
