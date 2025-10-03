package metrics

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync/atomic"
	"time"
)

// Collector keeps simple counters for observability without external deps.
type Collector struct {
	processed atomic.Int64
	errors    atomic.Int64
}

// IncProcessed increases the processed record counter.
func (c *Collector) IncProcessed(delta int) {
	c.processed.Add(int64(delta))
}

// IncErrors increases the error counter.
func (c *Collector) IncErrors() {
	c.errors.Add(1)
}

// Serve spins up a lightweight metrics endpoint.
func Serve(ctx context.Context, addr string, collector *Collector) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		payload := fmt.Sprintf("worker_processed_total %d\nworker_errors_total %d\n", collector.processed.Load(), collector.errors.Load())
		_, _ = w.Write([]byte(payload))
	})

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("metrics server failed: %v", err)
	}
}
