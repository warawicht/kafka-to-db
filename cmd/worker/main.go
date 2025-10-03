package main

import (
	"context"
	"errors"
	"log"
	"os/signal"
	"syscall"

	"demo/internal/config"
	"demo/internal/consumer"
	"demo/internal/metrics"
	"demo/internal/storage"
	"demo/internal/worker"
)

func main() {
	cfg, err := config.FromEnv()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	collector := &metrics.Collector{}
	go metrics.Serve(ctx, cfg.MetricsAddr, collector)

	writer, err := storage.NewPostgresWriter(ctx, cfg.DBURL, cfg.DBTable, cfg.DBMaxConns, cfg.DBMaxConnLifetime, cfg.DBMaxConnIdleTime)
	if err != nil {
		log.Fatalf("connect postgres: %v", err)
	}
	defer writer.Close()

	pool := worker.NewPool(writer, worker.Options{
		WorkerCount: cfg.WorkerCount,
		JobBuffer:   cfg.JobBuffer,
		BatchSize:   cfg.BatchSize,
		FlushEvery:  cfg.BatchFlushInterval,
		MaxRetries:  cfg.MaxRetries,
		OnError: func(err error) {
			collector.IncErrors()
			log.Printf("process batch failed: %v", err)
		},
		OnSuccess: collector.IncProcessed,
	})

	pool.Start(ctx)
	defer pool.Stop()

	runner, err := consumer.NewRunner(ctx, cfg, pool)
	if err != nil {
		log.Fatalf("init consumer: %v", err)
	}
	defer runner.Close()

	if err := runner.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("runner terminated: %v", err)
	}
}
