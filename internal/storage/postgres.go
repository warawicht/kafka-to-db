package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"demo/internal/worker"
)

// PostgresWriter persists Kafka records into a Postgres table using batched inserts.
type PostgresWriter struct {
	pool      *pgxpool.Pool
	tableName string
}

// NewPostgresWriter initialises a connection pool tuned for high throughput.
func NewPostgresWriter(ctx context.Context, dsn, table string, maxConns int32, maxLifetime, maxIdle time.Duration) (*PostgresWriter, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse postgres dsn: %w", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	if maxLifetime > 0 {
		cfg.MaxConnLifetime = maxLifetime
	}
	if maxIdle > 0 {
		cfg.MaxConnIdleTime = maxIdle
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pgx pool: %w", err)
	}
	return &PostgresWriter{pool: pool, tableName: table}, nil
}

// Close releases underlying resources.
func (w *PostgresWriter) Close() {
	w.pool.Close()
}

// ProcessBatch implements worker.Processor and writes messages to Postgres.
func (w *PostgresWriter) ProcessBatch(ctx context.Context, records []worker.Record) error {
	if len(records) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	quoted := quoteIdentifier(w.tableName)
	query := fmt.Sprintf(`INSERT INTO %s (topic, partition, message_offset, key, value, headers, event_time)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (topic, partition, message_offset) DO NOTHING`, quoted)

	for _, rec := range records {
		headersJSON, err := json.Marshal(rec.Headers)
		if err != nil {
			return fmt.Errorf("marshal headers: %w", err)
		}
		batch.Queue(query, rec.Topic, rec.Partition, rec.Offset, rec.Key, rec.Value, headersJSON, rec.Timestamp)
	}

	br := w.pool.SendBatch(ctx, batch)
	if err := br.Close(); err != nil {
		return fmt.Errorf("run batch: %w", err)
	}

	return nil
}

func quoteIdentifier(id string) string {
	return `"` + strings.ReplaceAll(id, `"`, `""`) + `"`
}
