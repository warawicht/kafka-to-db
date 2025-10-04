# Kafka → Postgres Worker POC

This proof-of-concept streams messages from Kafka into Postgres using the worker pool under `cmd/worker`.

## 1. Provision staging dependencies
- Kafka topic: ensure at least 12 partitions for the target 10k TPS (e.g. `staging.events`).
- Postgres table:
  ```sql
  CREATE TABLE IF NOT EXISTS kafka_events (
      topic text NOT NULL,
      partition int NOT NULL,
      message_offset bigint NOT NULL,
      key bytea,
      value bytea NOT NULL,
      headers jsonb,
      event_time timestamptz NOT NULL,
      PRIMARY KEY (topic, partition, message_offset)
  );
  ```

## 2. Configure environment
```bash
cp .env.example .env
# edit .env with staging credentials
export $(grep -v '^#' .env | xargs)
```

## 3. Resolve Go modules
```bash
go mod tidy
```
*(Required once to download `sarama`, `pgx`, and `backoff` packages.)*

## 4. Build and run the worker
```bash
make build
./bin/worker
```

The worker exposes `:2112/metrics` and `:2112/healthz`.

## 5. Load test checklist
- Produce to staging topic with the target rate (10–12k TPS) using the shared `kafka-producer-perf-test.sh` profile.
- Watch metrics: `worker_processed_total`, `worker_errors_total`, Kafka consumer lag (`kafka-consumer-groups.sh --describe`).
- Inspect Postgres `pg_stat_statements` for latency > 6 ms; adjust `WORKER_COUNT`, `BATCH_SIZE`, or `DB_MAX_CONNS` accordingly.

## 6. Shutdown
Use `Ctrl+C`; the process drains in-flight batches, commits offsets, and closes connections.
