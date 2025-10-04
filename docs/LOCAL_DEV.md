# Local end-to-end stack

Run the worker together with Kafka and Postgres using Docker Compose.

## Requirements
- Docker Engine 24+
- Docker Compose plugin (`docker compose`)

## Usage
```bash
docker compose up --build
```

This brings up:
- `zookeeper` and `kafka` brokers (Confluent Platform `7.6.0`, port `29092` exposed for local producers)
- `topic-init` helper that waits for Kafka and seeds the `staging.events` topic (4 partitions)
- `postgres` initialised with the `kafka_events` table
- `worker` container publishing metrics at <http://localhost:2112/metrics>

To stop the stack:
```bash
docker compose down
```

Add `-v` to `docker compose down -v` when you also want to drop the persisted Postgres volume.

> Note: Bitnami removed their Kafka/Zookeeper images from Docker Hub, so the stack now uses Confluent Platform containers (`confluentinc/cp-kafka` and `confluentinc/cp-zookeeper`).

## Load generator (optional)

To exercise the pipeline with synthetic traffic, start the load generator service:

```bash
docker compose --profile loadtest up loadgen
```

Defaults:
- 1,000 messages/second
- 512-byte JSON payloads
- Infinite run (`GEN_TOTAL_MESSAGES=0`)

Tune behaviour via environment variables such as `GEN_MESSAGE_RATE`, `GEN_MESSAGE_SIZE`, `GEN_TOTAL_MESSAGES`, and `GEN_COMPRESSION` (see `internal/generator/config.go`).

You can also run the binary locally without Docker:

```bash
go run ./cmd/generator
```
