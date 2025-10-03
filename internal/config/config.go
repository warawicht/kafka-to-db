package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config captures runtime parameters for the worker pool POC.
type Config struct {
	KafkaBrokers        []string
	KafkaTopic          string
	KafkaGroup          string
	KafkaVersion        string
	KafkaSessionTimeout time.Duration
	KafkaHeartbeat      time.Duration
	KafkaMaxPollRecords int

	DBURL             string
	DBTable           string
	DBMaxConns        int32
	DBMaxConnLifetime time.Duration
	DBMaxConnIdleTime time.Duration

	WorkerCount        int
	JobBuffer          int
	BatchFlushInterval time.Duration
	BatchSize          int
	MaxRetries         int

	MetricsAddr string
}

// FromEnv constructs Config using environment variables with sensible defaults.
func FromEnv() (Config, error) {
	cfg := Config{
		KafkaVersion:        getenv("KAFKA_VERSION", "3.6.0"),
		KafkaSessionTimeout: mustParseDuration(getenv("KAFKA_SESSION_TIMEOUT", "30s")),
		KafkaHeartbeat:      mustParseDuration(getenv("KAFKA_HEARTBEAT", "3s")),
		KafkaMaxPollRecords: mustParseInt(getenv("KAFKA_MAX_POLL", "500")),
		DBTable:             getenv("DB_TABLE", "kafka_events"),
		DBMaxConns:          int32(mustParseInt(getenv("DB_MAX_CONNS", "128"))),
		DBMaxConnLifetime:   mustParseDuration(getenv("DB_MAX_CONN_LIFETIME", "30m")),
		DBMaxConnIdleTime:   mustParseDuration(getenv("DB_MAX_CONN_IDLE", "5m")),
		WorkerCount:         mustParseInt(getenv("WORKER_COUNT", "80")),
		JobBuffer:           mustParseInt(getenv("JOB_BUFFER", "8192")),
		BatchFlushInterval:  mustParseDuration(getenv("BATCH_FLUSH_INTERVAL", "40ms")),
		BatchSize:           mustParseInt(getenv("BATCH_SIZE", "256")),
		MaxRetries:          mustParseInt(getenv("MAX_RETRIES", "5")),
		MetricsAddr:         getenv("METRICS_ADDR", ":2112"),
	}

	brokers := strings.Split(getenv("KAFKA_BROKERS", "localhost:9092"), ",")
	if len(brokers) == 1 && brokers[0] == "" {
		return Config{}, fmt.Errorf("KAFKA_BROKERS must be provided")
	}
	cfg.KafkaBrokers = brokers
	cfg.KafkaTopic = getenv("KAFKA_TOPIC", "staging.events")
	cfg.KafkaGroup = getenv("KAFKA_GROUP", "event-writer")
	cfg.DBURL = strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if cfg.DBURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL must be provided")
	}
	return cfg, nil
}

func getenv(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func mustParseDuration(v string) time.Duration {
	d, err := time.ParseDuration(v)
	if err != nil {
		panic(fmt.Sprintf("invalid duration %q: %v", v, err))
	}
	return d
}

func mustParseInt(v string) int {
	iv, err := strconv.Atoi(v)
	if err != nil {
		panic(fmt.Sprintf("invalid int %q: %v", v, err))
	}
	return iv
}
