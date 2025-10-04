package generator

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Brokers       []string
	Topic         string
	KafkaVersion  string
	MessageRate   int
	MessageSize   int
	TotalMessages int
	KeyPrefix     string
	Compression   string
	ClientID      string
	LogInterval   time.Duration
}

func FromEnv() (Config, error) {
	cfg := Config{
		Topic:        getenv("KAFKA_TOPIC", "staging.events"),
		KafkaVersion: getenv("KAFKA_VERSION", "3.6.1"),
		KeyPrefix:    getenv("GEN_KEY_PREFIX", "loadgen"),
		Compression:  strings.ToLower(getenv("GEN_COMPRESSION", "none")),
		ClientID:     getenv("GEN_CLIENT_ID", "load-generator"),
	}

	brokersRaw := getenv("KAFKA_BROKERS", "localhost:29092")
	brokers := splitAndTrim(brokersRaw)
	if len(brokers) == 0 {
		return Config{}, fmt.Errorf("KAFKA_BROKERS must be provided")
	}
	cfg.Brokers = brokers

	if cfg.Topic == "" {
		return Config{}, fmt.Errorf("KAFKA_TOPIC must be provided")
	}

	var err error
	if cfg.MessageRate, err = parsePositiveInt("GEN_MESSAGE_RATE", 1000); err != nil {
		return Config{}, err
	}

	if cfg.MessageSize, err = parsePositiveInt("GEN_MESSAGE_SIZE", 512); err != nil {
		return Config{}, err
	}

	if cfg.TotalMessages, err = parseNonNegativeInt("GEN_TOTAL_MESSAGES", 0); err != nil {
		return Config{}, err
	}

	if cfg.LogInterval, err = parseDuration("GEN_LOG_INTERVAL", "5s"); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func getenv(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func splitAndTrim(input string) []string {
	raw := strings.Split(input, ",")
	var items []string
	for _, r := range raw {
		trimmed := strings.TrimSpace(r)
		if trimmed != "" {
			items = append(items, trimmed)
		}
	}
	return items
}

func parsePositiveInt(key string, fallback int) (int, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		if fallback <= 0 {
			return 0, fmt.Errorf("fallback for %s must be > 0", key)
		}
		return fallback, nil
	}
	parsed, err := strconv.Atoi(v)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", key)
	}
	return parsed, nil
}

func parseNonNegativeInt(key string, fallback int) (int, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		if fallback < 0 {
			return 0, fmt.Errorf("fallback for %s must be >= 0", key)
		}
		return fallback, nil
	}
	parsed, err := strconv.Atoi(v)
	if err != nil || parsed < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer", key)
	}
	return parsed, nil
}

func parseDuration(key, fallback string) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		value = fallback
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be a valid duration: %w", key, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("%s must be > 0", key)
	}
	return d, nil
}
