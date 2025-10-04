package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"golang.org/x/time/rate"

	"demo/internal/generator"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	cfg, err := generator.FromEnv()
	if err != nil {
		log.Fatalf("load generator config: %v", err)
	}

	if cfg.MessageRate <= 0 {
		log.Fatalf("GEN_MESSAGE_RATE must be > 0")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	saramaCfg := sarama.NewConfig()
	saramaCfg.ClientID = cfg.ClientID
	saramaCfg.Producer.RequiredAcks = sarama.WaitForLocal
	saramaCfg.Producer.Return.Errors = true

	version, err := sarama.ParseKafkaVersion(cfg.KafkaVersion)
	if err != nil {
		log.Fatalf("parse kafka version %q: %v", cfg.KafkaVersion, err)
	}
	saramaCfg.Version = version

	if codec, err := parseCompression(cfg.Compression); err != nil {
		log.Fatalf("compression: %v", err)
	} else {
		saramaCfg.Producer.Compression = codec
	}

	producer, err := sarama.NewAsyncProducer(cfg.Brokers, saramaCfg)
	if err != nil {
		log.Fatalf("create producer: %v", err)
	}

	var errorCount int64
	errorsDone := make(chan struct{})
	go func() {
		for err := range producer.Errors() {
			atomic.AddInt64(&errorCount, 1)
			log.Printf("produce error: %v", err)
		}
		close(errorsDone)
	}()

	limiter := rate.NewLimiter(rate.Limit(cfg.MessageRate), cfg.MessageRate)
	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	var produced int64
	nextLog := time.Now().Add(cfg.LogInterval)
	start := time.Now()

loop:
	for {
		if cfg.TotalMessages > 0 && produced >= int64(cfg.TotalMessages) {
			log.Printf("completed sending %d messages", produced)
			break
		}

		if err := limiter.Wait(ctx); err != nil {
			log.Printf("limiter stopped: %v", err)
			break
		}

		seq := atomic.AddInt64(&produced, 1)
		key := fmt.Sprintf("%s-%d", cfg.KeyPrefix, seq)
		value := buildPayload(random, seq, cfg.MessageSize)

		msg := &sarama.ProducerMessage{
			Topic: cfg.Topic,
			Key:   sarama.StringEncoder(key),
			Value: sarama.ByteEncoder(value),
		}

		select {
		case producer.Input() <- msg:
		case <-ctx.Done():
			log.Printf("context cancelled; stopping")
			break loop
		}

		if time.Now().After(nextLog) {
			elapsed := time.Since(start).Seconds()
			avgRate := float64(produced) / elapsed
			log.Printf("produced=%d errors=%d avg_rate=%.1f msg/s goroutines=%d", produced, atomic.LoadInt64(&errorCount), avgRate, runtime.NumGoroutine())
			nextLog = time.Now().Add(cfg.LogInterval)
		}
	}

	producer.AsyncClose()
	<-errorsDone
	log.Printf("producer closed (%d messages sent, %d errors)", produced, atomic.LoadInt64(&errorCount))
}

func parseCompression(value string) (sarama.CompressionCodec, error) {
	switch strings.ToLower(value) {
	case "none", "":
		return sarama.CompressionNone, nil
	case "gzip":
		return sarama.CompressionGZIP, nil
	case "snappy":
		return sarama.CompressionSnappy, nil
	case "lz4":
		return sarama.CompressionLZ4, nil
	case "zstd":
		return sarama.CompressionZSTD, nil
	default:
		return sarama.CompressionNone, fmt.Errorf("unsupported compression codec %q", value)
	}
}

func buildPayload(r *rand.Rand, seq int64, size int) []byte {
	ts := time.Now().UTC().Format(time.RFC3339Nano)
	header := fmt.Sprintf("{\"id\":%d,\"ts\":\"%s\",\"payload\":\"", seq, ts)
	footer := "\"}"
	fillerLen := size - len(header) - len(footer)
	if fillerLen < 0 {
		fillerLen = 0
	}
	filler := randomAlphaNumeric(r, fillerLen)
	return []byte(header + filler + footer)
}

var alphaNumeric = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func randomAlphaNumeric(r *rand.Rand, n int) string {
	if n <= 0 {
		return ""
	}
	b := make([]rune, n)
	for i := range b {
		b[i] = alphaNumeric[r.Intn(len(alphaNumeric))]
	}
	return string(b)
}
