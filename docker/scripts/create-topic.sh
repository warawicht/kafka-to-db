#!/bin/bash
set -euo pipefail

until kafka-topics --bootstrap-server kafka:9092 --list >/dev/null 2>&1; do
  echo "waiting for kafka broker..."
  sleep 3
done

kafka-topics --create --if-not-exists --topic staging.events --partitions 4 --replication-factor 1 --bootstrap-server kafka:9092

echo "topic staging.events ready."
