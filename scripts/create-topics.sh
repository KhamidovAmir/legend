#!/usr/bin/env bash
# Creates all platform topics (local stand). On Kubernetes stands the same list lives in deploy/platform/kafka-topics.yaml.
# Usage: create-topics.sh <bootstrap-server>
set -euo pipefail
BOOTSTRAP=${1:-localhost:9092}
BIN=${KAFKA_BIN:-/opt/kafka/bin}
PARTITIONS=${PARTITIONS:-3}

TOPICS=(
  external.delivery.events external.delivery.results
  exception.events exception.commands
  resolution.events
  integration.commands integration.queries integration.events
)
for topic in "${TOPICS[@]}"; do
  for name in "$topic" "$topic.dlt"; do
    "$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
      --topic "$name" --partitions "$PARTITIONS" --replication-factor 1
  done
done

# Debezium heartbeat topic (topic.prefix = resolution-cdc). With auto-creation disabled the connector task
# blocks on its first heartbeat and silently stops publishing the outbox if this topic is missing.
"$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
  --topic __debezium-heartbeat.resolution-cdc --partitions 1 --replication-factor 1
