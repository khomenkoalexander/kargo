#!/bin/bash
# Waits for the bundled Kafka broker to be ready, then creates the two required topics
# with the settings defined in docs.
set -e

BOOTSTRAP="${KFILE_KAFKA_BOOTSTRAP:-kafka:9092}"
INFO_TOPIC="${KFILE_INFO_TOPIC:-xfer.info}"
DATA_TOPIC="${KFILE_DATA_TOPIC:-xfer.data}"
DATA_PARTITIONS="${KFILE_DATA_PARTITIONS:-6}"

KAFKA_TOPICS=/opt/kafka/bin/kafka-topics.sh

echo "Waiting for Kafka at ${BOOTSTRAP}..."
until ${KAFKA_TOPICS} --bootstrap-server "${BOOTSTRAP}" --list > /dev/null 2>&1; do
    sleep 2
done
echo "Kafka is ready."

create_topic_if_missing() {
    local topic="$1"
    shift
    if ${KAFKA_TOPICS} --bootstrap-server "${BOOTSTRAP}" --describe --topic "$topic" > /dev/null 2>&1; then
        echo "Topic '$topic' already exists — skipping."
    else
        echo "Creating topic '$topic'..."
        ${KAFKA_TOPICS} --bootstrap-server "${BOOTSTRAP}" --create --topic "$topic" "$@"
        echo "Created '$topic'."
    fi
}

# xfer.info — compacted WAL, single partition, short segment for fast compaction
create_topic_if_missing "${INFO_TOPIC}" \
    --partitions 1 \
    --replication-factor 1 \
    --config cleanup.policy=compact \
    --config min.cleanable.dirty.ratio=0.1 \
    --config min.compaction.lag.ms=0 \
    --config delete.retention.ms=60000 \
    --config segment.ms=300000 \
    --config segment.bytes=10485760

# xfer.data — delete policy, short retention, fast segment rolling
create_topic_if_missing "${DATA_TOPIC}" \
    --partitions "${DATA_PARTITIONS}" \
    --replication-factor 1 \
    --config cleanup.policy=delete \
    --config retention.ms=30000 \
    --config segment.ms=10000 \
    --config segment.bytes=16777216 \
    --config max.message.bytes=4194304 \
    --config compression.type=producer \
    --config unclean.leader.election.enable=false

echo "Topic initialization complete."
