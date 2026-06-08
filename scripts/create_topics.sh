#!/bin/bash

echo "Creating Kafka topics..."

kafka-topics \
--bootstrap-server kafka:9092 \
--create \
--if-not-exists \
--topic user-events \
--partitions 3 \
--replication-factor 1

kafka-topics \
--bootstrap-server kafka:9092 \
--create \
--if-not-exists \
--topic content-metadata \
--partitions 1 \
--replication-factor 1 \
--config cleanup.policy=compact

kafka-topics \
--bootstrap-server kafka:9092 \
--create \
--if-not-exists \
--topic feature-store \
--partitions 1 \
--replication-factor 1 \
--config cleanup.policy=compact

echo "Topics created successfully."