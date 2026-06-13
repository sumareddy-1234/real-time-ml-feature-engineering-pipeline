# Real-Time ML Feature Engineering Pipeline with Kafka & Apache Flink

This repository contains a production-style, real-time feature engineering pipeline designed for low-latency machine learning models (e.g. content recommendations, personalization). It uses **Apache Kafka** as a high-throughput event bus and state store, **Apache Flink** for stateful stream processing (windowing, joins, watermarking), and a **Streamlit** dashboard for real-time observability.

## System Architecture

The pipeline orchestrates the following components:
1. **Data Sources (Producer)**: A Python service simulating user interactions (views, clicks, likes, shares) and content metadata. It automatically initializes Kafka topics and generates deliberate late events (35 to 90 seconds in the past) in accelerated simulation time to test pipeline resilience.
2. **Apache Kafka Broker**: Serves as the durable log:
   - `user-events`: Streaming input topic (3 partitions).
   - `content-metadata`: Compacted lookup topic for enrichment.
   - `feature-store`: Compacted output topic representing the feature store.
   - `pipeline-metrics`: Standard topic for operational health metrics.
3. **Apache Flink Job**: A Java application processing event time streams:
   - **Tumbling 1-Hour User Window**: computes user `click_rate` and `avg_dwell_time`.
   - **Sliding 15-Minute Content Window (5-Minute Slide)**: computes content `engagement_rate`.
   - **Stream-Table Join**: enriches user events with content categories, then computes a 1-hour tumbling user `category_affinity_score`.
   - **Metrics Tracker**: intercepts late events and computes watermark lag, publishing updates to the metrics topic.
4. **Observability Dashboard**: A Streamlit application displaying real-time feature updates, late-event counters, watermark lag, and an entity feature viewer.

---

## Getting Started

### Prerequisites
- Docker & Docker Compose (v2.0+)

### Setup and Execution

1. **Configure Environment Variables**:
   Copy the example environment file:
   ```bash
   cp .env.example .env
   ```

2. **Start the Pipeline**:
   Build and start all containerized services:
   ```bash
   docker compose up --build -d
   ```
   *Note: Flink compilation is done inside a Maven builder container during startup, meaning no Java/Maven installation is required on the host system.*

3. **Monitor Container Health**:
   All services are configured with healthchecks to ensure stable startup. Verify health status using:
   ```bash
   docker compose ps
   ```

4. **Access the Observability Dashboard**:
   Open your browser and navigate to:
   ```
   http://localhost:8501
   ```

---

## Configuration Settings (`.env`)

Exposed environment configurations include:
- `KAFKA_BOOTSTRAP_SERVERS`: Connection URI for the Kafka broker (default: `kafka:9092`).
- `USER_EVENTS_TOPIC`: Input topic for simulated user actions.
- `CONTENT_METADATA_TOPIC`: Compacted topic for content dimensions.
- `FEATURE_STORE_TOPIC`: Compacted topic acting as the real-time feature store.
- `PIPELINE_METRICS_TOPIC`: Topic for metrics.
- `FLINK_PARALLELISM`: Default parallelism for Flink jobs.
- `DASHBOARD_PORT`: Streamlit port (default: `8501`).

---

## Verification Guide

1. **Verify Topic Properties**:
   Check if topic compaction is enabled on `content-metadata` and `feature-store`:
   ```bash
   docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic feature-store
   ```
   You should see `cleanup.policy=compact` in the configuration.

2. **Inspect Dashboard Metrics**:
   On the dashboard, check the `Late Events Dropped` counter (which should increment because the producer generates ~7% late events) and the `Watermark Lag` showing how close event time is to wall-clock time.

3. **Inspect Entity Features**:
   Enter `usr_1001` in the Dashboard input box to view its live features, including:
   - `click_rate`
   - `avg_dwell_time`
   - `category_affinity_score` (a JSON map of event counts per category)
