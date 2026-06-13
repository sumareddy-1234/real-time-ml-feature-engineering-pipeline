# Real-Time ML Feature Engineering Pipeline Analysis

This report documents the architectural trade-offs, differences between batch and streaming computations, and the watermarking strategy for late-arriving events in the Apache Flink and Apache Kafka feature engineering pipeline.

## Batch vs. Streaming Divergence

When calculating machine learning features (such as user click rates or content engagement rates) using an offline batch script (e.g., Pandas, Apache Spark) versus a real-time streaming pipeline (Apache Flink), several key differences emerge:

### 1. Late Event Inclusion & Drops
- **Streaming Pipeline**: Apache Flink uses Event Time combined with a Watermark Strategy. In our implementation, a bounded out-of-orderness of 30 seconds is defined. If a user event arrives with an event timestamp that is older than the current watermark minus 30 seconds, Flink's window operator considers it too late and discards it.
- **Batch Processing**: An offline batch job reads all events from a static partition (e.g. daily logs) and executes aggregates across the entire dataset. It does not drop late events because all data is available statically, regardless of when it arrived at the ingestion broker.
- **Divergence**: Streaming features will occasionally omit late events that are dropped due to watermarks, whereas batch features will fully include them. This results in slightly different aggregate values (e.g. `click_rate` and `avg_dwell_time`) for windows that had late-arriving data.

### 2. Window Boundary Semantics & Latency
- **Streaming Pipeline**: Emits feature updates incrementally. Tumbling windows emit once the watermark passes the window end, whereas sliding windows emit every slide interval (e.g., every 5 minutes based on the last 15 minutes of data).
- **Batch Processing**: Typically runs at scheduled intervals (hourly or daily) and computes the final aggregate of a window.
- **ML Implications**: Machine learning models served with real-time streaming features will receive fresher features but must be robust to slight noise caused by dropped late events. If a model is trained exclusively on batch features (which include all late events) but served with streaming features (which drop late events), it can suffer from **feature drift** (skew between training and serving datasets), leading to degraded predictive accuracy.

---

## Late Event Handling

In real-world distributed environments, network lag, client-side buffering, and broker delays lead to out-of-order and late events. 

### 1. Watermark Strategy
Our Flink job implements a bounded out-of-orderness watermark strategy:
- **Time Characteristic**: Event Time (processing based on the ISO 8601 timestamp inside the message).
- **Tolerance**: 30 seconds.
- **Behavior**: The watermark is computed as `max_timestamp_seen - 30 seconds`. Flink assumes that no more events will arrive with timestamps older than this watermark.
- **Late events**:
  - If a user event is late by less than 30 seconds, it is correctly incorporated into its corresponding window (tumbling or sliding) because the window has not closed yet.
  - If an event is late by more than 30 seconds (e.g., between 35 and 90 seconds, as simulated by our data producer), it arrives after the watermark has passed the window's boundary. In this case, Flink's window operators drop the event.

### 2. Evidence of Late Event Handling
- **Metrics Emitter**: Our custom `PipelineMetricsEmitter` class processes the stream directly after watermarks are assigned. It compares each event's timestamp against the current watermark:
  ```java
  boolean isLate = timestamp < currentWatermark;
  if (isLate) {
      lateCount++;
  }
  ```
- **Dashboard Visibility**: The dashboard consumes these metrics in real-time from the `pipeline-metrics` Kafka topic and displays the `Late Events Dropped` counter.
- **What if an event arrived even later?**: If an event arrives after the window is completely finalized, Flink discards it by default. In a production scenario, if we must capture very late events, we can use Flink's `allowedLateness` feature to keep the window state active for a longer duration, or route the late data to a side-output topic for batch correction.
