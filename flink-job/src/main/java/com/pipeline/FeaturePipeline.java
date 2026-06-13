package com.pipeline;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class FeaturePipeline {

    public static void main(String[] args) throws Exception {
        System.out.println("Starting Flink Feature Engineering Pipeline...");

        // Setup Execution Environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Retrieve environment variables
        String bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        if (bootstrapServers == null) bootstrapServers = "kafka:9092";

        String userEventsTopic = System.getenv("USER_EVENTS_TOPIC");
        if (userEventsTopic == null) userEventsTopic = "user-events";

        String contentMetadataTopic = System.getenv("CONTENT_METADATA_TOPIC");
        if (contentMetadataTopic == null) contentMetadataTopic = "content-metadata";

        String featureStoreTopic = System.getenv("FEATURE_STORE_TOPIC");
        if (featureStoreTopic == null) featureStoreTopic = "feature-store";

        String pipelineMetricsTopic = System.getenv("PIPELINE_METRICS_TOPIC");
        if (pipelineMetricsTopic == null) pipelineMetricsTopic = "pipeline-metrics";

        System.out.println("Kafka Servers: " + bootstrapServers);
        System.out.println("User Events Topic: " + userEventsTopic);
        System.out.println("Content Metadata Topic: " + contentMetadataTopic);
        System.out.println("Feature Store Topic: " + featureStoreTopic);
        System.out.println("Pipeline Metrics Topic: " + pipelineMetricsTopic);

        // 1. Consume User Events
        KafkaSource<String> userEventsSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(userEventsTopic)
                .setGroupId("flink-user-events-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 2. Consume Content Metadata
        KafkaSource<String> metadataSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(contentMetadataTopic)
                .setGroupId("flink-metadata-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Parse user events (as Raw Stream first, then parse and assign timestamps/watermarks)
        DataStream<String> rawUserEvents = env.fromSource(userEventsSource, WatermarkStrategy.noWatermarks(), "UserEventsSource");
        DataStream<UserEvent> parsedUserEvents = rawUserEvents.flatMap(new UserEventParser());

        // Define watermark strategy: bounded out-of-orderness of exactly 30 seconds
        WatermarkStrategy<UserEvent> watermarkStrategy = WatermarkStrategy
                .<UserEvent>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                .withTimestampAssigner(new SerializableTimestampAssigner<UserEvent>() {
                    @Override
                    public long extractTimestamp(UserEvent element, long recordTimestamp) {
                        try {
                            return Instant.parse(element.getTimestamp()).toEpochMilli();
                        } catch (Exception e) {
                            return System.currentTimeMillis();
                        }
                    }
                });

        DataStream<UserEvent> userEventsWithWatermarks = parsedUserEvents.assignTimestampsAndWatermarks(watermarkStrategy);

        // Parse content metadata. Note: We mark this stream as IDLE so that it doesn't hold back downstream watermarks.
        WatermarkStrategy<String> metadataWatermarkStrategy = WatermarkStrategy.<String>noWatermarks()
                .withIdleness(Duration.ofSeconds(5));

        DataStream<ContentMetadata> contentMetadataParsed = env.fromSource(metadataSource, metadataWatermarkStrategy, "ContentMetadataSource")
                .flatMap(new ContentMetadataParser());

        // 3. Detect late events & track watermark lag
        OutputTag<MetricRecord> metricsTag = new OutputTag<MetricRecord>("pipeline-metrics-tag"){};
        SingleOutputStreamOperator<UserEvent> processedEvents = userEventsWithWatermarks
                .process(new PipelineMetricsEmitter(metricsTag));

        DataStream<MetricRecord> metricsStream = processedEvents.getSideOutput(metricsTag);

        // 4. Compute User Features (1-Hour Tumbling Window)
        DataStream<FeatureValue> userFeatures = processedEvents
                .keyBy(UserEvent::getUserId)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new UserFeatureAggregator(), new UserFeatureProcessWindowFunction());

        // 5. Compute Content Features (15-Minute Sliding Window, 5-Minute Slide)
        DataStream<FeatureValue> contentFeatures = processedEvents
                .keyBy(UserEvent::getContentId)
                .window(SlidingEventTimeWindows.of(Time.minutes(15), Time.minutes(5)))
                .aggregate(new ContentFeatureAggregator(), new ContentFeatureProcessWindowFunction());

        // 6. Enrich Stream (Stream-Table Join)
        DataStream<EnrichedUserEvent> enrichedEvents = processedEvents
                .keyBy(UserEvent::getContentId)
                .connect(contentMetadataParsed.keyBy(ContentMetadata::getContentId))
                .process(new EnrichmentCoProcessFunction());

        // 7. Compute User-Category Affinity (1-Hour Tumbling Window)
        DataStream<FeatureValue> affinityFeatures = enrichedEvents
                .keyBy(EnrichedUserEvent::getUserId)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new CategoryAffinityAggregator(), new CategoryAffinityProcessWindowFunction());

        // 8. Define Sinks using builder (avoids direct ProducerRecord / kafka-clients import)
        KafkaSink<FeatureValue> featureSink = KafkaSink.<FeatureValue>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                    KafkaRecordSerializationSchema.<FeatureValue>builder()
                        .setTopic(featureStoreTopic)
                        .setKeySerializationSchema(new FeatureValueKeySchema())
                        .setValueSerializationSchema(new FeatureValueJsonSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        KafkaSink<MetricRecord> metricsSink = KafkaSink.<MetricRecord>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                    KafkaRecordSerializationSchema.<MetricRecord>builder()
                        .setTopic(pipelineMetricsTopic)
                        .setValueSerializationSchema(new MetricRecordJsonSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // 9. Sink computed features and metrics
        userFeatures.sinkTo(featureSink);
        contentFeatures.sinkTo(featureSink);
        affinityFeatures.sinkTo(featureSink);
        metricsStream.sinkTo(metricsSink);

        env.execute("Flink Real-Time Feature Engineering Job");
    }

    // --- POJOs ---

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class UserEvent implements Serializable {
        @JsonProperty("user_id") public String userId;
        @JsonProperty("content_id") public String contentId;
        @JsonProperty("event_type") public String eventType;
        @JsonProperty("dwell_time_ms") public Integer dwellTimeMs;
        @JsonProperty("timestamp") public String timestamp;

        public UserEvent() {}

        public String getUserId() { return userId; }
        public String getContentId() { return contentId; }
        public String getEventType() { return eventType; }
        public Integer getDwellTimeMs() { return dwellTimeMs; }
        public String getTimestamp() { return timestamp; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ContentMetadata implements Serializable {
        @JsonProperty("content_id") public String contentId;
        @JsonProperty("category") public String category;
        @JsonProperty("creator_id") public String creatorId;
        @JsonProperty("publish_timestamp") public String publishTimestamp;

        public ContentMetadata() {}

        public String getContentId() { return contentId; }
        public String getCategory() { return category; }
        public String getCreatorId() { return creatorId; }
        public String getPublishTimestamp() { return publishTimestamp; }
    }

    public static class EnrichedUserEvent implements Serializable {
        public String userId;
        public String contentId;
        public String eventType;
        public Integer dwellTimeMs;
        public String timestamp;
        public String category;

        public EnrichedUserEvent() {}

        public String getUserId() { return userId; }
        public String getContentId() { return contentId; }
        public String getEventType() { return eventType; }
        public Integer getDwellTimeMs() { return dwellTimeMs; }
        public String getTimestamp() { return timestamp; }
        public String getCategory() { return category; }

        public void setUserId(String userId) { this.userId = userId; }
        public void setContentId(String contentId) { this.contentId = contentId; }
        public void setEventType(String eventType) { this.eventType = eventType; }
        public void setDwellTimeMs(Integer dwellTimeMs) { this.dwellTimeMs = dwellTimeMs; }
        public void setTimestamp(String timestamp) { this.timestamp = timestamp; }
        public void setCategory(String category) { this.category = category; }
    }

    public static class FeatureValue implements Serializable {
        @JsonProperty("entity_id") public String entityId;
        @JsonProperty("feature_name") public String featureName;
        @JsonProperty("feature_value") public Object featureValue;
        @JsonProperty("computed_at") public String computedAt;

        public FeatureValue() {}

        public FeatureValue(String entityId, String featureName, Object featureValue, String computedAt) {
            this.entityId = entityId;
            this.featureName = featureName;
            this.featureValue = featureValue;
            this.computedAt = computedAt;
        }
    }

    public static class MetricRecord implements Serializable {
        @JsonProperty("late_events_dropped") public Long lateEventsDropped;
        @JsonProperty("current_watermark") public Long currentWatermark;
        @JsonProperty("wall_clock_time") public Long wallClockTime;

        public MetricRecord() {}

        public MetricRecord(Long lateEventsDropped, Long currentWatermark, Long wallClockTime) {
            this.lateEventsDropped = lateEventsDropped;
            this.currentWatermark = currentWatermark;
            this.wallClockTime = wallClockTime;
        }
    }

    // --- Parsers ---

    public static class UserEventParser implements FlatMapFunction<String, UserEvent> {
        private transient ObjectMapper mapper;

        @Override
        public void flatMap(String value, Collector<UserEvent> out) {
            if (mapper == null) mapper = new ObjectMapper();
            try {
                UserEvent ev = mapper.readValue(value, UserEvent.class);
                if (ev.userId != null && ev.timestamp != null) {
                    out.collect(ev);
                }
            } catch (Exception e) {
                // Ignore parsing errors
            }
        }
    }

    public static class ContentMetadataParser implements FlatMapFunction<String, ContentMetadata> {
        private transient ObjectMapper mapper;

        @Override
        public void flatMap(String value, Collector<ContentMetadata> out) {
            if (mapper == null) mapper = new ObjectMapper();
            try {
                ContentMetadata meta = mapper.readValue(value, ContentMetadata.class);
                if (meta.contentId != null) {
                    out.collect(meta);
                }
            } catch (Exception e) {
                // Ignore parsing errors
            }
        }
    }

    // --- Process Function for Metrics ---

    public static class PipelineMetricsEmitter extends ProcessFunction<UserEvent, UserEvent> {
        private final OutputTag<MetricRecord> metricsTag;
        private static final long EMIT_INTERVAL_MS = 2000; // Emit metrics every 2 seconds
        private long lateCount = 0;
        private long lastEmitTime = 0;

        public PipelineMetricsEmitter(OutputTag<MetricRecord> metricsTag) {
            this.metricsTag = metricsTag;
        }

        @Override
        public void processElement(UserEvent value, Context ctx, Collector<UserEvent> out) throws Exception {
            long currentWatermark = ctx.timerService().currentWatermark();
            long timestamp = ctx.timestamp();
            long now = System.currentTimeMillis();

            boolean isLate = timestamp < currentWatermark;
            if (isLate) {
                lateCount++;
            }

            if (now - lastEmitTime >= EMIT_INTERVAL_MS) {
                lastEmitTime = now;
                ctx.output(metricsTag, new MetricRecord(lateCount, currentWatermark, now));
            }

            out.collect(value);
        }
    }

    // --- Stream-Table Enrichment Join Function ---

    public static class EnrichmentCoProcessFunction extends CoProcessFunction<UserEvent, ContentMetadata, EnrichedUserEvent> {
        private transient ValueState<ContentMetadata> metadataState;

        @Override
        public void open(Configuration parameters) throws Exception {
            metadataState = getRuntimeContext().getState(new ValueStateDescriptor<>("metadata-state", ContentMetadata.class));
        }

        @Override
        public void processElement1(UserEvent event, Context ctx, Collector<EnrichedUserEvent> out) throws Exception {
            ContentMetadata metadata = metadataState.value();
            String category = (metadata != null) ? metadata.getCategory() : "Unknown";

            EnrichedUserEvent enriched = new EnrichedUserEvent();
            enriched.setUserId(event.getUserId());
            enriched.setContentId(event.getContentId());
            enriched.setEventType(event.getEventType());
            enriched.setDwellTimeMs(event.getDwellTimeMs());
            enriched.setTimestamp(event.getTimestamp());
            enriched.setCategory(category);
            out.collect(enriched);
        }

        @Override
        public void processElement2(ContentMetadata metadata, Context ctx, Collector<EnrichedUserEvent> out) throws Exception {
            metadataState.update(metadata);
        }
    }

    // --- Tumbling 1-Hour User Window Aggregation ---

    public static class UserFeatureAccumulator implements Serializable {
        public long totalEvents = 0;
        public long clickEvents = 0;
        public double dwellTimeSum = 0;
    }

    public static class UserFeatureAggregator implements AggregateFunction<UserEvent, UserFeatureAccumulator, UserFeatureAccumulator> {
        @Override
        public UserFeatureAccumulator createAccumulator() {
            return new UserFeatureAccumulator();
        }

        @Override
        public UserFeatureAccumulator add(UserEvent value, UserFeatureAccumulator accumulator) {
            accumulator.totalEvents++;
            if ("click".equalsIgnoreCase(value.getEventType())) {
                accumulator.clickEvents++;
            }
            if (value.getDwellTimeMs() != null) {
                accumulator.dwellTimeSum += value.getDwellTimeMs();
            }
            return accumulator;
        }

        @Override
        public UserFeatureAccumulator getResult(UserFeatureAccumulator accumulator) {
            return accumulator;
        }

        @Override
        public UserFeatureAccumulator merge(UserFeatureAccumulator a, UserFeatureAccumulator b) {
            a.totalEvents += b.totalEvents;
            a.clickEvents += b.clickEvents;
            a.dwellTimeSum += b.dwellTimeSum;
            return a;
        }
    }

    public static class UserFeatureProcessWindowFunction extends ProcessWindowFunction<UserFeatureAccumulator, FeatureValue, String, TimeWindow> {
        @Override
        public void process(String userId, Context context, Iterable<UserFeatureAccumulator> elements, Collector<FeatureValue> out) {
            UserFeatureAccumulator acc = elements.iterator().next();
            long windowEnd = context.window().getEnd();
            String computedAt = Instant.ofEpochMilli(windowEnd).toString();

            double clickRate = acc.totalEvents > 0 ? (double) acc.clickEvents / acc.totalEvents : 0.0;
            double avgDwellTime = acc.totalEvents > 0 ? acc.dwellTimeSum / acc.totalEvents : 0.0;

            out.collect(new FeatureValue(userId, "click_rate", clickRate, computedAt));
            out.collect(new FeatureValue(userId, "avg_dwell_time", avgDwellTime, computedAt));
        }
    }

    // --- Sliding 15-Minute Content Window Aggregation ---

    public static class ContentFeatureAccumulator implements Serializable {
        public long views = 0;
        public long likesShares = 0;
    }

    public static class ContentFeatureAggregator implements AggregateFunction<UserEvent, ContentFeatureAccumulator, ContentFeatureAccumulator> {
        @Override
        public ContentFeatureAccumulator createAccumulator() {
            return new ContentFeatureAccumulator();
        }

        @Override
        public ContentFeatureAccumulator add(UserEvent value, ContentFeatureAccumulator accumulator) {
            if ("view".equalsIgnoreCase(value.getEventType())) {
                accumulator.views++;
            } else if ("like".equalsIgnoreCase(value.getEventType()) || "share".equalsIgnoreCase(value.getEventType())) {
                accumulator.likesShares++;
            }
            return accumulator;
        }

        @Override
        public ContentFeatureAccumulator getResult(ContentFeatureAccumulator accumulator) {
            return accumulator;
        }

        @Override
        public ContentFeatureAccumulator merge(ContentFeatureAccumulator a, ContentFeatureAccumulator b) {
            a.views += b.views;
            a.likesShares += b.likesShares;
            return a;
        }
    }

    public static class ContentFeatureProcessWindowFunction extends ProcessWindowFunction<ContentFeatureAccumulator, FeatureValue, String, TimeWindow> {
        @Override
        public void process(String contentId, Context context, Iterable<ContentFeatureAccumulator> elements, Collector<FeatureValue> out) {
            ContentFeatureAccumulator acc = elements.iterator().next();
            long windowEnd = context.window().getEnd();
            String computedAt = Instant.ofEpochMilli(windowEnd).toString();

            double engagementRate = acc.views > 0 ? (double) acc.likesShares / acc.views : 0.0;

            out.collect(new FeatureValue(contentId, "engagement_rate", engagementRate, computedAt));
        }
    }

    // --- Tumbling 1-Hour User-Category Affinity Aggregation ---

    public static class CategoryAffinityAccumulator implements Serializable {
        public Map<String, Long> categoryCounts = new HashMap<>();
    }

    public static class CategoryAffinityAggregator implements AggregateFunction<EnrichedUserEvent, CategoryAffinityAccumulator, CategoryAffinityAccumulator> {
        @Override
        public CategoryAffinityAccumulator createAccumulator() {
            return new CategoryAffinityAccumulator();
        }

        @Override
        public CategoryAffinityAccumulator add(EnrichedUserEvent value, CategoryAffinityAccumulator accumulator) {
            String category = value.getCategory();
            accumulator.categoryCounts.put(category, accumulator.categoryCounts.getOrDefault(category, 0L) + 1);
            return accumulator;
        }

        @Override
        public CategoryAffinityAccumulator getResult(CategoryAffinityAccumulator accumulator) {
            return accumulator;
        }

        @Override
        public CategoryAffinityAccumulator merge(CategoryAffinityAccumulator a, CategoryAffinityAccumulator b) {
            for (Map.Entry<String, Long> entry : b.categoryCounts.entrySet()) {
                a.categoryCounts.put(entry.getKey(), a.categoryCounts.getOrDefault(entry.getKey(), 0L) + entry.getValue());
            }
            return a;
        }
    }

    public static class CategoryAffinityProcessWindowFunction extends ProcessWindowFunction<CategoryAffinityAccumulator, FeatureValue, String, TimeWindow> {
        @Override
        public void process(String userId, Context context, Iterable<CategoryAffinityAccumulator> elements, Collector<FeatureValue> out) {
            CategoryAffinityAccumulator acc = elements.iterator().next();
            long windowEnd = context.window().getEnd();
            String computedAt = Instant.ofEpochMilli(windowEnd).toString();

            out.collect(new FeatureValue(userId, "category_affinity_score", acc.categoryCounts, computedAt));
        }
    }

    // --- Flink SerializationSchema impls (no direct kafka-clients imports needed) ---

    /** Extracts the Kafka message key: "entityId:featureName" */
    public static class FeatureValueKeySchema implements SerializationSchema<FeatureValue> {
        @Override
        public byte[] serialize(FeatureValue element) {
            return (element.entityId + ":" + element.featureName).getBytes(StandardCharsets.UTF_8);
        }
    }

    /** Serializes a FeatureValue to JSON bytes (Kafka message value) */
    public static class FeatureValueJsonSchema implements SerializationSchema<FeatureValue> {
        private transient ObjectMapper mapper;

        @Override
        public byte[] serialize(FeatureValue element) {
            if (mapper == null) mapper = new ObjectMapper();
            try {
                return mapper.writeValueAsBytes(element);
            } catch (Exception e) {
                throw new RuntimeException("Error serializing FeatureValue", e);
            }
        }
    }

    /** Serializes a MetricRecord to JSON bytes (Kafka message value, no key) */
    public static class MetricRecordJsonSchema implements SerializationSchema<MetricRecord> {
        private transient ObjectMapper mapper;

        @Override
        public byte[] serialize(MetricRecord element) {
            if (mapper == null) mapper = new ObjectMapper();
            try {
                return mapper.writeValueAsBytes(element);
            } catch (Exception e) {
                throw new RuntimeException("Error serializing MetricRecord", e);
            }
        }
    }
}
