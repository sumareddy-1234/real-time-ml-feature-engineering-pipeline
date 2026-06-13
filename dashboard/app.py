import streamlit as st
import json
import os
import time
import threading
from datetime import datetime, timezone
from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError

load_dotenv()

st.set_page_config(
    page_title="Real-Time Feature Store Observability",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Premium Dark-Mode Glassmorphism Styling
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=Outfit:wght@400;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap');

    /* Global dark background */
    .stApp {
        background: linear-gradient(135deg, #0a0a0f 0%, #0d1117 40%, #0f0b1e 100%);
    }

    html, body, [class*="css"]  {
        font-family: 'Inter', sans-serif;
        color: #e2e8f0;
    }

    /* Hide Streamlit defaults */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}

    /* Main title with animated gradient */
    .main-header {
        font-family: 'Outfit', sans-serif;
        font-weight: 800;
        background: linear-gradient(135deg, #c084fc 0%, #818cf8 30%, #38bdf8 60%, #34d399 100%);
        background-size: 200% 200%;
        animation: gradient-shift 6s ease infinite;
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-size: 2.6rem;
        letter-spacing: -0.02em;
        margin-bottom: 0.2rem;
        line-height: 1.2;
    }

    @keyframes gradient-shift {
        0% { background-position: 0% 50%; }
        50% { background-position: 100% 50%; }
        100% { background-position: 0% 50%; }
    }

    .sub-header {
        color: #64748b;
        font-size: 1rem;
        font-weight: 400;
        margin-bottom: 1.8rem;
        letter-spacing: 0.01em;
    }

    .sub-header span {
        display: inline-block;
        padding: 2px 10px;
        border-radius: 20px;
        font-size: 0.75rem;
        font-weight: 600;
        letter-spacing: 0.05em;
        text-transform: uppercase;
        margin-left: 8px;
        vertical-align: middle;
    }

    .badge-kafka {
        background: rgba(239, 68, 68, 0.15);
        color: #f87171;
        border: 1px solid rgba(239, 68, 68, 0.25);
    }
    .badge-flink {
        background: rgba(99, 102, 241, 0.15);
        color: #a5b4fc;
        border: 1px solid rgba(99, 102, 241, 0.25);
    }
    .badge-live {
        background: rgba(52, 211, 153, 0.15);
        color: #6ee7b7;
        border: 1px solid rgba(52, 211, 153, 0.3);
        animation: pulse-badge 2s ease-in-out infinite;
    }

    @keyframes pulse-badge {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.6; }
    }

    /* Section Headers */
    .section-title {
        font-family: 'Outfit', sans-serif;
        font-weight: 700;
        font-size: 1.3rem;
        color: #e2e8f0;
        margin-top: 1.5rem;
        margin-bottom: 1rem;
        padding-bottom: 0.5rem;
        border-bottom: 1px solid rgba(148, 163, 184, 0.1);
    }

    /* Metric Cards with glassmorphism */
    .metric-card {
        background: rgba(15, 23, 42, 0.6);
        border: 1px solid rgba(148, 163, 184, 0.08);
        border-radius: 16px;
        padding: 1.4rem 1.6rem;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3), inset 0 1px 0 rgba(255, 255, 255, 0.04);
        backdrop-filter: blur(12px);
        -webkit-backdrop-filter: blur(12px);
        margin-bottom: 0.8rem;
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        position: relative;
        overflow: hidden;
    }

    .metric-card::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 2px;
        border-radius: 16px 16px 0 0;
    }

    .metric-card:hover {
        transform: translateY(-3px);
        border-color: rgba(148, 163, 184, 0.15);
        box-shadow: 0 12px 40px rgba(0, 0, 0, 0.4), inset 0 1px 0 rgba(255, 255, 255, 0.06);
    }

    .card-purple::before { background: linear-gradient(90deg, #c084fc, #818cf8); }
    .card-blue::before { background: linear-gradient(90deg, #38bdf8, #818cf8); }
    .card-red::before { background: linear-gradient(90deg, #f87171, #fb923c); }
    .card-amber::before { background: linear-gradient(90deg, #fbbf24, #f59e0b); }
    .card-emerald::before { background: linear-gradient(90deg, #34d399, #6ee7b7); }
    .card-cyan::before { background: linear-gradient(90deg, #22d3ee, #38bdf8); }

    .metric-icon {
        font-size: 1.6rem;
        margin-bottom: 0.4rem;
    }

    .metric-label {
        font-size: 0.72rem;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: #64748b;
        font-weight: 600;
        margin-bottom: 0.3rem;
    }

    .metric-value {
        font-family: 'Outfit', sans-serif;
        font-size: 2rem;
        font-weight: 700;
        color: #f1f5f9;
        line-height: 1.1;
    }

    .metric-sub {
        font-size: 0.75rem;
        color: #475569;
        margin-top: 0.5rem;
        font-weight: 400;
    }

    /* Feature table styling */
    .feature-row {
        background: rgba(15, 23, 42, 0.5);
        border: 1px solid rgba(148, 163, 184, 0.06);
        border-radius: 12px;
        padding: 1rem 1.4rem;
        margin-bottom: 0.5rem;
        display: flex;
        justify-content: space-between;
        align-items: center;
        transition: all 0.2s ease;
    }

    .feature-row:hover {
        background: rgba(15, 23, 42, 0.8);
        border-color: rgba(148, 163, 184, 0.12);
    }

    .feature-name {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.85rem;
        color: #c084fc;
        font-weight: 500;
    }

    .feature-val {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.9rem;
        color: #f1f5f9;
        font-weight: 600;
    }

    .feature-time {
        font-size: 0.72rem;
        color: #475569;
    }

    /* Entity search */
    .stTextInput > div > div > input {
        background: rgba(15, 23, 42, 0.7) !important;
        border: 1px solid rgba(148, 163, 184, 0.12) !important;
        border-radius: 12px !important;
        color: #e2e8f0 !important;
        font-family: 'JetBrains Mono', monospace !important;
        padding: 0.7rem 1rem !important;
    }

    .stTextInput > div > div > input:focus {
        border-color: rgba(192, 132, 252, 0.4) !important;
        box-shadow: 0 0 0 3px rgba(192, 132, 252, 0.1) !important;
    }

    /* Divider */
    hr {
        border: none;
        border-top: 1px solid rgba(148, 163, 184, 0.08);
        margin: 1.5rem 0;
    }

    /* Status dot */
    .status-dot {
        display: inline-block;
        width: 8px;
        height: 8px;
        border-radius: 50%;
        background: #34d399;
        margin-right: 6px;
        animation: pulse-dot 2s ease-in-out infinite;
        box-shadow: 0 0 8px rgba(52, 211, 153, 0.5);
    }

    @keyframes pulse-dot {
        0%, 100% { box-shadow: 0 0 8px rgba(52, 211, 153, 0.5); }
        50% { box-shadow: 0 0 16px rgba(52, 211, 153, 0.8); }
    }

    /* Expander styling */
    .streamlit-expanderHeader {
        background: rgba(15, 23, 42, 0.5) !important;
        border-radius: 12px !important;
        border: 1px solid rgba(148, 163, 184, 0.08) !important;
    }
</style>
""", unsafe_allow_html=True)


# Thread-safe Kafka Receiver Class
class KafkaReceiver:
    def __init__(self):
        self.lock = threading.Lock()
        self.features = {}  # entity_id -> {feature_name: {value, computed_at}}
        self.metrics = {
            "late_events_dropped": 0,
            "current_watermark": 0,
            "wall_clock_time": 0
        }
        self.last_update_times = {}  # feature_name -> timestamp of arrival
        self.event_count = 0

        # Start Background Thread
        self.thread = threading.Thread(target=self._consume_loop, daemon=True)
        self.thread.start()

    def _consume_loop(self):
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        feature_store_topic = os.getenv("FEATURE_STORE_TOPIC", "feature-store")
        pipeline_metrics_topic = os.getenv("PIPELINE_METRICS_TOPIC", "pipeline-metrics")

        # Keep retrying connection until Kafka is reachable
        connected = False
        consumer = None
        while not connected:
            try:
                consumer = Consumer({
                    "bootstrap.servers": bootstrap_servers,
                    "group.id": f"dashboard-group-{time.time()}",
                    "auto.offset.reset": "earliest",
                    "enable.auto.commit": True
                })
                consumer.subscribe([feature_store_topic, pipeline_metrics_topic])
                connected = True
                print("Dashboard Kafka Consumer connected successfully.")
            except Exception as e:
                print(f"Dashboard waiting for Kafka... Error: {e}")
                time.sleep(2)

        while True:
            try:
                msg = consumer.poll(0.5)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"Consumer error: {msg.error()}")
                    continue

                topic = msg.topic()
                payload = msg.value().decode("utf-8")
                data = json.loads(payload)

                with self.lock:
                    now_ts = time.time()
                    if topic == feature_store_topic:
                        entity_id = data.get("entity_id")
                        feature_name = data.get("feature_name")
                        feature_value = data.get("feature_value")
                        computed_at = data.get("computed_at")

                        if entity_id and feature_name:
                            if entity_id not in self.features:
                                self.features[entity_id] = {}
                            self.features[entity_id][feature_name] = {
                                "value": feature_value,
                                "computed_at": computed_at
                            }
                            self.last_update_times[feature_name] = now_ts
                            self.event_count += 1
                    elif topic == pipeline_metrics_topic:
                        self.metrics["late_events_dropped"] = data.get("late_events_dropped", 0)
                        self.metrics["current_watermark"] = data.get("current_watermark", 0)
                        self.metrics["wall_clock_time"] = data.get("wall_clock_time", 0)
            except Exception as e:
                print(f"Error processing Kafka message: {e}")
                time.sleep(0.5)


# Cache Receiver across Streamlit reruns
@st.cache_resource
def get_kafka_receiver():
    return KafkaReceiver()


receiver = get_kafka_receiver()

# ---- Header ----
st.markdown("""
<div class="main-header">Real-Time Feature Engineering</div>
<div class="sub-header">
    <span class="status-dot"></span>Live Pipeline Observability
    <span class="badge-kafka">Kafka</span>
    <span class="badge-flink">Flink</span>
    <span class="badge-live">Live</span>
</div>
""", unsafe_allow_html=True)

# Acquire snapshot
with receiver.lock:
    metrics = receiver.metrics.copy()
    features_data = {k: dict(v) for k, v in receiver.features.items()}
    freshness_times = receiver.last_update_times.copy()
    total_features = receiver.event_count

# ---- Metrics Row 1: Pipeline Health ----
st.markdown('<div class="section-title">⚡ Pipeline Health</div>', unsafe_allow_html=True)
col1, col2, col3, col4 = st.columns(4)

# Feature freshness: click_rate
click_rate_updated = freshness_times.get("click_rate")
if click_rate_updated:
    secs = int(time.time() - click_rate_updated)
    click_freshness = f"{secs}s" if secs < 120 else f"{secs // 60}m"
else:
    click_freshness = "Awaiting"

with col1:
    st.markdown(f"""
    <div class="metric-card card-purple">
        <div class="metric-icon">🎯</div>
        <div class="metric-label">click_rate Freshness</div>
        <div class="metric-value" style="color: #c084fc;">{click_freshness}</div>
        <div class="metric-sub">Tumbling 1-Hour User Window</div>
    </div>
    """, unsafe_allow_html=True)

# Feature freshness: engagement_rate
engagement_updated = freshness_times.get("engagement_rate")
if engagement_updated:
    secs = int(time.time() - engagement_updated)
    eng_freshness = f"{secs}s" if secs < 120 else f"{secs // 60}m"
else:
    eng_freshness = "Awaiting"

with col2:
    st.markdown(f"""
    <div class="metric-card card-blue">
        <div class="metric-icon">📊</div>
        <div class="metric-label">engagement_rate Freshness</div>
        <div class="metric-value" style="color: #38bdf8;">{eng_freshness}</div>
        <div class="metric-sub">Sliding 15-Min Content Window</div>
    </div>
    """, unsafe_allow_html=True)

# Late Events Dropped
late_dropped = metrics.get("late_events_dropped", 0)
with col3:
    st.markdown(f"""
    <div class="metric-card card-red">
        <div class="metric-icon">🚨</div>
        <div class="metric-label">Late Events Dropped</div>
        <div class="metric-value" style="color: #f87171;">{late_dropped}</div>
        <div class="metric-sub">Dropped by 30s Watermark</div>
    </div>
    """, unsafe_allow_html=True)

# Watermark Lag
wm = metrics.get("current_watermark", 0)
wall = metrics.get("wall_clock_time", 0)

if wm and wall:
    lag_seconds = max(0.0, (wall - wm) / 1000.0)
    lag_str = f"{lag_seconds:.1f}s"
else:
    lag_str = "N/A"

with col4:
    st.markdown(f"""
    <div class="metric-card card-amber">
        <div class="metric-icon">⏱️</div>
        <div class="metric-label">Watermark Lag</div>
        <div class="metric-value" style="color: #fbbf24;">{lag_str}</div>
        <div class="metric-sub">Wall-Clock vs Event-Time Delta</div>
    </div>
    """, unsafe_allow_html=True)

# ---- Metrics Row 2: Store Stats ----
st.markdown('<div class="section-title">🗄️ Feature Store Stats</div>', unsafe_allow_html=True)
col5, col6, col7, col8 = st.columns(4)

# Total entities
entity_count = len(features_data)
with col5:
    st.markdown(f"""
    <div class="metric-card card-emerald">
        <div class="metric-icon">👥</div>
        <div class="metric-label">Active Entities</div>
        <div class="metric-value" style="color: #34d399;">{entity_count}</div>
        <div class="metric-sub">Users + Content Items</div>
    </div>
    """, unsafe_allow_html=True)

# Total feature updates
with col6:
    st.markdown(f"""
    <div class="metric-card card-cyan">
        <div class="metric-icon">🔄</div>
        <div class="metric-label">Feature Updates</div>
        <div class="metric-value" style="color: #22d3ee;">{total_features}</div>
        <div class="metric-sub">Total Computed Features</div>
    </div>
    """, unsafe_allow_html=True)

# Unique feature types
all_feature_names = set()
for fmap in features_data.values():
    all_feature_names.update(fmap.keys())
with col7:
    st.markdown(f"""
    <div class="metric-card card-purple">
        <div class="metric-icon">🧬</div>
        <div class="metric-label">Feature Types</div>
        <div class="metric-value" style="color: #a78bfa;">{len(all_feature_names)}</div>
        <div class="metric-sub">{', '.join(sorted(all_feature_names)) if all_feature_names else 'Waiting...'}</div>
    </div>
    """, unsafe_allow_html=True)

# Affinity freshness
affinity_updated = freshness_times.get("category_affinity_score")
if affinity_updated:
    secs = int(time.time() - affinity_updated)
    aff_freshness = f"{secs}s" if secs < 120 else f"{secs // 60}m"
else:
    aff_freshness = "Awaiting"

with col8:
    st.markdown(f"""
    <div class="metric-card card-blue">
        <div class="metric-icon">🧠</div>
        <div class="metric-label">Affinity Score Freshness</div>
        <div class="metric-value" style="color: #818cf8;">{aff_freshness}</div>
        <div class="metric-sub">1-Hour User-Category Window</div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("---")

# ---- Entity Feature Viewer ----
st.markdown('<div class="section-title">🔍 Entity Feature Viewer</div>', unsafe_allow_html=True)
search_id = st.text_input("Enter User ID or Content ID to inspect features:", value="usr_1001", label_visibility="collapsed", placeholder="Search entity (e.g. usr_1001, content_tech_001)").strip()

if search_id:
    entity_features = features_data.get(search_id)
    if entity_features:
        for name, details in sorted(entity_features.items()):
            val = details["value"]
            if isinstance(val, dict):
                val_str = json.dumps(val, indent=2)
            elif isinstance(val, float):
                val_str = f"{val:.4f}"
            else:
                val_str = str(val)

            computed = details.get("computed_at", "N/A")

            st.markdown(f"""
            <div class="feature-row">
                <div>
                    <div class="feature-name">{name}</div>
                    <div class="feature-time">Computed at: {computed}</div>
                </div>
                <div class="feature-val">{val_str}</div>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.markdown(f"""
        <div class="metric-card card-blue" style="text-align:center; padding: 2rem;">
            <div style="font-size: 1.5rem; margin-bottom: 0.5rem;">⏳</div>
            <div style="color: #64748b;">No features computed yet for <span style="color:#c084fc; font-family:'JetBrains Mono',monospace;">{search_id}</span></div>
            <div style="color: #475569; font-size: 0.8rem; margin-top: 0.3rem;">Features will appear after the first window closes</div>
        </div>
        """, unsafe_allow_html=True)

# Full Entity Store (collapsible)
with st.expander("📂 View All Entities in Feature Store"):
    if features_data:
        for eid in sorted(features_data.keys()):
            st.markdown(f"**`{eid}`**")
            for fname, fdetails in sorted(features_data[eid].items()):
                fval = fdetails["value"]
                if isinstance(fval, dict):
                    fval = json.dumps(fval)
                elif isinstance(fval, float):
                    fval = f"{fval:.4f}"
                st.markdown(f"  - `{fname}`: **{fval}** _(at {fdetails.get('computed_at', 'N/A')})_")
    else:
        st.markdown("_Feature store is currently empty. Waiting for first window to close..._")

# Self-Rerun for Real-Time Liveness
time.sleep(2)
st.rerun()