import streamlit as st

st.set_page_config(
    page_title="Real-Time ML Feature Store",
    layout="wide"
)

st.title("Real-Time ML Feature Engineering Dashboard")

st.success("Dashboard container is running successfully.")

st.subheader("Entity Viewer")

entity_id = st.text_input(
    "Enter User ID or Content ID"
)

if entity_id:
    st.write(
        f"Searching features for: {entity_id}"
    )

st.subheader("Pipeline Metrics")

st.metric(
    "Feature Freshness",
    "0 sec"
)

st.metric(
    "Late Events Dropped",
    "0"
)

st.metric(
    "Current Watermark Lag",
    "0 sec"
)