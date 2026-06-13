import os
import json
from datetime import datetime, timezone
from confluent_kafka import Consumer, KafkaError

def main():
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    user_events_topic = os.getenv("USER_EVENTS_TOPIC", "user-events")
    
    print(f"Connecting batch consumer to {bootstrap_servers}...")
    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": "batch-compute-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })
    
    consumer.subscribe([user_events_topic])
    
    events = []
    print("Reading events from Kafka (waiting 5 seconds for messages)...")
    
    # Poll for messages with a timeout
    start_time = datetime.now()
    while (datetime.now() - start_time).seconds < 5:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"Consumer error: {msg.error()}")
            continue
        
        try:
            event = json.loads(msg.value().decode("utf-8"))
            events.append(event)
        except Exception as e:
            print(f"Error parsing event: {e}")
            
    consumer.close()
    print(f"Read {len(events)} events.")
    
    if not events:
        print("No events found. Exiting.")
        return
        
    # Perform Batch aggregation
    # 1. Tumbling 1-Hour User Window Features: click_rate, avg_dwell_time
    user_windows = {}
    for ev in events:
        uid = ev["user_id"]
        ts_str = ev["timestamp"]
        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        # Get hour window start/end
        window_start = ts.replace(minute=0, second=0, microsecond=0)
        window_end = window_start + timedelta_hour(1)
        window_key = (uid, window_start.isoformat(), window_end.isoformat())
        
        if window_key not in user_windows:
            user_windows[window_key] = {"total": 0, "clicks": 0, "dwell_sum": 0.0}
            
        user_windows[window_key]["total"] += 1
        if ev["event_type"].lower() == "click":
            user_windows[window_key]["clicks"] += 1
        user_windows[window_key]["dwell_sum"] += ev.get("dwell_time_ms", 0)
        
    print("\n--- BATCH USER FEATURES (1-Hour Tumbling Windows) ---")
    for key, stats in user_windows.items():
        uid, start, end = key
        click_rate = stats["clicks"] / stats["total"] if stats["total"] > 0 else 0
        avg_dwell = stats["dwell_sum"] / stats["total"] if stats["total"] > 0 else 0
        print(f"User: {uid} | Window: {start} to {end}")
        print(f"  click_rate: {click_rate:.4f}")
        print(f"  avg_dwell_time: {avg_dwell:.1f}ms")

def timedelta_hour(hours):
    import datetime
    return datetime.timedelta(hours=hours)

if __name__ == "__main__":
    main()
