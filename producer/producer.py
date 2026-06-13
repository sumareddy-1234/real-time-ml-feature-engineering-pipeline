import os
import json
import random
import time
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
USER_EVENTS_TOPIC = os.getenv("USER_EVENTS_TOPIC", "user-events")
CONTENT_METADATA_TOPIC = os.getenv("CONTENT_METADATA_TOPIC", "content-metadata")
FEATURE_STORE_TOPIC = os.getenv("FEATURE_STORE_TOPIC", "feature-store")
PIPELINE_METRICS_TOPIC = os.getenv("PIPELINE_METRICS_TOPIC", "pipeline-metrics")

# Setup Producer
producer = Producer({
    "bootstrap.servers": BOOTSTRAP_SERVERS
})

users = [
    "usr_1001",
    "usr_1002",
    "usr_1003",
    "usr_1004",
    "usr_1005"
]

contents = [
    ("content_tech_001", "Technology"),
    ("content_scifi_001", "SciFi"),
    ("content_news_001", "News"),
    ("content_sports_001", "Sports"),
    ("content_edu_001", "Education")
]

event_types = [
    "view",
    "click",
    "like",
    "share"
]


def create_topics():
    print(f"Connecting to AdminClient at {BOOTSTRAP_SERVERS}...")
    admin_client = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})

    topics = [
        NewTopic(USER_EVENTS_TOPIC, num_partitions=3, replication_factor=1),
        NewTopic(CONTENT_METADATA_TOPIC, num_partitions=1, replication_factor=1, config={"cleanup.policy": "compact"}),
        NewTopic(FEATURE_STORE_TOPIC, num_partitions=1, replication_factor=1, config={"cleanup.policy": "compact"}),
        NewTopic(PIPELINE_METRICS_TOPIC, num_partitions=1, replication_factor=1)
    ]

    fs = admin_client.create_topics(topics)

    # Wait for topics creation to finish
    for topic, f in fs.items():
        try:
            f.result()
            print(f"Topic '{topic}' created successfully.")
        except Exception as e:
            if "TopicAlreadyExistsException" in str(e) or "already exists" in str(e).lower():
                print(f"Topic '{topic}' already exists.")
            else:
                print(f"Failed to create topic '{topic}': {e}")


def publish_metadata():
    print("Publishing content metadata...")

    for index, (content_id, category) in enumerate(contents, start=1):
        metadata = {
            "content_id": content_id,
            "category": category,
            "creator_id": f"creator_{index:03}",
            "publish_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        }

        producer.produce(
            CONTENT_METADATA_TOPIC,
            key=content_id.encode('utf-8'),
            value=json.dumps(metadata).encode('utf-8')
        )

    producer.flush()
    print("Metadata published successfully.")


def generate_event(virtual_clock):
    user_id = random.choice(users)
    content_id, _ = random.choice(contents)
    event_type = random.choice(event_types)
    dwell_time_ms = random.randint(500, 10000)

    # Ensure at least 5% of events are late (we use 7% to be safe)
    is_late_event = random.random() < 0.07

    if is_late_event:
        # Deliberately between 35 and 90 seconds in the past relative to virtual clock
        delay_seconds = random.randint(35, 90)
        event_time = virtual_clock - timedelta(seconds=delay_seconds)
    else:
        event_time = virtual_clock

    event = {
        "user_id": user_id,
        "content_id": content_id,
        "event_type": event_type,
        "dwell_time_ms": dwell_time_ms,
        "timestamp": event_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    }

    return event, is_late_event


def main():
    # Keep retrying connection/topic creation until Kafka is healthy
    retries = 30
    while retries > 0:
        try:
            create_topics()
            break
        except Exception as e:
            print(f"Waiting for Kafka to be ready... Error: {e}")
            time.sleep(2)
            retries -= 1

    publish_metadata()

    # The simulation runs in accelerated time
    # 1 second of real-time simulates 60 seconds (1 minute) of user activity
    # A 1-hour tumbling window (3600 seconds) will fire every 60 seconds (1 minute) of real time.
    virtual_clock = datetime.now(timezone.utc) - timedelta(hours=2) # Start in the past so it's fresh
    print("Starting user event generation...")

    while True:
        # Advance virtual clock by 60 simulated seconds for each loop iteration (1 real second)
        virtual_clock += timedelta(seconds=60)
        
        event, is_late = generate_event(virtual_clock)

        producer.produce(
            USER_EVENTS_TOPIC,
            key=event["user_id"].encode('utf-8'),
            value=json.dumps(event).encode('utf-8')
        )

        producer.poll(0)

        late_str = " [LATE]" if is_late else ""
        print(f"Generated event at {event['timestamp']}{late_str}: {event}")

        time.sleep(1)


if __name__ == "__main__":
    main()