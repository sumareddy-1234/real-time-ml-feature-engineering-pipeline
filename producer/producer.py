import os
import json
import random
import time
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from confluent_kafka import Producer

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:9092"
)

USER_EVENTS_TOPIC = os.getenv(
    "USER_EVENTS_TOPIC",
    "user-events"
)

CONTENT_METADATA_TOPIC = os.getenv(
    "CONTENT_METADATA_TOPIC",
    "content-metadata"
)

producer = Producer(
    {
        "bootstrap.servers": BOOTSTRAP_SERVERS
    }
)

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


def publish_metadata():
    print("Publishing content metadata...")

    for index, (content_id, category) in enumerate(contents, start=1):

        metadata = {
            "content_id": content_id,
            "category": category,
            "creator_id": f"creator_{index:03}",
            "publish_timestamp": datetime.now(
                timezone.utc
            ).isoformat()
        }

        producer.produce(
            CONTENT_METADATA_TOPIC,
            key=content_id,
            value=json.dumps(metadata)
        )

    producer.flush()

    print("Metadata published successfully.")


def generate_event():

    user_id = random.choice(users)

    content_id, _ = random.choice(contents)

    event_type = random.choice(event_types)

    dwell_time_ms = random.randint(
        500,
        10000
    )

    current_time = datetime.now(
        timezone.utc
    )

    is_late_event = random.random() < 0.05

    if is_late_event:

        delay_seconds = random.randint(
            35,
            90
        )

        event_time = current_time - timedelta(
            seconds=delay_seconds
        )

    else:

        event_time = current_time

    event = {
        "user_id": user_id,
        "content_id": content_id,
        "event_type": event_type,
        "dwell_time_ms": dwell_time_ms,
        "timestamp": event_time.isoformat()
    }

    return event


def main():

    publish_metadata()

    print("Starting user event generation...")

    while True:

        event = generate_event()

        producer.produce(
            USER_EVENTS_TOPIC,
            value=json.dumps(event)
        )

        producer.poll(0)

        print(event)

        time.sleep(1)


if __name__ == "__main__":
    main()