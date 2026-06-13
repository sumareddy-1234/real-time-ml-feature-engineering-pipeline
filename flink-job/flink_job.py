import os
import json
import time
from confluent_kafka import Consumer

print("Flink Feature Engineering Job Started")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("USER_EVENTS_TOPIC", "user-events")

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "feature-group",
    "auto.offset.reset": "earliest"
})

consumer.subscribe([TOPIC])

features = {}

FILE_PATH = "/app/features.json"

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue

    if msg.error():
        continue

    event = json.loads(msg.value().decode("utf-8"))

    uid = event["user_id"]
    etype = event["event_type"]

    if uid not in features:
        features[uid] = {
            "events": 0,
            "likes": 0,
            "shares": 0,
            "views": 0,
            "clicks": 0
        }

    features[uid]["events"] += 1
    features[uid][etype + "s"] += 1

    # SAVE TO SHARED FILE (KEY FIX)
    with open(FILE_PATH, "w") as f:
        json.dump(features, f)

    print("Updated:", uid, features[uid])
    time.sleep(0.2)