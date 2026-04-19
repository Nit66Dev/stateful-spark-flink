import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

plans = {"Basic": 149, "HD": 199, "FullHD": 499, "4K": 649}
actions = [
    "buy",
    "buy",
    "buy",
    "cancel",
]  # Weighting it so we get more buys than cancels

print("Sending subscription data to Kafka topic 'subscriptions'...")
while True:
    plan = random.choice(list(plans.keys()))
    action = random.choice(actions)

    # If they cancel, revenue is 0 for this event
    revenue = plans[plan] if action == "buy" else 0.0

    event = {
        "user_id": f"user_{random.randint(1000, 9999)}",
        "action": action,
        "plan": plan,
        "revenue": revenue,
        "timestamp": int(time.time()),
    }
    producer.send("subscriptions", event)
    print(f"Sent: {event}")
    time.sleep(0.5)
