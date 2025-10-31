from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime, timezone

# Kafka configuration
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),  # convert dict to JSON bytes
    key_serializer=lambda k: str(k).encode('utf-8')
)

topic_name = 'test'

def generate_event():
    """Generate a random JSON event"""
    return {
        "user_id": random.randint(100, 105),
        "amount": round(random.uniform(10, 500), 2),
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    }

print("🚀 Starting Kafka producer... Press Ctrl+C to stop.")

try:
    while True:
        event = generate_event()
        print(f"Producing event: {event}")

        producer.send(topic_name, key=event['user_id'], value=event)
        producer.flush()  # ensure message is sent immediately

        time.sleep(2)  # produce every 2 seconds

except KeyboardInterrupt:
    print("\n🛑 Stopping producer...")

finally:
    producer.close()
