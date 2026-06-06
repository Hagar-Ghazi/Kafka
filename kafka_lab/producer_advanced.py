import json
import time
import random
from confluent_kafka import Producer

# Configuration using the internal network listener
config = {
    'bootstrap.servers': 'localhost:9092', # Port mapped to host
    'client.id': 'advanced-transaction-producer',
    'acks': 'all' # Guarantee durability
}

producer = Producer(config)
topic_name = "topic_raw"

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        print(f"✅ Sent to {msg.topic()} [Partition: {msg.partition()}] Offset: {msg.offset()}")

# Mock streaming records
mock_transactions = [
    {"transaction_id": "T101", "user_id": 1001, "amount": 150000.0, "location": "Cairo"},
    {"transaction_id": "T102", "user_id": 1002, "amount": 4500.0, "location": "Alexandria"},
    {"transaction_id": "T103", "user_id": 1003, "amount": 230000.0, "location": "Cairo"},
    {"transaction_id": "T104", "user_id": 1004, "amount": 95000.0, "location": "Cairo"},
    {"transaction_id": "T105", "user_id": 1005, "amount": 12000.0, "location": "Alexandria"},
]

print("Starting Advanced Partition-Targeted Producer...")

try:
    for tx in mock_transactions:
        payload = json.dumps(tx)
        
        # Rule-based business routing logic
        if tx["location"] == "Cairo":
            target_partition = 0
        elif tx["location"] == "Alexandria":
            target_partition = 1
        else:
            continue # Skip unknown locations
            
        # Produce explicitly assigning the target partition
        producer.produce(
            topic=topic_name,
            value=payload,
            partition=target_partition,
            callback=delivery_report
        )
        # Flush internal buffer out to broker network
        producer.poll(0)
        time.sleep(1)

except KeyboardInterrupt:
    print("\n🛑 Stopping Producer pipeline")
finally:
    producer.flush()