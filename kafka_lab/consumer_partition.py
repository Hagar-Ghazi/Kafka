import json
from confluent_kafka import Consumer, TopicPartition, Producer

consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'isolated-cairo-engine',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False # Manual offset commit for production safety
}

producer_config = {'bootstrap.servers': 'localhost:9092'}

consumer = Consumer(consumer_config)
producer = Producer(producer_config)

# Explicitly pin this consumer instance to Partition 0 only
target_partition = TopicPartition("topic_raw", 0)
consumer.assign([target_partition])

print("🕵️ Cairo Filter Engine Active. Watching Partition 0 exclusively")

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"❌ Consumer error: {msg.error()}")
            continue

        # Parse data payload
        data = json.loads(msg.value().decode('utf-8'))
        print(f"📥 Scanned Record: {data['transaction_id']} | Amt: {data['amount']} | Loc: {data['location']}")

        # Fraud Rule evaluation
        if data["amount"] > 100000.0:
            print(f"🚨 ALERT: High Value Fraud Candidate Detected: {data['transaction_id']}")
            
            # Forward record downstream
            producer.produce(
                topic="topic_fraud",
                value=json.dumps(data)
            )
            producer.flush()
            
        # Manually commit offset progress safely
        consumer.commit(msg, asynchronous=False)

except KeyboardInterrupt:
    print("\n🛑 Shutting down Filter Engine")
finally:
    consumer.close()