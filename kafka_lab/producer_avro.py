import time
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# 1. Define the exact Avro contract matching your lab instructions
avro_schema_str = """
{
  "type": "record",
  "name": "OrderValue",
  "namespace": "com.iti.sales",
  "fields": [
    {"name": "order_id", "type": "int"},
    {"name": "item_name", "type": "string"},
    {"name": "price", "type": "float"}
  ]
}
"""

# 2. Configure clients to use the local registry server listener
schema_registry_conf = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

avro_serializer = AvroSerializer(
    schema_registry_client,
    avro_schema_str
)

producer_config = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'avro-sales-producer'
}
producer = Producer(producer_config)

topic_name = "sales_topic"

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Avro Delivery failed: {err}")
    else:
        print(f"💎 Avro Encoded Message Sent to {msg.topic()} [Partition: {msg.partition()}]")

# Mock data conforming strictly to the Avro schema datatypes
mock_orders = [
    {"order_id": 5001, "item_name": "MacBook Pro", "price": 2499.99},
    {"order_id": 5002, "item_name": "Dell XPS", "price": 1450.50},
    {"order_id": 5003, "item_name": "iPad Air", "price": 599.00}
]

print("🚀 Launching Binary Avro Schema Registered Producer...")

# Build the explicit serialization context matching your topic name
ctx = SerializationContext(topic_name, MessageField.VALUE)

try:
    for order in mock_orders:
        producer.produce(
            topic=topic_name,
            key=str(order["order_id"]),
            value=avro_serializer(order, ctx), # 💡 Pass the explicit context here instead of None
            callback=delivery_report
        )
        producer.poll(0)
        time.sleep(1)
except KeyboardInterrupt:
    print("\n🛑 Stopping Avro Producer.")
finally:
    producer.flush()