# consumer_avro_to_parquet.py
import os
import time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

# 1. Configure the schema registry connection to decode binary data
schema_registry_conf = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)
avro_deserializer = AvroDeserializer(schema_registry_client)

config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'avro-sales-parquet-sink',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(config)
consumer.subscribe(["sales_topic"])

BUFFER_LIMIT = 3
sales_buffer = []

print("💾 Avro Sales Sink Engine Active. Listening to 'sales_topic'")

def flush_sales_to_parquet(buffer_data):
    if not buffer_data:
        return
    
    timestamp = int(time.time())
    file_path = f"./data_lake/sales_batch_{timestamp}.parquet"
    
    # Convert list of deserialized dicts to Parquet
    df = pd.DataFrame(buffer_data)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, file_path)
    print(f"💎 Successfully flushed sales batch to disk: {file_path}")

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"❌ Error: {msg.error()}")
            continue

        # Automatically decode binary bytes back into a Python dictionary using Schema Registry
        decoded_order = avro_deserializer(msg.value(), None)
        sales_buffer.append(decoded_order)
        print(f"📦 Buffered sales order record: {decoded_order['order_id']}")

        if len(sales_buffer) >= BUFFER_LIMIT:
            flush_sales_to_parquet(sales_buffer)
            sales_buffer.clear()

except KeyboardInterrupt:
    if sales_buffer:
        print("\nFlushing remaining sales records before closing...")
        flush_sales_to_parquet(sales_buffer)
finally:
    consumer.close()