import json
import os
import time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer


config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'parquet-lakehouse-sink',
    'auto.offset.reset': 'earliest'
}


consumer = Consumer(config)
consumer.subscribe(["topic_fraud"])

BUFFER_LIMIT = 2
msg_buffer = []

print("💾 Parquet Lakehouse Sink Engine Active. Listening to 'topic_fraud'")

def flush_buffer_to_parquet(buffer_data):
    if not buffer_data:
        return
    
    timestamp = int(time.time())
    file_path = f"./data_lake/fraud_batch_{timestamp}.parquet"
    
    # Convert list of dicts to Pandas DataFrame then to PyArrow Table
    df = pd.DataFrame(buffer_data)
    table = pa.Table.from_pandas(df)
    
    # Commit table out onto disk storage filesystem as Parquet
    pq.write_table(table, file_path)
    print(f"Successfully flushed batch out to disk: {file_path}")

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"❌ Error: {msg.error()}")
            continue

        data = json.loads(msg.value().decode('utf-8'))
        msg_buffer.append(data)
        print(f"📦 Buffered fraud alert record: {data['transaction_id']}")

        # Time or Size-based buffer trigger threshold
        if len(msg_buffer) >= BUFFER_LIMIT:
            flush_buffer_to_parquet(msg_buffer)
            msg_buffer.clear()

except KeyboardInterrupt:
    if msg_buffer:
        print("\nFlushing remaining buffered records before closing")
        flush_buffer_to_parquet(msg_buffer)
finally:
    consumer.close()