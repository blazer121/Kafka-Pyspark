from kafka import KafkaConsumer
import os
from datetime import datetime

TOPIC = "USA"
BOOTSTRAP_SERVERS = ["localhost:9092"]
OUTPUT_DIR = r"E:\nikhil\Spark_stream"
BATCH_SIZE = 50

os.makedirs(OUTPUT_DIR, exist_ok=True)

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: x.decode('utf-8')
)

print(f"Listening to Kafka topic '{TOPIC}' and writing batches into {OUTPUT_DIR}...")

batch = []
for message in consumer:
    batch.append(message.value)

    if len(batch) >= BATCH_SIZE:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"batch_{timestamp}.txt"
        file_path = os.path.join(OUTPUT_DIR, filename)

        with open(file_path, 'w', encoding='utf-8') as f:
            f.write("\n".join(batch))

        print(f" Wrote {len(batch)} messages -> {file_path}")
        batch = [] 
