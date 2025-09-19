from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *

# 1. Create Spark session (no need for .config if jar is in jars folder)
spark = SparkSession.builder \
    .appName("KafkaSparkUSA") \
    .getOrCreate()

# 2. Define schema for JSON messages
location_schema = StructType([
    StructField("id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True)
])

# 3. Read from Kafka topic
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "USA") \
    .load()

# 4. Parse JSON from Kafka's "value" column
df_parsed = df_raw.select(
    from_json(col("value").cast("string"), location_schema).alias("data")
).select("data.*")

# 5. Write to PostgreSQL using foreachBatch
def write_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/PMG") \
        .option("dbtable", "public.kafka_stream_data") \
        .option("user", "postgres") \
        .option("password", "TestPass123") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

query = df_parsed.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

query.awaitTermination()
