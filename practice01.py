from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *

# 1. Create Spark session
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

# 3. Read stream directly from Kafka topic "USA"
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "USA") \
    .load()

# 4. Parse JSON from Kafka's "value" column
df_parsed = df_raw.select(
    from_json(col("value").cast("string"), location_schema).alias("data")
).select("data.*")


df1 = df_parsed.drop.na()
# 5. Example: just print out the live messages
query = df_parsed.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "E:/nikhil/Spark_stream/output") \
    .option("checkpointLocation", "E:/nikhil/Spark_stream/checkpoint") \
    .start()

query.awaitTermination()