from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, sum as _sum, count as _count
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

# Initialize Spark Session
spark = (
    SparkSession.builder.appName("StatefulProcessing")
    .master("local[*]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .getOrCreate()
)

# Define schema for the new generator payload
schema = (
    StructType()
    .add("user_id", StringType())
    .add("action", StringType())
    .add("plan", StringType())
    .add("revenue", DoubleType())
    .add("timestamp", TimestampType())
)

# Read streaming data from Kafka
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "subscriptions")
    .load()
)

parsed_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# STATEFUL OPERATION: Tumbling Window Aggregation (1 minute)
# Group by time window, subscription plan, and the action (buy/cancel)
aggregated_df = (
    parsed_df.withWatermark("timestamp", "10 seconds")
    .groupBy(window(col("timestamp"), "1 minute"), col("plan"), col("action"))
    .agg(_sum("revenue").alias("total_revenue"), _count("user_id").alias("event_count"))
)

# Format the output to match our Postgres table schema
final_df = aggregated_df.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("plan"),
    col("action"),
    col("total_revenue"),
    col("event_count"),
)

# Start the stream - Output to Console
query = (
    final_df.writeStream.outputMode("update")
    .format("console")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()
