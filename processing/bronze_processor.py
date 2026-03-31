import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, MapType
)

# Schema for all incoming messages
MESSAGE_SCHEMA = StructType([
    StructField("source", StringType(), True),
    StructField("company", StringType(), True),
    StructField("text", StringType(), True),
    StructField("url", StringType(), True),
    StructField("score", LongType(), True),
    StructField("timestamp", StringType(), True),
    StructField("metadata", MapType(StringType(), StringType()), True)
])

def create_spark_session():
    return SparkSession.builder \
        .appName("PulseBoard-Bronze") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "io.delta:delta-spark_2.12:3.2.0") \
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.streaming.checkpointLocation",
                "./checkpoints/bronze") \
        .getOrCreate()

def run_bronze_pipeline(spark):
    print("Starting Bronze pipeline...")

    # Read from all raw Kafka topics
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "raw.hackernews,raw.github,raw.reddit") \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON messages
    parsed = raw_stream \
        .select(
            from_json(
                col("value").cast("string"),
                MESSAGE_SCHEMA
            ).alias("data"),
            col("timestamp").alias("kafka_timestamp"),
            col("topic")
        ) \
        .select(
            col("data.source"),
            col("data.company"),
            col("data.text"),
            col("data.url"),
            col("data.score"),
            col("data.timestamp").alias("event_timestamp"),
            col("data.metadata"),
            col("kafka_timestamp"),
            col("topic"),
            current_timestamp().alias("ingestion_timestamp"),
            lit("1.0").alias("pipeline_version")
        ) \
        .filter(col("source").isNotNull()) \
        .filter(col("company").isNotNull()) \
        .filter(col("text").isNotNull())

    # Write to Delta Lake Bronze table
    query = parsed.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "./checkpoints/bronze") \
        .start("./data/bronze")

    print("Bronze pipeline running — writing to ./data/bronze")
    print("Press Ctrl+C to stop")
    query.awaitTermination()

if __name__ == "__main__":
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    run_bronze_pipeline(spark)