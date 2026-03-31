from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, count, round, max, min,
    current_timestamp, desc, sum as spark_sum
)

def create_spark_session():
    return SparkSession.builder \
        .appName("PulseBoard-Gold") \
        .config("spark.jars.packages",
                "io.delta:delta-spark_2.12:3.2.0") \
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

def run_gold_pipeline(spark):
    print("Starting Gold pipeline...")

    # Read from Silver Delta table
    silver_df = spark.read.format("delta").load("./data/silver")
    print(f"Silver records loaded: {silver_df.count()}")

    # Gold Table 1 — Company Sentiment Summary
    company_sentiment = silver_df.groupBy("company") \
        .agg(
            round(avg("sentiment_score"), 4).alias("avg_sentiment"),
            count("*").alias("total_mentions"),
            count(col("sentiment_label") == "positive").alias("positive_count"),
            round(avg("score"), 2).alias("avg_engagement_score"),
            max("ingestion_timestamp").alias("last_updated")
        ) \
        .withColumn(
            "trending_score",
            round(
                (col("total_mentions") * 0.4) +
                (col("avg_sentiment") * 10 * 0.3) +
                (col("avg_engagement_score") * 0.001 * 0.3),
                4
            )
        ) \
        .orderBy(desc("trending_score"))

    print("\n=== COMPANY LEADERBOARD ===")
    company_sentiment.select(
        "company", "avg_sentiment", "total_mentions", "trending_score"
    ).show(20, truncate=30)

    # Gold Table 2 — Source Breakdown
    source_breakdown = silver_df.groupBy("source", "company") \
        .agg(
            count("*").alias("mention_count"),
            round(avg("sentiment_score"), 4).alias("avg_sentiment")
        ) \
        .orderBy(desc("mention_count"))

    print("\n=== SOURCE BREAKDOWN ===")
    source_breakdown.show(10, truncate=30)

    # Gold Table 3 — Sentiment Distribution
    sentiment_dist = silver_df.groupBy("company", "sentiment_label") \
        .agg(count("*").alias("count")) \
        .orderBy("company", "sentiment_label")

    # Write all Gold tables
    company_sentiment.write \
        .format("delta") \
        .mode("overwrite") \
        .save("./data/gold/company_sentiment")

    source_breakdown.write \
        .format("delta") \
        .mode("overwrite") \
        .save("./data/gold/source_breakdown")

    sentiment_dist.write \
        .format("delta") \
        .mode("overwrite") \
        .save("./data/gold/sentiment_distribution")

    print("\nGold pipeline complete!")
    print("Tables written: company_sentiment, source_breakdown, sentiment_distribution")

if __name__ == "__main__":
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    run_gold_pipeline(spark)