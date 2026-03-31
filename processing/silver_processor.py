from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, udf, lower, trim, length,
    current_timestamp, lit, hash
)
from pyspark.sql.types import FloatType, StringType, BooleanType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from langdetect import detect, LangDetectException

# Sentiment analyzer
analyzer = SentimentIntensityAnalyzer()

def get_sentiment(text):
    try:
        if not text or len(text.strip()) == 0:
            return 0.0
        scores = analyzer.polarity_scores(text)
        return float(scores['compound'])
    except:
        return 0.0

def get_language(text):
    try:
        if not text or len(text.strip()) < 10:
            return "unknown"
        return detect(text)
    except LangDetectException:
        return "unknown"

def is_english(text):
    try:
        if not text or len(text.strip()) < 10:
            return False
        return detect(text) == 'en'
    except:
        return False

# Register UDFs
sentiment_udf = udf(get_sentiment, FloatType())
language_udf = udf(get_language, StringType())
is_english_udf = udf(is_english, BooleanType())

def create_spark_session():
    return SparkSession.builder \
        .appName("PulseBoard-Silver") \
        .config("spark.jars.packages",
                "io.delta:delta-spark_2.12:3.2.0") \
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

def run_silver_pipeline(spark):
    print("Starting Silver pipeline...")

    # Read from Bronze Delta table
    bronze_df = spark.read.format("delta").load("./data/bronze")
    print(f"Bronze records loaded: {bronze_df.count()}")

    # Step 1 — Clean text
    cleaned = bronze_df \
        .withColumn("text", trim(col("text"))) \
        .withColumn("text_lower", lower(col("text"))) \
        .filter(length(col("text")) > 10) \
        .filter(col("text").isNotNull())

    # Step 2 — Language detection + filter English only
    with_language = cleaned.withColumn("language", language_udf(col("text")))
    english_only = with_language.filter(
        (col("language") == "en") | (col("language") == "unknown")
    )
    print(f"After language filter: {english_only.count()}")

    # Step 3 — Sentiment scoring
    with_sentiment = english_only.withColumn(
        "sentiment_score", sentiment_udf(col("text"))
    )

    # Step 4 — Sentiment label
    from pyspark.sql.functions import when
    with_label = with_sentiment.withColumn(
        "sentiment_label",
        when(col("sentiment_score") >= 0.05, "positive")
        .when(col("sentiment_score") <= -0.05, "negative")
        .otherwise("neutral")
    )

    # Step 5 — Deduplication by URL + company
    deduplicated = with_label.dropDuplicates(["url", "company"])
    print(f"After deduplication: {deduplicated.count()}")

    # Step 6 — Select final Silver schema
    silver_df = deduplicated.select(
        "source", "company", "text", "url", "score",
        "event_timestamp", "ingestion_timestamp",
        "sentiment_score", "sentiment_label", "language",
        "pipeline_version"
    )

    # Write to Silver Delta table
    silver_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save("./data/silver")

    print(f"Silver records written: {silver_df.count()}")
    print("Sample Silver data:")
    silver_df.select(
        "source", "company", "text", "sentiment_score", "sentiment_label"
    ).show(10, truncate=50)
    print("Silver pipeline complete!")

if __name__ == "__main__":
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    run_silver_pipeline(spark)