import os
from google.cloud import bigquery
from pyspark.sql import SparkSession

def create_spark_session():
    return SparkSession.builder \
        .appName("PulseBoard-BigQuery-Loader") \
        .config("spark.jars.packages",
                "io.delta:delta-spark_2.12:3.2.0") \
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

def load_to_bigquery():
    project_id = "pulseboard-ai"
    dataset_id = "pulseboard_gold"
    credentials_path = os.path.expanduser("~/projects/pulseboard/credentials.json")

    # Initialize BigQuery client
    client = bigquery.Client.from_service_account_json(credentials_path)
    print(f"Connected to BigQuery project: {project_id}")

    # Load Spark session and read Gold tables
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    # Load company_sentiment table
    print("Loading company_sentiment to BigQuery...")
    df = spark.read.format("delta").load("./data/gold/company_sentiment")
    pandas_df = df.toPandas()

    table_id = f"{project_id}.{dataset_id}.company_sentiment"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE"
    )
    job = client.load_table_from_dataframe(
        pandas_df, table_id, job_config=job_config
    )
    job.result()
    print(f"Loaded {len(pandas_df)} rows to {table_id}")

    # Load source_breakdown table
    print("Loading source_breakdown to BigQuery...")
    df2 = spark.read.format("delta").load("./data/gold/source_breakdown")
    pandas_df2 = df2.toPandas()

    table_id2 = f"{project_id}.{dataset_id}.source_breakdown"
    job2 = client.load_table_from_dataframe(
        pandas_df2, table_id2, job_config=job_config
    )
    job2.result()
    print(f"Loaded {len(pandas_df2)} rows to {table_id2}")

    # Load sentiment_distribution table
    print("Loading sentiment_distribution to BigQuery...")
    df3 = spark.read.format("delta").load("./data/gold/sentiment_distribution")
    pandas_df3 = df3.toPandas()

    table_id3 = f"{project_id}.{dataset_id}.sentiment_distribution"
    job3 = client.load_table_from_dataframe(
        pandas_df3, table_id3, job_config=job_config
    )
    job3.result()
    print(f"Loaded {len(pandas_df3)} rows to {table_id3}")

    print("\nAll Gold tables loaded to BigQuery successfully!")
    print(f"View at: https://console.cloud.google.com/bigquery?project={project_id}")

if __name__ == "__main__":
    load_to_bigquery()