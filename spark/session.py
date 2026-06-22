# spark/session.py
import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()

def create_spark_session(app_name: str = "MiningAnalytics") -> SparkSession:
    """
    Creates a production-ready Spark session with:
    - Kafka connector
    - PostgreSQL JDBC driver
    - S3 support via Hadoop AWS
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(os.getenv("SPARK_MASTER_URL", "local[*]"))

        # Kafka + PostgreSQL + S3 packages
        .config(
            "spark.jars.packages",
            ",".join([
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
                "org.postgresql:postgresql:42.7.1",
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.262",
            ])
        )

        # S3 config
        .config("spark.hadoop.fs.s3a.access.key",        os.getenv("AWS_ACCESS_KEY_ID", ""))
        .config("spark.hadoop.fs.s3a.secret.key",        os.getenv("AWS_SECRET_ACCESS_KEY", ""))
        .config("spark.hadoop.fs.s3a.endpoint",          "s3.amazonaws.com")
        .config("spark.hadoop.fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem")

        # Performance tuning
        .config("spark.sql.shuffle.partitions",           "4")       # small cluster — don't over-partition
        .config("spark.streaming.kafka.maxRatePerPartition", "1000")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints")

        # Exactly-once guarantees
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark
