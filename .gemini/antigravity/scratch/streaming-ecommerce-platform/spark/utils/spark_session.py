"""
SparkSession factory configured with Kafka and MinIO (S3A) storage connectors.
"""

import os
import logging
from pyspark.sql import SparkSession

logger = logging.getLogger("SparkSessionBuilder")


def get_spark_session(app_name: str = "EcommerceStreamingPipeline") -> SparkSession:
    """
    Creates or retrieves a configured SparkSession.
    Configures S3A connectors for MinIO and Kafka integration.
    """
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    spark_master = os.getenv("SPARK_MASTER_URL", "local[*]")

    # Required Maven packages for Kafka streaming and AWS S3/MinIO
    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    ]

    builder = (
        SparkSession.builder.appName(app_name)
        .master(spark_master)
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.shuffle.partitions", "4")
        # S3A MinIO Configuration
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"Initialized SparkSession: version={spark.version}, master={spark_master}")
    return spark
