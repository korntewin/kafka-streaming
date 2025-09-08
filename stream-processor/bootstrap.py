from typing import Any

import config
from minio import Minio  # type: ignore
from pymongo import MongoClient
from pyspark.sql import SparkSession
from utils import (
    build_spark,
    ensure_bucket_exists,
    ensure_mongo_collection,
    ensure_table,
    get_bucket_name,
    optimize_table,
    vacuum_table,
)


def bootstrap() -> SparkSession:
    # Ensure mongo DB
    client = MongoClient[Any](config.MONGO_URI)

    ensure_mongo_collection(client, config.MONGO_DB_NAME, config.MONGO_COLLECTION)

    # Bootstrap S3 buckets
    client = Minio(
        config.AWS_ENDPOINT_URL.replace("http://", "").replace("https://", ""),
        access_key=config.AWS_ACCESS_KEY_ID,
        secret_key=config.AWS_SECRET_ACCESS_KEY,
        secure=False,
    )

    ensure_bucket_exists(client, get_bucket_name(config.SILVER_PATH))
    ensure_bucket_exists(client, get_bucket_name(config.GOLD_PATH))
    ensure_bucket_exists(client, get_bucket_name(config.SILVER_CKPT))
    ensure_bucket_exists(client, get_bucket_name(config.GOLD_CKPT))
    ensure_bucket_exists(client, get_bucket_name(config.SPARK_EVENT_LOG_DIR))
    ensure_bucket_exists(client, get_bucket_name(config.MONGO_CKPT))

    # Bootstrap delta tables
    spark = build_spark()

    ensure_table(
        spark,
        config.SILVER_PATH,
        name="silver_reviews",
        clustering_cols=["minute_timestamp"],
        schema=config.RAW_SCHEMA,
    )

    ensure_table(
        spark,
        config.GOLD_PATH,
        name="gold_reviews",
        clustering_cols=["group_id"],
        schema=config.AGGREGATION_SCHEMA,
    )

    # Optimize tables if exist
    optimize_table(spark, "silver_reviews", [])
    optimize_table(spark, "gold_reviews", [])
    vacuum_table(spark, "silver_reviews", config.SILVER_PATH, retention_hours=168)
    vacuum_table(spark, "gold_reviews", config.GOLD_PATH, retention_hours=168)

    return spark
