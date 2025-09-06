from typing import Any

from pyspark.sql import SparkSession
from minio import Minio  # type: ignore
from pymongo import MongoClient

from utils import (
    ensure_table,
    build_spark,
    ensure_bucket_exists,
    get_bucket_name,
    ensure_mongo_collection,
)
import config


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
        clustering_cols=["group_id", "id"],
        schema=config.RAW_SCHEMA,
    )

    ensure_table(
        spark,
        config.GOLD_PATH,
        name="gold_reviews",
        clustering_cols=["group_id"],  # using group only for aggregation
        schema=config.AGGREGATION_SCHEMA,
    )

    return spark
