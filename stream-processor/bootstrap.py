from pyspark.sql import SparkSession
from minio import Minio  # type: ignore

from utils import ensure_table, build_spark, ensure_bucket_exists, get_bucket_name
import config


def bootstrap() -> SparkSession:
    client = Minio(
        config.AWS_ENDPOINT_URL.replace("http://", "").replace("https://", ""),
        access_key=config.AWS_ACCESS_KEY_ID,
        secret_key=config.AWS_SECRET_ACCESS_KEY,
        secure=False,
    )

    ensure_bucket_exists(client, get_bucket_name(config.BRONZE_PATH))
    ensure_bucket_exists(client, get_bucket_name(config.SILVER_PATH))
    ensure_bucket_exists(client, get_bucket_name(config.GOLD_PATH))
    ensure_bucket_exists(client, get_bucket_name(config.BRONZE_CKPT))
    ensure_bucket_exists(client, get_bucket_name(config.SILVER_CKPT))
    ensure_bucket_exists(client, get_bucket_name(config.GOLD_CKPT))
    ensure_bucket_exists(client, get_bucket_name(config.SPARK_EVENT_LOG_DIR))
    ensure_bucket_exists(client, get_bucket_name(config.SPARK_HISTORY_LOG_DIR))

    spark = build_spark()
    ensure_table(
        spark,
        config.BRONZE_PATH,
        name="bronze_reviews",
        clustering_cols=["event_timestamp"],
        schema=config.RAW_SCHEMA,
    )

    ensure_table(
        spark,
        config.SILVER_PATH,
        name="silver_reviews",
        clustering_cols=["event_timestamp", "group_id", "id"],
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
