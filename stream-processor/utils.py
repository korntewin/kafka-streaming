from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from delta import *  # type: ignore
from minio import Minio # type: ignore

import config


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("reviews-stream-processor")
        # Spark cluster config
        .master(config.MASTER_URL)
        .config("spark.ui.port", config.UI_PORT)
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", config.SPARK_EVENT_LOG_DIR)
        .config("spark.history.fs.logDirectory", config.SPARK_HISTORY_LOG_DIR)
        .config("spark.executor.instances", str(config.SPARK_WORKER_INSTANCES))
        .config("spark.executor.cores", "2")
        .config("spark.executor.memory", "2g")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", config.SPARK_SHUFFLE_PARTITIONS)

        # Delta Lake config
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")

        # S3 compatible object storage config
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.access.key", config.AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", config.AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.endpoint", config.AWS_ENDPOINT_URL)
        .config("spark.hadoop.fs.s3a.comitter.name", "directory")
        .config("spark.hadoop.fs.s3a.comitter.magic.enabled", "false")
    )
    spark = configure_spark_with_delta_pip(
        builder, extra_packages=config.SPARK_PACKAGES
    ).getOrCreate()
    spark.sparkContext.setLogLevel(config.SPARK_LOG_LEVEL)
    return spark


def ensure_table(
    spark: SparkSession,
    path: str,
    name: str,
    clustering_cols: list[str],
    enable_cdf: bool = True,
    schema: StructType | None = None,
):
    """Create Delta table at path if it does not exist with requested properties.

    Uses a simple empty DataFrame write to create the table with schema & properties
    instead of raw SQL + private APIs for broader compatibility.
    """
    if schema is None:
        raise ValueError("Schema required to create table")

    (
        DeltaTable.createIfNotExists(spark)
        .tableName(name)
        .addColumns(schema)
        .location(str(path))
        .clusterBy(*clustering_cols)
        .property("delta.enableChangeDataFeed", "true" if enable_cdf else "false")
        .execute()
    )


def ensure_bucket_exists(client: Minio, bucket_name: str):
    if not client.bucket_exists(bucket_name):
        print(f"Creating bucket {bucket_name}")
        client.make_bucket(bucket_name)


def get_bucket_name(uri: str) -> str:
    return uri.split("/")[2]