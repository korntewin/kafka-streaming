from typing import Any

import config
from delta import DeltaTable, configure_spark_with_delta_pip
from minio import Minio  # type: ignore
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


def build_spark(instances: int = config.SPARK_WORKER_INSTANCES) -> SparkSession:
    builder = (
        SparkSession.builder.appName("reviews-stream-processor")
        # Spark cluster config
        .master(config.MASTER_URL)
        .config("spark.ui.port", config.UI_PORT)
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", config.SPARK_EVENT_LOG_DIR)
        .config("spark.history.fs.logDirectory", config.SPARK_HISTORY_LOG_DIR)
        .config("spark.executor.instances", str(instances))
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
        # RocksDB State Store configuration
        .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
        .config("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")
        # MongoDB
        .config("spark.mongodb.connection.uri", config.MONGO_URI)
    )
    spark = configure_spark_with_delta_pip(
        builder, extra_packages=config.SPARK_PACKAGES
    ).getOrCreate()
    spark.sparkContext.setLogLevel(config.SPARK_LOG_LEVEL)
    return spark


# ruff: noqa: PLR0913
def ensure_table(
    spark: SparkSession,
    path: str,
    name: str,
    clustering_cols: list[str] | None = None,
    partition_cols: list[str] | None = None,
    zorder_cols: list[str] | None = None,
    enable_cdf: bool = True,
    schema: StructType | None = None,
):
    """Create Delta table at path if it does not exist with requested properties.

    Uses a simple empty DataFrame write to create the table with schema & properties
    instead of raw SQL + private APIs for broader compatibility.

    Args:
        spark: SparkSession instance
        path: Path to the Delta table
        name: Table name
        clustering_cols: Columns to cluster by (for liquid clustering)
        partition_cols: Columns to partition by (traditional partitioning)
        zorder_cols: Columns to Z-ORDER by (for optimization)
        enable_cdf: Enable Change Data Feed
        schema: Table schema
    """
    if schema is None:
        raise ValueError("Schema required to create table")

    # Build the table creation command
    table_builder = (
        DeltaTable.createIfNotExists(spark).tableName(name).addColumns(schema).location(str(path))
    )

    # Add clustering columns if specified (liquid clustering)
    if clustering_cols:
        table_builder = table_builder.clusterBy(*clustering_cols)

    # Add partition columns if specified (traditional partitioning)
    if partition_cols:
        table_builder = table_builder.partitionedBy(*partition_cols)

    # Set table properties
    table_builder = (
        table_builder.property("delta.enableChangeDataFeed", "true" if enable_cdf else "false")
        .property("delta.autoOptimize.optimizeWrite", "true")
        .property("delta.autoOptimize.autoCompact", "true")
        .property("delta.deletedFileRetentionDuration", "1 hours")
    )

    # Execute table creation
    table_builder.execute()

    # Add Z-ORDER optimization if specified (must be done after table creation)
    if zorder_cols:
        spark.sql(f"OPTIMIZE {name} ZORDER BY ({', '.join(zorder_cols)})")  # type: ignore


def optimize_table(spark: SparkSession, name: str, zorder_cols: list[str]) -> None:
    """Optimize Delta table with Z-ORDER by specified columns.

    Args:
        spark: SparkSession instance
        name: Table name
        zorder_cols: Columns to Z-ORDER by
    """
    if not zorder_cols:
        spark.sql(f"OPTIMIZE {name}")  # type: ignore
        return

    # Perform the optimization with Z-ORDER
    spark.sql(f"OPTIMIZE {name} ZORDER BY ({', '.join(zorder_cols)})")  # type: ignore


def vacuum_table(spark: SparkSession, name: str, path: str, retention_hours: int = 1) -> None:
    """Vacuum Delta table to clean up old files.

    Args:
        spark: SparkSession instance
        path: Path to the Delta table
        retention_hours: Retention period in hours
    """
    DeltaTable.forPath(spark, path).vacuum(retention_hours)


def ensure_bucket_exists(client: Minio, bucket_name: str):
    if not client.bucket_exists(bucket_name):
        print(f"Creating bucket {bucket_name}")
        client.make_bucket(bucket_name)


def get_bucket_name(uri: str) -> str:
    return uri.split("/")[2]


def ensure_mongo_collection(client: MongoClient[Any], db_name: str, collection_name: str):
    # Select (and implicitly create) the database
    db = client[db_name]

    # Create the collection if it doesn't exist (this also creates the DB if new)
    if collection_name not in db.list_collection_names():
        db.create_collection(collection_name, capped=False)


def build_spark_debug() -> SparkSession:
    builder = (
        SparkSession.builder.appName("reviews-stream-processor")
        # Spark cluster config
        .config("spark.eventLog.enabled", "false")
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
        # RocksDB State Store configuration
        .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
        .config("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")
        # MongoDB
        .config("spark.mongodb.connection.uri", config.MONGO_URI)
    )
    spark = configure_spark_with_delta_pip(
        builder, extra_packages=config.SPARK_PACKAGES
    ).getOrCreate()
    spark.sparkContext.setLogLevel(config.SPARK_LOG_LEVEL)
    return spark
