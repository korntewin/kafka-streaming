import os

from pyspark.sql import SparkSession, functions as F, DataFrame
from pyspark.sql.window import Window
from delta import *  # type: ignore

import config


def merge_to_silver(batch_df: DataFrame, batch_id: int):
    spark = batch_df.sparkSession

    # Keep only the latest record per (group_id, id) based on event_timestamp
    window = Window.partitionBy("group_id", "id").orderBy(F.col("event_timestamp").desc())
    latest_per_key = (
        batch_df.withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    delta_tbl = DeltaTable.forPath(spark, str(config.SILVER_PATH))
    (
        delta_tbl.alias("t")
        .merge(
            latest_per_key.alias("s"),
            "t.group_id = s.group_id AND t.id = s.id",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def start_silver_stream(spark: SparkSession):
    # Read CDF from bronze
    kafka_source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
        .option("subscribe", config.KAFKA_TOPIC)
        .option("startingOffsets", os.getenv("KAFKA_STARTING_OFFSETS", "latest"))
        .load()
    )

    parsed = (
        kafka_source.select(
            F.col("key").cast("string").alias("k"), F.col("value").cast("string").alias("value")
        )
        .withColumn("json", F.from_json(F.col("value"), config.EVENT_SCHEMA))
        .select("json.*")
        .withColumn("ingest_timestamp", F.unix_timestamp(F.current_timestamp()))
    )

    return (
        parsed.writeStream.format("delta")
        .foreachBatch(merge_to_silver)
        .option("checkpointLocation", str(config.SILVER_CKPT))
        .outputMode("update")
        .trigger(processingTime=config.TRIGGER_INTERVAL)
        .start()
    )
