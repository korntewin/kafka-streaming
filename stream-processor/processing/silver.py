import os

import config
from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
# from pyspark.sql.window import Window


def merge_to_silver(batch_df: DataFrame, batch_id: int):
    spark = batch_df.sparkSession

    # Keep only the latest record per (group_id, id) based on event_timestamp
    # window = Window.partitionBy("group_id", "id").orderBy(F.col("event_timestamp").desc())
    # latest_per_key = (
    #     batch_df.withColumn("rn", F.row_number().over(window)).filter(F.col("rn") == 1).drop("rn")
    # )
    latest_per_key = batch_df

    # Prune partition to only the partitions we are inserting
    # this help Liquid Clustering to skip unnecessary files
    mins = [r[0] for r in latest_per_key.select("minute_timestamp").distinct().collect()]
    part_list = ",".join(map(str, mins))
    cond = (
        f"t.minute_timestamp IN ({part_list}) "
        f"AND t.minute_timestamp = s.minute_timestamp "
        f"AND t.id = s.id"
    )

    delta_tbl = DeltaTable.forPath(spark, str(config.SILVER_PATH))

    (
        delta_tbl.alias("t")
        .merge(
            latest_per_key.alias("s"),
            cond,
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def start_silver_stream(spark: SparkSession):
    # Read CDF from bronze
    kafka_source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
        .option("subscribe", config.KAFKA_TOPIC)
        .option("startingOffsets", os.getenv("KAFKA_STARTING_OFFSETS", "earliest"))
        .load()
    )

    parsed = (
        kafka_source.select(
            F.col("key").cast("string").alias("k"), F.col("value").cast("string").alias("value")
        )
        .withColumn("json", F.from_json(F.col("value"), config.EVENT_SCHEMA))
        .select("json.*")
        .withColumn("ingest_timestamp", F.unix_timestamp(F.current_timestamp()))
        # Add partition columns
        .withColumn("minute_timestamp", (F.col("event_timestamp") / 1000 / 180).cast("long"))
        .withColumn("timestamp", (F.col("event_timestamp") / 1000).cast("timestamp"))
        .withWatermark("timestamp", "30 seconds")
        # Deduplication uses RocksDB state store with S3 checkpoint location
        # State store configuration is set in the Spark session (utils.py)
        .dropDuplicatesWithinWatermark(["id"])
    )

    return (
        parsed.writeStream.format("delta")
        .foreachBatch(merge_to_silver)
        .option("checkpointLocation", str(config.SILVER_CKPT))
        .option("maxOffsetsPerTrigger", str(config.MAX_OFFSETS_PER_TRIGGER))
        .outputMode("update")
        .trigger(processingTime=config.TRIGGER_INTERVAL)
        .start(queryName="kafka_to_silver")
    )
