import config
from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def upsert_gold(batch_df: DataFrame, batch_id: int):
    spark = batch_df.sparkSession

    # Build incremental aggregates per group
    agg = batch_df.groupBy("group_id").agg(
        F.sum("score").alias("batch_score"),
        F.count("*").alias("batch_events"),
        F.min("event_timestamp").alias("first_event_timestamp"),
        F.max("event_timestamp").alias("last_event_timestamp"),
    )

    gold_tbl = DeltaTable.forPath(spark, str(config.GOLD_PATH))
    (
        gold_tbl.alias("t")
        .merge(agg.alias("s"), "t.group_id = s.group_id")
        .whenMatchedUpdate(
            set={
                "cumulative_score": "t.cumulative_score + s.batch_score",
                "event_count": "t.event_count + s.batch_events",
                "avg_score": (
                    "(t.cumulative_score + s.batch_score) / (t.event_count + s.batch_events)"
                ),
                "first_event_timestamp": "s.first_event_timestamp",
                "last_event_timestamp": "s.last_event_timestamp",
                "updated_at": F.unix_timestamp(F.current_timestamp()) * 1000,
            }
        )
        .whenNotMatchedInsert(
            values={
                "group_id": "s.group_id",
                "cumulative_score": "s.batch_score",
                "event_count": "s.batch_events",
                "avg_score": "(s.batch_score / s.batch_events)",
                "first_event_timestamp": "s.first_event_timestamp",
                "last_event_timestamp": "s.last_event_timestamp",
                "updated_at": F.unix_timestamp(F.current_timestamp()) * 1000,
            }
        )
        .execute()
    )


def start_gold_stream(spark: SparkSession):
    silver_cdf = (
        spark.readStream.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", "0")
        .load(str(config.SILVER_PATH))
        # Ignore updates and deletes since they are duplicated events from silver stream
        # (the merge keys are group_id, id)
        .filter(~F.col("_change_type").isin("update_postimage", "update_preimage", "delete"))
        .drop("_commit_version", "_commit_timestamp", "_change_type")
    )

    return (
        silver_cdf.writeStream.format("delta")
        .foreachBatch(upsert_gold)
        .option("checkpointLocation", str(config.GOLD_CKPT))
        .option("maxOffsetsPerTrigger", str(config.MAX_FILES_PER_TRIGGER))
        .outputMode("update")
        .trigger(processingTime=config.TRIGGER_INTERVAL)
        .start(queryName="silver_to_gold")
    )
