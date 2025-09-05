from pyspark.sql import SparkSession, functions as F, DataFrame
from delta import *  # type: ignore

import config


def upsert_gold(batch_df: DataFrame, batch_id: int):
    spark = batch_df.sparkSession

    # Build incremental aggregates per group
    agg = (
        batch_df.groupBy("group_id")
        .agg(
            F.sum("score").alias("batch_score"),
            F.count("id").alias("batch_events"),
        )
        .withColumn("batch_id", F.lit(batch_id))
    )

    gold_tbl = DeltaTable.forPath(spark, str(config.GOLD_PATH))
    existing = gold_tbl.toDF().select("group_id", "cumulative_score", "event_count")
    merged = (
        agg.join(existing, "group_id", "left")
        .withColumn(
            "cumulative_score",
            F.coalesce(F.col("cumulative_score"), F.lit(0.0)) + F.col("batch_score"),
        )
        .withColumn(
            "event_count",
            F.coalesce(F.col("event_count"), F.lit(0)) + F.col("batch_events"),
        )
        .withColumn(
            "avg_score",
            (F.col("cumulative_score") / F.col("event_count")).cast("float"),
        )
        .withColumn("updated_at", F.unix_timestamp(F.current_timestamp()))
        .select("group_id", "cumulative_score", "event_count", "avg_score", "updated_at")
    )

    temp_view = "gold_upserts"
    merged.createOrReplaceTempView(temp_view)

    gold_tbl.alias("t").merge(
        merged.alias("s"), "t.group_id = s.group_id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


def start_gold_stream(spark: SparkSession):
    silver_cdf = (
        spark.readStream.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", "0")
        .load(str(config.SILVER_PATH))
        .filter("_change_type != 'update_preimage'")
        .drop("_commit_version", "_commit_timestamp", "_change_type")
    )

    return (
        silver_cdf.writeStream.format("delta")
        .foreachBatch(upsert_gold)
        .option("checkpointLocation", str(config.GOLD_CKPT))
        .outputMode("update")
        .trigger(processingTime=config.TRIGGER_INTERVAL)
        .start()
    )
