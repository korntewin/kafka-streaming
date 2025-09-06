from pyspark.sql import SparkSession, DataFrame, functions as F

import config


def write_to_mongo(df: DataFrame, epoch_id: int):
    del epoch_id

    (
        df.withColumn("_id", F.col("group_id"))
        .write.format("mongodb")
        .option("database", config.MONGO_DB_NAME)
        .option("collection", config.MONGO_COLLECTION)
        .mode("append")
        .save()
    )


def start_to_mongo_stream(spark: SparkSession) -> None:
    gold = (
        spark.readStream.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", "0")
        .load(str(config.GOLD_PATH))
        .filter("_change_type != 'update_preimage'")
        .drop("_commit_version", "_commit_timestamp", "_change_type")
    )

    (
        gold.writeStream
        .foreachBatch(write_to_mongo)
        .option("checkpointLocation", str(config.MONGO_CKPT))
        .trigger(processingTime=config.TRIGGER_INTERVAL)
        .start()
    )