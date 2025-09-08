import config
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def write_to_mongo(df: DataFrame, epoch_id: int):
    del epoch_id

    (
        df.withColumn("_id", F.col("group_id"))
        .withColumn("updated_at", F.unix_timestamp(F.current_timestamp()) * 1000)
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
        gold.writeStream.foreachBatch(write_to_mongo)
        .option("checkpointLocation", str(config.MONGO_CKPT))
        .trigger(processingTime=config.TRIGGER_INTERVAL)
        .start(queryName="gold_to_mongo")
    )
