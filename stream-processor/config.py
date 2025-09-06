import os

from dotenv import load_dotenv

from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType

load_dotenv("../.env")


KAFKA_BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "broker-1:19092,broker-2:19092,broker-3:19092"
)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "reviews")

SILVER_PATH = "s3a://kafka/silver/" + KAFKA_TOPIC
GOLD_PATH = "s3a://kafka/gold/" + KAFKA_TOPIC
SILVER_CKPT = "s3a://kafka/_checkpoint_silver/" + KAFKA_TOPIC
GOLD_CKPT = "s3a://kafka/_checkpoint_gold/" + KAFKA_TOPIC
MONGO_CKPT = "s3a://kafka/_checkpoint_mongo/" + KAFKA_TOPIC

# Micro-batch trigger
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "2 seconds")

# Schema of Kafka message value (JSON)
EVENT_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("group_id", StringType(), False),
        StructField("score", FloatType(), False),
        StructField("event_timestamp", LongType(), False),
    ]
)

RAW_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("group_id", StringType(), False),
        StructField("score", FloatType(), False),
        StructField("event_timestamp", LongType(), False),
        StructField("ingest_timestamp", LongType(), False),
    ]
)

AGGREGATION_SCHEMA = StructType(
    [
        StructField("group_id", StringType(), False),
        StructField("cumulative_score", FloatType(), False),
        StructField("event_count", LongType(), False),
        StructField("avg_score", FloatType(), False),
        StructField("updated_at", LongType(), False),
    ]
)

# AWS S3 (MinIO) config
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123")
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://localhost:9999")
SPARK_PACKAGES = [
    "org.apache.spark:spark-hadoop-cloud_2.13:4.0.0",
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0",
    "org.mongodb.spark:mongo-spark-connector_2.13:10.5.0",
]

# Spark cluster config
SPARK_SHUFFLE_PARTITIONS = int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
SPARK_LOG_LEVEL = os.getenv("SPARK_LOG_LEVEL", "INFO")
MASTER_URL = os.getenv("MASTER_URL", "spark://spark-driver:7077")
UI_PORT = int(os.getenv("SPARK_UI_PORT", "4040"))
SPARK_WORKER_INSTANCES = int(os.getenv("SPARK_WORKER_INSTANCES", "2"))
SPARK_EVENT_LOG_DIR = os.getenv("SPARK_EVENT_LOG_DIR", "s3a://spark-events")
SPARK_HISTORY_LOG_DIR = SPARK_EVENT_LOG_DIR

# MongoDB config
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "reviews_db")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://root:example@mongodb:27017/?authSource=admin")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "reviews")
