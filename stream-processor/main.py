from delta import *  # type: ignore

import config
from bootstrap import bootstrap
from processing import silver, gold


def main():  # noqa: D401
    spark = bootstrap()
    print(
        f"Starting streaming pipeline topic={config.KAFKA_TOPIC} bootstrap={config.KAFKA_BOOTSTRAP}"
    )

    silver_q = silver.start_silver_stream(spark)
    gold_q = gold.start_gold_stream(spark)

    # Keep references to avoid GC (pyspark best practice)
    _ = (silver_q, gold_q)

    print("Streams started. Awaiting termination (Ctrl+C to stop)...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
