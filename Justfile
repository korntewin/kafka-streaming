set dotenv-load := true

run-app:
    @docker-compose up -d --build --scale spark-worker=${SPARK_WORKER_INSTANCES}

teardown-app:
    @docker-compose down -v
