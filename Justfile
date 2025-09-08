set dotenv-load := true

run-app: run-spark-cluster run-publish-daemon run-kafka-ui run-webapp run-spark-history-server

run-spark-cluster:
    @docker-compose up -d --build --scale spark-worker=${SPARK_WORKER_INSTANCES} spark-stream

run-spark-history-server:
    @docker-compose up -d --build spark-history-server

run-publish-daemon:
    @docker-compose up -d --build publish-daemon

run-kafka-ui:
    @docker-compose up -d --build kafka-ui

run-webapp:
    @docker-compose up -d --build webapp

teardown-app:
    @docker-compose down -v
