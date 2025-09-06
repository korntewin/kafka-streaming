set dotenv-load := true

run-app: run-spark-cluster run-publish-daemon run-kafka-ui run-webapp

run-spark-cluster:
    @docker-compose up -d --build --scale spark-worker=3 spark-stream

run-publish-daemon:
    @docker-compose up -d --build publish-daemon

run-kafka-ui:
    @docker-compose up -d --build kafka-ui

run-webapp:
    @docker-compose up -d --build webapp

teardown-app:
    @docker-compose down -v
