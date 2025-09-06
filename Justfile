set dotenv-load := true

run-app:
    @docker-compose up -d --build --scale spark-worker=3

teardown-app:
    @docker-compose down -v
