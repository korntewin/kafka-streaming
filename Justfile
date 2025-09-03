run-app:
    @docker-compose up -d --build

teardown-app:
    @docker-compose down -v
