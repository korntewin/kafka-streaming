# === Rust cargo-chef builder ===
FROM rust:1.89-bookworm AS chef
RUN apt-get update && apt-get install -y --no-install-recommends cmake
RUN cargo install cargo-chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS chef-base
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# === Kafka API Gateway ===
FROM chef-base AS kafka-api-gateway-builder
COPY . .
RUN cargo build --release -p kafka-api-gateway

FROM ubuntu:25.10 AS kafka-api-gateway
RUN useradd -u 10001 -r -m -d /home/app -s /usr/sbin/nologin appuser
COPY --chown=appuser --from=kafka-api-gateway-builder /app/target/release/kafka-api-gateway /usr/local/bin
USER appuser
ENTRYPOINT ["kafka-api-gateway"]

# === Publish Daemon ===
FROM chef-base AS daemon-builder
COPY . .
RUN cargo build --release -p kafka-api-gateway --bin publish_daemon

FROM ubuntu:25.10 AS publish-daemon
RUN useradd -u 10001 -r -m -d /home/app -s /usr/sbin/nologin appuser
COPY --chown=appuser --from=daemon-builder /app/target/release/publish_daemon /usr/local/bin
USER appuser
ENTRYPOINT ["publish_daemon"]

# === Python ===
FROM python:3.13-bookworm AS python-app
COPY --from=ghcr.io/astral-sh/uv:0.8.15 /uv /uvx /bin/
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY stream-processor/pyproject.toml stream-processor/pyproject.toml
RUN uv sync --package stream-processor

ENV JAVA_HOME=/usr
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# RUN useradd -u 10001 -r -m -d /home/app -s /usr/sbin/nologin appuser
# USER appuser
COPY . .
# RUN uv run --package stream-processor pyspark --packages org.apache.spark:spark-hadoop-cloud_2.13:4.0.0
ENTRYPOINT ["uv", "run", "--package", "stream-processor", "python3", "stream-processor/main.py"]