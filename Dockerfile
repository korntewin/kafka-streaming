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