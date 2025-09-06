use dotenvy::dotenv;
use kafka_api_gateway::AppState;
use kafka_api_gateway::publish_event::publish;
use kafka_client::{KafkaPublisher, utils};
use log::{error, info};
use ntex::web;
use rdkafka::admin::AdminClient;
use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureProducer;
use std::sync::Arc;

fn load_env(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

#[ntex::main]
async fn main() -> std::io::Result<()> {
    dotenv().ok();
    env_logger::init();

    let brokers = load_env("KAFKA_BROKERS", "localhost:9092");
    let topic = load_env("KAFKA_TOPIC", "demo-topic");
    let partitions: i32 = load_env("KAFKA_TOPIC_PARTITIONS", "3").parse().unwrap_or(3);
    let replication: i32 = load_env("KAFKA_TOPIC_REPLICATION", "2")
        .parse()
        .unwrap_or(2);
    let bind_addr = load_env("BIND_ADDRESS", "0.0.0.0:8080");

    info!("Starting Kafka API Gateway on {bind_addr}, broker={brokers}, topic={topic}");

    let admin: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()
        .expect("admin client");

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("producer");

    if let Err(e) = utils::ensure_topic(&admin, &topic, partitions, replication).await {
        error!("Topic ensure failed: {e}");
    }

    let kafka_client = KafkaPublisher::new(producer.clone());

    let state = Arc::new(AppState {
        publisher: kafka_client,
        topic,
    });

    web::server(move || {
        let s = state.clone();
        web::App::new()
            .state(s)
            .route("/health", web::get().to(async || "OK"))
            .route("/publish", web::post().to(publish))
    })
    .bind(&bind_addr)?
    .run()
    .await
}
