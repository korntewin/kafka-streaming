pub mod publish_event;

use kafka_client::KafkaPublisher;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone)]
pub struct AppState {
    pub publisher: KafkaPublisher,
    pub topic: String,
}

#[derive(Debug, Deserialize)]
pub struct PublishRequest {
    pub key: Option<String>,
    pub value: Value,
}

#[derive(Debug, Serialize)]
pub struct PublishResponse<'a> {
    pub status: &'a str,
    pub message: &'a str,
}
