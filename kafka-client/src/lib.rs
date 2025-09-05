pub mod utils;

use anyhow::Result;
use rdkafka::producer::FutureProducer;
use std::future::Future;

pub trait Publisher {
    fn publish(
        &self,
        key: &str,
        payload: &str,
        topic: &str,
    ) -> impl Future<Output = Result<()>> + Send;
}

#[derive(Clone)]
pub struct KafkaPublisher {
    pub producer: FutureProducer,
}

impl KafkaPublisher {
    pub fn new(producer: FutureProducer) -> Self {
        KafkaPublisher { producer }
    }
}

impl Publisher for KafkaPublisher {
    fn publish(
        &self,
        key: &str,
        payload: &str,
        topic: &str,
    ) -> impl Future<Output = Result<()>> + Send {
        let producer = self.producer.clone();
        let key = key.to_string();
        let payload = payload.to_string();
        async move {
            utils::publish_message(&producer, topic, &key, &payload).await?;
            Ok(())
        }
    }
}
