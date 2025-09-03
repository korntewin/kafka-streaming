use log::{debug, error, info};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::error::KafkaError;
use rdkafka::error::RDKafkaErrorCode;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;

pub async fn publish_message(
    producer: &FutureProducer,
    topic: &str,
    key: &str,
    payload: &str,
) -> Result<(), rdkafka::error::KafkaError> {
    let record = FutureRecord::to(topic).key(key).payload(payload);

    let produce_future = producer.send(record, Duration::from_secs(0));

    match produce_future.await {
        Ok(delivery) => {
            debug!("Message delivered {:?}", delivery);
            Ok(())
        }
        Err(e) => {
            error!("Failed to enqueue message: {:?}", e);
            Err(e.0)
        }
    }
}

pub async fn ensure_topic(
    admin: &AdminClient<rdkafka::client::DefaultClientContext>,
    topic: &str,
    partitions: i32,
    replication: i32,
) -> Result<(), rdkafka::error::KafkaError> {
    let new_topic = NewTopic::new(topic, partitions, TopicReplication::Fixed(replication))
        .set("cleanup.policy", "delete");
    let res = admin
        .create_topics([&new_topic], &AdminOptions::new())
        .await?;

    for r in res {
        match r {
            Ok(t) => info!("Created topic: {t}"),
            Err((t, e)) => {
                if e == RDKafkaErrorCode::TopicAlreadyExists {
                    info!("Topic already exists: {t}");
                } else {
                    return Err(KafkaError::AdminOp(e));
                }
            }
        }
    }

    Ok(())
}
