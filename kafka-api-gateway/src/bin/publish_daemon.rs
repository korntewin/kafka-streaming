use std::time::Duration;
use std::time;

use clap::Parser;
use dotenvy::dotenv;
use log::{error, info};
use rand::distributions::{Distribution, Uniform};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde_json::json;
use uuid::Uuid;
use reqwest::ClientBuilder;

#[derive(Parser, Debug)]
#[command(
    name = "publish-daemon",
    about = "Mock event publisher to Kafka with rate & concurrency control"
)]
struct Args {
    /// Kafka bootstrap broker list
    #[arg(long, env = "KAFKA_BROKERS", default_value = "localhost:9092")]
    brokers: String,

    /// Kafka topic to publish to
    #[arg(long, env = "KAFKA_TOPIC", default_value = "demo-topic")]
    topic: String,

    /// Total publish rate (messages / second) across all workers
    #[arg(short, long, default_value_t = 10.0)]
    rate: f64,

    /// Number of concurrent workers (each has fixed group_id)
    #[arg(short, long, default_value_t = 1)]
    concurrency: usize,

    /// Base seed for deterministic group_id & event generation
    #[arg(short, long, default_value_t = 42u64)]
    seed: u64,

    /// Optional number of messages per worker (if omitted, runs indefinitely)
    #[arg(short, long)]
    messages: Option<u64>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();
    env_logger::init();

    let args = Args::parse();

    if args.concurrency == 0 {
        anyhow::bail!("concurrency must be > 0");
    }
    if args.rate <= 0.0 {
        anyhow::bail!("rate must be > 0");
    }

    let per_worker_rate = args.rate;
    info!(
        "Starting publish daemon: brokers={}, topic={}, total_rate={} msg/s, per_worker_rate={:.3}, concurrency={}, seed={}, messages_per_worker={:?}",
        args.brokers,
        args.topic,
        args.rate,
        per_worker_rate,
        args.concurrency,
        args.seed,
        args.messages
    );

    let endpoint = std::env::var("API_GATEWAY_ENDPOINT").expect("API_GATEWAY_ENDPOINT must be set");
    let http_client = ClientBuilder::new()
        .timeout(Duration::from_secs(10))
        .build()?;

    let mut handles = Vec::with_capacity(args.concurrency);

    for i in 0..args.concurrency {
        let client_clone = http_client.clone();
        let endpoint = endpoint.clone() + "/publish";
        let seed = args.seed + i as u64; // deterministic seed per worker
        let messages_target = args.messages; // copy
        let per_rate = per_worker_rate;
        let worker_index = i;

        handles.push(tokio::spawn(async move {
            let mut rng = StdRng::seed_from_u64(seed);
            let group_uuid = deterministic_uuid(&mut rng);
            let group_id = group_uuid.to_string();

            let sleep_duration = if per_rate.is_finite() && per_rate > 0.0 {
                Duration::from_secs_f64(1.0 / per_rate)
            } else {
                Duration::from_millis(0)
            };

            info!(
                "worker={} started group_id={} per_rate={:.3} msg/s target_messages={:?}",
                worker_index, group_id, per_rate, messages_target
            );

            let mut sent: u64 = 0;
            loop {
                if let Some(limit) = messages_target {
                    if sent >= limit {
                        break;
                    }
                }

                let id = deterministic_uuid(&mut rng).to_string();
                let uniform = Uniform::new(0.0f32, 1.0f32);
                let score: f32 = uniform.sample(&mut rng);
                let event = json!({
                    "id": id,
                    "group_id": group_id,
                    "score": score,
                    "event_timestamp": time::SystemTime::now().duration_since(time::UNIX_EPOCH).unwrap().as_millis()
                });
                let api_payload = json!({
                    "key": group_id,
                    "value": event
                });
                let payload_str = api_payload.to_string();

                let _ = client_clone
                    .post(&endpoint)
                    .header("Content-Type", "application/json")
                    .body(payload_str)
                    .send()
                    .await
                    .map_err(|e| error!("worker={} HTTP send error: {e}", worker_index));

                sent += 1;
                if sent % 1000 == 0 {
                    info!(
                        "worker={} group_id={} sent={}",
                        worker_index, group_id, sent
                    );
                }

                if !sleep_duration.is_zero() {
                    tokio::time::sleep(sleep_duration).await;
                }
            }

            info!("worker={} finished total_sent={}", worker_index, sent);
        }));
    }

    // If running indefinitely, wait for Ctrl-C. If finite, wait for tasks to finish.
    if args.messages.is_none() {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => { info!("Ctrl-C received, shutting down (tasks will stop after current message)"); }
        }
    }

    // For finite mode or after Ctrl-C, wait for all tasks (they may still run some iterations until completion)
    for h in handles {
        let _ = h.await;
    }

    info!("publish daemon exiting");
    Ok(())
}

fn deterministic_uuid(rng: &mut StdRng) -> Uuid {
    let mut bytes = [0u8; 16];
    rng.fill(&mut bytes);
    // Set UUID version & variant bits to resemble a v4 UUID
    bytes[6] = (bytes[6] & 0x0F) | 0x40; // version 4
    bytes[8] = (bytes[8] & 0x3F) | 0x80; // variant RFC4122
    Uuid::from_bytes(bytes)
}
