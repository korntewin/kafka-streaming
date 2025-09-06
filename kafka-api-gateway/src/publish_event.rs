use crate::{AppState, PublishRequest, PublishResponse};
use kafka_client::Publisher;
use log::{error, info};
use ntex::http::StatusCode;
use ntex::web;
use std::sync::Arc;

pub async fn publish(
    data: web::types::State<Arc<AppState>>,
    body: web::types::Json<PublishRequest>,
) -> Result<web::HttpResponse, web::Error> {
    let key_opt = body.key.as_ref().map(|s| s.as_str());

    let value_str = match serde_json::to_string(&body.value) {
        Ok(s) => s,
        Err(e) => {
            return Ok(web::HttpResponse::BadRequest().body(format!("invalid JSON value: {e}")));
        }
    };

    info!(
        "Publishing message to topic={:?} with key={:?}, value={}",
        data.topic, key_opt, value_str
    );
    match data
        .publisher
        .publish(key_opt.unwrap_or(""), &value_str, &data.topic)
        .await
    {
        Ok(()) => {
            let resp = PublishResponse {
                status: "ok",
                message: "Message published successfully",
            };
            Ok(web::HttpResponse::Ok().json(&resp))
        }
        Err(e) => {
            error!("Failed to deliver message: {e}");
            Ok(web::HttpResponse::build(StatusCode::INTERNAL_SERVER_ERROR)
                .body(format!("publish error: {e}")))
        }
    }
}
