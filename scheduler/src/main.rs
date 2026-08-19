use axum::{Json, Router, extract::State, routing::post};
use dotenvy;
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Deserialize, Serialize};
use std::env;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Deserialize, Serialize)]
struct GenerateRequest {
    model: String,
    prompt: String,
    stream: Option<bool>,
}

#[derive(Clone)]
pub struct AppState {
    pub database_url: String,
    pub kafka_brokers: String,
    pub ollama_url: String,
    pub scheduler_policy: String,
    pub server_port: String,
    pub producer: Arc<FutureProducer>,
}
#[tokio::main]
async fn main() {
    dotenvy::from_path("../.env").ok();
    let kafka_brokers = env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string());

    // Build Kafka producer
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &kafka_brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Failed to create Kafka producer");

    let state = AppState {
        database_url: env::var("DATABASE_URL").expect("DATABASE_URL must be set"),
        kafka_brokers,
        ollama_url: env::var("OLLAMA_URL").unwrap_or_else(|_| "http://localhost:11434".to_string()),
        scheduler_policy: env::var("SCHEDULER_POLICY").expect("SCHEDULER_POLICY must be set"),
        server_port: env::var("SERVER_PORT").unwrap_or_else(|_| "3000".to_string()),
        producer: Arc::new(producer),
    };
    let app = Router::new()
        .route("/api/generate", post(handle_generate))
        .with_state(state.clone());

    let addr = format!("0.0.0.0:{}", state.server_port);
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();

    println!("Scheduler running on {}", addr);
    println!("Kafka brokers: {}", state.kafka_brokers);
    println!("Policy: {}", state.scheduler_policy);

    axum::serve(listener, app).await.unwrap();
}

async fn handle_generate(
    State(state): State<AppState>,
    Json(payload): Json<GenerateRequest>,
) -> Json<serde_json::Value> {
    let request_id = Uuid::new_v4().to_string();
    println!("Queuing request {} — model: {}", request_id, payload.model);

    let _client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(300))
        .build()
        .unwrap();

    // Publish to Kafka
    let message = serde_json::json!({
        "request_id": request_id,
        "model": payload.model,
        "prompt": payload.prompt,
        "stream": payload.stream.unwrap_or(false),
        "arived_at": chrono::Utc::now().to_rfc3339(),
    });

    let payload_str = message.to_string();

    let result = state
        .producer
        .send(
            FutureRecord::to("llm_requests")
                .payload(&payload_str)
                .key(&request_id),
            std::time::Duration::from_secs(5),
        )
        .await;

    // let ollama_response = client
    //     .post(&format!("{}/api/generate", state.ollama_url))
    //     .json(&serde_json::json!({
    //         "model": payload.model,
    //         "prompt": payload.prompt,
    //         "stream": false
    //     }))
    //     .send()
    //     .await;

    // match ollama_response {
    //     Ok(res) => {
    //         let body: serde_json::Value = res.json().await.unwrap();
    //         println!(
    //             "Ollama responded — tokens: {}",
    //             body["eval_count"].as_u64().unwrap_or(0)
    //         );
    //         Json(body)
    //     }
    //     Err(e) => {
    //         println!("Ollama error: {}", e);
    //         Json(serde_json::json!({
    //                    "error": e.to_string(),
    //         "is_timeout": e.is_timeout(),
    //         "is_connect": e.is_connect(),
    //             }))
    //     }

    match result {
        Ok(_) => {
            println!("Published request {} to Kafka", request_id);
            Json(serde_json::json!({
                "request_id": request_id,
                "status": "queued",
            }))
        }
        Err(e) => {
            println!("Kafka error: {:?}", e);
            Json(serde_json::json!({
                "error": "Failed to queue request",
            }))
        }
    }
}
