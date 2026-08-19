use axum::{Json, Router, extract::State, routing::post};
use dotenvy::dotenv;
use serde::{Deserialize, Serialize};
use std::env;

#[derive(Debug, Deserialize, Serialize)]
struct GenerateRequest {
    model: String,
    prompt: String,
    stream: Option<bool>,
}

#[derive(Clone, Debug)]
pub struct AppState {
    database_url: String,
    kafka_brokers: String,
    ollama_url: String,
    scheduler_policy: String,
    server_port: String,
}
#[tokio::main]
async fn main() {
    dotenvy::from_path("../.env").ok();
    let state = AppState {
        database_url: env::var("DATABASE_URL").expect("DATABASE_URL must be set"),
        kafka_brokers: env::var("KAFKA_BROKERS").expect("KAFKA_BROKERS must be set"),
        ollama_url: env::var("OLLAMA_URL").unwrap_or_else(|_| "http://localhost:11434".to_string()),
        scheduler_policy: env::var("SCHEDULER_POLICY").expect("SCHEDULER_POLICY must be set"),
        server_port: env::var("SERVER_PORT").unwrap_or_else(|_| "3000".to_string()),
    };
    let app = Router::new()
        .route("/api/generate", post(handle_generate))
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind(&format!("0.0.0.0:{}", state.server_port))
        .await
        .unwrap();

    println!("Scheduler running on port {}", state.server_port);
    println!("Forwarding to Ollama at {}", state.ollama_url);

    axum::serve(listener, app).await.unwrap();
}

async fn handle_generate(
    State(state): State<AppState>,
    Json(payload): Json<GenerateRequest>,
) -> Json<serde_json::Value> {
    println!(
        "Received request — model: {}, prompt: {}",
        payload.model, payload.prompt
    );

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(300))
        .build()
        .unwrap();

    let ollama_response = client
        .post(&format!("{}/api/generate", state.ollama_url))
        .json(&serde_json::json!({
            "model": payload.model,
            "prompt": payload.prompt,
            "stream": false
        }))
        .send()
        .await;

    match ollama_response {
        Ok(res) => {
            let body: serde_json::Value = res.json().await.unwrap();
            println!(
                "Ollama responded — tokens: {}",
                body["eval_count"].as_u64().unwrap_or(0)
            );
            Json(body)
        }
        Err(e) => {
            println!("Ollama error: {}", e);
            Json(serde_json::json!({
                       "error": e.to_string(),
            "is_timeout": e.is_timeout(),
            "is_connect": e.is_connect(),
                }))
        }
    }
}
