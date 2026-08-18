use axum::{routing::post, Router, Json};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
struct GenerateRequest {
    model: String,
    prompt: String,
    stream: Option<bool>,
}

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/api/generate", post(handle_generate));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000")
        .await
        .unwrap();

    println!("Scheduler running on port 3000");
    println!("Forwarding to Ollama at http://localhost:11434");

    axum::serve(listener, app).await.unwrap();
}

async fn handle_generate(
    Json(payload): Json<GenerateRequest>,
) -> Json<serde_json::Value> {
    println!("Received request — model: {}, prompt: {}", payload.model, payload.prompt);

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(300))
        .build()
        .unwrap();

    let ollama_response = client
        .post("http://localhost:11434/api/generate")
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
            println!("Ollama responded — tokens: {}", 
                body["eval_count"].as_u64().unwrap_or(0));
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