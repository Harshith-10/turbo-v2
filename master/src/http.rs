//! Master Node - HTTP API
//!
//! Provides REST API for clients to submit code and check job status.

use crate::state::{AppState, FinalResponse, JobContext, JobState};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use common::scheduler::{TestCaseResult, TestCase as ProtoTestCase};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tracing::info;
use uuid::Uuid;

/// Request body for code submission
#[derive(Debug, Deserialize)]
pub struct SubmitRequest {
    pub language: String,
    pub source_code: String,
    pub test_cases: Vec<TestCaseInput>,
    #[serde(default)]
    pub compiler_flags: Vec<String>,
    #[serde(default = "default_time_limit")]
    pub time_limit_ms: u32,
    #[serde(default = "default_memory_limit")]
    pub memory_limit_mb: u32,
}

fn default_time_limit() -> u32 {
    2000 // 2 seconds
}

fn default_memory_limit() -> u32 {
    128 // 128 MB
}

#[derive(Debug, Deserialize)]
pub struct TestCaseInput {
    pub id: String,
    pub input: String,
    pub expected_output: String,
}

/// Response for job submission
#[derive(Debug, Serialize)]
pub struct SubmitResponse {
    pub job_id: String,
    pub message: String,
}

/// Response for job status
#[derive(Debug, Serialize)]
pub struct StatusResponse {
    pub job_id: String,
    pub state: String,
    pub results: Vec<TestResultOutput>,
    pub compiler_output: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct TestResultOutput {
    pub test_id: String,
    pub status: String,
    pub time_ms: i32,
    pub memory_bytes: i32,
    pub stdout: String,
    pub stderr: String,
}

impl From<TestCaseResult> for TestResultOutput {
    fn from(r: TestCaseResult) -> Self {
        Self {
            test_id: r.test_id,
            status: r.status,
            time_ms: r.time_ms,
            memory_bytes: r.memory_bytes,
            stdout: r.stdout,
            stderr: r.stderr,
        }
    }
}

/// Create the HTTP router
pub fn create_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health_check))
        .route("/submit", post(submit_job))
        .route("/status/{job_id}", get(get_job_status))
        .route("/workers", get(list_workers))
        .with_state(state)
}

async fn health_check() -> impl IntoResponse {
    Json(serde_json::json!({ "status": "ok" }))
}

async fn submit_job(
    State(state): State<AppState>,
    Json(req): Json<SubmitRequest>,
) -> impl IntoResponse {
    let job_id = Uuid::new_v4().to_string();

    info!(
        job_id = %job_id,
        language = %req.language,
        test_cases = req.test_cases.len(),
        "Job submitted"
    );

    // Create oneshot channel for response
    let (tx, _rx) = oneshot::channel::<FinalResponse>();

    // Create job context
    let job = JobContext {
        id: job_id.clone(),
        language: req.language.clone(),
        source_code: req.source_code.clone(),
        total_test_cases: req.test_cases.len(),
        results: vec![],
        state: JobState::Compiling,
        binary: None,
        compiler_output: None,
        responder: Some(tx),
    };

    // Store job
    state.jobs.insert(job_id.clone(), job);

    // Check if we have any available workers
    if state.workers.is_empty() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(SubmitResponse {
                job_id: job_id.clone(),
                message: "No workers available. Job queued.".to_string(),
            }),
        );
    }

    // TODO: Dispatch to worker based on language
    // For now, just acknowledge receipt

    (
        StatusCode::ACCEPTED,
        Json(SubmitResponse {
            job_id,
            message: "Job accepted and queued for execution".to_string(),
        }),
    )
}

async fn get_job_status(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
) -> impl IntoResponse {
    if let Some(job) = state.jobs.get(&job_id) {
        let state_str = match &job.state {
            JobState::Compiling => "compiling",
            JobState::Executing { pending_batches } => {
                if *pending_batches > 0 {
                    "executing"
                } else {
                    "completed"
                }
            }
            JobState::Completed => "completed",
        };

        (
            StatusCode::OK,
            Json(StatusResponse {
                job_id,
                state: state_str.to_string(),
                results: job.results.iter().cloned().map(Into::into).collect(),
                compiler_output: job.compiler_output.clone(),
                error: None,
            }),
        )
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(StatusResponse {
                job_id,
                state: "not_found".to_string(),
                results: vec![],
                compiler_output: None,
                error: Some("Job not found".to_string()),
            }),
        )
    }
}

async fn list_workers(State(state): State<AppState>) -> impl IntoResponse {
    let workers: Vec<_> = state
        .workers
        .iter()
        .map(|entry| {
            serde_json::json!({
                "id": entry.key().clone(),
                "cpu_cores": entry.value().cpu_cores,
                "total_ram_mb": entry.value().total_ram_mb,
                "cpu_load_percent": entry.value().cpu_load_percent,
                "ram_usage_mb": entry.value().ram_usage_mb,
                "active_tasks": entry.value().active_tasks,
                "tags": entry.value().tags,
            })
        })
        .collect();

    Json(serde_json::json!({ "workers": workers }))
}
