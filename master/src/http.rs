//! Master Node - HTTP API
//!
//! Provides REST API for clients to submit code and check job status.

use crate::state::{AppState, FinalResponse, JobContext, JobState, JobUpdate};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use common::scheduler::TestCaseResult;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tracing::info;
use uuid::Uuid;
use axum::response::sse::{Event, KeepAlive, Sse};
use tokio_stream::wrappers::BroadcastStream;

use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

async fn get_job_status(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
) -> impl IntoResponse {
    let mut initial_state: Option<JobUpdate> = None;
    
    // Check if job exists and get its current state
    if let Some(job) = state.jobs.get(&job_id) {
        tracing::info!(
            job_id = %job_id,
            state = ?std::mem::discriminant(&job.state),
            results_count = job.results.len(),
            result_statuses = ?job.results.iter().map(|r| r.status.as_str()).collect::<Vec<_>>(),
            "SSE client connected - checking job state"
        );
        
        initial_state = Some(match &job.state {
            JobState::Compiling => JobUpdate::Compiling,
            JobState::Executing { pending_batches, .. } => JobUpdate::Executing {
                completed: job.total_test_cases.saturating_sub(pending_batches * 1),
                total: job.total_test_cases
            },
            JobState::Completed => {
                // Job already finished - return the final results
                let all_passed = job.results.iter().all(|r| r.status == "PASSED");
                tracing::info!(
                    job_id = %job_id,
                    all_passed = all_passed,
                    "Building Completed response for SSE"
                );
                let final_response = FinalResponse::from_results(
                    job.id.clone(),
                    all_passed,
                    job.results.clone(),
                    job.compiler_output.clone(),
                    None,
                );
                JobUpdate::Completed(final_response)
            }
        });
    }
    
    // Subscribe to updates for this job (using job: prefix for pattern matching)
    let rx = state.pub_sub.subscribe(format!("job:{}", job_id));

    // Convert BroadcastStream items to SSE Events
    let stream = BroadcastStream::new(rx).map(move |item| {
        match item {
            Ok(update) => {
                match serde_json::to_string(&update) {
                    Ok(json) => Ok::<_, axum::Error>(Event::default().data(json)),
                    Err(_) => Ok::<_, axum::Error>(Event::default().event("error").data("serialization error")),
                }
            },
            Err(BroadcastStreamRecvError::Lagged(_)) => Ok::<_, axum::Error>(Event::default().event("error").data("stream lagged")),
        }
    });

    // If we have an initial state, prefix it to the stream
    let final_stream = if let Some(initial) = initial_state {
        let initial_event = match serde_json::to_string(&initial) {
            Ok(json) => Ok::<_, axum::Error>(Event::default().data(json)),
            Err(_) => Ok::<_, axum::Error>(Event::default().event("error").data("serialization error")),
        };
        tokio_stream::once(initial_event).chain(stream).boxed()
    } else {
        stream.boxed()
    };

    Sse::new(final_stream)
        .keep_alive(KeepAlive::default())
        .into_response()
}



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
        .route("/status/:job_id", get(get_job_status))
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

    // Check if we have any available workers
    if state.workers.is_empty() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(SubmitResponse {
                job_id: job_id.clone(),
                message: "No workers available".to_string(),
            }),
        );
    }

    // Determine if language is interpreted or compiled
    let is_interpreted = matches!(
        req.language.to_lowercase().as_str(),
        "python" | "python3" | "javascript" | "js" | "node" | "ruby"
    );

    // Convert test cases to protobuf format
    let proto_test_cases: Vec<common::scheduler::TestCase> = req
        .test_cases
        .iter()
        .map(|tc| common::scheduler::TestCase {
            id: tc.id.clone(),
            input: tc.input.clone(),
            expected_output: tc.expected_output.clone(),
        })
        .collect();

    // Create oneshot channel for response
    let (tx, _rx) = oneshot::channel::<FinalResponse>();

    // Pre-compute batches for interpreted languages to set correct pending_batches from start
    // This prevents race condition where worker completes before pending_batches is updated
    let batches: Option<Vec<Vec<common::scheduler::TestCase>>> = if is_interpreted {
        Some(crate::scheduler::create_batches(proto_test_cases.clone()))
    } else {
        None
    };

    // Determine initial state based on language type
    let initial_state = if is_interpreted {
        JobState::Executing {
            pending_batches: batches.as_ref().map(|b| b.len()).unwrap_or(1),
        }
    } else {
        JobState::Compiling
    };

    // Create job context
    let job = JobContext {
        id: job_id.clone(),
        language: req.language.clone(),
        source_code: req.source_code.clone(),
        total_test_cases: req.test_cases.len(),
        results: vec![],
        state: initial_state,
        binary: None,
        compiler_output: None,
        responder: Some(tx),
        test_cases: proto_test_cases.clone(),
        time_limit_ms: req.time_limit_ms,
        memory_limit_mb: req.memory_limit_mb,
        created_at: std::time::Instant::now(),
    };

    // Store job with correct pending_batches already set
    state.jobs.insert(job_id.clone(), job);

    // Find a suitable worker (least loaded)
    let worker_id = state
        .workers
        .iter()
        .min_by(|a, b| {
            a.value()
                .cpu_load_percent
                .partial_cmp(&b.value().cpu_load_percent)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|entry| entry.key().clone());

    let Some(worker_id) = worker_id else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(SubmitResponse {
                job_id,
                message: "No workers available".to_string(),
            }),
        );
    };

    // Dispatch to worker
    if let Some(batches) = batches {
        // Interpreted languages: dispatch pre-computed batches

        for (i, batch) in batches.into_iter().enumerate() {
            let task = common::scheduler::ExecuteBatchTask {
                job_id: job_id.clone(),
                batch_id: format!("batch_{}", i),
                language: req.language.clone(),
                payload: Some(common::scheduler::execute_batch_task::Payload::SourceCode(
                    req.source_code.clone(),
                )),
                inputs: batch,
                time_limit_ms: req.time_limit_ms,
                memory_limit_mb: req.memory_limit_mb,
            };

            let cmd = common::scheduler::MasterCommand {
                task: Some(common::scheduler::master_command::Task::Execute(task)),
            };

            if let Some(worker) = state.workers.get(&worker_id) {
                let _ = worker.sender.send(Ok(cmd)).await;
                info!(job_id = %job_id, worker_id = %worker_id, batch_idx = %i, "Dispatched execute batch task");
            }
        }
    } else {
        // For compiled languages, send CompileTask first
        let task = common::scheduler::CompileTask {
            job_id: job_id.clone(),
            language: req.language.clone(),
            source_code: req.source_code.clone(),
            flags: req.compiler_flags.clone(),
        };

        let cmd = common::scheduler::MasterCommand {
            task: Some(common::scheduler::master_command::Task::Compile(task)),
        };

        if let Some(worker) = state.workers.get(&worker_id) {
            let _ = worker.sender.send(Ok(cmd)).await;
            info!(job_id = %job_id, worker_id = %worker_id, "Dispatched compile task");
        }
    }

    (
        StatusCode::ACCEPTED,
        Json(SubmitResponse {
            job_id,
            message: "Job accepted and dispatched for execution".to_string(),
        }),
    )
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
