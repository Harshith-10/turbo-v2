//! Master Node - State Management
//! 
//! Provides thread-safe state containers for workers and jobs using DashMap.

use crate::rusq::BroadcastEngine;
use common::scheduler::{MasterCommand, TestCaseResult};
use dashmap::DashMap;
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug, Clone, Serialize)]
pub struct SerializableTestCaseResult {
    pub test_id: String,
    pub status: String,
    pub time_ms: i32,
    pub memory_bytes: i32,
    pub stdout: String,
    pub stderr: String,
}

impl From<TestCaseResult> for SerializableTestCaseResult {
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


/// Updates sent to SSE clients
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", content = "data")]
pub enum JobUpdate {
    Compiling,
    Compiled { success: bool, output: Option<String> },
    Executing { completed: usize, total: usize },
    Result(SerializableTestCaseResult),
    Completed(FinalResponse),
    Error(String),
}

/// Final response sent back to HTTP client
#[derive(Debug, Clone, Serialize)]
pub struct FinalResponse {
    pub job_id: String,
    pub success: bool,
    pub results: Vec<SerializableTestCaseResult>,
    pub compiler_output: Option<String>,
    pub error: Option<String>,
}

impl FinalResponse {
    pub fn from_results(
        job_id: String,
        success: bool,
        results: Vec<TestCaseResult>,
        compiler_output: Option<String>,
        error: Option<String>,
    ) -> Self {
        Self {
            job_id,
            success,
            results: results.into_iter().map(Into::into).collect(),
            compiler_output,
            error,
        }
    }
}


/// Current state of a job in the pipeline
#[derive(Debug, Clone)]
pub enum JobState {
    /// Phase 1: Waiting for compilation to complete
    Compiling,
    /// Phase 2: Executing test batches
    Executing { pending_batches: usize },
    /// Job completed (success or failure)
    Completed,
}

/// Context for an active job
pub struct JobContext {
    pub id: String,
    pub language: String,
    pub source_code: String,
    pub total_test_cases: usize,
    /// Store results as they come in from various batches
    pub results: Vec<TestCaseResult>,
    pub state: JobState,
    /// Compiled binary (populated after Phase 1)
    pub binary: Option<Vec<u8>>,
    /// Compiler output for display
    pub compiler_output: Option<String>,
    /// Channel to reply to the HTTP thread once done
    pub responder: Option<oneshot::Sender<FinalResponse>>,
    /// Test cases for this job
    pub test_cases: Vec<common::scheduler::TestCase>,
    /// Time limit per test case in milliseconds
    pub time_limit_ms: u32,
    /// Memory limit per test case in MB
    pub memory_limit_mb: u32,
    /// Timestamp when job was created (for GC)
    pub created_at: std::time::Instant,
}

/// Worker connection info
pub struct WorkerInfo {
    /// gRPC stream sender to push commands to this worker
    pub sender: mpsc::Sender<Result<MasterCommand, tonic::Status>>,
    /// Number of CPU cores
    pub cpu_cores: u32,
    /// Total RAM in MB
    pub total_ram_mb: u64,
    /// Worker capabilities (e.g., "can_compile", "high_memory")
    pub tags: Vec<String>,
    /// Last known CPU load percentage
    pub cpu_load_percent: f32,
    /// Last known RAM usage in MB
    pub ram_usage_mb: u64,
    /// Number of active tasks on this worker
    pub active_tasks: u32,
}

/// Application-wide shared state
#[derive(Clone)]
pub struct AppState {
    /// Active worker connections: WorkerID -> WorkerInfo
    pub workers: Arc<DashMap<String, WorkerInfo>>,
    /// Active jobs: JobID -> JobContext
    pub jobs: Arc<DashMap<String, JobContext>>,
    /// Pub/Sub engine for job updates
    pub pub_sub: Arc<BroadcastEngine<JobUpdate>>,
}

use crate::config::MasterConfig;

impl AppState {
    pub fn new(config: &MasterConfig) -> Self {
        Self {
            workers: Arc::new(DashMap::new()),
            jobs: Arc::new(DashMap::new()),
            pub_sub: Arc::new(BroadcastEngine::new(config.pubsub_capacity)),
        }
    }
}

impl Default for AppState {
    fn default() -> Self {
        let config = MasterConfig::from_env();
        Self::new(&config)
    }
}
