//! Master Node - State Management
//! 
//! Provides thread-safe state containers for workers and jobs using DashMap.

use common::scheduler::{MasterCommand, TestCaseResult};
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

/// Final response sent back to HTTP client
#[derive(Debug, Clone)]
pub struct FinalResponse {
    pub job_id: String,
    pub success: bool,
    pub results: Vec<TestCaseResult>,
    pub compiler_output: Option<String>,
    pub error: Option<String>,
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
}

impl AppState {
    pub fn new() -> Self {
        Self {
            workers: Arc::new(DashMap::new()),
            jobs: Arc::new(DashMap::new()),
        }
    }
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}
