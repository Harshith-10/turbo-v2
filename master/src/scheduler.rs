//! Master Node - Job Scheduler
//!
//! Handles worker selection and test case batching.

use crate::state::AppState;
use common::scheduler::{
    execute_batch_task, CompileTask, ExecuteBatchTask, MasterCommand, TestCase,
};
use tracing::{error, info, warn};

/// Batch size for distributing test cases
const BATCH_SIZE: usize = 20;

/// Select a worker capable of compilation (has "can_compile" tag and low load)
pub fn select_compile_worker(state: &AppState) -> Option<String> {
    state
        .workers
        .iter()
        .filter(|entry| {
            entry.value().tags.contains(&"can_compile".to_string())
                && entry.value().cpu_load_percent < 50.0
        })
        .min_by(|a, b| {
            a.value()
                .cpu_load_percent
                .partial_cmp(&b.value().cpu_load_percent)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|entry| entry.key().clone())
}

/// Select workers for execution (round robin with load consideration)
pub fn select_execution_workers(state: &AppState, count: usize) -> Vec<String> {
    let mut workers: Vec<_> = state
        .workers
        .iter()
        .filter(|entry| entry.value().cpu_load_percent < 80.0)
        .map(|entry| (entry.key().clone(), entry.value().cpu_load_percent))
        .collect();

    // Sort by CPU load (ascending)
    workers.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

    workers.into_iter().take(count).map(|(id, _)| id).collect()
}

/// Split test cases into batches
pub fn create_batches(test_cases: Vec<TestCase>) -> Vec<Vec<TestCase>> {
    test_cases.chunks(BATCH_SIZE).map(|c| c.to_vec()).collect()
}

/// Send a compile task to a specific worker
pub async fn dispatch_compile_task(
    state: &AppState,
    worker_id: &str,
    job_id: &str,
    language: &str,
    source_code: &str,
    flags: Vec<String>,
) -> Result<(), String> {
    if let Some(worker) = state.workers.get(worker_id) {
        let cmd = MasterCommand {
            task: Some(common::scheduler::master_command::Task::Compile(
                CompileTask {
                    job_id: job_id.to_string(),
                    language: language.to_string(),
                    source_code: source_code.to_string(),
                    flags,
                },
            )),
        };

        worker
            .sender
            .send(Ok(cmd))
            .await
            .map_err(|e| format!("Failed to send compile task: {}", e))?;

        info!(
            job_id = %job_id,
            worker_id = %worker_id,
            "Dispatched compile task"
        );

        Ok(())
    } else {
        Err(format!("Worker {} not found", worker_id))
    }
}

/// Send an execute batch task to a specific worker
pub async fn dispatch_execute_task(
    state: &AppState,
    worker_id: &str,
    job_id: &str,
    batch_id: &str,
    language: &str,
    binary: Option<Vec<u8>>,
    source_code: Option<String>,
    test_cases: Vec<TestCase>,
    time_limit_ms: u32,
    memory_limit_mb: u32,
) -> Result<(), String> {
    if let Some(worker) = state.workers.get(worker_id) {
        let payload = if let Some(bin) = binary {
            Some(execute_batch_task::Payload::BinaryArtifact(bin))
        } else if let Some(src) = source_code {
            Some(execute_batch_task::Payload::SourceCode(src))
        } else {
            return Err("Neither binary nor source code provided".to_string());
        };

        let cmd = MasterCommand {
            task: Some(common::scheduler::master_command::Task::Execute(
                ExecuteBatchTask {
                    job_id: job_id.to_string(),
                    batch_id: batch_id.to_string(),
                    language: language.to_string(),
                    payload,
                    inputs: test_cases,
                    time_limit_ms,
                    memory_limit_mb,
                },
            )),
        };

        worker
            .sender
            .send(Ok(cmd))
            .await
            .map_err(|e| format!("Failed to send execute task: {}", e))?;

        info!(
            job_id = %job_id,
            batch_id = %batch_id,
            worker_id = %worker_id,
            "Dispatched execute task"
        );

        Ok(())
    } else {
        Err(format!("Worker {} not found", worker_id))
    }
}
