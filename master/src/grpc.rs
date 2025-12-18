//! Master Node - gRPC Server Implementation
//!
//! Handles bidirectional streaming connections from workers.

use crate::state::{AppState, FinalResponse, JobState, WorkerInfo};
use common::scheduler::{
    worker_message::Payload, worker_service_server::WorkerService, MasterCommand, WorkerMessage,
};
use std::pin::Pin;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};
use tracing::{error, info, warn};

pub struct WorkerServiceImpl {
    pub state: AppState,
}

impl WorkerServiceImpl {
    pub fn new(state: AppState) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl WorkerService for WorkerServiceImpl {
    type RegisterStreamStream =
        Pin<Box<dyn Stream<Item = Result<MasterCommand, Status>> + Send + 'static>>;

    async fn register_stream(
        &self,
        request: Request<Streaming<WorkerMessage>>,
    ) -> Result<Response<Self::RegisterStreamStream>, Status> {
        let mut stream = request.into_inner();
        let state = self.state.clone();

        // Channel for sending commands to this worker
        let (tx, rx) = mpsc::channel::<Result<MasterCommand, Status>>(32);

        // Spawn a task to handle incoming messages from this worker
        tokio::spawn(async move {
            let mut worker_id: Option<String> = None;

            while let Some(result) = stream.next().await {
                match result {
                    Ok(msg) => {
                        if let Some(payload) = msg.payload {
                            match payload {
                                Payload::Register(reg) => {
                                    info!(
                                        worker_id = %reg.worker_id,
                                        cpu_cores = reg.cpu_cores,
                                        ram_mb = reg.total_ram_mb,
                                        tags = ?reg.tags,
                                        "Worker registered"
                                    );

                                    worker_id = Some(reg.worker_id.clone());

                                    // Store worker info
                                    state.workers.insert(
                                        reg.worker_id.clone(),
                                        WorkerInfo {
                                            sender: tx.clone(),
                                            cpu_cores: reg.cpu_cores,
                                            total_ram_mb: reg.total_ram_mb,
                                            tags: reg.tags,
                                            cpu_load_percent: 0.0,
                                            ram_usage_mb: 0,
                                            active_tasks: 0,
                                        },
                                    );
                                }

                                Payload::Heartbeat(hb) => {
                                    info!(
                                        worker_id = %hb.worker_id,
                                        cpu_load = hb.cpu_load_percent,
                                        ram_mb = hb.ram_usage_mb,
                                        active_tasks = hb.active_tasks,
                                        "Heartbeat received"
                                    );

                                    // Update worker metrics
                                    if let Some(mut worker) = state.workers.get_mut(&hb.worker_id) {
                                        worker.cpu_load_percent = hb.cpu_load_percent;
                                        worker.ram_usage_mb = hb.ram_usage_mb;
                                        worker.active_tasks = hb.active_tasks;
                                    }
                                }

                                Payload::CompileResult(result) => {
                                    info!(
                                        job_id = %result.job_id,
                                        success = result.success,
                                        duration_ms = result.duration_ms,
                                        "Compile result received"
                                    );

                                    handle_compile_result(&state, result).await;
                                }

                                Payload::BatchResult(result) => {
                                    info!(
                                        job_id = %result.job_id,
                                        batch_id = %result.batch_id,
                                        num_results = result.results.len(),
                                        "Batch execution result received"
                                    );

                                    handle_batch_result(&state, result).await;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("Error receiving from worker: {}", e);
                        break;
                    }
                }
            }

            // Worker disconnected - clean up
            if let Some(id) = worker_id {
                info!(worker_id = %id, "Worker disconnected");
                state.workers.remove(&id);
            }
        });

        // Return the receiver stream for sending commands to worker
        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(output_stream)))
    }
}

async fn handle_compile_result(state: &AppState, result: common::scheduler::CompileResult) {
    if let Some(mut job) = state.jobs.get_mut(&result.job_id) {
        job.compiler_output = Some(result.compiler_output.clone());

        if result.success {
            job.binary = Some(result.binary_payload);
            // TODO: Trigger Phase 2 - batch execution
            info!(job_id = %result.job_id, "Compilation successful, ready for execution phase");
        } else {
            // Compilation failed - complete the job with error
            job.state = JobState::Completed;

            if let Some(responder) = job.responder.take() {
                let _ = responder.send(FinalResponse {
                    job_id: result.job_id,
                    success: false,
                    results: vec![],
                    compiler_output: Some(result.compiler_output),
                    error: Some("Compilation failed".to_string()),
                });
            }
        }
    }
}

async fn handle_batch_result(state: &AppState, result: common::scheduler::BatchExecutionResult) {
    if let Some(mut job) = state.jobs.get_mut(&result.job_id) {
        // Append results
        job.results.extend(result.results);

        // Check if this was a system error
        if !result.system_error.is_empty() {
            warn!(
                job_id = %result.job_id,
                batch_id = %result.batch_id,
                error = %result.system_error,
                "Batch execution had system error"
            );
        }

        // Decrement pending batches
        if let JobState::Executing { pending_batches } = &mut job.state {
            *pending_batches = pending_batches.saturating_sub(1);

            if *pending_batches == 0 {
                // All batches complete
                job.state = JobState::Completed;

                if let Some(responder) = job.responder.take() {
                    let _ = responder.send(FinalResponse {
                        job_id: result.job_id,
                        success: true,
                        results: job.results.clone(),
                        compiler_output: job.compiler_output.clone(),
                        error: None,
                    });
                }
            }
        }
    }
}
