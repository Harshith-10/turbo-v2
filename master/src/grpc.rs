//! Master Node - gRPC Server Implementation
//!
//! Handles bidirectional streaming connections from workers.

use crate::state::{AppState, FinalResponse, JobState, JobUpdate, WorkerInfo};
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
    let job_id = result.job_id.clone();
    
    // First, update the job with compile result
    let dispatch_info = {
        if let Some(mut job) = state.jobs.get_mut(&job_id) {
            job.compiler_output = Some(result.compiler_output.clone());

            if result.success {
                job.binary = Some(result.binary_payload.clone());
                job.state = JobState::Executing { pending_batches: 1 };
                
                // Publish update
                state.pub_sub.publish(job_id.clone(), JobUpdate::Compiled {
                    success: true,
                    output: Some(result.compiler_output.clone()),
                });
                
                // Gather info needed for dispatch
                Some((
                    job.language.clone(),
                    result.binary_payload.clone(),
                    job.test_cases.clone(),
                    job.time_limit_ms,
                    job.memory_limit_mb,
                ))
            } else {
                // Compilation failed - complete the job with error
                job.state = JobState::Completed;
                
                state.pub_sub.publish(job_id.clone(), JobUpdate::Compiled {
                    success: false,
                    output: Some(result.compiler_output.clone()),
                });
                state.pub_sub.publish(job_id.clone(), JobUpdate::Completed(FinalResponse::from_results(
                    job_id.clone(),
                    false,
                    vec![],
                    Some(result.compiler_output.clone()),
                    Some("Compilation failed".to_string()),
                )));
                
                None
            }
        } else {
            None
        }
    };

    // Dispatch execution if compilation succeeded
    if let Some((language, binary, test_cases, time_limit, memory_limit)) = dispatch_info {
        info!(job_id = %job_id, "Compilation successful, dispatching execution phase");

        // Find a worker to execute
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

        if let Some(worker_id) = worker_id {
            let task = common::scheduler::ExecuteBatchTask {
                job_id: job_id.clone(),
                batch_id: "batch_1".to_string(),
                language,
                payload: Some(common::scheduler::execute_batch_task::Payload::BinaryArtifact(binary)),
                inputs: test_cases,
                time_limit_ms: time_limit,
                memory_limit_mb: memory_limit,
            };

            let cmd = MasterCommand {
                task: Some(common::scheduler::master_command::Task::Execute(task)),
            };

            if let Some(worker) = state.workers.get(&worker_id) {
                let _ = worker.sender.send(Ok(cmd)).await;
                info!(job_id = %job_id, worker_id = %worker_id, "Dispatched execute task with binary");
            }
        } else {
            warn!(job_id = %job_id, "No workers available for execution phase");
        }
    } else {
        info!(job_id = %job_id, "Compilation failed");
    }
}

async fn handle_batch_result(state: &AppState, result: common::scheduler::BatchExecutionResult) {
    if let Some(mut job) = state.jobs.get_mut(&result.job_id) {
        // Append results
        job.results.extend(result.results.clone());

        // Publish individual results
        for res in &result.results {
            state.pub_sub.publish(result.job_id.clone(), JobUpdate::Result(res.clone().into()));
        }

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
                let total = job.total_test_cases;
                if let JobState::Executing { pending_batches } = &mut job.state {
                    *pending_batches = pending_batches.saturating_sub(1);
        
                    if *pending_batches == 0 {
                        // All batches complete
                        job.state = JobState::Completed;
        
                        if let Some(responder) = job.responder.take() {
                            let final_response = FinalResponse::from_results(
                                result.job_id.clone(),
                                true,
                                job.results.clone(),
                                job.compiler_output.clone(),
                                None,
                            );
                    
                    state.pub_sub.publish(result.job_id.clone(), JobUpdate::Completed(final_response.clone()));
                    let _ = responder.send(final_response);
                }
            } else {
                 // Publish progress update
                 state.pub_sub.publish(result.job_id.clone(), JobUpdate::Executing {
                     completed: total.saturating_sub(*pending_batches * 1), // approximate if batches > 1 case
                     total,
                 });
            }

        }
    }
}
