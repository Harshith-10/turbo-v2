//! Worker Node - gRPC Client
//!
//! Handles connection to Master with reconnection logic.

use common::scheduler::{
    master_command::Task, worker_message::Payload, worker_service_client::WorkerServiceClient,
    Heartbeat, Register, WorkerMessage,
};
use crate::docker::DockerExecutor;
use crate::metrics::MetricsCollector;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::interval;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tracing::{error, info, warn};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const RECONNECT_BASE_DELAY: Duration = Duration::from_secs(2);
const RECONNECT_MAX_DELAY: Duration = Duration::from_secs(60);

pub struct GrpcClient {
    worker_id: String,
    master_addr: String,
    metrics: MetricsCollector,
    docker: Arc<DockerExecutor>,
    active_tasks: Arc<AtomicU32>,
}

impl GrpcClient {
    pub fn new(worker_id: String, master_addr: String, docker: Arc<DockerExecutor>) -> Self {
        Self {
            worker_id,
            master_addr,
            metrics: MetricsCollector::new(),
            docker,
            active_tasks: Arc::new(AtomicU32::new(0)),
        }
    }

    /// Main connection loop with exponential backoff
    pub async fn run(&mut self) {
        let mut retry_count = 0;

        loop {
            info!(
                master = %self.master_addr,
                "Connecting to Master..."
            );

            match self.connect_and_process().await {
                Ok(_) => {
                    info!("Connection closed gracefully");
                    retry_count = 0;
                }
                Err(e) => {
                    error!("Connection error: {}", e);
                }
            }

            // Exponential backoff with jitter
            let delay = std::cmp::min(
                RECONNECT_BASE_DELAY * 2u32.pow(retry_count),
                RECONNECT_MAX_DELAY,
            );
            let jitter = Duration::from_millis(rand_jitter(500));

            warn!(
                delay_secs = (delay + jitter).as_secs(),
                "Reconnecting in {} seconds...",
                (delay + jitter).as_secs()
            );

            tokio::time::sleep(delay + jitter).await;
            retry_count = std::cmp::min(retry_count + 1, 6);
        }
    }

    async fn connect_and_process(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let channel = Channel::from_shared(self.master_addr.clone())?
            .connect()
            .await?;

        let mut client = WorkerServiceClient::new(channel);

        // Create channel for sending messages to master
        let (tx, rx) = mpsc::channel::<WorkerMessage>(32);
        let rx_stream = ReceiverStream::new(rx);

        // Start the bidirectional stream
        let response = client.register_stream(rx_stream).await?;
        let mut inbound = response.into_inner();

        // Send registration
        self.metrics.refresh();
        let register_msg = WorkerMessage {
            payload: Some(Payload::Register(Register {
                worker_id: self.worker_id.clone(),
                cpu_cores: self.metrics.cpu_cores(),
                total_ram_mb: self.metrics.total_ram_mb(),
                tags: vec!["can_compile".to_string()], // TODO: detect capabilities
            })),
        };
        tx.send(register_msg).await?;
        info!("Sent registration to Master");

        // Spawn heartbeat task
        let heartbeat_tx = tx.clone();
        let worker_id = self.worker_id.clone();
        let active_tasks_hb = Arc::clone(&self.active_tasks);
        let mut metrics = MetricsCollector::new();
        let heartbeat_handle = tokio::spawn(async move {
            let mut interval = interval(HEARTBEAT_INTERVAL);
            loop {
                interval.tick().await;
                metrics.refresh();

                let hb = WorkerMessage {
                    payload: Some(Payload::Heartbeat(Heartbeat {
                        worker_id: worker_id.clone(),
                        cpu_load_percent: metrics.cpu_load_percent(),
                        ram_usage_mb: metrics.ram_usage_mb(),
                        active_tasks: active_tasks_hb.load(Ordering::Relaxed),
                    })),
                };

                if heartbeat_tx.send(hb).await.is_err() {
                    break;
                }
            }
        });

        // Process incoming commands
        while let Some(result) = inbound.message().await? {
            if let Some(task) = result.task {
                let tx = tx.clone();
                let docker = self.docker.clone();
                let worker_id = self.worker_id.clone();
                let active_tasks = Arc::clone(&self.active_tasks);

                match task {
                    Task::Compile(compile_task) => {
                        info!(
                            job_id = %compile_task.job_id,
                            language = %compile_task.language,
                            "Received compile task"
                        );

                        active_tasks.fetch_add(1, Ordering::Relaxed);
                        let active_tasks_clone = Arc::clone(&active_tasks);

                        tokio::spawn(async move {
                            let result = docker
                                .compile(
                                    &compile_task.job_id,
                                    &compile_task.language,
                                    &compile_task.source_code,
                                    &compile_task.flags,
                                )
                                .await;

                            let msg = WorkerMessage {
                                payload: Some(Payload::CompileResult(result)),
                            };
                            let _ = tx.send(msg).await;
                            active_tasks_clone.fetch_sub(1, Ordering::Relaxed);
                        });
                    }

                    Task::Execute(exec_task) => {
                        info!(
                            job_id = %exec_task.job_id,
                            batch_id = %exec_task.batch_id,
                            test_cases = exec_task.inputs.len(),
                            "Received execute task"
                        );

                        active_tasks.fetch_add(1, Ordering::Relaxed);
                        let active_tasks_clone = Arc::clone(&active_tasks);

                        tokio::spawn(async move {
                            let (binary, source) = match exec_task.payload {
                                Some(common::scheduler::execute_batch_task::Payload::BinaryArtifact(b)) => {
                                    (Some(b), None)
                                }
                                Some(common::scheduler::execute_batch_task::Payload::SourceCode(s)) => {
                                    (None, Some(s))
                                }
                                None => (None, None),
                            };

                            let result = docker
                                .execute_batch(
                                    &exec_task.job_id,
                                    &exec_task.batch_id,
                                    &worker_id,
                                    &exec_task.language,
                                    binary.as_deref(),
                                    source.as_deref(),
                                    &exec_task.inputs,
                                    exec_task.time_limit_ms,
                                    exec_task.memory_limit_mb,
                                )
                                .await;

                            let msg = WorkerMessage {
                                payload: Some(Payload::BatchResult(result)),
                            };
                            let _ = tx.send(msg).await;
                            active_tasks_clone.fetch_sub(1, Ordering::Relaxed);
                        });
                    }

                    Task::Shutdown(shutdown) => {
                        info!(reason = %shutdown.reason, "Received shutdown request");
                        break;
                    }
                }
            }
        }

        heartbeat_handle.abort();
        Ok(())
    }
}

/// Generate random jitter in milliseconds
fn rand_jitter(max_ms: u64) -> u64 {
    use std::time::SystemTime;
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos() as u64;
    nanos % max_ms
}
