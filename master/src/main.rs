//! Master Node - Distributed Code Execution System
//!
//! Central command server that:
//! - Exposes a gRPC server for worker connections (port 50051)
//! - Exposes an HTTP API for client submissions (port 8080)
//! - Orchestrates the split-phase execution pipeline

mod config;
mod grpc;
mod http;
pub mod rusq;
pub mod state;
mod scheduler;

use config::MasterConfig;
use common::scheduler::worker_service_server::WorkerServiceServer;
use grpc::WorkerServiceImpl;
use state::AppState;
use tonic::transport::Server as TonicServer;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    // Load configuration
    let config = MasterConfig::from_env();
    info!("Starting Master Node...");
    info!("Configuration loaded: gRPC={}, HTTP={}", config.grpc_addr, config.http_addr);

    // Create shared state
    let state: AppState = AppState::new(&config);

    // Start Garbage Collection Task
    let gc_state = state.clone();
    let gc_interval = config.gc_interval;
    let job_ttl = config.job_ttl;
    
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(gc_interval);
        loop {
            interval.tick().await;
            
            let now = std::time::Instant::now();
            let mut to_remove = Vec::new();

            // Identify old jobs
            for entry in gc_state.jobs.iter() {
                if now.duration_since(entry.value().created_at) > job_ttl {
                    to_remove.push(entry.key().clone());
                }
            }

            // Process removals
            for job_id in to_remove {
                // If the job wasn't completed, we should notify clients that it timed out (System TLE)
                // This handles cases where workers crash or messages get lost
                if let Some(mut job) = gc_state.jobs.get_mut(&job_id) {
                    if !matches!(job.state, state::JobState::Completed) {
                         info!(job_id = %job_id, "Job timed out (System TLE) - cleaning up");
                         
                         let final_response = state::FinalResponse {
                             job_id: job_id.clone(),
                             success: false,
                             results: vec![],
                             compiler_output: None,
                             error: Some("Time Limit Exceeded (System Timeout)".to_string()),
                         };
                         
                         // Notify any listeners
                         gc_state.pub_sub.publish(job_id.clone(), state::JobUpdate::Completed(final_response.clone()));
                         
                         // Notify HTTP waiter if present
                         if let Some(responder) = job.responder.take() {
                             let _ = responder.send(final_response);
                         }
                    } else {
                        info!(job_id = %job_id, "Garbage collecting completed job");
                    }
                }
            
                // Actually remove it
                gc_state.jobs.remove(&job_id);
                // Also cleanup the broadcast topic
                gc_state.pub_sub.remove_topic(&job_id);
            }
        }
    });

    // Start gRPC server for workers
    let grpc_state = state.clone();
    let grpc_addr = config.grpc_addr;
    let grpc_handle = tokio::spawn(async move {
        let service = WorkerServiceImpl::new(grpc_state);

        info!("gRPC server listening on {}", grpc_addr);

        TonicServer::builder()
            .add_service(WorkerServiceServer::new(service))
            .serve(grpc_addr)
            .await
            .expect("gRPC server failed");
    });

    // Start HTTP server for clients
    let http_state = state.clone();
    let http_addr = config.http_addr;
    let http_handle = tokio::spawn(async move {
        let app = http::create_router(http_state);
        let listener = tokio::net::TcpListener::bind(http_addr)
            .await
            .expect("Failed to bind HTTP server");

        info!("HTTP server listening on {}", http_addr);

        axum::serve(listener, app)
            .await
            .expect("HTTP server failed");
    });

    // Wait for both servers
    tokio::select! {
        _ = grpc_handle => {
            info!("gRPC server terminated");
        }
        _ = http_handle => {
            info!("HTTP server terminated");
        }
    }

    Ok(())
}
