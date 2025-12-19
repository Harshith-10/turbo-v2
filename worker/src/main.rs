//! Worker Node - Distributed Code Execution System
//!
//! Stateless execution unit that:
//! - Connects to Master via gRPC
//! - Sends periodic heartbeats with system metrics
//! - Executes compilation and code execution tasks in Docker

mod docker;
mod grpc;
mod metrics;

mod config;

use config::WorkerConfig;
use docker::DockerExecutor;
use grpc::GrpcClient;
use std::sync::Arc;
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    // Load configuration
    let config = WorkerConfig::from_env();

    // Generate unique worker ID
    let worker_id = Uuid::new_v4().to_string();
    info!(worker_id = %worker_id, "Starting Worker Node...");
    info!(master_addr = %config.master_addr, "Configuration loaded");

    // Initialize Docker executor
    let docker = match DockerExecutor::new(config.clone()) {
        Ok(d) => Arc::new(d),
        Err(e) => {
            error!("Failed to connect to Docker: {}", e);
            error!("Make sure Docker is running and accessible");
            return Err(e.into());
        }
    };

    info!("Docker connection established");

    // Create and run gRPC client
    let mut client = GrpcClient::new(worker_id, config.clone(), docker);
    client.run().await;

    Ok(())
}
