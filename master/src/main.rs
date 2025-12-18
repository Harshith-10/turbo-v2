//! Master Node - Distributed Code Execution System
//!
//! Central command server that:
//! - Exposes a gRPC server for worker connections (port 50051)
//! - Exposes an HTTP API for client submissions (port 8080)
//! - Orchestrates the split-phase execution pipeline

mod grpc;
mod http;
mod scheduler;
mod state;

use common::scheduler::worker_service_server::WorkerServiceServer;
use grpc::WorkerServiceImpl;
use state::AppState;
use tonic::transport::Server as TonicServer;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

const GRPC_ADDR: &str = "0.0.0.0:50051";
const HTTP_ADDR: &str = "0.0.0.0:8080";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    info!("Starting Master Node...");

    // Create shared state
    let state = AppState::new();

    // Start gRPC server for workers
    let grpc_state = state.clone();
    let grpc_handle = tokio::spawn(async move {
        let addr = GRPC_ADDR.parse().expect("Invalid gRPC address");
        let service = WorkerServiceImpl::new(grpc_state);

        info!("gRPC server listening on {}", GRPC_ADDR);

        TonicServer::builder()
            .add_service(WorkerServiceServer::new(service))
            .serve(addr)
            .await
            .expect("gRPC server failed");
    });

    // Start HTTP server for clients
    let http_state = state.clone();
    let http_handle = tokio::spawn(async move {
        let app = http::create_router(http_state);
        let listener = tokio::net::TcpListener::bind(HTTP_ADDR)
            .await
            .expect("Failed to bind HTTP server");

        info!("HTTP server listening on {}", HTTP_ADDR);

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
