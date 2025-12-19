use std::env;
use std::path::Path;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct MasterConfig {
    pub grpc_addr: std::net::SocketAddr,
    pub http_addr: std::net::SocketAddr,
    pub gc_interval: Duration,
    pub job_ttl: Duration,
    pub pubsub_capacity: usize,
}

impl MasterConfig {
    pub fn from_env() -> Self {
        // Try to find .env file
        if dotenv::dotenv().is_err() {
            // If running from root, try looking in master/
            if Path::new("master/.env").exists() {
                dotenv::from_filename("master/.env").ok();
            }
        }

        Self {
            grpc_addr: env::var("GRPC_ADDR")
                .unwrap_or_else(|_| "0.0.0.0:50051".to_string())
                .parse()
                .expect("Invalid GRPC_ADDR"),
            
            http_addr: env::var("HTTP_ADDR")
                .unwrap_or_else(|_| "0.0.0.0:8080".to_string())
                .parse()
                .expect("Invalid HTTP_ADDR"),
            
            gc_interval: Duration::from_secs(
                env::var("GC_INTERVAL_SEC")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(30)
            ),
            
            job_ttl: Duration::from_secs(
                env::var("JOB_TTL_SEC")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(120)
            ),
            
            pubsub_capacity: env::var("PUBSUB_CAPACITY")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(100),
        }
    }
}
