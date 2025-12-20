use std::time::Duration;

#[derive(Clone, Debug)]
pub struct MasterConfig {
    pub grpc_addr: std::net::SocketAddr,
    pub http_addr: std::net::SocketAddr,
    pub gc_interval: Duration,
    pub job_ttl: Duration,

    pub pubsub_capacity: usize,
    pub redis_url: String,
}

impl MasterConfig {
    pub fn from_env() -> Self {
        dotenv::dotenv().ok();
        
        let grpc_addr = std::env::var("GRPC_ADDR")
           .unwrap_or_else(|_| "0.0.0.0:50051".to_string())
           .parse()
           .expect("Invalid GRPC_ADDR");
           
        let http_addr = std::env::var("HTTP_ADDR")
           .unwrap_or_else(|_| "0.0.0.0:8080".to_string())
           .parse()
           .expect("Invalid HTTP_ADDR");

        let gc_interval = std::env::var("GC_INTERVAL_SEC")
            .unwrap_or_else(|_| "60".to_string())
            .parse::<u64>()
            .map(std::time::Duration::from_secs)
            .expect("Invalid GC_INTERVAL_SEC");

        let job_ttl = std::env::var("JOB_TTL_SEC")
            .unwrap_or_else(|_| "3600".to_string()) // 1 hour
            .parse::<u64>()
            .map(std::time::Duration::from_secs)
            .expect("Invalid JOB_TTL_SEC");

        let redis_url = std::env::var("REDIS_URL")
            .unwrap_or_else(|_| "redis://localhost:6379".to_string());

        Self {
            grpc_addr,
            http_addr,
            gc_interval,
            job_ttl,
            pubsub_capacity: 1000, 
            redis_url,
        }
    }
}
