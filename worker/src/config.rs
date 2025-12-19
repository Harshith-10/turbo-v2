use std::env;
use std::path::Path;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct WorkerConfig {
    pub master_addr: String,
    pub heartbeat_interval: Duration,
    pub reconnect_base_delay: Duration,
    pub reconnect_max_delay: Duration,
    pub compile_memory_limit_bytes: i64,
    pub compile_cpu_nano: i64,
    pub execute_cpu_nano: i64,
    pub compile_timeout: Duration,
}

impl WorkerConfig {
    pub fn from_env() -> Self {
        // Try to find .env file
        if dotenv::dotenv().is_err() {
            // If running from root, try looking in worker/
            if Path::new("worker/.env").exists() {
                dotenv::from_filename("worker/.env").ok();
            }
        }

        Self {
            master_addr: env::var("MASTER_ADDR")
                .unwrap_or_else(|_| "http://127.0.0.1:50051".to_string()),
            
            heartbeat_interval: Duration::from_millis(
                env::var("HEARTBEAT_INTERVAL_MS")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(5000)
            ),
            
            reconnect_base_delay: Duration::from_millis(
                env::var("RECONNECT_BASE_DELAY_MS")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(2000)
            ),
            
            reconnect_max_delay: Duration::from_millis(
                env::var("RECONNECT_MAX_DELAY_MS")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(60000)
            ),
            
            compile_memory_limit_bytes: env::var("COMPILE_MEMORY_LIMIT_BYTES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(512 * 1024 * 1024), // 512 MB
                
            compile_cpu_nano: env::var("COMPILE_CPU_NANO")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(2_000_000_000), // 2.0 CPUs
                
            execute_cpu_nano: env::var("EXECUTE_CPU_NANO")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1_000_000_000), // 1.0 CPU
                
            compile_timeout: Duration::from_secs(
                env::var("COMPILE_TIMEOUT_SEC")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(60)
            ),
        }
    }
}
