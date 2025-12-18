//! Worker Node - System Metrics
//!
//! Collects CPU and RAM metrics for heartbeat messages.

use sysinfo::System;

pub struct MetricsCollector {
    system: System,
}

impl MetricsCollector {
    pub fn new() -> Self {
        let mut system = System::new_all();
        system.refresh_all();
        Self { system }
    }

    /// Refresh system metrics
    pub fn refresh(&mut self) {
        self.system.refresh_cpu_all();
        self.system.refresh_memory();
    }

    /// Get CPU load as percentage (0-100)
    pub fn cpu_load_percent(&self) -> f32 {
        let cpus = self.system.cpus();
        if cpus.is_empty() {
            return 0.0;
        }
        let total: f32 = cpus.iter().map(|cpu| cpu.cpu_usage()).sum();
        total / cpus.len() as f32
    }

    /// Get RAM usage in MB
    pub fn ram_usage_mb(&self) -> u64 {
        self.system.used_memory() / (1024 * 1024)
    }

    /// Get total RAM in MB
    pub fn total_ram_mb(&self) -> u64 {
        self.system.total_memory() / (1024 * 1024)
    }

    /// Get number of CPU cores
    pub fn cpu_cores(&self) -> u32 {
        self.system.cpus().len() as u32
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}
