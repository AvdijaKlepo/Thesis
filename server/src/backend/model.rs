use std::{
    sync::atomic::{AtomicU64, AtomicUsize},
    time::Duration,
};

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Backend {
    pub id: String,
    pub address: String,
    pub weight: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct BackendMetricsSnapshot {
    pub active_connections: usize,
    pub latency_us: u64,
    pub total_requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub total_bytes_sent: u64,
    pub total_bytes_received: u64,
}

#[derive(Debug)]
pub struct BackendMetrics {
    pub active_connections: AtomicUsize,
    pub latency_us: AtomicU64,
    pub total_requests: AtomicU64,
    pub successful_requests: AtomicU64,
    pub failed_requests: AtomicU64,
    pub total_bytes_sent: AtomicU64,
    pub total_bytes_received: AtomicU64,
}

#[derive(Clone, Debug)]
pub struct Feedback {
    pub latency: Duration,
    pub success: bool,
}

pub const DEFAULT_INITIAL_LATENCY: Duration = Duration::from_millis(1);
pub const DEFAULT_FAILURE_PENALTY: Duration = Duration::from_secs(10);

impl BackendMetrics {
    pub fn new() -> Self {
        Self {
            active_connections: AtomicUsize::new(0),
            latency_us: AtomicU64::new(DEFAULT_INITIAL_LATENCY.as_micros() as u64),
            total_requests: AtomicU64::new(0),
            successful_requests: AtomicU64::new(0),
            failed_requests: AtomicU64::new(0),
            total_bytes_sent: AtomicU64::new(0),
            total_bytes_received: AtomicU64::new(0),
        }
    }

    pub fn record_start(&self) {
        self.active_connections
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.total_requests
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn record_end(&self, feedback: &Feedback, bytes_sent: usize, bytes_received: usize) {
        let _ = self.active_connections.fetch_update(
            std::sync::atomic::Ordering::SeqCst,
            std::sync::atomic::Ordering::Relaxed,
            |val| Some(val.saturating_sub(1)),
        );

        if feedback.success {
            self.successful_requests
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            let measured = feedback.latency.as_micros() as u64;
            let old = self.latency_us.load(std::sync::atomic::Ordering::Relaxed);
            let alpha = 0.2;
            let new_latency = ((1.0 - alpha) * old as f64 + alpha * measured as f64) as u64;
            self.latency_us
                .store(new_latency.max(1), std::sync::atomic::Ordering::Relaxed);
        } else {
            self.failed_requests
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            let penalty = (feedback.latency.as_micros() as u64)
                .max(DEFAULT_FAILURE_PENALTY.as_micros() as u64);
            let old = self.latency_us.load(std::sync::atomic::Ordering::Relaxed);
            let alpha = 0.2;
            let new_latency = ((1.0 - alpha) * old as f64 + alpha * penalty as f64) as u64;
            self.latency_us
                .store(new_latency.max(1), std::sync::atomic::Ordering::Relaxed);
        }

        self.total_bytes_sent
            .fetch_add(bytes_sent as u64, std::sync::atomic::Ordering::Relaxed);
        self.total_bytes_received
            .fetch_add(bytes_received as u64, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> BackendMetricsSnapshot {
        BackendMetricsSnapshot {
            active_connections: self
                .active_connections
                .load(std::sync::atomic::Ordering::Relaxed),
            latency_us: self.latency_us.load(std::sync::atomic::Ordering::Relaxed),
            total_requests: self
                .total_requests
                .load(std::sync::atomic::Ordering::Relaxed),
            successful_requests: self
                .successful_requests
                .load(std::sync::atomic::Ordering::Relaxed),
            failed_requests: self
                .failed_requests
                .load(std::sync::atomic::Ordering::Relaxed),
            total_bytes_sent: self
                .total_bytes_sent
                .load(std::sync::atomic::Ordering::Relaxed),
            total_bytes_received: self
                .total_bytes_received
                .load(std::sync::atomic::Ordering::Relaxed),
        }
    }
}

impl Default for BackendMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[path = "model_tests.rs"]
mod tests;
