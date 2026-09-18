use std::sync::atomic::Ordering;

use crate::algorithms::balancers::{BackendNode, LoadBalancer};

pub struct LeastConnections {
    backends: Vec<BackendNode>,
}

impl LeastConnections {
    pub fn new(backends: Vec<BackendNode>) -> Self {
        Self { backends }
    }
}

impl LoadBalancer for LeastConnections {
    fn next_excluding(&self, allow_unhealthy: bool, excluded: &[&str]) -> Option<BackendNode> {
        self.backends
            .iter()
            .filter(|backend| {
                !excluded.contains(&backend.id.as_str())
                    && (allow_unhealthy || backend.healthy.load(Ordering::Relaxed))
            })
            .min_by_key(|b| b.metrics.active_connections.load(Ordering::Relaxed))
            .cloned()
        }

    fn name(&self) -> &'static str {
        "least_connections"
    }

    fn backends(&self) -> &[BackendNode] {
        &self.backends
    }
}

pub struct LeastResponseTime {
    backends: Vec<BackendNode>,
}

impl LeastResponseTime {
    pub fn new(backends: Vec<BackendNode>) -> Self {
        Self { backends }
    }
}

impl LoadBalancer for LeastResponseTime {
    fn next_excluding(&self, allow_unhealthy: bool, excluded: &[&str]) -> Option<BackendNode> {
        self.backends
            .iter()
            .filter(|backend| {
                !excluded.contains(&backend.id.as_str())
                    && (allow_unhealthy || backend.healthy.load(Ordering::Relaxed))
            })
            .min_by(|a, b| {
                let a_latency = a.metrics.latency_us.load(Ordering::Relaxed);
                let b_latency = b.metrics.latency_us.load(Ordering::Relaxed);

                let a_connections = a.metrics.active_connections.load(Ordering::Relaxed);
                let b_connections = b.metrics.active_connections.load(Ordering::Relaxed);

                let a_score = a_latency.saturating_mul((a_connections + 1) as u64);
                let b_score = b_latency.saturating_mul((b_connections + 1) as u64);

                a_score.cmp(&b_score)
            })
            .cloned()
    }

    fn name(&self) -> &'static str {
        "least_response_time"
    }

    fn backends(&self) -> &[BackendNode] {
        &self.backends
    }
}

