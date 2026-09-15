use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use crate::{
    Backend,
    backend::model::{BackendMetrics, Feedback},
};

#[derive(Clone, Debug)]
pub struct BackendNode {
    pub backend: Backend,
    pub metrics: Arc<BackendMetrics>,
    pub healthy: Arc<AtomicBool>,
}

impl BackendNode {
    pub fn new(backend: Backend) -> Self {
        Self {
            backend,
            metrics: Arc::new(BackendMetrics::new()),
            healthy: Arc::new(AtomicBool::new(true)),
        }
    }
}

impl From<Backend> for BackendNode {
    fn from(backend: Backend) -> Self {
        Self::new(backend)
    }
}

impl std::ops::Deref for BackendNode {
    type Target = Backend;

    fn deref(&self) -> &Self::Target {
        &self.backend
    }
}

pub trait LoadBalancer: Send + Sync {
    /// Selects the next eligible backend.
    ///
    /// When `allow_unhealthy` is false, unhealthy backends are excluded.
    /// An empty eligible set always returns `None`.
    fn next(&self, allow_unhealthy: bool) -> Option<BackendNode>;

    fn release(&self, _backend: &BackendNode, _feedback: Feedback) {}

    fn name(&self) -> &'static str;

    fn backends(&self) -> &[BackendNode] {
        &[]
    }
}

pub struct RoundRobin {
    backends: Vec<BackendNode>,
    counter: AtomicUsize,
}

impl RoundRobin {
    pub fn new(backends: Vec<BackendNode>) -> Self {
        Self {
            backends,
            counter: AtomicUsize::new(0),
        }
    }
}

impl LoadBalancer for RoundRobin {
    fn next(&self, allow_unhealthy: bool) -> Option<BackendNode> {
        let eligible: Vec<&BackendNode> = self
            .backends
            .iter()
            .filter(|backend| allow_unhealthy || backend.healthy.load(Ordering::Relaxed))
            .collect();
        if eligible.is_empty() {
            return None;
        }

        let index = self.counter.fetch_add(1, Ordering::Relaxed) % eligible.len();
        Some(eligible[index].clone())
    }

    fn name(&self) -> &'static str {
        "round_robin"
    }

    fn backends(&self) -> &[BackendNode] {
        &self.backends
    }
}

struct WeightState {
    current: i64,
    effective_weight: i64,
}

pub struct WeightedRoundRobin {
    backends: Vec<BackendNode>,
    state: Mutex<Vec<WeightState>>,
}

impl WeightedRoundRobin {
    pub fn new(backends: Vec<BackendNode>) -> Self {
        let state = backends
            .iter()
            .map(|b| WeightState {
                current: 0,
                effective_weight: b.backend.weight as i64,
            })
            .collect();

        Self {
            backends,
            state: Mutex::new(state),
        }
    }
}

impl LoadBalancer for WeightedRoundRobin {
    fn next(&self, allow_unhealthy: bool) -> Option<BackendNode> {
        let mut state = self.state.lock().unwrap();

        let eligible: Vec<usize> = self
            .backends
            .iter()
            .enumerate()
            .filter(|(_, backend)| allow_unhealthy || backend.healthy.load(Ordering::Relaxed))
            .map(|(index, _)| index)
            .collect();
        if eligible.is_empty() {
            return None;
        }

        let total: i64 = eligible
            .iter()
            .map(|&index| state[index].effective_weight)
            .sum();

        for &index in &eligible {
            state[index].current += state[index].effective_weight;
        }

        let best = eligible
            .iter()
            .copied()
            .max_by_key(|&index| state[index].current)?;

        state[best].current -= total;
        Some(self.backends[best].clone())
    }

    fn name(&self) -> &'static str {
        "weighted_round_robin"
    }

    fn backends(&self) -> &[BackendNode] {
        &self.backends
    }
}

pub struct LeastConnections {
    backends: Vec<BackendNode>,
}

impl LeastConnections {
    pub fn new(backends: Vec<BackendNode>) -> Self {
        Self { backends }
    }
}

impl LoadBalancer for LeastConnections {
    fn next(&self, allow_unhealthy: bool) -> Option<BackendNode> {
        self.backends
            .iter()
            .filter(|backend| allow_unhealthy || backend.healthy.load(Ordering::Relaxed))
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
    fn next(&self, allow_unhealthy: bool) -> Option<BackendNode> {
        self.backends
            .iter()
            .filter(|backend| allow_unhealthy || backend.healthy.load(Ordering::Relaxed))
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
#[cfg(test)]
#[path = "balancers_tests.rs"]
mod tests;
