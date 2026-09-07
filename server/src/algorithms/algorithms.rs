use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use crate::{Backend, backend::backend_server::{BackendMetrics, Feedback}};



#[derive(Clone, Debug)]
pub struct BackendNode {
    pub backend: Backend,
    pub metrics: Arc<BackendMetrics>,
}

impl BackendNode {
    pub fn new(backend: Backend) -> Self {
        Self {
            backend,
            metrics: Arc::new(BackendMetrics::new()),
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

pub struct LatencyBalancer {
    backends: Vec<BackendNode>,
}
pub fn default_backends() -> Vec<BackendNode> {
    let backends = vec![
        BackendNode {
            backend: Backend {
                id: 1.to_string(),
                address: "127.0.0.1:8081".into(),
                weight: 3,
            },
            metrics: Arc::new(BackendMetrics::new()),
        },
        BackendNode {
            backend: Backend {
                id: 2.to_string(),
                address: "127.0.0.1:8082".into(),
                weight: 1,
            },
            metrics: Arc::new(BackendMetrics::new()),
        },
        BackendNode {
            backend: Backend {
                id: 3.to_string(),
                address: "127.0.0.1:8083".into(),
                weight: 1,
            },
            metrics: Arc::new(BackendMetrics::new()),
        },
    ];
    backends
}
pub trait LoadBalancer: Send + Sync {
    fn next(&self) -> BackendNode;

    fn release(&self, _backend: &BackendNode, _feedback: Feedback) {}
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
    fn next(&self) -> BackendNode {
        let i = self
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            % self.backends.len();
        self.backends[i].clone()
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
    fn next(&self) -> BackendNode {
        let mut state = self.state.lock().unwrap();
        let total: i64 = state.iter().map(|s| s.effective_weight).sum();

        let best = state
            .iter()
            .enumerate()
            .max_by_key(|&(_, s)| s.current)
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        state[best].current -= total;
        self.backends[best].clone()
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
    fn next(&self) -> BackendNode {
        let best = self
            .backends
            .iter()
            .min_by_key(|b| b.metrics.active_connections.load(Ordering::Relaxed))
            .unwrap();

        best.metrics
            .active_connections
            .fetch_add(1, Ordering::Relaxed);

        best.clone()
    }

    fn release(&self, backend: &BackendNode, feedback: Feedback) {
        backend
            .metrics
            .active_connections
            .fetch_sub(1, Ordering::Relaxed);
    }
}

pub struct LeastResponseTime {
    backends: Vec<BackendNode>,
    alpha: f64,
}

impl LeastResponseTime {
    pub fn new(backends: Vec<BackendNode>) -> Self {
        Self {
            backends,
            alpha: 0.2,
        }
    }
}

impl LoadBalancer for LeastResponseTime {
    fn next(&self) -> BackendNode {
        let best = self
            .backends
            .iter()
            .min_by(|a, b| {
                let a_latency = a.metrics.latency_us.load(Ordering::Relaxed);
                let b_latency = b.metrics.latency_us.load(Ordering::Relaxed);

                let a_connections = a.metrics.active_connections.load(Ordering::Relaxed);

                let b_connections = b.metrics.active_connections.load(Ordering::Relaxed);

                let a_score = a_latency.saturating_mul((a_connections + 1) as u64);
                let b_score = b_latency.saturating_mul((b_connections + 1) as u64);

                a_score.cmp(&b_score)
            })
            .unwrap();

        best.metrics
            .active_connections
            .fetch_add(1, Ordering::Relaxed);

        best.clone()
    }

    fn release(&self, backend: &BackendNode, feedback: Feedback) {
        backend
            .metrics
            .active_connections
            .fetch_sub(1, Ordering::Relaxed);

        if !feedback.success {
            return;
        }

        let measured = feedback.latency.as_micros() as u64;

        let old = backend.metrics.latency_us.load(Ordering::Relaxed);

        let alpha = 0.2;

        let new_latency = ((1.0 - alpha) * old as f64 + alpha * measured as f64) as u64;

        backend
            .metrics
            .latency_us
            .store(new_latency.max(1), Ordering::Relaxed);
    }
}
