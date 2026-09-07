use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
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

        best.clone()
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

        best.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_node(id: &str, port: u16, weight: usize) -> BackendNode {
        BackendNode::new(Backend {
            id: id.into(),
            address: format!("127.0.0.1:{port}"),
            weight,
        })
    }

    #[test]
    fn test_least_connections_selects_minimum() {
        let n1 = test_node("1", 8081, 1);
        let n2 = test_node("2", 8082, 1);
        let n3 = test_node("3", 8083, 1);

        n1.metrics.active_connections.store(3, Ordering::Relaxed);
        n2.metrics.active_connections.store(0, Ordering::Relaxed);
        n3.metrics.active_connections.store(2, Ordering::Relaxed);

        let lb = LeastConnections::new(vec![n1.clone(), n2.clone(), n3.clone()]);
        let selected = lb.next();
        assert_eq!(selected.id, "2");

        // When node 2 gets more connections, node 3 should be selected next
        n2.metrics.active_connections.store(4, Ordering::Relaxed);
        let selected2 = lb.next();
        assert_eq!(selected2.id, "3");
    }

    #[test]
    fn test_round_robin() {
        let n1 = test_node("1", 8081, 1);
        let n2 = test_node("2", 8082, 1);
        let lb = RoundRobin::new(vec![n1.clone(), n2.clone()]);

        assert_eq!(lb.next().id, "1");
        assert_eq!(lb.next().id, "2");
        assert_eq!(lb.next().id, "1");
    }
}

