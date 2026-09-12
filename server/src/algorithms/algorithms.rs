use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use crate::{
    Backend,
    backend::backend_server::{BackendMetrics, Feedback},
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

pub struct LatencyBalancer {
    backends: Vec<BackendNode>,
}
pub fn default_backends() -> Vec<BackendNode> {
    vec![
        BackendNode::new(Backend {
            id: 1.to_string(),
            address: "127.0.0.1:8081".into(),
            weight: 3,
        }),
        BackendNode::new(Backend {
            id: 2.to_string(),
            address: "127.0.0.1:8082".into(),
            weight: 1,
        }),
        BackendNode::new(Backend {
            id: 3.to_string(),
            address: "127.0.0.1:8083".into(),
            weight: 1,
        }),
    ]
}
pub trait LoadBalancer: Send + Sync {
    fn next(&self) -> BackendNode;

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
    fn next(&self) -> BackendNode {
        assert!(!self.backends.is_empty(), "backends must not be empty");
        let healthy: Vec<&BackendNode> = self
            .backends
            .iter()
            .filter(|b| b.healthy.load(Ordering::Relaxed))
            .collect();

        let pool = if healthy.is_empty() {
            &self.backends[..]
        } else {
            let i = self.counter.fetch_add(1, Ordering::Relaxed) % healthy.len();
            return (*healthy[i]).clone();
        };

        let i = self.counter.fetch_add(1, Ordering::Relaxed) % pool.len();
        pool[i].clone()
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
    fn next(&self) -> BackendNode {
        assert!(!self.backends.is_empty(), "backends must not be empty");
        let mut state = self.state.lock().unwrap();

        let any_healthy = self
            .backends
            .iter()
            .any(|b| b.healthy.load(Ordering::Relaxed));

        let total: i64 = self
            .backends
            .iter()
            .enumerate()
            .filter(|(_, b)| !any_healthy || b.healthy.load(Ordering::Relaxed))
            .map(|(idx, _)| state[idx].effective_weight)
            .sum();

        for (idx, b) in self.backends.iter().enumerate() {
            if !any_healthy || b.healthy.load(Ordering::Relaxed) {
                state[idx].current += state[idx].effective_weight;
            }
        }

        let best = self
            .backends
            .iter()
            .enumerate()
            .filter(|(_, b)| !any_healthy || b.healthy.load(Ordering::Relaxed))
            .max_by_key(|&(idx, _)| state[idx].current)
            .map(|(idx, _)| idx)
            .unwrap_or(0);

        state[best].current -= total;
        self.backends[best].clone()
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
    fn next(&self) -> BackendNode {
        assert!(!self.backends.is_empty(), "backends must not be empty");
        let any_healthy = self
            .backends
            .iter()
            .any(|b| b.healthy.load(Ordering::Relaxed));

        let best = self
            .backends
            .iter()
            .filter(|b| !any_healthy || b.healthy.load(Ordering::Relaxed))
            .min_by_key(|b| b.metrics.active_connections.load(Ordering::Relaxed))
            .unwrap();

        best.clone()
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
    fn next(&self) -> BackendNode {
        assert!(!self.backends.is_empty(), "backends must not be empty");
        let any_healthy = self
            .backends
            .iter()
            .any(|b| b.healthy.load(Ordering::Relaxed));

        let best = self
            .backends
            .iter()
            .filter(|b| !any_healthy || b.healthy.load(Ordering::Relaxed))
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

    fn name(&self) -> &'static str {
        "least_response_time"
    }

    fn backends(&self) -> &[BackendNode] {
        &self.backends
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

    #[test]
    fn test_weighted_round_robin_distribution() {
        let n1 = test_node("1", 8081, 5);
        let n2 = test_node("2", 8082, 2);
        let n3 = test_node("3", 8083, 3);

        let lb = WeightedRoundRobin::new(vec![n1, n2, n3]);
        let mut counts = std::collections::HashMap::new();

        for _ in 0..1000 {
            let selected = lb.next();
            *counts.entry(selected.id.clone()).or_insert(0) += 1;
        }

        assert_eq!(counts.get("1"), Some(&500));
        assert_eq!(counts.get("2"), Some(&200));
        assert_eq!(counts.get("3"), Some(&300));
    }

    #[test]
    fn test_weighted_round_robin_default_backends_distribution() {
        let backends = default_backends(); // weights: 3, 1, 1
        let lb = WeightedRoundRobin::new(backends);
        let mut counts = std::collections::HashMap::new();

        for _ in 0..500 {
            let selected = lb.next();
            *counts.entry(selected.id.clone()).or_insert(0) += 1;
        }

        assert_eq!(counts.get("1"), Some(&300));
        assert_eq!(counts.get("2"), Some(&100));
        assert_eq!(counts.get("3"), Some(&100));
    }

    #[test]
    fn test_weighted_round_robin_smooth_interleaving() {
        let n1 = test_node("1", 8081, 3);
        let n2 = test_node("2", 8082, 1);
        let n3 = test_node("3", 8083, 1);

        let lb = WeightedRoundRobin::new(vec![n1, n2, n3]);
        let sequence: Vec<String> = (0..5).map(|_| lb.next().id.clone()).collect();

        // 3 + 1 + 1 = 5 selections: node 1 appears 3 times, nodes 2 and 3 appear 1 time each
        let count_1 = sequence.iter().filter(|id| *id == "1").count();
        let count_2 = sequence.iter().filter(|id| *id == "2").count();
        let count_3 = sequence.iter().filter(|id| *id == "3").count();
        assert_eq!(count_1, 3);
        assert_eq!(count_2, 1);
        assert_eq!(count_3, 1);

        // Smooth distribution: node 1 should not hog all 3 selections at the beginning
        assert_eq!(sequence, vec!["1", "3", "1", "2", "1"]);
    }

    #[test]
    fn test_weighted_round_robin_zero_weight() {
        let n1 = test_node("1", 8081, 4);
        let n2 = test_node("2", 8082, 0);

        let lb = WeightedRoundRobin::new(vec![n1, n2]);
        let mut counts = std::collections::HashMap::new();

        for _ in 0..100 {
            let selected = lb.next();
            *counts.entry(selected.id.clone()).or_insert(0) += 1;
        }

        assert_eq!(counts.get("1"), Some(&100));
        assert_eq!(counts.get("2"), None);
    }

    #[test]
    fn test_load_balancer_names_and_backends() {
        let n1 = test_node("1", 8081, 1);
        let n2 = test_node("2", 8082, 2);

        let rr = RoundRobin::new(vec![n1.clone(), n2.clone()]);
        assert_eq!(rr.name(), "round_robin");
        assert_eq!(rr.backends().len(), 2);

        let wrr = WeightedRoundRobin::new(vec![n1.clone(), n2.clone()]);
        assert_eq!(wrr.name(), "weighted_round_robin");
        assert_eq!(wrr.backends().len(), 2);

        let lc = LeastConnections::new(vec![n1.clone(), n2.clone()]);
        assert_eq!(lc.name(), "least_connections");
        assert_eq!(lc.backends().len(), 2);

        let lrt = LeastResponseTime::new(vec![n1.clone(), n2.clone()]);
        assert_eq!(lrt.name(), "least_response_time");
        assert_eq!(lrt.backends().len(), 2);
    }

    #[test]
    fn test_load_balancers_skip_unhealthy_backends() {
        let n1 = test_node("1", 8081, 1);
        let n2 = test_node("2", 8082, 1);
        let n3 = test_node("3", 8083, 1);

        // Mark node 2 as unhealthy
        n2.healthy.store(false, Ordering::Relaxed);

        let rr = RoundRobin::new(vec![n1.clone(), n2.clone(), n3.clone()]);
        let mut rr_seen = std::collections::HashSet::new();
        for _ in 0..10 {
            rr_seen.insert(rr.next().id.clone());
        }
        assert!(rr_seen.contains("1"));
        assert!(!rr_seen.contains("2"));
        assert!(rr_seen.contains("3"));

        let wrr = WeightedRoundRobin::new(vec![n1.clone(), n2.clone(), n3.clone()]);
        let mut wrr_seen = std::collections::HashSet::new();
        for _ in 0..10 {
            wrr_seen.insert(wrr.next().id.clone());
        }
        assert!(wrr_seen.contains("1"));
        assert!(!wrr_seen.contains("2"));
        assert!(wrr_seen.contains("3"));

        // Least connections: even if node 2 has 0 connections, it's unhealthy so skip it
        n1.metrics.active_connections.store(5, Ordering::Relaxed);
        n2.metrics.active_connections.store(0, Ordering::Relaxed);
        n3.metrics.active_connections.store(2, Ordering::Relaxed);
        let lc = LeastConnections::new(vec![n1.clone(), n2.clone(), n3.clone()]);
        assert_eq!(lc.next().id, "3");
    }

    #[test]
    fn test_load_balancers_fallback_when_all_unhealthy() {
        let n1 = test_node("1", 8081, 1);
        let n2 = test_node("2", 8082, 1);

        n1.healthy.store(false, Ordering::Relaxed);
        n2.healthy.store(false, Ordering::Relaxed);

        let rr = RoundRobin::new(vec![n1.clone(), n2.clone()]);
        // When all are unhealthy, graceful degradation selects from all backends without panicking
        let id1 = rr.next().id.clone();
        let id2 = rr.next().id.clone();
        assert!(id1 == "1" || id1 == "2");
        assert!(id2 == "1" || id2 == "2");

        let wrr = WeightedRoundRobin::new(vec![n1.clone(), n2.clone()]);
        let wid = wrr.next().id.clone();
        assert!(wid == "1" || wid == "2");
    }
}
