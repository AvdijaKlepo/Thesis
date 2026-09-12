use arc_swap::ArcSwap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::{
    algorithms::algorithms::{
        BackendNode, LeastConnections, LeastResponseTime, LoadBalancer, RoundRobin,
        WeightedRoundRobin,
    },
    backend::registry::BackendRegistry,
};

pub mod algorithm_server;
pub mod algorithms;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AlgorithmKind {
    RoundRobin,
    WeightedRoundRobin,
    LeastConnections,
    LeastResponseTime,
}

impl AlgorithmKind {
    pub fn from_str_name(value: &str) -> Option<Self> {
        match value {
            "round_robin" => Some(Self::RoundRobin),
            "weighted_round_robin" => Some(Self::WeightedRoundRobin),
            "least_connections" => Some(Self::LeastConnections),
            "least_response_time" => Some(Self::LeastResponseTime),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::RoundRobin => "round_robin",
            Self::WeightedRoundRobin => "weighted_round_robin",
            Self::LeastConnections => "least_connections",
            Self::LeastResponseTime => "least_response_time",
        }
    }
}

pub fn create_load_balancer(
    algorithm: &str,
    backends: Vec<BackendNode>,
) -> Option<Box<dyn LoadBalancer>> {
    AlgorithmKind::from_str_name(algorithm).map(|kind| create_load_balancer_for(kind, backends))
}

pub fn create_load_balancer_for(
    algorithm: AlgorithmKind,
    backends: Vec<BackendNode>,
) -> Box<dyn LoadBalancer> {
    match algorithm {
        AlgorithmKind::RoundRobin => Box::new(RoundRobin::new(backends)),
        AlgorithmKind::WeightedRoundRobin => Box::new(WeightedRoundRobin::new(backends)),
        AlgorithmKind::LeastConnections => Box::new(LeastConnections::new(backends)),
        AlgorithmKind::LeastResponseTime => Box::new(LeastResponseTime::new(backends)),
    }
}

/// Updates the active load balancer held in `lb_slot` with `backends` while preserving the active algorithm.
pub fn sync_load_balancer_backends(
    lb_slot: &ArcSwap<Box<dyn LoadBalancer>>,
    backends: Vec<BackendNode>,
) {
    let current_algo = lb_slot.load().name();
    if let Some(new_lb) = create_load_balancer(current_algo, backends) {
        lb_slot.store(Arc::new(new_lb));
    }
}

/// Syncs the active load balancer in `lb_slot` with all current backends from the registry.
pub fn sync_load_balancer(lb_slot: &ArcSwap<Box<dyn LoadBalancer>>, registry: &BackendRegistry) {
    sync_load_balancer_backends(lb_slot, registry.all());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Backend;
    use std::sync::atomic::Ordering;

    fn test_node(id: &str, port: u16, weight: usize) -> BackendNode {
        BackendNode::new(Backend {
            id: id.into(),
            address: format!("127.0.0.1:{port}"),
            weight,
        })
    }

    #[test]
    fn algorithm_kind_round_trips_through_its_name() {
        let kinds = [
            AlgorithmKind::RoundRobin,
            AlgorithmKind::WeightedRoundRobin,
            AlgorithmKind::LeastConnections,
            AlgorithmKind::LeastResponseTime,
        ];

        for kind in kinds {
            assert_eq!(AlgorithmKind::from_str_name(kind.as_str()), Some(kind));
        }
        assert_eq!(AlgorithmKind::from_str_name("unknown"), None);
    }

    #[test]
    fn test_sync_load_balancer_updates_backends_without_algorithm_switch() {
        let registry = BackendRegistry::new();
        registry.add(test_node("1", 8081, 1));
        registry.add(test_node("2", 8082, 1));

        let lb = create_load_balancer("round_robin", registry.all()).unwrap();
        let lb_slot = ArcSwap::from_pointee(lb);

        // Before adding backend "3", only "1" and "2" are selected
        assert_eq!(lb_slot.load().next(false).unwrap().id, "1");
        assert_eq!(lb_slot.load().next(false).unwrap().id, "2");
        assert_eq!(lb_slot.load().next(false).unwrap().id, "1");

        // Add backend "3" to the registry
        registry.add(test_node("3", 8083, 1));

        // Before syncing, active balancer still holds the old 2-backend vector
        assert_eq!(lb_slot.load().backends().len(), 2);

        // Sync load balancer with registry
        sync_load_balancer(&lb_slot, &registry);

        // After syncing, active balancer now holds all 3 backends and maintains "round_robin"
        assert_eq!(lb_slot.load().name(), "round_robin");
        assert_eq!(lb_slot.load().backends().len(), 3);

        let ids: Vec<String> = (0..3)
            .map(|_| lb_slot.load().next(false).unwrap().id.clone())
            .collect();
        assert!(ids.contains(&"1".to_string()));
        assert!(ids.contains(&"2".to_string()));
        assert!(ids.contains(&"3".to_string()));
    }

    #[test]
    fn test_sync_load_balancer_weighted_round_robin() {
        let registry = BackendRegistry::new();
        registry.add(test_node("1", 8081, 2));
        registry.add(test_node("2", 8082, 2));

        let lb = create_load_balancer("weighted_round_robin", registry.all()).unwrap();
        let lb_slot = ArcSwap::from_pointee(lb);

        // Add backend "3" with weight 4
        registry.add(test_node("3", 8083, 4));
        sync_load_balancer(&lb_slot, &registry);

        assert_eq!(lb_slot.load().name(), "weighted_round_robin");
        assert_eq!(lb_slot.load().backends().len(), 3);

        let mut counts = std::collections::HashMap::new();
        for _ in 0..800 {
            let node = lb_slot.load().next(false).unwrap();
            *counts.entry(node.id.clone()).or_insert(0) += 1;
        }

        assert_eq!(counts.get("1"), Some(&200));
        assert_eq!(counts.get("2"), Some(&200));
        assert_eq!(counts.get("3"), Some(&400));
    }

    #[test]
    fn test_sync_load_balancer_least_connections() {
        let registry = BackendRegistry::new();
        let n1 = test_node("1", 8081, 1);
        let n2 = test_node("2", 8082, 1);
        n1.metrics.active_connections.store(5, Ordering::Relaxed);
        n2.metrics.active_connections.store(3, Ordering::Relaxed);
        registry.add(n1);
        registry.add(n2);

        let lb = create_load_balancer("least_connections", registry.all()).unwrap();
        let lb_slot = ArcSwap::from_pointee(lb);

        assert_eq!(lb_slot.load().next(false).unwrap().id, "2");

        // Add backend "3" with 0 active connections
        let n3 = test_node("3", 8083, 1);
        registry.add(n3);
        sync_load_balancer(&lb_slot, &registry);

        assert_eq!(lb_slot.load().name(), "least_connections");
        assert_eq!(lb_slot.load().next(false).unwrap().id, "3");
    }
}
