use std::sync::Arc;
use arc_swap::ArcSwap;

use crate::{
    algorithms::algorithms::{
        BackendNode, LeastConnections, LeastResponseTime, LoadBalancer, RoundRobin,
        WeightedRoundRobin,
    },
    backend::registry::BackendRegistry,
};

pub mod algorithms;
pub mod algorithm_server;

pub fn create_load_balancer(
    algorithm: &str,
    backends: Vec<BackendNode>,
) -> Option<Box<dyn LoadBalancer>> {
    match algorithm {
        "round_robin" => Some(Box::new(RoundRobin::new(backends))),
        "weighted_round_robin" => Some(Box::new(WeightedRoundRobin::new(backends))),
        "least_connections" => Some(Box::new(LeastConnections::new(backends))),
        "least_response_time" => Some(Box::new(LeastResponseTime::new(backends))),
        _ => None,
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
pub fn sync_load_balancer(
    lb_slot: &ArcSwap<Box<dyn LoadBalancer>>,
    registry: &BackendRegistry,
) {
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
    fn test_sync_load_balancer_updates_backends_without_algorithm_switch() {
        let registry = BackendRegistry::new();
        registry.add(test_node("1", 8081, 1));
        registry.add(test_node("2", 8082, 1));

        let lb = create_load_balancer("round_robin", registry.all()).unwrap();
        let lb_slot = ArcSwap::from_pointee(lb);

        // Before adding backend "3", only "1" and "2" are selected
        assert_eq!(lb_slot.load().next().id, "1");
        assert_eq!(lb_slot.load().next().id, "2");
        assert_eq!(lb_slot.load().next().id, "1");

        // Add backend "3" to the registry
        registry.add(test_node("3", 8083, 1));

        // Before syncing, active balancer still holds the old 2-backend vector
        assert_eq!(lb_slot.load().backends().len(), 2);

        // Sync load balancer with registry
        sync_load_balancer(&lb_slot, &registry);

        // After syncing, active balancer now holds all 3 backends and maintains "round_robin"
        assert_eq!(lb_slot.load().name(), "round_robin");
        assert_eq!(lb_slot.load().backends().len(), 3);

        let ids: Vec<String> = (0..3).map(|_| lb_slot.load().next().id.clone()).collect();
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
            let node = lb_slot.load().next();
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

        assert_eq!(lb_slot.load().next().id, "2");

        // Add backend "3" with 0 active connections
        let n3 = test_node("3", 8083, 1);
        registry.add(n3);
        sync_load_balancer(&lb_slot, &registry);

        assert_eq!(lb_slot.load().name(), "least_connections");
        assert_eq!(lb_slot.load().next().id, "3");
    }
}