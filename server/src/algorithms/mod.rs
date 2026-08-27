use crate::algorithms::algorithms::{BackendNode, LeastConnections, LeastResponseTime, LoadBalancer, RoundRobin, WeightedRoundRobin};

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