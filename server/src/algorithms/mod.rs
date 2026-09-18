use serde::{Deserialize, Serialize};

use crate::algorithms::balancers::{
    AdaptiveBalancing, AdaptiveBalancingV2, BackendNode, LeastConnections, LeastResponseTime,
    LoadBalancer, RoundRobin, WeightedRoundRobin,
};

pub use crate::algorithms::balancers::AdaptiveV2Settings;

pub mod balancers;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AlgorithmKind {
    RoundRobin,
    WeightedRoundRobin,
    LeastConnections,
    LeastResponseTime,
    AdaptiveBalancing,
    AdaptiveBalancingV2,
}

impl AlgorithmKind {
    pub fn from_str_name(value: &str) -> Option<Self> {
        match value {
            "round_robin" => Some(Self::RoundRobin),
            "weighted_round_robin" => Some(Self::WeightedRoundRobin),
            "least_connections" => Some(Self::LeastConnections),
            "least_response_time" => Some(Self::LeastResponseTime),
            "adaptive_balancing" => Some(Self::AdaptiveBalancing),
            "adaptive_balancing_v2" => Some(Self::AdaptiveBalancingV2),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::RoundRobin => "round_robin",
            Self::WeightedRoundRobin => "weighted_round_robin",
            Self::LeastConnections => "least_connections",
            Self::LeastResponseTime => "least_response_time",
            Self::AdaptiveBalancing => "adaptive_balancing",
            Self::AdaptiveBalancingV2 => "adaptive_balancing_v2",
        }
    }
}

pub fn create_load_balancer_for(
    algorithm: AlgorithmKind,
    backends: Vec<BackendNode>,
) -> Box<dyn LoadBalancer> {
    create_load_balancer_for_with_adaptive_v2_settings(
        algorithm,
        backends,
        AdaptiveV2Settings::default(),
    )
}

pub fn create_load_balancer_for_with_adaptive_v2_settings(
    algorithm: AlgorithmKind,
    backends: Vec<BackendNode>,
    adaptive_v2_settings: AdaptiveV2Settings,
) -> Box<dyn LoadBalancer> {
    match algorithm {
        AlgorithmKind::RoundRobin => Box::new(RoundRobin::new(backends)),
        AlgorithmKind::WeightedRoundRobin => Box::new(WeightedRoundRobin::new(backends)),
        AlgorithmKind::LeastConnections => Box::new(LeastConnections::new(backends)),
        AlgorithmKind::LeastResponseTime => Box::new(LeastResponseTime::new(backends)),
        AlgorithmKind::AdaptiveBalancing => Box::new(AdaptiveBalancing::new(backends)),
        AlgorithmKind::AdaptiveBalancingV2 => Box::new(AdaptiveBalancingV2::with_settings(
            backends,
            adaptive_v2_settings,
        )),
    }
}

#[cfg(test)]
#[path = "tests/mod_tests.rs"]
mod tests;
