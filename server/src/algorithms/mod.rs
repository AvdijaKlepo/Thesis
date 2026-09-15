use serde::{Deserialize, Serialize};

use crate::algorithms::balancers::{
    BackendNode, LeastConnections, LeastResponseTime, LoadBalancer, RoundRobin, WeightedRoundRobin,
};

pub mod balancers;

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

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
