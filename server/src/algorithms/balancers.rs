use std::sync::{
    Arc,
    atomic::AtomicBool,
};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

use crate::{
    Backend,
    backend::model::{BackendMetrics, Feedback},
};

pub use super::round_robin::{RoundRobin, WeightedRoundRobin};
pub use super::least_connections::{LeastConnections, LeastResponseTime};
pub use super::adaptive_v1::{AdaptiveBalancing, DEFAULT_ADAPTIVE_DEADLINE};
pub use super::adaptive_v2::{AdaptiveBalancingV2, AdaptiveV2Settings};
pub(crate) use super::adaptive_v2::AdaptiveV2BackendState;
pub use std::sync::atomic::Ordering;
pub use std::time::Duration;

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
    fn next(&self, allow_unhealthy: bool) -> Option<BackendNode> {
        self.next_excluding(allow_unhealthy, &[])
    }

    /// Selects the next eligible backend, excluding backends whose IDs are in `excluded`.
    fn next_excluding(&self, allow_unhealthy: bool, excluded: &[&str]) -> Option<BackendNode>;

    fn release(&self, _backend: &BackendNode, _feedback: Feedback) {}

    fn name(&self) -> &'static str;

    fn backends(&self) -> &[BackendNode] {
        &[]
    }

    fn adaptive_diagnostics(&self) -> Option<AdaptiveDiagnosticSnapshot> {
        None
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct AdaptiveBackendDiagnostic {
    pub backend_id: String,
    pub observations: u64,
    pub selections: u64,
    pub last_selection_generation: u64,
    pub deadline_hit_ewma: f64,
    pub latency_utility_ewma: Option<f64>,
    pub staleness_bonus: f64,
    pub in_flight: usize,
    pub score: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct AdaptiveDiagnosticSnapshot {
    pub algorithm: String,
    pub settings: Option<AdaptiveV2Settings>,
    pub total_selections: u64,
    pub snapshot_unix_ms: u64,
    pub backends: Vec<AdaptiveBackendDiagnostic>,
}

pub(crate) fn unix_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
#[path = "tests/balancers_tests.rs"]
mod tests;
