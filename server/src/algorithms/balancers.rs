use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;

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
    fn next_excluding(&self, allow_unhealthy: bool, excluded: &[&str]) -> Option<BackendNode> {
        let eligible: Vec<&BackendNode> = self
            .backends
            .iter()
            .filter(|backend| {
                !excluded.contains(&backend.id.as_str())
                    && (allow_unhealthy || backend.healthy.load(Ordering::Relaxed))
            })
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
    fn next_excluding(&self, allow_unhealthy: bool, excluded: &[&str]) -> Option<BackendNode> {
        let mut state = self.state.lock().unwrap();

        let eligible: Vec<usize> = self
            .backends
            .iter()
            .enumerate()
            .filter(|(_, backend)| {
                !excluded.contains(&backend.id.as_str())
                    && (allow_unhealthy || backend.healthy.load(Ordering::Relaxed))
            })
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
    fn next_excluding(&self, allow_unhealthy: bool, excluded: &[&str]) -> Option<BackendNode> {
        self.backends
            .iter()
            .filter(|backend| {
                !excluded.contains(&backend.id.as_str())
                    && (allow_unhealthy || backend.healthy.load(Ordering::Relaxed))
            })
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

/// Default service-level objective used by [`AdaptiveBalancing`].
///
/// The policy can be constructed with a different target in focused studies,
/// while the normal server factory keeps existing configuration files valid by
/// using this default.
pub const DEFAULT_ADAPTIVE_DEADLINE: Duration = Duration::from_millis(200);

const ADAPTIVE_EWMA_ALPHA: f64 = 0.2;
const ADAPTIVE_EXPLORATION_STRENGTH: f64 = 0.25;
const ADAPTIVE_IN_FLIGHT_PENALTY: f64 = 0.05;
const ADAPTIVE_INITIAL_PROBABILITY: f64 = 0.5;
const ADAPTIVE_INITIAL_SAMPLES: u64 = 1;

#[derive(Clone, Debug)]
struct AdaptiveBackendState {
    observations: u64,
    selections: u64,
    in_flight: usize,
    deadline_success_probability: f64,
}

impl AdaptiveBackendState {
    fn new() -> Self {
        Self {
            observations: 0,
            selections: 0,
            in_flight: 0,
            deadline_success_probability: ADAPTIVE_INITIAL_PROBABILITY,
        }
    }

    fn score(&self, total_selections: u64) -> f64 {
        let exploration = ADAPTIVE_EXPLORATION_STRENGTH
            * (((total_selections + 1) as f64).ln() / (self.selections + 1) as f64).sqrt();
        let in_flight_penalty = ADAPTIVE_IN_FLIGHT_PENALTY * self.in_flight as f64;

        self.deadline_success_probability + exploration - in_flight_penalty
    }
}

#[derive(Debug)]
struct AdaptiveState {
    backends: Vec<AdaptiveBackendState>,
    total_selections: u64,
    cursor: usize,
}

/// Deadline-aware load balancer with deterministic UCB-style exploration.
///
/// Each backend learns an exponentially weighted probability of completing a
/// transport attempt successfully within `deadline`. A confidence bonus keeps
/// occasionally probing less-used backends so the policy can detect recovery,
/// while an in-flight penalty avoids concentrating concurrent warm-up traffic
/// on a single backend.
pub struct AdaptiveBalancing {
    backends: Vec<BackendNode>,
    deadline: Duration,
    state: Mutex<AdaptiveState>,
}

impl AdaptiveBalancing {
    pub fn new(backends: Vec<BackendNode>) -> Self {
        Self::with_deadline(backends, DEFAULT_ADAPTIVE_DEADLINE)
    }

    pub fn with_deadline(backends: Vec<BackendNode>, deadline: Duration) -> Self {
        assert!(!deadline.is_zero(), "adaptive deadline must be positive");
        let state = AdaptiveState {
            backends: (0..backends.len())
                .map(|_| AdaptiveBackendState::new())
                .collect(),
            total_selections: 0,
            cursor: 0,
        };

        Self {
            backends,
            deadline,
            state: Mutex::new(state),
        }
    }

    fn rotating_distance(cursor: usize, index: usize, backend_count: usize) -> usize {
        (index + backend_count - cursor) % backend_count
    }
}

impl LoadBalancer for AdaptiveBalancing {
    fn next_excluding(&self, allow_unhealthy: bool, excluded: &[&str]) -> Option<BackendNode> {
        let eligible: Vec<usize> = self
            .backends
            .iter()
            .enumerate()
            .filter(|(_, backend)| {
                !excluded.contains(&backend.id.as_str())
                    && (allow_unhealthy || backend.healthy.load(Ordering::Relaxed))
            })
            .map(|(index, _)| index)
            .collect();
        if eligible.is_empty() {
            return None;
        }

        let mut state = self.state.lock().unwrap();
        let cursor = state.cursor % self.backends.len();

        // Account for reservations as well as completed observations. Without
        // this, a concurrent cold start can send every request to backend zero
        // before the first response supplies feedback.
        let selected = eligible
            .iter()
            .copied()
            .filter(|&index| {
                let backend = &state.backends[index];
                backend.observations + (backend.in_flight as u64) < ADAPTIVE_INITIAL_SAMPLES
            })
            .min_by_key(|&index| Self::rotating_distance(cursor, index, self.backends.len()))
            .or_else(|| {
                eligible.iter().copied().max_by(|&left, &right| {
                    let left_score = state.backends[left].score(state.total_selections);
                    let right_score = state.backends[right].score(state.total_selections);
                    left_score.total_cmp(&right_score).then_with(|| {
                        let left_distance =
                            Self::rotating_distance(cursor, left, self.backends.len());
                        let right_distance =
                            Self::rotating_distance(cursor, right, self.backends.len());
                        right_distance.cmp(&left_distance)
                    })
                })
            })?;

        state.total_selections = state.total_selections.saturating_add(1);
        state.cursor = (selected + 1) % self.backends.len();
        state.backends[selected].selections = state.backends[selected].selections.saturating_add(1);
        state.backends[selected].in_flight = state.backends[selected].in_flight.saturating_add(1);

        Some(self.backends[selected].clone())
    }

    fn release(&self, backend: &BackendNode, feedback: Feedback) {
        let Some(index) = self
            .backends
            .iter()
            .position(|candidate| candidate.id == backend.id)
        else {
            return;
        };

        let mut state = self.state.lock().unwrap();
        let backend_state = &mut state.backends[index];
        backend_state.in_flight = backend_state.in_flight.saturating_sub(1);

        let met_deadline = feedback.success && feedback.latency <= self.deadline;
        let observation = if met_deadline { 1.0 } else { 0.0 };
        if backend_state.observations == 0 {
            backend_state.deadline_success_probability = observation;
        } else {
            backend_state.deadline_success_probability = (1.0 - ADAPTIVE_EWMA_ALPHA)
                * backend_state.deadline_success_probability
                + ADAPTIVE_EWMA_ALPHA * observation;
        }
        backend_state.observations = backend_state.observations.saturating_add(1);
    }

    fn name(&self) -> &'static str {
        "adaptive_balancing"
    }

    fn backends(&self) -> &[BackendNode] {
        &self.backends
    }
}

impl LeastResponseTime {
    pub fn new(backends: Vec<BackendNode>) -> Self {
        Self { backends }
    }
}

impl LoadBalancer for LeastResponseTime {
    fn next_excluding(&self, allow_unhealthy: bool, excluded: &[&str]) -> Option<BackendNode> {
        self.backends
            .iter()
            .filter(|backend| {
                !excluded.contains(&backend.id.as_str())
                    && (allow_unhealthy || backend.healthy.load(Ordering::Relaxed))
            })
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
