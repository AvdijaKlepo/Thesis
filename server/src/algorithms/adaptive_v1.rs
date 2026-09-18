use std::sync::{
    Mutex,
    atomic::Ordering,
};
use std::time::Duration;

use crate::{
    algorithms::balancers::{
        AdaptiveBackendDiagnostic, AdaptiveDiagnosticSnapshot, BackendNode, LoadBalancer,
        unix_timestamp_ms,
    },
    backend::model::Feedback,
};

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
    last_selected: u64,
}

impl AdaptiveBackendState {
    fn new() -> Self {
        Self {
            observations: 0,
            selections: 0,
            in_flight: 0,
            deadline_success_probability: ADAPTIVE_INITIAL_PROBABILITY,
            last_selected: 0,
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
        let selection_number = state.total_selections;
        state.cursor = (selected + 1) % self.backends.len();
        state.backends[selected].selections = state.backends[selected].selections.saturating_add(1);
        state.backends[selected].in_flight = state.backends[selected].in_flight.saturating_add(1);
        state.backends[selected].last_selected = selection_number;

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

    fn adaptive_diagnostics(&self) -> Option<AdaptiveDiagnosticSnapshot> {
        let state = self.state.lock().unwrap();
        let backends = self
            .backends
            .iter()
            .zip(&state.backends)
            .map(|(backend, backend_state)| {
                let score = backend_state.score(state.total_selections);
                let staleness_bonus = (score - backend_state.deadline_success_probability
                    + ADAPTIVE_IN_FLIGHT_PENALTY * backend_state.in_flight as f64)
                    .max(0.0);
                AdaptiveBackendDiagnostic {
                    backend_id: backend.id.clone(),
                    observations: backend_state.observations,
                    selections: backend_state.selections,
                    last_selection_generation: backend_state.last_selected,
                    deadline_hit_ewma: backend_state.deadline_success_probability,
                    latency_utility_ewma: None,
                    staleness_bonus,
                    in_flight: backend_state.in_flight,
                    score,
                }
            })
            .collect();
        Some(AdaptiveDiagnosticSnapshot {
            algorithm: self.name().into(),
            settings: None,
            total_selections: state.total_selections,
            snapshot_unix_ms: unix_timestamp_ms(),
            backends,
        })
    }
}
