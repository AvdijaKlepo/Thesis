use std::sync::{
    Mutex,
    atomic::Ordering,
};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::{
    algorithms::balancers::{
        AdaptiveBackendDiagnostic, AdaptiveDiagnosticSnapshot, BackendNode, LoadBalancer,
        unix_timestamp_ms,
    },
    backend::model::Feedback,
};

/// Runtime-tunable settings for the second adaptive balancing policy.
///
/// The type is shared by configuration, management, and the balancer so the
/// defaults and validation rules cannot drift between those surfaces.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct AdaptiveV2Settings {
    pub deadline_ms: u64,
    pub ewma_alpha: f64,
    pub slo_weight: f64,
    pub probe_interval_per_backend: u64,
    pub in_flight_penalty: f64,
}

impl Default for AdaptiveV2Settings {
    fn default() -> Self {
        Self {
            deadline_ms: 200,
            ewma_alpha: 0.2,
            slo_weight: 0.7,
            probe_interval_per_backend: 32,
            in_flight_penalty: 0.05,
        }
    }
}

impl AdaptiveV2Settings {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.deadline_ms == 0 {
            return Err("deadline_ms must be greater than zero");
        }
        if !self.ewma_alpha.is_finite() || self.ewma_alpha <= 0.0 || self.ewma_alpha > 1.0 {
            return Err("ewma_alpha must be finite and in (0, 1]");
        }
        if !self.slo_weight.is_finite() || !(0.0..=1.0).contains(&self.slo_weight) {
            return Err("slo_weight must be finite and in [0, 1]");
        }
        if self.probe_interval_per_backend == 0 {
            return Err("probe_interval_per_backend must be greater than zero");
        }
        if !self.in_flight_penalty.is_finite() || self.in_flight_penalty < 0.0 {
            return Err("in_flight_penalty must be finite and non-negative");
        }
        Ok(())
    }

    pub fn deadline(&self) -> Duration {
        Duration::from_millis(self.deadline_ms)
    }
}

const ADAPTIVE_V2_INITIAL_REWARD: f64 = 0.5;
const ADAPTIVE_V2_STALENESS_BONUS: f64 = 0.1;

#[derive(Clone, Debug)]
pub(crate) struct AdaptiveV2BackendState {
    pub(crate) observations: u64,
    pub(crate) selections: u64,
    pub(crate) in_flight: usize,
    pub(crate) deadline_success_probability: f64,
    pub(crate) latency_utility: f64,
    pub(crate) last_selected: u64,
}

impl AdaptiveV2BackendState {
    fn new() -> Self {
        Self {
            observations: 0,
            selections: 0,
            in_flight: 0,
            deadline_success_probability: ADAPTIVE_V2_INITIAL_REWARD,
            latency_utility: ADAPTIVE_V2_INITIAL_REWARD,
            last_selected: 0,
        }
    }

    pub(crate) fn score(
        &self,
        settings: AdaptiveV2Settings,
        weight: usize,
        total_selections: u64,
        probe_span: u64,
    ) -> f64 {
        let reward = settings.slo_weight * self.deadline_success_probability
            + (1.0 - settings.slo_weight) * self.latency_utility;
        let age = total_selections.saturating_sub(self.last_selected);
        let staleness =
            (age as f64 / probe_span.max(1) as f64).min(1.0) * ADAPTIVE_V2_STALENESS_BONUS;
        let capacity = weight.max(1) as f64;
        let in_flight_penalty = settings.in_flight_penalty * self.in_flight as f64 / capacity;
        (reward + staleness - in_flight_penalty).clamp(-1.0e12, 1.0e12)
    }
}

#[derive(Debug)]
pub(crate) struct AdaptiveV2State {
    pub(crate) backends: Vec<AdaptiveV2BackendState>,
    pub(crate) total_selections: u64,
    pub(crate) cursor: usize,
}

/// Adaptive policy using a bounded staleness probe and a continuous latency
/// utility. It intentionally has a distinct name and state from v1 so existing
/// experiments remain reproducible.
pub struct AdaptiveBalancingV2 {
    backends: Vec<BackendNode>,
    settings: AdaptiveV2Settings,
    pub(crate) state: Mutex<AdaptiveV2State>,
}

impl AdaptiveBalancingV2 {
    pub fn new(backends: Vec<BackendNode>) -> Self {
        Self::with_settings(backends, AdaptiveV2Settings::default())
    }

    pub fn with_settings(backends: Vec<BackendNode>, settings: AdaptiveV2Settings) -> Self {
        Self::try_with_settings(backends, settings).expect("invalid adaptive_balancing_v2 settings")
    }

    pub fn try_with_settings(
        backends: Vec<BackendNode>,
        settings: AdaptiveV2Settings,
    ) -> Result<Self, &'static str> {
        settings.validate()?;
        let state = AdaptiveV2State {
            backends: (0..backends.len())
                .map(|_| AdaptiveV2BackendState::new())
                .collect(),
            total_selections: 0,
            cursor: 0,
        };
        Ok(Self {
            backends,
            settings,
            state: Mutex::new(state),
        })
    }

    pub fn settings(&self) -> AdaptiveV2Settings {
        self.settings
    }

    fn rotating_distance(cursor: usize, index: usize, backend_count: usize) -> usize {
        (index + backend_count - cursor) % backend_count
    }
}

impl LoadBalancer for AdaptiveBalancingV2 {
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
        let eligible_count = eligible.len() as u64;
        let probe_span = self
            .settings
            .probe_interval_per_backend
            .saturating_mul(eligible_count)
            .max(1);

        // Reserve each backend once during cold start, including concurrent
        // selections whose feedback has not arrived yet.
        let selected = eligible
            .iter()
            .copied()
            .filter(|&index| {
                let backend = &state.backends[index];
                backend
                    .observations
                    .saturating_add(backend.in_flight as u64)
                    == 0
            })
            .min_by_key(|&index| Self::rotating_distance(cursor, index, self.backends.len()))
            .or_else(|| {
                let stale = eligible.iter().copied().filter(|&index| {
                    state
                        .total_selections
                        .saturating_sub(state.backends[index].last_selected)
                        >= probe_span
                });
                stale.max_by(|&left, &right| {
                    let left_age = state
                        .total_selections
                        .saturating_sub(state.backends[left].last_selected);
                    let right_age = state
                        .total_selections
                        .saturating_sub(state.backends[right].last_selected);
                    left_age.cmp(&right_age).then_with(|| {
                        let left_distance =
                            Self::rotating_distance(cursor, left, self.backends.len());
                        let right_distance =
                            Self::rotating_distance(cursor, right, self.backends.len());
                        right_distance.cmp(&left_distance)
                    })
                })
            })
            .or_else(|| {
                eligible.iter().copied().max_by(|&left, &right| {
                    let left_score = state.backends[left].score(
                        self.settings,
                        self.backends[left].weight,
                        state.total_selections,
                        probe_span,
                    );
                    let right_score = state.backends[right].score(
                        self.settings,
                        self.backends[right].weight,
                        state.total_selections,
                        probe_span,
                    );
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
        let backend_state = &mut state.backends[selected];
        backend_state.selections = backend_state.selections.saturating_add(1);
        backend_state.in_flight = backend_state.in_flight.saturating_add(1);
        backend_state.last_selected = selection_number;
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
        let alpha = self.settings.ewma_alpha;
        let met_deadline = feedback.success && feedback.latency <= self.settings.deadline();
        let deadline_sample = if met_deadline { 1.0 } else { 0.0 };
        let latency_sample = if feedback.success {
            let ratio = feedback.latency.as_secs_f64() / self.settings.deadline().as_secs_f64();
            (1.0 / (1.0 + ratio)).clamp(0.0, 1.0)
        } else {
            0.0
        };
        if backend_state.observations == 0 {
            backend_state.deadline_success_probability = deadline_sample;
            backend_state.latency_utility = latency_sample;
        } else {
            backend_state.deadline_success_probability = (1.0 - alpha)
                * backend_state.deadline_success_probability
                + alpha * deadline_sample;
            backend_state.latency_utility =
                (1.0 - alpha) * backend_state.latency_utility + alpha * latency_sample;
        }
        backend_state.observations = backend_state.observations.saturating_add(1);
    }

    fn name(&self) -> &'static str {
        "adaptive_balancing_v2"
    }

    fn backends(&self) -> &[BackendNode] {
        &self.backends
    }

    fn adaptive_diagnostics(&self) -> Option<AdaptiveDiagnosticSnapshot> {
        let state = self.state.lock().unwrap();
        let probe_span = self
            .settings
            .probe_interval_per_backend
            .saturating_mul(self.backends.len() as u64)
            .max(1);
        let backends = self
            .backends
            .iter()
            .zip(&state.backends)
            .map(|(backend, backend_state)| {
                let score = backend_state.score(
                    self.settings,
                    backend.weight,
                    state.total_selections,
                    probe_span,
                );
                let age = state
                    .total_selections
                    .saturating_sub(backend_state.last_selected);
                let staleness_bonus = (age as f64 / probe_span as f64).min(1.0)
                    * ADAPTIVE_V2_STALENESS_BONUS;
                AdaptiveBackendDiagnostic {
                    backend_id: backend.id.clone(),
                    observations: backend_state.observations,
                    selections: backend_state.selections,
                    last_selection_generation: backend_state.last_selected,
                    deadline_hit_ewma: backend_state.deadline_success_probability,
                    latency_utility_ewma: Some(backend_state.latency_utility),
                    staleness_bonus,
                    in_flight: backend_state.in_flight,
                    score,
                }
            })
            .collect();
        Some(AdaptiveDiagnosticSnapshot {
            algorithm: self.name().into(),
            settings: Some(self.settings),
            total_selections: state.total_selections,
            snapshot_unix_ms: unix_timestamp_ms(),
            backends,
        })
    }
}
