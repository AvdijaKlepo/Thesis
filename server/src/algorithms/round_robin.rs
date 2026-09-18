use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

use crate::algorithms::balancers::{BackendNode, LoadBalancer};

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
