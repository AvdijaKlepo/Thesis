use std::sync::RwLock;

use crate::algorithms::balancers::BackendNode;

#[derive(Debug)]
pub struct BackendRegistry {
    backends: RwLock<Vec<BackendNode>>,
}

impl BackendRegistry {
    pub fn new() -> Self {
        Self {
            backends: RwLock::new(Vec::new()),
        }
    }

    pub fn add(&self, backend: BackendNode) {
        let mut backends = self.backends.write().unwrap();
        backends.push(backend);
    }

    pub fn remove(&self, id: &str) -> Option<BackendNode> {
        let mut backends = self.backends.write().unwrap();

        let index = backends.iter().position(|backend| backend.id == id)?;

        Some(backends.remove(index))
    }

    pub fn replace(&self, id: &str, backend: BackendNode) -> Option<BackendNode> {
        let mut backends = self.backends.write().unwrap();
        let index = backends.iter().position(|candidate| candidate.id == id)?;
        Some(std::mem::replace(&mut backends[index], backend))
    }

    pub fn get(&self, id: &str) -> Option<BackendNode> {
        let backends = self.backends.read().unwrap();

        backends.iter().find(|backend| backend.id == id).cloned()
    }

    pub fn all(&self) -> Vec<BackendNode> {
        let backends = self.backends.read().unwrap();
        backends.clone()
    }

    pub fn next_id(&self) -> usize {
        let backends = self.backends.read().unwrap();

        backends
            .iter()
            .filter_map(|backend| backend.id.parse::<usize>().ok())
            .max()
            .unwrap_or(0)
            + 1
    }

    pub fn metrics_summary(&self) -> Vec<BackendMetricsReport> {
        let backends = self.backends.read().unwrap();
        backends
            .iter()
            .map(|node| BackendMetricsReport {
                id: node.backend.id.clone(),
                address: node.backend.address.clone(),
                weight: node.backend.weight,
                healthy: node.healthy.load(std::sync::atomic::Ordering::Relaxed),
                metrics: node.metrics.snapshot(),
            })
            .collect()
    }
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct BackendMetricsReport {
    pub id: String,
    pub address: String,
    pub weight: usize,
    pub healthy: bool,
    pub metrics: crate::backend::model::BackendMetricsSnapshot,
}

impl Default for BackendRegistry {
    fn default() -> Self {
        Self::new()
    }
}
#[cfg(test)]
#[path = "tests/registry_tests.rs"]
mod tests;
