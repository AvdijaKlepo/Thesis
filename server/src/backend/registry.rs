use std::sync::{Arc, RwLock};

use crate::{Backend, algorithms::algorithms::BackendNode, backend::{self, backend_server::BackendMetrics}};

#[derive(Debug)]
pub struct BackendRegistry {
    backends: RwLock<Vec<Backend>>
}


impl BackendRegistry {
    pub fn new() -> Self {
        Self { backends: RwLock::new(Vec::new()) }
    }
    

    pub fn add(&self, backend: Backend) {
        let mut backends = self.backends.write().unwrap();
        backends.push(backend);
    }

    pub fn remove(&self, id: &str) -> Option<Backend> {
        let mut backends = self.backends.write().unwrap();

        let index = backends.iter().position(|backend| backend.id == id)?;

        Some(backends.remove(index))
    }

    pub fn get(&self, id: &str) -> Option<Backend> {
        let backends = self.backends.read().unwrap();

        backends
            .iter()
            .find(|backend| backend.id == id)
            .cloned()
    }

    pub fn all(&self) -> Vec<BackendNode> {
    let backends = self.backends.read().unwrap();

    backends
        .iter()
        .cloned()
        .map(|backend| BackendNode {
            backend,
            metrics: Arc::new(BackendMetrics::new()),
        })
        .collect()
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
}
impl Default for BackendRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn backend(id: &str, port: u16) -> Backend {
        Backend {
            id: id.into(),
            address: format!("127.0.0.1:{port}"),
            weight: 1,
        }
    }

    #[test]
    fn can_add_and_get_backend() {
        let registry = BackendRegistry::new();

        registry.add(backend("1", 8081));

        let result = registry.get("1");

        assert!(result.is_some());
        assert_eq!(result.unwrap().address, "127.0.0.1:8081");
    }

    #[test]
    fn can_remove_backend() {
        let registry = BackendRegistry::new();

        registry.add(backend("1", 8081));

        let removed = registry.remove("1");

        assert!(removed.is_some());
        assert!(registry.get("1").is_none());
    }

    #[test]
    fn all_returns_all_backends() {
        let registry = BackendRegistry::new();

        registry.add(backend("1", 8081));
        registry.add(backend("2", 8082));
        registry.add(backend("3", 8083));

        let backends = registry.all();

        assert_eq!(backends.len(), 3);
    }
}