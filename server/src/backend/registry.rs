use std::sync::RwLock;

use crate::algorithms::algorithms::BackendNode;

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

    pub fn get(&self, id: &str) -> Option<BackendNode> {
        let backends = self.backends.read().unwrap();

        backends
            .iter()
            .find(|backend| backend.id == id)
            .cloned()
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
}

impl Default for BackendRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;
    use crate::Backend;

    fn backend_node(id: &str, port: u16) -> BackendNode {
        BackendNode::new(Backend {
            id: id.into(),
            address: format!("127.0.0.1:{port}"),
            weight: 1,
        })
    }

    #[test]
    fn can_add_and_get_backend() {
        let registry = BackendRegistry::new();

        registry.add(backend_node("1", 8081));

        let result = registry.get("1");

        assert!(result.is_some());
        assert_eq!(result.unwrap().address, "127.0.0.1:8081");
    }

    #[test]
    fn can_remove_backend() {
        let registry = BackendRegistry::new();

        registry.add(backend_node("1", 8081));

        let removed = registry.remove("1");

        assert!(removed.is_some());
        assert!(registry.get("1").is_none());
    }

    #[test]
    fn all_returns_all_backends() {
        let registry = BackendRegistry::new();

        registry.add(backend_node("1", 8081));
        registry.add(backend_node("2", 8082));
        registry.add(backend_node("3", 8083));

        let backends = registry.all();

        assert_eq!(backends.len(), 3);
    }

    #[test]
    fn metrics_stay_consistent() {
        let registry = BackendRegistry::new();

        registry.add(backend_node("1", 8081));

        let all1 = registry.all();
        all1[0]
            .metrics
            .active_connections
            .fetch_add(5, Ordering::Relaxed);
        all1[0]
            .metrics
            .latency_us
            .store(42, Ordering::Relaxed);

        let all2 = registry.all();
        assert_eq!(
            all2[0].metrics.active_connections.load(Ordering::Relaxed),
            5
        );
        assert_eq!(
            all2[0].metrics.latency_us.load(Ordering::Relaxed),
            42
        );

        let from_get = registry.get("1").unwrap();
        assert_eq!(
            from_get.metrics.active_connections.load(Ordering::Relaxed),
            5
        );
        assert_eq!(
            from_get.metrics.latency_us.load(Ordering::Relaxed),
            42
        );
    }
}
