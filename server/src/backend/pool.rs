use std::{
    error::Error,
    fmt::{Display, Formatter},
    sync::{Arc, Mutex, RwLock},
};

use arc_swap::ArcSwap;

use crate::{
    Backend,
    algorithms::{
        AlgorithmKind,
        algorithms::{BackendNode, LoadBalancer},
        create_load_balancer_for,
    },
    backend::registry::{BackendMetricsReport, BackendRegistry},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BackendPoolError {
    DuplicateBackendId(String),
    DuplicateBackendAddress(String),
    InvalidBackend(&'static str),
    BackendNotFound(String),
}

impl Display for BackendPoolError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DuplicateBackendId(id) => write!(formatter, "backend id already exists: {id}"),
            Self::DuplicateBackendAddress(address) => {
                write!(formatter, "backend address already exists: {address}")
            }
            Self::InvalidBackend(reason) => write!(formatter, "invalid backend: {reason}"),
            Self::BackendNotFound(id) => write!(formatter, "backend not found: {id}"),
        }
    }
}

impl Error for BackendPoolError {}

/// Owns the backend membership and load-balancing policy for one proxy service.
pub struct BackendPool {
    registry: Arc<BackendRegistry>,
    algorithm: RwLock<AlgorithmKind>,
    load_balancer: Arc<ArcSwap<Box<dyn LoadBalancer>>>,
    mutation_lock: Mutex<()>,
}

impl BackendPool {
    pub fn new(algorithm: AlgorithmKind, backends: Vec<Backend>) -> Result<Self, BackendPoolError> {
        validate_backends(&backends)?;

        let registry = Arc::new(BackendRegistry::new());
        for backend in backends {
            registry.add(BackendNode::new(backend));
        }

        let load_balancer = Arc::new(ArcSwap::from_pointee(create_load_balancer_for(
            algorithm,
            registry.all(),
        )));

        Ok(Self {
            registry,
            algorithm: RwLock::new(algorithm),
            load_balancer,
            mutation_lock: Mutex::new(()),
        })
    }

    pub fn algorithm(&self) -> AlgorithmKind {
        *self.algorithm.read().unwrap()
    }

    pub fn change_algorithm(&self, algorithm: AlgorithmKind) {
        let _mutation = self.mutation_lock.lock().unwrap();
        *self.algorithm.write().unwrap() = algorithm;
        self.rebuild_load_balancer(algorithm);
    }

    pub fn add_backend(&self, backend: Backend) -> Result<BackendNode, BackendPoolError> {
        validate_backend(&backend)?;
        let _mutation = self.mutation_lock.lock().unwrap();

        if self.registry.get(&backend.id).is_some() {
            return Err(BackendPoolError::DuplicateBackendId(backend.id));
        }
        if self
            .registry
            .all()
            .iter()
            .any(|candidate| candidate.address == backend.address)
        {
            return Err(BackendPoolError::DuplicateBackendAddress(backend.address));
        }

        let node = BackendNode::new(backend);
        self.registry.add(node.clone());
        self.rebuild_load_balancer(self.algorithm());
        Ok(node)
    }

    pub fn update_backend(&self, backend: Backend) -> Result<BackendNode, BackendPoolError> {
        validate_backend(&backend)?;
        let _mutation = self.mutation_lock.lock().unwrap();

        let existing = self
            .registry
            .get(&backend.id)
            .ok_or_else(|| BackendPoolError::BackendNotFound(backend.id.clone()))?;

        if self
            .registry
            .all()
            .iter()
            .any(|candidate| candidate.id != backend.id && candidate.address == backend.address)
        {
            return Err(BackendPoolError::DuplicateBackendAddress(backend.address));
        }

        let updated = BackendNode {
            backend,
            metrics: existing.metrics,
            healthy: existing.healthy,
        };
        self.registry.replace(&updated.id, updated.clone());
        self.rebuild_load_balancer(self.algorithm());
        Ok(updated)
    }

    pub fn remove_backend(&self, id: &str) -> Result<BackendNode, BackendPoolError> {
        let _mutation = self.mutation_lock.lock().unwrap();
        let removed = self
            .registry
            .remove(id)
            .ok_or_else(|| BackendPoolError::BackendNotFound(id.to_string()))?;
        self.rebuild_load_balancer(self.algorithm());
        Ok(removed)
    }

    pub fn backends(&self) -> Vec<BackendNode> {
        self.registry.all()
    }

    pub fn next_id(&self) -> usize {
        self.registry.next_id()
    }

    pub fn metrics_summary(&self) -> Vec<BackendMetricsReport> {
        self.registry.metrics_summary()
    }

    /// Compatibility handle for the current single-service proxy transport.
    pub fn load_balancer(&self) -> Arc<ArcSwap<Box<dyn LoadBalancer>>> {
        Arc::clone(&self.load_balancer)
    }

    fn rebuild_load_balancer(&self, algorithm: AlgorithmKind) {
        self.load_balancer.store(Arc::new(create_load_balancer_for(
            algorithm,
            self.registry.all(),
        )));
    }
}

fn validate_backend(backend: &Backend) -> Result<(), BackendPoolError> {
    if backend.id.trim().is_empty() {
        return Err(BackendPoolError::InvalidBackend("id must not be empty"));
    }
    if backend.address.trim().is_empty() {
        return Err(BackendPoolError::InvalidBackend(
            "address must not be empty",
        ));
    }
    Ok(())
}

fn validate_backends(backends: &[Backend]) -> Result<(), BackendPoolError> {
    for (index, backend) in backends.iter().enumerate() {
        validate_backend(backend)?;
        if backends[..index]
            .iter()
            .any(|candidate| candidate.id == backend.id)
        {
            return Err(BackendPoolError::DuplicateBackendId(backend.id.clone()));
        }
        if backends[..index]
            .iter()
            .any(|candidate| candidate.address == backend.address)
        {
            return Err(BackendPoolError::DuplicateBackendAddress(
                backend.address.clone(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn backend(id: &str, port: u16, weight: usize) -> Backend {
        Backend {
            id: id.into(),
            address: format!("127.0.0.1:{port}"),
            weight,
        }
    }

    #[test]
    fn mutations_rebuild_only_this_pool() {
        let pool =
            BackendPool::new(AlgorithmKind::RoundRobin, vec![backend("1", 8081, 1)]).unwrap();

        pool.add_backend(backend("2", 8082, 1)).unwrap();
        assert_eq!(pool.load_balancer().load().backends().len(), 2);

        pool.change_algorithm(AlgorithmKind::WeightedRoundRobin);
        assert_eq!(pool.algorithm(), AlgorithmKind::WeightedRoundRobin);
        assert_eq!(pool.load_balancer().load().name(), "weighted_round_robin");

        pool.remove_backend("1").unwrap();
        assert_eq!(pool.backends().len(), 1);
        assert_eq!(pool.backends()[0].id, "2");
    }

    #[test]
    fn update_preserves_metrics_and_health_state() {
        let pool =
            BackendPool::new(AlgorithmKind::RoundRobin, vec![backend("1", 8081, 1)]).unwrap();
        let original = pool.backends().remove(0);
        original
            .metrics
            .total_requests
            .store(7, std::sync::atomic::Ordering::Relaxed);
        original
            .healthy
            .store(false, std::sync::atomic::Ordering::Relaxed);

        let updated = pool.update_backend(backend("1", 9091, 4)).unwrap();
        assert_eq!(updated.weight, 4);
        assert_eq!(
            updated
                .metrics
                .total_requests
                .load(std::sync::atomic::Ordering::Relaxed),
            7
        );
        assert!(!updated.healthy.load(std::sync::atomic::Ordering::Relaxed));
    }

    #[test]
    fn duplicate_ids_and_addresses_are_rejected() {
        let pool =
            BackendPool::new(AlgorithmKind::RoundRobin, vec![backend("1", 8081, 1)]).unwrap();

        assert!(matches!(
            pool.add_backend(backend("1", 8082, 1)),
            Err(BackendPoolError::DuplicateBackendId(_))
        ));
        assert!(matches!(
            pool.add_backend(backend("2", 8081, 1)),
            Err(BackendPoolError::DuplicateBackendAddress(_))
        ));
    }
}
