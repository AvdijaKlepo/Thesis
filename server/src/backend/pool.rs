use std::{
    error::Error,
    fmt::{Display, Formatter},
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, Ordering},
    },
};

use arc_swap::ArcSwap;

use crate::{
    Backend,
    algorithms::{
        AlgorithmKind,
        algorithms::{BackendNode, LoadBalancer},
        create_load_balancer_for,
    },
    backend::{
        backend_server::Feedback,
        registry::{BackendMetricsReport, BackendRegistry},
    },
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendSelectionError {
    EmptyPool,
    NoHealthyBackends,
}

impl Display for BackendSelectionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyPool => write!(formatter, "backend pool is empty"),
            Self::NoHealthyBackends => write!(formatter, "backend pool has no healthy backends"),
        }
    }
}

impl Error for BackendSelectionError {}

/// A backend selected together with the exact balancer snapshot that selected it.
/// Keeping the snapshot alive ensures feedback is returned to the same algorithm
/// even if the pool policy is changed while a request is in flight.
pub struct BackendSelection {
    node: BackendNode,
    load_balancer: Arc<Box<dyn LoadBalancer>>,
}

impl BackendSelection {
    pub fn release(&self, feedback: Feedback) {
        self.load_balancer.release(&self.node, feedback);
    }
}

impl std::ops::Deref for BackendSelection {
    type Target = BackendNode;

    fn deref(&self) -> &Self::Target {
        &self.node
    }
}

/// Owns the backend membership and load-balancing policy for one proxy service.
pub struct BackendPool {
    registry: Arc<BackendRegistry>,
    algorithm: RwLock<AlgorithmKind>,
    load_balancer: Arc<ArcSwap<Box<dyn LoadBalancer>>>,
    fail_open: AtomicBool,
    mutation_lock: Mutex<()>,
}

impl BackendPool {
    pub fn new(algorithm: AlgorithmKind, backends: Vec<Backend>) -> Result<Self, BackendPoolError> {
        Self::new_with_fail_open(algorithm, backends, false)
    }

    pub fn new_with_fail_open(
        algorithm: AlgorithmKind,
        backends: Vec<Backend>,
        fail_open: bool,
    ) -> Result<Self, BackendPoolError> {
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
            fail_open: AtomicBool::new(fail_open),
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

    pub fn fail_open(&self) -> bool {
        self.fail_open.load(Ordering::Relaxed)
    }

    pub fn set_fail_open(&self, fail_open: bool) {
        self.fail_open.store(fail_open, Ordering::Relaxed);
    }

    pub fn select_backend(&self) -> Result<BackendSelection, BackendSelectionError> {
        let load_balancer = self.load_balancer.load_full();
        if load_balancer.backends().is_empty() {
            return Err(BackendSelectionError::EmptyPool);
        }

        let node = load_balancer
            .next(self.fail_open())
            .ok_or(BackendSelectionError::NoHealthyBackends)?;
        Ok(BackendSelection {
            node,
            load_balancer,
        })
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

    #[test]
    fn empty_and_unhealthy_pools_fail_closed_without_panicking() {
        let empty = BackendPool::new(AlgorithmKind::RoundRobin, Vec::new()).unwrap();
        assert!(matches!(
            empty.select_backend(),
            Err(BackendSelectionError::EmptyPool)
        ));

        let pool =
            BackendPool::new(AlgorithmKind::RoundRobin, vec![backend("1", 8081, 1)]).unwrap();
        pool.backends()[0]
            .healthy
            .store(false, std::sync::atomic::Ordering::Relaxed);
        assert!(matches!(
            pool.select_backend(),
            Err(BackendSelectionError::NoHealthyBackends)
        ));
    }

    #[test]
    fn fail_open_must_be_enabled_explicitly() {
        let pool = BackendPool::new_with_fail_open(
            AlgorithmKind::RoundRobin,
            vec![backend("1", 8081, 1)],
            true,
        )
        .unwrap();
        pool.backends()[0]
            .healthy
            .store(false, std::sync::atomic::Ordering::Relaxed);

        assert!(pool.fail_open());
        assert_eq!(pool.select_backend().unwrap().id, "1");

        pool.set_fail_open(false);
        assert!(matches!(
            pool.select_backend(),
            Err(BackendSelectionError::NoHealthyBackends)
        ));
    }
}
