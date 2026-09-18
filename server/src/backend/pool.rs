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
        balancers::{AdaptiveV2Settings, BackendNode, LoadBalancer},
        create_load_balancer_for_with_adaptive_v2_settings,
    },
    backend::{
        model::Feedback,
        registry::{BackendMetricsReport, BackendRegistry},
    },
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BackendPoolError {
    DuplicateBackendId(String),
    DuplicateBackendAddress(String),
    InvalidBackend(&'static str),
    BackendNotFound(String),
    InvalidAdaptiveV2Settings(String),
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
            Self::InvalidAdaptiveV2Settings(reason) => {
                write!(
                    formatter,
                    "invalid adaptive_balancing_v2 settings: {reason}"
                )
            }
        }
    }
}

impl Error for BackendPoolError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendSelectionError {
    EmptyPool,
    NoHealthyBackends,
    AllBackendsExcluded,
}

impl Display for BackendSelectionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyPool => write!(formatter, "backend pool is empty"),
            Self::NoHealthyBackends => write!(formatter, "backend pool has no healthy backends"),
            Self::AllBackendsExcluded => {
                write!(formatter, "all eligible backends were already attempted")
            }
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
    adaptive_v2_settings: RwLock<AdaptiveV2Settings>,
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
        Self::new_with_fail_open_and_adaptive_v2_settings(
            algorithm,
            backends,
            fail_open,
            AdaptiveV2Settings::default(),
        )
    }

    pub fn new_with_adaptive_v2_settings(
        algorithm: AlgorithmKind,
        backends: Vec<Backend>,
        adaptive_v2_settings: AdaptiveV2Settings,
    ) -> Result<Self, BackendPoolError> {
        Self::new_with_fail_open_and_adaptive_v2_settings(
            algorithm,
            backends,
            false,
            adaptive_v2_settings,
        )
    }

    pub fn new_with_fail_open_and_adaptive_v2_settings(
        algorithm: AlgorithmKind,
        backends: Vec<Backend>,
        fail_open: bool,
        adaptive_v2_settings: AdaptiveV2Settings,
    ) -> Result<Self, BackendPoolError> {
        validate_backends(&backends)?;
        adaptive_v2_settings
            .validate()
            .map_err(|reason| BackendPoolError::InvalidAdaptiveV2Settings(reason.into()))?;

        let registry = Arc::new(BackendRegistry::new());
        for backend in backends {
            registry.add(BackendNode::new(backend));
        }

        let load_balancer = Arc::new(ArcSwap::from_pointee(
            create_load_balancer_for_with_adaptive_v2_settings(
                algorithm,
                registry.all(),
                adaptive_v2_settings,
            ),
        ));

        Ok(Self {
            registry,
            algorithm: RwLock::new(algorithm),
            load_balancer,
            fail_open: AtomicBool::new(fail_open),
            adaptive_v2_settings: RwLock::new(adaptive_v2_settings),
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

    pub fn adaptive_v2_settings(&self) -> AdaptiveV2Settings {
        *self.adaptive_v2_settings.read().unwrap()
    }

    pub fn set_adaptive_v2_settings(
        &self,
        settings: AdaptiveV2Settings,
    ) -> Result<(), BackendPoolError> {
        settings
            .validate()
            .map_err(|reason| BackendPoolError::InvalidAdaptiveV2Settings(reason.into()))?;
        let _mutation = self.mutation_lock.lock().unwrap();
        *self.adaptive_v2_settings.write().unwrap() = settings;
        if self.algorithm() == AlgorithmKind::AdaptiveBalancingV2 {
            self.rebuild_load_balancer(self.algorithm());
        }
        Ok(())
    }

    pub fn select_backend(&self) -> Result<BackendSelection, BackendSelectionError> {
        self.select_backend_excluding(&[])
    }

    pub fn select_backend_excluding(
        &self,
        excluded: &[&str],
    ) -> Result<BackendSelection, BackendSelectionError> {
        let load_balancer = self.load_balancer.load_full();
        if load_balancer.backends().is_empty() {
            return Err(BackendSelectionError::EmptyPool);
        }

        let allow_unhealthy = self.fail_open();
        let has_eligible_backend = load_balancer
            .backends()
            .iter()
            .any(|backend| allow_unhealthy || backend.healthy.load(Ordering::Relaxed));
        if !has_eligible_backend {
            return Err(BackendSelectionError::NoHealthyBackends);
        }

        let node = load_balancer
            .next_excluding(allow_unhealthy, excluded)
            .ok_or(BackendSelectionError::AllBackendsExcluded)?;
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
        self.load_balancer.store(Arc::new(
            create_load_balancer_for_with_adaptive_v2_settings(
                algorithm,
                self.registry.all(),
                self.adaptive_v2_settings(),
            ),
        ));
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
    if backend.weight == 0 {
        return Err(BackendPoolError::InvalidBackend(
            "weight must be greater than zero",
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
#[path = "tests/pool_tests.rs"]
mod tests;
