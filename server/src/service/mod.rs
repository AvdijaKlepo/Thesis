use std::{
    collections::HashMap,
    error::Error,
    fmt::{Display, Formatter},
    path::PathBuf,
    sync::{Arc, RwLock},
};

use serde::{Deserialize, Serialize};

use crate::backend::BackendPool;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RouteMatcher {
    pub host: Option<String>,
    pub path_prefix: String,
}

impl RouteMatcher {
    pub fn new(
        host: Option<impl Into<String>>,
        path_prefix: impl Into<String>,
    ) -> Result<Self, ServiceRegistryError> {
        let host = host
            .map(Into::into)
            .map(|value| normalize_host(&value))
            .filter(|value| !value.is_empty());
        let path_prefix = normalize_path_prefix(&path_prefix.into())?;
        Ok(Self { host, path_prefix })
    }

    pub fn matches(&self, host: Option<&str>, path: &str) -> bool {
        if let Some(expected_host) = &self.host {
            let Some(actual_host) = host else {
                return false;
            };
            if normalize_host(actual_host) != *expected_host {
                return false;
            }
        }

        path_matches_prefix(path, &self.path_prefix)
    }

    fn priority(&self) -> (u8, usize) {
        (u8::from(self.host.is_some()), self.path_prefix.len())
    }
}

pub enum ServiceTarget {
    Proxy(Arc<BackendPool>),
    Static { root: PathBuf },
}

pub struct Service {
    pub id: String,
    pub routes: Vec<RouteMatcher>,
    pub target: ServiceTarget,
}

#[derive(Clone)]
pub struct ServiceRouter {
    registry: Arc<ServiceRegistry>,
    default_service: Arc<Service>,
}

impl ServiceRouter {
    pub fn new(
        registry: Arc<ServiceRegistry>,
        default_service_id: impl Into<String>,
    ) -> Result<Self, ServiceRegistryError> {
        let default_service_id = default_service_id.into();
        let default_service = registry
            .get(&default_service_id)
            .ok_or(ServiceRegistryError::ServiceNotFound(default_service_id))?;

        Ok(Self {
            registry,
            default_service,
        })
    }

    pub fn resolve(&self, host: Option<&str>, path: &str) -> Arc<Service> {
        self.registry
            .resolve(host, path)
            .unwrap_or_else(|| Arc::clone(&self.default_service))
    }
}

impl Service {
    pub fn proxy(
        id: impl Into<String>,
        routes: Vec<RouteMatcher>,
        pool: Arc<BackendPool>,
    ) -> Result<Self, ServiceRegistryError> {
        Self::new(id.into(), routes, ServiceTarget::Proxy(pool))
    }

    pub fn static_files(
        id: impl Into<String>,
        routes: Vec<RouteMatcher>,
        root: impl Into<PathBuf>,
    ) -> Result<Self, ServiceRegistryError> {
        let root = root.into();
        if root.as_os_str().is_empty() {
            return Err(ServiceRegistryError::InvalidService(
                "static root must not be empty",
            ));
        }
        Self::new(id.into(), routes, ServiceTarget::Static { root })
    }

    pub fn proxy_pool(&self) -> Option<Arc<BackendPool>> {
        match &self.target {
            ServiceTarget::Proxy(pool) => Some(Arc::clone(pool)),
            ServiceTarget::Static { .. } => None,
        }
    }

    fn new(
        id: String,
        routes: Vec<RouteMatcher>,
        target: ServiceTarget,
    ) -> Result<Self, ServiceRegistryError> {
        if id.trim().is_empty() {
            return Err(ServiceRegistryError::InvalidService(
                "service id must not be empty",
            ));
        }
        if routes.is_empty() {
            return Err(ServiceRegistryError::InvalidService(
                "service must define at least one route",
            ));
        }
        for (index, route) in routes.iter().enumerate() {
            if routes[..index].contains(route) {
                return Err(ServiceRegistryError::DuplicateRoute(route.clone()));
            }
        }

        Ok(Self { id, routes, target })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ServiceRegistryError {
    DuplicateServiceId(String),
    DuplicateRoute(RouteMatcher),
    InvalidRoute(&'static str),
    InvalidService(&'static str),
    ServiceNotFound(String),
}

impl Display for ServiceRegistryError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DuplicateServiceId(id) => write!(formatter, "service id already exists: {id}"),
            Self::DuplicateRoute(route) => write!(
                formatter,
                "route already exists: host={:?}, path_prefix={}",
                route.host, route.path_prefix
            ),
            Self::InvalidRoute(reason) => write!(formatter, "invalid route: {reason}"),
            Self::InvalidService(reason) => write!(formatter, "invalid service: {reason}"),
            Self::ServiceNotFound(id) => write!(formatter, "service not found: {id}"),
        }
    }
}

impl Error for ServiceRegistryError {}

#[derive(Default)]
pub struct ServiceRegistry {
    services: RwLock<HashMap<String, Arc<Service>>>,
}

impl ServiceRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&self, service: Service) -> Result<Arc<Service>, ServiceRegistryError> {
        let mut services = self.services.write().unwrap();
        if services.contains_key(&service.id) {
            return Err(ServiceRegistryError::DuplicateServiceId(service.id));
        }

        for route in &service.routes {
            if services
                .values()
                .any(|existing| existing.routes.contains(route))
            {
                return Err(ServiceRegistryError::DuplicateRoute(route.clone()));
            }
        }

        let id = service.id.clone();
        let service = Arc::new(service);
        services.insert(id, Arc::clone(&service));
        Ok(service)
    }

    pub fn get(&self, id: &str) -> Option<Arc<Service>> {
        self.services.read().unwrap().get(id).cloned()
    }

    pub fn remove(&self, id: &str) -> Result<Arc<Service>, ServiceRegistryError> {
        self.services
            .write()
            .unwrap()
            .remove(id)
            .ok_or_else(|| ServiceRegistryError::ServiceNotFound(id.to_string()))
    }

    pub fn all(&self) -> Vec<Arc<Service>> {
        let mut services: Vec<_> = self.services.read().unwrap().values().cloned().collect();
        services.sort_by(|left, right| left.id.cmp(&right.id));
        services
    }

    pub fn resolve(&self, host: Option<&str>, path: &str) -> Option<Arc<Service>> {
        self.services
            .read()
            .unwrap()
            .values()
            .filter_map(|service| {
                service
                    .routes
                    .iter()
                    .filter(|route| route.matches(host, path))
                    .map(RouteMatcher::priority)
                    .max()
                    .map(|priority| (priority, Arc::clone(service)))
            })
            .max_by_key(|(priority, _)| *priority)
            .map(|(_, service)| service)
    }
}

fn normalize_host(host: &str) -> String {
    let host = host.trim();
    let host_without_port = if let Some(bracketed) = host.strip_prefix('[') {
        bracketed
            .split_once(']')
            .map(|(address, _)| address)
            .unwrap_or(host)
    } else if host.matches(':').count() == 1 {
        host.rsplit_once(':')
            .filter(|(_, port)| port.parse::<u16>().is_ok())
            .map(|(name, _)| name)
            .unwrap_or(host)
    } else {
        host
    };

    host_without_port.trim_end_matches('.').to_ascii_lowercase()
}

fn normalize_path_prefix(path_prefix: &str) -> Result<String, ServiceRegistryError> {
    if !path_prefix.starts_with('/') {
        return Err(ServiceRegistryError::InvalidRoute(
            "path prefix must begin with '/'",
        ));
    }

    let normalized = if path_prefix.len() > 1 {
        path_prefix.trim_end_matches('/').to_string()
    } else {
        path_prefix.to_string()
    };
    Ok(normalized)
}

fn path_matches_prefix(path: &str, prefix: &str) -> bool {
    if prefix == "/" {
        return path.starts_with('/');
    }
    path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|remainder| remainder.starts_with('/'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Backend, algorithms::AlgorithmKind};

    fn pool(id: &str, port: u16) -> Arc<BackendPool> {
        Arc::new(
            BackendPool::new(
                AlgorithmKind::RoundRobin,
                vec![Backend {
                    id: id.into(),
                    address: format!("127.0.0.1:{port}"),
                    weight: 1,
                }],
            )
            .unwrap(),
        )
    }

    fn route(host: Option<&str>, path: &str) -> RouteMatcher {
        RouteMatcher::new(host, path).unwrap()
    }

    #[test]
    fn exact_host_then_longest_path_prefix_wins() {
        let registry = ServiceRegistry::new();
        registry
            .add(Service::proxy("generic", vec![route(None, "/api")], pool("1", 8081)).unwrap())
            .unwrap();
        registry
            .add(
                Service::proxy(
                    "host-root",
                    vec![route(Some("example.com"), "/")],
                    pool("1", 8082),
                )
                .unwrap(),
            )
            .unwrap();
        registry
            .add(
                Service::proxy(
                    "host-api",
                    vec![route(Some("example.com"), "/api")],
                    pool("1", 8083),
                )
                .unwrap(),
            )
            .unwrap();

        assert_eq!(
            registry
                .resolve(Some("EXAMPLE.COM"), "/api/orders")
                .unwrap()
                .id,
            "host-api"
        );
        assert_eq!(
            registry.resolve(Some("example.com"), "/other").unwrap().id,
            "host-root"
        );
        assert_eq!(registry.resolve(None, "/api/orders").unwrap().id, "generic");
        assert!(registry.resolve(None, "/apix").is_none());
    }

    #[test]
    fn services_have_independent_pools() {
        let registry = ServiceRegistry::new();
        let first_pool = pool("replica", 8081);
        let second_pool = pool("replica", 8082);
        second_pool.change_algorithm(AlgorithmKind::LeastConnections);

        registry
            .add(
                Service::proxy(
                    "first",
                    vec![route(None, "/first")],
                    Arc::clone(&first_pool),
                )
                .unwrap(),
            )
            .unwrap();
        registry
            .add(
                Service::proxy(
                    "second",
                    vec![route(None, "/second")],
                    Arc::clone(&second_pool),
                )
                .unwrap(),
            )
            .unwrap();

        first_pool
            .add_backend(Backend {
                id: "extra".into(),
                address: "127.0.0.1:8083".into(),
                weight: 1,
            })
            .unwrap();

        assert_eq!(first_pool.backends().len(), 2);
        assert_eq!(second_pool.backends().len(), 1);
        assert_eq!(first_pool.algorithm(), AlgorithmKind::RoundRobin);
        assert_eq!(second_pool.algorithm(), AlgorithmKind::LeastConnections);
    }

    #[test]
    fn duplicate_service_ids_and_routes_are_rejected() {
        let registry = ServiceRegistry::new();
        registry
            .add(Service::proxy("first", vec![route(None, "/api")], pool("1", 8081)).unwrap())
            .unwrap();

        assert!(matches!(
            registry.add(
                Service::proxy("first", vec![route(None, "/other")], pool("1", 8082)).unwrap()
            ),
            Err(ServiceRegistryError::DuplicateServiceId(_))
        ));
        assert!(matches!(
            registry
                .add(Service::proxy("second", vec![route(None, "/api")], pool("1", 8083)).unwrap()),
            Err(ServiceRegistryError::DuplicateRoute(_))
        ));
    }

    #[test]
    fn static_and_proxy_targets_remain_distinct() {
        let static_service =
            Service::static_files("site", vec![route(Some("site.example.com"), "/")], "web")
                .unwrap();
        assert!(static_service.proxy_pool().is_none());

        let proxy_service =
            Service::proxy("api", vec![route(None, "/api")], pool("1", 8081)).unwrap();
        assert!(proxy_service.proxy_pool().is_some());
    }

    #[test]
    fn router_falls_back_and_ignores_host_ports() {
        let registry = Arc::new(ServiceRegistry::new());
        registry
            .add(
                Service::proxy(
                    "default",
                    vec![route(None, "/fallback")],
                    pool("default", 8081),
                )
                .unwrap(),
            )
            .unwrap();
        registry
            .add(
                Service::proxy(
                    "telemetry",
                    vec![route(Some("telemetry.example.com"), "/live")],
                    pool("telemetry", 8082),
                )
                .unwrap(),
            )
            .unwrap();
        let router = ServiceRouter::new(registry, "default").unwrap();

        assert_eq!(
            router
                .resolve(Some("TELEMETRY.EXAMPLE.COM:7879"), "/live/lap")
                .id,
            "telemetry"
        );
        assert_eq!(
            router.resolve(Some("other.example"), "/unknown").id,
            "default"
        );
    }
}
