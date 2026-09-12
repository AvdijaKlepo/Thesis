use std::{
    error::Error,
    fmt::{Display, Formatter},
    fs, io,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use serde::Deserialize;

use crate::{
    Backend,
    algorithms::AlgorithmKind,
    backend::{BackendPool, BackendPoolError},
    proxy::{HealthCheckConfig, RuntimeMode},
    service::{RouteMatcher, Service, ServiceRegistry, ServiceRegistryError},
};

#[derive(Debug)]
pub enum ConfigError {
    Read { path: PathBuf, source: io::Error },
    Parse(toml::de::Error),
    Invalid(String),
    BackendPool(BackendPoolError),
    ServiceRegistry(ServiceRegistryError),
}

impl Display for ConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Read { path, source } => {
                write!(
                    formatter,
                    "failed to read configuration {}: {source}",
                    path.display()
                )
            }
            Self::Parse(source) => write!(formatter, "invalid TOML configuration: {source}"),
            Self::Invalid(message) => write!(formatter, "invalid server configuration: {message}"),
            Self::BackendPool(source) => write!(formatter, "invalid backend pool: {source}"),
            Self::ServiceRegistry(source) => {
                write!(formatter, "invalid service registry: {source}")
            }
        }
    }
}

impl Error for ConfigError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Read { source, .. } => Some(source),
            Self::Parse(source) => Some(source),
            Self::BackendPool(source) => Some(source),
            Self::ServiceRegistry(source) => Some(source),
            Self::Invalid(_) => None,
        }
    }
}

impl From<toml::de::Error> for ConfigError {
    fn from(value: toml::de::Error) -> Self {
        Self::Parse(value)
    }
}

impl From<BackendPoolError> for ConfigError {
    fn from(value: BackendPoolError) -> Self {
        Self::BackendPool(value)
    }
}

impl From<ServiceRegistryError> for ConfigError {
    fn from(value: ServiceRegistryError) -> Self {
        Self::ServiceRegistry(value)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    pub server: ServerSettings,
    #[serde(default)]
    pub health: HealthSettings,
    #[serde(default)]
    pub observability: ObservabilitySettings,
    pub services: Vec<ServiceSettings>,
}

impl AppConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let contents = fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        let mut config = Self::from_toml(&contents)?;
        let base = path.parent().unwrap_or_else(|| Path::new("."));
        config.resolve_paths(base);
        config.validate_paths()?;
        Ok(config)
    }

    pub fn from_toml(contents: &str) -> Result<Self, ConfigError> {
        let config: Self = toml::from_str(contents)?;
        config.validate()?;
        Ok(config)
    }

    pub fn build_service_registry(&self) -> Result<ServiceRegistry, ConfigError> {
        self.validate()?;
        let registry = ServiceRegistry::new();

        for service in &self.services {
            let routes = service
                .routes
                .iter()
                .map(|route| RouteMatcher::new(route.host.clone(), route.path_prefix.clone()))
                .collect::<Result<Vec<_>, _>>()?;

            let runtime_service = match service.kind {
                ServiceKind::Proxy => {
                    let algorithm = service.algorithm.ok_or_else(|| {
                        ConfigError::Invalid(format!(
                            "proxy service '{}' must define an algorithm",
                            service.id
                        ))
                    })?;
                    let backends = service
                        .backends
                        .iter()
                        .map(|backend| Backend {
                            id: backend.id.clone(),
                            address: backend.address.clone(),
                            weight: backend.weight,
                        })
                        .collect();
                    let pool = Arc::new(BackendPool::new_with_fail_open(
                        algorithm,
                        backends,
                        service.fail_open,
                    )?);
                    Service::proxy(service.id.clone(), routes, pool)?
                }
                ServiceKind::Static => {
                    let root = service.root.clone().ok_or_else(|| {
                        ConfigError::Invalid(format!(
                            "static service '{}' must define a root",
                            service.id
                        ))
                    })?;
                    Service::static_files(service.id.clone(), routes, root)?
                }
            };
            registry.add(runtime_service)?;
        }

        Ok(registry)
    }

    pub fn default_backend_pool(
        &self,
        registry: &ServiceRegistry,
    ) -> Result<Arc<BackendPool>, ConfigError> {
        let service = registry.get(&self.server.default_service).ok_or_else(|| {
            ConfigError::Invalid(format!(
                "default service '{}' does not exist",
                self.server.default_service
            ))
        })?;
        service.proxy_pool().ok_or_else(|| {
            ConfigError::Invalid(format!(
                "default service '{}' must be a proxy service",
                self.server.default_service
            ))
        })
    }

    pub fn health_check_config(&self) -> HealthCheckConfig {
        HealthCheckConfig {
            interval: Duration::from_millis(self.health.interval_ms),
            timeout: Duration::from_millis(self.health.timeout_ms),
            unhealthy_threshold: self.health.unhealthy_threshold,
            healthy_threshold: self.health.healthy_threshold,
        }
    }

    fn validate(&self) -> Result<(), ConfigError> {
        let listeners = [
            ("proxy_address", &self.server.proxy_address),
            ("control_address", &self.server.control_address),
            ("admin_address", &self.server.admin_address),
        ];
        let parsed = listeners
            .iter()
            .map(|(name, address)| {
                address.parse::<SocketAddr>().map_err(|_| {
                    ConfigError::Invalid(format!("{name} must be an IP socket address: {address}"))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        if parsed[0] == parsed[1] || parsed[0] == parsed[2] || parsed[1] == parsed[2] {
            return Err(ConfigError::Invalid(
                "proxy, control, and admin addresses must be distinct".into(),
            ));
        }
        if self.server.thread_pool_size == 0 {
            return Err(ConfigError::Invalid(
                "thread_pool_size must be greater than zero".into(),
            ));
        }
        if self.server.default_service.trim().is_empty() {
            return Err(ConfigError::Invalid(
                "default_service must not be empty".into(),
            ));
        }
        if self.services.is_empty() {
            return Err(ConfigError::Invalid(
                "at least one service must be configured".into(),
            ));
        }
        if self.health.interval_ms == 0
            || self.health.timeout_ms == 0
            || self.health.unhealthy_threshold == 0
            || self.health.healthy_threshold == 0
        {
            return Err(ConfigError::Invalid(
                "health intervals, timeouts, and thresholds must be greater than zero".into(),
            ));
        }

        let default_service = self
            .services
            .iter()
            .find(|service| service.id == self.server.default_service)
            .ok_or_else(|| {
                ConfigError::Invalid(format!(
                    "default service '{}' does not exist",
                    self.server.default_service
                ))
            })?;
        if default_service.kind != ServiceKind::Proxy {
            return Err(ConfigError::Invalid(format!(
                "default service '{}' must be a proxy service while the control APIs use its backend pool",
                self.server.default_service
            )));
        }

        for service in &self.services {
            if service.id.trim().is_empty() {
                return Err(ConfigError::Invalid("service id must not be empty".into()));
            }
            if service.routes.is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "service '{}' must define at least one route",
                    service.id
                )));
            }
            match service.kind {
                ServiceKind::Proxy => {
                    if service.algorithm.is_none() {
                        return Err(ConfigError::Invalid(format!(
                            "proxy service '{}' must define an algorithm",
                            service.id
                        )));
                    }
                    if service.root.is_some() {
                        return Err(ConfigError::Invalid(format!(
                            "proxy service '{}' cannot define a static root",
                            service.id
                        )));
                    }
                    for backend in &service.backends {
                        if backend.weight == 0 {
                            return Err(ConfigError::Invalid(format!(
                                "backend '{}' in service '{}' must have a positive weight",
                                backend.id, service.id
                            )));
                        }
                    }
                }
                ServiceKind::Static => {
                    if service.root.is_none() {
                        return Err(ConfigError::Invalid(format!(
                            "static service '{}' must define a root",
                            service.id
                        )));
                    }
                    if service.algorithm.is_some()
                        || service.fail_open
                        || !service.backends.is_empty()
                    {
                        return Err(ConfigError::Invalid(format!(
                            "static service '{}' cannot define proxy settings",
                            service.id
                        )));
                    }
                }
            }
        }

        // Building applies route normalization and validates service/backend uniqueness.
        let _ = self.build_registry_for_validation()?;
        Ok(())
    }

    fn build_registry_for_validation(&self) -> Result<ServiceRegistry, ConfigError> {
        let registry = ServiceRegistry::new();
        for service in &self.services {
            let routes = service
                .routes
                .iter()
                .map(|route| RouteMatcher::new(route.host.clone(), route.path_prefix.clone()))
                .collect::<Result<Vec<_>, _>>()?;
            let target = match service.kind {
                ServiceKind::Proxy => {
                    let algorithm = service.algorithm.expect("validated above");
                    let backends = service
                        .backends
                        .iter()
                        .map(|backend| Backend {
                            id: backend.id.clone(),
                            address: backend.address.clone(),
                            weight: backend.weight,
                        })
                        .collect();
                    Service::proxy(
                        service.id.clone(),
                        routes,
                        Arc::new(BackendPool::new_with_fail_open(
                            algorithm,
                            backends,
                            service.fail_open,
                        )?),
                    )?
                }
                ServiceKind::Static => Service::static_files(
                    service.id.clone(),
                    routes,
                    service.root.clone().expect("validated above"),
                )?,
            };
            registry.add(target)?;
        }
        Ok(registry)
    }

    fn resolve_paths(&mut self, base: &Path) {
        if self.server.web_root.is_relative() {
            self.server.web_root = base.join(&self.server.web_root);
        }
        for service in &mut self.services {
            if let Some(root) = &service.root
                && root.is_relative()
            {
                service.root = Some(base.join(root));
            }
        }
    }

    fn validate_paths(&self) -> Result<(), ConfigError> {
        if !self.server.web_root.is_dir() {
            return Err(ConfigError::Invalid(format!(
                "web_root is not a directory: {}",
                self.server.web_root.display()
            )));
        }
        for service in &self.services {
            if let Some(root) = &service.root
                && !root.is_dir()
            {
                return Err(ConfigError::Invalid(format!(
                    "static root for service '{}' is not a directory: {}",
                    service.id,
                    root.display()
                )));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerSettings {
    pub proxy_address: String,
    pub control_address: String,
    pub admin_address: String,
    pub web_root: PathBuf,
    pub thread_pool_size: usize,
    pub runtime: RuntimeMode,
    pub default_service: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HealthSettings {
    pub enabled: bool,
    pub interval_ms: u64,
    pub timeout_ms: u64,
    pub unhealthy_threshold: usize,
    pub healthy_threshold: usize,
}

impl Default for HealthSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_ms: 5_000,
            timeout_ms: 2_000,
            unhealthy_threshold: 2,
            healthy_threshold: 2,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ObservabilitySettings {
    pub log_requests: bool,
}

impl Default for ObservabilitySettings {
    fn default() -> Self {
        Self { log_requests: true }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ServiceKind {
    Proxy,
    Static,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceSettings {
    pub id: String,
    pub kind: ServiceKind,
    pub routes: Vec<RouteSettings>,
    pub algorithm: Option<AlgorithmKind>,
    #[serde(default)]
    pub fail_open: bool,
    #[serde(default)]
    pub backends: Vec<BackendSettings>,
    pub root: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteSettings {
    pub host: Option<String>,
    pub path_prefix: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackendSettings {
    pub id: String,
    pub address: String,
    pub weight: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID_CONFIG: &str = r#"
[server]
proxy_address = "127.0.0.1:7879"
control_address = "127.0.0.1:7878"
admin_address = "127.0.0.1:7880"
web_root = "web"
thread_pool_size = 8
runtime = "thread_pool"
default_service = "api"

[health]
enabled = true
interval_ms = 5000
timeout_ms = 1000
unhealthy_threshold = 2
healthy_threshold = 2

[[services]]
id = "api"
kind = "proxy"
algorithm = "weighted_round_robin"
fail_open = false
routes = [{ path_prefix = "/api" }]
backends = [
    { id = "api-1", address = "api-1:8080", weight = 1 },
    { id = "api-2", address = "api-2:8080", weight = 3 },
]

[[services]]
id = "dashboard"
kind = "static"
routes = [{ host = "dashboard.example.com", path_prefix = "/" }]
root = "web"
"#;

    #[test]
    fn parses_and_builds_multiple_services() {
        let config = AppConfig::from_toml(VALID_CONFIG).unwrap();
        let registry = config.build_service_registry().unwrap();
        let pool = config.default_backend_pool(&registry).unwrap();

        assert_eq!(registry.all().len(), 2);
        assert_eq!(pool.algorithm(), AlgorithmKind::WeightedRoundRobin);
        assert_eq!(pool.backends().len(), 2);
        assert!(!pool.fail_open());
        assert_eq!(config.server.runtime, RuntimeMode::ThreadPool);
        assert!(config.health.enabled);
        assert!(config.observability.log_requests);
    }

    #[test]
    fn rejects_unknown_fields() {
        let invalid = VALID_CONFIG.replace(
            "thread_pool_size = 8",
            "thread_pool_size = 8\nthread_poll_size = 8",
        );
        assert!(matches!(
            AppConfig::from_toml(&invalid),
            Err(ConfigError::Parse(_))
        ));
    }

    #[test]
    fn rejects_duplicate_routes() {
        let invalid = VALID_CONFIG.replace(
            "routes = [{ host = \"dashboard.example.com\", path_prefix = \"/\" }]",
            "routes = [{ path_prefix = \"/api\" }]",
        );
        assert!(matches!(
            AppConfig::from_toml(&invalid),
            Err(ConfigError::ServiceRegistry(
                ServiceRegistryError::DuplicateRoute(_)
            ))
        ));
    }

    #[test]
    fn rejects_zero_weight_and_conflicting_listeners() {
        let zero_weight = VALID_CONFIG.replace("weight = 1", "weight = 0");
        assert!(matches!(
            AppConfig::from_toml(&zero_weight),
            Err(ConfigError::Invalid(_))
        ));

        let conflicting = VALID_CONFIG.replace(
            "admin_address = \"127.0.0.1:7880\"",
            "admin_address = \"127.0.0.1:7879\"",
        );
        assert!(matches!(
            AppConfig::from_toml(&conflicting),
            Err(ConfigError::Invalid(_))
        ));
    }

    #[test]
    fn requires_a_proxy_default_service() {
        let invalid = VALID_CONFIG.replace(
            "default_service = \"api\"",
            "default_service = \"dashboard\"",
        );
        assert!(matches!(
            AppConfig::from_toml(&invalid),
            Err(ConfigError::Invalid(_))
        ));
    }

    #[test]
    fn bundled_configuration_loads() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("config")
            .join("server.toml");

        let config = AppConfig::load(path).expect("bundled configuration should be valid");
        let registry = config.build_service_registry().unwrap();

        assert!(config.server.web_root.is_dir());
        assert_eq!(config.server.default_service, "default");
        assert!(config.default_backend_pool(&registry).is_ok());
    }
}
