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

#[test]
fn fixture_server_configurations_load() {
    let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("config")
        .join("fixtures");

    for (file_name, expected_backends) in [
        ("equal-capacity.toml", 3),
        ("heterogeneous.toml", 3),
        ("variable-latency.toml", 3),
        ("failure.toml", 3),
        ("equal-capacity-3.toml", 3),
        ("equal-capacity-6.toml", 6),
        ("cardinality-3.toml", 3),
        ("cardinality-6.toml", 6),
        ("cardinality-12.toml", 12),
        ("equal-capacity-12.toml", 12),
        ("heterogeneous-replicas-13.toml", 13),
        ("failure-12.toml", 12),
        ("variable-latency-12.toml", 12),
        ("adaptive-v2-tuning-balanced.toml", 6),
        ("adaptive-v2-tuning-deadline.toml", 6),
        ("adaptive-v2-tuning-latency.toml", 6),
    ] {
        let config = AppConfig::load(directory.join(file_name))
            .unwrap_or_else(|error| panic!("{file_name} should be valid: {error}"));
        let registry = config.build_service_registry().unwrap();
        assert_eq!(registry.all().len(), 1);
        assert_eq!(
            config
                .default_backend_pool(&registry)
                .unwrap()
                .backends()
                .len(),
            expected_backends
        );
    }
}

#[test]
fn adaptive_v2_settings_are_optional_and_configurable() {
    let defaults = AppConfig::from_toml(VALID_CONFIG).unwrap();
    assert_eq!(
        defaults.services[0].adaptive_v2,
        AdaptiveV2Settings::default()
    );

    let configured = VALID_CONFIG.replace(
        "algorithm = \"weighted_round_robin\"",
        "algorithm = \"adaptive_balancing_v2\"\nadaptive_v2 = { deadline_ms = 150, ewma_alpha = 0.4, slo_weight = 0.8, probe_interval_per_backend = 5, in_flight_penalty = 0.15 }",
    );
    let config = AppConfig::from_toml(&configured).unwrap();
    let registry = config.build_service_registry().unwrap();
    let pool = config.default_backend_pool(&registry).unwrap();
    assert_eq!(pool.algorithm(), AlgorithmKind::AdaptiveBalancingV2);
    assert_eq!(pool.adaptive_v2_settings().deadline_ms, 150);
    assert_eq!(pool.adaptive_v2_settings().probe_interval_per_backend, 5);
}

#[test]
fn rejects_invalid_adaptive_v2_settings() {
    let invalid = VALID_CONFIG.replace(
        "algorithm = \"weighted_round_robin\"",
        "algorithm = \"adaptive_balancing_v2\"\nadaptive_v2 = { ewma_alpha = 0.0 }",
    );
    assert!(matches!(
        AppConfig::from_toml(&invalid),
        Err(ConfigError::Invalid(_))
    ));
}
