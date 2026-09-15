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
        registry
            .add(Service::proxy("first", vec![route(None, "/other")], pool("1", 8082)).unwrap()),
        Err(ServiceRegistryError::DuplicateServiceId(_))
    ));
    assert!(matches!(
        registry.add(Service::proxy("second", vec![route(None, "/api")], pool("1", 8083)).unwrap()),
        Err(ServiceRegistryError::DuplicateRoute(_))
    ));
}

#[test]
fn static_and_proxy_targets_remain_distinct() {
    let static_service =
        Service::static_files("site", vec![route(Some("site.example.com"), "/")], "web").unwrap();
    assert!(static_service.proxy_pool().is_none());

    let proxy_service = Service::proxy("api", vec![route(None, "/api")], pool("1", 8081)).unwrap();
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

#[test]
fn router_observes_service_replacements() {
    let registry = Arc::new(ServiceRegistry::new());
    registry
        .add(Service::proxy("default", vec![route(None, "/old")], pool("one", 8081)).unwrap())
        .unwrap();
    let router = ServiceRouter::new(Arc::clone(&registry), "default").unwrap();

    registry
        .replace(Service::proxy("default", vec![route(None, "/new")], pool("two", 8082)).unwrap())
        .unwrap();

    assert_eq!(router.resolve(None, "/new").routes[0].path_prefix, "/new");
    assert_eq!(
        router.resolve(None, "/unknown").routes[0].path_prefix,
        "/new"
    );
}
