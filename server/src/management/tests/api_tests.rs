use super::*;
use crate::algorithms::balancers::AdaptiveV2Settings;
use crate::{Backend, backend::BackendPool, service::RouteMatcher};

fn registry() -> ServiceRegistry {
    let registry = ServiceRegistry::new();
    registry
        .add(
            Service::proxy(
                "default",
                vec![RouteMatcher::new(None::<String>, "/").unwrap()],
                Arc::new(
                    BackendPool::new(
                        AlgorithmKind::RoundRobin,
                        vec![Backend {
                            id: "one".into(),
                            address: "127.0.0.1:5001".into(),
                            weight: 1,
                        }],
                    )
                    .unwrap(),
                ),
            )
            .unwrap(),
        )
        .unwrap();
    registry
}

#[test]
fn patches_routes_without_replacing_the_backend_pool() {
    let registry = registry();
    let before = registry.get("default").unwrap().proxy_pool().unwrap();
    let updated = patch_service(
        &registry,
        "default",
        ServicePatch {
            routes: Some(vec![RouteMatcher {
                host: None,
                path_prefix: "/new/".into(),
            }]),
            algorithm: Some(AlgorithmKind::LeastConnections),
            fail_open: Some(true),
            adaptive_v2: Some(AdaptiveV2Settings {
                deadline_ms: 125,
                ..Default::default()
            }),
            root: None,
        },
    )
    .unwrap();
    let after = updated.proxy_pool().unwrap();
    assert!(Arc::ptr_eq(&before, &after));
    assert_eq!(updated.routes[0].path_prefix, "/new");
    assert_eq!(after.algorithm(), AlgorithmKind::LeastConnections);
    assert!(after.fail_open());
    assert_eq!(after.adaptive_v2_settings().deadline_ms, 125);
    assert_eq!(after.backends().len(), 1);
}

#[test]
fn bounds_probe_timeout() {
    assert_eq!(
        probe_timeout("/probe?timeout_ms=25").unwrap(),
        Duration::from_millis(25)
    );
    assert!(probe_timeout("/probe?timeout_ms=0").is_err());
    assert!(probe_timeout("/probe?timeout_ms=10001").is_err());
    assert!(probe_timeout("/probe?timeout_ms=not-a-number").is_err());
}
