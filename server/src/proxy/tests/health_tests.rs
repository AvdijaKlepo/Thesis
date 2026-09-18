use super::*;
use crate::{Backend, algorithms::AlgorithmKind, backend::BackendPool};
use std::net::TcpListener;

#[test]
fn test_health_checker_detects_failure_and_recovery() {
    // Bind an ephemeral TCP listener to simulate a healthy backend
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();

    let backend_pool = Arc::new(
        BackendPool::new(
            AlgorithmKind::RoundRobin,
            vec![Backend {
                id: "test-node".into(),
                address: format!("127.0.0.1:{port}"),
                weight: 1,
            }],
        )
        .unwrap(),
    );
    let node = backend_pool.backends().remove(0);

    let config = HealthCheckConfig {
        interval: Duration::from_millis(50),
        timeout: Duration::from_millis(100),
        unhealthy_threshold: 2,
        healthy_threshold: 2,
    };

    let registry = Arc::new(ServiceRegistry::new());
    registry
        .add(
            crate::service::Service::proxy(
                "test",
                vec![crate::service::RouteMatcher::new(None::<String>, "/").unwrap()],
                Arc::clone(&backend_pool),
            )
            .unwrap(),
        )
        .unwrap();
    let checker = HealthChecker::new(registry, config);

    // Initially healthy
    assert!(node.healthy.load(Ordering::Relaxed));

    // 1 check with listener open -> remains healthy
    checker.check_all();
    assert!(node.healthy.load(Ordering::Relaxed));

    // Close the listener to simulate backend failure
    drop(listener);

    // 1st failed check -> threshold 2 not reached yet, still marked healthy
    checker.check_all();
    assert!(node.healthy.load(Ordering::Relaxed));

    // 2nd failed check -> threshold 2 reached, marked unhealthy
    checker.check_all();
    assert!(!node.healthy.load(Ordering::Relaxed));

    // Re-open listener on the same port to simulate backend recovery
    let _listener_recovered = TcpListener::bind(format!("127.0.0.1:{port}")).unwrap();

    // 1st recovery check -> threshold 2 not reached yet, still unhealthy
    checker.check_all();
    assert!(!node.healthy.load(Ordering::Relaxed));

    // 2nd recovery check -> threshold 2 reached, marked healthy again!
    checker.check_all();
    assert!(node.healthy.load(Ordering::Relaxed));
}

#[test]
fn discovers_backends_added_after_startup() {
    let registry = Arc::new(ServiceRegistry::new());
    let checker = HealthChecker::new(
        Arc::clone(&registry),
        HealthCheckConfig {
            unhealthy_threshold: 1,
            ..HealthCheckConfig::default()
        },
    );
    let pool = Arc::new(BackendPool::new(AlgorithmKind::RoundRobin, Vec::new()).unwrap());
    registry
        .add(
            crate::service::Service::proxy(
                "dynamic",
                vec![crate::service::RouteMatcher::new(None::<String>, "/dynamic").unwrap()],
                Arc::clone(&pool),
            )
            .unwrap(),
        )
        .unwrap();
    pool.add_backend(Backend {
        id: "late".into(),
        address: "127.0.0.1:9".into(),
        weight: 1,
    })
    .unwrap();

    checker.check_all();
    assert!(!pool.backends()[0].healthy.load(Ordering::Relaxed));
}
