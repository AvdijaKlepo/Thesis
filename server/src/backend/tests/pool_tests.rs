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
    let pool = BackendPool::new(AlgorithmKind::RoundRobin, vec![backend("1", 8081, 1)]).unwrap();

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
    let pool = BackendPool::new(AlgorithmKind::RoundRobin, vec![backend("1", 8081, 1)]).unwrap();
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
    let pool = BackendPool::new(AlgorithmKind::RoundRobin, vec![backend("1", 8081, 1)]).unwrap();

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

    let pool = BackendPool::new(AlgorithmKind::RoundRobin, vec![backend("1", 8081, 1)]).unwrap();
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

#[test]
fn excluding_every_eligible_backend_is_distinct_from_an_unhealthy_pool() {
    let pool = BackendPool::new(
        AlgorithmKind::RoundRobin,
        vec![backend("1", 8081, 1), backend("2", 8082, 1)],
    )
    .unwrap();

    assert!(matches!(
        pool.select_backend_excluding(&["1", "2"]),
        Err(BackendSelectionError::AllBackendsExcluded)
    ));

    for backend in pool.backends() {
        backend
            .healthy
            .store(false, std::sync::atomic::Ordering::Relaxed);
    }
    assert!(matches!(
        pool.select_backend_excluding(&["1", "2"]),
        Err(BackendSelectionError::NoHealthyBackends)
    ));
}

#[test]
fn adaptive_v2_settings_survive_algorithm_and_membership_rebuilds() {
    let settings = AdaptiveV2Settings {
        deadline_ms: 125,
        ewma_alpha: 0.35,
        slo_weight: 0.6,
        probe_interval_per_backend: 7,
        in_flight_penalty: 0.2,
    };
    let pool = BackendPool::new_with_fail_open_and_adaptive_v2_settings(
        AlgorithmKind::AdaptiveBalancingV2,
        vec![backend("1", 8081, 1), backend("2", 8082, 2)],
        false,
        settings,
    )
    .unwrap();
    assert_eq!(pool.adaptive_v2_settings(), settings);

    pool.change_algorithm(AlgorithmKind::RoundRobin);
    pool.add_backend(backend("3", 8083, 1)).unwrap();
    pool.update_backend(backend("2", 9082, 3)).unwrap();
    pool.remove_backend("1").unwrap();
    assert_eq!(pool.adaptive_v2_settings(), settings);

    pool.change_algorithm(AlgorithmKind::AdaptiveBalancingV2);
    assert_eq!(pool.load_balancer().load().name(), "adaptive_balancing_v2");
}

#[test]
fn adaptive_v2_settings_reject_invalid_values() {
    let pool = BackendPool::new(AlgorithmKind::RoundRobin, vec![backend("1", 8081, 1)]).unwrap();
    let result = pool.set_adaptive_v2_settings(AdaptiveV2Settings {
        deadline_ms: 0,
        ..Default::default()
    });
    assert!(matches!(
        result,
        Err(BackendPoolError::InvalidAdaptiveV2Settings(_))
    ));
}
