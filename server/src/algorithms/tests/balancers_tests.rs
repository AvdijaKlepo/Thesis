use super::*;

fn feedback(latency_ms: u64, success: bool) -> Feedback {
    Feedback {
        latency: Duration::from_millis(latency_ms),
        success,
    }
}

fn test_node(id: &str, port: u16, weight: usize) -> BackendNode {
    BackendNode::new(Backend {
        id: id.into(),
        address: format!("127.0.0.1:{port}"),
        weight,
    })
}

#[test]
fn test_least_connections_selects_minimum() {
    let n1 = test_node("1", 8081, 1);
    let n2 = test_node("2", 8082, 1);
    let n3 = test_node("3", 8083, 1);

    n1.metrics.active_connections.store(3, Ordering::Relaxed);
    n2.metrics.active_connections.store(0, Ordering::Relaxed);
    n3.metrics.active_connections.store(2, Ordering::Relaxed);

    let lb = LeastConnections::new(vec![n1.clone(), n2.clone(), n3.clone()]);
    let selected = lb.next(false).unwrap();
    assert_eq!(selected.id, "2");

    // When node 2 gets more connections, node 3 should be selected next
    n2.metrics.active_connections.store(4, Ordering::Relaxed);
    let selected2 = lb.next(false).unwrap();
    assert_eq!(selected2.id, "3");
}

#[test]
fn test_round_robin() {
    let n1 = test_node("1", 8081, 1);
    let n2 = test_node("2", 8082, 1);
    let lb = RoundRobin::new(vec![n1.clone(), n2.clone()]);

    assert_eq!(lb.next(false).unwrap().id, "1");
    assert_eq!(lb.next(false).unwrap().id, "2");
    assert_eq!(lb.next(false).unwrap().id, "1");
}

#[test]
fn test_weighted_round_robin_distribution() {
    let n1 = test_node("1", 8081, 5);
    let n2 = test_node("2", 8082, 2);
    let n3 = test_node("3", 8083, 3);

    let lb = WeightedRoundRobin::new(vec![n1, n2, n3]);
    let mut counts = std::collections::HashMap::new();

    for _ in 0..1000 {
        let selected = lb.next(false).unwrap();
        *counts.entry(selected.id.clone()).or_insert(0) += 1;
    }

    assert_eq!(counts.get("1"), Some(&500));
    assert_eq!(counts.get("2"), Some(&200));
    assert_eq!(counts.get("3"), Some(&300));
}

#[test]
fn test_weighted_round_robin_three_one_one_distribution() {
    let backends = vec![
        test_node("1", 8081, 3),
        test_node("2", 8082, 1),
        test_node("3", 8083, 1),
    ];
    let lb = WeightedRoundRobin::new(backends);
    let mut counts = std::collections::HashMap::new();

    for _ in 0..500 {
        let selected = lb.next(false).unwrap();
        *counts.entry(selected.id.clone()).or_insert(0) += 1;
    }

    assert_eq!(counts.get("1"), Some(&300));
    assert_eq!(counts.get("2"), Some(&100));
    assert_eq!(counts.get("3"), Some(&100));
}

#[test]
fn test_weighted_round_robin_smooth_interleaving() {
    let n1 = test_node("1", 8081, 3);
    let n2 = test_node("2", 8082, 1);
    let n3 = test_node("3", 8083, 1);

    let lb = WeightedRoundRobin::new(vec![n1, n2, n3]);
    let sequence: Vec<String> = (0..5).map(|_| lb.next(false).unwrap().id.clone()).collect();

    // 3 + 1 + 1 = 5 selections: node 1 appears 3 times, nodes 2 and 3 appear 1 time each
    let count_1 = sequence.iter().filter(|id| *id == "1").count();
    let count_2 = sequence.iter().filter(|id| *id == "2").count();
    let count_3 = sequence.iter().filter(|id| *id == "3").count();
    assert_eq!(count_1, 3);
    assert_eq!(count_2, 1);
    assert_eq!(count_3, 1);

    // Smooth distribution: node 1 should not hog all 3 selections at the beginning
    assert_eq!(sequence, vec!["1", "3", "1", "2", "1"]);
}

#[test]
fn test_weighted_round_robin_zero_weight() {
    let n1 = test_node("1", 8081, 4);
    let n2 = test_node("2", 8082, 0);

    let lb = WeightedRoundRobin::new(vec![n1, n2]);
    let mut counts = std::collections::HashMap::new();

    for _ in 0..100 {
        let selected = lb.next(false).unwrap();
        *counts.entry(selected.id.clone()).or_insert(0) += 1;
    }

    assert_eq!(counts.get("1"), Some(&100));
    assert_eq!(counts.get("2"), None);
}

#[test]
fn test_load_balancer_names_and_backends() {
    let n1 = test_node("1", 8081, 1);
    let n2 = test_node("2", 8082, 2);

    let rr = RoundRobin::new(vec![n1.clone(), n2.clone()]);
    assert_eq!(rr.name(), "round_robin");
    assert_eq!(rr.backends().len(), 2);

    let wrr = WeightedRoundRobin::new(vec![n1.clone(), n2.clone()]);
    assert_eq!(wrr.name(), "weighted_round_robin");
    assert_eq!(wrr.backends().len(), 2);

    let lc = LeastConnections::new(vec![n1.clone(), n2.clone()]);
    assert_eq!(lc.name(), "least_connections");
    assert_eq!(lc.backends().len(), 2);

    let lrt = LeastResponseTime::new(vec![n1.clone(), n2.clone()]);
    assert_eq!(lrt.name(), "least_response_time");
    assert_eq!(lrt.backends().len(), 2);

    let adaptive = AdaptiveBalancing::new(vec![n1, n2]);
    assert_eq!(adaptive.name(), "adaptive_balancing");
    assert_eq!(adaptive.backends().len(), 2);

    let adaptive_v2 = AdaptiveBalancingV2::new(adaptive.backends().to_vec());
    assert_eq!(adaptive_v2.name(), "adaptive_balancing_v2");
    assert_eq!(adaptive_v2.backends().len(), 2);
}

#[test]
fn test_load_balancers_skip_unhealthy_backends() {
    let n1 = test_node("1", 8081, 1);
    let n2 = test_node("2", 8082, 1);
    let n3 = test_node("3", 8083, 1);

    // Mark node 2 as unhealthy
    n2.healthy.store(false, Ordering::Relaxed);

    let rr = RoundRobin::new(vec![n1.clone(), n2.clone(), n3.clone()]);
    let mut rr_seen = std::collections::HashSet::new();
    for _ in 0..10 {
        rr_seen.insert(rr.next(false).unwrap().id.clone());
    }
    assert!(rr_seen.contains("1"));
    assert!(!rr_seen.contains("2"));
    assert!(rr_seen.contains("3"));

    let wrr = WeightedRoundRobin::new(vec![n1.clone(), n2.clone(), n3.clone()]);
    let mut wrr_seen = std::collections::HashSet::new();
    for _ in 0..10 {
        wrr_seen.insert(wrr.next(false).unwrap().id.clone());
    }
    assert!(wrr_seen.contains("1"));
    assert!(!wrr_seen.contains("2"));
    assert!(wrr_seen.contains("3"));

    // Least connections: even if node 2 has 0 connections, it's unhealthy so skip it
    n1.metrics.active_connections.store(5, Ordering::Relaxed);
    n2.metrics.active_connections.store(0, Ordering::Relaxed);
    n3.metrics.active_connections.store(2, Ordering::Relaxed);
    let lc = LeastConnections::new(vec![n1.clone(), n2.clone(), n3.clone()]);
    assert_eq!(lc.next(false).unwrap().id, "3");
}

#[test]
fn test_load_balancers_fail_closed_when_all_unhealthy() {
    let n1 = test_node("1", 8081, 1);
    let n2 = test_node("2", 8082, 1);

    n1.healthy.store(false, Ordering::Relaxed);
    n2.healthy.store(false, Ordering::Relaxed);

    let rr = RoundRobin::new(vec![n1.clone(), n2.clone()]);
    assert!(rr.next(false).is_none());

    // Fail-open selection is explicit and may use unhealthy backends.
    let id1 = rr.next(true).unwrap().id.clone();
    let id2 = rr.next(true).unwrap().id.clone();
    assert!(id1 == "1" || id1 == "2");
    assert!(id2 == "1" || id2 == "2");

    let wrr = WeightedRoundRobin::new(vec![n1.clone(), n2.clone()]);
    assert!(wrr.next(false).is_none());
    let wid = wrr.next(true).unwrap().id.clone();
    assert!(wid == "1" || wid == "2");

    let least_connections = LeastConnections::new(vec![n1.clone(), n2.clone()]);
    assert!(least_connections.next(false).is_none());
    assert!(least_connections.next(true).is_some());

    let least_response_time = LeastResponseTime::new(vec![n1, n2]);
    assert!(least_response_time.next(false).is_none());
    assert!(least_response_time.next(true).is_some());

    let adaptive = AdaptiveBalancing::new(least_response_time.backends().to_vec());
    assert!(adaptive.next(false).is_none());
    assert!(adaptive.next(true).is_some());
}

#[test]
fn empty_load_balancers_return_none() {
    assert!(RoundRobin::new(Vec::new()).next(false).is_none());
    assert!(WeightedRoundRobin::new(Vec::new()).next(false).is_none());
    assert!(LeastConnections::new(Vec::new()).next(false).is_none());
    assert!(LeastResponseTime::new(Vec::new()).next(false).is_none());
    assert!(AdaptiveBalancing::new(Vec::new()).next(false).is_none());
    assert!(AdaptiveBalancingV2::new(Vec::new()).next(false).is_none());
}

#[test]
fn adaptive_v2_diagnostics_expose_settings_and_backend_state() {
    let first = test_node("first", 8081, 2);
    let second = test_node("second", 8082, 1);
    let settings = AdaptiveV2Settings {
        deadline_ms: 100,
        ..AdaptiveV2Settings::default()
    };
    let lb = AdaptiveBalancingV2::with_settings(vec![first.clone(), second.clone()], settings);

    let selected = lb.next(false).unwrap();
    lb.release(&selected, feedback(20, true));
    let snapshot = lb.adaptive_diagnostics().expect("adaptive diagnostics");
    assert_eq!(snapshot.algorithm, "adaptive_balancing_v2");
    assert_eq!(snapshot.settings, Some(settings));
    assert_eq!(snapshot.total_selections, 1);
    assert_eq!(snapshot.backends.len(), 2);
    let first = snapshot
        .backends
        .iter()
        .find(|backend| backend.backend_id == selected.id)
        .unwrap();
    assert_eq!(first.observations, 1);
    assert_eq!(first.selections, 1);
    assert_eq!(first.in_flight, 0);
    assert!(first.latency_utility_ewma.is_some());
    assert!(first.score.is_finite());
}

#[test]
fn adaptive_balancing_spreads_concurrent_cold_start_selections() {
    let backends = vec![
        test_node("1", 8081, 1),
        test_node("2", 8082, 1),
        test_node("3", 8083, 1),
    ];
    let lb = AdaptiveBalancing::new(backends);

    let selected: Vec<String> = (0..3).map(|_| lb.next(false).unwrap().id.clone()).collect();

    assert_eq!(selected, vec!["1", "2", "3"]);
}

#[test]
fn adaptive_balancing_prefers_backend_that_meets_deadline() {
    let fast = test_node("fast", 8081, 1);
    let slow = test_node("slow", 8082, 1);
    let lb = AdaptiveBalancing::with_deadline(
        vec![fast.clone(), slow.clone()],
        Duration::from_millis(100),
    );

    for _ in 0..8 {
        lb.release(&fast, feedback(40, true));
        lb.release(&slow, feedback(180, true));
    }

    let selected = lb.next(false).unwrap();
    assert_eq!(selected.id, "fast");
}

#[test]
fn adaptive_balancing_updates_when_backend_performance_changes() {
    let first = test_node("first", 8081, 1);
    let second = test_node("second", 8082, 1);
    let lb = AdaptiveBalancing::with_deadline(
        vec![first.clone(), second.clone()],
        Duration::from_millis(100),
    );

    for _ in 0..8 {
        lb.release(&first, feedback(40, true));
        lb.release(&second, feedback(180, true));
    }
    assert_eq!(lb.next(false).unwrap().id, "first");

    for _ in 0..16 {
        lb.release(&first, feedback(180, true));
        lb.release(&second, feedback(40, true));
    }
    assert_eq!(lb.next(false).unwrap().id, "second");
}

#[test]
fn adaptive_balancing_treats_transport_failure_as_deadline_miss() {
    let healthy = test_node("healthy", 8081, 1);
    let failing = test_node("failing", 8082, 1);
    let lb = AdaptiveBalancing::with_deadline(
        vec![healthy.clone(), failing.clone()],
        Duration::from_millis(100),
    );

    for _ in 0..8 {
        lb.release(&healthy, feedback(80, true));
        lb.release(&failing, feedback(5, false));
    }

    assert_eq!(lb.next(false).unwrap().id, "healthy");
}

#[test]
fn adaptive_v2_settings_validate_and_have_stable_defaults() {
    let settings = AdaptiveV2Settings::default();
    assert_eq!(settings.deadline_ms, 200);
    assert_eq!(settings.ewma_alpha, 0.2);
    assert_eq!(settings.slo_weight, 0.7);
    assert_eq!(settings.probe_interval_per_backend, 32);
    assert_eq!(settings.in_flight_penalty, 0.05);
    assert!(settings.validate().is_ok());

    for invalid in [
        AdaptiveV2Settings {
            deadline_ms: 0,
            ..settings
        },
        AdaptiveV2Settings {
            ewma_alpha: 0.0,
            ..settings
        },
        AdaptiveV2Settings {
            ewma_alpha: 1.1,
            ..settings
        },
        AdaptiveV2Settings {
            slo_weight: -0.1,
            ..settings
        },
        AdaptiveV2Settings {
            slo_weight: 1.1,
            ..settings
        },
        AdaptiveV2Settings {
            probe_interval_per_backend: 0,
            ..settings
        },
        AdaptiveV2Settings {
            in_flight_penalty: -0.1,
            ..settings
        },
    ] {
        assert!(invalid.validate().is_err());
    }
}

#[test]
fn adaptive_v2_tracks_bounded_reward_and_continuous_latency_utility() {
    let backend = test_node("backend", 8081, 1);
    let settings = AdaptiveV2Settings {
        deadline_ms: 100,
        ..Default::default()
    };
    let lb = AdaptiveBalancingV2::with_settings(vec![backend.clone()], settings);

    lb.release(&backend, feedback(50, true));
    {
        let state = lb.state.lock().unwrap();
        let backend_state = &state.backends[0];
        assert_eq!(backend_state.deadline_success_probability, 1.0);
        assert!((backend_state.latency_utility - (2.0 / 3.0)).abs() < 1.0e-12);
    }

    lb.release(&backend, feedback(u64::MAX, false));
    let state = lb.state.lock().unwrap();
    let backend_state = &state.backends[0];
    assert!((0.0..=1.0).contains(&backend_state.deadline_success_probability));
    assert!((0.0..=1.0).contains(&backend_state.latency_utility));
    assert!(backend_state.deadline_success_probability.is_finite());
    assert!(backend_state.latency_utility.is_finite());

    let state = AdaptiveV2BackendState {
        observations: 4,
        selections: 2,
        in_flight: 2,
        deadline_success_probability: 0.8,
        latency_utility: 0.4,
        last_selected: 3,
    };
    let score = state.score(
        AdaptiveV2Settings {
            slo_weight: 0.7,
            in_flight_penalty: 0.2,
            ..settings
        },
        4,
        10,
        8,
    );
    // .7*.8 + .3*.4 + (7/8)*.1 - .2*(2/4) = .6675
    assert!((score - 0.6675).abs() < 1.0e-12);
}

#[test]
fn adaptive_v2_ranks_successful_backends_by_latency_and_reacts_to_reversal() {
    let fast = test_node("fast", 8081, 1);
    let slow = test_node("slow", 8082, 1);
    let lb = AdaptiveBalancingV2::with_settings(
        vec![fast.clone(), slow.clone()],
        AdaptiveV2Settings {
            probe_interval_per_backend: 100,
            ..Default::default()
        },
    );

    for _ in 0..8 {
        lb.release(&fast, feedback(40, true));
        lb.release(&slow, feedback(180, true));
    }
    assert_eq!(lb.next(false).unwrap().id, "fast");

    for _ in 0..24 {
        lb.release(&fast, feedback(180, true));
        lb.release(&slow, feedback(40, true));
    }
    assert_eq!(lb.next(false).unwrap().id, "slow");
}

#[test]
fn adaptive_v2_normalizes_in_flight_penalty_by_capacity() {
    let small = test_node("small", 8081, 1);
    let large = test_node("large", 8082, 4);
    let lb = AdaptiveBalancingV2::with_settings(
        vec![small, large],
        AdaptiveV2Settings {
            in_flight_penalty: 1.0,
            ..Default::default()
        },
    );
    let mut state = lb.state.lock().unwrap();
    for backend in &mut state.backends {
        backend.observations = 1;
        backend.deadline_success_probability = 1.0;
        backend.latency_utility = 1.0;
    }
    state.backends[0].in_flight = 2;
    state.backends[1].in_flight = 2;
    state.total_selections = 10;
    drop(state);

    assert_eq!(lb.next(false).unwrap().id, "large");
}

#[test]
fn adaptive_v2_revisits_stale_backend_within_probe_bound() {
    let good = test_node("good", 8081, 1);
    let poor = test_node("poor", 8082, 1);
    let lb = AdaptiveBalancingV2::with_settings(
        vec![good.clone(), poor.clone()],
        AdaptiveV2Settings {
            probe_interval_per_backend: 2,
            ..Default::default()
        },
    );
    // Establish both observations, then let the poor backend age while the
    // good backend is selected repeatedly.
    lb.release(&good, feedback(20, true));
    lb.release(&poor, feedback(500, false));
    let mut seen = Vec::new();
    for _ in 0..5 {
        let selected = lb.next(false).unwrap();
        seen.push(selected.id.clone());
        lb.release(&selected, feedback(20, true));
        if selected.id == "poor" {
            break;
        }
    }
    assert!(seen.iter().position(|id| id == "poor").is_some());
}

#[test]
fn adaptive_v2_revisits_a_backend_after_health_recovery() {
    let good = test_node("good", 8081, 1);
    let recovered = test_node("recovered", 8082, 1);
    let lb = AdaptiveBalancingV2::with_settings(
        vec![good.clone(), recovered.clone()],
        AdaptiveV2Settings {
            probe_interval_per_backend: 2,
            ..Default::default()
        },
    );
    lb.release(&good, feedback(20, true));
    lb.release(&recovered, feedback(400, false));
    recovered.healthy.store(false, Ordering::Relaxed);
    for _ in 0..4 {
        let selected = lb.next(false).unwrap();
        assert_eq!(selected.id, "good");
        lb.release(&selected, feedback(20, true));
    }
    recovered.healthy.store(true, Ordering::Relaxed);
    assert_eq!(lb.next(false).unwrap().id, "recovered");
}

#[test]
fn adaptive_v2_cold_start_and_retry_exclusion_cover_all_backends() {
    let backends: Vec<_> = (0..6)
        .map(|index| test_node(&index.to_string(), 8100 + index, 1))
        .collect();
    let lb = AdaptiveBalancingV2::new(backends);
    let mut selected = std::collections::HashSet::new();
    for _ in 0..6 {
        let node = lb.next(false).unwrap();
        selected.insert(node.id.clone());
        lb.release(&node, feedback(20, true));
    }
    assert_eq!(selected.len(), 6);

    let first = lb.next(false).unwrap();
    let retry = lb
        .next_excluding(false, &[first.id.as_str()])
        .expect("a different backend should be available for retry");
    assert_ne!(retry.id, first.id);
}

#[test]
fn least_response_time_avoids_failing_backend() {
    let healthy = test_node("healthy", 8081, 1);
    let failing = test_node("failing", 8082, 1);

    // Initial state: both have default 1 ms latency.
    // Failing backend receives a request and fails:
    failing.metrics.record_start();
    failing.metrics.record_end(&feedback(10, false), 0, 0);

    // Healthy backend receives a request and succeeds:
    healthy.metrics.record_start();
    healthy.metrics.record_end(&feedback(40, true), 100, 200);

    let lrt = LeastResponseTime::new(vec![failing.clone(), healthy.clone()]);
    // Failing backend's latency was heavily penalized, so LRT chooses healthy
    assert_eq!(lrt.next(false).unwrap().id, "healthy");
}

#[test]
fn least_response_time_prefers_lower_latency() {
    let fast = test_node("fast", 8081, 1);
    let slow = test_node("slow", 8082, 1);

    fast.metrics.latency_us.store(20_000, Ordering::Relaxed);
    slow.metrics.latency_us.store(100_000, Ordering::Relaxed);

    let lrt = LeastResponseTime::new(vec![fast.clone(), slow.clone()]);
    assert_eq!(lrt.next(false).unwrap().id, "fast");
}

#[test]
fn least_response_time_considers_active_connections() {
    let n1 = test_node("1", 8081, 1);
    let n2 = test_node("2", 8082, 1);

    n1.metrics.latency_us.store(50_000, Ordering::Relaxed);
    n2.metrics.latency_us.store(50_000, Ordering::Relaxed);

    n1.metrics.active_connections.store(3, Ordering::Relaxed); // score = 50_000 * 4 = 200_000
    n2.metrics.active_connections.store(1, Ordering::Relaxed); // score = 50_000 * 2 = 100_000

    let lrt = LeastResponseTime::new(vec![n1, n2]);
    assert_eq!(lrt.next(false).unwrap().id, "2");
}

#[test]
fn least_response_time_retry_excludes_failed_backend_through_proxy_exchange() {
    use crate::algorithms::AlgorithmKind;
    use crate::backend::BackendPool;
    use crate::observability::{Observability, RequestOutcome};
    use crate::proxy::RuntimeMode;
    use crate::proxy::behavior::{AttemptFailure, NextAttempt, RequestPlan, plan_request};
    use crate::proxy::protocol::ClientRequest;
    use crate::service::{RouteMatcher, Service, ServiceRouter};

    let pool = Arc::new(
        BackendPool::new(
            AlgorithmKind::LeastResponseTime,
            vec![
                Backend {
                    id: "failing".into(),
                    address: "127.0.0.1:8081".into(),
                    weight: 1,
                },
                Backend {
                    id: "healthy".into(),
                    address: "127.0.0.1:8082".into(),
                    weight: 1,
                },
            ],
        )
        .unwrap(),
    );

    let nodes = pool.backends();
    let failing_node = nodes.iter().find(|node| node.id == "failing").unwrap();
    let healthy_node = nodes.iter().find(|node| node.id == "healthy").unwrap();

    // The failing backend starts with the better score. After one failure its
    // latency EWMA becomes 2,000,800 us, which is deliberately still lower
    // than the healthy backend's 3,000,000 us score. Only retry exclusion can
    // make the second attempt select the healthy backend.
    healthy_node
        .metrics
        .latency_us
        .store(3_000_000, Ordering::Relaxed);

    let registry = Arc::new(crate::service::ServiceRegistry::new());
    registry
        .add(
            Service::proxy(
                "default",
                vec![RouteMatcher::new(None::<String>, "/").unwrap()],
                Arc::clone(&pool),
            )
            .unwrap(),
        )
        .unwrap();
    let router = ServiceRouter::new(Arc::clone(&registry), "default").unwrap();
    let request = ClientRequest {
        raw: "GET /retry HTTP/1.1\r\nConnection: close\r\n\r\n".into(),
        header_end: 0,
        method: "GET".into(),
        host: None,
        path: "/retry".into(),
        is_idempotent: true,
        keep_alive: false,
    };
    let observability = Observability::new(["default"], false);
    let RequestPlan::Proxy(mut exchange) =
        plan_request(RuntimeMode::ThreadPool, &request, &router, &observability)
    else {
        panic!("proxy request plan expected");
    };

    let NextAttempt::Ready(attempt1) = exchange.next_attempt() else {
        panic!("first attempt expected");
    };
    assert_eq!(attempt1.backend_id(), "failing");

    assert!(exchange.attempt_failed(attempt1, AttemptFailure::Connect, 0, 0));
    assert_eq!(
        failing_node.metrics.latency_us.load(Ordering::Relaxed),
        2_000_800
    );

    let NextAttempt::Ready(attempt2) = exchange.next_attempt() else {
        panic!("second attempt expected");
    };
    assert_eq!(attempt2.backend_id(), "healthy");

    let result = exchange.attempt_succeeded(attempt2, 200, 50, 100);
    assert_eq!(result.outcome, RequestOutcome::Completed);
    assert_eq!(result.backend_id, "healthy");
    assert_eq!(result.attempts, 2);
}
