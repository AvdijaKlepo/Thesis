use super::*;

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
}

#[test]
fn empty_load_balancers_return_none() {
    assert!(RoundRobin::new(Vec::new()).next(false).is_none());
    assert!(WeightedRoundRobin::new(Vec::new()).next(false).is_none());
    assert!(LeastConnections::new(Vec::new()).next(false).is_none());
    assert!(LeastResponseTime::new(Vec::new()).next(false).is_none());
}
