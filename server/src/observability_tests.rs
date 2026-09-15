use super::*;
use crate::{
    Backend,
    backend::BackendPool,
    service::{RouteMatcher, Service},
};
use std::sync::Arc;

#[test]
fn records_per_service_and_runtime_metrics() {
    let observability = Observability::new(["telemetry"], false);
    observability.record(RequestObservation {
        request_id: observability.next_request_id(),
        runtime: RuntimeMode::Async,
        service_id: "telemetry",
        algorithm: Some(AlgorithmKind::LeastConnections),
        backend_id: "car-1",
        method: "GET",
        host: Some("telemetry.example"),
        path: "/live",
        outcome: RequestOutcome::Completed,
        status_code: 200,
        attempts: 2,
        latency: Duration::from_micros(150),
        upstream_bytes_sent: 80,
        downstream_bytes_sent: 200,
    });
    observability.record(RequestObservation {
        request_id: observability.next_request_id(),
        runtime: RuntimeMode::Async,
        service_id: "telemetry",
        algorithm: Some(AlgorithmKind::LeastConnections),
        backend_id: "none",
        method: "GET",
        host: Some("telemetry.example"),
        path: "/live",
        outcome: RequestOutcome::NoEligibleBackend,
        status_code: 503,
        attempts: 0,
        latency: Duration::from_micros(250),
        upstream_bytes_sent: 0,
        downstream_bytes_sent: 100,
    });

    let registry = ServiceRegistry::new();
    registry
        .add(
            Service::proxy(
                "telemetry",
                vec![RouteMatcher::new(None::<String>, "/").unwrap()],
                Arc::new(
                    BackendPool::new(
                        AlgorithmKind::LeastConnections,
                        vec![Backend {
                            id: "car-1".into(),
                            address: "127.0.0.1:8080".into(),
                            weight: 1,
                        }],
                    )
                    .unwrap(),
                ),
            )
            .unwrap(),
        )
        .unwrap();

    let snapshot = observability.snapshot(&registry);
    let metrics = snapshot
        .requests
        .iter()
        .find(|entry| entry.runtime == RuntimeMode::Async)
        .unwrap();
    assert_eq!(metrics.total_requests, 2);
    assert_eq!(metrics.successful_requests, 1);
    assert_eq!(metrics.failed_requests, 1);
    assert_eq!(metrics.total_attempts, 2);
    assert_eq!(metrics.total_retries, 1);
    assert_eq!(metrics.average_latency_us, 200);
    assert_eq!(metrics.min_latency_us, 150);
    assert_eq!(metrics.max_latency_us, 250);
    assert_eq!(metrics.status_2xx, 1);
    assert_eq!(metrics.status_5xx, 1);
    assert_eq!(metrics.no_eligible_backend, 1);
    assert_eq!(snapshot.services[0].service_id, "telemetry");
    assert_eq!(snapshot.services[0].backends.len(), 1);
}

#[test]
fn serializes_a_stable_request_event() {
    let event = RequestLogEvent {
        schema_version: SCHEMA_VERSION,
        event: "request_completed",
        timestamp_unix_ms: 123,
        run_id: "run-1",
        request_id: 7,
        runtime: RuntimeMode::ThreadPool,
        service_id: "timing",
        algorithm: Some(AlgorithmKind::RoundRobin),
        backend_id: "car-2",
        method: "GET",
        host: None,
        path: "/sector",
        outcome: RequestOutcome::Completed,
        success: true,
        status_code: 200,
        attempts: 1,
        latency_us: 42,
        upstream_bytes_sent: 10,
        downstream_bytes_sent: 20,
    };

    let value = serde_json::to_value(event).unwrap();
    assert_eq!(value["schema_version"], 1);
    assert_eq!(value["event"], "request_completed");
    assert_eq!(value["runtime"], "thread_pool");
    assert_eq!(value["outcome"], "completed");
    assert_eq!(value["success"], true);
    assert_eq!(value["algorithm"], "round_robin");
}

#[test]
fn records_services_added_after_observability_startup() {
    let observability = Observability::new(std::iter::empty::<String>(), false);
    observability.record(RequestObservation {
        request_id: observability.next_request_id(),
        runtime: RuntimeMode::ThreadPool,
        service_id: "dynamic",
        algorithm: Some(AlgorithmKind::RoundRobin),
        backend_id: "late",
        method: "GET",
        host: None,
        path: "/dynamic",
        outcome: RequestOutcome::Completed,
        status_code: 200,
        attempts: 1,
        latency: Duration::from_micros(75),
        upstream_bytes_sent: 10,
        downstream_bytes_sent: 20,
    });

    let registry = ServiceRegistry::new();
    registry
        .add(
            Service::proxy(
                "dynamic",
                vec![RouteMatcher::new(None::<String>, "/dynamic").unwrap()],
                Arc::new(
                    BackendPool::new(
                        AlgorithmKind::RoundRobin,
                        vec![Backend {
                            id: "late".into(),
                            address: "127.0.0.1:8080".into(),
                            weight: 1,
                        }],
                    )
                    .unwrap(),
                ),
            )
            .unwrap(),
        )
        .unwrap();

    let snapshot = observability.snapshot(&registry);
    let metrics = snapshot
        .requests
        .iter()
        .find(|entry| entry.service_id == "dynamic" && entry.runtime == RuntimeMode::ThreadPool)
        .unwrap();
    assert_eq!(metrics.total_requests, 1);
    assert_eq!(metrics.successful_requests, 1);
    assert_eq!(snapshot.services[0].service_id, "dynamic");
}
