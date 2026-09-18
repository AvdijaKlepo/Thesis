use super::*;
use crate::{Backend, algorithms::AlgorithmKind, backend::BackendPool};

fn request(method: &str) -> ClientRequest {
    ClientRequest {
        raw: format!("{method} / HTTP/1.1\r\nConnection: close\r\n\r\n").into(),
        header_end: 0,
        method: method.into(),
        host: None,
        path: "/".into(),
        is_idempotent: method == "GET",
        keep_alive: false,
    }
}

fn exchange<'a>(request: &'a ClientRequest, observability: &'a Observability) -> ProxyExchange<'a> {
    let pool = Arc::new(
        BackendPool::new(
            AlgorithmKind::RoundRobin,
            vec![Backend {
                id: "backend".into(),
                address: "127.0.0.1:1".into(),
                weight: 1,
            }],
        )
        .unwrap(),
    );
    ProxyExchange::new(
        RequestContext {
            request_id: 1,
            runtime: RuntimeMode::ThreadPool,
            service_id: "default".into(),
            algorithm: Some(AlgorithmKind::RoundRobin),
            started: Instant::now(),
            request,
            observability,
        },
        pool,
    )
}

#[test]
fn retries_connect_failures_for_any_method() {
    let request = request("POST");
    let observability = Observability::new(["default"], false);
    let mut exchange = exchange(&request, &observability);
    let NextAttempt::Ready(attempt) = exchange.next_attempt() else {
        panic!("attempt expected");
    };
    assert!(exchange.attempt_failed(attempt, AttemptFailure::Connect, 0, 0));
}

#[test]
fn exhausting_distinct_backends_produces_bad_gateway() {
    let request = request("GET");
    let observability = Observability::new(["default"], false);
    let mut exchange = exchange(&request, &observability);
    let NextAttempt::Ready(attempt) = exchange.next_attempt() else {
        panic!("attempt expected");
    };
    assert!(exchange.attempt_failed(attempt, AttemptFailure::Connect, 0, 0));

    assert!(matches!(exchange.next_attempt(), NextAttempt::Exhausted));
    let result = exchange.attempts_failed(0);
    assert_eq!(result.status_code, 502);
    assert_eq!(result.outcome, RequestOutcome::BackendAttemptsFailed);
    assert_eq!(result.attempts, 1);
}

#[test]
fn retries_post_write_failures_only_for_idempotent_methods() {
    for (method, expected) in [("GET", true), ("POST", false)] {
        let request = request(method);
        let observability = Observability::new(["default"], false);
        let mut exchange = exchange(&request, &observability);
        let NextAttempt::Ready(attempt) = exchange.next_attempt() else {
            panic!("attempt expected");
        };
        assert_eq!(
            exchange.attempt_failed(attempt, AttemptFailure::Response, 10, 0),
            expected
        );
    }
}
