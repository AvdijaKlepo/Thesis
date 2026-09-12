use std::{
    io::{Read, Write},
    net::{Shutdown, TcpListener, TcpStream},
    path::PathBuf,
    sync::Arc,
    thread,
};

use crate::{
    Backend,
    algorithms::AlgorithmKind,
    backend::BackendPool,
    observability::{Observability, RequestOutcome},
    proxy::{
        RuntimeMode,
        behavior::ProxyResult,
        connection::{proxy_connections, read_http_request},
        runtime::proxy_connections_async,
    },
    service::{RouteMatcher, Service, ServiceRegistry, ServiceRouter},
};

fn exercise_runtime(
    runtime: RuntimeMode,
    router: &ServiceRouter,
    observability: &Observability,
    request: &[u8],
) -> (Vec<u8>, ProxyResult) {
    match runtime {
        RuntimeMode::ThreadPool => exercise_blocking(router, observability, request),
        RuntimeMode::Async => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(exercise_async(router, observability, request)),
    }
}

fn exercise_blocking(
    router: &ServiceRouter,
    observability: &Observability,
    request: &[u8],
) -> (Vec<u8>, ProxyResult) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let request = request.to_vec();
    let client = thread::spawn(move || {
        let mut stream = TcpStream::connect(address).unwrap();
        stream.write_all(&request).unwrap();
        stream.shutdown(Shutdown::Write).unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).unwrap();
        response
    });

    let (stream, _) = listener.accept().unwrap();
    let result = proxy_connections(stream, router, observability).unwrap();
    (client.join().unwrap(), result)
}

async fn exercise_async(
    router: &ServiceRouter,
    observability: &Observability,
    request: &[u8],
) -> (Vec<u8>, ProxyResult) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let request = request.to_vec();
    let client = tokio::spawn(async move {
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream.write_all(&request).await.unwrap();
        stream.shutdown().await.unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        response
    });

    let (stream, _) = listener.accept().await.unwrap();
    let result = proxy_connections_async(stream, router, observability)
        .await
        .unwrap();
    (client.await.unwrap(), result)
}

fn backend(id: &str, address: impl ToString) -> Backend {
    Backend {
        id: id.into(),
        address: address.to_string(),
        weight: 1,
    }
}

fn router_with_default_pool(pool: Arc<BackendPool>) -> (Arc<ServiceRegistry>, ServiceRouter) {
    let registry = Arc::new(ServiceRegistry::new());
    registry
        .add(
            Service::proxy(
                "default",
                vec![RouteMatcher::new(None::<String>, "/").unwrap()],
                pool,
            )
            .unwrap(),
        )
        .unwrap();
    let router = ServiceRouter::new(Arc::clone(&registry), "default").unwrap();
    (registry, router)
}

fn responding_upstream(
    response: &'static [u8],
) -> (std::net::SocketAddr, thread::JoinHandle<Vec<u8>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let request = read_http_request(&mut stream).unwrap().unwrap();
        stream.write_all(response).unwrap();
        request.raw
    });
    (address, handle)
}

fn closing_upstream() -> (std::net::SocketAddr, thread::JoinHandle<Vec<u8>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_http_request(&mut stream).unwrap().unwrap().raw
    });
    (address, handle)
}

fn unused_address() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    address
}

fn empty_pool_case(runtime: RuntimeMode) {
    let pool = Arc::new(BackendPool::new(AlgorithmKind::RoundRobin, Vec::new()).unwrap());
    let (registry, router) = router_with_default_pool(pool);
    let observability = Observability::new(["default"], false);
    let (response, result) = exercise_runtime(
        runtime,
        &router,
        &observability,
        b"GET / HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n",
    );

    assert!(response.starts_with(b"HTTP/1.1 503 Service Unavailable"));
    assert_eq!(result.runtime, runtime);
    assert_eq!(result.outcome, RequestOutcome::NoEligibleBackend);
    assert_eq!(result.status_code, 503);
    assert_eq!(result.attempts, 0);
    assert_eq!(result.backend_id, "none");

    let snapshot = observability.snapshot(&registry);
    let metrics = snapshot
        .requests
        .iter()
        .find(|metrics| metrics.runtime == runtime && metrics.service_id == "default")
        .unwrap();
    assert_eq!(metrics.total_requests, 1);
    assert_eq!(metrics.no_eligible_backend, 1);
    assert_eq!(metrics.status_5xx, 1);
}

fn unhealthy_pool_case(runtime: RuntimeMode) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let pool = Arc::new(
        BackendPool::new(
            AlgorithmKind::RoundRobin,
            vec![backend("unhealthy", listener.local_addr().unwrap())],
        )
        .unwrap(),
    );
    pool.backends()[0]
        .healthy
        .store(false, std::sync::atomic::Ordering::Relaxed);
    let (_registry, router) = router_with_default_pool(pool);
    let observability = Observability::new(["default"], false);
    let (response, result) = exercise_runtime(
        runtime,
        &router,
        &observability,
        b"GET /health-rule HTTP/1.1\r\nConnection: close\r\n\r\n",
    );

    assert!(response.starts_with(b"HTTP/1.1 503 Service Unavailable"));
    assert_eq!(result.outcome, RequestOutcome::NoEligibleBackend);
    assert_eq!(result.attempts, 0);
    listener.set_nonblocking(true).unwrap();
    assert_eq!(
        listener.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
}

fn routing_framing_and_metrics_case(runtime: RuntimeMode) {
    let response =
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n9\r\ntelemetry\r\n0\r\n\r\n";
    let (upstream_address, upstream) = responding_upstream(response);
    let registry = Arc::new(ServiceRegistry::new());
    registry
        .add(
            Service::proxy(
                "default",
                vec![RouteMatcher::new(None::<String>, "/fallback").unwrap()],
                Arc::new(BackendPool::new(AlgorithmKind::RoundRobin, Vec::new()).unwrap()),
            )
            .unwrap(),
        )
        .unwrap();
    registry
        .add(
            Service::proxy(
                "telemetry",
                vec![RouteMatcher::new(Some("telemetry.example"), "/live").unwrap()],
                Arc::new(
                    BackendPool::new(
                        AlgorithmKind::LeastConnections,
                        vec![backend("telemetry-backend", upstream_address)],
                    )
                    .unwrap(),
                ),
            )
            .unwrap(),
        )
        .unwrap();
    let router = ServiceRouter::new(Arc::clone(&registry), "default").unwrap();
    let observability = Observability::new(["default", "telemetry"], false);
    let (proxy_response, result) = exercise_runtime(
        runtime,
        &router,
        &observability,
        b"GET /live/lap?session=1 HTTP/1.1\r\nHost: telemetry.example:7879\r\nConnection: close\r\n\r\n",
    );
    let upstream_request = upstream.join().unwrap();

    assert_eq!(proxy_response, response);
    assert!(upstream_request.starts_with(b"GET /live/lap?session=1 HTTP/1.1"));
    assert!(String::from_utf8_lossy(&upstream_request).contains("Connection: close\r\n"));
    assert_eq!(result.service_id, "telemetry");
    assert_eq!(result.backend_id, "telemetry-backend");
    assert_eq!(result.status_code, 200);
    assert_eq!(result.attempts, 1);

    let snapshot = observability.snapshot(&registry);
    let metrics = snapshot
        .requests
        .iter()
        .find(|metrics| metrics.runtime == runtime && metrics.service_id == "telemetry")
        .unwrap();
    assert_eq!(metrics.total_requests, 1);
    assert_eq!(metrics.successful_requests, 1);
    assert_eq!(metrics.total_attempts, 1);
    assert_eq!(metrics.status_2xx, 1);
}

fn idempotent_retry_case(runtime: RuntimeMode) {
    let dead_address = unused_address();
    let response = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
    let (healthy_address, upstream) = responding_upstream(response);
    let pool = Arc::new(
        BackendPool::new(
            AlgorithmKind::RoundRobin,
            vec![
                backend("dead", dead_address),
                backend("healthy", healthy_address),
            ],
        )
        .unwrap(),
    );
    let (registry, router) = router_with_default_pool(Arc::clone(&pool));
    let observability = Observability::new(["default"], false);
    let (proxy_response, result) = exercise_runtime(
        runtime,
        &router,
        &observability,
        b"GET /retry HTTP/1.1\r\nConnection: close\r\n\r\n",
    );
    upstream.join().unwrap();

    assert_eq!(proxy_response, response);
    assert_eq!(result.outcome, RequestOutcome::Completed);
    assert_eq!(result.backend_id, "healthy");
    assert_eq!(result.attempts, 2);
    let backend_metrics = pool.metrics_summary();
    assert_eq!(backend_metrics[0].metrics.failed_requests, 1);
    assert_eq!(backend_metrics[1].metrics.successful_requests, 1);

    let snapshot = observability.snapshot(&registry);
    let metrics = snapshot
        .requests
        .iter()
        .find(|metrics| metrics.runtime == runtime && metrics.service_id == "default")
        .unwrap();
    assert_eq!(metrics.total_attempts, 2);
    assert_eq!(metrics.total_retries, 1);
}

fn non_idempotent_response_failure_case(runtime: RuntimeMode) {
    let (failing_address, failing_upstream) = closing_upstream();
    let unused_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let pool = Arc::new(
        BackendPool::new(
            AlgorithmKind::RoundRobin,
            vec![
                backend("failing", failing_address),
                backend("must-not-run", unused_listener.local_addr().unwrap()),
            ],
        )
        .unwrap(),
    );
    let (_registry, router) = router_with_default_pool(pool);
    let observability = Observability::new(["default"], false);
    let body = b"important";
    let request = format!(
        "POST /command HTTP/1.1\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        String::from_utf8_lossy(body)
    );
    let (response, result) = exercise_runtime(runtime, &router, &observability, request.as_bytes());
    let upstream_request = failing_upstream.join().unwrap();

    assert!(upstream_request.ends_with(body));
    assert!(response.ends_with(b"502 Bad Gateway: All backend attempts failed\n"));
    assert_eq!(result.outcome, RequestOutcome::BackendAttemptsFailed);
    assert_eq!(result.attempts, 1);
    unused_listener.set_nonblocking(true).unwrap();
    assert_eq!(
        unused_listener.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
}

fn static_service_case(runtime: RuntimeMode) {
    let registry = Arc::new(ServiceRegistry::new());
    registry
        .add(
            Service::proxy(
                "default",
                vec![RouteMatcher::new(None::<String>, "/fallback").unwrap()],
                Arc::new(BackendPool::new(AlgorithmKind::RoundRobin, Vec::new()).unwrap()),
            )
            .unwrap(),
        )
        .unwrap();
    registry
        .add(
            Service::static_files(
                "dashboard",
                vec![RouteMatcher::new(Some("dashboard.example"), "/").unwrap()],
                PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("web"),
            )
            .unwrap(),
        )
        .unwrap();
    let router = ServiceRouter::new(registry, "default").unwrap();
    let observability = Observability::new(["default", "dashboard"], false);
    let (response, result) = exercise_runtime(
        runtime,
        &router,
        &observability,
        b"GET / HTTP/1.1\r\nHost: dashboard.example\r\nConnection: close\r\n\r\n",
    );

    assert!(response.starts_with(b"HTTP/1.1 200 OK"));
    assert_eq!(result.service_id, "dashboard");
    assert_eq!(result.backend_id, "static");
    assert_eq!(result.attempts, 0);
    assert!(result.success);
}

macro_rules! runtime_contract {
    ($module:ident, $case:ident) => {
        mod $module {
            use super::*;

            #[test]
            fn thread_pool() {
                $case(RuntimeMode::ThreadPool);
            }

            #[test]
            fn async_runtime() {
                $case(RuntimeMode::Async);
            }
        }
    };
}

runtime_contract!(empty_pool, empty_pool_case);
runtime_contract!(unhealthy_pool, unhealthy_pool_case);
runtime_contract!(
    routing_framing_and_metrics,
    routing_framing_and_metrics_case
);
runtime_contract!(idempotent_retry, idempotent_retry_case);
runtime_contract!(
    non_idempotent_response_failure,
    non_idempotent_response_failure_case
);
runtime_contract!(static_service, static_service_case);
