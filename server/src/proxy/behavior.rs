//! Runtime-neutral routing, retry, health-selection, and metrics policy.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{
    algorithms::AlgorithmKind,
    backend::{
        BackendPool, BackendSelection, BackendSelectionError,
        backend_server::{BackendMetrics, Feedback},
    },
    control::StaticFileHandler,
    observability::{Observability, RequestObservation, RequestOutcome},
    proxy::{mode::RuntimeMode, protocol::ClientRequest},
    service::{ServiceRouter, ServiceTarget},
};

pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
pub const READ_TIMEOUT: Duration = Duration::from_secs(10);
pub const WRITE_TIMEOUT: Duration = Duration::from_secs(10);
pub const MAX_RETRIES: usize = 2;
const MAX_ATTEMPTS: usize = MAX_RETRIES + 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttemptFailure {
    Connect,
    Write,
    Response,
}

pub enum RequestPlan<'a> {
    Proxy(ProxyExchange<'a>),
    Static(StaticExchange<'a>),
}

pub fn plan_request<'a>(
    runtime: RuntimeMode,
    request: &'a ClientRequest,
    router: &ServiceRouter,
    observability: &'a Observability,
) -> RequestPlan<'a> {
    let started = Instant::now();
    let service = router.resolve(request.host.as_deref(), &request.path);
    let context = RequestContext {
        request_id: observability.next_request_id(),
        runtime,
        service_id: service.id.clone(),
        algorithm: service.proxy_pool().map(|pool| pool.algorithm()),
        started,
        request,
        observability,
    };

    match &service.target {
        ServiceTarget::Proxy(pool) => {
            RequestPlan::Proxy(ProxyExchange::new(context, Arc::clone(pool)))
        }
        ServiceTarget::Static { root } => RequestPlan::Static(StaticExchange {
            context,
            root: root.clone(),
        }),
    }
}

pub struct StaticExchange<'a> {
    context: RequestContext<'a>,
    root: PathBuf,
}

impl StaticExchange<'_> {
    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn path(&self) -> &str {
        &self.context.request.path
    }

    pub fn complete(
        self,
        response_length: usize,
        status_code: u16,
        served: bool,
        written: bool,
    ) -> ProxyResult {
        let outcome = if !served {
            RequestOutcome::StaticFailure
        } else if !written {
            RequestOutcome::ClientWriteFailure
        } else {
            RequestOutcome::Completed
        };
        self.context
            .finish("static".into(), outcome, status_code, 0, 0, response_length)
    }
}

pub struct StaticResponse {
    pub bytes: Vec<u8>,
    pub status_code: u16,
    pub served: bool,
}

pub fn load_static_response(root: &Path, path: &str) -> StaticResponse {
    match StaticFileHandler::new(root.to_path_buf()).serve(path) {
        Ok(response) => StaticResponse {
            status_code: response.status_code(),
            bytes: response.to_http(),
            served: true,
        },
        Err(error) => {
            eprintln!("Static service failed: {error}");
            StaticResponse {
                bytes: internal_server_error_response(),
                status_code: 500,
                served: false,
            }
        }
    }
}

pub struct ProxyExchange<'a> {
    context: RequestContext<'a>,
    backend_pool: Arc<BackendPool>,
    upstream_request: Vec<u8>,
    attempts: usize,
    upstream_bytes_sent: usize,
    downstream_bytes_sent: usize,
    last_backend_id: String,
}

impl<'a> ProxyExchange<'a> {
    fn new(context: RequestContext<'a>, backend_pool: Arc<BackendPool>) -> Self {
        let upstream_request =
            crate::proxy::protocol::prepare_upstream_request(&context.request.raw);
        Self {
            context,
            backend_pool,
            upstream_request,
            attempts: 0,
            upstream_bytes_sent: 0,
            downstream_bytes_sent: 0,
            last_backend_id: "none".into(),
        }
    }

    pub fn upstream_request(&self) -> &[u8] {
        &self.upstream_request
    }

    pub fn next_attempt(&mut self) -> NextAttempt {
        if self.attempts >= MAX_ATTEMPTS {
            return NextAttempt::Exhausted;
        }

        match self.backend_pool.select_backend() {
            Ok(backend) => {
                self.attempts += 1;
                self.last_backend_id = backend.backend.id.clone();
                NextAttempt::Ready(ProxyAttempt::new(backend))
            }
            Err(error) => NextAttempt::NoEligibleBackend(error),
        }
    }

    /// Records a failed transport attempt and returns whether policy permits
    /// another attempt. Connect failures are safe to retry because no request
    /// bytes reached an upstream. Once writing starts, only idempotent methods
    /// may be retried.
    pub fn attempt_failed(
        &mut self,
        attempt: ProxyAttempt,
        failure: AttemptFailure,
        upstream_bytes_sent: usize,
        downstream_bytes_sent: usize,
    ) -> bool {
        self.upstream_bytes_sent += upstream_bytes_sent;
        self.downstream_bytes_sent += downstream_bytes_sent;
        attempt.complete(false, upstream_bytes_sent, downstream_bytes_sent);

        self.attempts < MAX_ATTEMPTS
            && (failure == AttemptFailure::Connect || self.context.request.is_idempotent)
    }

    pub fn attempt_succeeded(
        mut self,
        attempt: ProxyAttempt,
        status_code: u16,
        upstream_bytes_sent: usize,
        downstream_bytes_sent: usize,
    ) -> ProxyResult {
        self.upstream_bytes_sent += upstream_bytes_sent;
        self.downstream_bytes_sent += downstream_bytes_sent;
        let backend_id = attempt.backend_id().to_string();
        attempt.complete(true, upstream_bytes_sent, downstream_bytes_sent);
        self.context.finish(
            backend_id,
            RequestOutcome::Completed,
            status_code,
            self.attempts,
            self.upstream_bytes_sent,
            self.downstream_bytes_sent,
        )
    }

    pub fn no_eligible_backend(self, response_length: usize) -> ProxyResult {
        self.context.finish(
            self.last_backend_id,
            RequestOutcome::NoEligibleBackend,
            503,
            self.attempts,
            self.upstream_bytes_sent,
            response_length,
        )
    }

    pub fn attempts_failed(self, response_length: usize) -> ProxyResult {
        self.context.finish(
            self.last_backend_id,
            RequestOutcome::BackendAttemptsFailed,
            502,
            self.attempts,
            self.upstream_bytes_sent,
            response_length,
        )
    }
}

pub enum NextAttempt {
    Ready(ProxyAttempt),
    NoEligibleBackend(BackendSelectionError),
    Exhausted,
}

pub struct ProxyAttempt {
    backend: BackendSelection,
    metrics: ActiveConnectionGuard,
    started: Instant,
}

impl ProxyAttempt {
    fn new(backend: BackendSelection) -> Self {
        Self {
            metrics: ActiveConnectionGuard::new(Arc::clone(&backend.metrics)),
            backend,
            started: Instant::now(),
        }
    }

    pub fn backend_id(&self) -> &str {
        &self.backend.backend.id
    }

    pub fn address(&self) -> &str {
        &self.backend.backend.address
    }

    fn complete(self, success: bool, bytes_sent: usize, bytes_received: usize) {
        let feedback = Feedback {
            latency: self.started.elapsed(),
            success,
        };
        self.backend.release(feedback.clone());
        self.metrics.complete(&feedback, bytes_sent, bytes_received);
    }
}

struct ActiveConnectionGuard {
    metrics: Arc<BackendMetrics>,
    completed: bool,
}

impl ActiveConnectionGuard {
    fn new(metrics: Arc<BackendMetrics>) -> Self {
        metrics.record_start();
        Self {
            metrics,
            completed: false,
        }
    }

    fn complete(mut self, feedback: &Feedback, bytes_sent: usize, bytes_received: usize) {
        self.completed = true;
        self.metrics
            .record_end(feedback, bytes_sent, bytes_received);
    }
}

impl Drop for ActiveConnectionGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.metrics.record_end(
                &Feedback {
                    latency: Duration::ZERO,
                    success: false,
                },
                0,
                0,
            );
        }
    }
}

struct RequestContext<'a> {
    request_id: u64,
    runtime: RuntimeMode,
    service_id: String,
    algorithm: Option<AlgorithmKind>,
    started: Instant,
    request: &'a ClientRequest,
    observability: &'a Observability,
}

impl RequestContext<'_> {
    fn finish(
        self,
        backend_id: String,
        outcome: RequestOutcome,
        status_code: u16,
        attempts: usize,
        upstream_bytes_sent: usize,
        downstream_bytes_sent: usize,
    ) -> ProxyResult {
        let result = ProxyResult {
            request_id: self.request_id,
            runtime: self.runtime,
            service_id: self.service_id,
            backend_id,
            outcome,
            status_code,
            attempts,
            latency: self.started.elapsed(),
            success: outcome.is_success(),
            bytes_sent: upstream_bytes_sent,
            bytes_received: downstream_bytes_sent,
        };

        self.observability.record(RequestObservation {
            request_id: result.request_id,
            runtime: result.runtime,
            service_id: &result.service_id,
            algorithm: self.algorithm,
            backend_id: &result.backend_id,
            method: &self.request.method,
            host: self.request.host.as_deref(),
            path: &self.request.path,
            outcome: result.outcome,
            status_code: result.status_code,
            attempts: result.attempts,
            latency: result.latency,
            upstream_bytes_sent: result.bytes_sent,
            downstream_bytes_sent: result.bytes_received,
        });

        result
    }
}

#[derive(Debug)]
pub struct ProxyResult {
    pub request_id: u64,
    pub runtime: RuntimeMode,
    pub service_id: String,
    pub backend_id: String,
    pub outcome: RequestOutcome,
    pub status_code: u16,
    pub attempts: usize,
    pub latency: Duration,
    pub success: bool,
    pub bytes_sent: usize,
    pub bytes_received: usize,
}

pub fn service_unavailable_response() -> Vec<u8> {
    error_response(503, "Service Unavailable", "No eligible backend")
}

pub fn bad_gateway_response() -> Vec<u8> {
    error_response(502, "Bad Gateway", "All backend attempts failed")
}

pub fn internal_server_error_response() -> Vec<u8> {
    let body = "500 Internal Server Error\n";
    format!(
        "HTTP/1.1 500 Internal Server Error\r\nContent-Length: {}\r\nConnection: close\r\nContent-Type: text/plain\r\n\r\n{body}",
        body.len()
    )
    .into_bytes()
}

fn error_response(status: u16, reason: &str, message: &str) -> Vec<u8> {
    let body = format!("{status} {reason}: {message}\n");
    format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: close\r\nContent-Type: text/plain\r\n\r\n{body}",
        body.len()
    )
    .into_bytes()
}

#[cfg(test)]
mod tests {
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

    fn exchange<'a>(
        request: &'a ClientRequest,
        observability: &'a Observability,
    ) -> ProxyExchange<'a> {
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
}
