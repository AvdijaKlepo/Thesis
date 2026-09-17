use std::{
    collections::HashMap,
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde::Serialize;

use crate::{
    algorithms::AlgorithmKind,
    algorithms::balancers::AdaptiveDiagnosticSnapshot,
    backend::registry::BackendMetricsReport,
    proxy::RuntimeMode,
    service::{RouteMatcher, ServiceRegistry, ServiceTarget},
};

const SCHEMA_VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestOutcome {
    Completed,
    NoEligibleBackend,
    BackendAttemptsFailed,
    StaticFailure,
    ClientWriteFailure,
}

impl RequestOutcome {
    pub fn is_success(self) -> bool {
        self == Self::Completed
    }
}

pub struct RequestObservation<'a> {
    pub request_id: u64,
    pub runtime: RuntimeMode,
    pub service_id: &'a str,
    pub algorithm: Option<AlgorithmKind>,
    pub backend_id: &'a str,
    pub method: &'a str,
    pub host: Option<&'a str>,
    pub path: &'a str,
    pub outcome: RequestOutcome,
    pub status_code: u16,
    pub attempts: usize,
    pub latency: Duration,
    pub upstream_bytes_sent: usize,
    pub downstream_bytes_sent: usize,
}

pub struct Observability {
    run_id: String,
    started: Instant,
    next_request_id: AtomicU64,
    log_requests: bool,
    request_metrics: RwLock<HashMap<String, Arc<ServiceRequestMetrics>>>,
}

impl Observability {
    pub fn new<I, S>(service_ids: I, log_requests: bool) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut request_metrics = HashMap::new();
        for service_id in service_ids {
            request_metrics.insert(
                service_id.into(),
                Arc::new(ServiceRequestMetrics::default()),
            );
        }

        let timestamp_ms = unix_timestamp_ms();
        Self {
            run_id: format!("{timestamp_ms}-{}", std::process::id()),
            started: Instant::now(),
            next_request_id: AtomicU64::new(1),
            log_requests,
            request_metrics: RwLock::new(request_metrics),
        }
    }

    pub fn next_request_id(&self) -> u64 {
        self.next_request_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn record(&self, observation: RequestObservation<'_>) {
        self.metrics_for(observation.service_id)
            .for_runtime(observation.runtime)
            .record(&observation);

        if self.log_requests {
            let event = RequestLogEvent {
                schema_version: SCHEMA_VERSION,
                event: "request_completed",
                timestamp_unix_ms: unix_timestamp_ms(),
                run_id: &self.run_id,
                request_id: observation.request_id,
                runtime: observation.runtime,
                service_id: observation.service_id,
                algorithm: observation.algorithm,
                backend_id: observation.backend_id,
                method: observation.method,
                host: observation.host,
                path: observation.path,
                outcome: observation.outcome,
                success: observation.outcome.is_success(),
                status_code: observation.status_code,
                attempts: observation.attempts,
                latency_us: duration_micros(observation.latency),
                upstream_bytes_sent: observation.upstream_bytes_sent,
                downstream_bytes_sent: observation.downstream_bytes_sent,
            };
            if let Ok(line) = serde_json::to_string(&event) {
                println!("{line}");
            }
        }
    }

    pub fn snapshot(&self, services: &ServiceRegistry) -> ObservabilitySnapshot {
        for service in services.all() {
            self.metrics_for(&service.id);
        }
        let request_metrics = self
            .request_metrics
            .read()
            .unwrap()
            .iter()
            .map(|(id, metrics)| (id.clone(), Arc::clone(metrics)))
            .collect::<Vec<_>>();
        let mut requests = request_metrics
            .iter()
            .flat_map(|(service_id, metrics)| {
                [
                    metrics
                        .thread_pool
                        .snapshot(service_id, RuntimeMode::ThreadPool),
                    metrics
                        .async_runtime
                        .snapshot(service_id, RuntimeMode::Async),
                ]
            })
            .collect::<Vec<_>>();
        requests.sort_by(|left, right| {
            left.service_id
                .cmp(&right.service_id)
                .then_with(|| left.runtime.as_str().cmp(right.runtime.as_str()))
        });

        let services = services
            .all()
            .into_iter()
            .map(|service| match &service.target {
                ServiceTarget::Proxy(pool) => ServiceMetricsSnapshot {
                    service_id: service.id.clone(),
                    target: "proxy",
                    routes: service.routes.clone(),
                    algorithm: Some(pool.algorithm()),
                    fail_open: Some(pool.fail_open()),
                    backends: pool.metrics_summary(),
                    adaptive_diagnostics: pool.load_balancer().load().adaptive_diagnostics(),
                },
                ServiceTarget::Static { .. } => ServiceMetricsSnapshot {
                    service_id: service.id.clone(),
                    target: "static",
                    routes: service.routes.clone(),
                    algorithm: None,
                    fail_open: None,
                    backends: Vec::new(),
                    adaptive_diagnostics: None,
                },
            })
            .collect();

        ObservabilitySnapshot {
            schema_version: SCHEMA_VERSION,
            run_id: self.run_id.clone(),
            uptime_ms: duration_millis(self.started.elapsed()),
            requests,
            services,
        }
    }

    fn metrics_for(&self, service_id: &str) -> Arc<ServiceRequestMetrics> {
        if let Some(metrics) = self
            .request_metrics
            .read()
            .unwrap()
            .get(service_id)
            .cloned()
        {
            return metrics;
        }
        let mut request_metrics = self.request_metrics.write().unwrap();
        Arc::clone(
            request_metrics
                .entry(service_id.to_string())
                .or_insert_with(|| Arc::new(ServiceRequestMetrics::default())),
        )
    }
}

#[derive(Default)]
struct ServiceRequestMetrics {
    thread_pool: RequestMetrics,
    async_runtime: RequestMetrics,
}

impl ServiceRequestMetrics {
    fn for_runtime(&self, runtime: RuntimeMode) -> &RequestMetrics {
        match runtime {
            RuntimeMode::ThreadPool => &self.thread_pool,
            RuntimeMode::Async => &self.async_runtime,
        }
    }
}

#[derive(Default)]
struct RequestMetrics {
    total_requests: AtomicU64,
    successful_requests: AtomicU64,
    failed_requests: AtomicU64,
    total_attempts: AtomicU64,
    total_retries: AtomicU64,
    total_latency_us: AtomicU64,
    min_latency_us: AtomicU64,
    max_latency_us: AtomicU64,
    total_upstream_bytes_sent: AtomicU64,
    total_downstream_bytes_sent: AtomicU64,
    status_1xx: AtomicU64,
    status_2xx: AtomicU64,
    status_3xx: AtomicU64,
    status_4xx: AtomicU64,
    status_5xx: AtomicU64,
    status_other: AtomicU64,
    no_eligible_backend: AtomicU64,
    backend_attempt_failures: AtomicU64,
    static_failures: AtomicU64,
    client_write_failures: AtomicU64,
}

impl RequestMetrics {
    fn record(&self, observation: &RequestObservation<'_>) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        if observation.outcome.is_success() {
            self.successful_requests.fetch_add(1, Ordering::Relaxed);
        } else {
            self.failed_requests.fetch_add(1, Ordering::Relaxed);
        }
        self.total_attempts
            .fetch_add(observation.attempts as u64, Ordering::Relaxed);
        self.total_retries.fetch_add(
            observation.attempts.saturating_sub(1) as u64,
            Ordering::Relaxed,
        );

        let latency_us = duration_micros(observation.latency);
        self.total_latency_us
            .fetch_add(latency_us, Ordering::Relaxed);
        update_min(&self.min_latency_us, latency_us);
        self.max_latency_us.fetch_max(latency_us, Ordering::Relaxed);
        self.total_upstream_bytes_sent
            .fetch_add(observation.upstream_bytes_sent as u64, Ordering::Relaxed);
        self.total_downstream_bytes_sent
            .fetch_add(observation.downstream_bytes_sent as u64, Ordering::Relaxed);

        match observation.status_code {
            100..=199 => &self.status_1xx,
            200..=299 => &self.status_2xx,
            300..=399 => &self.status_3xx,
            400..=499 => &self.status_4xx,
            500..=599 => &self.status_5xx,
            _ => &self.status_other,
        }
        .fetch_add(1, Ordering::Relaxed);

        match observation.outcome {
            RequestOutcome::Completed => {}
            RequestOutcome::NoEligibleBackend => {
                self.no_eligible_backend.fetch_add(1, Ordering::Relaxed);
            }
            RequestOutcome::BackendAttemptsFailed => {
                self.backend_attempt_failures
                    .fetch_add(1, Ordering::Relaxed);
            }
            RequestOutcome::StaticFailure => {
                self.static_failures.fetch_add(1, Ordering::Relaxed);
            }
            RequestOutcome::ClientWriteFailure => {
                self.client_write_failures.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn snapshot(&self, service_id: &str, runtime: RuntimeMode) -> RequestMetricsSnapshot {
        let total_requests = self.total_requests.load(Ordering::Relaxed);
        let total_latency_us = self.total_latency_us.load(Ordering::Relaxed);
        RequestMetricsSnapshot {
            service_id: service_id.to_string(),
            runtime,
            total_requests,
            successful_requests: self.successful_requests.load(Ordering::Relaxed),
            failed_requests: self.failed_requests.load(Ordering::Relaxed),
            total_attempts: self.total_attempts.load(Ordering::Relaxed),
            total_retries: self.total_retries.load(Ordering::Relaxed),
            total_latency_us,
            average_latency_us: total_latency_us
                .checked_div(total_requests)
                .unwrap_or_default(),
            min_latency_us: if total_requests > 0 {
                self.min_latency_us.load(Ordering::Relaxed)
            } else {
                0
            },
            max_latency_us: self.max_latency_us.load(Ordering::Relaxed),
            total_upstream_bytes_sent: self.total_upstream_bytes_sent.load(Ordering::Relaxed),
            total_downstream_bytes_sent: self.total_downstream_bytes_sent.load(Ordering::Relaxed),
            status_1xx: self.status_1xx.load(Ordering::Relaxed),
            status_2xx: self.status_2xx.load(Ordering::Relaxed),
            status_3xx: self.status_3xx.load(Ordering::Relaxed),
            status_4xx: self.status_4xx.load(Ordering::Relaxed),
            status_5xx: self.status_5xx.load(Ordering::Relaxed),
            status_other: self.status_other.load(Ordering::Relaxed),
            no_eligible_backend: self.no_eligible_backend.load(Ordering::Relaxed),
            backend_attempt_failures: self.backend_attempt_failures.load(Ordering::Relaxed),
            static_failures: self.static_failures.load(Ordering::Relaxed),
            client_write_failures: self.client_write_failures.load(Ordering::Relaxed),
        }
    }
}

fn update_min(target: &AtomicU64, value: u64) {
    let mut current = target.load(Ordering::Relaxed);
    while (current == 0 || value < current)
        && target
            .compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
    {
        current = target.load(Ordering::Relaxed);
    }
}

fn unix_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn duration_micros(duration: Duration) -> u64 {
    duration.as_micros().min(u64::MAX as u128) as u64
}

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u64::MAX as u128) as u64
}

#[derive(Serialize)]
struct RequestLogEvent<'a> {
    schema_version: u8,
    event: &'static str,
    timestamp_unix_ms: u64,
    run_id: &'a str,
    request_id: u64,
    runtime: RuntimeMode,
    service_id: &'a str,
    algorithm: Option<AlgorithmKind>,
    backend_id: &'a str,
    method: &'a str,
    host: Option<&'a str>,
    path: &'a str,
    outcome: RequestOutcome,
    success: bool,
    status_code: u16,
    attempts: usize,
    latency_us: u64,
    upstream_bytes_sent: usize,
    downstream_bytes_sent: usize,
}

#[derive(Debug, Serialize)]
pub struct ObservabilitySnapshot {
    pub schema_version: u8,
    pub run_id: String,
    pub uptime_ms: u64,
    pub requests: Vec<RequestMetricsSnapshot>,
    pub services: Vec<ServiceMetricsSnapshot>,
}

#[derive(Debug, Serialize)]
pub struct RequestMetricsSnapshot {
    pub service_id: String,
    pub runtime: RuntimeMode,
    pub total_requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub total_attempts: u64,
    pub total_retries: u64,
    pub total_latency_us: u64,
    pub average_latency_us: u64,
    pub min_latency_us: u64,
    pub max_latency_us: u64,
    pub total_upstream_bytes_sent: u64,
    pub total_downstream_bytes_sent: u64,
    pub status_1xx: u64,
    pub status_2xx: u64,
    pub status_3xx: u64,
    pub status_4xx: u64,
    pub status_5xx: u64,
    pub status_other: u64,
    pub no_eligible_backend: u64,
    pub backend_attempt_failures: u64,
    pub static_failures: u64,
    pub client_write_failures: u64,
}

#[derive(Debug, Serialize)]
pub struct ServiceMetricsSnapshot {
    pub service_id: String,
    pub target: &'static str,
    pub routes: Vec<RouteMatcher>,
    pub algorithm: Option<AlgorithmKind>,
    pub fail_open: Option<bool>,
    pub backends: Vec<BackendMetricsReport>,
    pub adaptive_diagnostics: Option<AdaptiveDiagnosticSnapshot>,
}
#[cfg(test)]
#[path = "observability_tests.rs"]
mod tests;
