//! Deterministic backend process used by Docker-based experiments.

use std::{
    error::Error,
    fmt::{Display, Formatter},
    io::{self, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs},
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};

use serde::Serialize;

use crate::worker::ThreadPool;

const MAX_REQUEST_HEADER_SIZE: usize = 64 * 1024;
const IO_TIMEOUT: Duration = Duration::from_secs(5);
const HEALTHCHECK_TIMEOUT: Duration = Duration::from_secs(2);
const ERROR_SCALE: u64 = 1_000_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorMode {
    Status,
    Disconnect,
}

impl ErrorMode {
    pub fn parse(value: &str) -> Result<Self, FixtureConfigError> {
        match value {
            "status" => Ok(Self::Status),
            "disconnect" => Ok(Self::Disconnect),
            _ => Err(FixtureConfigError::InvalidValue {
                field: "error_mode",
                value: value.into(),
                reason: "must be 'status' or 'disconnect'",
            }),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct FixtureConfig {
    pub id: String,
    pub listen_address: String,
    pub capacity: usize,
    pub latency_ms: u64,
    pub latency_jitter_ms: u64,
    pub processing_ms: u64,
    pub error_rate: f64,
    pub error_mode: ErrorMode,
    pub seed: u64,
}

impl Default for FixtureConfig {
    fn default() -> Self {
        Self {
            id: "fixture-1".into(),
            listen_address: "0.0.0.0:8080".into(),
            capacity: 4,
            latency_ms: 0,
            latency_jitter_ms: 0,
            processing_ms: 10,
            error_rate: 0.0,
            error_mode: ErrorMode::Status,
            seed: 1,
        }
    }
}

impl FixtureConfig {
    pub fn from_environment() -> Result<Self, FixtureConfigError> {
        let mut config = Self::default();
        config.apply_optional("FIXTURE_ID", std::env::var("FIXTURE_ID").ok())?;
        config.apply_optional("FIXTURE_LISTEN", std::env::var("FIXTURE_LISTEN").ok())?;
        config.apply_optional("FIXTURE_CAPACITY", std::env::var("FIXTURE_CAPACITY").ok())?;
        config.apply_optional(
            "FIXTURE_LATENCY_MS",
            std::env::var("FIXTURE_LATENCY_MS").ok(),
        )?;
        config.apply_optional(
            "FIXTURE_LATENCY_JITTER_MS",
            std::env::var("FIXTURE_LATENCY_JITTER_MS").ok(),
        )?;
        config.apply_optional(
            "FIXTURE_PROCESSING_MS",
            std::env::var("FIXTURE_PROCESSING_MS").ok(),
        )?;
        config.apply_optional(
            "FIXTURE_ERROR_RATE",
            std::env::var("FIXTURE_ERROR_RATE").ok(),
        )?;
        config.apply_optional(
            "FIXTURE_ERROR_MODE",
            std::env::var("FIXTURE_ERROR_MODE").ok(),
        )?;
        config.apply_optional("FIXTURE_SEED", std::env::var("FIXTURE_SEED").ok())?;
        config.validate()?;
        Ok(config)
    }

    pub fn apply_arguments<I, S>(&mut self, arguments: I) -> Result<(), FixtureConfigError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut arguments = arguments.into_iter().map(Into::into);
        while let Some(flag) = arguments.next() {
            let field = match flag.as_str() {
                "--id" => "FIXTURE_ID",
                "--listen" => "FIXTURE_LISTEN",
                "--capacity" => "FIXTURE_CAPACITY",
                "--latency-ms" => "FIXTURE_LATENCY_MS",
                "--latency-jitter-ms" => "FIXTURE_LATENCY_JITTER_MS",
                "--processing-ms" => "FIXTURE_PROCESSING_MS",
                "--error-rate" => "FIXTURE_ERROR_RATE",
                "--error-mode" => "FIXTURE_ERROR_MODE",
                "--seed" => "FIXTURE_SEED",
                _ => return Err(FixtureConfigError::UnknownArgument(flag)),
            };
            let value = arguments
                .next()
                .ok_or_else(|| FixtureConfigError::MissingArgumentValue(flag.clone()))?;
            self.apply_value(field, &value)?;
        }
        self.validate()
    }

    pub fn validate(&self) -> Result<(), FixtureConfigError> {
        if self.id.trim().is_empty() {
            return Err(invalid("id", &self.id, "must not be empty"));
        }
        if self.listen_address.parse::<SocketAddr>().is_err() {
            return Err(invalid(
                "listen_address",
                &self.listen_address,
                "must be an IP socket address",
            ));
        }
        if self.capacity == 0 {
            return Err(invalid("capacity", "0", "must be greater than zero"));
        }
        if !self.error_rate.is_finite() || !(0.0..=1.0).contains(&self.error_rate) {
            return Err(invalid(
                "error_rate",
                self.error_rate.to_string(),
                "must be between 0.0 and 1.0",
            ));
        }
        Ok(())
    }

    fn apply_optional(
        &mut self,
        field: &'static str,
        value: Option<String>,
    ) -> Result<(), FixtureConfigError> {
        if let Some(value) = value {
            self.apply_value(field, &value)?;
        }
        Ok(())
    }

    fn apply_value(&mut self, field: &'static str, value: &str) -> Result<(), FixtureConfigError> {
        match field {
            "FIXTURE_ID" => self.id = value.into(),
            "FIXTURE_LISTEN" => self.listen_address = value.into(),
            "FIXTURE_CAPACITY" => self.capacity = parse(field, value)?,
            "FIXTURE_LATENCY_MS" => self.latency_ms = parse(field, value)?,
            "FIXTURE_LATENCY_JITTER_MS" => self.latency_jitter_ms = parse(field, value)?,
            "FIXTURE_PROCESSING_MS" => self.processing_ms = parse(field, value)?,
            "FIXTURE_ERROR_RATE" => self.error_rate = parse(field, value)?,
            "FIXTURE_ERROR_MODE" => self.error_mode = ErrorMode::parse(value)?,
            "FIXTURE_SEED" => self.seed = parse(field, value)?,
            _ => unreachable!("known fixture configuration field"),
        }
        Ok(())
    }

    fn sampled_latency_ms(&self, request_id: u64) -> u64 {
        if self.latency_jitter_ms == 0 {
            return self.latency_ms;
        }
        let offset = deterministic_sample(self.seed, request_id, 0x4c41_5445_4e43_5901)
            % self.latency_jitter_ms.saturating_add(1);
        self.latency_ms.saturating_add(offset)
    }

    fn should_fail(&self, request_id: u64) -> bool {
        let threshold = (self.error_rate * ERROR_SCALE as f64).round() as u64;
        threshold > 0
            && (threshold >= ERROR_SCALE
                || deterministic_sample(self.seed, request_id, 0x4552_524f_5201) % ERROR_SCALE
                    < threshold)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FixtureConfigError {
    UnknownArgument(String),
    MissingArgumentValue(String),
    InvalidValue {
        field: &'static str,
        value: String,
        reason: &'static str,
    },
}

impl Display for FixtureConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownArgument(argument) => write!(formatter, "unknown argument: {argument}"),
            Self::MissingArgumentValue(argument) => {
                write!(formatter, "missing value for argument: {argument}")
            }
            Self::InvalidValue {
                field,
                value,
                reason,
            } => write!(formatter, "invalid {field} '{value}': {reason}"),
        }
    }
}

impl Error for FixtureConfigError {}

pub struct FixtureServer {
    config: FixtureConfig,
    workers: ThreadPool,
    state: Arc<FixtureState>,
}

impl FixtureServer {
    pub fn new(config: FixtureConfig) -> Result<Self, FixtureConfigError> {
        config.validate()?;
        Ok(Self {
            workers: ThreadPool::new(config.capacity),
            config,
            state: Arc::new(FixtureState::default()),
        })
    }

    pub fn run(&self) -> io::Result<()> {
        let listener = TcpListener::bind(&self.config.listen_address)?;
        eprintln!(
            "Fixture '{}' listening on {} with capacity {}",
            self.config.id, self.config.listen_address, self.config.capacity
        );

        for stream in listener.incoming() {
            let stream = stream?;
            let config = self.config.clone();
            let state = Arc::clone(&self.state);
            self.workers.execute(move || {
                if let Err(error) = handle_connection(stream, &config, &state) {
                    eprintln!("Fixture '{}' connection error: {error}", config.id);
                }
            });
        }
        Ok(())
    }
}

#[derive(Default)]
struct FixtureState {
    next_request_id: AtomicU64,
    total_requests: AtomicU64,
    successful_requests: AtomicU64,
    failed_requests: AtomicU64,
    active_requests: AtomicUsize,
    max_active_requests: AtomicUsize,
}

impl FixtureState {
    fn next_request_id(&self) -> u64 {
        self.next_request_id.fetch_add(1, Ordering::Relaxed) + 1
    }

    fn begin(&self) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        let active = self.active_requests.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active_requests
            .fetch_max(active, Ordering::Relaxed);
    }

    fn finish(&self, success: bool) {
        self.active_requests.fetch_sub(1, Ordering::SeqCst);
        if success {
            self.successful_requests.fetch_add(1, Ordering::Relaxed);
        } else {
            self.failed_requests.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn snapshot(&self, capacity: usize) -> FixtureMetricsSnapshot {
        FixtureMetricsSnapshot {
            capacity,
            total_requests: self.total_requests.load(Ordering::Relaxed),
            successful_requests: self.successful_requests.load(Ordering::Relaxed),
            failed_requests: self.failed_requests.load(Ordering::Relaxed),
            active_requests: self.active_requests.load(Ordering::Relaxed),
            max_active_requests: self.max_active_requests.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Serialize)]
struct FixtureMetricsSnapshot {
    capacity: usize,
    total_requests: u64,
    successful_requests: u64,
    failed_requests: u64,
    active_requests: usize,
    max_active_requests: usize,
}

#[derive(Serialize)]
struct HealthResponse<'a> {
    status: &'static str,
    backend_id: &'a str,
}

#[derive(Serialize)]
struct WorkResponse<'a> {
    backend_id: &'a str,
    request_id: u64,
    outcome: &'static str,
    capacity: usize,
    latency_ms: u64,
    processing_ms: u64,
}

fn handle_connection(
    mut stream: TcpStream,
    config: &FixtureConfig,
    state: &FixtureState,
) -> io::Result<()> {
    stream.set_read_timeout(Some(IO_TIMEOUT))?;
    stream.set_write_timeout(Some(IO_TIMEOUT))?;
    let Some(path) = read_request_path(&mut stream)? else {
        return Ok(());
    };

    match path.as_str() {
        "/health" => write_json(
            &mut stream,
            200,
            "OK",
            &HealthResponse {
                status: "ok",
                backend_id: &config.id,
            },
            &config.id,
            None,
        ),
        "/config" => write_json(&mut stream, 200, "OK", config, &config.id, None),
        "/metrics" => write_json(
            &mut stream,
            200,
            "OK",
            &state.snapshot(config.capacity),
            &config.id,
            None,
        ),
        _ => handle_workload(&mut stream, config, state),
    }
}

fn handle_workload(
    stream: &mut TcpStream,
    config: &FixtureConfig,
    state: &FixtureState,
) -> io::Result<()> {
    let request_id = state.next_request_id();
    state.begin();
    let latency_ms = config.sampled_latency_ms(request_id);
    sleep_ms(latency_ms);
    sleep_ms(config.processing_ms);
    let configured_failure = config.should_fail(request_id);

    let response = if configured_failure && config.error_mode == ErrorMode::Disconnect {
        state.finish(false);
        return Ok(());
    } else if configured_failure {
        write_json(
            stream,
            500,
            "Internal Server Error",
            &WorkResponse {
                backend_id: &config.id,
                request_id,
                outcome: "configured_error",
                capacity: config.capacity,
                latency_ms,
                processing_ms: config.processing_ms,
            },
            &config.id,
            Some(request_id),
        )
    } else {
        write_json(
            stream,
            200,
            "OK",
            &WorkResponse {
                backend_id: &config.id,
                request_id,
                outcome: "success",
                capacity: config.capacity,
                latency_ms,
                processing_ms: config.processing_ms,
            },
            &config.id,
            Some(request_id),
        )
    };

    state.finish(!configured_failure && response.is_ok());
    response
}

fn read_request_path(stream: &mut TcpStream) -> io::Result<Option<String>> {
    let mut request = Vec::with_capacity(1024);
    let mut chunk = [0_u8; 1024];
    loop {
        let read = stream.read(&mut chunk)?;
        if read == 0 {
            return if request.is_empty() {
                Ok(None)
            } else {
                Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "connection closed before request headers were complete",
                ))
            };
        }
        request.extend_from_slice(&chunk[..read]);
        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
        if request.len() > MAX_REQUEST_HEADER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "request headers exceeded maximum size",
            ));
        }
    }

    let request_line = String::from_utf8_lossy(&request)
        .lines()
        .next()
        .unwrap_or("")
        .to_string();
    let target = request_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "request line has no target"))?;
    Ok(Some(
        target.split(['?', '#']).next().unwrap_or("/").to_string(),
    ))
}

fn write_json<T: Serialize>(
    stream: &mut TcpStream,
    status: u16,
    reason: &str,
    value: &T,
    backend_id: &str,
    request_id: Option<u64>,
) -> io::Result<()> {
    let body = serde_json::to_vec(value).map_err(io::Error::other)?;
    let request_header = request_id
        .map(|id| format!("X-Fixture-Request-Id: {id}\r\n"))
        .unwrap_or_default();
    let headers = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\nX-Fixture-Backend: {backend_id}\r\n{request_header}\r\n",
        body.len()
    );
    stream.write_all(headers.as_bytes())?;
    stream.write_all(&body)?;
    stream.flush()
}

pub fn healthcheck(address: &str) -> io::Result<()> {
    let addresses = address.to_socket_addrs()?.collect::<Vec<_>>();
    let mut last_error = None;
    for address in addresses {
        match TcpStream::connect_timeout(&address, HEALTHCHECK_TIMEOUT) {
            Ok(mut stream) => {
                stream.set_read_timeout(Some(HEALTHCHECK_TIMEOUT))?;
                stream.set_write_timeout(Some(HEALTHCHECK_TIMEOUT))?;
                stream.write_all(
                    format!("GET /health HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n")
                        .as_bytes(),
                )?;
                let mut response = [0_u8; 128];
                let read = stream.read(&mut response)?;
                if response[..read].starts_with(b"HTTP/1.1 200") {
                    return Ok(());
                }
                last_error = Some(io::Error::other("fixture health endpoint was not healthy"));
            }
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or_else(|| io::Error::other("address resolved to no endpoints")))
}

fn sleep_ms(milliseconds: u64) {
    if milliseconds > 0 {
        thread::sleep(Duration::from_millis(milliseconds));
    }
}

fn deterministic_sample(seed: u64, request_id: u64, salt: u64) -> u64 {
    let mut value = seed ^ request_id.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ salt;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn parse<T>(field: &'static str, value: &str) -> Result<T, FixtureConfigError>
where
    T: std::str::FromStr,
{
    value
        .parse()
        .map_err(|_| invalid(field, value, "has the wrong type"))
}

fn invalid(
    field: &'static str,
    value: impl Into<String>,
    reason: &'static str,
) -> FixtureConfigError {
    FixtureConfigError::InvalidValue {
        field,
        value: value.into(),
        reason,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exchange(config: FixtureConfig, request: &[u8]) -> Vec<u8> {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let client_request = request.to_vec();
        let client = thread::spawn(move || {
            let mut stream = TcpStream::connect(address).unwrap();
            stream.write_all(&client_request).unwrap();
            let mut response = Vec::new();
            stream.read_to_end(&mut response).unwrap();
            response
        });
        let (stream, _) = listener.accept().unwrap();
        handle_connection(stream, &config, &FixtureState::default()).unwrap();
        client.join().unwrap()
    }

    #[test]
    fn arguments_override_configuration_and_are_validated() {
        let mut config = FixtureConfig::default();
        config
            .apply_arguments([
                "--id",
                "slow-a",
                "--capacity",
                "2",
                "--latency-ms",
                "15",
                "--latency-jitter-ms",
                "20",
                "--processing-ms",
                "30",
                "--error-rate",
                "0.25",
                "--error-mode",
                "disconnect",
                "--seed",
                "42",
            ])
            .unwrap();
        assert_eq!(config.id, "slow-a");
        assert_eq!(config.capacity, 2);
        assert_eq!(config.latency_ms, 15);
        assert_eq!(config.latency_jitter_ms, 20);
        assert_eq!(config.processing_ms, 30);
        assert_eq!(config.error_rate, 0.25);
        assert_eq!(config.error_mode, ErrorMode::Disconnect);
        assert_eq!(config.seed, 42);

        assert!(
            FixtureConfig {
                capacity: 0,
                ..FixtureConfig::default()
            }
            .validate()
            .is_err()
        );
        assert!(
            FixtureConfig {
                error_rate: 1.1,
                ..FixtureConfig::default()
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn seeded_latency_and_failures_are_repeatable() {
        let config = FixtureConfig {
            latency_ms: 10,
            latency_jitter_ms: 50,
            error_rate: 0.4,
            seed: 123,
            ..FixtureConfig::default()
        };
        let first = (1..=20)
            .map(|id| (config.sampled_latency_ms(id), config.should_fail(id)))
            .collect::<Vec<_>>();
        let second = (1..=20)
            .map(|id| (config.sampled_latency_ms(id), config.should_fail(id)))
            .collect::<Vec<_>>();
        assert_eq!(first, second);
        assert!(first.iter().all(|(latency, _)| (10..=60).contains(latency)));
        assert!(first.iter().any(|(_, failed)| *failed));
        assert!(first.iter().any(|(_, failed)| !failed));
    }

    #[test]
    fn control_endpoints_bypass_workload_faults() {
        let config = FixtureConfig {
            id: "always-fails".into(),
            error_rate: 1.0,
            error_mode: ErrorMode::Disconnect,
            ..FixtureConfig::default()
        };
        let response = exchange(
            config.clone(),
            b"GET /health HTTP/1.1\r\nConnection: close\r\n\r\n",
        );
        assert!(response.starts_with(b"HTTP/1.1 200 OK"));
        assert!(String::from_utf8_lossy(&response).contains("always-fails"));

        let response = exchange(config, b"GET /work HTTP/1.1\r\nConnection: close\r\n\r\n");
        assert!(response.is_empty());
    }

    #[test]
    fn status_failures_are_visible_and_machine_readable() {
        let config = FixtureConfig {
            id: "status-error".into(),
            processing_ms: 0,
            error_rate: 1.0,
            error_mode: ErrorMode::Status,
            ..FixtureConfig::default()
        };
        let response = exchange(config, b"GET /work HTTP/1.1\r\nConnection: close\r\n\r\n");
        assert!(response.starts_with(b"HTTP/1.1 500 Internal Server Error"));
        assert!(String::from_utf8_lossy(&response).contains("configured_error"));
    }
}
