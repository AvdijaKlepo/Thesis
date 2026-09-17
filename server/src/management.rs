//! Typed client for the server management API.
//!
//! The scenario runner uses this client directly. The management CLI can use
//! the same API instead of growing a second set of endpoint assumptions.

use std::{
    collections::BTreeMap,
    error::Error,
    fmt::{Display, Formatter},
    io::{Read, Write},
    net::{IpAddr, SocketAddr, TcpStream},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

use crate::{
    Backend,
    algorithms::{AlgorithmKind, balancers::AdaptiveV2Settings},
    backend::{BackendPool, BackendPoolError},
    proxy::RuntimeMode,
    service::{RouteMatcher, Service, ServiceRegistryError, ServiceTarget},
};

pub mod api;

const DEFAULT_MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ManagedServiceKind {
    Proxy,
    Static,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceDefinition {
    pub id: String,
    pub kind: ManagedServiceKind,
    pub routes: Vec<RouteMatcher>,
    pub algorithm: Option<AlgorithmKind>,
    #[serde(default)]
    pub fail_open: bool,
    #[serde(default)]
    pub backends: Vec<Backend>,
    #[serde(default)]
    pub adaptive_v2: AdaptiveV2Settings,
    pub root: Option<PathBuf>,
}

impl ServiceDefinition {
    pub fn from_service(service: &Service) -> Self {
        match &service.target {
            ServiceTarget::Proxy(pool) => Self {
                id: service.id.clone(),
                kind: ManagedServiceKind::Proxy,
                routes: service.routes.clone(),
                algorithm: Some(pool.algorithm()),
                fail_open: pool.fail_open(),
                backends: pool
                    .backends()
                    .into_iter()
                    .map(|node| node.backend)
                    .collect(),
                adaptive_v2: pool.adaptive_v2_settings(),
                root: None,
            },
            ServiceTarget::Static { root } => Self {
                id: service.id.clone(),
                kind: ManagedServiceKind::Static,
                routes: service.routes.clone(),
                algorithm: None,
                fail_open: false,
                backends: Vec::new(),
                adaptive_v2: AdaptiveV2Settings::default(),
                root: Some(root.clone()),
            },
        }
    }

    pub fn build(self) -> Result<Service, ManagementModelError> {
        validate_resource_id("service", &self.id)?;
        let routes = normalize_routes(self.routes)?;
        match self.kind {
            ManagedServiceKind::Proxy => {
                if self.root.is_some() {
                    return Err(ManagementModelError::Invalid(
                        "proxy service cannot define root".into(),
                    ));
                }
                let algorithm = self.algorithm.ok_or_else(|| {
                    ManagementModelError::Invalid("proxy service must define an algorithm".into())
                })?;
                for backend in &self.backends {
                    validate_resource_id("backend", &backend.id)?;
                }
                let pool = BackendPool::new_with_fail_open_and_adaptive_v2_settings(
                    algorithm,
                    self.backends,
                    self.fail_open,
                    self.adaptive_v2,
                )?;
                Ok(Service::proxy(self.id, routes, Arc::new(pool))?)
            }
            ManagedServiceKind::Static => {
                if self.algorithm.is_some()
                    || self.fail_open
                    || !self.backends.is_empty()
                    || self.adaptive_v2 != AdaptiveV2Settings::default()
                {
                    return Err(ManagementModelError::Invalid(
                        "static service cannot define proxy settings".into(),
                    ));
                }
                let root = self.root.ok_or_else(|| {
                    ManagementModelError::Invalid("static service must define root".into())
                })?;
                if !root.is_absolute() || !root.is_dir() {
                    return Err(ManagementModelError::Invalid(format!(
                        "static root must be an existing absolute directory: {}",
                        root.display()
                    )));
                }
                Ok(Service::static_files(self.id, routes, root)?)
            }
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ServicePatch {
    pub routes: Option<Vec<RouteMatcher>>,
    pub algorithm: Option<AlgorithmKind>,
    pub fail_open: Option<bool>,
    #[serde(default)]
    pub adaptive_v2: Option<AdaptiveV2Settings>,
    pub root: Option<PathBuf>,
}

impl ServicePatch {
    pub fn is_empty(&self) -> bool {
        self.routes.is_none()
            && self.algorithm.is_none()
            && self.fail_open.is_none()
            && self.adaptive_v2.is_none()
            && self.root.is_none()
    }
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BackendPatch {
    pub address: Option<String>,
    pub weight: Option<usize>,
}

impl BackendPatch {
    pub fn is_empty(&self) -> bool {
        self.address.is_none() && self.weight.is_none()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BackendStatus {
    pub id: String,
    pub address: String,
    pub weight: usize,
    pub healthy: bool,
}

impl BackendStatus {
    pub fn from_node(node: &crate::algorithms::balancers::BackendNode) -> Self {
        Self {
            id: node.id.clone(),
            address: node.address.clone(),
            weight: node.weight,
            healthy: node.healthy.load(std::sync::atomic::Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BackendProbeResult {
    pub service_id: String,
    pub backend_id: String,
    pub address: String,
    pub configured_healthy: bool,
    pub reachable: bool,
    pub latency_us: u64,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ServiceProbeResult {
    pub service_id: String,
    pub kind: ManagedServiceKind,
    pub available: bool,
    pub backend_probes: Vec<BackendProbeResult>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DiscoveredPort {
    pub address: SocketAddr,
    pub connect_latency_us: u64,
}

/// Scans a deliberately small loopback-only range for TCP listeners.
///
/// Discovery is a CLI convenience, never part of backend creation. Callers
/// still choose which discovered address, if any, to register.
pub fn discover_loopback_ports(
    host: IpAddr,
    start_port: u16,
    end_port: u16,
    timeout: Duration,
) -> Result<Vec<DiscoveredPort>, ManagementError> {
    if !host.is_loopback() {
        return Err(ManagementError::InvalidResponse(
            "port discovery is restricted to loopback addresses".into(),
        ));
    }
    if start_port == 0 || end_port < start_port || usize::from(end_port - start_port) >= 256 {
        return Err(ManagementError::InvalidResponse(
            "port discovery requires a non-zero range of at most 256 ports".into(),
        ));
    }
    if timeout.is_zero() || timeout > Duration::from_millis(500) {
        return Err(ManagementError::InvalidResponse(
            "port discovery timeout must be between 1 and 500 ms".into(),
        ));
    }

    let mut discovered = Vec::new();
    for port in start_port..=end_port {
        let address = SocketAddr::new(host, port);
        let started = Instant::now();
        if TcpStream::connect_timeout(&address, timeout).is_ok() {
            discovered.push(DiscoveredPort {
                address,
                connect_latency_us: duration_micros(started.elapsed()),
            });
        }
    }
    Ok(discovered)
}

fn duration_micros(duration: Duration) -> u64 {
    duration.as_micros().min(u128::from(u64::MAX)) as u64
}

#[derive(Debug)]
pub enum ManagementModelError {
    Invalid(String),
    Backend(BackendPoolError),
    Service(ServiceRegistryError),
}

impl Display for ManagementModelError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(message) => formatter.write_str(message),
            Self::Backend(error) => Display::fmt(error, formatter),
            Self::Service(error) => Display::fmt(error, formatter),
        }
    }
}

impl Error for ManagementModelError {}

impl From<BackendPoolError> for ManagementModelError {
    fn from(value: BackendPoolError) -> Self {
        Self::Backend(value)
    }
}

impl From<ServiceRegistryError> for ManagementModelError {
    fn from(value: ServiceRegistryError) -> Self {
        Self::Service(value)
    }
}

pub fn validate_resource_id(label: &str, value: &str) -> Result<(), ManagementModelError> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(ManagementModelError::Invalid(format!(
            "{label} id must use only letters, digits, '-', '_' and '.'"
        )));
    }
    Ok(())
}

pub fn normalize_routes(
    routes: Vec<RouteMatcher>,
) -> Result<Vec<RouteMatcher>, ManagementModelError> {
    routes
        .into_iter()
        .map(|route| RouteMatcher::new(route.host, route.path_prefix).map_err(Into::into))
        .collect()
}

#[derive(Debug)]
pub enum ManagementError {
    Io(std::io::Error),
    InvalidResponse(String),
    Api { status_code: u16, body: String },
    Json(serde_json::Error),
}

impl Display for ManagementError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "management connection failed: {error}"),
            Self::InvalidResponse(message) => write!(formatter, "invalid HTTP response: {message}"),
            Self::Api { status_code, body } => {
                write!(
                    formatter,
                    "management API returned HTTP {status_code}: {body}"
                )
            }
            Self::Json(error) => write!(formatter, "invalid management API JSON: {error}"),
        }
    }
}

impl Error for ManagementError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Json(error) => Some(error),
            Self::InvalidResponse(_) | Self::Api { .. } => None,
        }
    }
}

impl From<std::io::Error> for ManagementError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for ManagementError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

#[derive(Clone, Debug)]
pub struct HttpRequest<'a> {
    pub method: &'a str,
    pub path: &'a str,
    pub host: &'a str,
    pub headers: &'a BTreeMap<String, String>,
    pub body: &'a [u8],
    pub timeout: Duration,
    pub max_response_bytes: usize,
}

#[derive(Clone, Debug)]
pub struct HttpResponse {
    pub status_code: u16,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
    pub bytes_received: usize,
    pub truncated: bool,
}

impl HttpResponse {
    pub fn body_text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .get(&name.to_ascii_lowercase())
            .map(String::as_str)
    }
}

/// Sends a bounded HTTP/1.1 request to an IP socket.
///
/// The project intentionally uses address-and-path management endpoints, so
/// this client has no URL discovery, TLS, redirects, or ambient proxy behavior.
pub fn send_http(
    address: SocketAddr,
    request: &HttpRequest<'_>,
) -> Result<HttpResponse, ManagementError> {
    validate_http_token("method", request.method)?;
    validate_header_value("path", request.path)?;
    validate_header_value("host", request.host)?;
    if !request.path.starts_with('/') {
        return Err(ManagementError::InvalidResponse(
            "request path must start with '/'".into(),
        ));
    }
    for (name, value) in request.headers {
        validate_http_token("header name", name)?;
        validate_header_value("header value", value)?;
    }

    let mut stream = TcpStream::connect_timeout(&address, request.timeout)?;
    stream.set_read_timeout(Some(request.timeout))?;
    stream.set_write_timeout(Some(request.timeout))?;

    let mut head = format!(
        "{} {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nContent-Length: {}\r\n",
        request.method,
        request.path,
        request.host,
        request.body.len()
    );
    for (name, value) in request.headers {
        if name.eq_ignore_ascii_case("host")
            || name.eq_ignore_ascii_case("connection")
            || name.eq_ignore_ascii_case("content-length")
        {
            continue;
        }
        head.push_str(name);
        head.push_str(": ");
        head.push_str(value);
        head.push_str("\r\n");
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes())?;
    stream.write_all(request.body)?;
    stream.flush()?;

    let limit = request.max_response_bytes.saturating_add(1);
    let mut raw = Vec::new();
    stream.take(limit as u64).read_to_end(&mut raw)?;
    let truncated = raw.len() > request.max_response_bytes;
    if truncated {
        raw.truncate(request.max_response_bytes);
    }
    parse_http_response(raw, truncated)
}

fn parse_http_response(raw: Vec<u8>, truncated: bool) -> Result<HttpResponse, ManagementError> {
    let header_end = raw
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|position| position + 4)
        .ok_or_else(|| ManagementError::InvalidResponse("missing header terminator".into()))?;
    let head = std::str::from_utf8(&raw[..header_end])
        .map_err(|_| ManagementError::InvalidResponse("headers are not UTF-8".into()))?;
    let mut lines = head.split("\r\n");
    let status_line = lines
        .next()
        .ok_or_else(|| ManagementError::InvalidResponse("missing status line".into()))?;
    let mut status_parts = status_line.split_whitespace();
    let protocol = status_parts.next().unwrap_or_default();
    if !protocol.starts_with("HTTP/") {
        return Err(ManagementError::InvalidResponse(
            "status line has no HTTP version".into(),
        ));
    }
    let status_code = status_parts
        .next()
        .and_then(|value| value.parse::<u16>().ok())
        .ok_or_else(|| ManagementError::InvalidResponse("invalid status code".into()))?;
    let mut headers = BTreeMap::new();
    for line in lines.filter(|line| !line.is_empty()) {
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| ManagementError::InvalidResponse("malformed response header".into()))?;
        headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
    }
    let bytes_received = raw.len();
    Ok(HttpResponse {
        status_code,
        headers,
        body: raw[header_end..].to_vec(),
        bytes_received,
        truncated,
    })
}

fn validate_http_token(label: &str, value: &str) -> Result<(), ManagementError> {
    if value.is_empty()
        || !value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    byte,
                    b'!' | b'#'
                        | b'$'
                        | b'%'
                        | b'&'
                        | b'\''
                        | b'*'
                        | b'+'
                        | b'-'
                        | b'.'
                        | b'^'
                        | b'_'
                        | b'`'
                        | b'|'
                        | b'~'
                )
        })
    {
        return Err(ManagementError::InvalidResponse(format!("invalid {label}")));
    }
    Ok(())
}

fn validate_header_value(label: &str, value: &str) -> Result<(), ManagementError> {
    if value.contains(['\r', '\n']) {
        return Err(ManagementError::InvalidResponse(format!(
            "{label} contains a line break"
        )));
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub struct ManagementClient {
    address: SocketAddr,
    timeout: Duration,
}

impl ManagementClient {
    pub fn new(address: SocketAddr, timeout: Duration) -> Self {
        Self { address, timeout }
    }

    pub fn address(&self) -> SocketAddr {
        self.address
    }

    pub fn set_algorithm(&self, algorithm: AlgorithmKind) -> Result<(), ManagementError> {
        self.expect_success(self.request(
            "POST",
            "/algorithm",
            algorithm.as_str().as_bytes(),
            Some("text/plain"),
        )?)?;
        Ok(())
    }

    pub fn set_runtime(&self, runtime: RuntimeMode) -> Result<(), ManagementError> {
        self.expect_success(self.request(
            "POST",
            "/runtime",
            runtime.as_str().as_bytes(),
            Some("text/plain"),
        )?)?;
        Ok(())
    }

    pub fn runtime(&self) -> Result<RuntimeMode, ManagementError> {
        let response = self.expect_success(self.request("GET", "/runtime", &[], None)?)?;
        let value: Value = serde_json::from_slice(&response.body)?;
        value
            .get("runtime")
            .and_then(Value::as_str)
            .and_then(RuntimeMode::from_str_name)
            .ok_or_else(|| ManagementError::InvalidResponse("missing runtime field".into()))
    }

    pub fn metrics(&self) -> Result<Value, ManagementError> {
        let response = self.expect_success(self.request("GET", "/metrics", &[], None)?)?;
        Ok(serde_json::from_slice(&response.body)?)
    }

    pub fn get(&self, path: &str) -> Result<HttpResponse, ManagementError> {
        self.request("GET", path, &[], None)
    }

    pub fn list_services(&self) -> Result<Vec<ServiceDefinition>, ManagementError> {
        self.request_json("GET", "/v1/services", &[])
    }

    pub fn add_service(
        &self,
        service: &ServiceDefinition,
    ) -> Result<ServiceDefinition, ManagementError> {
        self.request_json("POST", "/v1/services", &serde_json::to_vec(service)?)
    }

    pub fn update_service(
        &self,
        service_id: &str,
        patch: &ServicePatch,
    ) -> Result<ServiceDefinition, ManagementError> {
        let path = resource_path("/v1/services", service_id)?;
        self.request_json("PATCH", &path, &serde_json::to_vec(patch)?)
    }

    pub fn remove_service(&self, service_id: &str) -> Result<ServiceDefinition, ManagementError> {
        let path = resource_path("/v1/services", service_id)?;
        self.request_json("DELETE", &path, &[])
    }

    pub fn probe_service(
        &self,
        service_id: &str,
        timeout: Duration,
    ) -> Result<ServiceProbeResult, ManagementError> {
        let path = format!(
            "{}/probe?timeout_ms={}",
            resource_path("/v1/services", service_id)?,
            timeout.as_millis()
        );
        self.request_json("GET", &path, &[])
    }

    pub fn list_backends(&self, service_id: &str) -> Result<Vec<BackendStatus>, ManagementError> {
        let path = format!("{}/backends", resource_path("/v1/services", service_id)?);
        self.request_json("GET", &path, &[])
    }

    pub fn add_backend(
        &self,
        service_id: &str,
        backend: &Backend,
    ) -> Result<BackendStatus, ManagementError> {
        validate_resource_id("backend", &backend.id)
            .map_err(|error| ManagementError::InvalidResponse(error.to_string()))?;
        let path = format!("{}/backends", resource_path("/v1/services", service_id)?);
        self.request_json("POST", &path, &serde_json::to_vec(backend)?)
    }

    pub fn update_backend(
        &self,
        service_id: &str,
        backend_id: &str,
        patch: &BackendPatch,
    ) -> Result<BackendStatus, ManagementError> {
        let path = backend_path(service_id, backend_id)?;
        self.request_json("PATCH", &path, &serde_json::to_vec(patch)?)
    }

    pub fn remove_backend(
        &self,
        service_id: &str,
        backend_id: &str,
    ) -> Result<BackendStatus, ManagementError> {
        let path = backend_path(service_id, backend_id)?;
        self.request_json("DELETE", &path, &[])
    }

    pub fn probe_backend(
        &self,
        service_id: &str,
        backend_id: &str,
        timeout: Duration,
    ) -> Result<BackendProbeResult, ManagementError> {
        let path = format!(
            "{}/probe?timeout_ms={}",
            backend_path(service_id, backend_id)?,
            timeout.as_millis()
        );
        self.request_json("GET", &path, &[])
    }

    pub fn set_service_algorithm(
        &self,
        service_id: &str,
        algorithm: AlgorithmKind,
    ) -> Result<ServiceDefinition, ManagementError> {
        self.update_service(
            service_id,
            &ServicePatch {
                algorithm: Some(algorithm),
                ..ServicePatch::default()
            },
        )
    }

    fn request_json<T: DeserializeOwned>(
        &self,
        method: &str,
        path: &str,
        body: &[u8],
    ) -> Result<T, ManagementError> {
        let response = self.expect_success(self.request(
            method,
            path,
            body,
            (!body.is_empty()).then_some("application/json"),
        )?)?;
        Ok(serde_json::from_slice(&response.body)?)
    }

    fn request(
        &self,
        method: &str,
        path: &str,
        body: &[u8],
        content_type: Option<&str>,
    ) -> Result<HttpResponse, ManagementError> {
        let mut headers = BTreeMap::new();
        if let Some(content_type) = content_type {
            headers.insert("Content-Type".into(), content_type.into());
        }
        send_http(
            self.address,
            &HttpRequest {
                method,
                path,
                host: &self.address.to_string(),
                headers: &headers,
                body,
                timeout: self.timeout,
                max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
            },
        )
    }

    fn expect_success(&self, response: HttpResponse) -> Result<HttpResponse, ManagementError> {
        if (200..300).contains(&response.status_code) {
            Ok(response)
        } else {
            Err(ManagementError::Api {
                status_code: response.status_code,
                body: response.body_text(),
            })
        }
    }
}

fn resource_path(prefix: &str, id: &str) -> Result<String, ManagementError> {
    validate_resource_id("resource", id)
        .map_err(|error| ManagementError::InvalidResponse(error.to_string()))?;
    Ok(format!("{prefix}/{id}"))
}

fn backend_path(service_id: &str, backend_id: &str) -> Result<String, ManagementError> {
    validate_resource_id("backend", backend_id)
        .map_err(|error| ManagementError::InvalidResponse(error.to_string()))?;
    Ok(format!(
        "{}/backends/{backend_id}",
        resource_path("/v1/services", service_id)?
    ))
}
#[cfg(test)]
#[path = "management_tests.rs"]
mod tests;
