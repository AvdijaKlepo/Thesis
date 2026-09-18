//! HTTP surface for runtime server management.

use std::{
    io::Read,
    net::{TcpStream, ToSocketAddrs},
    path::Path,
    sync::{Arc, atomic::Ordering},
    time::{Duration, Instant},
};

use arc_swap::ArcSwap;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::json;
use tiny_http::{Method, Request, Response, Server, StatusCode};

use crate::{
    Backend,
    algorithms::AlgorithmKind,
    backend::BackendPoolError,
    observability::Observability,
    proxy::RuntimeMode,
    service::{Service, ServiceRegistry, ServiceRegistryError, ServiceTarget},
};

use super::{
    BackendPatch, BackendProbeResult, BackendStatus, ManagedServiceKind, ManagementModelError,
    ServiceDefinition, ServicePatch, ServiceProbeResult, normalize_routes, validate_resource_id,
};

const MAX_REQUEST_BODY_BYTES: u64 = 1024 * 1024;
const DEFAULT_PROBE_TIMEOUT_MS: u64 = 500;
const MAX_PROBE_TIMEOUT_MS: u64 = 10_000;

pub fn create_server(
    address: impl AsRef<str>,
    default_service_id: impl Into<String>,
    runtime_mode: Arc<ArcSwap<RuntimeMode>>,
    service_registry: Arc<ServiceRegistry>,
    observability: Arc<Observability>,
) {
    let address = address.as_ref();
    let server = match Server::http(address) {
        Ok(server) => server,
        Err(error) => {
            eprintln!("Management server failed to bind to {address}: {error}");
            return;
        }
    };
    let state = Arc::new(ManagementState {
        default_service_id: default_service_id.into(),
        runtime_mode,
        service_registry,
        observability,
    });
    eprintln!("Management server listening on {address}");

    for request in server.incoming_requests() {
        let state = Arc::clone(&state);
        std::thread::spawn(move || handle_request(request, state));
    }
}

struct ManagementState {
    default_service_id: String,
    runtime_mode: Arc<ArcSwap<RuntimeMode>>,
    service_registry: Arc<ServiceRegistry>,
    observability: Arc<Observability>,
}

fn handle_request(request: Request, state: Arc<ManagementState>) {
    let method = request.method().as_str().to_string();
    let url = request.url().to_string();
    let path = url.split('?').next().unwrap_or_default();
    let segments = path
        .trim_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();

    if request.method() == &Method::Options {
        respond_empty(request, 204);
        return;
    }

    let result = match (method.as_str(), segments.as_slice()) {
        ("GET", ["v1", "services"]) => list_services(request, &state),
        ("POST", ["v1", "services"]) => add_service(request, &state),
        ("GET", ["v1", "services", service_id]) => get_service(request, &state, service_id),
        ("PATCH", ["v1", "services", service_id]) => update_service(request, &state, service_id),
        ("DELETE", ["v1", "services", service_id]) => remove_service(request, &state, service_id),
        ("GET", ["v1", "services", service_id, "probe"]) => {
            probe_service_endpoint(request, &state, service_id, &url)
        }
        ("GET", ["v1", "services", service_id, "backends"]) => {
            list_backends(request, &state, service_id)
        }
        ("POST", ["v1", "services", service_id, "backends"]) => {
            add_backend(request, &state, service_id)
        }
        ("PATCH", ["v1", "services", service_id, "backends", backend_id]) => {
            update_backend(request, &state, service_id, backend_id)
        }
        ("DELETE", ["v1", "services", service_id, "backends", backend_id]) => {
            remove_backend(request, &state, service_id, backend_id)
        }
        (
            "GET",
            [
                "v1",
                "services",
                service_id,
                "backends",
                backend_id,
                "probe",
            ],
        ) => probe_backend_endpoint(request, &state, service_id, backend_id, &url),
        ("GET", ["metrics"]) => get_metrics(request, &state),
        ("GET", ["runtime"]) => get_runtime(request, &state),
        ("POST", ["runtime"]) => set_runtime(request, &state),
        ("POST", ["algorithm"]) => set_default_algorithm(request, &state),
        ("GET", ["backends"]) => list_backends(request, &state, &state.default_service_id),
        _ => Err(Box::new((
            request,
            ApiError::new(404, "not_found", "management endpoint not found"),
        ))),
    };
    if let Err(error) = result {
        let (request, error) = *error;
        respond_error(request, error);
    }
}

type HandlerResult = Result<(), Box<(Request, ApiError)>>;

macro_rules! handler_try {
    ($request:ident, $expression:expr) => {
        match $expression {
            Ok(value) => value,
            Err(error) => return Err(Box::new(($request, error))),
        }
    };
}

fn list_services(request: Request, state: &ManagementState) -> HandlerResult {
    let services = state
        .service_registry
        .all()
        .iter()
        .map(|service| ServiceDefinition::from_service(service))
        .collect::<Vec<_>>();
    respond_json(request, 200, &services)
}

fn get_service(request: Request, state: &ManagementState, service_id: &str) -> HandlerResult {
    let service = handler_try!(request, service(state, service_id));
    respond_json(request, 200, &ServiceDefinition::from_service(&service))
}

fn add_service(mut request: Request, state: &ManagementState) -> HandlerResult {
    let definition: ServiceDefinition = handler_try!(request, read_json(&mut request));
    let service = handler_try!(
        request,
        definition
            .build()
            .map_err(ApiError::from)
            .and_then(|service| state.service_registry.add(service).map_err(ApiError::from))
    );
    respond_json(request, 201, &ServiceDefinition::from_service(&service))
}

fn update_service(
    mut request: Request,
    state: &ManagementState,
    service_id: &str,
) -> HandlerResult {
    handler_try!(
        request,
        validate_resource_id("service", service_id).map_err(ApiError::from)
    );
    let patch: ServicePatch = handler_try!(request, read_json(&mut request));
    let service = handler_try!(
        request,
        patch_service(&state.service_registry, service_id, patch)
    );
    respond_json(request, 200, &ServiceDefinition::from_service(&service))
}

fn remove_service(request: Request, state: &ManagementState, service_id: &str) -> HandlerResult {
    if service_id == state.default_service_id {
        return Err(Box::new((
            request,
            ApiError::new(
                409,
                "default_service",
                "the default service cannot be removed while the server is running",
            ),
        )));
    }
    let service = handler_try!(
        request,
        state
            .service_registry
            .remove(service_id)
            .map_err(ApiError::from)
    );
    respond_json(request, 200, &ServiceDefinition::from_service(&service))
}

fn list_backends(request: Request, state: &ManagementState, service_id: &str) -> HandlerResult {
    let pool = handler_try!(request, proxy_pool(state, service_id));
    let backends = pool
        .backends()
        .iter()
        .map(BackendStatus::from_node)
        .collect::<Vec<_>>();
    respond_json(request, 200, &backends)
}

fn add_backend(mut request: Request, state: &ManagementState, service_id: &str) -> HandlerResult {
    let backend: Backend = handler_try!(request, read_json(&mut request));
    handler_try!(
        request,
        validate_resource_id("backend", &backend.id).map_err(ApiError::from)
    );
    let pool = handler_try!(request, proxy_pool(state, service_id));
    let node = handler_try!(request, pool.add_backend(backend).map_err(ApiError::from));
    respond_json(request, 201, &BackendStatus::from_node(&node))
}

fn update_backend(
    mut request: Request,
    state: &ManagementState,
    service_id: &str,
    backend_id: &str,
) -> HandlerResult {
    handler_try!(
        request,
        validate_resource_id("backend", backend_id).map_err(ApiError::from)
    );
    let patch: BackendPatch = handler_try!(request, read_json(&mut request));
    if patch.is_empty() {
        return Err(Box::new((
            request,
            ApiError::new(400, "empty_patch", "backend update has no changes"),
        )));
    }
    let pool = handler_try!(request, proxy_pool(state, service_id));
    let current = handler_try!(
        request,
        pool.backends()
            .into_iter()
            .find(|backend| backend.id == backend_id)
            .ok_or_else(|| ApiError::new(404, "backend_not_found", "backend not found"))
    );
    let backend = Backend {
        id: backend_id.to_string(),
        address: patch.address.unwrap_or(current.address.clone()),
        weight: patch.weight.unwrap_or(current.weight),
    };
    let node = handler_try!(
        request,
        pool.update_backend(backend).map_err(ApiError::from)
    );
    respond_json(request, 200, &BackendStatus::from_node(&node))
}

fn remove_backend(
    request: Request,
    state: &ManagementState,
    service_id: &str,
    backend_id: &str,
) -> HandlerResult {
    let pool = handler_try!(request, proxy_pool(state, service_id));
    let node = handler_try!(
        request,
        pool.remove_backend(backend_id).map_err(ApiError::from)
    );
    respond_json(request, 200, &BackendStatus::from_node(&node))
}

fn probe_backend_endpoint(
    request: Request,
    state: &ManagementState,
    service_id: &str,
    backend_id: &str,
    url: &str,
) -> HandlerResult {
    let timeout = handler_try!(request, probe_timeout(url));
    let pool = handler_try!(request, proxy_pool(state, service_id));
    let node = handler_try!(
        request,
        pool.backends()
            .into_iter()
            .find(|backend| backend.id == backend_id)
            .ok_or_else(|| ApiError::new(404, "backend_not_found", "backend not found"))
    );
    let result = probe_backend(service_id, &node, timeout);
    respond_json(request, 200, &result)
}

fn probe_service_endpoint(
    request: Request,
    state: &ManagementState,
    service_id: &str,
    url: &str,
) -> HandlerResult {
    let timeout = handler_try!(request, probe_timeout(url));
    let service = handler_try!(request, service(state, service_id));
    let result = match &service.target {
        ServiceTarget::Proxy(pool) => {
            let backend_probes = pool
                .backends()
                .iter()
                .map(|backend| probe_backend(service_id, backend, timeout))
                .collect::<Vec<_>>();
            ServiceProbeResult {
                service_id: service_id.into(),
                kind: ManagedServiceKind::Proxy,
                available: backend_probes.iter().any(|probe| probe.reachable),
                error: backend_probes
                    .is_empty()
                    .then(|| "proxy service has no backends".into()),
                backend_probes,
            }
        }
        ServiceTarget::Static { root } => {
            let available = root.is_dir();
            ServiceProbeResult {
                service_id: service_id.into(),
                kind: ManagedServiceKind::Static,
                available,
                backend_probes: Vec::new(),
                error: (!available)
                    .then(|| format!("static root is not a directory: {}", root.display())),
            }
        }
    };
    respond_json(request, 200, &result)
}

fn get_metrics(request: Request, state: &ManagementState) -> HandlerResult {
    respond_json(
        request,
        200,
        &state.observability.snapshot(&state.service_registry),
    )
}

fn get_runtime(request: Request, state: &ManagementState) -> HandlerResult {
    respond_json(
        request,
        200,
        &json!({"runtime": state.runtime_mode.load().as_str()}),
    )
}

fn set_runtime(mut request: Request, state: &ManagementState) -> HandlerResult {
    let body = handler_try!(request, read_body(&mut request));
    let text = String::from_utf8_lossy(&body);
    let runtime_name = serde_json::from_slice::<serde_json::Value>(&body)
        .ok()
        .and_then(|value| {
            value
                .get("runtime")
                .or_else(|| value.get("mode"))
                .and_then(|value| value.as_str().map(str::to_string))
        })
        .unwrap_or_else(|| text.trim().to_string());
    let runtime = handler_try!(
        request,
        RuntimeMode::from_str_name(&runtime_name).ok_or_else(|| ApiError::new(
            400,
            "invalid_runtime",
            "unknown runtime mode"
        ))
    );
    state.runtime_mode.store(Arc::new(runtime));
    respond_json(request, 200, &json!({"runtime": runtime}))
}

fn set_default_algorithm(mut request: Request, state: &ManagementState) -> HandlerResult {
    let body = handler_try!(request, read_body(&mut request));
    let text = String::from_utf8_lossy(&body);
    let algorithm_name = serde_json::from_slice::<serde_json::Value>(&body)
        .ok()
        .and_then(|value| {
            value
                .get("algorithm")
                .and_then(|value| value.as_str().map(str::to_string))
        })
        .unwrap_or_else(|| text.trim().to_string());
    let algorithm = handler_try!(
        request,
        AlgorithmKind::from_str_name(&algorithm_name).ok_or_else(|| ApiError::new(
            400,
            "invalid_algorithm",
            "unknown algorithm"
        ))
    );
    let pool = handler_try!(request, proxy_pool(state, &state.default_service_id));
    pool.change_algorithm(algorithm);
    respond_json(request, 200, &json!({"algorithm": algorithm}))
}

fn patch_service(
    registry: &ServiceRegistry,
    service_id: &str,
    patch: ServicePatch,
) -> Result<Arc<Service>, ApiError> {
    if patch.is_empty() {
        return Err(ApiError::new(
            400,
            "empty_patch",
            "service update has no changes",
        ));
    }
    let existing = registry
        .get(service_id)
        .ok_or_else(|| ApiError::new(404, "service_not_found", "service not found"))?;
    let routes = match patch.routes {
        Some(routes) => normalize_routes(routes).map_err(ApiError::from)?,
        None => existing.routes.clone(),
    };

    match &existing.target {
        ServiceTarget::Proxy(pool) => {
            if patch.root.is_some() {
                return Err(ApiError::new(
                    400,
                    "invalid_service",
                    "proxy service cannot define root",
                ));
            }
            if let Some(settings) = patch.adaptive_v2 {
                settings
                    .validate()
                    .map_err(|reason| ApiError::new(400, "invalid_adaptive_v2_settings", reason))?;
            }
            if routes != existing.routes {
                registry
                    .replace(Service::proxy(service_id, routes, Arc::clone(pool))?)
                    .map_err(ApiError::from)?;
            }
            if let Some(algorithm) = patch.algorithm {
                pool.change_algorithm(algorithm);
            }
            if let Some(fail_open) = patch.fail_open {
                pool.set_fail_open(fail_open);
            }
            if let Some(settings) = patch.adaptive_v2 {
                pool.set_adaptive_v2_settings(settings)
                    .map_err(ApiError::from)?;
            }
        }
        ServiceTarget::Static { root } => {
            if patch.algorithm.is_some() || patch.fail_open.is_some() || patch.adaptive_v2.is_some()
            {
                return Err(ApiError::new(
                    400,
                    "invalid_service",
                    "static service cannot define proxy settings",
                ));
            }
            let root = patch.root.unwrap_or_else(|| root.clone());
            validate_static_root(&root)?;
            registry
                .replace(Service::static_files(service_id, routes, root)?)
                .map_err(ApiError::from)?;
        }
    }
    registry
        .get(service_id)
        .ok_or_else(|| ApiError::new(404, "service_not_found", "service not found"))
}

fn validate_static_root(root: &Path) -> Result<(), ApiError> {
    if root.is_absolute() && root.is_dir() {
        Ok(())
    } else {
        Err(ApiError::new(
            400,
            "invalid_static_root",
            format!(
                "static root must be an existing absolute directory: {}",
                root.display()
            ),
        ))
    }
}

fn service(state: &ManagementState, service_id: &str) -> Result<Arc<Service>, ApiError> {
    validate_resource_id("service", service_id).map_err(ApiError::from)?;
    state
        .service_registry
        .get(service_id)
        .ok_or_else(|| ApiError::new(404, "service_not_found", "service not found"))
}

fn proxy_pool(
    state: &ManagementState,
    service_id: &str,
) -> Result<Arc<crate::backend::BackendPool>, ApiError> {
    service(state, service_id)?
        .proxy_pool()
        .ok_or_else(|| ApiError::new(409, "not_proxy_service", "service is not a proxy"))
}

fn probe_backend(
    service_id: &str,
    backend: &crate::algorithms::balancers::BackendNode,
    timeout: Duration,
) -> BackendProbeResult {
    let started = Instant::now();
    let result = backend
        .address
        .to_socket_addrs()
        .map_err(|error| error.to_string())
        .and_then(|addresses| {
            let mut attempted = false;
            let mut last_error = None;
            for address in addresses {
                attempted = true;
                match TcpStream::connect_timeout(&address, timeout) {
                    Ok(stream) => return Ok(stream),
                    Err(error) => last_error = Some(error),
                }
            }
            Err(last_error
                .map(|error| error.to_string())
                .unwrap_or_else(|| {
                    if attempted {
                        "backend connection failed".into()
                    } else {
                        "backend address resolved to no sockets".into()
                    }
                }))
        });
    BackendProbeResult {
        service_id: service_id.into(),
        backend_id: backend.id.clone(),
        address: backend.address.clone(),
        configured_healthy: backend.healthy.load(Ordering::Relaxed),
        reachable: result.is_ok(),
        latency_us: started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
        error: result.err(),
    }
}

fn probe_timeout(url: &str) -> Result<Duration, ApiError> {
    let supplied = url.split_once('?').and_then(|(_, query)| {
        query.split('&').find_map(|parameter| {
            let (name, value) = parameter.split_once('=')?;
            (name == "timeout_ms").then_some(value)
        })
    });
    let timeout_ms = match supplied {
        Some(value) => value.parse::<u64>().map_err(|_| {
            ApiError::new(
                400,
                "invalid_timeout",
                "timeout_ms must be an integer number of milliseconds",
            )
        })?,
        None => DEFAULT_PROBE_TIMEOUT_MS,
    };
    if timeout_ms == 0 || timeout_ms > MAX_PROBE_TIMEOUT_MS {
        return Err(ApiError::new(
            400,
            "invalid_timeout",
            format!("timeout_ms must be between 1 and {MAX_PROBE_TIMEOUT_MS}"),
        ));
    }
    Ok(Duration::from_millis(timeout_ms))
}

fn read_json<T: DeserializeOwned>(request: &mut Request) -> Result<T, ApiError> {
    let body = read_body(request)?;
    serde_json::from_slice(&body).map_err(|error| {
        ApiError::new(
            400,
            "invalid_json",
            format!("invalid JSON request: {error}"),
        )
    })
}

fn read_body(request: &mut Request) -> Result<Vec<u8>, ApiError> {
    let mut body = Vec::new();
    request
        .as_reader()
        .take(MAX_REQUEST_BODY_BYTES + 1)
        .read_to_end(&mut body)
        .map_err(|error| ApiError::new(400, "invalid_body", error.to_string()))?;
    if body.len() as u64 > MAX_REQUEST_BODY_BYTES {
        return Err(ApiError::new(
            413,
            "body_too_large",
            "request body exceeds 1 MiB",
        ));
    }
    Ok(body)
}

fn respond_json<T: Serialize>(request: Request, status: u16, value: &T) -> HandlerResult {
    let body = match serde_json::to_string(value) {
        Ok(body) => body,
        Err(error) => {
            return Err(Box::new((
                request,
                ApiError::new(500, "serialization_failed", error.to_string()),
            )));
        }
    };
    let response = Response::from_string(body)
        .with_status_code(StatusCode(status))
        .with_header(json_header())
        .with_header(cors_origin_header());
    if let Err(error) = request.respond(response) {
        eprintln!("Management response failed: {error}");
    }
    Ok(())
}

fn respond_error(request: Request, error: ApiError) {
    let body = json!({
        "error": {
            "code": error.code,
            "message": error.message,
        }
    });
    let response = Response::from_string(body.to_string())
        .with_status_code(StatusCode(error.status))
        .with_header(json_header())
        .with_header(cors_origin_header());
    let _ = request.respond(response);
}

fn respond_empty(request: Request, status: u16) {
    let response = Response::empty(StatusCode(status))
        .with_header(cors_origin_header())
        .with_header(cors_methods_header())
        .with_header(cors_headers_header());
    let _ = request.respond(response);
}

fn json_header() -> tiny_http::Header {
    tiny_http::Header::from_bytes("Content-Type", "application/json").unwrap()
}

fn cors_origin_header() -> tiny_http::Header {
    tiny_http::Header::from_bytes("Access-Control-Allow-Origin", "*").unwrap()
}

fn cors_methods_header() -> tiny_http::Header {
    tiny_http::Header::from_bytes(
        "Access-Control-Allow-Methods",
        "GET, POST, PATCH, DELETE, OPTIONS",
    )
    .unwrap()
}

fn cors_headers_header() -> tiny_http::Header {
    tiny_http::Header::from_bytes("Access-Control-Allow-Headers", "Content-Type").unwrap()
}

#[derive(Debug)]
struct ApiError {
    status: u16,
    code: &'static str,
    message: String,
}

impl ApiError {
    fn new(status: u16, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }
}

impl From<ManagementModelError> for ApiError {
    fn from(value: ManagementModelError) -> Self {
        match value {
            ManagementModelError::Backend(error) => Self::from(error),
            ManagementModelError::Service(error) => Self::from(error),
            ManagementModelError::Invalid(message) => Self::new(400, "invalid_model", message),
        }
    }
}

impl From<BackendPoolError> for ApiError {
    fn from(value: BackendPoolError) -> Self {
        let (status, code) = match value {
            BackendPoolError::BackendNotFound(_) => (404, "backend_not_found"),
            BackendPoolError::DuplicateBackendId(_)
            | BackendPoolError::DuplicateBackendAddress(_) => (409, "backend_conflict"),
            BackendPoolError::InvalidBackend(_) => (400, "invalid_backend"),
            BackendPoolError::InvalidAdaptiveV2Settings(_) => (400, "invalid_adaptive_v2_settings"),
        };
        Self::new(status, code, value.to_string())
    }
}

impl From<ServiceRegistryError> for ApiError {
    fn from(value: ServiceRegistryError) -> Self {
        let (status, code) = match value {
            ServiceRegistryError::ServiceNotFound(_) => (404, "service_not_found"),
            ServiceRegistryError::DuplicateServiceId(_)
            | ServiceRegistryError::DuplicateRoute(_) => (409, "service_conflict"),
            ServiceRegistryError::InvalidRoute(_) | ServiceRegistryError::InvalidService(_) => {
                (400, "invalid_service")
            }
        };
        Self::new(status, code, value.to_string())
    }
}
#[cfg(test)]
#[path = "tests/api_tests.rs"]
mod tests;
