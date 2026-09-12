use std::sync::Arc;

use arc_swap::ArcSwap;
use tiny_http::Server;

use crate::{
    Backend,
    algorithms::AlgorithmKind,
    backend::{BackendPool, BackendPoolError},
    observability::Observability,
    proxy::runtime::RuntimeMode,
    service::ServiceRegistry,
};

pub fn create_server(
    address: impl AsRef<str>,
    backend_pool: Arc<BackendPool>,
    runtime_mode: Arc<ArcSwap<RuntimeMode>>,
    service_registry: Arc<ServiceRegistry>,
    observability: Arc<Observability>,
) {
    let address = address.as_ref();
    let server = match Server::http(address) {
        Ok(server) => server,
        Err(e) => {
            eprintln!("Admin server failed to bind to {address}: {e}");
            return;
        }
    };
    eprintln!("Admin server listening on {address}");

    for request in server.incoming_requests() {
        let backend_pool = Arc::clone(&backend_pool);
        let runtime_mode = Arc::clone(&runtime_mode);
        let service_registry = Arc::clone(&service_registry);
        let observability = Arc::clone(&observability);
        std::thread::spawn(move || {
            handle_requests(
                request,
                backend_pool,
                runtime_mode,
                service_registry,
                observability,
            )
        });
    }
}

fn handle_requests(
    request: tiny_http::Request,
    backend_pool: Arc<BackendPool>,
    runtime_mode: Arc<ArcSwap<RuntimeMode>>,
    service_registry: Arc<ServiceRegistry>,
    observability: Arc<Observability>,
) {
    let path = request.url().split('?').next().unwrap_or("");

    match (request.method(), path) {
        (&tiny_http::Method::Post, "/algorithm") => {
            change_algorithm(request, backend_pool);
        }

        (&tiny_http::Method::Post, "/backends") => {
            create_backends_endpoint(request, backend_pool);
        }

        (&tiny_http::Method::Get, "/backends") => {
            get_backends_endpoint(request, backend_pool);
        }

        (&tiny_http::Method::Get, "/metrics") => {
            get_metrics_endpoint(request, service_registry, observability);
        }

        (&tiny_http::Method::Post, "/runtime") => {
            change_runtime(request, runtime_mode);
        }

        (&tiny_http::Method::Get, "/runtime") => {
            get_runtime(request, runtime_mode);
        }

        (&tiny_http::Method::Options, _) => {
            let response = tiny_http::Response::empty(204)
                .with_header(
                    tiny_http::Header::from_bytes(&b"Access-Control-Allow-Origin"[..], &b"*"[..])
                        .unwrap(),
                )
                .with_header(
                    tiny_http::Header::from_bytes(
                        &b"Access-Control-Allow-Methods"[..],
                        &b"GET, POST, OPTIONS"[..],
                    )
                    .unwrap(),
                )
                .with_header(
                    tiny_http::Header::from_bytes(
                        &b"Access-Control-Allow-Headers"[..],
                        &b"Content-Type"[..],
                    )
                    .unwrap(),
                );
            let _ = request.respond(response);
        }

        _ => {
            let response = tiny_http::Response::from_string("not found").with_status_code(404);

            let _ = request.respond(response);
        }
    }
}

fn get_metrics_endpoint(
    request: tiny_http::Request,
    service_registry: Arc<ServiceRegistry>,
    observability: Arc<Observability>,
) {
    let summary = observability.snapshot(&service_registry);
    let body = match serde_json::to_string(&summary) {
        Ok(body) => body,
        Err(_) => {
            let response = tiny_http::Response::from_string("Failed to serialize metrics")
                .with_status_code(500);
            let _ = request.respond(response);
            return;
        }
    };

    let response = tiny_http::Response::from_string(body)
        .with_status_code(200)
        .with_header(
            tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
        );
    let _ = request.respond(response);
}

fn get_backends_endpoint(request: tiny_http::Request, backend_pool: Arc<BackendPool>) {
    let backends: Vec<Backend> = backend_pool
        .backends()
        .into_iter()
        .map(|node| node.backend)
        .collect();
    let body = match serde_json::to_string(&backends) {
        Ok(body) => body,
        Err(_) => {
            let response = tiny_http::Response::from_string("Failed to serialize backends")
                .with_status_code(500);
            let _ = request.respond(response);
            return;
        }
    };

    let response = tiny_http::Response::from_string(body)
        .with_status_code(200)
        .with_header(
            tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
        );
    let _ = request.respond(response);
}

pub(crate) fn create_backends(
    backend_pool: &BackendPool,
    count: usize,
) -> Result<Vec<Backend>, BackendPoolError> {
    let mut start_id = backend_pool.next_id();

    let mut created = Vec::with_capacity(count);

    for _ in 0..count {
        let backend = Backend {
            id: (start_id).to_string(),
            address: format!("127.0.0.1:{}", 5052 + start_id),
            weight: 1,
        };

        backend_pool.add_backend(backend.clone())?;
        created.push(backend);
        start_id += 1;
    }

    Ok(created)
}

fn create_backends_endpoint(request: tiny_http::Request, backend_pool: Arc<BackendPool>) {
    let url = request.url();

    let count = url.split_once("?").and_then(|(_, query)| {
        query.split('&').find_map(|param| {
            let (key, value) = param.split_once('=')?;

            if key == "count" {
                value.parse::<usize>().ok()
            } else {
                None
            }
        })
    });

    let count = match count {
        Some(count) if count > 0 && count <= 50 => count,
        _ => {
            let response = tiny_http::Response::from_string("count must be between 1 and 50")
                .with_status_code(400);

            let _ = request.respond(response);
            return;
        }
    };

    let backends = match create_backends(&backend_pool, count) {
        Ok(backends) => backends,
        Err(error) => {
            let response =
                tiny_http::Response::from_string(error.to_string()).with_status_code(400);
            let _ = request.respond(response);
            return;
        }
    };

    let body = match serde_json::to_string(&backends) {
        Ok(body) => body,
        Err(_) => {
            let response = tiny_http::Response::from_string("Failed to serialize backends")
                .with_status_code(500);

            let _ = request.respond(response);
            return;
        }
    };

    let response = tiny_http::Response::from_string(body).with_status_code(200);

    let _ = request.respond(response);
}

fn change_algorithm(mut request: tiny_http::Request, backend_pool: Arc<BackendPool>) {
    if request.method() != &tiny_http::Method::Post || request.url() != "/algorithm" {
        let response = tiny_http::Response::from_string("not found").with_status_code(400);

        let _ = request.respond(response);
        return;
    }

    let mut body = String::new();

    if request.as_reader().read_to_string(&mut body).is_err() {
        let response =
            tiny_http::Response::from_string("Invalid request body").with_status_code(400);

        let _ = request.respond(response);
        return;
    }

    let trimmed = body.trim();
    let algo = serde_json::from_str::<serde_json::Value>(trimmed)
        .ok()
        .and_then(|v| {
            v.get("algorithm")
                .and_then(|a| a.as_str().map(|s| s.to_string()))
        })
        .unwrap_or_else(|| trimmed.to_string());

    let algorithm = match AlgorithmKind::from_str_name(algo.trim()) {
        Some(algorithm) => algorithm,
        None => {
            let response =
                tiny_http::Response::from_string("unknown algorithm").with_status_code(400);

            let _ = request.respond(response);
            return;
        }
    };
    backend_pool.change_algorithm(algorithm);

    let response = tiny_http::Response::from_string("switched").with_status_code(200);

    let _ = request.respond(response);
}

fn change_runtime(mut request: tiny_http::Request, runtime_mode: Arc<ArcSwap<RuntimeMode>>) {
    let mut body = String::new();

    if request.as_reader().read_to_string(&mut body).is_err() {
        let response = tiny_http::Response::from_string("Invalid request body")
            .with_status_code(400)
            .with_header(
                tiny_http::Header::from_bytes(&b"Access-Control-Allow-Origin"[..], &b"*"[..])
                    .unwrap(),
            );
        let _ = request.respond(response);
        return;
    }

    let trimmed = body.trim();
    let mode_str = if trimmed.starts_with('{') {
        #[derive(serde::Deserialize)]
        struct RuntimeReq {
            runtime: Option<String>,
            mode: Option<String>,
        }
        serde_json::from_str::<RuntimeReq>(trimmed)
            .ok()
            .and_then(|r| r.runtime.or(r.mode))
            .unwrap_or_else(|| trimmed.to_string())
    } else {
        trimmed.to_string()
    };

    let new_mode = match RuntimeMode::from_str_name(&mode_str) {
        Some(mode) => mode,
        None => {
            let response =
                tiny_http::Response::from_string(format!("unknown runtime mode: {mode_str}"))
                    .with_status_code(400)
                    .with_header(
                        tiny_http::Header::from_bytes(
                            &b"Access-Control-Allow-Origin"[..],
                            &b"*"[..],
                        )
                        .unwrap(),
                    );
            let _ = request.respond(response);
            return;
        }
    };

    runtime_mode.store(Arc::new(new_mode));
    eprintln!("Switched proxy runtime mode to: {:?}", new_mode);

    let resp_json = format!(
        r#"{{"status":"switched","runtime":"{}"}}"#,
        new_mode.as_str()
    );
    let response = tiny_http::Response::from_string(resp_json)
        .with_status_code(200)
        .with_header(
            tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
        )
        .with_header(
            tiny_http::Header::from_bytes(&b"Access-Control-Allow-Origin"[..], &b"*"[..]).unwrap(),
        );
    let _ = request.respond(response);
}

fn get_runtime(request: tiny_http::Request, runtime_mode: Arc<ArcSwap<RuntimeMode>>) {
    let mode = **runtime_mode.load();
    let resp_json = format!(r#"{{"runtime":"{}"}}"#, mode.as_str());
    let response = tiny_http::Response::from_string(resp_json)
        .with_status_code(200)
        .with_header(
            tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
        )
        .with_header(
            tiny_http::Header::from_bytes(&b"Access-Control-Allow-Origin"[..], &b"*"[..]).unwrap(),
        );
    let _ = request.respond(response);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_backends_adds_to_registry() {
        let backend_pool = BackendPool::new(AlgorithmKind::RoundRobin, Vec::new()).unwrap();
        let initial_id = backend_pool.next_id();
        assert_eq!(initial_id, 1);

        let created = create_backends(&backend_pool, 3).unwrap();
        assert_eq!(created.len(), 3);
        assert_eq!(backend_pool.backends().len(), 3);

        assert_eq!(created[0].id, "1");
        assert_eq!(created[1].id, "2");
        assert_eq!(created[2].id, "3");
    }

    #[test]
    fn test_create_backends_updates_active_balancer_without_algorithm_switch() {
        let backend_pool = BackendPool::new(AlgorithmKind::RoundRobin, Vec::new()).unwrap();
        let _ = create_backends(&backend_pool, 2).unwrap();
        let lb_slot = backend_pool.load_balancer();

        assert_eq!(lb_slot.load().backends().len(), 2);
        assert_eq!(lb_slot.load().name(), "round_robin");

        // Simulate admin API POST /backends?count=2:
        // 1) create backends in registry
        // 2) sync active load balancer slot
        let created_more = create_backends(&backend_pool, 2).unwrap();
        assert_eq!(created_more.len(), 2);

        // Active balancer must immediately have 4 backends without any algorithm switch
        assert_eq!(lb_slot.load().backends().len(), 4);
        assert_eq!(lb_slot.load().name(), "round_robin");

        // Verify that the new backends are reachable from the active balancer
        let mut seen_ids = std::collections::HashSet::new();
        for _ in 0..8 {
            seen_ids.insert(lb_slot.load().next(false).unwrap().id.clone());
        }
        assert!(seen_ids.contains("1"));
        assert!(seen_ids.contains("2"));
        assert!(seen_ids.contains("3"));
        assert!(seen_ids.contains("4"));
    }
}
