use std::{error::Error, sync::Arc};

use arc_swap::ArcSwap;
use tiny_http::Server;

use crate::{
    Backend,
    algorithms::{
        algorithms::{BackendNode, LoadBalancer},
        create_load_balancer,
    },
    backend::registry::BackendRegistry,
    proxy::runtime::RuntimeMode,
};

fn start_server_with_fallback(
    host: &str,
    base_port: u16,
    max_attempts: u16,
) -> Result<Server, Box<dyn Error>> {
    let mut current_port = base_port;

    for _ in 0..max_attempts {
        let addr = format!("{}:{}", host, current_port);

        match Server::http(&addr) {
            Ok(server) => {
                println!("Successfully bound to {}", addr);
                return Ok(server);
            }
            Err(e) => {
                if let Some(io_err) = e.downcast_ref::<std::io::Error>() {
                    if io_err.kind() == std::io::ErrorKind::AddrInUse {
                        println!("Port {} is busy. Trying next port...", current_port);
                        current_port += 1;
                        continue;
                    }
                }

                return Err(e);
            }
        }
    }

    Err(format!(
        "Could not find an available port after {} attempts",
        max_attempts
    )
    .into())
}

pub fn create_server(lb_slot: Arc<ArcSwap<Box<dyn LoadBalancer>>>, registry: Arc<BackendRegistry>, runtime_mode: Arc<ArcSwap<RuntimeMode>>) {
    let server = match start_server_with_fallback("127.0.0.1", 7880, 10) {
        Ok(server) => server,
        Err(e) => {
            eprintln!("Initialization failed: {}", e);
            std::process::exit(1);
        }
    };

    for request in server.incoming_requests() {
        let lb_slot = Arc::clone(&lb_slot);
        let registry = Arc::clone(&registry);
        let runtime_mode = Arc::clone(&runtime_mode);
        std::thread::spawn(move || handle_requests(request, lb_slot, registry, runtime_mode));
    }
}

fn handle_requests(
    request: tiny_http::Request,
    lb_slot: Arc<ArcSwap<Box<dyn LoadBalancer>>>,
    registry: Arc<BackendRegistry>,
    runtime_mode: Arc<ArcSwap<RuntimeMode>>,
) {
    let path = request.url().split('?').next().unwrap_or("");

    match (request.method(), path) {
        (&tiny_http::Method::Post, "/algorithm") => {
            change_algorithm(request, lb_slot, registry);
        }

        (&tiny_http::Method::Post, "/backends") => {
            create_backends_endpoint(request, registry);
        }

        (&tiny_http::Method::Get, "/backends") => {
            get_backends_endpoint(request, registry);
        }

        (&tiny_http::Method::Get, "/metrics") => {
            get_metrics_endpoint(request, registry);
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

fn get_metrics_endpoint(request: tiny_http::Request, registry: Arc<BackendRegistry>) {
    let summary = registry.metrics_summary();
    let body = match serde_json::to_string(&summary) {
        Ok(body) => body,
        Err(_) => {
            let response =
                tiny_http::Response::from_string("Failed to serialize metrics").with_status_code(500);
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

fn get_backends_endpoint(request: tiny_http::Request, registry: Arc<BackendRegistry>) {
    let backends: Vec<Backend> = registry.all().into_iter().map(|n| n.backend).collect();
    let body = match serde_json::to_string(&backends) {
        Ok(body) => body,
        Err(_) => {
            let response =
                tiny_http::Response::from_string("Failed to serialize backends").with_status_code(500);
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

fn create_backends(registry: &BackendRegistry, count: usize) -> Vec<Backend> {
    println!("Registry contains: {:?}", registry.all());
    println!("Next ID: {}", registry.next_id());
    let mut start_id = registry.next_id();

    let mut created = Vec::with_capacity(count);

    for _ in 0..count {
        
       
        let backend = Backend {
            id: (start_id).to_string(),
            address: format!("127.0.0.1:{}", 5052 + start_id),
            weight: 1,
        };
      

        registry.add(BackendNode::new(backend.clone()));
        created.push(backend);
        start_id+=1;
    }
  
    created


}
fn create_backends_endpoint(request: tiny_http::Request, registry: Arc<BackendRegistry>) {
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

    let backends = create_backends(&registry, count);

   

    let body = match serde_json::to_string(&backends) {
    Ok(body) => body,
    Err(_) => {
        let response =
            tiny_http::Response::from_string("Failed to serialize backends")
                .with_status_code(500);

        let _ = request.respond(response);
        return;
    }
};

let response = tiny_http::Response::from_string(body)
    .with_status_code(200);

let _ = request.respond(response);
}

fn change_algorithm(
    mut request: tiny_http::Request,
    lb_slot: Arc<ArcSwap<Box<dyn LoadBalancer>>>,
    registry: Arc<BackendRegistry>,
) {
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

    let backends = registry.all();

    let new_lb = match create_load_balancer(body.trim(), backends) {
        Some(lb) => lb,

        None => {
            let response =
                tiny_http::Response::from_string("unknown algorithm").with_status_code(400);

            let _ = request.respond(response);
            return;
        }
    };
    lb_slot.store(Arc::new(new_lb));

    let response = tiny_http::Response::from_string("switched").with_status_code(200);

    let _ = request.respond(response);
}

fn change_runtime(
    mut request: tiny_http::Request,
    runtime_mode: Arc<ArcSwap<RuntimeMode>>,
) {
    let mut body = String::new();

    if request.as_reader().read_to_string(&mut body).is_err() {
        let response = tiny_http::Response::from_string("Invalid request body")
            .with_status_code(400)
            .with_header(
                tiny_http::Header::from_bytes(&b"Access-Control-Allow-Origin"[..], &b"*"[..]).unwrap(),
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
            let response = tiny_http::Response::from_string(format!("unknown runtime mode: {mode_str}"))
                .with_status_code(400)
                .with_header(
                    tiny_http::Header::from_bytes(&b"Access-Control-Allow-Origin"[..], &b"*"[..]).unwrap(),
                );
            let _ = request.respond(response);
            return;
        }
    };

    runtime_mode.store(Arc::new(new_mode));
    println!("Switched proxy runtime mode to: {:?}", new_mode);

    let resp_json = format!(r#"{{"status":"switched","runtime":"{}"}}"#, new_mode.as_str());
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

fn get_runtime(
    request: tiny_http::Request,
    runtime_mode: Arc<ArcSwap<RuntimeMode>>,
) {
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

