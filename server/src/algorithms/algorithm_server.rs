use std::{error::Error, sync::Arc};

use arc_swap::ArcSwap;
use tiny_http::Server;

use crate::algorithms::{algorithms::{
    LeastConnections, LeastResponseTime, LoadBalancer, RoundRobin, WeightedRoundRobin,
    default_backends,
}, create_load_balancer};

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

pub fn create_server(lb_slot: Arc<ArcSwap<Box<dyn LoadBalancer>>>) {
    let server = match start_server_with_fallback("127.0.0.1", 7880, 10) {
        Ok(server) => server,
        Err(e) => {
            eprintln!("Initialization failed: {}", e);
            std::process::exit(1);
        }
    };

    for request in server.incoming_requests() {
        let lb_slot = Arc::clone(&lb_slot);
        std::thread::spawn(move || change_algorithm(request, lb_slot));
    }
}

fn change_algorithm(mut request: tiny_http::Request, lb_slot: Arc<ArcSwap<Box<dyn LoadBalancer>>>) {
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

    let new_lb = match create_load_balancer(body.trim(), default_backends()) {
    Some(lb) => lb,

    None => {
        let response =
            tiny_http::Response::from_string("unknown algorithm")
                .with_status_code(400);

        let _ = request.respond(response);
        return;
    }
};
    lb_slot.store(Arc::new(new_lb));

    let response = tiny_http::Response::from_string("switched").with_status_code(200);

    let _ = request.respond(response);
}
