use std::env;

use server::{Backend, BackendServer};

fn main() {
    let port = env::args().nth(1).unwrap_or_else(|| "8081".into());
    let id = env::args().nth(2).unwrap_or_else(|| "1".into());

    let backend = Backend {
        id,
        address: format!("127.0.0.1:{port}"),
        weight:1
    };

    let server = BackendServer::new(backend, 4);

    if let Err(error) = server.run() {
        eprintln!("Backend failed: {error}");
    }
}
