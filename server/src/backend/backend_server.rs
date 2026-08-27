use std::{
    io::{Read, Write}, net::{TcpListener, TcpStream}, sync::{Arc, atomic::{AtomicBool, AtomicU64, AtomicUsize}}, time::Duration,
};

use crate::worker::ThreadPool;

#[derive(Clone, Debug)]
pub struct Backend {
    pub id: String,
    pub address: String,
    pub weight: usize
}

pub struct BackendMetrics {
    pub active_connections: AtomicUsize,
    pub latency_us: AtomicU64,
}

pub struct Feedback {
    pub latency: Duration,
    pub success: bool,
}
impl BackendMetrics {
    pub fn new() -> Self {
        Self {
            active_connections: AtomicUsize::new(0),
            latency_us: AtomicU64::new(1000),
        }
    }
}

pub struct BackendServer {
    backend: Backend,
    pool: ThreadPool,
    healthy: Arc<AtomicBool>,
}

impl BackendServer {
    pub fn new(backend: Backend, workers: usize) -> Self {
        Self {
            backend,
            pool: ThreadPool::new(workers),
            healthy: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn run(&self) -> std::io::Result<()> {

        println!("Attemptin to bind backend to :{}", self.backend.address);

        let listener = TcpListener::bind(&self.backend.address)?;

        println!(
            "Backend {} listening on {}",
            self.backend.id, self.backend.address
        );

        for stream in listener.incoming() {
            let stream = stream?;

            let backend = self.backend.clone();
            let healthy = Arc::clone(&self.healthy);

            self.pool.execute(move || {
                handle_connection(stream, &backend, &healthy);
            });
        }
        Ok(())
    }

    pub fn set_healthy(&self, healthy: bool) {
        self.healthy
            .store(healthy, std::sync::atomic::Ordering::Relaxed);
    }
}

fn handle_connection(mut stream: TcpStream, backend: &Backend, healthy: &AtomicBool) {
    if !healthy.load(std::sync::atomic::Ordering::Relaxed) {
        return;
    }

    let mut buf = [0; 1024];

    if stream.read(&mut buf).is_err() {
        return;
    }
    let body = format!("Hello from backend {}\n", backend.id);

    let response = format!(
        "HTTP/1.1 200 OK\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {}",
        body.len(),
        body
    );

    let _ = stream.write_all(response.as_bytes());
}
