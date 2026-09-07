use std::{
    io::{Read, Write}, net::{TcpListener, TcpStream}, sync::{Arc, atomic::{AtomicBool, AtomicU64, AtomicUsize}}, time::Duration,
};

use serde::Serialize;

use crate::worker::ThreadPool;

#[derive(Clone, Debug, Serialize)]
pub struct Backend {
    pub id: String,
    pub address: String,
    pub weight: usize
}

#[derive(Clone, Debug, Serialize)]
pub struct BackendMetricsSnapshot {
    pub active_connections: usize,
    pub latency_us: u64,
    pub total_requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub total_bytes_sent: u64,
    pub total_bytes_received: u64,
}

#[derive(Debug)]
pub struct BackendMetrics {
    pub active_connections: AtomicUsize,
    pub latency_us: AtomicU64,
    pub total_requests: AtomicU64,
    pub successful_requests: AtomicU64,
    pub failed_requests: AtomicU64,
    pub total_bytes_sent: AtomicU64,
    pub total_bytes_received: AtomicU64,
}

#[derive(Clone, Debug)]
pub struct Feedback {
    pub latency: Duration,
    pub success: bool,
}

impl BackendMetrics {
    pub fn new() -> Self {
        Self {
            active_connections: AtomicUsize::new(0),
            latency_us: AtomicU64::new(1000),
            total_requests: AtomicU64::new(0),
            successful_requests: AtomicU64::new(0),
            failed_requests: AtomicU64::new(0),
            total_bytes_sent: AtomicU64::new(0),
            total_bytes_received: AtomicU64::new(0),
        }
    }

    pub fn record_start(&self) {
        self.active_connections.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.total_requests.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn record_end(&self, feedback: &Feedback, bytes_sent: usize, bytes_received: usize) {
        let _ = self.active_connections.fetch_update(
            std::sync::atomic::Ordering::SeqCst,
            std::sync::atomic::Ordering::Relaxed,
            |val| Some(val.saturating_sub(1)),
        );

        if feedback.success {
            self.successful_requests.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            let measured = feedback.latency.as_micros() as u64;
            let old = self.latency_us.load(std::sync::atomic::Ordering::Relaxed);
            let alpha = 0.2;
            let new_latency = ((1.0 - alpha) * old as f64 + alpha * measured as f64) as u64;
            self.latency_us.store(new_latency.max(1), std::sync::atomic::Ordering::Relaxed);
        } else {
            self.failed_requests.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        self.total_bytes_sent.fetch_add(bytes_sent as u64, std::sync::atomic::Ordering::Relaxed);
        self.total_bytes_received.fetch_add(bytes_received as u64, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> BackendMetricsSnapshot {
        BackendMetricsSnapshot {
            active_connections: self.active_connections.load(std::sync::atomic::Ordering::Relaxed),
            latency_us: self.latency_us.load(std::sync::atomic::Ordering::Relaxed),
            total_requests: self.total_requests.load(std::sync::atomic::Ordering::Relaxed),
            successful_requests: self.successful_requests.load(std::sync::atomic::Ordering::Relaxed),
            failed_requests: self.failed_requests.load(std::sync::atomic::Ordering::Relaxed),
            total_bytes_sent: self.total_bytes_sent.load(std::sync::atomic::Ordering::Relaxed),
            total_bytes_received: self.total_bytes_received.load(std::sync::atomic::Ordering::Relaxed),
        }
    }
}

impl Default for BackendMetrics {
    fn default() -> Self {
        Self::new()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backend_metrics_lifecycle() {
        let metrics = BackendMetrics::new();
        assert_eq!(metrics.active_connections.load(std::sync::atomic::Ordering::Relaxed), 0);

        metrics.record_start();
        assert_eq!(metrics.active_connections.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(metrics.total_requests.load(std::sync::atomic::Ordering::Relaxed), 1);

        metrics.record_end(
            &Feedback {
                latency: Duration::from_micros(500),
                success: true,
            },
            100,
            250,
        );

        let snap = metrics.snapshot();
        assert_eq!(snap.active_connections, 0);
        assert_eq!(snap.total_requests, 1);
        assert_eq!(snap.successful_requests, 1);
        assert_eq!(snap.failed_requests, 0);
        assert_eq!(snap.total_bytes_sent, 100);
        assert_eq!(snap.total_bytes_received, 250);
        // (1.0 - 0.2) * 1000 + 0.2 * 500 = 800 + 100 = 900
        assert_eq!(snap.latency_us, 900);
    }

    #[test]
    fn test_backend_metrics_failure() {
        let metrics = BackendMetrics::new();
        metrics.record_start();
        metrics.record_end(
            &Feedback {
                latency: Duration::from_millis(10),
                success: false,
            },
            50,
            0,
        );

        let snap = metrics.snapshot();
        assert_eq!(snap.active_connections, 0);
        assert_eq!(snap.total_requests, 1);
        assert_eq!(snap.successful_requests, 0);
        assert_eq!(snap.failed_requests, 1);
        assert_eq!(snap.total_bytes_sent, 50);
        assert_eq!(snap.total_bytes_received, 0);
    }

    #[test]
    fn test_active_connections_underflow_prevention() {
        let metrics = BackendMetrics::new();
        // Record end without start should not underflow
        metrics.record_end(
            &Feedback {
                latency: Duration::from_millis(1),
                success: false,
            },
            0,
            0,
        );
        assert_eq!(metrics.active_connections.load(std::sync::atomic::Ordering::Relaxed), 0);
    }
}

