use std::{
    collections::HashMap,
    net::{SocketAddr, TcpStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use crate::backend::BackendPool;

#[derive(Clone, Debug)]
pub struct HealthCheckConfig {
    pub interval: Duration,
    pub timeout: Duration,
    pub unhealthy_threshold: usize,
    pub healthy_threshold: usize,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(5),
            timeout: Duration::from_secs(2),
            unhealthy_threshold: 2,
            healthy_threshold: 2,
        }
    }
}

#[derive(Default)]
struct BackendHealthTracker {
    consecutive_successes: usize,
    consecutive_failures: usize,
}

pub struct HealthChecker {
    backend_pool: Arc<BackendPool>,
    config: HealthCheckConfig,
    trackers: Mutex<HashMap<String, BackendHealthTracker>>,
}

impl HealthChecker {
    pub fn new(backend_pool: Arc<BackendPool>, config: HealthCheckConfig) -> Self {
        Self {
            backend_pool,
            config,
            trackers: Mutex::new(HashMap::new()),
        }
    }

    /// Performs one round of TCP connect health checks against all backends currently in the registry.
    pub fn check_all(&self) {
        let nodes = self.backend_pool.backends();
        let mut trackers = self.trackers.lock().unwrap();

        for node in nodes {
            let id = &node.backend.id;
            let tracker = trackers.entry(id.clone()).or_default();
            let is_currently_healthy = node.healthy.load(Ordering::Relaxed);

            // Attempt TCP connect with timeout
            let is_up = match node.backend.address.parse::<SocketAddr>() {
                Ok(addr) => TcpStream::connect_timeout(&addr, self.config.timeout).is_ok(),
                Err(_) => false,
            };

            if is_up {
                tracker.consecutive_successes += 1;
                tracker.consecutive_failures = 0;

                if tracker.consecutive_successes >= self.config.healthy_threshold
                    && !is_currently_healthy
                {
                    node.healthy.store(true, Ordering::Relaxed);
                    eprintln!(
                        "Health check: backend {} at {} is now HEALTHY",
                        node.backend.id, node.backend.address
                    );
                }
            } else {
                tracker.consecutive_failures += 1;
                tracker.consecutive_successes = 0;

                if tracker.consecutive_failures >= self.config.unhealthy_threshold
                    && is_currently_healthy
                {
                    node.healthy.store(false, Ordering::Relaxed);
                    eprintln!(
                        "Health check: backend {} at {} is now UNHEALTHY",
                        node.backend.id, node.backend.address
                    );
                }
            }
        }
    }

    /// Spawns a background thread that periodically runs `check_all` until `shutdown` is signaled.
    pub fn start(self: Arc<Self>, shutdown: Arc<AtomicBool>) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            while !shutdown.load(Ordering::Relaxed) {
                self.check_all();

                // Sleep in small increments so we can exit promptly when shutdown is requested
                let interval = self.config.interval;
                let step = Duration::from_millis(200);
                let mut elapsed = Duration::ZERO;

                while elapsed < interval && !shutdown.load(Ordering::Relaxed) {
                    thread::sleep(step);
                    elapsed += step;
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Backend, algorithms::AlgorithmKind};
    use std::net::TcpListener;

    #[test]
    fn test_health_checker_detects_failure_and_recovery() {
        // Bind an ephemeral TCP listener to simulate a healthy backend
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();

        let backend_pool = Arc::new(
            BackendPool::new(
                AlgorithmKind::RoundRobin,
                vec![Backend {
                    id: "test-node".into(),
                    address: format!("127.0.0.1:{port}"),
                    weight: 1,
                }],
            )
            .unwrap(),
        );
        let node = backend_pool.backends().remove(0);

        let config = HealthCheckConfig {
            interval: Duration::from_millis(50),
            timeout: Duration::from_millis(100),
            unhealthy_threshold: 2,
            healthy_threshold: 2,
        };

        let checker = HealthChecker::new(Arc::clone(&backend_pool), config);

        // Initially healthy
        assert!(node.healthy.load(Ordering::Relaxed));

        // 1 check with listener open -> remains healthy
        checker.check_all();
        assert!(node.healthy.load(Ordering::Relaxed));

        // Close the listener to simulate backend failure
        drop(listener);

        // 1st failed check -> threshold 2 not reached yet, still marked healthy
        checker.check_all();
        assert!(node.healthy.load(Ordering::Relaxed));

        // 2nd failed check -> threshold 2 reached, marked unhealthy
        checker.check_all();
        assert!(!node.healthy.load(Ordering::Relaxed));

        // Re-open listener on the same port to simulate backend recovery
        let _listener_recovered = TcpListener::bind(format!("127.0.0.1:{port}")).unwrap();

        // 1st recovery check -> threshold 2 not reached yet, still unhealthy
        checker.check_all();
        assert!(!node.healthy.load(Ordering::Relaxed));

        // 2nd recovery check -> threshold 2 reached, marked healthy again!
        checker.check_all();
        assert!(node.healthy.load(Ordering::Relaxed));
    }
}
