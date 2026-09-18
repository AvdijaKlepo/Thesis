use std::{
    collections::HashMap,
    net::{TcpStream, ToSocketAddrs},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use crate::service::ServiceRegistry;

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
    service_registry: Arc<ServiceRegistry>,
    config: HealthCheckConfig,
    trackers: Mutex<HashMap<(String, String), BackendHealthTracker>>,
}

impl HealthChecker {
    pub fn new(service_registry: Arc<ServiceRegistry>, config: HealthCheckConfig) -> Self {
        Self {
            service_registry,
            config,
            trackers: Mutex::new(HashMap::new()),
        }
    }

    /// Performs one round of TCP connect health checks against all backends currently in the registry.
    pub fn check_all(&self) {
        let mut trackers = self.trackers.lock().unwrap();
        for service in self.service_registry.all() {
            let Some(pool) = service.proxy_pool() else {
                continue;
            };
            for node in pool.backends() {
                let tracker = trackers
                    .entry((service.id.clone(), node.backend.id.clone()))
                    .or_default();
                let is_currently_healthy = node.healthy.load(Ordering::Relaxed);
                let is_up = node
                    .backend
                    .address
                    .to_socket_addrs()
                    .is_ok_and(|addresses| {
                        addresses.into_iter().any(|address| {
                            TcpStream::connect_timeout(&address, self.config.timeout).is_ok()
                        })
                    });

                if is_up {
                    tracker.consecutive_successes += 1;
                    tracker.consecutive_failures = 0;

                    if tracker.consecutive_successes >= self.config.healthy_threshold
                        && !is_currently_healthy
                    {
                        node.healthy.store(true, Ordering::Relaxed);
                        eprintln!(
                            "Health check: backend {}/{} at {} is now HEALTHY",
                            service.id, node.backend.id, node.backend.address
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
                            "Health check: backend {}/{} at {} is now UNHEALTHY",
                            service.id, node.backend.id, node.backend.address
                        );
                    }
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
#[path = "tests/health_tests.rs"]
mod tests;
