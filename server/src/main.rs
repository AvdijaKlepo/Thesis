use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};

use arc_swap::ArcSwap;
use server::{
    Backend, RouteMatcher, RuntimeMode, Service, ServiceRegistry,
    algorithms::{AlgorithmKind, algorithm_server::create_server},
    backend::BackendPool,
    control::ControlServer,
    proxy::{HealthCheckConfig, HealthChecker, ProxyServer},
};

fn main() {
    let backend_pool = Arc::new(
        BackendPool::new(
            AlgorithmKind::RoundRobin,
            vec![
                Backend {
                    id: "1".into(),
                    address: "127.0.0.1:5053".into(),
                    weight: 1,
                },
                Backend {
                    id: "2".into(),
                    address: "127.0.0.1:5054".into(),
                    weight: 3,
                },
                Backend {
                    id: "3".into(),
                    address: "127.0.0.1:5055".into(),
                    weight: 3,
                },
            ],
        )
        .expect("default backend pool must be valid"),
    );

    let service_registry = Arc::new(ServiceRegistry::new());
    let default_service = Service::proxy(
        "default",
        vec![RouteMatcher::new(None::<String>, "/").expect("default route must be valid")],
        Arc::clone(&backend_pool),
    )
    .expect("default service must be valid");
    service_registry
        .add(default_service)
        .expect("default service must be unique");

    let admin_pool = Arc::clone(&backend_pool);
    let runtime_mode = Arc::new(ArcSwap::from_pointee(RuntimeMode::ThreadPool));

    let admin_runtime_mode = Arc::clone(&runtime_mode);
    thread::spawn(move || create_server(admin_pool, admin_runtime_mode));

    let control_pool = Arc::clone(&backend_pool);

    let control_handle = thread::spawn(|| {
        let web_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("web");

        let server = ControlServer::new("127.0.0.1:7878", web_root, control_pool);

        if let Err(e) = server.run() {
            eprintln!("Control server stopped: {e}");
        }
    });

    let health_shutdown = Arc::new(AtomicBool::new(false));
    let health_checker = Arc::new(HealthChecker::new(
        Arc::clone(&backend_pool),
        HealthCheckConfig::default(),
    ));
    let health_handle = Arc::clone(&health_checker).start(Arc::clone(&health_shutdown));

    let proxy_server = ProxyServer::new(
        "127.0.0.1:7879",
        8,
        Arc::clone(&runtime_mode),
        Arc::clone(&backend_pool),
    );

    let proxy_handle = thread::spawn(move || {
        if let Err(e) = proxy_server.run() {
            eprintln!("Proxy server stopped: {e}");
        }
    });

    control_handle.join().unwrap();
    proxy_handle.join().unwrap();
    health_shutdown.store(true, Ordering::Relaxed);
    let _ = health_handle.join();

    println!("Shutting down!");
}
