use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
};

use arc_swap::ArcSwap;
use server::{
    algorithms::{
        algorithm_server::create_server,
        algorithms::{LoadBalancer, RoundRobin},
    },
    backend::registry::BackendRegistry,
    control::ControlServer,
    proxy::{HealthCheckConfig, HealthChecker, ProxyServer},
    Backend, RuntimeMode,
};

fn main() {

    let registry = Arc::new(BackendRegistry::new());

    registry.add(
    Backend {
        id: "1".into(),
        address: "127.0.0.1:5053".into(),
        weight: 1,
    }
    .into()
);
registry.add(
    Backend {
        id: "2".into(),
        address: "127.0.0.1:5054".into(),
        weight: 3,
    }
    .into()
);
registry.add(
    Backend {
        id: "3".into(),
        address: "127.0.0.1:5055".into(),
        weight: 3,
    }
    .into()
);



    let backends = registry.all();
    
    let lb_slot = Arc::new(ArcSwap::from_pointee(
    Box::new(RoundRobin::new(backends)) as Box<dyn LoadBalancer>,
));
    let admin_registry = Arc::clone(&registry);
    let runtime_mode = Arc::new(ArcSwap::from_pointee(RuntimeMode::ThreadPool));

    let admin_slot: Arc<arc_swap::ArcSwapAny<Arc<Box<dyn LoadBalancer>>>> = Arc::clone(&lb_slot);
    let admin_runtime_mode = Arc::clone(&runtime_mode);
    thread::spawn(move || create_server(admin_slot, admin_registry, admin_runtime_mode));


    let control_registry = Arc::clone(&registry);

    let control_handle = thread::spawn(|| {
        let web_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("web");

        let server = ControlServer::new("127.0.0.1:7878", web_root, control_registry);

        if let Err(e) = server.run() {
            eprintln!("Control server stopped: {e}");
        }
    });

    let health_shutdown = Arc::new(AtomicBool::new(false));
    let health_checker = Arc::new(HealthChecker::new(
        Arc::clone(&registry),
        HealthCheckConfig::default(),
    ));
    let health_handle = Arc::clone(&health_checker).start(Arc::clone(&health_shutdown));

    let proxy_server = ProxyServer::new("127.0.0.1:7879", 8, Arc::clone(&runtime_mode), Arc::clone(&lb_slot));

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
