use std::{sync::Arc, thread};

use arc_swap::ArcSwap;
use server::{
    Backend, algorithms::{
        algorithm_server::create_server,
        algorithms::{LoadBalancer, RoundRobin, default_backends},
    }, backend::registry::{self, BackendRegistry}, control::ControlServer, proxy::ProxyServer,
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

    let admin_slot: Arc<arc_swap::ArcSwapAny<Arc<Box<dyn LoadBalancer>>>> = Arc::clone(&lb_slot);
    thread::spawn(move || create_server(admin_slot,admin_registry));


    let control_registry = Arc::clone(&registry);

    let control_handle = thread::spawn(|| {
        let web_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("web");

        let server = ControlServer::new("127.0.0.1:7878", web_root, control_registry);

        if let Err(e) = server.run() {
            eprintln!("Control server stopped: {e}");
        }
    });

    let proxy_server = ProxyServer::new("127.0.0.1:7879", 8, Arc::clone(&lb_slot));

    let proxy_handle = thread::spawn(move || {
        if let Err(e) = proxy_server.run() {
            eprintln!("Proxy server stopped: {e}");
        }
    });

    control_handle.join().unwrap();
    proxy_handle.join().unwrap();

    println!("Shutting down!");
}
