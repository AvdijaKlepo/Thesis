use std::{
    sync::Arc, thread,
   
};

use arc_swap::ArcSwap;
use server::ThreadPool;

use crate::{algorithm_listener::run_admin_listener, algorithms::{LoadBalancer, RoundRobin, default_backends}, control::ControlServer, proxy::ProxyServer};

mod algorithms;
mod algorithm_listener;
mod control;
mod proxy;

fn main() {
   

   

    let lb_slot = Arc::new(ArcSwap::from_pointee(
        Box::new(RoundRobin::new(default_backends())) as Box<dyn LoadBalancer>
    ));

    let admin_slot = Arc::clone(&lb_slot);
    thread::spawn(move || run_admin_listener(admin_slot));



    let control_handle = thread::spawn(|| {
    let web_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("web");

    let server = ControlServer::new(
        "127.0.0.1:7878",
        web_root,
    );

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





