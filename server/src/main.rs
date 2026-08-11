use std::{
    fs, io::{BufRead, BufReader, Read, Write}, net::{TcpListener, TcpStream}, sync::Arc, thread, time::Instant,
   
};

use arc_swap::ArcSwap;
use server::ThreadPool;

use crate::{algorithm_listener::run_admin_listener, algorithms::{LoadBalancer, RoundRobin, default_backends}, control::ControlServer};

mod algorithms;
mod algorithm_listener;
mod control;

fn main() {
    let backends = default_backends();

    let chosen_algorithm = RoundRobin::new(backends);

    let lb_slot = Arc::new(ArcSwap::from_pointee(
        Box::new(RoundRobin::new(default_backends())) as Box<dyn LoadBalancer>
    ));

    let admin_slot = Arc::clone(&lb_slot);
    thread::spawn(move || run_admin_listener(admin_slot));

    let lb: Arc<dyn LoadBalancer> = Arc::new(chosen_algorithm);

    let pool = ThreadPool::new(8);

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

    

    let proxy_handle = thread::spawn(move || {
        let data_listener = TcpListener::bind("127.0.0.1:7879").unwrap();

        for stream in data_listener.incoming() {
            let stream = stream.unwrap();

            let lb_slot = Arc::clone(&lb_slot);

            pool.execute(move || proxy_connections(stream, &lb_slot));
        }
    });

    control_handle.join().unwrap();
    proxy_handle.join().unwrap();

    println!("Shutting down!");
}

fn proxy_connections(mut client: TcpStream, lb_slot: &Arc<ArcSwap<Box<dyn LoadBalancer>>>) {
    let mut buf = [0; 1024];
    let lb = lb_slot.load();

    let n = match client.read(&mut buf) {
        Ok(0) => return,
        Ok(n) => n,
        Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => {
            println!("Client disconnected abruptly.");
            return;
        }
        Err(e) => {
            
            eprintln!("Unexpected network error:{}",e);
            return;
        }
    };

    let backend = lb.next();

    let start = Instant::now();
    let latency = start.elapsed();


    
        let mut upstream = match TcpStream::connect(&backend.backend.addr) {
            Ok(stream) => stream,
            Err(e) => {
                eprintln!(
                    "Failed to connect to backend {}: {}",
                    backend.backend.addr,
                    e
                );

                lb.release(&backend, latency, false);

                return;
            }
        };
        upstream.write_all(&buf[..n]).unwrap();
    
        let mut resp = Vec::new();
        upstream.read_to_end(&mut resp).unwrap();
        client.write_all(&resp).unwrap();
    


    //let latency = start.elapsed();

  

   
}



