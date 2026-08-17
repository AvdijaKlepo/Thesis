use std::{
    io::{Read, Write},
    net::TcpStream,
    sync::Arc,
    time::Instant,
};

use arc_swap::ArcSwap;

use crate::algorithms::LoadBalancer;

pub fn proxy_connections(mut client: TcpStream, lb_slot: &Arc<ArcSwap<Box<dyn LoadBalancer>>>) {
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
    
    }