use std::{
    io::{Read, Write},
    net::TcpStream,
    sync::Arc,
    time::{Duration, Instant},
};

use arc_swap::ArcSwap;

use crate::{algorithms::algorithms::LoadBalancer, backend::backend_server::Feedback};

pub fn proxy_connections(
    mut client: TcpStream,
    lb_slot: &Arc<ArcSwap<Box<dyn LoadBalancer>>>,
) -> Option<ProxyResult> {
    println!("Proxy connection received");
    let mut buf = [0; 1024];

    let n = match client.read(&mut buf) {
        Ok(0) => return None,
        Ok(n) => n,
        Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => {
            println!("Client disconnected abruptly.");
            return None;
        }
        Err(e) => {
            eprintln!("Unexpected network error:{}", e);
            return None;
        }
    };
    println!("Received {} bytes from client", n);
    println!("Request:\n{}", String::from_utf8_lossy(&buf[..n]));

    let lb = lb_slot.load();

    let start = Instant::now();
    let backend = lb.next();

    println!(
        "Selected backend {} at {}",
        backend.backend.id, backend.backend.address
    );

    let mut upstream = match TcpStream::connect(&backend.backend.address) {
        Ok(stream) => stream,
        Err(e) => {
            eprintln!(
                "Failed to connect to backend {}: {}",
                backend.backend.address, e
            );

            let latency = start.elapsed();

            lb.release(
                &backend,
                Feedback {
                    latency,
                    success: false,
                },
            );

            return Some(ProxyResult {
                backend_id: backend.backend.id,
                latency,
                success: false,
                bytes_sent: 0,
                bytes_received: 0,
            });
        }
    };
    if let Err(e) = upstream.write_all(&buf[..n]) {
        eprintln!("Failed to send request to backend: {e}");

        let latency = start.elapsed();

        lb.release(
            &backend,
            Feedback {
                latency,
                success: false,
            },
        );

        return Some(ProxyResult {
            backend_id: backend.backend.id,
            latency,
            success: false,
            bytes_sent: n,
            bytes_received: 0,
        });
    }

    let mut resp = Vec::new();

    if let Err(e) = upstream.read_to_end(&mut resp) {
        eprintln!("Failed to read response from backend: {e}");

        let latency = start.elapsed();

        lb.release(
            &backend,
            Feedback {
                latency,
                success: false,
            },
        );

        return Some(ProxyResult {
            backend_id: backend.backend.id,
            latency,
            success: false,
            bytes_sent: n,
            bytes_received: resp.len(),
        });
    }
    println!("About to write {} bytes to client", resp.len());

    println!("Client peer: {:?}", client.peer_addr());

    if let Err(e) = client.write_all(&resp) {
        eprintln!("Failed to send response to client: {e}");

        let latency = start.elapsed();

        lb.release(
            &backend,
            Feedback {
                latency,
                success: false,
            },
        );

        return Some(ProxyResult {
            backend_id: backend.backend.id,
            latency,
            success: false,
            bytes_sent: n,
            bytes_received: resp.len(),
        });
    }

    let latency = start.elapsed();

    lb.release(
        &backend,
        Feedback {
            latency,
            success: false,
        },
    );

    return Some(ProxyResult {
        backend_id: backend.backend.id,
        latency,
        success: true,
        bytes_sent: n,
        bytes_received: resp.len(),
    });
}

pub struct ProxyResult {
    pub backend_id: String,
    pub latency: Duration,
    pub success: bool,
    pub bytes_sent: usize,
    pub bytes_received: usize,
}
