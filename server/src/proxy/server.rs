use std::{io::{self, }, net::TcpListener, sync::Arc};

use arc_swap::ArcSwap;


use crate::{algorithms::algorithms::LoadBalancer, proxy::connection::proxy_connections, worker::ThreadPool};

pub struct ProxyServer {
    address: String,
    pool: ThreadPool,
    load_balancer: Arc<ArcSwap<Box<dyn LoadBalancer>>>,
  
}

impl ProxyServer {
    pub fn new(address: impl Into<String>, pool_size: usize, load_balancer: Arc<ArcSwap<Box<dyn LoadBalancer>>>) -> Self {
        Self { address: address.into(), pool: ThreadPool::new(pool_size), load_balancer}
    }

    pub fn run(&self) -> io::Result<()> {
        let listener = TcpListener::bind(&self.address)?;

        println!("Proxy server listening on {}", self.address);

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let load_balancer = Arc::clone(&self.load_balancer);

                    self.pool.execute(move || {
                        if let Some(result) = proxy_connections(stream, &load_balancer) {
                            println!(
                                "Proxy request finished: backend={} success={} latency={:?} bytes_sent={} bytes_recv={}",
                                result.backend_id, result.success, result.latency, result.bytes_sent, result.bytes_received
                            );
                        }
                    });

                    
                }

                Err(e) => {
                    eprintln!("Failed to accept proxy connection: {e}");
                }
            }
        }

        Ok(())
    }

    
}



