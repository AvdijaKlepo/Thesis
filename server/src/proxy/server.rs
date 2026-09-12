use std::{
    io,
    sync::{Arc, Mutex},
};

use arc_swap::ArcSwap;

use crate::{
    algorithms::algorithms::LoadBalancer,
    proxy::connection::proxy_connections,
    proxy::runtime::{RuntimeMode, proxy_connections_async},
    worker::ThreadPool,
};

pub struct ProxyServer {
    address: String,
    pool: Arc<Mutex<ThreadPool>>,
    runtime_mode: Arc<ArcSwap<RuntimeMode>>,
    load_balancer: Arc<ArcSwap<Box<dyn LoadBalancer>>>,
}

impl ProxyServer {
    pub fn new(
        address: impl Into<String>,
        pool_size: usize,
        runtime_mode: Arc<ArcSwap<RuntimeMode>>,
        load_balancer: Arc<ArcSwap<Box<dyn LoadBalancer>>>,
    ) -> Self {
        Self {
            address: address.into(),
            pool: Arc::new(Mutex::new(ThreadPool::new(pool_size))),
            runtime_mode,
            load_balancer,
        }
    }

    /// Start the proxy server.
    ///
    /// Builds a Tokio multi-thread runtime internally and runs an async
    /// accept loop.  Each accepted connection is dispatched according to
    /// the current [`RuntimeMode`]:
    ///
    /// * **ThreadPool** — the tokio stream is converted to a std stream
    ///   and handed to `spawn_blocking` which runs the existing
    ///   synchronous `proxy_connections()`.
    /// * **Async** — the connection is processed inside a `tokio::spawn`
    ///   task using fully asynchronous I/O.
    pub fn run(&self) -> io::Result<()> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        let address = self.address.clone();
        let pool = Arc::clone(&self.pool);
        let runtime_mode = Arc::clone(&self.runtime_mode);
        let load_balancer = Arc::clone(&self.load_balancer);

        rt.block_on(async move {
            let listener = tokio::net::TcpListener::bind(&address).await?;
            println!("Proxy server listening on {}", address);

            loop {
                let (stream, _addr) = listener.accept().await?;

                let mode = **runtime_mode.load();
                let lb = Arc::clone(&load_balancer);

                match mode {
                    RuntimeMode::ThreadPool => {
                        // Convert to std stream for the blocking handler.
                        let std_stream = stream.into_std()?;
                        let pool = Arc::clone(&pool);
                        pool.lock().unwrap().execute(move || {
                            if let Some(result) = proxy_connections(std_stream, &lb) {
                                println!(
                                    "Proxy request finished [thread_pool]: backend={} success={} latency={:?} bytes_sent={} bytes_recv={}",
                                    result.backend_id, result.success, result.latency, result.bytes_sent, result.bytes_received
                                );
                            }
                        });
                    }
                    RuntimeMode::Async => {
                        tokio::spawn(async move {
                            if let Some(result) = proxy_connections_async(stream, &lb).await {
                                println!(
                                    "Proxy request finished [async]: backend={} success={} latency={:?} bytes_sent={} bytes_recv={}",
                                    result.backend_id, result.success, result.latency, result.bytes_sent, result.bytes_received
                                );
                            }
                        });
                    }
                }
            }
        })
    }
}