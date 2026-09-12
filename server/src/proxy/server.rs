use std::{
    io,
    sync::{Arc, Mutex},
};

use arc_swap::ArcSwap;

use crate::{
    observability::Observability,
    proxy::connection::proxy_connections,
    proxy::runtime::{RuntimeMode, proxy_connections_async},
    service::ServiceRouter,
    worker::ThreadPool,
};

pub struct ProxyServer {
    address: String,
    pool: Arc<Mutex<ThreadPool>>,
    runtime_mode: Arc<ArcSwap<RuntimeMode>>,
    router: ServiceRouter,
    observability: Arc<Observability>,
}

impl ProxyServer {
    pub fn new(
        address: impl Into<String>,
        pool_size: usize,
        runtime_mode: Arc<ArcSwap<RuntimeMode>>,
        router: ServiceRouter,
        observability: Arc<Observability>,
    ) -> Self {
        Self {
            address: address.into(),
            pool: Arc::new(Mutex::new(ThreadPool::new(pool_size))),
            runtime_mode,
            router,
            observability,
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
        let router = self.router.clone();
        let observability = Arc::clone(&self.observability);

        rt.block_on(async move {
            let listener = tokio::net::TcpListener::bind(&address).await?;
            eprintln!("Proxy server listening on {}", address);

            loop {
                let (stream, _addr) = listener.accept().await?;

                let mode = **runtime_mode.load();
                let router = router.clone();
                let observability = Arc::clone(&observability);

                match mode {
                    RuntimeMode::ThreadPool => {
                        // Convert to std stream for the blocking handler.
                        let std_stream = stream.into_std()?;
                        let pool = Arc::clone(&pool);
                        pool.lock().unwrap().execute(move || {
                            let _ = proxy_connections(std_stream, &router, &observability);
                        });
                    }
                    RuntimeMode::Async => {
                        tokio::spawn(async move {
                            let _ = proxy_connections_async(stream, &router, &observability).await;
                        });
                    }
                }
            }
        })
    }
}
