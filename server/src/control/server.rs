use std::{
    io::{self, BufRead, BufReader, Write},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    sync::Arc,
};

use crate::{
    backend::BackendPool, control::static_file::StaticFileHandler, observability::Observability,
    service::ServiceRegistry,
};

pub struct ControlServer {
    address: String,

    static_files: StaticFileHandler,

    backend_pool: Arc<BackendPool>,

    service_registry: Arc<ServiceRegistry>,

    observability: Arc<Observability>,
}

impl ControlServer {
    pub fn new(
        address: impl Into<String>,
        root: impl Into<PathBuf>,
        backend_pool: Arc<BackendPool>,
        service_registry: Arc<ServiceRegistry>,
        observability: Arc<Observability>,
    ) -> Self {
        Self {
            address: address.into(),
            static_files: StaticFileHandler::new(root),
            backend_pool,
            service_registry,
            observability,
        }
    }

    pub fn run(&self) -> io::Result<()> {
        let listener = TcpListener::bind(&self.address)?;

        eprintln!("Control server listening on {}", self.address);

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    if let Err(e) = self.handle_connections(stream) {
                        eprint!("Control connection error: {e}");
                    }
                }
                Err(e) => {
                    eprintln!("Failed to accept control connection: {e}");
                }
            }
        }

        Ok(())
    }

    fn handle_connections(&self, mut stream: TcpStream) -> io::Result<()> {
        let request_line = {
            let reader = BufReader::new(&stream);

            match reader.lines().next() {
                Some(Ok(line)) => line,
                Some(Err(e)) => return Err(e),
                None => return Ok(()),
            }
        };

        let request_path = request_line
            .split_whitespace()
            .nth(1)
            .unwrap_or("/")
            .split('?')
            .next()
            .unwrap_or("/");

        if request_path == "/api/metrics" {
            let summary = self.observability.snapshot(&self.service_registry);
            let json = serde_json::to_string(&summary).unwrap_or_else(|_| "{}".into());
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                json.len(),
                json
            );
            stream.write_all(resp.as_bytes())?;
            return Ok(());
        }

        if request_path == "/metrics" {
            let summary = self.backend_pool.metrics_summary();
            let json = serde_json::to_string(&summary).unwrap_or_else(|_| "[]".into());
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                json.len(),
                json
            );
            stream.write_all(resp.as_bytes())?;
            return Ok(());
        }

        let response = self.static_files.serve(request_path)?;

        stream.write_all(&response.to_http())?;

        Ok(())
    }
}
#[cfg(test)]
#[path = "server_tests.rs"]
mod tests;
