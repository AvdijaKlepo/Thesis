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
mod tests {
    use super::*;
    use crate::{
        algorithms::AlgorithmKind,
        service::{RouteMatcher, Service},
    };
    use std::{
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        thread,
    };

    #[test]
    fn structured_metrics_include_requests_and_all_services() {
        let backend_pool =
            Arc::new(BackendPool::new(AlgorithmKind::RoundRobin, Vec::new()).unwrap());
        let service_registry = Arc::new(ServiceRegistry::new());
        service_registry
            .add(
                Service::proxy(
                    "default",
                    vec![RouteMatcher::new(None::<String>, "/").unwrap()],
                    Arc::clone(&backend_pool),
                )
                .unwrap(),
            )
            .unwrap();
        let observability = Arc::new(Observability::new(["default"], false));
        let server = ControlServer::new(
            "127.0.0.1:0",
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("web"),
            backend_pool,
            service_registry,
            observability,
        );

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let client = thread::spawn(move || {
            let mut stream = TcpStream::connect(address).unwrap();
            stream
                .write_all(b"GET /api/metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
                .unwrap();
            let mut response = String::new();
            stream.read_to_string(&mut response).unwrap();
            response
        });

        let (stream, _) = listener.accept().unwrap();
        server.handle_connections(stream).unwrap();
        let response = client.join().unwrap();
        let body = response.split("\r\n\r\n").nth(1).unwrap();
        let value: serde_json::Value = serde_json::from_str(body).unwrap();

        assert_eq!(value["schema_version"], 1);
        assert_eq!(value["requests"].as_array().unwrap().len(), 2);
        assert_eq!(value["services"][0]["service_id"], "default");
        assert_eq!(value["services"][0]["algorithm"], "round_robin");
    }
}
