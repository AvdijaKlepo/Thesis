use std::{
    io::{Read, Write},
    net::TcpStream,
    sync::Arc,
    time::{Duration, Instant},
};

use arc_swap::ArcSwap;

use crate::{algorithms::algorithms::LoadBalancer, backend::backend_server::Feedback};

pub struct ActiveConnectionGuard {
    metrics: Arc<crate::backend::backend_server::BackendMetrics>,
    completed: bool,
}

impl ActiveConnectionGuard {
    pub fn new(metrics: Arc<crate::backend::backend_server::BackendMetrics>) -> Self {
        metrics.record_start();
        Self {
            metrics,
            completed: false,
        }
    }

    pub fn complete(mut self, feedback: &Feedback, bytes_sent: usize, bytes_received: usize) {
        self.completed = true;
        self.metrics.record_end(feedback, bytes_sent, bytes_received);
    }
}

impl Drop for ActiveConnectionGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.metrics.record_end(
                &Feedback {
                    latency: Duration::ZERO,
                    success: false,
                },
                0,
                0,
            );
        }
    }
}

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
    let guard = ActiveConnectionGuard::new(Arc::clone(&backend.metrics));

    println!(
        "Selected backend {} at {} (active connections: {})",
        backend.backend.id,
        backend.backend.address,
        backend.metrics.active_connections.load(std::sync::atomic::Ordering::Relaxed)
    );

    let mut upstream = match TcpStream::connect(&backend.backend.address) {
        Ok(stream) => stream,
        Err(e) => {
            eprintln!(
                "Failed to connect to backend {}: {}",
                backend.backend.address, e
            );

            let latency = start.elapsed();
            let feedback = Feedback {
                latency,
                success: false,
            };

            lb.release(&backend, feedback.clone());
            guard.complete(&feedback, 0, 0);

            return Some(ProxyResult {
                backend_id: backend.backend.id,
                latency,
                success: false,
                bytes_sent: 0,
                bytes_received: 0,
            });
        }
    };
    let upstream_req = prepare_upstream_request(&buf[..n]);

    if let Err(e) = upstream.write_all(&upstream_req) {
        eprintln!("Failed to send request to backend: {e}");

        let latency = start.elapsed();
        let feedback = Feedback {
            latency,
            success: false,
        };

        lb.release(&backend, feedback.clone());
        guard.complete(&feedback, upstream_req.len(), 0);

        return Some(ProxyResult {
            backend_id: backend.backend.id,
            latency,
            success: false,
            bytes_sent: upstream_req.len(),
            bytes_received: 0,
        });
    }

    let resp = match read_http_response(&mut upstream) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Failed to read response from backend: {e}");

            let latency = start.elapsed();
            let feedback = Feedback {
                latency,
                success: false,
            };

            lb.release(&backend, feedback.clone());
            guard.complete(&feedback, upstream_req.len(), 0);

            return Some(ProxyResult {
                backend_id: backend.backend.id,
                latency,
                success: false,
                bytes_sent: upstream_req.len(),
                bytes_received: 0,
            });
        }
    };

    if resp.is_empty() {
        eprintln!("Empty response from backend");

        let latency = start.elapsed();
        let feedback = Feedback {
            latency,
            success: false,
        };

        lb.release(&backend, feedback.clone());
        guard.complete(&feedback, upstream_req.len(), 0);

        return Some(ProxyResult {
            backend_id: backend.backend.id,
            latency,
            success: false,
            bytes_sent: upstream_req.len(),
            bytes_received: 0,
        });
    }

    println!("About to write {} bytes to client", resp.len());

    println!("Client peer: {:?}", client.peer_addr());

    if let Err(e) = client.write_all(&resp) {
        eprintln!("Failed to send response to client: {e}");

        let latency = start.elapsed();
        let feedback = Feedback {
            latency,
            success: false,
        };

        lb.release(&backend, feedback.clone());
        guard.complete(&feedback, upstream_req.len(), resp.len());

        return Some(ProxyResult {
            backend_id: backend.backend.id,
            latency,
            success: false,
            bytes_sent: upstream_req.len(),
            bytes_received: resp.len(),
        });
    }

    let latency = start.elapsed();
    let feedback = Feedback {
        latency,
        success: true,
    };

    lb.release(&backend, feedback.clone());
    guard.complete(&feedback, upstream_req.len(), resp.len());

    return Some(ProxyResult {
        backend_id: backend.backend.id,
        latency,
        success: true,
        bytes_sent: upstream_req.len(),
        bytes_received: resp.len(),
    });
}

pub fn find_header_end(buf: &[u8]) -> Option<usize> {
    for i in 0..buf.len() {
        if buf[i..].starts_with(b"\r\n\r\n") {
            return Some(i + 4);
        }
        if buf[i..].starts_with(b"\n\n") {
            return Some(i + 2);
        }
    }
    None
}

pub fn prepare_upstream_request(raw_req: &[u8]) -> Vec<u8> {
    if let Some(header_end) = find_header_end(raw_req) {
        let headers_bytes = &raw_req[..header_end];
        let body_bytes = &raw_req[header_end..];

        let header_str = String::from_utf8_lossy(headers_bytes);
        let mut new_headers = String::with_capacity(headers_bytes.len() + 32);
        let mut has_connection = false;

        for line in header_str.lines() {
            if line.is_empty() {
                continue;
            }
            if let Some((name, _)) = line.split_once(':') {
                if name.trim().eq_ignore_ascii_case("connection") {
                    has_connection = true;
                    new_headers.push_str("Connection: close\r\n");
                    continue;
                }
            }
            new_headers.push_str(line);
            new_headers.push_str("\r\n");
        }

        if !has_connection {
            if let Some(idx) = new_headers.find("\r\n") {
                new_headers.insert_str(idx + 2, "Connection: close\r\n");
            }
        }
        new_headers.push_str("\r\n");

        let mut out = new_headers.into_bytes();
        out.extend_from_slice(body_bytes);
        out
    } else {
        raw_req.to_vec()
    }
}

pub fn read_http_response<R: Read>(stream: &mut R) -> std::io::Result<Vec<u8>> {
    let mut buffer = Vec::with_capacity(8192);
    let mut chunk_buf = [0u8; 4096];
    let mut header_end = None;

    while header_end.is_none() {
        let n = stream.read(&mut chunk_buf)?;
        if n == 0 {
            if buffer.is_empty() {
                return Ok(Vec::new());
            }
            return Ok(buffer);
        }
        buffer.extend_from_slice(&chunk_buf[..n]);
        if let Some(pos) = find_header_end(&buffer) {
            header_end = Some(pos);
        }
    }

    let header_end_idx = header_end.unwrap();
    let header_str = String::from_utf8_lossy(&buffer[..header_end_idx]);

    // Parse status code
    let status_code = header_str
        .lines()
        .next()
        .and_then(|line| {
            let mut parts = line.split_whitespace();
            parts.next()?;
            parts.next()?.parse::<u16>().ok()
        })
        .unwrap_or(200);

    // 1xx, 204, 304 have no body
    if (100..200).contains(&status_code) || status_code == 204 || status_code == 304 {
        buffer.truncate(header_end_idx);
        return Ok(buffer);
    }

    let is_chunked = header_str.lines().any(|line| {
        if let Some((name, val)) = line.split_once(':') {
            if name.trim().eq_ignore_ascii_case("transfer-encoding") {
                return val.to_ascii_lowercase().contains("chunked");
            }
        }
        false
    });

    let content_length = header_str.lines().find_map(|line| {
        let (name, val) = line.split_once(':')?;
        if name.trim().eq_ignore_ascii_case("content-length") {
            val.trim().parse::<usize>().ok()
        } else {
            None
        }
    });

    if is_chunked {
        let mut cursor = header_end_idx;
        loop {
            let crlf_pos = loop {
                if let Some(pos) = buffer[cursor..]
                    .windows(2)
                    .position(|w| w == b"\r\n")
                {
                    break cursor + pos;
                }
                let n = stream.read(&mut chunk_buf)?;
                if n == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "Unexpected EOF while reading chunk size",
                    ));
                }
                buffer.extend_from_slice(&chunk_buf[..n]);
            };

            let line = String::from_utf8_lossy(&buffer[cursor..crlf_pos]);
            let hex_part = line.split(';').next().unwrap_or("").trim();
            let chunk_size = usize::from_str_radix(hex_part, 16).map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Invalid chunk size: {e}"))
            })?;

            if chunk_size == 0 {
                let trailer_start = crlf_pos + 2;
                loop {
                    if buffer.len() >= trailer_start + 2
                        && &buffer[trailer_start..trailer_start + 2] == b"\r\n"
                    {
                        buffer.truncate(trailer_start + 2);
                        return Ok(buffer);
                    }
                    if let Some(pos) = buffer[trailer_start..]
                        .windows(4)
                        .position(|w| w == b"\r\n\r\n")
                    {
                        buffer.truncate(trailer_start + pos + 4);
                        return Ok(buffer);
                    }
                    let n = stream.read(&mut chunk_buf)?;
                    if n == 0 {
                        return Ok(buffer);
                    }
                    buffer.extend_from_slice(&chunk_buf[..n]);
                }
            }

            let chunk_data_start = crlf_pos + 2;
            let chunk_data_end = chunk_data_start + chunk_size;
            let chunk_full_end = chunk_data_end + 2;

            while buffer.len() < chunk_full_end {
                let n = stream.read(&mut chunk_buf)?;
                if n == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "Unexpected EOF while reading chunk body",
                    ));
                }
                buffer.extend_from_slice(&chunk_buf[..n]);
            }

            cursor = chunk_full_end;
        }
    } else if let Some(cl) = content_length {
        let total_needed = header_end_idx + cl;
        while buffer.len() < total_needed {
            let n = stream.read(&mut chunk_buf)?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Unexpected EOF while reading Content-Length body",
                ));
            }
            buffer.extend_from_slice(&chunk_buf[..n]);
        }
        buffer.truncate(total_needed);
        Ok(buffer)
    } else {
        stream.read_to_end(&mut buffer)?;
        Ok(buffer)
    }
}

pub struct ProxyResult {
    pub backend_id: String,
    pub latency: Duration,
    pub success: bool,
    pub bytes_sent: usize,
    pub bytes_received: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_content_length() {
        let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello";
        let mut cursor = Cursor::new(raw);
        let resp = read_http_response(&mut cursor).unwrap();
        assert_eq!(resp, raw);
    }

    #[test]
    fn test_content_length_with_trailing_data() {
        let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhelloEXTRA_BYTES";
        let mut cursor = Cursor::new(raw);
        let resp = read_http_response(&mut cursor).unwrap();
        assert_eq!(resp, b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello");
    }

    #[test]
    fn test_chunked() {
        let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n";
        let mut cursor = Cursor::new(raw);
        let resp = read_http_response(&mut cursor).unwrap();
        assert_eq!(resp, raw);
    }

    #[test]
    fn test_chunked_with_trailing_data() {
        let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\nEXTRA_PIPELINE";
        let mut cursor = Cursor::new(raw);
        let resp = read_http_response(&mut cursor).unwrap();
        assert_eq!(resp, b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n");
    }

    #[test]
    fn test_status_204_no_body() {
        let raw = b"HTTP/1.1 204 No Content\r\nConnection: keep-alive\r\n\r\n";
        let mut cursor = Cursor::new(raw);
        let resp = read_http_response(&mut cursor).unwrap();
        assert_eq!(resp, raw);
    }

    #[test]
    fn test_prepare_upstream_request_injects_connection_close() {
        let raw = b"GET /company HTTP/1.1\r\nHost: localhost:7879\r\n\r\n";
        let modified = prepare_upstream_request(raw);
        let s = String::from_utf8_lossy(&modified);
        assert!(s.contains("Connection: close\r\n"));
    }

    #[test]
    fn test_prepare_upstream_request_replaces_keep_alive() {
        let raw = b"GET /company HTTP/1.1\r\nHost: localhost:7879\r\nConnection: keep-alive\r\n\r\n";
        let modified = prepare_upstream_request(raw);
        let s = String::from_utf8_lossy(&modified);
        assert!(s.contains("Connection: close\r\n"));
        assert!(!s.contains("keep-alive"));
    }

    #[test]
    fn test_active_connection_guard_complete() {
        let metrics = Arc::new(crate::backend::backend_server::BackendMetrics::new());
        assert_eq!(metrics.active_connections.load(std::sync::atomic::Ordering::Relaxed), 0);

        let guard = ActiveConnectionGuard::new(Arc::clone(&metrics));
        assert_eq!(metrics.active_connections.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(metrics.total_requests.load(std::sync::atomic::Ordering::Relaxed), 1);

        guard.complete(
            &Feedback {
                latency: Duration::from_millis(5),
                success: true,
            },
            100,
            200,
        );

        let snap = metrics.snapshot();
        assert_eq!(snap.active_connections, 0);
        assert_eq!(snap.successful_requests, 1);
        assert_eq!(snap.failed_requests, 0);
    }

    #[test]
    fn test_active_connection_guard_drop_on_error() {
        let metrics = Arc::new(crate::backend::backend_server::BackendMetrics::new());
        {
            let _guard = ActiveConnectionGuard::new(Arc::clone(&metrics));
            assert_eq!(metrics.active_connections.load(std::sync::atomic::Ordering::Relaxed), 1);
            // Dropped here without calling complete
        }
        let snap = metrics.snapshot();
        assert_eq!(snap.active_connections, 0);
        assert_eq!(snap.total_requests, 1);
        assert_eq!(snap.successful_requests, 0);
        assert_eq!(snap.failed_requests, 1);
    }
}


