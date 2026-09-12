use std::{
    io::{Read, Write},
    net::TcpStream,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{
    backend::backend_server::Feedback,
    control::StaticFileHandler,
    service::{ServiceRouter, ServiceTarget},
};

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
        self.metrics
            .record_end(feedback, bytes_sent, bytes_received);
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

pub const MAX_BODY_SIZE: usize = 2 * 1024 * 1024; // 2 MiB
pub const MAX_HEADER_SIZE: usize = 64 * 1024; // 64 KiB
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
pub const READ_TIMEOUT: Duration = Duration::from_secs(10);
pub const WRITE_TIMEOUT: Duration = Duration::from_secs(10);
pub const MAX_RETRIES: usize = 2;

pub fn service_unavailable_response() -> Vec<u8> {
    let body = "503 Service Unavailable: No eligible backend\n";
    format!(
        "HTTP/1.1 503 Service Unavailable\r\nContent-Length: {}\r\nConnection: close\r\nContent-Type: text/plain\r\n\r\n{}",
        body.len(),
        body
    )
    .into_bytes()
}

pub fn internal_server_error_response() -> Vec<u8> {
    let body = "500 Internal Server Error\n";
    format!(
        "HTTP/1.1 500 Internal Server Error\r\nContent-Length: {}\r\nConnection: close\r\nContent-Type: text/plain\r\n\r\n{}",
        body.len(),
        body
    )
    .into_bytes()
}

#[derive(Clone, Debug)]
pub struct ClientRequest {
    pub raw: Vec<u8>,
    pub header_end: usize,
    pub host: Option<String>,
    pub path: String,
    pub is_idempotent: bool,
    pub keep_alive: bool,
}

pub fn request_route(header_str: &str, first_line: &str) -> (Option<String>, String) {
    let target = first_line.split_whitespace().nth(1).unwrap_or("/");
    let (authority, path) = split_request_target(target);
    let host = header_str
        .lines()
        .skip(1)
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.trim()
                .eq_ignore_ascii_case("host")
                .then(|| value.trim().to_string())
        })
        .filter(|value| !value.is_empty())
        .or(authority);

    (host, path)
}

fn split_request_target(target: &str) -> (Option<String>, String) {
    let (authority, path_and_query) = if let Some((_, remainder)) = target.split_once("://") {
        let boundary = remainder.find(['/', '?']).unwrap_or(remainder.len());
        let authority = remainder[..boundary].to_string();
        let path = remainder.get(boundary..).unwrap_or("/");
        (Some(authority), path)
    } else {
        (None, target)
    };
    let path = path_and_query.split(['?', '#']).next().unwrap_or("/");
    let path = if path.starts_with('/') && !path.is_empty() {
        path.to_string()
    } else {
        "/".to_string()
    };

    (authority.filter(|value| !value.is_empty()), path)
}

pub fn read_http_request<R: Read>(stream: &mut R) -> std::io::Result<Option<ClientRequest>> {
    let mut buffer = Vec::with_capacity(4096);
    let mut chunk = [0u8; 2048];
    let mut header_end = None;

    while header_end.is_none() {
        let n = stream.read(&mut chunk)?;
        if n == 0 {
            if buffer.is_empty() {
                return Ok(None);
            }
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Connection closed while reading request headers",
            ));
        }
        buffer.extend_from_slice(&chunk[..n]);
        if let Some(end) = find_header_end(&buffer) {
            header_end = Some(end);
        } else if buffer.len() > MAX_HEADER_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Request headers exceeded maximum allowed size",
            ));
        }
    }

    let header_end_idx = header_end.unwrap();
    let header_str = String::from_utf8_lossy(&buffer[..header_end_idx]);

    let first_line = header_str.lines().next().unwrap_or("");
    let method = first_line.split_whitespace().next().unwrap_or("GET");
    let is_idempotent = matches!(method, "GET" | "HEAD" | "OPTIONS" | "PUT" | "DELETE");
    let (host, path) = request_route(&header_str, first_line);

    let is_http_10 = first_line.contains("HTTP/1.0");
    let has_close = header_str.lines().any(|l| {
        l.split_once(':').map_or(false, |(k, v)| {
            k.trim().eq_ignore_ascii_case("connection") && v.to_ascii_lowercase().contains("close")
        })
    });
    let has_keep_alive = header_str.lines().any(|l| {
        l.split_once(':').map_or(false, |(k, v)| {
            k.trim().eq_ignore_ascii_case("connection")
                && v.to_ascii_lowercase().contains("keep-alive")
        })
    });
    let keep_alive = if is_http_10 {
        has_keep_alive
    } else {
        !has_close
    };

    let is_chunked = header_str.lines().any(|l| {
        l.split_once(':').map_or(false, |(k, v)| {
            k.trim().eq_ignore_ascii_case("transfer-encoding")
                && v.to_ascii_lowercase().contains("chunked")
        })
    });

    let content_length = header_str.lines().find_map(|l| {
        let (k, v) = l.split_once(':')?;
        if k.trim().eq_ignore_ascii_case("content-length") {
            v.trim().parse::<usize>().ok()
        } else {
            None
        }
    });

    if is_chunked {
        let mut cursor = header_end_idx;
        loop {
            let crlf_pos = loop {
                if let Some(pos) = buffer[cursor..].windows(2).position(|w| w == b"\r\n") {
                    break cursor + pos;
                }
                let n = stream.read(&mut chunk)?;
                if n == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "Unexpected EOF reading request chunk size",
                    ));
                }
                buffer.extend_from_slice(&chunk[..n]);
                if buffer.len() - header_end_idx > MAX_BODY_SIZE {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Request body exceeded 2 MiB limit",
                    ));
                }
            };

            let line = String::from_utf8_lossy(&buffer[cursor..crlf_pos]);
            let hex_part = line.split(';').next().unwrap_or("").trim();
            let chunk_size = usize::from_str_radix(hex_part, 16).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Invalid request chunk size: {e}"),
                )
            })?;

            if chunk_size == 0 {
                let trailer_start = crlf_pos + 2;
                loop {
                    if buffer.len() >= trailer_start + 2
                        && &buffer[trailer_start..trailer_start + 2] == b"\r\n"
                    {
                        buffer.truncate(trailer_start + 2);
                        break;
                    }
                    if let Some(pos) = buffer[trailer_start..]
                        .windows(4)
                        .position(|w| w == b"\r\n\r\n")
                    {
                        buffer.truncate(trailer_start + pos + 4);
                        break;
                    }
                    let n = stream.read(&mut chunk)?;
                    if n == 0 {
                        break;
                    }
                    buffer.extend_from_slice(&chunk[..n]);
                }
                break;
            }

            let chunk_data_end = crlf_pos + 2 + chunk_size + 2;
            while buffer.len() < chunk_data_end {
                let n = stream.read(&mut chunk)?;
                if n == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "Unexpected EOF reading request chunk data",
                    ));
                }
                buffer.extend_from_slice(&chunk[..n]);
                if buffer.len() - header_end_idx > MAX_BODY_SIZE {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Request body exceeded 2 MiB limit",
                    ));
                }
            }
            cursor = chunk_data_end;
        }
    } else if let Some(cl) = content_length {
        if cl > MAX_BODY_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Request body size ({cl} bytes) exceeds 2 MiB limit"),
            ));
        }
        let total_needed = header_end_idx + cl;
        while buffer.len() < total_needed {
            let n = stream.read(&mut chunk)?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Unexpected EOF reading request body",
                ));
            }
            buffer.extend_from_slice(&chunk[..n]);
        }
        buffer.truncate(total_needed);
    }

    Ok(Some(ClientRequest {
        raw: buffer,
        header_end: header_end_idx,
        host,
        path,
        is_idempotent,
        keep_alive,
    }))
}

pub fn forward_response_stream<R: Read, W: Write>(
    upstream: &mut R,
    client: &mut W,
) -> std::io::Result<(usize, u16)> {
    let mut buffer = Vec::with_capacity(4096);
    let mut chunk_buf = [0u8; 8192];
    let mut header_end = None;

    while header_end.is_none() {
        let n = upstream.read(&mut chunk_buf)?;
        if n == 0 {
            if buffer.is_empty() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Empty response from upstream",
                ));
            }
            client.write_all(&buffer)?;
            client.flush()?;
            return Ok((buffer.len(), 200));
        }
        buffer.extend_from_slice(&chunk_buf[..n]);
        if let Some(pos) = find_header_end(&buffer) {
            header_end = Some(pos);
        }
    }

    let header_end_idx = header_end.unwrap();
    let header_str = String::from_utf8_lossy(&buffer[..header_end_idx]);

    let status_code = header_str
        .lines()
        .next()
        .and_then(|line| {
            let mut parts = line.split_whitespace();
            parts.next()?;
            parts.next()?.parse::<u16>().ok()
        })
        .unwrap_or(200);

    if (100..200).contains(&status_code) || status_code == 204 || status_code == 304 {
        client.write_all(&buffer[..header_end_idx])?;
        client.flush()?;
        return Ok((header_end_idx, status_code));
    }

    let is_chunked = header_str.lines().any(|line| {
        line.split_once(':').map_or(false, |(k, v)| {
            k.trim().eq_ignore_ascii_case("transfer-encoding")
                && v.to_ascii_lowercase().contains("chunked")
        })
    });

    let content_length = header_str.lines().find_map(|line| {
        let (k, v) = line.split_once(':')?;
        if k.trim().eq_ignore_ascii_case("content-length") {
            v.trim().parse::<usize>().ok()
        } else {
            None
        }
    });

    client.write_all(&buffer)?;
    let mut total_written = buffer.len();
    let body_bytes_already_read = buffer.len() - header_end_idx;

    if is_chunked {
        let already_has_terminal = buffer[header_end_idx..]
            .windows(5)
            .any(|w| w == b"0\r\n\r\n");

        if !already_has_terminal {
            loop {
                let n = upstream.read(&mut chunk_buf)?;
                if n == 0 {
                    break;
                }
                client.write_all(&chunk_buf[..n])?;
                total_written += n;

                if chunk_buf[..n].windows(5).any(|w| w == b"0\r\n\r\n") {
                    break;
                }
            }
        }
    } else if let Some(cl) = content_length {
        let mut remaining = cl.saturating_sub(body_bytes_already_read);
        while remaining > 0 {
            let to_read = remaining.min(chunk_buf.len());
            let n = upstream.read(&mut chunk_buf[..to_read])?;
            if n == 0 {
                break;
            }
            client.write_all(&chunk_buf[..n])?;
            total_written += n;
            remaining -= n;
        }
    } else {
        loop {
            let n = upstream.read(&mut chunk_buf)?;
            if n == 0 {
                break;
            }
            client.write_all(&chunk_buf[..n])?;
            total_written += n;
        }
    }

    client.flush()?;
    Ok((total_written, status_code))
}

pub fn proxy_connections(mut client: TcpStream, router: &ServiceRouter) -> Option<ProxyResult> {
    println!("Proxy connection received");
    let _ = client.set_read_timeout(Some(READ_TIMEOUT));
    let _ = client.set_write_timeout(Some(WRITE_TIMEOUT));

    let mut last_result = None;

    loop {
        let req = match read_http_request(&mut client) {
            Ok(Some(req)) => req,
            Ok(None) => break,
            Err(e) => {
                if e.kind() != std::io::ErrorKind::TimedOut
                    && e.kind() != std::io::ErrorKind::WouldBlock
                {
                    eprintln!("Error reading client request: {e}");
                }
                break;
            }
        };

        println!(
            "Received {} bytes from client (idempotent={}, keep_alive={})",
            req.raw.len(),
            req.is_idempotent,
            req.keep_alive
        );

        let service = router.resolve(req.host.as_deref(), &req.path);
        let service_id = service.id.clone();
        let backend_pool = match &service.target {
            ServiceTarget::Proxy(pool) => Arc::clone(pool),
            ServiceTarget::Static { root } => {
                let start = Instant::now();
                let (response, served) = match StaticFileHandler::new(root.clone()).serve(&req.path)
                {
                    Ok(response) => (response.to_http(), true),
                    Err(error) => {
                        eprintln!("Static service '{service_id}' failed: {error}");
                        (internal_server_error_response(), false)
                    }
                };
                let success =
                    served && client.write_all(&response).is_ok() && client.flush().is_ok();
                return Some(ProxyResult {
                    service_id,
                    backend_id: "static".into(),
                    latency: start.elapsed(),
                    success,
                    bytes_sent: 0,
                    bytes_received: response.len(),
                });
            }
        };

        println!(
            "Routing host={:?} path={} to service={}",
            req.host, req.path, service_id
        );

        let upstream_req = prepare_upstream_request(&req.raw);
        let mut attempt = 0;
        let mut request_succeeded = false;

        while attempt <= MAX_RETRIES {
            let start = Instant::now();
            let backend = match backend_pool.select_backend() {
                Ok(backend) => backend,
                Err(error) => {
                    eprintln!("Unable to select a backend: {error}");
                    let response = service_unavailable_response();
                    let _ = client.write_all(&response);
                    let _ = client.flush();
                    return Some(ProxyResult {
                        service_id,
                        backend_id: "none".into(),
                        latency: start.elapsed(),
                        success: false,
                        bytes_sent: 0,
                        bytes_received: response.len(),
                    });
                }
            };
            let guard = ActiveConnectionGuard::new(Arc::clone(&backend.metrics));

            println!(
                "Attempt {}: Selected backend {} at {} (active connections: {})",
                attempt + 1,
                backend.backend.id,
                backend.backend.address,
                backend
                    .metrics
                    .active_connections
                    .load(std::sync::atomic::Ordering::Relaxed)
            );

            let upstream_addr = match backend.backend.address.parse::<std::net::SocketAddr>() {
                Ok(addr) => addr,
                Err(_) => {
                    let feedback = Feedback {
                        latency: start.elapsed(),
                        success: false,
                    };
                    backend.release(feedback.clone());
                    guard.complete(&feedback, 0, 0);
                    attempt += 1;
                    continue;
                }
            };

            let mut upstream = match TcpStream::connect_timeout(&upstream_addr, CONNECT_TIMEOUT) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!(
                        "Failed to connect to backend {}: {e}",
                        backend.backend.address
                    );
                    let feedback = Feedback {
                        latency: start.elapsed(),
                        success: false,
                    };
                    backend.release(feedback.clone());
                    guard.complete(&feedback, 0, 0);
                    attempt += 1;
                    continue;
                }
            };

            let _ = upstream.set_read_timeout(Some(READ_TIMEOUT));
            let _ = upstream.set_write_timeout(Some(WRITE_TIMEOUT));

            if let Err(e) = upstream.write_all(&upstream_req) {
                eprintln!(
                    "Failed to write to backend {}: {e}",
                    backend.backend.address
                );
                let feedback = Feedback {
                    latency: start.elapsed(),
                    success: false,
                };
                backend.release(feedback.clone());
                guard.complete(&feedback, upstream_req.len(), 0);

                if req.is_idempotent {
                    attempt += 1;
                    continue;
                } else {
                    break;
                }
            }

            match forward_response_stream(&mut upstream, &mut client) {
                Ok((bytes_sent_to_client, _status)) => {
                    let latency = start.elapsed();
                    let feedback = Feedback {
                        latency,
                        success: true,
                    };
                    backend.release(feedback.clone());
                    guard.complete(&feedback, upstream_req.len(), bytes_sent_to_client);

                    last_result = Some(ProxyResult {
                        service_id: service_id.clone(),
                        backend_id: backend.backend.id.clone(),
                        latency,
                        success: true,
                        bytes_sent: upstream_req.len(),
                        bytes_received: bytes_sent_to_client,
                    });
                    request_succeeded = true;
                    break;
                }
                Err(e) => {
                    eprintln!(
                        "Failed forwarding response from backend {}: {e}",
                        backend.backend.address
                    );
                    let feedback = Feedback {
                        latency: start.elapsed(),
                        success: false,
                    };
                    backend.release(feedback.clone());
                    guard.complete(&feedback, upstream_req.len(), 0);

                    if req.is_idempotent {
                        attempt += 1;
                        continue;
                    } else {
                        break;
                    }
                }
            }
        }

        if !request_succeeded {
            let error_body = "502 Bad Gateway: All backend attempts failed\n";
            let error_resp = format!(
                "HTTP/1.1 502 Bad Gateway\r\nContent-Length: {}\r\nConnection: close\r\nContent-Type: text/plain\r\n\r\n{}",
                error_body.len(),
                error_body
            );
            let _ = client.write_all(error_resp.as_bytes());
            let _ = client.flush();

            last_result = Some(ProxyResult {
                service_id,
                backend_id: "none".into(),
                latency: Duration::ZERO,
                success: false,
                bytes_sent: upstream_req.len(),
                bytes_received: error_resp.len(),
            });
            break;
        }

        if !req.keep_alive {
            break;
        }
    }

    last_result
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
                if let Some(pos) = buffer[cursor..].windows(2).position(|w| w == b"\r\n") {
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
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Invalid chunk size: {e}"),
                )
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
    pub service_id: String,
    pub backend_id: String,
    pub latency: Duration,
    pub success: bool,
    pub bytes_sent: usize,
    pub bytes_received: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Backend,
        algorithms::AlgorithmKind,
        backend::BackendPool,
        service::{RouteMatcher, Service, ServiceRegistry},
    };
    use std::{
        io::{Cursor, Read, Write},
        net::{TcpListener, TcpStream},
        path::PathBuf,
        thread,
    };

    fn single_service_router(backend_pool: BackendPool) -> ServiceRouter {
        let registry = Arc::new(ServiceRegistry::new());
        registry
            .add(
                Service::proxy(
                    "default",
                    vec![RouteMatcher::new(None::<String>, "/").unwrap()],
                    Arc::new(backend_pool),
                )
                .unwrap(),
            )
            .unwrap();
        ServiceRouter::new(registry, "default").unwrap()
    }

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
        assert_eq!(
            resp,
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n"
        );
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
        let raw =
            b"GET /company HTTP/1.1\r\nHost: localhost:7879\r\nConnection: keep-alive\r\n\r\n";
        let modified = prepare_upstream_request(raw);
        let s = String::from_utf8_lossy(&modified);
        assert!(s.contains("Connection: close\r\n"));
        assert!(!s.contains("keep-alive"));
    }

    #[test]
    fn test_active_connection_guard_complete() {
        let metrics = Arc::new(crate::backend::backend_server::BackendMetrics::new());
        assert_eq!(
            metrics
                .active_connections
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );

        let guard = ActiveConnectionGuard::new(Arc::clone(&metrics));
        assert_eq!(
            metrics
                .active_connections
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        assert_eq!(
            metrics
                .total_requests
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );

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
            assert_eq!(
                metrics
                    .active_connections
                    .load(std::sync::atomic::Ordering::Relaxed),
                1
            );
            // Dropped here without calling complete
        }
        let snap = metrics.snapshot();
        assert_eq!(snap.active_connections, 0);
        assert_eq!(snap.total_requests, 1);
        assert_eq!(snap.successful_requests, 0);
        assert_eq!(snap.failed_requests, 1);
    }

    #[test]
    fn test_read_http_request_large_body() {
        // 8 KiB payload (well beyond 1 KiB limit)
        let body = "A".repeat(8192);
        let raw = format!(
            "POST /upload HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        let mut cursor = Cursor::new(raw.as_bytes());
        let req = read_http_request(&mut cursor).unwrap().unwrap();

        assert_eq!(req.raw.len(), raw.len());
        assert!(!req.is_idempotent); // POST
        assert!(req.keep_alive); // HTTP/1.1 default
        assert_eq!(&req.raw[req.header_end..], body.as_bytes());
    }

    #[test]
    fn test_read_http_request_chunked() {
        let raw = b"POST /stream HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n4\r\nWiki\r\n6\r\npedia \r\n0\r\n\r\n";
        let mut cursor = Cursor::new(raw);
        let req = read_http_request(&mut cursor).unwrap().unwrap();

        assert_eq!(req.raw, raw);
        assert!(!req.is_idempotent);
    }

    #[test]
    fn request_route_extracts_host_and_path() {
        let headers = "GET http://origin.example/live/lap?session=1 HTTP/1.1\r\nHost: TELEMETRY.EXAMPLE:7879\r\n\r\n";
        let (host, path) = request_route(headers, headers.lines().next().unwrap());
        assert_eq!(host.as_deref(), Some("TELEMETRY.EXAMPLE:7879"));
        assert_eq!(path, "/live/lap");

        let absolute = "GET http://origin.example:8080/status?full=true HTTP/1.1";
        let (host, path) = request_route(absolute, absolute);
        assert_eq!(host.as_deref(), Some("origin.example:8080"));
        assert_eq!(path, "/status");
    }

    #[test]
    fn test_forward_response_stream_streaming() {
        let raw_resp = b"HTTP/1.1 200 OK\r\nContent-Length: 12\r\n\r\nHello World!";
        let mut upstream = Cursor::new(raw_resp);
        let mut client_sink = Vec::new();

        let (bytes_sent, status) =
            forward_response_stream(&mut upstream, &mut client_sink).unwrap();
        assert_eq!(status, 200);
        assert_eq!(bytes_sent, raw_resp.len());
        assert_eq!(client_sink, raw_resp);
    }

    #[test]
    fn empty_pool_returns_service_unavailable() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let client = thread::spawn(move || {
            let mut stream = TcpStream::connect(address).unwrap();
            stream
                .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .unwrap();
            let mut response = String::new();
            stream.read_to_string(&mut response).unwrap();
            response
        });

        let (stream, _) = listener.accept().unwrap();
        let backend_pool = BackendPool::new(AlgorithmKind::RoundRobin, Vec::new()).unwrap();
        let router = single_service_router(backend_pool);
        let result = proxy_connections(stream, &router).unwrap();
        let response = client.join().unwrap();

        assert!(response.starts_with("HTTP/1.1 503 Service Unavailable"));
        assert_eq!(result.service_id, "default");
        assert_eq!(result.backend_id, "none");
        assert!(!result.success);
        assert_eq!(result.bytes_sent, 0);
    }

    #[test]
    fn routes_request_to_matching_service() {
        let upstream_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let upstream_address = upstream_listener.local_addr().unwrap();
        let upstream = thread::spawn(move || {
            let (mut stream, _) = upstream_listener.accept().unwrap();
            let mut request = [0; 2048];
            let bytes = stream.read(&mut request).unwrap();
            assert!(
                String::from_utf8_lossy(&request[..bytes]).starts_with("GET /live/lap?session=1")
            );
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\ntelemetry")
                .unwrap();
        });

        let registry = Arc::new(ServiceRegistry::new());
        for (service_id, backend_id, route) in [
            ("default", "default-backend", "/fallback"),
            ("telemetry", "telemetry-backend", "/live"),
        ] {
            let pool = BackendPool::new(
                AlgorithmKind::RoundRobin,
                vec![Backend {
                    id: backend_id.into(),
                    address: upstream_address.to_string(),
                    weight: 1,
                }],
            )
            .unwrap();
            let host = (service_id == "telemetry").then_some("telemetry.example");
            registry
                .add(
                    Service::proxy(
                        service_id,
                        vec![RouteMatcher::new(host, route).unwrap()],
                        Arc::new(pool),
                    )
                    .unwrap(),
                )
                .unwrap();
        }
        let router = ServiceRouter::new(registry, "default").unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let client = thread::spawn(move || {
            let mut stream = TcpStream::connect(address).unwrap();
            stream
                .write_all(
                    b"GET /live/lap?session=1 HTTP/1.1\r\nHost: telemetry.example:7879\r\nConnection: close\r\n\r\n",
                )
                .unwrap();
            let mut response = String::new();
            stream.read_to_string(&mut response).unwrap();
            response
        });

        let (stream, _) = listener.accept().unwrap();
        let result = proxy_connections(stream, &router).unwrap();
        let response = client.join().unwrap();
        upstream.join().unwrap();

        assert!(response.ends_with("telemetry"));
        assert_eq!(result.service_id, "telemetry");
        assert_eq!(result.backend_id, "telemetry-backend");
        assert!(result.success);
    }

    #[test]
    fn serves_matching_static_service() {
        let registry = Arc::new(ServiceRegistry::new());
        registry
            .add(
                Service::proxy(
                    "default",
                    vec![RouteMatcher::new(None::<String>, "/fallback").unwrap()],
                    Arc::new(BackendPool::new(AlgorithmKind::RoundRobin, Vec::new()).unwrap()),
                )
                .unwrap(),
            )
            .unwrap();
        registry
            .add(
                Service::static_files(
                    "dashboard",
                    vec![RouteMatcher::new(Some("dashboard.example"), "/").unwrap()],
                    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("web"),
                )
                .unwrap(),
            )
            .unwrap();
        let router = ServiceRouter::new(registry, "default").unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let client = thread::spawn(move || {
            let mut stream = TcpStream::connect(address).unwrap();
            stream
                .write_all(
                    b"GET / HTTP/1.1\r\nHost: dashboard.example\r\nConnection: close\r\n\r\n",
                )
                .unwrap();
            let mut response = String::new();
            stream.read_to_string(&mut response).unwrap();
            response
        });

        let (stream, _) = listener.accept().unwrap();
        let result = proxy_connections(stream, &router).unwrap();
        let response = client.join().unwrap();

        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert_eq!(result.service_id, "dashboard");
        assert_eq!(result.backend_id, "static");
        assert!(result.success);
    }
}
