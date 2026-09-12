use std::{
    io,
    sync::Arc,
    time::Instant,
};

use arc_swap::ArcSwap;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream as TokioTcpStream,
    time::timeout,
};

use crate::{
    algorithms::algorithms::LoadBalancer,
    backend::backend_server::Feedback,
    proxy::connection::{
        ActiveConnectionGuard, ClientRequest, ProxyResult, CONNECT_TIMEOUT, MAX_BODY_SIZE,
        MAX_HEADER_SIZE, MAX_RETRIES, READ_TIMEOUT, WRITE_TIMEOUT, find_header_end,
        prepare_upstream_request,
    },
};

/// Controls how new proxy connections are dispatched.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeMode {
    /// Dispatch each connection to a blocking thread (default).
    ThreadPool,
    /// Dispatch each connection as a Tokio async task.
    Async,
}

impl RuntimeMode {
    pub fn from_str_name(s: &str) -> Option<Self> {
        match s {
            "thread_pool" => Some(Self::ThreadPool),
            "async" => Some(Self::Async),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ThreadPool => "thread_pool",
            Self::Async => "async",
        }
    }
}

/// Read the full HTTP response from `stream` asynchronously.
///
/// Mirrors the logic of `connection::read_http_response` but uses
/// `tokio::io::AsyncReadExt` instead of blocking `std::io::Read`.
pub async fn read_http_response_async<R: AsyncReadExt + Unpin>(stream: &mut R) -> io::Result<Vec<u8>> {
    let mut buffer = Vec::with_capacity(8192);
    let mut chunk_buf = [0u8; 4096];
    let mut header_end = None;

    // --- read until we have the full header block ---
    while header_end.is_none() {
        let n = stream.read(&mut chunk_buf).await?;
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
                let n = stream.read(&mut chunk_buf).await?;
                if n == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "Unexpected EOF while reading chunk size",
                    ));
                }
                buffer.extend_from_slice(&chunk_buf[..n]);
            };

            let line = String::from_utf8_lossy(&buffer[cursor..crlf_pos]);
            let hex_part = line.split(';').next().unwrap_or("").trim();
            let chunk_size = usize::from_str_radix(hex_part, 16).map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidData, format!("Invalid chunk size: {e}"))
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
                    let n = stream.read(&mut chunk_buf).await?;
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
                let n = stream.read(&mut chunk_buf).await?;
                if n == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
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
            let n = stream.read(&mut chunk_buf).await?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Unexpected EOF while reading Content-Length body",
                ));
            }
            buffer.extend_from_slice(&chunk_buf[..n]);
        }
        buffer.truncate(total_needed);
        Ok(buffer)
    } else {
        stream.read_to_end(&mut buffer).await?;
        Ok(buffer)
    }
}

pub async fn read_http_request_async<R: AsyncReadExt + Unpin>(
    stream: &mut R,
) -> io::Result<Option<ClientRequest>> {
    let mut buffer = Vec::with_capacity(4096);
    let mut chunk = [0u8; 2048];
    let mut header_end = None;

    while header_end.is_none() {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            if buffer.is_empty() {
                return Ok(None);
            }
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Connection closed while reading request headers",
            ));
        }
        buffer.extend_from_slice(&chunk[..n]);
        if let Some(end) = find_header_end(&buffer) {
            header_end = Some(end);
        } else if buffer.len() > MAX_HEADER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Request headers exceeded maximum allowed size",
            ));
        }
    }

    let header_end_idx = header_end.unwrap();
    let header_str = String::from_utf8_lossy(&buffer[..header_end_idx]);

    let first_line = header_str.lines().next().unwrap_or("");
    let method = first_line.split_whitespace().next().unwrap_or("GET");
    let is_idempotent = matches!(method, "GET" | "HEAD" | "OPTIONS" | "PUT" | "DELETE");

    let is_http_10 = first_line.contains("HTTP/1.0");
    let has_close = header_str.lines().any(|l| {
        l.split_once(':').map_or(false, |(k, v)| {
            k.trim().eq_ignore_ascii_case("connection") && v.to_ascii_lowercase().contains("close")
        })
    });
    let has_keep_alive = header_str.lines().any(|l| {
        l.split_once(':').map_or(false, |(k, v)| {
            k.trim().eq_ignore_ascii_case("connection") && v.to_ascii_lowercase().contains("keep-alive")
        })
    });
    let keep_alive = if is_http_10 {
        has_keep_alive
    } else {
        !has_close
    };

    let is_chunked = header_str.lines().any(|l| {
        l.split_once(':').map_or(false, |(k, v)| {
            k.trim().eq_ignore_ascii_case("transfer-encoding") && v.to_ascii_lowercase().contains("chunked")
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
                let n = stream.read(&mut chunk).await?;
                if n == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "Unexpected EOF reading request chunk size",
                    ));
                }
                buffer.extend_from_slice(&chunk[..n]);
                if buffer.len() - header_end_idx > MAX_BODY_SIZE {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Request body exceeded 2 MiB limit",
                    ));
                }
            };

            let line = String::from_utf8_lossy(&buffer[cursor..crlf_pos]);
            let hex_part = line.split(';').next().unwrap_or("").trim();
            let chunk_size = usize::from_str_radix(hex_part, 16).map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidData, format!("Invalid request chunk size: {e}"))
            })?;

            if chunk_size == 0 {
                let trailer_start = crlf_pos + 2;
                loop {
                    if buffer.len() >= trailer_start + 2 && &buffer[trailer_start..trailer_start + 2] == b"\r\n" {
                        buffer.truncate(trailer_start + 2);
                        break;
                    }
                    if let Some(pos) = buffer[trailer_start..].windows(4).position(|w| w == b"\r\n\r\n") {
                        buffer.truncate(trailer_start + pos + 4);
                        break;
                    }
                    let n = stream.read(&mut chunk).await?;
                    if n == 0 {
                        break;
                    }
                    buffer.extend_from_slice(&chunk[..n]);
                }
                break;
            }

            let chunk_data_end = crlf_pos + 2 + chunk_size + 2;
            while buffer.len() < chunk_data_end {
                let n = stream.read(&mut chunk).await?;
                if n == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "Unexpected EOF reading request chunk data",
                    ));
                }
                buffer.extend_from_slice(&chunk[..n]);
                if buffer.len() - header_end_idx > MAX_BODY_SIZE {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Request body exceeded 2 MiB limit",
                    ));
                }
            }
            cursor = chunk_data_end;
        }
    } else if let Some(cl) = content_length {
        if cl > MAX_BODY_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Request body size ({cl} bytes) exceeds 2 MiB limit"),
            ));
        }
        let total_needed = header_end_idx + cl;
        while buffer.len() < total_needed {
            let n = stream.read(&mut chunk).await?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
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
        is_idempotent,
        keep_alive,
    }))
}

pub async fn forward_response_stream_async<R: AsyncReadExt + Unpin, W: AsyncWriteExt + Unpin>(
    upstream: &mut R,
    client: &mut W,
) -> io::Result<(usize, u16)> {
    let mut buffer = Vec::with_capacity(4096);
    let mut chunk_buf = [0u8; 8192];
    let mut header_end = None;

    while header_end.is_none() {
        let n = upstream.read(&mut chunk_buf).await?;
        if n == 0 {
            if buffer.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Empty response from upstream",
                ));
            }
            client.write_all(&buffer).await?;
            client.flush().await?;
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
        client.write_all(&buffer[..header_end_idx]).await?;
        client.flush().await?;
        return Ok((header_end_idx, status_code));
    }

    let is_chunked = header_str.lines().any(|line| {
        line.split_once(':').map_or(false, |(k, v)| {
            k.trim().eq_ignore_ascii_case("transfer-encoding") && v.to_ascii_lowercase().contains("chunked")
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

    client.write_all(&buffer).await?;
    let mut total_written = buffer.len();
    let body_bytes_already_read = buffer.len() - header_end_idx;

    if is_chunked {
        let already_has_terminal = buffer[header_end_idx..]
            .windows(5)
            .any(|w| w == b"0\r\n\r\n");

        if !already_has_terminal {
            loop {
                let n = upstream.read(&mut chunk_buf).await?;
                if n == 0 {
                    break;
                }
                client.write_all(&chunk_buf[..n]).await?;
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
            let n = upstream.read(&mut chunk_buf[..to_read]).await?;
            if n == 0 {
                break;
            }
            client.write_all(&chunk_buf[..n]).await?;
            total_written += n;
            remaining -= n;
        }
    } else {
        loop {
            let n = upstream.read(&mut chunk_buf).await?;
            if n == 0 {
                break;
            }
            client.write_all(&chunk_buf[..n]).await?;
            total_written += n;
        }
    }

    client.flush().await?;
    Ok((total_written, status_code))
}

pub async fn proxy_connections_async(
    mut client: TokioTcpStream,
    lb_slot: &Arc<ArcSwap<Box<dyn LoadBalancer>>>,
) -> Option<ProxyResult> {
    println!("Proxy connection received (async)");
    let mut last_result = None;

    loop {
        let req = match timeout(READ_TIMEOUT, read_http_request_async(&mut client)).await {
            Ok(Ok(Some(req))) => req,
            Ok(Ok(None)) => break,
            Ok(Err(e)) => {
                if e.kind() != io::ErrorKind::TimedOut && e.kind() != io::ErrorKind::WouldBlock {
                    eprintln!("Error reading client request (async): {e}");
                }
                break;
            }
            Err(_) => {
                break;
            }
        };

        println!(
            "Received {} bytes from client [async] (idempotent={}, keep_alive={})",
            req.raw.len(),
            req.is_idempotent,
            req.keep_alive
        );

        let upstream_req = prepare_upstream_request(&req.raw);
        let mut attempt = 0;
        let mut request_succeeded = false;

        while attempt <= MAX_RETRIES {
            let lb = lb_slot.load();
            let start = Instant::now();
            let backend = lb.next();
            let guard = ActiveConnectionGuard::new(Arc::clone(&backend.metrics));

            println!(
                "Attempt {} [async]: Selected backend {} at {} (active connections: {})",
                attempt + 1,
                backend.backend.id,
                backend.backend.address,
                backend
                    .metrics
                    .active_connections
                    .load(std::sync::atomic::Ordering::Relaxed)
            );

            let mut upstream = match timeout(CONNECT_TIMEOUT, TokioTcpStream::connect(&backend.backend.address)).await {
                Ok(Ok(s)) => s,
                Ok(Err(e)) => {
                    eprintln!("Failed to connect to backend {} [async]: {e}", backend.backend.address);
                    let feedback = Feedback {
                        latency: start.elapsed(),
                        success: false,
                    };
                    lb.release(&backend, feedback.clone());
                    guard.complete(&feedback, 0, 0);
                    attempt += 1;
                    continue;
                }
                Err(_) => {
                    eprintln!("Connect to backend {} timed out [async]", backend.backend.address);
                    let feedback = Feedback {
                        latency: start.elapsed(),
                        success: false,
                    };
                    lb.release(&backend, feedback.clone());
                    guard.complete(&feedback, 0, 0);
                    attempt += 1;
                    continue;
                }
            };

            let write_res = timeout(WRITE_TIMEOUT, upstream.write_all(&upstream_req)).await;
            if write_res.is_err() || write_res.unwrap().is_err() {
                eprintln!("Failed writing to backend {} [async]", backend.backend.address);
                let feedback = Feedback {
                    latency: start.elapsed(),
                    success: false,
                };
                lb.release(&backend, feedback.clone());
                guard.complete(&feedback, upstream_req.len(), 0);

                if req.is_idempotent {
                    attempt += 1;
                    continue;
                } else {
                    break;
                }
            }

            match timeout(READ_TIMEOUT, forward_response_stream_async(&mut upstream, &mut client)).await {
                Ok(Ok((bytes_sent_to_client, _status))) => {
                    let latency = start.elapsed();
                    let feedback = Feedback {
                        latency,
                        success: true,
                    };
                    lb.release(&backend, feedback.clone());
                    guard.complete(&feedback, upstream_req.len(), bytes_sent_to_client);

                    last_result = Some(ProxyResult {
                        backend_id: backend.backend.id.clone(),
                        latency,
                        success: true,
                        bytes_sent: upstream_req.len(),
                        bytes_received: bytes_sent_to_client,
                    });
                    request_succeeded = true;
                    break;
                }
                _ => {
                    eprintln!(
                        "Failed forwarding response from backend {} [async]",
                        backend.backend.address
                    );
                    let feedback = Feedback {
                        latency: start.elapsed(),
                        success: false,
                    };
                    lb.release(&backend, feedback.clone());
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
            let _ = client.write_all(error_resp.as_bytes()).await;
            let _ = client.flush().await;

            last_result = Some(ProxyResult {
                backend_id: "none".into(),
                latency: std::time::Duration::ZERO,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_runtime_mode_parsing() {
        assert_eq!(RuntimeMode::from_str_name("thread_pool"), Some(RuntimeMode::ThreadPool));
        assert_eq!(RuntimeMode::from_str_name("async"), Some(RuntimeMode::Async));
        assert_eq!(RuntimeMode::from_str_name("unknown"), None);

        assert_eq!(RuntimeMode::ThreadPool.as_str(), "thread_pool");
        assert_eq!(RuntimeMode::Async.as_str(), "async");
    }

    #[tokio::test]
    async fn test_async_content_length() {
        let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello";
        let mut cursor = Cursor::new(raw.to_vec());
        let resp = read_http_response_async(&mut cursor).await.unwrap();
        assert_eq!(resp, raw);
    }

    #[tokio::test]
    async fn test_async_content_length_with_trailing_data() {
        let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhelloEXTRA_BYTES";
        let mut cursor = Cursor::new(raw.to_vec());
        let resp = read_http_response_async(&mut cursor).await.unwrap();
        assert_eq!(resp, b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello");
    }

    #[tokio::test]
    async fn test_async_chunked() {
        let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n";
        let mut cursor = Cursor::new(raw.to_vec());
        let resp = read_http_response_async(&mut cursor).await.unwrap();
        assert_eq!(resp, raw);
    }

    #[tokio::test]
    async fn test_async_chunked_with_trailing_data() {
        let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\nEXTRA_PIPELINE";
        let mut cursor = Cursor::new(raw.to_vec());
        let resp = read_http_response_async(&mut cursor).await.unwrap();
        assert_eq!(resp, b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n");
    }

    #[tokio::test]
    async fn test_async_status_204_no_body() {
        let raw = b"HTTP/1.1 204 No Content\r\nConnection: keep-alive\r\n\r\n";
        let mut cursor = Cursor::new(raw.to_vec());
        let resp = read_http_response_async(&mut cursor).await.unwrap();
        assert_eq!(resp, raw);
    }

    #[tokio::test]
    async fn test_read_http_request_async_large_body() {
        let body = vec![b'x'; 10000];
        let req_header = format!(
            "POST /submit HTTP/1.1\r\nHost: example.com\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
            body.len()
        );
        let mut full_req = req_header.into_bytes();
        full_req.extend_from_slice(&body);

        let mut cursor = Cursor::new(full_req.clone());
        let parsed = read_http_request_async(&mut cursor).await.unwrap().unwrap();
        assert_eq!(parsed.raw, full_req);
        assert!(!parsed.is_idempotent);
        assert!(parsed.keep_alive);
    }

    #[tokio::test]
    async fn test_read_http_request_async_chunked() {
        let raw = b"POST /upload HTTP/1.1\r\nHost: test.com\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n";
        let mut cursor = Cursor::new(raw.to_vec());
        let parsed = read_http_request_async(&mut cursor).await.unwrap().unwrap();
        assert_eq!(parsed.raw, raw);
        assert!(!parsed.is_idempotent);
        assert!(parsed.keep_alive);
    }

    #[tokio::test]
    async fn test_forward_response_stream_async_streaming() {
        let upstream_data = b"HTTP/1.1 200 OK\r\nContent-Length: 11\r\nContent-Type: text/plain\r\n\r\nhello world";
        let mut upstream = Cursor::new(upstream_data.to_vec());
        let mut client_buf = Vec::new();

        let (total_written, status) = forward_response_stream_async(&mut upstream, &mut client_buf).await.unwrap();
        assert_eq!(status, 200);
        assert_eq!(total_written, upstream_data.len());
        assert_eq!(client_buf, upstream_data);
    }
}

