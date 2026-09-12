use std::{
    io,
    sync::Arc,
    time::Instant,
};

use arc_swap::ArcSwap;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream as TokioTcpStream,
};

use crate::{
    algorithms::algorithms::LoadBalancer,
    backend::backend_server::Feedback,
    proxy::connection::{ActiveConnectionGuard, ProxyResult, find_header_end, prepare_upstream_request},
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

/// Async equivalent of `proxy_connections`.
///
/// Uses `tokio::net::TcpStream` for fully non-blocking I/O while
/// preserving the same metric recording and load-balancer feedback.
pub async fn proxy_connections_async(
    mut client: TokioTcpStream,
    lb_slot: &Arc<ArcSwap<Box<dyn LoadBalancer>>>,
) -> Option<ProxyResult> {
    println!("Proxy connection received (async)");
    let mut buf = [0u8; 1024];

    let n = match client.read(&mut buf).await {
        Ok(0) => return None,
        Ok(n) => n,
        Err(e) if e.kind() == io::ErrorKind::ConnectionReset => {
            println!("Client disconnected abruptly.");
            return None;
        }
        Err(e) => {
            eprintln!("Unexpected network error: {}", e);
            return None;
        }
    };
    println!("Received {} bytes from client (async)", n);

    let lb = lb_slot.load();

    let start = Instant::now();
    let backend = lb.next();
    let guard = ActiveConnectionGuard::new(Arc::clone(&backend.metrics));

    println!(
        "Selected backend {} at {} (active connections: {}) [async]",
        backend.backend.id,
        backend.backend.address,
        backend
            .metrics
            .active_connections
            .load(std::sync::atomic::Ordering::Relaxed)
    );

    let mut upstream = match TokioTcpStream::connect(&backend.backend.address).await {
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

    if let Err(e) = upstream.write_all(&upstream_req).await {
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

    let resp = match read_http_response_async(&mut upstream).await {
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

    if let Err(e) = client.write_all(&resp).await {
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

    Some(ProxyResult {
        backend_id: backend.backend.id,
        latency,
        success: true,
        bytes_sent: upstream_req.len(),
        bytes_received: resp.len(),
    })
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
}
