//! Tokio transport adapter for the shared proxy behavior.

use std::io;

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};

use crate::{
    observability::Observability,
    proxy::{
        behavior::{
            AttemptFailure, CONNECT_TIMEOUT, NextAttempt, ProxyResult, READ_TIMEOUT, RequestPlan,
            WRITE_TIMEOUT, bad_gateway_response, load_static_response, plan_request,
            service_unavailable_response,
        },
        protocol::{
            ChunkedBodyDecoder, ClientRequest, DecodeResult, MAX_HEADER_SIZE, ResponseBody,
            decode_request, decode_response_head,
        },
    },
    service::ServiceRouter,
};

pub use crate::proxy::mode::RuntimeMode;

pub async fn read_http_request_async<R: AsyncRead + Unpin>(
    stream: &mut R,
) -> io::Result<Option<ClientRequest>> {
    let mut buffer = Vec::with_capacity(4096);
    let mut chunk = [0_u8; 2048];

    loop {
        match decode_request(&buffer)? {
            DecodeResult::Complete(request) => return Ok(Some(request)),
            DecodeResult::Incomplete => {}
        }

        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            if buffer.is_empty() {
                return Ok(None);
            }
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Connection closed before the request was complete",
            ));
        }
        buffer.extend_from_slice(&chunk[..read]);
    }
}

pub async fn forward_response_stream_async<R, W>(
    upstream: &mut R,
    client: &mut W,
) -> io::Result<(usize, u16)>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut initial = Vec::with_capacity(4096);
    let mut chunk = [0_u8; 8192];
    let head = loop {
        if let Some(head) = decode_response_head(&initial)? {
            break head;
        }
        let read = upstream.read(&mut chunk).await?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Upstream closed before sending response headers",
            ));
        }
        initial.extend_from_slice(&chunk[..read]);
        if initial.len() > MAX_HEADER_SIZE && decode_response_head(&initial)?.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Response headers exceeded maximum allowed size",
            ));
        }
    };

    let mut total_written = 0;
    client.write_all(&initial[..head.header_end]).await?;
    total_written += head.header_end;
    let initial_body = &initial[head.header_end..];

    match head.body {
        ResponseBody::None => {}
        ResponseBody::ContentLength(length) => {
            let prefix = length.min(initial_body.len());
            client.write_all(&initial_body[..prefix]).await?;
            total_written += prefix;
            let mut remaining = length - prefix;
            while remaining > 0 {
                let capacity = remaining.min(chunk.len());
                let read = upstream.read(&mut chunk[..capacity]).await?;
                if read == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "Upstream closed before the Content-Length body was complete",
                    ));
                }
                client.write_all(&chunk[..read]).await?;
                total_written += read;
                remaining -= read;
            }
        }
        ResponseBody::Chunked => {
            let mut decoder = ChunkedBodyDecoder::new();
            let consumed = decoder.consume(initial_body)?;
            client.write_all(&initial_body[..consumed]).await?;
            total_written += consumed;

            while !decoder.is_complete() {
                let read = upstream.read(&mut chunk).await?;
                if read == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "Upstream closed before the chunked body was complete",
                    ));
                }
                let consumed = decoder.consume(&chunk[..read])?;
                client.write_all(&chunk[..consumed]).await?;
                total_written += consumed;
            }
        }
        ResponseBody::UntilClose => {
            client.write_all(initial_body).await?;
            total_written += initial_body.len();
            loop {
                let read = upstream.read(&mut chunk).await?;
                if read == 0 {
                    break;
                }
                client.write_all(&chunk[..read]).await?;
                total_written += read;
            }
        }
    }

    client.flush().await?;
    Ok((total_written, head.status_code))
}

pub async fn proxy_connections_async(
    mut client: TcpStream,
    router: &ServiceRouter,
    observability: &Observability,
) -> Option<ProxyResult> {
    let mut last_result = None;

    loop {
        let request = match timeout(READ_TIMEOUT, read_http_request_async(&mut client)).await {
            Ok(Ok(Some(request))) => request,
            Ok(Ok(None)) => break,
            Ok(Err(error)) => {
                if !matches!(
                    error.kind(),
                    io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
                ) {
                    eprintln!("Error reading client request [async]: {error}");
                }
                break;
            }
            Err(_) => break,
        };
        let keep_alive = request.keep_alive;

        match plan_request(RuntimeMode::Async, &request, router, observability) {
            RequestPlan::Static(exchange) => {
                let root = exchange.root().to_path_buf();
                let path = exchange.path().to_string();
                let response =
                    match tokio::task::spawn_blocking(move || load_static_response(&root, &path))
                        .await
                    {
                        Ok(response) => response,
                        Err(error) => {
                            eprintln!("Static service task failed: {error}");
                            crate::proxy::behavior::StaticResponse {
                                bytes: crate::proxy::behavior::internal_server_error_response(),
                                status_code: 500,
                                served: false,
                            }
                        }
                    };
                let written = matches!(
                    timeout(WRITE_TIMEOUT, async {
                        client.write_all(&response.bytes).await?;
                        client.flush().await
                    })
                    .await,
                    Ok(Ok(()))
                );
                return Some(exchange.complete(
                    response.bytes.len(),
                    response.status_code,
                    response.served,
                    written,
                ));
            }
            RequestPlan::Proxy(mut exchange) => {
                let result = loop {
                    let attempt = match exchange.next_attempt() {
                        NextAttempt::Ready(attempt) => attempt,
                        NextAttempt::NoEligibleBackend(error) => {
                            eprintln!("Unable to select a backend [async]: {error}");
                            let response = service_unavailable_response();
                            let _ = write_client(&mut client, &response).await;
                            break exchange.no_eligible_backend(response.len());
                        }
                        NextAttempt::Exhausted => {
                            let response = bad_gateway_response();
                            let _ = write_client(&mut client, &response).await;
                            break exchange.attempts_failed(response.len());
                        }
                    };

                    let mut upstream =
                        match timeout(CONNECT_TIMEOUT, TcpStream::connect(attempt.address())).await
                        {
                            Ok(Ok(upstream)) => upstream,
                            Ok(Err(error)) => {
                                eprintln!(
                                    "Failed to connect to backend {} [async]: {error}",
                                    attempt.address()
                                );
                                if exchange.attempt_failed(attempt, AttemptFailure::Connect, 0, 0) {
                                    continue;
                                }
                                let response = bad_gateway_response();
                                let _ = write_client(&mut client, &response).await;
                                break exchange.attempts_failed(response.len());
                            }
                            Err(_) => {
                                eprintln!(
                                    "Connect to backend {} timed out [async]",
                                    attempt.address()
                                );
                                if exchange.attempt_failed(attempt, AttemptFailure::Connect, 0, 0) {
                                    continue;
                                }
                                let response = bad_gateway_response();
                                let _ = write_client(&mut client, &response).await;
                                break exchange.attempts_failed(response.len());
                            }
                        };
                    let request_length = exchange.upstream_request().len();

                    if !matches!(
                        timeout(
                            WRITE_TIMEOUT,
                            upstream.write_all(exchange.upstream_request())
                        )
                        .await,
                        Ok(Ok(()))
                    ) {
                        eprintln!("Failed writing to backend {} [async]", attempt.address());
                        if exchange.attempt_failed(
                            attempt,
                            AttemptFailure::Write,
                            request_length,
                            0,
                        ) {
                            continue;
                        }
                        let response = bad_gateway_response();
                        let _ = write_client(&mut client, &response).await;
                        break exchange.attempts_failed(response.len());
                    }

                    match timeout(
                        READ_TIMEOUT,
                        forward_response_stream_async(&mut upstream, &mut client),
                    )
                    .await
                    {
                        Ok(Ok((response_length, status_code))) => {
                            break exchange.attempt_succeeded(
                                attempt,
                                status_code,
                                request_length,
                                response_length,
                            );
                        }
                        _ => {
                            eprintln!(
                                "Failed forwarding response from backend {} [async]",
                                attempt.address()
                            );
                            if exchange.attempt_failed(
                                attempt,
                                AttemptFailure::Response,
                                request_length,
                                0,
                            ) {
                                continue;
                            }
                            let response = bad_gateway_response();
                            let _ = write_client(&mut client, &response).await;
                            break exchange.attempts_failed(response.len());
                        }
                    }
                };

                let succeeded = result.success;
                last_result = Some(result);
                if !succeeded || !keep_alive {
                    break;
                }
            }
        }
    }

    last_result
}

async fn write_client(client: &mut TcpStream, response: &[u8]) -> io::Result<()> {
    timeout(WRITE_TIMEOUT, async {
        client.write_all(response).await?;
        client.flush().await
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "client write timed out"))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[tokio::test]
    async fn async_reader_uses_shared_request_decoder() {
        let raw = b"POST /upload HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello";
        let request = read_http_request_async(&mut Cursor::new(raw.to_vec()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(request.raw, raw);
        assert_eq!(request.path, "/upload");
        assert!(!request.is_idempotent);
        assert!(!request.keep_alive);
    }

    #[tokio::test]
    async fn async_response_transport_obeys_shared_chunk_framing() {
        let raw =
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\nEXTRA";
        let mut output = Vec::new();
        let (written, status) =
            forward_response_stream_async(&mut Cursor::new(raw.to_vec()), &mut output)
                .await
                .unwrap();
        assert_eq!(status, 200);
        assert_eq!(written, raw.len() - 5);
        assert!(!output.ends_with(b"EXTRA"));
    }
}
