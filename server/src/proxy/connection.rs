//! Blocking transport adapter for the shared proxy behavior.

use std::{
    io::{self, Read, Write},
    net::TcpStream,
};

use crate::{
    observability::Observability,
    proxy::{
        behavior::{
            AttemptFailure, CONNECT_TIMEOUT, NextAttempt, ProxyResult, READ_TIMEOUT, RequestPlan,
            WRITE_TIMEOUT, bad_gateway_response, load_static_response, plan_request,
            service_unavailable_response,
        },
        mode::RuntimeMode,
        protocol::{
            ChunkedBodyDecoder, ClientRequest, DecodeResult, MAX_HEADER_SIZE, ResponseBody,
            decode_request, decode_response_head,
        },
    },
    service::ServiceRouter,
};

pub fn read_http_request<R: Read>(stream: &mut R) -> io::Result<Option<ClientRequest>> {
    let mut buffer = Vec::with_capacity(4096);
    let mut chunk = [0_u8; 2048];

    loop {
        match decode_request(&buffer)? {
            DecodeResult::Complete(request) => return Ok(Some(request)),
            DecodeResult::Incomplete => {}
        }

        let read = stream.read(&mut chunk)?;
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

pub fn forward_response_stream<R: Read, W: Write>(
    upstream: &mut R,
    client: &mut W,
) -> io::Result<(usize, u16)> {
    let mut initial = Vec::with_capacity(4096);
    let mut chunk = [0_u8; 8192];
    let head = loop {
        if let Some(head) = decode_response_head(&initial)? {
            break head;
        }
        let read = upstream.read(&mut chunk)?;
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
    client.write_all(&initial[..head.header_end])?;
    total_written += head.header_end;
    let initial_body = &initial[head.header_end..];

    match head.body {
        ResponseBody::None => {}
        ResponseBody::ContentLength(length) => {
            let prefix = length.min(initial_body.len());
            client.write_all(&initial_body[..prefix])?;
            total_written += prefix;
            let mut remaining = length - prefix;
            while remaining > 0 {
                let capacity = remaining.min(chunk.len());
                let read = upstream.read(&mut chunk[..capacity])?;
                if read == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "Upstream closed before the Content-Length body was complete",
                    ));
                }
                client.write_all(&chunk[..read])?;
                total_written += read;
                remaining -= read;
            }
        }
        ResponseBody::Chunked => {
            let mut decoder = ChunkedBodyDecoder::new();
            let consumed = decoder.consume(initial_body)?;
            client.write_all(&initial_body[..consumed])?;
            total_written += consumed;

            while !decoder.is_complete() {
                let read = upstream.read(&mut chunk)?;
                if read == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "Upstream closed before the chunked body was complete",
                    ));
                }
                let consumed = decoder.consume(&chunk[..read])?;
                client.write_all(&chunk[..consumed])?;
                total_written += consumed;
            }
        }
        ResponseBody::UntilClose => {
            client.write_all(initial_body)?;
            total_written += initial_body.len();
            loop {
                let read = upstream.read(&mut chunk)?;
                if read == 0 {
                    break;
                }
                client.write_all(&chunk[..read])?;
                total_written += read;
            }
        }
    }

    client.flush()?;
    Ok((total_written, head.status_code))
}

pub fn proxy_connections(
    mut client: TcpStream,
    router: &ServiceRouter,
    observability: &Observability,
) -> Option<ProxyResult> {
    let _ = client.set_read_timeout(Some(READ_TIMEOUT));
    let _ = client.set_write_timeout(Some(WRITE_TIMEOUT));
    let mut last_result = None;

    loop {
        let request = match read_http_request(&mut client) {
            Ok(Some(request)) => request,
            Ok(None) => break,
            Err(error) => {
                if !matches!(
                    error.kind(),
                    io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
                ) {
                    eprintln!("Error reading client request: {error}");
                }
                break;
            }
        };
        let keep_alive = request.keep_alive;

        match plan_request(RuntimeMode::ThreadPool, &request, router, observability) {
            RequestPlan::Static(exchange) => {
                let response = load_static_response(exchange.root(), exchange.path());
                let written = client.write_all(&response.bytes).is_ok() && client.flush().is_ok();
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
                            eprintln!("Unable to select a backend: {error}");
                            let response = service_unavailable_response();
                            let _ = client.write_all(&response);
                            let _ = client.flush();
                            break exchange.no_eligible_backend(response.len());
                        }
                        NextAttempt::Exhausted => {
                            let response = bad_gateway_response();
                            let _ = client.write_all(&response);
                            let _ = client.flush();
                            break exchange.attempts_failed(response.len());
                        }
                    };

                    let address = match attempt.address().parse() {
                        Ok(address) => address,
                        Err(error) => {
                            eprintln!("Invalid backend address {}: {error}", attempt.address());
                            if exchange.attempt_failed(attempt, AttemptFailure::Connect, 0, 0) {
                                continue;
                            }
                            let response = bad_gateway_response();
                            let _ = client.write_all(&response);
                            let _ = client.flush();
                            break exchange.attempts_failed(response.len());
                        }
                    };

                    let mut upstream = match TcpStream::connect_timeout(&address, CONNECT_TIMEOUT) {
                        Ok(upstream) => upstream,
                        Err(error) => {
                            eprintln!(
                                "Failed to connect to backend {}: {error}",
                                attempt.address()
                            );
                            if exchange.attempt_failed(attempt, AttemptFailure::Connect, 0, 0) {
                                continue;
                            }
                            let response = bad_gateway_response();
                            let _ = client.write_all(&response);
                            let _ = client.flush();
                            break exchange.attempts_failed(response.len());
                        }
                    };
                    let _ = upstream.set_read_timeout(Some(READ_TIMEOUT));
                    let _ = upstream.set_write_timeout(Some(WRITE_TIMEOUT));
                    let request_length = exchange.upstream_request().len();

                    if let Err(error) = upstream.write_all(exchange.upstream_request()) {
                        eprintln!("Failed writing to backend {}: {error}", attempt.address());
                        if exchange.attempt_failed(
                            attempt,
                            AttemptFailure::Write,
                            request_length,
                            0,
                        ) {
                            continue;
                        }
                        let response = bad_gateway_response();
                        let _ = client.write_all(&response);
                        let _ = client.flush();
                        break exchange.attempts_failed(response.len());
                    }

                    match forward_response_stream(&mut upstream, &mut client) {
                        Ok((response_length, status_code)) => {
                            break exchange.attempt_succeeded(
                                attempt,
                                status_code,
                                request_length,
                                response_length,
                            );
                        }
                        Err(error) => {
                            eprintln!(
                                "Failed forwarding response from backend {}: {error}",
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
                            let _ = client.write_all(&response);
                            let _ = client.flush();
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn blocking_reader_uses_shared_request_decoder() {
        let raw = b"POST /upload HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello";
        let request = read_http_request(&mut Cursor::new(raw)).unwrap().unwrap();
        assert_eq!(request.raw, raw);
        assert_eq!(request.path, "/upload");
        assert!(!request.is_idempotent);
        assert!(!request.keep_alive);
    }

    #[test]
    fn blocking_response_transport_obeys_shared_chunk_framing() {
        let raw =
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\nEXTRA";
        let mut output = Vec::new();
        let (written, status) =
            forward_response_stream(&mut Cursor::new(raw), &mut output).unwrap();
        assert_eq!(status, 200);
        assert_eq!(written, raw.len() - 5);
        assert!(!output.ends_with(b"EXTRA"));
    }
}
