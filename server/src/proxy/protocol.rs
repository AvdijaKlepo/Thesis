//! Incremental HTTP parsing and framing shared by both transport adapters.

use std::io;

pub const MAX_BODY_SIZE: usize = 2 * 1024 * 1024;
pub const MAX_HEADER_SIZE: usize = 64 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientRequest {
    pub raw: Vec<u8>,
    pub header_end: usize,
    pub method: String,
    pub host: Option<String>,
    pub path: String,
    pub is_idempotent: bool,
    pub keep_alive: bool,
}

#[derive(Debug, Eq, PartialEq)]
pub enum DecodeResult<T> {
    Incomplete,
    Complete(T),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResponseBody {
    None,
    ContentLength(usize),
    Chunked,
    UntilClose,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResponseHead {
    pub header_end: usize,
    pub status_code: u16,
    pub body: ResponseBody,
}

pub fn decode_request(buffer: &[u8]) -> io::Result<DecodeResult<ClientRequest>> {
    let Some(header_end) = find_header_end(buffer) else {
        if buffer.len() > MAX_HEADER_SIZE {
            return Err(invalid_data(
                "Request headers exceeded maximum allowed size",
            ));
        }
        return Ok(DecodeResult::Incomplete);
    };

    if header_end > MAX_HEADER_SIZE {
        return Err(invalid_data(
            "Request headers exceeded maximum allowed size",
        ));
    }

    let header_str = String::from_utf8_lossy(&buffer[..header_end]);
    let first_line = header_str.lines().next().unwrap_or("");
    let method = first_line
        .split_whitespace()
        .next()
        .unwrap_or("GET")
        .to_string();
    let (host, path) = request_route(&header_str, first_line);
    let is_idempotent = matches!(
        method.as_str(),
        "GET" | "HEAD" | "OPTIONS" | "PUT" | "DELETE"
    );

    let is_http_10 = first_line.contains("HTTP/1.0");
    let has_close = header_contains(&header_str, "connection", "close");
    let has_keep_alive = header_contains(&header_str, "connection", "keep-alive");
    let keep_alive = if is_http_10 {
        has_keep_alive
    } else {
        !has_close
    };

    let body = &buffer[header_end..];
    let message_end = if header_contains(&header_str, "transfer-encoding", "chunked") {
        if body.len() > MAX_BODY_SIZE {
            return Err(invalid_data("Request body exceeded 2 MiB limit"));
        }
        let mut decoder = ChunkedBodyDecoder::new();
        let consumed = decoder.consume(body)?;
        if !decoder.is_complete() {
            return Ok(DecodeResult::Incomplete);
        }
        header_end + consumed
    } else if let Some(content_length) = content_length(&header_str)? {
        if content_length > MAX_BODY_SIZE {
            return Err(invalid_data(format!(
                "Request body size ({content_length} bytes) exceeds 2 MiB limit"
            )));
        }
        let total = header_end
            .checked_add(content_length)
            .ok_or_else(|| invalid_data("Request size overflow"))?;
        if buffer.len() < total {
            return Ok(DecodeResult::Incomplete);
        }
        total
    } else {
        header_end
    };

    Ok(DecodeResult::Complete(ClientRequest {
        raw: buffer[..message_end].to_vec(),
        header_end,
        method,
        host,
        path,
        is_idempotent,
        keep_alive,
    }))
}

pub fn decode_response_head(buffer: &[u8]) -> io::Result<Option<ResponseHead>> {
    let Some(header_end) = find_header_end(buffer) else {
        if buffer.len() > MAX_HEADER_SIZE {
            return Err(invalid_data(
                "Response headers exceeded maximum allowed size",
            ));
        }
        return Ok(None);
    };
    if header_end > MAX_HEADER_SIZE {
        return Err(invalid_data(
            "Response headers exceeded maximum allowed size",
        ));
    }

    let header_str = String::from_utf8_lossy(&buffer[..header_end]);
    let status_code = header_str
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|status| status.parse::<u16>().ok())
        .unwrap_or(200);

    let body = if (100..200).contains(&status_code) || status_code == 204 || status_code == 304 {
        ResponseBody::None
    } else if header_contains(&header_str, "transfer-encoding", "chunked") {
        ResponseBody::Chunked
    } else if let Some(length) = content_length(&header_str)? {
        ResponseBody::ContentLength(length)
    } else {
        ResponseBody::UntilClose
    };

    Ok(Some(ResponseHead {
        header_end,
        status_code,
        body,
    }))
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

pub fn prepare_upstream_request(raw_request: &[u8]) -> Vec<u8> {
    let Some(header_end) = find_header_end(raw_request) else {
        return raw_request.to_vec();
    };
    let headers = &raw_request[..header_end];
    let body = &raw_request[header_end..];
    let header_str = String::from_utf8_lossy(headers);
    let mut rewritten = String::with_capacity(headers.len() + 32);
    let mut has_connection = false;

    for line in header_str.lines() {
        if line.is_empty() {
            continue;
        }
        if let Some((name, _)) = line.split_once(':')
            && name.trim().eq_ignore_ascii_case("connection")
        {
            has_connection = true;
            rewritten.push_str("Connection: close\r\n");
            continue;
        }
        rewritten.push_str(line);
        rewritten.push_str("\r\n");
    }

    if !has_connection && let Some(first_line_end) = rewritten.find("\r\n") {
        rewritten.insert_str(first_line_end + 2, "Connection: close\r\n");
    }
    rewritten.push_str("\r\n");

    let mut output = rewritten.into_bytes();
    output.extend_from_slice(body);
    output
}

pub fn find_header_end(buffer: &[u8]) -> Option<usize> {
    let crlf = buffer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|position| (position, position + 4));
    let lf = buffer
        .windows(2)
        .position(|window| window == b"\n\n")
        .map(|position| (position, position + 2));

    match (crlf, lf) {
        (Some(left), Some(right)) => Some(if left.0 <= right.0 { left.1 } else { right.1 }),
        (Some((_, end)), None) | (None, Some((_, end))) => Some(end),
        (None, None) => None,
    }
}

#[derive(Debug)]
enum ChunkState {
    Size(Vec<u8>),
    Data(usize),
    DataTerminator(usize),
    Trailers(Vec<u8>),
    Complete,
}

#[derive(Debug)]
pub struct ChunkedBodyDecoder {
    state: ChunkState,
}

impl ChunkedBodyDecoder {
    pub fn new() -> Self {
        Self {
            state: ChunkState::Size(Vec::new()),
        }
    }

    /// Consumes one body fragment and returns the number of bytes belonging to
    /// the chunked message. Bytes after the terminal trailer are not consumed.
    pub fn consume(&mut self, input: &[u8]) -> io::Result<usize> {
        let mut cursor = 0;
        while cursor < input.len() && !self.is_complete() {
            match &mut self.state {
                ChunkState::Size(line) => {
                    line.push(input[cursor]);
                    cursor += 1;
                    if line.len() > MAX_HEADER_SIZE {
                        return Err(invalid_data("Chunk size line is too large"));
                    }
                    if line.ends_with(b"\r\n") {
                        let value = String::from_utf8_lossy(&line[..line.len() - 2]);
                        let hexadecimal = value.split(';').next().unwrap_or("").trim();
                        let size = usize::from_str_radix(hexadecimal, 16).map_err(|error| {
                            invalid_data(format!("Invalid chunk size: {error}"))
                        })?;
                        self.state = if size == 0 {
                            ChunkState::Trailers(Vec::new())
                        } else {
                            ChunkState::Data(size)
                        };
                    }
                }
                ChunkState::Data(remaining) => {
                    let consumed = (*remaining).min(input.len() - cursor);
                    *remaining -= consumed;
                    cursor += consumed;
                    if *remaining == 0 {
                        self.state = ChunkState::DataTerminator(0);
                    }
                }
                ChunkState::DataTerminator(matched) => {
                    let expected = b"\r\n"[*matched];
                    if input[cursor] != expected {
                        return Err(invalid_data("Chunk data is not followed by CRLF"));
                    }
                    *matched += 1;
                    cursor += 1;
                    if *matched == 2 {
                        self.state = ChunkState::Size(Vec::new());
                    }
                }
                ChunkState::Trailers(trailers) => {
                    trailers.push(input[cursor]);
                    cursor += 1;
                    if trailers.len() > MAX_HEADER_SIZE {
                        return Err(invalid_data("Chunk trailers are too large"));
                    }
                    if trailers.as_slice() == b"\r\n" || trailers.ends_with(b"\r\n\r\n") {
                        self.state = ChunkState::Complete;
                    }
                }
                ChunkState::Complete => break,
            }
        }
        Ok(cursor)
    }

    pub fn is_complete(&self) -> bool {
        matches!(self.state, ChunkState::Complete)
    }
}

impl Default for ChunkedBodyDecoder {
    fn default() -> Self {
        Self::new()
    }
}

fn header_contains(headers: &str, expected_name: &str, expected_value: &str) -> bool {
    headers.lines().skip(1).any(|line| {
        line.split_once(':').is_some_and(|(name, value)| {
            name.trim().eq_ignore_ascii_case(expected_name)
                && value
                    .split(',')
                    .any(|part| part.trim().eq_ignore_ascii_case(expected_value))
        })
    })
}

fn content_length(headers: &str) -> io::Result<Option<usize>> {
    headers
        .lines()
        .skip(1)
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.trim()
                .eq_ignore_ascii_case("content-length")
                .then(|| value.trim())
        })
        .map(|value| {
            value
                .parse::<usize>()
                .map_err(|_| invalid_data("Invalid Content-Length header"))
        })
        .transpose()
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_decoder_applies_shared_routing_and_connection_rules() {
        let raw = b"GET http://origin.example/live/lap?session=1 HTTP/1.1\r\nHost: TELEMETRY.EXAMPLE:7879\r\nConnection: close\r\n\r\n";
        let DecodeResult::Complete(request) = decode_request(raw).unwrap() else {
            panic!("request should be complete");
        };

        assert_eq!(request.host.as_deref(), Some("TELEMETRY.EXAMPLE:7879"));
        assert_eq!(request.path, "/live/lap");
        assert!(request.is_idempotent);
        assert!(!request.keep_alive);
        assert_eq!(request.raw, raw);
    }

    #[test]
    fn request_decoder_waits_for_content_length_and_ignores_pipeline_bytes() {
        let partial = b"POST / HTTP/1.1\r\nContent-Length: 5\r\n\r\nhel";
        assert_eq!(decode_request(partial).unwrap(), DecodeResult::Incomplete);

        let complete = b"POST / HTTP/1.1\r\nContent-Length: 5\r\n\r\nhelloNEXT";
        let DecodeResult::Complete(request) = decode_request(complete).unwrap() else {
            panic!("request should be complete");
        };
        assert!(request.raw.ends_with(b"hello"));
        assert!(!request.raw.ends_with(b"NEXT"));
    }

    #[test]
    fn chunk_decoder_handles_fragmented_terminator_and_trailers() {
        let mut decoder = ChunkedBodyDecoder::new();
        assert_eq!(decoder.consume(b"5\r\nhello\r").unwrap(), 9);
        assert!(!decoder.is_complete());
        assert_eq!(decoder.consume(b"\n0\r\nX-Test: yes\r\n").unwrap(), 17);
        assert!(!decoder.is_complete());
        assert_eq!(decoder.consume(b"\r\nEXTRA").unwrap(), 2);
        assert!(decoder.is_complete());
    }

    #[test]
    fn response_head_chooses_body_framing_once() {
        let chunked = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n";
        assert_eq!(
            decode_response_head(chunked).unwrap().unwrap().body,
            ResponseBody::Chunked
        );
        let no_body = b"HTTP/1.1 204 No Content\r\nContent-Length: 10\r\n\r\n";
        assert_eq!(
            decode_response_head(no_body).unwrap().unwrap().body,
            ResponseBody::None
        );
    }

    #[test]
    fn request_decoder_enforces_the_shared_body_limit() {
        let raw = format!(
            "POST / HTTP/1.1\r\nContent-Length: {}\r\n\r\n",
            MAX_BODY_SIZE + 1
        );
        let error = decode_request(raw.as_bytes()).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn request_decoder_rejects_malformed_chunk_framing() {
        let raw = b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\nnope\r\n";
        let error = decode_request(raw).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn upstream_request_forces_connection_close_without_changing_body() {
        let raw = b"POST / HTTP/1.1\r\nHost: local\r\nConnection: keep-alive\r\nContent-Length: 4\r\n\r\ndata";
        let rewritten = prepare_upstream_request(raw);
        let text = String::from_utf8_lossy(&rewritten);
        assert!(text.contains("Connection: close\r\n"));
        assert!(!text.contains("keep-alive"));
        assert!(rewritten.ends_with(b"data"));
    }
}
