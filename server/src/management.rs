//! Typed client for the server management API.
//!
//! The scenario runner uses this client directly. The management CLI can use
//! the same API instead of growing a second set of endpoint assumptions.

use std::{
    collections::BTreeMap,
    error::Error,
    fmt::{Display, Formatter},
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    time::Duration,
};

use serde_json::Value;

use crate::{algorithms::AlgorithmKind, proxy::RuntimeMode};

const DEFAULT_MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug)]
pub enum ManagementError {
    Io(std::io::Error),
    InvalidResponse(String),
    Api { status_code: u16, body: String },
    Json(serde_json::Error),
}

impl Display for ManagementError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "management connection failed: {error}"),
            Self::InvalidResponse(message) => write!(formatter, "invalid HTTP response: {message}"),
            Self::Api { status_code, body } => {
                write!(
                    formatter,
                    "management API returned HTTP {status_code}: {body}"
                )
            }
            Self::Json(error) => write!(formatter, "invalid management API JSON: {error}"),
        }
    }
}

impl Error for ManagementError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Json(error) => Some(error),
            Self::InvalidResponse(_) | Self::Api { .. } => None,
        }
    }
}

impl From<std::io::Error> for ManagementError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for ManagementError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

#[derive(Clone, Debug)]
pub struct HttpRequest<'a> {
    pub method: &'a str,
    pub path: &'a str,
    pub host: &'a str,
    pub headers: &'a BTreeMap<String, String>,
    pub body: &'a [u8],
    pub timeout: Duration,
    pub max_response_bytes: usize,
}

#[derive(Clone, Debug)]
pub struct HttpResponse {
    pub status_code: u16,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
    pub bytes_received: usize,
    pub truncated: bool,
}

impl HttpResponse {
    pub fn body_text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .get(&name.to_ascii_lowercase())
            .map(String::as_str)
    }
}

/// Sends a bounded HTTP/1.1 request to an IP socket.
///
/// The project intentionally uses address-and-path management endpoints, so
/// this client has no URL discovery, TLS, redirects, or ambient proxy behavior.
pub fn send_http(
    address: SocketAddr,
    request: &HttpRequest<'_>,
) -> Result<HttpResponse, ManagementError> {
    validate_http_token("method", request.method)?;
    validate_header_value("path", request.path)?;
    validate_header_value("host", request.host)?;
    if !request.path.starts_with('/') {
        return Err(ManagementError::InvalidResponse(
            "request path must start with '/'".into(),
        ));
    }
    for (name, value) in request.headers {
        validate_http_token("header name", name)?;
        validate_header_value("header value", value)?;
    }

    let mut stream = TcpStream::connect_timeout(&address, request.timeout)?;
    stream.set_read_timeout(Some(request.timeout))?;
    stream.set_write_timeout(Some(request.timeout))?;

    let mut head = format!(
        "{} {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nContent-Length: {}\r\n",
        request.method,
        request.path,
        request.host,
        request.body.len()
    );
    for (name, value) in request.headers {
        if name.eq_ignore_ascii_case("host")
            || name.eq_ignore_ascii_case("connection")
            || name.eq_ignore_ascii_case("content-length")
        {
            continue;
        }
        head.push_str(name);
        head.push_str(": ");
        head.push_str(value);
        head.push_str("\r\n");
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes())?;
    stream.write_all(request.body)?;
    stream.flush()?;

    let limit = request.max_response_bytes.saturating_add(1);
    let mut raw = Vec::new();
    stream.take(limit as u64).read_to_end(&mut raw)?;
    let truncated = raw.len() > request.max_response_bytes;
    if truncated {
        raw.truncate(request.max_response_bytes);
    }
    parse_http_response(raw, truncated)
}

fn parse_http_response(raw: Vec<u8>, truncated: bool) -> Result<HttpResponse, ManagementError> {
    let header_end = raw
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|position| position + 4)
        .ok_or_else(|| ManagementError::InvalidResponse("missing header terminator".into()))?;
    let head = std::str::from_utf8(&raw[..header_end])
        .map_err(|_| ManagementError::InvalidResponse("headers are not UTF-8".into()))?;
    let mut lines = head.split("\r\n");
    let status_line = lines
        .next()
        .ok_or_else(|| ManagementError::InvalidResponse("missing status line".into()))?;
    let mut status_parts = status_line.split_whitespace();
    let protocol = status_parts.next().unwrap_or_default();
    if !protocol.starts_with("HTTP/") {
        return Err(ManagementError::InvalidResponse(
            "status line has no HTTP version".into(),
        ));
    }
    let status_code = status_parts
        .next()
        .and_then(|value| value.parse::<u16>().ok())
        .ok_or_else(|| ManagementError::InvalidResponse("invalid status code".into()))?;
    let mut headers = BTreeMap::new();
    for line in lines.filter(|line| !line.is_empty()) {
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| ManagementError::InvalidResponse("malformed response header".into()))?;
        headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
    }
    let bytes_received = raw.len();
    Ok(HttpResponse {
        status_code,
        headers,
        body: raw[header_end..].to_vec(),
        bytes_received,
        truncated,
    })
}

fn validate_http_token(label: &str, value: &str) -> Result<(), ManagementError> {
    if value.is_empty()
        || !value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    byte,
                    b'!' | b'#'
                        | b'$'
                        | b'%'
                        | b'&'
                        | b'\''
                        | b'*'
                        | b'+'
                        | b'-'
                        | b'.'
                        | b'^'
                        | b'_'
                        | b'`'
                        | b'|'
                        | b'~'
                )
        })
    {
        return Err(ManagementError::InvalidResponse(format!("invalid {label}")));
    }
    Ok(())
}

fn validate_header_value(label: &str, value: &str) -> Result<(), ManagementError> {
    if value.contains(['\r', '\n']) {
        return Err(ManagementError::InvalidResponse(format!(
            "{label} contains a line break"
        )));
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub struct ManagementClient {
    address: SocketAddr,
    timeout: Duration,
}

impl ManagementClient {
    pub fn new(address: SocketAddr, timeout: Duration) -> Self {
        Self { address, timeout }
    }

    pub fn address(&self) -> SocketAddr {
        self.address
    }

    pub fn set_algorithm(&self, algorithm: AlgorithmKind) -> Result<(), ManagementError> {
        self.expect_success(self.request(
            "POST",
            "/algorithm",
            algorithm.as_str().as_bytes(),
            Some("text/plain"),
        )?)?;
        Ok(())
    }

    pub fn set_runtime(&self, runtime: RuntimeMode) -> Result<(), ManagementError> {
        self.expect_success(self.request(
            "POST",
            "/runtime",
            runtime.as_str().as_bytes(),
            Some("text/plain"),
        )?)?;
        Ok(())
    }

    pub fn runtime(&self) -> Result<RuntimeMode, ManagementError> {
        let response = self.expect_success(self.request("GET", "/runtime", &[], None)?)?;
        let value: Value = serde_json::from_slice(&response.body)?;
        value
            .get("runtime")
            .and_then(Value::as_str)
            .and_then(RuntimeMode::from_str_name)
            .ok_or_else(|| ManagementError::InvalidResponse("missing runtime field".into()))
    }

    pub fn metrics(&self) -> Result<Value, ManagementError> {
        let response = self.expect_success(self.request("GET", "/metrics", &[], None)?)?;
        Ok(serde_json::from_slice(&response.body)?)
    }

    pub fn get(&self, path: &str) -> Result<HttpResponse, ManagementError> {
        self.request("GET", path, &[], None)
    }

    fn request(
        &self,
        method: &str,
        path: &str,
        body: &[u8],
        content_type: Option<&str>,
    ) -> Result<HttpResponse, ManagementError> {
        let mut headers = BTreeMap::new();
        if let Some(content_type) = content_type {
            headers.insert("Content-Type".into(), content_type.into());
        }
        send_http(
            self.address,
            &HttpRequest {
                method,
                path,
                host: &self.address.to_string(),
                headers: &headers,
                body,
                timeout: self.timeout,
                max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
            },
        )
    }

    fn expect_success(&self, response: HttpResponse) -> Result<HttpResponse, ManagementError> {
        if (200..300).contains(&response.status_code) {
            Ok(response)
        } else {
            Err(ManagementError::Api {
                status_code: response.status_code,
                body: response.body_text(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_response_headers_case_insensitively() {
        let response = parse_http_response(
            b"HTTP/1.1 200 OK\r\nX-Fixture-Backend: car-7\r\nContent-Length: 2\r\n\r\nok".to_vec(),
            false,
        )
        .unwrap();
        assert_eq!(response.status_code, 200);
        assert_eq!(response.header("X-FIXTURE-BACKEND"), Some("car-7"));
        assert_eq!(response.body, b"ok");
    }

    #[test]
    fn rejects_header_injection() {
        let error = validate_header_value("header", "valid\r\nInjected: true").unwrap_err();
        assert!(error.to_string().contains("line break"));
    }
}
