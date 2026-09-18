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
    let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\nEXTRA";
    let mut output = Vec::new();
    let (written, status) =
        forward_response_stream_async(&mut Cursor::new(raw.to_vec()), &mut output)
            .await
            .unwrap();
    assert_eq!(status, 200);
    assert_eq!(written, raw.len() - 5);
    assert!(!output.ends_with(b"EXTRA"));
}
