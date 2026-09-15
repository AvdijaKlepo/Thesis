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
