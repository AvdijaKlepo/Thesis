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

#[test]
fn port_discovery_rejects_non_loopback_and_large_ranges() {
    assert!(
        discover_loopback_ports(
            "192.0.2.1".parse().unwrap(),
            8000,
            8001,
            Duration::from_millis(10)
        )
        .is_err()
    );
    assert!(
        discover_loopback_ports(
            "127.0.0.1".parse().unwrap(),
            8000,
            9000,
            Duration::from_millis(10)
        )
        .is_err()
    );
}

#[test]
fn managed_service_models_validate_ids_and_normalize_routes() {
    let service = ServiceDefinition {
        id: "catalog.v2".into(),
        kind: ManagedServiceKind::Proxy,
        routes: vec![RouteMatcher {
            host: Some("EXAMPLE.TEST:8080".into()),
            path_prefix: "/catalog/".into(),
        }],
        algorithm: Some(AlgorithmKind::LeastConnections),
        fail_open: false,
        backends: vec![Backend {
            id: "catalog-a".into(),
            address: "catalog-a.internal:8080".into(),
            weight: 2,
        }],
        adaptive_v2: AdaptiveV2Settings::default(),
        root: None,
    }
    .build()
    .unwrap();

    assert_eq!(service.id, "catalog.v2");
    assert_eq!(service.routes[0].host.as_deref(), Some("example.test"));
    assert_eq!(service.routes[0].path_prefix, "/catalog");

    let invalid = ServiceDefinition {
        id: "not/a/path".into(),
        kind: ManagedServiceKind::Proxy,
        routes: vec![RouteMatcher {
            host: None,
            path_prefix: "/".into(),
        }],
        algorithm: Some(AlgorithmKind::RoundRobin),
        fail_open: false,
        backends: Vec::new(),
        adaptive_v2: AdaptiveV2Settings::default(),
        root: None,
    };
    assert!(invalid.build().is_err());
}
