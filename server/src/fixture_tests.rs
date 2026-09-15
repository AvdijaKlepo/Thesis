use super::*;

fn exchange(config: FixtureConfig, request: &[u8]) -> Vec<u8> {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let client_request = request.to_vec();
    let client = thread::spawn(move || {
        let mut stream = TcpStream::connect(address).unwrap();
        stream.write_all(&client_request).unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).unwrap();
        response
    });
    let (stream, _) = listener.accept().unwrap();
    handle_connection(stream, &config, &FixtureState::default()).unwrap();
    client.join().unwrap()
}

#[test]
fn arguments_override_configuration_and_are_validated() {
    let mut config = FixtureConfig::default();
    config
        .apply_arguments([
            "--id",
            "slow-a",
            "--capacity",
            "2",
            "--latency-ms",
            "15",
            "--latency-jitter-ms",
            "20",
            "--processing-ms",
            "30",
            "--error-rate",
            "0.25",
            "--error-mode",
            "disconnect",
            "--seed",
            "42",
        ])
        .unwrap();
    assert_eq!(config.id, "slow-a");
    assert_eq!(config.capacity, 2);
    assert_eq!(config.latency_ms, 15);
    assert_eq!(config.latency_jitter_ms, 20);
    assert_eq!(config.processing_ms, 30);
    assert_eq!(config.error_rate, 0.25);
    assert_eq!(config.error_mode, ErrorMode::Disconnect);
    assert_eq!(config.seed, 42);

    assert!(
        FixtureConfig {
            capacity: 0,
            ..FixtureConfig::default()
        }
        .validate()
        .is_err()
    );
    assert!(
        FixtureConfig {
            error_rate: 1.1,
            ..FixtureConfig::default()
        }
        .validate()
        .is_err()
    );
}

#[test]
fn seeded_latency_and_failures_are_repeatable() {
    let config = FixtureConfig {
        latency_ms: 10,
        latency_jitter_ms: 50,
        error_rate: 0.4,
        seed: 123,
        ..FixtureConfig::default()
    };
    let first = (1..=20)
        .map(|id| (config.sampled_latency_ms(id), config.should_fail(id)))
        .collect::<Vec<_>>();
    let second = (1..=20)
        .map(|id| (config.sampled_latency_ms(id), config.should_fail(id)))
        .collect::<Vec<_>>();
    assert_eq!(first, second);
    assert!(first.iter().all(|(latency, _)| (10..=60).contains(latency)));
    assert!(first.iter().any(|(_, failed)| *failed));
    assert!(first.iter().any(|(_, failed)| !failed));
}

#[test]
fn control_endpoints_bypass_workload_faults() {
    let config = FixtureConfig {
        id: "always-fails".into(),
        error_rate: 1.0,
        error_mode: ErrorMode::Disconnect,
        ..FixtureConfig::default()
    };
    let response = exchange(
        config.clone(),
        b"GET /health HTTP/1.1\r\nConnection: close\r\n\r\n",
    );
    assert!(response.starts_with(b"HTTP/1.1 200 OK"));
    assert!(String::from_utf8_lossy(&response).contains("always-fails"));

    let response = exchange(config, b"GET /work HTTP/1.1\r\nConnection: close\r\n\r\n");
    assert!(response.is_empty());
}

#[test]
fn status_failures_are_visible_and_machine_readable() {
    let config = FixtureConfig {
        id: "status-error".into(),
        processing_ms: 0,
        error_rate: 1.0,
        error_mode: ErrorMode::Status,
        ..FixtureConfig::default()
    };
    let response = exchange(config, b"GET /work HTTP/1.1\r\nConnection: close\r\n\r\n");
    assert!(response.starts_with(b"HTTP/1.1 500 Internal Server Error"));
    assert!(String::from_utf8_lossy(&response).contains("configured_error"));
}
