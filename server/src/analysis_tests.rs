use super::*;

#[test]
fn percentile_uses_linear_interpolation() {
    assert_eq!(percentile(&[10, 20, 30, 40], 0.5), Some(25.0));
    assert_eq!(percentile(&[10], 0.99), Some(10.0));
    assert_eq!(percentile(&[], 0.5), None);
}

#[test]
fn jain_index_captures_equal_and_unequal_load() {
    assert_eq!(jain_index(&[5.0, 5.0, 5.0]), Some(1.0));
    let unequal = jain_index(&[10.0, 0.0]).unwrap();
    assert!((unequal - 0.5).abs() < f64::EPSILON);
    assert_eq!(jain_index(&[0.0, 0.0]), None);
}

#[test]
fn recovery_requires_a_consecutive_success_sequence() {
    let completions = [
        (100, true),
        (110, true),
        (120, false),
        (130, true),
        (140, true),
        (150, true),
    ];
    assert_eq!(first_stable_success(&completions, 3), Some(130));
    assert_eq!(first_stable_success(&completions, 4), None);
}

#[test]
fn recovery_detection_ignores_project_and_file_name_fragments() {
    let event = |label: &str, command: &str| {
        serde_json::from_value::<EventInput>(serde_json::json!({
            "event": "failure_completed",
            "label": label,
            "success": true,
            "elapsed_us": 2_500_000,
            "details": { "command": command }
        }))
        .unwrap()
    };

    assert!(!is_recovery_action(&event(
        "stop-healthy-backend",
        "docker compose -p webserver-benchmark-failure-recovery -f compose.fixtures.yml stop --timeout 0 failure-healthy"
    )));
    assert!(!is_recovery_action(&event(
        "pause-backend",
        "docker compose -f C:/experiments/failure-recovery.yml pause backend"
    )));
    assert!(is_recovery_action(&event(
        "restore-healthy-backend",
        "custom-fixture-controller --ready"
    )));
    assert!(is_recovery_action(&event(
        "backend-action",
        "docker compose -p webserver-benchmark-failure-recovery start --wait failure-healthy"
    )));
}

#[test]
fn recovery_analysis_starts_clock_from_action_started() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_recovery_clock_{}", unix_timestamp_ms()));
    fs::create_dir_all(&temp_dir).unwrap();
    let events_path = temp_dir.join("events.jsonl");
    let events_content = r#"{"event":"failure_started","label":"restore-healthy-backend","elapsed_us":4500000,"details":{"target_backend_id":"failure-healthy"}}
{"event":"failure_completed","label":"restore-healthy-backend","success":true,"elapsed_us":10300000,"details":{"target_backend_id":"failure-healthy"}}
"#;
    fs::write(&events_path, events_content).unwrap();

    let requests = vec![
        RequestInput {
            started_offset_us: 4_800_000,
            latency_us: 10_000,
            transport_success: true,
            http_success: true,
            backend_id: Some("failure-healthy".to_string()),
        },
        RequestInput {
            started_offset_us: 4_815_000,
            latency_us: 10_000,
            transport_success: true,
            http_success: false,
            backend_id: Some("failure-healthy".to_string()),
        },
        RequestInput {
            started_offset_us: 4_830_000,
            latency_us: 10_000,
            transport_success: true,
            http_success: true,
            backend_id: Some("failure-healthy".to_string()),
        },
        RequestInput {
            started_offset_us: 4_850_000,
            latency_us: 10_000,
            transport_success: true,
            http_success: true,
            backend_id: Some("failure-healthy".to_string()),
        },
        RequestInput {
            started_offset_us: 4_870_000,
            latency_us: 10_000,
            transport_success: true,
            http_success: true,
            backend_id: Some("failure-healthy".to_string()),
        },
        RequestInput {
            started_offset_us: 4_890_000,
            latency_us: 10_000,
            transport_success: true,
            http_success: true,
            backend_id: Some("failure-healthy".to_string()),
        },
        RequestInput {
            started_offset_us: 4_910_000,
            latency_us: 10_000,
            transport_success: true,
            http_success: true,
            backend_id: Some("failure-healthy".to_string()),
        },
    ];

    let result = recovery_analysis(&temp_dir, &requests).unwrap();
    let _ = fs::remove_dir_all(&temp_dir);

    assert_eq!(result.len(), 1);
    let recovery = &result[0];
    assert_eq!(recovery.action, "restore-healthy-backend");
    assert_eq!(
        recovery.target_backend_id.as_deref(),
        Some("failure-healthy")
    );
    assert_eq!(recovery.action_started_offset_ms, Some(4500.0));
    assert_eq!(recovery.action_completed_offset_ms, 10300.0);
    // Completion of first success is at 4810000 us (4810 ms)
    assert_eq!(recovery.first_success_offset_ms, Some(4810.0));
    // Time to first success relative to action_started (4810 - 4500 = 310 ms)
    assert_eq!(recovery.time_to_first_success_ms, Some(310.0));
    // 5 consecutive successes start at 4840000 us (4840 ms)
    assert_eq!(recovery.stable_success_offset_ms, Some(4840.0));
    assert_eq!(recovery.time_to_stable_success_ms, Some(340.0));
}

#[test]
fn recovery_analysis_ignores_flaky_successes_before_target_backend_recovers() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_recovery_flaky_{}", unix_timestamp_ms()));
    fs::create_dir_all(&temp_dir).unwrap();
    let events_path = temp_dir.join("events.jsonl");
    let events_content = r#"{"event":"failure_started","label":"restore-healthy-backend","elapsed_us":4500000,"details":{"target_backend_id":"failure-healthy"}}
{"event":"failure_completed","label":"restore-healthy-backend","success":true,"elapsed_us":10300000,"details":{"command":"docker compose start --wait failure-healthy","target_backend_id":"failure-healthy"}}
"#;
    fs::write(&events_path, events_content).unwrap();

    let requests = vec![
        // 5 consecutive successes from flaky backend right after action started (4600-4700 ms)
        RequestInput {
            started_offset_us: 4_600_000,
            latency_us: 10_000,
            transport_success: true,
            http_success: true,
            backend_id: Some("failure-flaky".to_string()),
        },
        RequestInput {
            started_offset_us: 4_620_000,
            latency_us: 10_000,
            transport_success: true,
            http_success: true,
            backend_id: Some("failure-flaky".to_string()),
        },
        RequestInput {
            started_offset_us: 4_640_000,
            latency_us: 10_000,
            transport_success: true,
            http_success: true,
            backend_id: Some("failure-flaky".to_string()),
        },
        RequestInput {
            started_offset_us: 4_660_000,
            latency_us: 10_000,
            transport_success: true,
            http_success: true,
            backend_id: Some("failure-flaky".to_string()),
        },
        RequestInput {
            started_offset_us: 4_680_000,
            latency_us: 10_000,
            transport_success: true,
            http_success: true,
            backend_id: Some("failure-flaky".to_string()),
        },
        // Restored backend only recovers at 4900 ms
        RequestInput {
            started_offset_us: 4_900_000,
            latency_us: 10_000,
            transport_success: true,
            http_success: true,
            backend_id: Some("failure-healthy".to_string()),
        },
        RequestInput {
            started_offset_us: 4_920_000,
            latency_us: 10_000,
            transport_success: true,
            http_success: true,
            backend_id: Some("failure-healthy".to_string()),
        },
        RequestInput {
            started_offset_us: 4_940_000,
            latency_us: 10_000,
            transport_success: true,
            http_success: true,
            backend_id: Some("failure-healthy".to_string()),
        },
        RequestInput {
            started_offset_us: 4_960_000,
            latency_us: 10_000,
            transport_success: true,
            http_success: true,
            backend_id: Some("failure-healthy".to_string()),
        },
        RequestInput {
            started_offset_us: 4_980_000,
            latency_us: 10_000,
            transport_success: true,
            http_success: true,
            backend_id: Some("failure-healthy".to_string()),
        },
    ];

    let result = recovery_analysis(&temp_dir, &requests).unwrap();
    let _ = fs::remove_dir_all(&temp_dir);

    assert_eq!(result.len(), 1);
    let recovery = &result[0];
    assert_eq!(
        recovery.target_backend_id.as_deref(),
        Some("failure-healthy")
    );
    // First success MUST NOT be 4610 ms (from flaky), it MUST be 4910 ms (from healthy)!
    assert_eq!(recovery.first_success_offset_ms, Some(4910.0));
    assert_eq!(recovery.time_to_first_success_ms, Some(410.0));
    // Stable success MUST NOT be 4610 ms (from flaky), it MUST be 4910 ms (from healthy)!
    assert_eq!(recovery.stable_success_offset_ms, Some(4910.0));
    assert_eq!(recovery.time_to_stable_success_ms, Some(410.0));
}

#[test]
fn recovery_analysis_falls_back_when_started_event_missing() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_recovery_fallback_{}", unix_timestamp_ms()));
    fs::create_dir_all(&temp_dir).unwrap();
    let events_path = temp_dir.join("events.jsonl");
    let events_content = r#"{"event":"failure_completed","label":"restore-healthy-backend","success":true,"elapsed_us":10300000,"details":{"target_backend_id":"failure-healthy"}}
"#;
    fs::write(&events_path, events_content).unwrap();

    let requests = vec![
        // This request completes before action_completed, so it should be ignored by fallback clock
        RequestInput {
            started_offset_us: 4_800_000,
            latency_us: 10_000,
            transport_success: true,
            http_success: true,
            backend_id: Some("failure-healthy".to_string()),
        },
        // This request completes after action_completed (10410 ms > 10300 ms)
        RequestInput {
            started_offset_us: 10_400_000,
            latency_us: 10_000,
            transport_success: true,
            http_success: true,
            backend_id: Some("failure-healthy".to_string()),
        },
    ];

    let result = recovery_analysis(&temp_dir, &requests).unwrap();
    let _ = fs::remove_dir_all(&temp_dir);

    assert_eq!(result.len(), 1);
    let recovery = &result[0];
    assert_eq!(
        recovery.target_backend_id.as_deref(),
        Some("failure-healthy")
    );
    assert_eq!(recovery.action_started_offset_ms, None);
    assert_eq!(recovery.action_completed_offset_ms, 10300.0);
    assert_eq!(recovery.first_success_offset_ms, Some(10410.0));
    assert_eq!(recovery.time_to_first_success_ms, Some(110.0));
}

#[test]
fn recovery_analysis_without_explicit_target_reports_unknown_recovery() {
    let temp_dir = std::env::temp_dir().join(format!(
        "test_recovery_unknown_target_{}",
        unix_timestamp_ms()
    ));
    fs::create_dir_all(&temp_dir).unwrap();
    let events_path = temp_dir.join("events.jsonl");
    let events_content = r#"{"event":"failure_completed","label":"restore-backend","success":true,"elapsed_us":1000000,"details":{"command":"fixture start backend"}}
"#;
    fs::write(&events_path, events_content).unwrap();

    let requests = vec![RequestInput {
        started_offset_us: 1_100_000,
        latency_us: 10_000,
        transport_success: true,
        http_success: true,
        backend_id: Some("some-backend".to_string()),
    }];

    let result = recovery_analysis(&temp_dir, &requests).unwrap();
    let _ = fs::remove_dir_all(&temp_dir);

    assert_eq!(result.len(), 1);
    let recovery = &result[0];
    assert_eq!(recovery.target_backend_id, None);
    assert_eq!(recovery.first_success_offset_ms, None);
    assert_eq!(recovery.time_to_first_success_ms, None);
    assert_eq!(recovery.stable_success_offset_ms, None);
    assert_eq!(recovery.time_to_stable_success_ms, None);
}

#[test]
fn sha256_matches_a_standard_vector() {
    let mut hasher = Sha256::new();
    hasher.update(b"abc");
    let digest = hasher
        .finish()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    assert_eq!(
        digest,
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
}

#[test]
fn summary_uses_sample_standard_deviation() {
    let summary = summarize_values([1.0, 2.0, 3.0].into_iter());
    assert_eq!(summary.mean, Some(2.0));
    assert_eq!(summary.standard_deviation, Some(1.0));
}
