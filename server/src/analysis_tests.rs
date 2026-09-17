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
            workload_id: "test".to_string(),
            scheduled_offset_us: 0,
            backend_id: Some("failure-healthy".to_string()),
        },
        RequestInput {
            started_offset_us: 4_815_000,
            latency_us: 10_000,
            transport_success: true,
            http_success: false,
            workload_id: "test".to_string(),
            scheduled_offset_us: 0,
            backend_id: Some("failure-healthy".to_string()),
        },
        RequestInput {
            started_offset_us: 4_830_000,
            latency_us: 10_000,
            transport_success: true,
            http_success: true,
            workload_id: "test".to_string(),
            scheduled_offset_us: 0,
            backend_id: Some("failure-healthy".to_string()),
        },
        RequestInput {
            started_offset_us: 4_850_000,
            latency_us: 10_000,
            transport_success: true,
            http_success: true,
            workload_id: "test".to_string(),
            scheduled_offset_us: 0,
            backend_id: Some("failure-healthy".to_string()),
        },
        RequestInput {
            started_offset_us: 4_870_000,
            latency_us: 10_000,
            transport_success: true,
            http_success: true,
            workload_id: "test".to_string(),
            scheduled_offset_us: 0,
            backend_id: Some("failure-healthy".to_string()),
        },
        RequestInput {
            started_offset_us: 4_890_000,
            latency_us: 10_000,
            transport_success: true,
            http_success: true,
            workload_id: "test".to_string(),
            scheduled_offset_us: 0,
            backend_id: Some("failure-healthy".to_string()),
        },
        RequestInput {
            started_offset_us: 4_910_000,
            latency_us: 10_000,
            transport_success: true,
            http_success: true,
            workload_id: "test".to_string(),
            scheduled_offset_us: 0,
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
            workload_id: "test".to_string(),
            scheduled_offset_us: 0,
            backend_id: Some("failure-flaky".to_string()),
        },
        RequestInput {
            started_offset_us: 4_620_000,
            latency_us: 10_000,
            transport_success: true,
            http_success: true,
            workload_id: "test".to_string(),
            scheduled_offset_us: 0,
            backend_id: Some("failure-flaky".to_string()),
        },
        RequestInput {
            started_offset_us: 4_640_000,
            latency_us: 10_000,
            transport_success: true,
            http_success: true,
            workload_id: "test".to_string(),
            scheduled_offset_us: 0,
            backend_id: Some("failure-flaky".to_string()),
        },
        RequestInput {
            started_offset_us: 4_660_000,
            latency_us: 10_000,
            transport_success: true,
            http_success: true,
            workload_id: "test".to_string(),
            scheduled_offset_us: 0,
            backend_id: Some("failure-flaky".to_string()),
        },
        RequestInput {
            started_offset_us: 4_680_000,
            latency_us: 10_000,
            transport_success: true,
            http_success: true,
            workload_id: "test".to_string(),
            scheduled_offset_us: 0,
            backend_id: Some("failure-flaky".to_string()),
        },
        // Restored backend only recovers at 4900 ms
        RequestInput {
            started_offset_us: 4_900_000,
            latency_us: 10_000,
            transport_success: true,
            http_success: true,
            workload_id: "test".to_string(),
            scheduled_offset_us: 0,
            backend_id: Some("failure-healthy".to_string()),
        },
        RequestInput {
            started_offset_us: 4_920_000,
            latency_us: 10_000,
            transport_success: true,
            http_success: true,
            workload_id: "test".to_string(),
            scheduled_offset_us: 0,
            backend_id: Some("failure-healthy".to_string()),
        },
        RequestInput {
            started_offset_us: 4_940_000,
            latency_us: 10_000,
            transport_success: true,
            http_success: true,
            workload_id: "test".to_string(),
            scheduled_offset_us: 0,
            backend_id: Some("failure-healthy".to_string()),
        },
        RequestInput {
            started_offset_us: 4_960_000,
            latency_us: 10_000,
            transport_success: true,
            http_success: true,
            workload_id: "test".to_string(),
            scheduled_offset_us: 0,
            backend_id: Some("failure-healthy".to_string()),
        },
        RequestInput {
            started_offset_us: 4_980_000,
            latency_us: 10_000,
            transport_success: true,
            http_success: true,
            workload_id: "test".to_string(),
            scheduled_offset_us: 0,
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
            workload_id: "test".to_string(),
            scheduled_offset_us: 0,
            backend_id: Some("failure-healthy".to_string()),
        },
        // This request completes after action_completed (10410 ms > 10300 ms)
        RequestInput {
            started_offset_us: 10_400_000,
            latency_us: 10_000,
            transport_success: true,
            http_success: true,
            workload_id: "test".to_string(),
            scheduled_offset_us: 0,
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
        workload_id: "test".to_string(),
        scheduled_offset_us: 0,
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

fn phase_two_request(
    workload_id: &str,
    scheduled_offset_us: u64,
    started_offset_us: u64,
    latency_us: u64,
    transport_success: bool,
    http_success: bool,
    backend_id: Option<&str>,
) -> RequestInput {
    RequestInput {
        workload_id: workload_id.to_string(),
        scheduled_offset_us,
        started_offset_us,
        latency_us,
        transport_success,
        http_success,
        backend_id: backend_id.map(str::to_string),
    }
}

#[test]
fn slo_counts_successes_at_the_boundary_and_failures_as_misses() {
    let requests = vec![
        phase_two_request("foreground", 0, 1_000, 100_000, true, true, Some("fast")),
        phase_two_request("foreground", 0, 2_000, 100_001, true, true, Some("fast")),
        phase_two_request("foreground", 0, 3_000, 50_000, true, false, Some("slow")),
        phase_two_request("foreground", 0, 4_000, 10_000, false, false, None),
    ];

    let slo = slo_analysis(&requests, Some(100));
    assert_eq!(slo.target_ms, Some(100));
    assert_eq!(slo.eligible_requests, Some(4));
    assert_eq!(slo.successful_within_slo, Some(1));
    assert_eq!(slo.miss_count, Some(3));
    assert_eq!(slo.attainment_rate, Some(0.25));
    assert_eq!(slo.miss_rate, Some(0.75));
    assert_eq!(slo_analysis(&requests, None), SloAnalysis::default());
}

#[test]
fn scheduling_lag_uses_planned_and_actual_offsets() {
    let requests = vec![
        phase_two_request("load", 0, 0, 10, true, true, None),
        phase_two_request("load", 1_000, 1_100, 10, true, true, None),
        phase_two_request("load", 2_000, 2_300, 10, true, true, None),
        phase_two_request("load", 3_000, 3_600, 10, true, true, None),
    ];

    let lag = scheduling_lag_analysis(&requests);
    assert_eq!(lag.samples, 4);
    assert_eq!(lag.mean_us, Some(250.0));
    assert_eq!(lag.p50_us, Some(200.0));
    assert!((lag.p95_us.unwrap() - 555.0).abs() < 1e-9);
    assert!((lag.p99_us.unwrap() - 591.0).abs() < 1e-9);
    assert_eq!(lag.max_us, Some(600));
}

#[test]
fn workload_and_window_summaries_preserve_identity_and_half_open_boundaries() {
    let requests = vec![
        phase_two_request("background", 0, 0, 10, true, true, Some("slow")),
        phase_two_request("foreground", 1_000, 1_000, 20, true, true, Some("fast")),
        phase_two_request("foreground", 2_000, 2_000, 30, true, false, Some("fast")),
        phase_two_request("background", 3_000, 3_000, 40, true, true, Some("slow")),
    ];
    let workloads = workload_analysis(&requests, Some(1));
    assert_eq!(
        workloads
            .iter()
            .map(|workload| workload.workload_id.as_str())
            .collect::<Vec<_>>(),
        vec!["background", "foreground"]
    );
    assert_eq!(workloads[0].total_requests, 2);
    assert_eq!(workloads[1].successful_requests, 1);
    assert_eq!(workloads[1].http_errors, 1);

    let windows = window_analysis(
        &requests,
        &[
            AnalysisWindowInput {
                id: "warmup".into(),
                start_ms: 0,
                end_ms: 2,
            },
            AnalysisWindowInput {
                id: "steady".into(),
                start_ms: 2,
                end_ms: 4,
            },
        ],
    );
    assert_eq!(windows[0].total_requests, 2);
    assert_eq!(windows[0].backend_requests.get("slow"), Some(&1));
    assert_eq!(windows[1].total_requests, 2);
    assert_eq!(windows[1].backend_requests.get("fast"), Some(&1));
    assert_eq!(windows[1].backend_requests.get("slow"), Some(&1));
}

#[test]
fn legacy_request_json_uses_phase_two_defaults() {
    let request: RequestInput = serde_json::from_value(serde_json::json!({
        "started_offset_us": 42,
        "latency_us": 7,
        "transport_success": true,
        "http_success": true,
        "backend_id": "legacy"
    }))
    .unwrap();
    assert!(request.workload_id.is_empty());
    assert_eq!(request.scheduled_offset_us, 0);
    assert_eq!(scheduling_lag_analysis(&[request]).p50_us, Some(42.0));
}

#[test]
fn resource_analysis_keeps_host_and_proxy_metrics_distinct() {
    let temp_dir = std::env::temp_dir().join(format!(
        "test_resource_host_metrics_{}",
        unix_timestamp_ms()
    ));
    fs::create_dir_all(&temp_dir).unwrap();
    fs::write(
        temp_dir.join("resource-samples.jsonl"),
        concat!(
            "{\"elapsed_us\":0,\"cpu_time_us\":100,\"resident_memory_bytes\":10,",
            "\"virtual_memory_bytes\":20,\"host_cpu_percent\":40.0,\"host_memory_bytes\":100}\n",
            "{\"elapsed_us\":100000,\"cpu_time_us\":1100,\"resident_memory_bytes\":12,",
            "\"virtual_memory_bytes\":22,\"host_cpu_percent\":60.0,\"host_memory_bytes\":120}\n"
        ),
    )
    .unwrap();

    let resource = resource_analysis(&temp_dir).unwrap();
    let _ = fs::remove_dir_all(&temp_dir);
    assert_eq!(resource.samples, 2);
    assert_eq!(resource.host_samples, 2);
    assert_eq!(resource.average_cpu_percent, Some(1.0));
    assert_eq!(resource.host_average_cpu_percent, Some(50.0));
    assert_eq!(resource.mean_resident_memory_bytes, Some(11.0));
    assert_eq!(resource.host_mean_memory_bytes, Some(110.0));
}

#[test]
fn synthetic_experiment_writes_phase_two_report_and_long_form_outputs() {
    let root =
        std::env::temp_dir().join(format!("test_phase_two_experiment_{}", unix_timestamp_ms()));
    let run = root.join("0001-run");
    fs::create_dir_all(&run).unwrap();
    fs::write(
        run.join("metadata.json"),
        serde_json::json!({
            "run_id": "0001-run",
            "status": "completed",
            "scenario": {
                "id": "scaled-three",
                "slo_target_ms": 100,
                "analysis_windows": [
                    {"id": "warmup", "start_ms": 0, "end_ms": 2}
                ]
            },
            "algorithm": "adaptive_balancing_v2",
            "runtime": "async",
            "repetition": 1,
            "run_seed": 17
        })
        .to_string(),
    )
    .unwrap();
    fs::write(
        run.join("requests.jsonl"),
        concat!(
            "{\"started_offset_us\":0,\"latency_us\":100000,\"transport_success\":true,",
            "\"http_success\":true,\"backend_id\":\"fast\",\"workload_id\":\"foreground\",",
            "\"scheduled_offset_us\":0}\n",
            "{\"started_offset_us\":1000,\"latency_us\":200000,\"transport_success\":true,",
            "\"http_success\":false,\"backend_id\":\"slow\",\"workload_id\":\"background\",",
            "\"scheduled_offset_us\":0}\n"
        ),
    )
    .unwrap();

    let report = analyze_experiment(&root, &AnalysisOptions::default()).unwrap();
    assert_eq!(report.schema_version, 2);
    assert_eq!(report.runs[0].slo.successful_within_slo, Some(1));
    assert_eq!(report.runs[0].workloads.len(), 2);
    assert_eq!(report.runs[0].windows[0].total_requests, 2);
    assert!(root.join("analysis/workload-summary.csv").is_file());
    assert!(root.join("analysis/window-summary.csv").is_file());
    assert!(root.join("analysis/group-window-summary.csv").is_file());
    let _ = fs::remove_dir_all(&root);
}
