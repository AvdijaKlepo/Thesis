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
