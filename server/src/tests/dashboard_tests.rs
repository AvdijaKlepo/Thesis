use super::*;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_FIXTURE_ID: AtomicU64 = AtomicU64::new(1);

fn fixture_root() -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "webserver-dashboard-{}-{}-{}",
        std::process::id(),
        unix_timestamp_ms(),
        NEXT_FIXTURE_ID.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir_all(&root).unwrap();
    root
}

#[test]
fn rejects_paths_that_could_escape_the_results_root() {
    let root = fixture_root();
    assert!(matches!(
        child_directory(&root, "..", "experiment"),
        Err(DashboardError::Invalid(_))
    ));
    assert!(matches!(
        child_directory(&root, "a/b", "experiment"),
        Err(DashboardError::Invalid(_))
    ));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn lists_checked_in_experiment_artifacts() {
    let root = fixture_root();
    let experiment = root.join("latency-study-123");
    fs::create_dir_all(&experiment).unwrap();
    fs::write(
        experiment.join("manifest.resolved.json"),
        r#"{"name":"latency-study"}"#,
    )
    .unwrap();
    fs::write(experiment.join("plan.json"), "[]").unwrap();
    let value = list_experiments(&root).unwrap();
    assert!(
        value["experiments"]
            .as_array()
            .is_some_and(|items| !items.is_empty())
    );
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn compacts_large_command_output_from_events() {
    let mut event = json!({"details": {"command": "run", "stdout": "large", "stderr": "large"}});
    compact_event(&mut event);
    assert_eq!(event.pointer("/details/command"), Some(&json!("run")));
    assert!(event.pointer("/details/stdout").is_none());
    assert!(event.pointer("/details/stderr").is_none());
}

#[test]
fn aligns_workload_relative_data_with_run_timestamps() {
    let metadata = json!({"started_unix_ms": 1_000, "duration_ms": 5_000});
    let mut events = vec![json!({
        "event": "workloads_started",
        "timestamp_unix_ms": 3_000,
        "elapsed_us": 25
    })];
    let offset = workload_offset_us(&metadata, &events);
    assert_eq!(offset, 1_999_975);
    add_event_timeline(&mut events[0], &metadata, offset);
    assert_eq!(events[0]["timeline_us"], 2_000_000);
    assert_eq!(
        timeline_duration_us(&metadata, &[], &[], &events, offset),
        5_000_000
    );
}
