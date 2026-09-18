use super::*;
use std::{collections::BTreeMap, fs};

fn workload() -> WorkloadManifest {
    WorkloadManifest {
        id: "paced".into(),
        requests: 3,
        concurrency: 1,
        method: "GET".into(),
        path: "/".into(),
        host: None,
        headers: BTreeMap::new(),
        body: String::new(),
        start_after_ms: 20,
        requests_per_second: Some(10.0),
        jitter_ms: 10,
        timeout_ms: None,
        max_response_bytes: 1024,
    }
}

#[test]
fn workload_schedule_is_seeded_and_repeatable() {
    let workload = workload();
    let first = scheduled_offset(&workload, 2, 99);
    let second = scheduled_offset(&workload, 2, 99);
    let another = scheduled_offset(&workload, 2, 100);
    assert_eq!(first, second);
    assert_ne!(first, another);
    assert!(first >= Duration::from_millis(220));
    assert!(first <= Duration::from_millis(230));
}

#[test]
fn stable_hash_does_not_depend_on_process_state() {
    assert_eq!(stable_hash("scenario"), 0xde4217cc72f4d0cf);
}

#[test]
fn request_log_is_visible_before_the_workload_finishes() {
    let directory = std::env::temp_dir().join(format!(
        "webserver-request-log-{}-{}",
        std::process::id(),
        unix_timestamp_ms()
    ));
    fs::create_dir_all(&directory).unwrap();
    let log = RequestLog::create(&directory).unwrap();
    log.record(&RequestMeasurement {
        schema_version: 1,
        workload_id: "paced".into(),
        request_index: 0,
        worker_id: 0,
        method: "GET".into(),
        path: "/".into(),
        scheduled_offset_us: 0,
        started_offset_us: 10,
        started_unix_ms: 100,
        completed_unix_ms: 101,
        latency_us: 50,
        transport_success: true,
        http_success: true,
        status_code: Some(200),
        response_bytes: 12,
        response_truncated: false,
        backend_id: Some("backend-1".into()),
        fixture_request_id: Some("1".into()),
        error: None,
    });

    let contents = fs::read_to_string(directory.join("requests.jsonl")).unwrap();
    let request: serde_json::Value = serde_json::from_str(contents.trim()).unwrap();
    assert_eq!(request["workload_id"], "paced");
    assert_eq!(request["latency_us"], 50);
    log.finish().unwrap();
    fs::remove_dir_all(directory).unwrap();
}
