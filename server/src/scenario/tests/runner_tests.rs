use super::*;

use std::{
    io::{Read, Write},
    net::TcpListener,
};

use crate::scenario::manifest::FixtureChange;

fn manifest() -> ExperimentManifest {
    ExperimentManifest::from_toml(
        r#"
schema_version = 1
name = "matrix"
seed = 7
repetitions = 2
output_directory = "results"
algorithms = ["round_robin", "least_connections"]
runtimes = ["thread_pool", "async"]

[server]
command = "server"

[[scenarios]]
id = "one"
server_config = "one.toml"
workloads = [{ id = "load", requests = 1, concurrency = 1 }]

[[scenarios]]
id = "two"
server_config = "two.toml"
workloads = [{ id = "load", requests = 1, concurrency = 1 }]
"#,
    )
    .unwrap()
}

#[test]
fn expands_the_full_matrix_with_stable_seeds() {
    let first = ScenarioRunner::new(manifest(), RunnerOptions::default())
        .unwrap()
        .plan();
    let second = ScenarioRunner::new(manifest(), RunnerOptions::default())
        .unwrap()
        .plan();
    assert_eq!(first.len(), 16);
    assert_eq!(first[0].seed, second[0].seed);
    assert_ne!(first[0].seed, first[1].seed);
}

#[test]
fn blocked_randomized_order_is_deterministic_within_each_block() {
    let configured = manifest_toml_with_options();
    let manifest = ExperimentManifest::from_toml(&configured).unwrap();
    let first = ScenarioRunner::new(manifest.clone(), RunnerOptions::default())
        .unwrap()
        .plan();
    let second = ScenarioRunner::new(manifest, RunnerOptions::default())
        .unwrap()
        .plan();
    assert_eq!(first, second);
    assert_eq!(first.len(), 8);
    assert_eq!(
        first
            .iter()
            .take(2)
            .map(|run| run.runtime)
            .collect::<Vec<_>>(),
        vec![RuntimeMode::ThreadPool, RuntimeMode::ThreadPool]
    );
    assert_ne!(first[0].algorithm, first[1].algorithm);
    assert_eq!(first[0].repetition, 1);
    assert_eq!(first[1].repetition, 1);
}

#[test]
fn paired_workload_seed_excludes_algorithm_and_scenario_identity() {
    let first = derive_workload_seed(7, "cardinality", RuntimeMode::Async, 2, "load");
    let second = derive_workload_seed(7, "cardinality", RuntimeMode::Async, 2, "load");
    assert_eq!(first, second);
    assert_ne!(
        first,
        derive_workload_seed(7, "other-group", RuntimeMode::Async, 2, "load")
    );
}

fn manifest_toml_with_options() -> String {
    manifest_toml_base()
}

fn manifest_toml_base() -> String {
    r#"
schema_version = 1
name = "matrix"
seed = 7
repetitions = 1
output_directory = "results"
algorithms = ["round_robin", "least_connections"]
runtimes = ["thread_pool", "async"]
paired_workload_seeds = true
execution_order = "blocked_randomized"

[server]
command = "server"

[[scenarios]]
id = "one"
server_config = "one.toml"
workload_seed_group = "cardinality"
workloads = [{ id = "load", requests = 1, concurrency = 1 }]

[[scenarios]]
id = "two"
server_config = "two.toml"
workload_seed_group = "cardinality"
workloads = [{ id = "load", requests = 1, concurrency = 1 }]
"#
    .into()
}

#[test]
fn filters_matrix_dimensions_without_reordering() {
    let options = RunnerOptions {
        scenarios: vec!["two".into()],
        algorithms: vec![AlgorithmKind::LeastConnections],
        runtimes: vec![RuntimeMode::Async],
        repetitions: Some(1),
        output_directory: None,
    };
    let plan = ScenarioRunner::new(manifest(), options).unwrap().plan();
    assert_eq!(plan.len(), 1);
    assert_eq!(plan[0].scenario, "two");
    assert_eq!(plan[0].algorithm, AlgorithmKind::LeastConnections);
    assert_eq!(plan[0].runtime, RuntimeMode::Async);
}

#[test]
fn expands_placeholders_without_a_shell() {
    let context = PlaceholderContext {
        manifest_directory: PathBuf::from("/experiments"),
        server_config: PathBuf::from("/configs/server.toml"),
        run_directory: PathBuf::from("/results/run"),
        seed: 9,
        scenario: "failure".into(),
        algorithm: "round_robin".into(),
        runtime: "async".into(),
        repetition: 2,
    };
    assert_eq!(
        expand("{scenario}-{runtime}-{seed}-{repetition}", &context),
        "failure-async-9-2"
    );
}

#[test]
fn verifies_the_selected_default_service_algorithm() {
    let metrics = json!({
        "services": [
            {"service_id": "other", "algorithm": "least_connections"},
            {"service_id": "default", "algorithm": "round_robin"}
        ]
    });
    assert!(verify_algorithm(&metrics, "default", AlgorithmKind::RoundRobin).is_ok());
    assert!(verify_algorithm(&metrics, "default", AlgorithmKind::LeastConnections).is_err());
}

#[test]
fn event_log_is_visible_before_the_run_finishes() {
    let directory = std::env::temp_dir().join(format!(
        "webserver-event-log-{}-{}",
        std::process::id(),
        unix_timestamp_ms()
    ));
    fs::create_dir_all(&directory).unwrap();
    let events = EventLog::create(&directory).unwrap();
    events.record(
        "workloads_started",
        "all",
        Some(true),
        json!({"count": 2}),
        None,
    );

    let contents = fs::read_to_string(directory.join("events.jsonl")).unwrap();
    let event: Value = serde_json::from_str(contents.trim()).unwrap();
    assert_eq!(event["sequence"], 0);
    assert_eq!(event["event"], "workloads_started");
    events.finish().unwrap();
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn fixture_changes_record_effective_before_and_after_configurations() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = Vec::new();
            let mut chunk = [0_u8; 1024];
            let header_end = loop {
                let read = stream.read(&mut chunk).unwrap();
                assert!(read > 0);
                request.extend_from_slice(&chunk[..read]);
                if let Some(position) = request.windows(4).position(|window| window == b"\r\n\r\n")
                {
                    break position + 4;
                }
            };
            let request_text = String::from_utf8_lossy(&request[..header_end]);
            let response_body = if request_text.starts_with("GET /config") {
                r#"{"id":"fixture-a","listen_address":"127.0.0.1:5101","capacity":4,"latency_ms":50,"latency_jitter_ms":0,"processing_ms":10,"error_rate":0.0,"error_mode":"status","seed":7}"#
            } else {
                assert!(request_text.starts_with("POST /control"));
                let content_length = request_text
                    .lines()
                    .find_map(|line| line.strip_prefix("Content-Length: "))
                    .unwrap()
                    .parse::<usize>()
                    .unwrap();
                while request.len() < header_end + content_length {
                    let read = stream.read(&mut chunk).unwrap();
                    assert!(read > 0);
                    request.extend_from_slice(&chunk[..read]);
                }
                assert!(String::from_utf8_lossy(&request).contains("latency_ms"));
                r#"{"id":"fixture-a","listen_address":"127.0.0.1:5101","capacity":4,"latency_ms":5,"latency_jitter_ms":0,"processing_ms":10,"error_rate":0.0,"error_mode":"status","seed":7}"#
            };
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                response_body.len(),
                response_body
            );
            stream.write_all(response.as_bytes()).unwrap();
        }
    });

    let directory = std::env::temp_dir().join(format!(
        "webserver-fixture-change-{}-{}",
        std::process::id(),
        unix_timestamp_ms()
    ));
    fs::create_dir_all(&directory).unwrap();
    let events = Arc::new(EventLog::create(&directory).unwrap());
    let mut experiment = manifest();
    experiment.scenarios[0].fixture_changes = vec![FixtureChange {
        id: "speed-up".into(),
        at_ms: 0,
        address: address.to_string(),
        patch: crate::fixture::FixtureConfigPatch {
            latency_ms: Some(5),
            ..Default::default()
        },
        allow_failure: false,
    }];

    let handles = start_fixture_changes(
        &experiment.scenarios[0],
        Instant::now(),
        Arc::clone(&events),
        1_000,
    );
    assert_eq!(handles.len(), 1);
    assert!(
        handles
            .into_iter()
            .next()
            .unwrap()
            .join()
            .unwrap()
            .is_none()
    );
    events.finish().unwrap();
    server.join().unwrap();

    let lines = fs::read_to_string(directory.join("events.jsonl")).unwrap();
    let records = lines
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["event"], "fixture_change_started");
    assert_eq!(records[0]["details"]["effective_before"]["latency_ms"], 50);
    assert_eq!(records[1]["event"], "fixture_change_completed");
    assert_eq!(records[1]["success"], true);
    assert_eq!(records[1]["details"]["effective_after"]["latency_ms"], 5);
    assert_eq!(records[1]["details"]["effective_before"]["latency_ms"], 50);
    fs::remove_dir_all(directory).unwrap();
}
