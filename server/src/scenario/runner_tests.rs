use super::*;

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
