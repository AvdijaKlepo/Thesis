use super::*;

const VALID: &str = r#"
schema_version = 1
name = "smoke"
seed = 42
repetitions = 2
output_directory = "results"
algorithms = ["round_robin", "least_connections"]
runtimes = ["thread_pool", "async"]

[server]
command = "cargo"
args = ["run", "--", "--config", "{server_config}"]

[[scenarios]]
id = "steady"
server_config = "server.toml"

[[scenarios.workloads]]
id = "reads"
requests = 10
concurrency = 2
requests_per_second = 5.0

[[scenarios.failures]]
id = "pause-one"
at_ms = 250
action = { command = "docker", args = ["stop", "one"] }

[[scenarios.collect]]
id = "fixture-one"
address = "127.0.0.1:5101"
path = "/metrics"
"#;

#[test]
fn parses_a_matrix_manifest() {
    let manifest = ExperimentManifest::from_toml(VALID).unwrap();
    assert_eq!(manifest.repetitions, 2);
    assert_eq!(manifest.algorithms.len(), 2);
    assert_eq!(manifest.runtimes.len(), 2);
    assert_eq!(manifest.scenarios[0].collect[0].phases.len(), 2);
    assert_eq!(manifest.scenarios[0].workloads[0].method, "GET");
    assert_eq!(manifest.scenarios[0].failures[0].target_backend_id, None);
}

#[test]
fn parses_and_validates_an_explicit_failure_target() {
    let targeted = VALID.replace(
        "at_ms = 250",
        "at_ms = 250\ntarget_backend_id = \"backend-one\"",
    );
    let manifest = ExperimentManifest::from_toml(&targeted).unwrap();
    assert_eq!(
        manifest.scenarios[0].failures[0]
            .target_backend_id
            .as_deref(),
        Some("backend-one")
    );

    let empty = VALID.replace("at_ms = 250", "at_ms = 250\ntarget_backend_id = \" \"");
    assert!(matches!(
        ExperimentManifest::from_toml(&empty),
        Err(ManifestError::Invalid(_))
    ));
}

#[test]
fn rejects_zero_work_and_unknown_fields() {
    let zero = VALID.replace("requests = 10", "requests = 0");
    assert!(matches!(
        ExperimentManifest::from_toml(&zero),
        Err(ManifestError::Invalid(_))
    ));

    let unknown = VALID.replace("seed = 42", "seed = 42\nmystery = true");
    assert!(matches!(
        ExperimentManifest::from_toml(&unknown),
        Err(ManifestError::Parse(_))
    ));
}

#[test]
fn rejects_duplicate_matrix_dimensions() {
    let duplicate = VALID.replace(
        "algorithms = [\"round_robin\", \"least_connections\"]",
        "algorithms = [\"round_robin\", \"round_robin\"]",
    );
    assert!(
        ExperimentManifest::from_toml(&duplicate)
            .unwrap_err()
            .to_string()
            .contains("duplicate algorithm")
    );
}
