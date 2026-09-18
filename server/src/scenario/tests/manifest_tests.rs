use super::*;

use std::path::PathBuf;

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

#[test]
fn parses_phase_two_analysis_options_and_validates_windows() {
    let configured = VALID.replace(
        "schema_version = 1",
        "schema_version = 1\npaired_workload_seeds = true\nexecution_order = \"blocked_randomized\"",
    ).replace(
        "id = \"steady\"",
        "id = \"steady\"\nslo_target_ms = 200\nworkload_seed_group = \"cardinality\"\nanalysis_windows = [{ id = \"warmup\", start_ms = 0, end_ms = 1000 }]",
    );
    let manifest = ExperimentManifest::from_toml(&configured).unwrap();
    assert!(manifest.paired_workload_seeds);
    assert_eq!(manifest.execution_order, ExecutionOrder::BlockedRandomized);
    assert_eq!(manifest.scenarios[0].slo_target_ms, Some(200));
    assert_eq!(manifest.scenarios[0].analysis_windows[0].id, "warmup");

    let invalid = configured.replace("end_ms = 1000", "end_ms = 0");
    assert!(matches!(
        ExperimentManifest::from_toml(&invalid),
        Err(ManifestError::Invalid(_))
    ));
}

#[test]
fn parses_and_validates_typed_fixture_changes() {
    let configured = VALID.replace(
        "id = \"steady\"",
        "id = \"steady\"\nfixture_changes = [{ id = \"slow-down\", at_ms = 100, address = \"127.0.0.1:5101\", patch = { latency_ms = 250, error_rate = 0.1 } }]",
    );
    let manifest = ExperimentManifest::from_toml(&configured).unwrap();
    let change = &manifest.scenarios[0].fixture_changes[0];
    assert_eq!(change.id, "slow-down");
    assert_eq!(change.patch.latency_ms, Some(250));
    assert_eq!(change.patch.error_rate, Some(0.1));

    let immutable = configured.replace(
        "patch = { latency_ms = 250, error_rate = 0.1 }",
        "patch = { capacity = 1 }",
    );
    assert!(matches!(
        ExperimentManifest::from_toml(&immutable),
        Err(ManifestError::Parse(_))
    ));
}

#[test]
fn phase_five_manifests_load_the_scaled_fixture_matrix() {
    let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let experiments = repository.parent().unwrap().join("experiments");
    let expected = [
        ("backend-cardinality-calibration.toml", 3),
        ("adaptive-v2-tuning.toml", 3),
        ("adaptive-cardinality.toml", 3),
        ("equal-capacity-12.toml", 1),
        ("heterogeneous-capacity-13.toml", 1),
        ("variable-latency-12.toml", 1),
        ("saturation-sweep-12.toml", 3),
        ("burst-load-12.toml", 1),
        ("retry-behavior-12.toml", 2),
        ("adaptive-phase-change-12.toml", 1),
    ];

    for (file_name, scenario_count) in expected {
        let manifest = ExperimentManifest::load(experiments.join(file_name))
            .unwrap_or_else(|error| panic!("{file_name} should be valid: {error}"));
        assert_eq!(manifest.scenarios.len(), scenario_count, "{file_name}");
        assert!(manifest.paired_workload_seeds, "{file_name}");
        assert_eq!(manifest.execution_order, ExecutionOrder::BlockedRandomized);
    }
}
