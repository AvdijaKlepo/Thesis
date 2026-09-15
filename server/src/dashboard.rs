//! Read-only HTTP dashboard over scenario-runner result artifacts.

use std::{
    fs, io,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::Serialize;
use serde_json::{Map, Value, json};
use tiny_http::{Header, Method, Request, Response, Server, StatusCode};

use crate::analysis::analyze_run_snapshot;

const INDEX_HTML: &str = include_str!("../dashboard/index.html");
const APP_CSS: &str = include_str!("../dashboard/app.css");
const APP_JS: &str = include_str!("../dashboard/app.js");

pub struct DashboardServer {
    address: String,
    results_root: PathBuf,
}

impl DashboardServer {
    pub fn new(address: impl Into<String>, results_root: impl Into<PathBuf>) -> Self {
        Self {
            address: address.into(),
            results_root: results_root.into(),
        }
    }

    pub fn run(&self) -> Result<(), String> {
        let root = fs::canonicalize(&self.results_root).map_err(|error| {
            format!(
                "failed to resolve results root {}: {error}",
                self.results_root.display()
            )
        })?;
        let server = Server::http(&self.address)
            .map_err(|error| format!("failed to bind dashboard to {}: {error}", self.address))?;
        eprintln!(
            "Experiment dashboard listening on http://{} (results: {})",
            self.address,
            root.display()
        );
        for request in server.incoming_requests() {
            let root = root.clone();
            std::thread::spawn(move || handle_request(request, &root));
        }
        Ok(())
    }
}

fn handle_request(request: Request, results_root: &Path) {
    if request.method() != &Method::Get {
        respond_error(request, 405, "method_not_allowed", "only GET is supported");
        return;
    }

    let url = request.url();
    let path = url.split('?').next().unwrap_or_default();
    let segments = path
        .trim_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    match segments.as_slice() {
        [] => respond_static(request, "text/html; charset=utf-8", INDEX_HTML),
        ["app.css"] => respond_static(request, "text/css; charset=utf-8", APP_CSS),
        ["app.js"] => respond_static(request, "application/javascript; charset=utf-8", APP_JS),
        ["api", "experiments"] => match list_experiments(results_root) {
            Ok(value) => respond_json(request, &value),
            Err(error) => respond_error(request, 500, "read_failed", error),
        },
        ["api", "experiments", experiment_id] => {
            match experiment_overview(results_root, experiment_id) {
                Ok(value) => respond_json(request, &value),
                Err(error) => respond_data_error(request, error),
            }
        }
        ["api", "experiments", experiment_id, "runs", run_id] => {
            match run_snapshot(results_root, experiment_id, run_id) {
                Ok(value) => respond_json(request, &value),
                Err(error) => respond_data_error(request, error),
            }
        }
        _ => respond_error(request, 404, "not_found", "dashboard endpoint not found"),
    }
}

#[derive(Debug)]
enum DashboardError {
    NotFound(String),
    Invalid(String),
    Read(String),
}

impl From<io::Error> for DashboardError {
    fn from(error: io::Error) -> Self {
        Self::Read(error.to_string())
    }
}

fn respond_data_error(request: Request, error: DashboardError) {
    match error {
        DashboardError::NotFound(message) => respond_error(request, 404, "not_found", message),
        DashboardError::Invalid(message) => {
            respond_error(request, 400, "invalid_identifier", message)
        }
        DashboardError::Read(message) => respond_error(request, 500, "read_failed", message),
    }
}

fn list_experiments(results_root: &Path) -> Result<Value, String> {
    let mut experiments = fs::read_dir(results_root)
        .map_err(|error| error.to_string())?
        .filter_map(Result::ok)
        .filter_map(|entry| {
            entry
                .file_type()
                .ok()
                .filter(|kind| kind.is_dir())
                .and_then(|_| experiment_summary(&entry.path()).ok())
        })
        .collect::<Vec<_>>();
    experiments.sort_by(|left, right| {
        json_u64(right, "started_unix_ms")
            .cmp(&json_u64(left, "started_unix_ms"))
            .then_with(|| json_string(right, "id").cmp(&json_string(left, "id")))
    });
    Ok(json!({
        "schema_version": 1,
        "generated_unix_ms": unix_timestamp_ms(),
        "experiments": experiments,
    }))
}

fn experiment_summary(directory: &Path) -> Result<Value, DashboardError> {
    let id = directory
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| DashboardError::Read("experiment directory is not UTF-8".into()))?;
    let manifest = read_json_if_exists(&directory.join("manifest.resolved.json"))
        .or_else(|| read_json_if_exists(&directory.join("experiment.json")))
        .unwrap_or_else(|| json!({}));
    let final_report = read_json_if_exists(&directory.join("experiment.json"));
    let runs = discover_runs(directory)?;
    let running_runs = runs.iter().filter(|run| run["status"] == "running").count();
    let completed_runs = runs
        .iter()
        .filter(|run| run["status"] == "completed")
        .count();
    let failed_runs = runs.iter().filter(|run| run["status"] == "failed").count();
    let plan_count = read_json_if_exists(&directory.join("plan.json"))
        .and_then(|value| value.as_array().map(Vec::len))
        .unwrap_or(runs.len());
    let status = if running_runs > 0 {
        "running"
    } else if final_report.is_some() {
        "completed"
    } else {
        "interrupted"
    };
    let started_unix_ms = final_report
        .as_ref()
        .and_then(|value| value.get("started_unix_ms"))
        .and_then(Value::as_u64)
        .or_else(|| infer_started_unix_ms(id))
        .unwrap_or_default();
    let completed_unix_ms = final_report
        .as_ref()
        .and_then(|value| value.get("completed_unix_ms"))
        .and_then(Value::as_u64);

    Ok(json!({
        "id": id,
        "name": manifest.get("name").and_then(Value::as_str).unwrap_or(id),
        "status": status,
        "started_unix_ms": started_unix_ms,
        "completed_unix_ms": completed_unix_ms,
        "planned_runs": plan_count,
        "discovered_runs": runs.len(),
        "running_runs": running_runs,
        "completed_runs": completed_runs,
        "failed_runs": failed_runs,
    }))
}

fn experiment_overview(results_root: &Path, id: &str) -> Result<Value, DashboardError> {
    let directory = child_directory(results_root, id, "experiment")?;
    let summary = experiment_summary(&directory)?;
    let manifest = read_json_if_exists(&directory.join("manifest.resolved.json"));
    let plan = read_json_if_exists(&directory.join("plan.json"));
    let runs = discover_runs(&directory)?;
    Ok(json!({
        "schema_version": 1,
        "generated_unix_ms": unix_timestamp_ms(),
        "experiment": summary,
        "manifest": manifest,
        "plan": plan,
        "runs": runs,
    }))
}

fn discover_runs(experiment_directory: &Path) -> Result<Vec<Value>, DashboardError> {
    let mut runs = fs::read_dir(experiment_directory)?
        .filter_map(Result::ok)
        .filter_map(|entry| {
            entry
                .file_type()
                .ok()
                .filter(|kind| kind.is_dir())
                .map(|_| entry.path())
        })
        .filter(|directory| directory.join("metadata.json").is_file())
        .filter_map(|directory| run_summary(&directory).ok())
        .collect::<Vec<_>>();
    runs.sort_by(|left, right| json_string(left, "run_id").cmp(&json_string(right, "run_id")));
    Ok(runs)
}

fn run_summary(directory: &Path) -> Result<Value, DashboardError> {
    let metadata = read_json(&directory.join("metadata.json"))?;
    let artifact_status = metadata
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let status = effective_run_status(directory, artifact_status);
    let request_count = count_json_lines(&directory.join("requests.jsonl"))?;
    let event_count = count_json_lines(&directory.join("events.jsonl"))?;
    let resource_count = count_json_lines(&directory.join("resource-samples.jsonl"))?;
    Ok(json!({
        "run_id": metadata.get("run_id").and_then(Value::as_str).unwrap_or_default(),
        "status": status,
        "started_unix_ms": metadata.get("started_unix_ms").and_then(Value::as_u64),
        "completed_unix_ms": metadata.get("completed_unix_ms").and_then(Value::as_u64),
        "scenario": metadata.pointer("/scenario/id").and_then(Value::as_str),
        "algorithm": metadata.get("algorithm").and_then(Value::as_str),
        "runtime": metadata.get("runtime").and_then(Value::as_str),
        "repetition": metadata.get("repetition").and_then(Value::as_u64),
        "request_count": request_count,
        "event_count": event_count,
        "resource_sample_count": resource_count,
    }))
}

fn run_snapshot(
    results_root: &Path,
    experiment_id: &str,
    run_id: &str,
) -> Result<Value, DashboardError> {
    let experiment_directory = child_directory(results_root, experiment_id, "experiment")?;
    let run_directory = child_directory(&experiment_directory, run_id, "run")?;
    if !run_directory.join("metadata.json").is_file() {
        return Err(DashboardError::NotFound(format!("run not found: {run_id}")));
    }

    let metadata = read_json(&run_directory.join("metadata.json"))?;
    let effective_status = effective_run_status(
        &run_directory,
        metadata
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("unknown"),
    );
    let requests = read_json_lines(&run_directory.join("requests.jsonl"))?;
    let resources = read_json_lines(&run_directory.join("resource-samples.jsonl"))?;
    let mut events = read_json_lines(&run_directory.join("events.jsonl"))?;
    let workload_offset_us = workload_offset_us(&metadata, &events);
    for event in &mut events {
        add_event_timeline(event, &metadata, workload_offset_us);
        compact_event(event);
    }
    let (analysis, analysis_error) = match analyze_run_snapshot(&run_directory) {
        Ok(run_analysis) => (
            serde_json::to_value(run_analysis)
                .map_err(|error| DashboardError::Read(error.to_string()))?,
            None,
        ),
        Err(error) => (Value::Null, Some(error.to_string())),
    };
    let duration_us = timeline_duration_us(
        &metadata,
        &requests,
        &resources,
        &events,
        workload_offset_us,
    );
    let run_seed = metadata
        .get("run_seed")
        .and_then(Value::as_u64)
        .map(|seed| seed.to_string());

    Ok(json!({
        "schema_version": 1,
        "generated_unix_ms": unix_timestamp_ms(),
        "experiment_id": experiment_id,
        "effective_status": effective_status,
        "run_seed": run_seed,
        "metadata": metadata,
        "analysis": analysis,
        "analysis_error": analysis_error,
        "timeline_duration_us": duration_us,
        "workload_offset_us": workload_offset_us,
        "requests": requests,
        "resource_samples": resources,
        "events": events,
        "source_artifacts": [
            "metadata.json",
            "requests.jsonl",
            "events.jsonl",
            "resource-samples.jsonl",
            "metrics-before.json",
            "metrics-after.json"
        ]
    }))
}

fn child_directory(root: &Path, id: &str, kind: &str) -> Result<PathBuf, DashboardError> {
    if id.is_empty()
        || id == "."
        || id == ".."
        || id.contains(['/', '\\'])
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(DashboardError::Invalid(format!(
            "invalid {kind} identifier"
        )));
    }
    let path = root.join(id);
    if !path.is_dir() {
        return Err(DashboardError::NotFound(format!("{kind} not found: {id}")));
    }
    Ok(path)
}

fn read_json(path: &Path) -> Result<Value, DashboardError> {
    let bytes = fs::read(path).map_err(|error| {
        DashboardError::Read(format!("failed to read {}: {error}", path.display()))
    })?;
    serde_json::from_slice(&bytes).map_err(|error| {
        DashboardError::Read(format!("failed to parse {}: {error}", path.display()))
    })
}

fn read_json_if_exists(path: &Path) -> Option<Value> {
    path.is_file().then(|| read_json(path).ok()).flatten()
}

fn read_json_lines(path: &Path) -> Result<Vec<Value>, DashboardError> {
    if !path.is_file() {
        return Ok(Vec::new());
    }
    let bytes = fs::read(path)?;
    let has_complete_tail = bytes.ends_with(b"\n");
    let mut values = Vec::new();
    let lines = bytes.split(|byte| *byte == b'\n').collect::<Vec<_>>();
    for (index, raw_line) in lines.iter().enumerate() {
        let line = String::from_utf8_lossy(raw_line);
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str(&line) {
            Ok(value) => values.push(value),
            Err(_) if !has_complete_tail && index + 1 == lines.len() => {}
            Err(error) => {
                return Err(DashboardError::Read(format!(
                    "failed to parse {} line {}: {error}",
                    path.display(),
                    index + 1
                )));
            }
        }
    }
    Ok(values)
}

fn count_json_lines(path: &Path) -> Result<usize, DashboardError> {
    Ok(read_json_lines(path)?.len())
}

fn compact_event(event: &mut Value) {
    let Some(details) = event.get_mut("details").and_then(Value::as_object_mut) else {
        return;
    };
    details.remove("stdout");
    details.remove("stderr");
    if let Some(command) = details.get("command").and_then(Value::as_str) {
        if command.chars().count() > 240 {
            let command = format!("{}…", command.chars().take(240).collect::<String>());
            details.insert("command".into(), Value::String(command));
        }
    }
}

fn timeline_duration_us(
    metadata: &Value,
    requests: &[Value],
    resources: &[Value],
    events: &[Value],
    workload_offset_us: u64,
) -> u64 {
    let request_end = requests
        .iter()
        .map(|request| {
            workload_offset_us
                .saturating_add(json_u64(request, "started_offset_us"))
                .saturating_add(json_u64(request, "latency_us"))
        })
        .max()
        .unwrap_or_default();
    let resource_end = resources
        .iter()
        .map(|sample| workload_offset_us.saturating_add(json_u64(sample, "elapsed_us")))
        .max()
        .unwrap_or_default();
    let event_end = events
        .iter()
        .filter_map(|event| event.get("timeline_us").and_then(Value::as_u64))
        .max()
        .unwrap_or_default();
    let metadata_duration = metadata
        .get("duration_ms")
        .and_then(Value::as_u64)
        .unwrap_or_default()
        .saturating_mul(1_000);
    request_end
        .max(resource_end)
        .max(event_end)
        .max(metadata_duration)
}

fn workload_offset_us(metadata: &Value, events: &[Value]) -> u64 {
    let run_started_us = metadata
        .get("started_unix_ms")
        .and_then(Value::as_u64)
        .unwrap_or_default()
        .saturating_mul(1_000);
    events
        .iter()
        .find(|event| event.get("event").and_then(Value::as_str) == Some("workloads_started"))
        .and_then(|event| {
            let timestamp_us = event
                .get("timestamp_unix_ms")
                .and_then(Value::as_u64)?
                .saturating_mul(1_000);
            let elapsed_us = event
                .get("elapsed_us")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            Some(
                timestamp_us
                    .saturating_sub(run_started_us)
                    .saturating_sub(elapsed_us),
            )
        })
        .unwrap_or_default()
}

fn add_event_timeline(event: &mut Value, metadata: &Value, workload_offset_us: u64) {
    let run_started_us = metadata
        .get("started_unix_ms")
        .and_then(Value::as_u64)
        .unwrap_or_default()
        .saturating_mul(1_000);
    let timeline_us = event
        .get("elapsed_us")
        .and_then(Value::as_u64)
        .map(|elapsed| workload_offset_us.saturating_add(elapsed))
        .or_else(|| {
            event
                .get("timestamp_unix_ms")
                .and_then(Value::as_u64)
                .map(|timestamp| {
                    timestamp
                        .saturating_mul(1_000)
                        .saturating_sub(run_started_us)
                })
        })
        .unwrap_or_default();
    if let Some(object) = event.as_object_mut() {
        object.insert("timeline_us".into(), Value::from(timeline_us));
    }
}

fn infer_started_unix_ms(id: &str) -> Option<u64> {
    id.rsplit_once('-')?.1.parse().ok()
}

fn effective_run_status(directory: &Path, artifact_status: &str) -> String {
    if artifact_status != "running" || has_recent_activity(directory) {
        artifact_status.to_string()
    } else {
        "interrupted".into()
    }
}

fn has_recent_activity(directory: &Path) -> bool {
    const LIVE_ACTIVITY_WINDOW_MS: u64 = 15 * 60 * 1_000;
    let latest = [
        "metadata.json",
        "events.jsonl",
        "requests.jsonl",
        "resource-samples.jsonl",
        "server.stdout.log",
        "server.stderr.log",
    ]
    .iter()
    .filter_map(|name| fs::metadata(directory.join(name)).ok())
    .filter_map(|metadata| metadata.modified().ok())
    .filter_map(|modified| modified.duration_since(UNIX_EPOCH).ok())
    .map(|duration| duration.as_millis() as u64)
    .max();
    latest.is_some_and(|modified| {
        unix_timestamp_ms().saturating_sub(modified) <= LIVE_ACTIVITY_WINDOW_MS
    })
}

fn json_u64(value: &Value, key: &str) -> u64 {
    value.get(key).and_then(Value::as_u64).unwrap_or_default()
}

fn json_string<'a>(value: &'a Value, key: &str) -> &'a str {
    value.get(key).and_then(Value::as_str).unwrap_or_default()
}

fn unix_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn respond_json(request: Request, value: &impl Serialize) {
    match serde_json::to_string(value) {
        Ok(body) => respond(request, 200, "application/json; charset=utf-8", body),
        Err(error) => respond_error(request, 500, "serialization_failed", error.to_string()),
    }
}

fn respond_static(request: Request, content_type: &str, body: &str) {
    respond(request, 200, content_type, body.to_string());
}

fn respond(request: Request, status: u16, content_type: &str, body: String) {
    let response = Response::from_string(body)
        .with_status_code(StatusCode(status))
        .with_header(Header::from_bytes("Content-Type", content_type).unwrap())
        .with_header(Header::from_bytes("Cache-Control", "no-store").unwrap())
        .with_header(Header::from_bytes("X-Content-Type-Options", "nosniff").unwrap());
    let _ = request.respond(response);
}

fn respond_error(request: Request, status: u16, code: &str, message: impl Into<String>) {
    let mut error = Map::new();
    error.insert("code".into(), Value::String(code.into()));
    error.insert("message".into(), Value::String(message.into()));
    respond(
        request,
        status,
        "application/json; charset=utf-8",
        json!({"error": error}).to_string(),
    );
}

#[cfg(test)]
mod tests {
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
        let mut event =
            json!({"details": {"command": "run", "stdout": "large", "stderr": "large"}});
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
}
