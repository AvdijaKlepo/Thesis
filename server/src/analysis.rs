use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt::{Display, Formatter, Write as FmtWrite},
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

const ANALYSIS_SCHEMA_VERSION: u32 = 2;
const STABLE_RECOVERY_SUCCESSES: usize = 5;

#[derive(Debug)]
pub struct AnalysisError(String);

impl AnalysisError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl Display for AnalysisError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for AnalysisError {}

impl From<std::io::Error> for AnalysisError {
    fn from(value: std::io::Error) -> Self {
        Self(value.to_string())
    }
}

impl From<serde_json::Error> for AnalysisError {
    fn from(value: serde_json::Error) -> Self {
        Self(value.to_string())
    }
}

#[derive(Clone, Debug, Default)]
pub struct AnalysisOptions {
    pub output_directory: Option<PathBuf>,
    pub include_failed: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct AnalysisReport {
    pub schema_version: u32,
    pub analyzer_version: &'static str,
    pub generated_unix_ms: u64,
    pub experiment_directory: PathBuf,
    pub analysis_directory: PathBuf,
    pub include_failed_runs_in_aggregates: bool,
    pub methodology: Methodology,
    pub source_artifacts: Vec<SourceArtifact>,
    pub runs: Vec<RunAnalysis>,
    pub groups: Vec<GroupAnalysis>,
    pub group_windows: Vec<GroupWindowAnalysis>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Methodology {
    pub throughput: &'static str,
    pub errors: &'static str,
    pub latency_percentiles: &'static str,
    pub slo: &'static str,
    pub scheduling_lag: &'static str,
    pub fairness: &'static str,
    pub resource_usage: &'static str,
    pub windows: &'static str,
    pub environment_limits: &'static str,
    pub recovery_time: &'static str,
    pub aggregates: &'static str,
}

#[derive(Clone, Debug, Serialize)]
pub struct SourceArtifact {
    pub relative_path: String,
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct RunAnalysis {
    pub run_id: String,
    pub scenario: String,
    pub algorithm: String,
    pub runtime: String,
    pub repetition: usize,
    pub run_seed: u64,
    pub slo_target_ms: Option<u64>,
    pub run_status: String,
    pub included_in_aggregates: bool,
    pub raw_directory: PathBuf,
    pub measurement_duration_s: Option<f64>,
    pub total_requests: usize,
    pub successful_requests: usize,
    pub transport_errors: usize,
    pub http_errors: usize,
    pub error_rate: Option<f64>,
    pub throughput_requests_per_s: Option<f64>,
    pub successful_throughput_requests_per_s: Option<f64>,
    pub latency: LatencyAnalysis,
    pub slo: SloAnalysis,
    pub scheduling_lag: SchedulingLagAnalysis,
    pub fairness: FairnessAnalysis,
    pub resource_usage: ResourceAnalysis,
    pub workloads: Vec<WorkloadAnalysis>,
    pub windows: Vec<WindowAnalysis>,
    pub environment_limited: bool,
    pub recoveries: Vec<RecoveryAnalysis>,
    pub warnings: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct SloAnalysis {
    pub target_ms: Option<u64>,
    pub eligible_requests: Option<usize>,
    pub successful_within_slo: Option<usize>,
    pub miss_count: Option<usize>,
    pub attainment_rate: Option<f64>,
    pub miss_rate: Option<f64>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct SchedulingLagAnalysis {
    pub samples: usize,
    pub mean_us: Option<f64>,
    pub p50_us: Option<f64>,
    pub p95_us: Option<f64>,
    pub p99_us: Option<f64>,
    pub max_us: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct WorkloadAnalysis {
    pub workload_id: String,
    pub total_requests: usize,
    pub successful_requests: usize,
    pub transport_errors: usize,
    pub http_errors: usize,
    pub error_rate: Option<f64>,
    pub latency: LatencyAnalysis,
    pub slo: SloAnalysis,
    pub scheduling_lag: SchedulingLagAnalysis,
}

#[derive(Clone, Debug, Serialize)]
pub struct WindowAnalysis {
    pub window_id: String,
    pub start_ms: u64,
    pub end_ms: u64,
    pub total_requests: usize,
    pub successful_requests: usize,
    pub latency: LatencyAnalysis,
    pub backend_requests: BTreeMap<String, u64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct GroupWindowAnalysis {
    pub scenario: String,
    pub algorithm: String,
    pub runtime: String,
    pub window_id: String,
    pub start_ms: u64,
    pub end_ms: u64,
    pub runs: usize,
    pub total_requests: StatisticalSummary,
    pub successful_requests: StatisticalSummary,
    pub latency_p95_us: StatisticalSummary,
    pub backend_requests: BTreeMap<String, u64>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct LatencyAnalysis {
    pub samples: usize,
    pub mean_us: Option<f64>,
    pub min_us: Option<u64>,
    pub p50_us: Option<f64>,
    pub p90_us: Option<f64>,
    pub p95_us: Option<f64>,
    pub p99_us: Option<f64>,
    pub p999_us: Option<f64>,
    pub max_us: Option<u64>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct FairnessAnalysis {
    pub backend_count: usize,
    pub jain_index: Option<f64>,
    pub weighted_jain_index: Option<f64>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ResourceAnalysis {
    pub samples: usize,
    pub average_cpu_percent: Option<f64>,
    pub cpu_time_ms: Option<f64>,
    pub mean_resident_memory_bytes: Option<f64>,
    pub peak_resident_memory_bytes: Option<u64>,
    pub mean_virtual_memory_bytes: Option<f64>,
    pub peak_virtual_memory_bytes: Option<u64>,
    pub host_samples: usize,
    pub host_average_cpu_percent: Option<f64>,
    pub host_mean_memory_bytes: Option<f64>,
    pub host_peak_memory_bytes: Option<u64>,
    pub host_metrics_warning: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct RecoveryAnalysis {
    pub action: String,
    pub target_backend_id: Option<String>,
    pub action_started_offset_ms: Option<f64>,
    pub action_completed_offset_ms: f64,
    pub first_success_offset_ms: Option<f64>,
    pub time_to_first_success_ms: Option<f64>,
    pub stable_success_offset_ms: Option<f64>,
    pub time_to_stable_success_ms: Option<f64>,
    pub stable_successes_required: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct BackendAnalysis {
    pub run_id: String,
    pub scenario: String,
    pub algorithm: String,
    pub runtime: String,
    pub service_id: String,
    pub backend_id: String,
    pub weight: u64,
    pub requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub normalized_load: Option<f64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct GroupAnalysis {
    pub scenario: String,
    pub algorithm: String,
    pub runtime: String,
    pub runs: usize,
    pub throughput_requests_per_s: StatisticalSummary,
    pub successful_throughput_requests_per_s: StatisticalSummary,
    pub error_rate: StatisticalSummary,
    pub latency_p50_us: StatisticalSummary,
    pub latency_p95_us: StatisticalSummary,
    pub latency_p99_us: StatisticalSummary,
    pub slo_attainment_rate: StatisticalSummary,
    pub slo_miss_rate: StatisticalSummary,
    pub scheduling_lag_p95_us: StatisticalSummary,
    pub fairness_jain_index: StatisticalSummary,
    pub weighted_fairness_jain_index: StatisticalSummary,
    pub average_cpu_percent: StatisticalSummary,
    pub peak_resident_memory_bytes: StatisticalSummary,
    pub recovery_time_ms: StatisticalSummary,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct StatisticalSummary {
    pub samples: usize,
    pub mean: Option<f64>,
    pub standard_deviation: Option<f64>,
    pub confidence_interval_95_low: Option<f64>,
    pub confidence_interval_95_high: Option<f64>,
    pub min: Option<f64>,
    pub max: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct MetadataInput {
    run_id: String,
    status: String,
    scenario: ScenarioInput,
    algorithm: String,
    runtime: String,
    repetition: usize,
    run_seed: u64,
}

#[derive(Debug, Deserialize)]
struct ScenarioInput {
    id: String,
    #[serde(default)]
    slo_target_ms: Option<u64>,
    #[serde(default)]
    analysis_windows: Vec<AnalysisWindowInput>,
}

#[derive(Debug, Deserialize)]
struct AnalysisWindowInput {
    id: String,
    start_ms: u64,
    end_ms: u64,
}

#[derive(Clone, Debug, Deserialize)]
struct RequestInput {
    #[serde(default)]
    workload_id: String,
    #[serde(default)]
    scheduled_offset_us: u64,
    started_offset_us: u64,
    latency_us: u64,
    transport_success: bool,
    http_success: bool,
    #[serde(default)]
    backend_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct EventInput {
    event: String,
    label: String,
    success: Option<bool>,
    elapsed_us: Option<u64>,
    #[serde(default)]
    details: Value,
}

#[derive(Debug, Deserialize)]
struct ResourceInput {
    elapsed_us: u64,
    cpu_time_us: Option<u64>,
    resident_memory_bytes: Option<u64>,
    virtual_memory_bytes: Option<u64>,
    #[serde(default)]
    host_cpu_percent: Option<f64>,
    #[serde(default)]
    host_memory_bytes: Option<u64>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct GroupKey {
    scenario: String,
    algorithm: String,
    runtime: String,
}

pub fn analyze_experiment(
    experiment_directory: &Path,
    options: &AnalysisOptions,
) -> Result<AnalysisReport, AnalysisError> {
    if !experiment_directory.is_dir() {
        return Err(AnalysisError::new(format!(
            "experiment directory does not exist: {}",
            experiment_directory.display()
        )));
    }
    let experiment_directory = fs::canonicalize(experiment_directory).map_err(|error| {
        AnalysisError::new(format!(
            "failed to resolve experiment directory {}: {error}",
            experiment_directory.display()
        ))
    })?;
    let analysis_directory = match &options.output_directory {
        Some(path) if path.is_absolute() => path.clone(),
        Some(path) => std::env::current_dir()?.join(path),
        None => experiment_directory.join("analysis"),
    };
    let normalized_output = absolute_without_requiring_existence(&analysis_directory)?;
    if normalized_output == experiment_directory {
        return Err(AnalysisError::new(
            "analysis output must not overwrite the experiment directory",
        ));
    }

    let run_directories = discover_run_directories(&experiment_directory)?;
    if run_directories.is_empty() {
        return Err(AnalysisError::new(format!(
            "no run directories containing metadata.json found in {}",
            experiment_directory.display()
        )));
    }
    if run_directories
        .iter()
        .any(|run_directory| normalized_output.starts_with(run_directory))
    {
        return Err(AnalysisError::new(
            "analysis output must be separate from every raw run directory",
        ));
    }

    let mut runs = Vec::with_capacity(run_directories.len());
    let mut backends = Vec::new();
    for run_directory in &run_directories {
        let (run, mut run_backends) = analyze_run(run_directory, options.include_failed)?;
        runs.push(run);
        backends.append(&mut run_backends);
    }
    runs.sort_by(|left, right| left.run_id.cmp(&right.run_id));
    apply_environment_limits(&mut runs);
    backends.sort_by(|left, right| {
        left.run_id
            .cmp(&right.run_id)
            .then_with(|| left.service_id.cmp(&right.service_id))
            .then_with(|| left.backend_id.cmp(&right.backend_id))
    });
    let groups = aggregate_runs(&runs);
    let group_windows = aggregate_windows(&runs);
    let source_artifacts =
        inventory_sources(&experiment_directory, &run_directories, &normalized_output)?;
    let report = AnalysisReport {
        schema_version: ANALYSIS_SCHEMA_VERSION,
        analyzer_version: env!("CARGO_PKG_VERSION"),
        generated_unix_ms: unix_timestamp_ms(),
        experiment_directory,
        analysis_directory: normalized_output,
        include_failed_runs_in_aggregates: options.include_failed,
        methodology: methodology(),
        source_artifacts,
        runs,
        groups,
        group_windows,
    };
    write_outputs(&report, &backends)?;
    Ok(report)
}

/// Derives the same run-level metrics used by [`analyze_experiment`] without
/// writing analysis artifacts. This is used by the dashboard for both active
/// runs and saved-run replay.
pub fn analyze_run_snapshot(run_directory: &Path) -> Result<RunAnalysis, AnalysisError> {
    analyze_run(run_directory, true).map(|(run, _)| run)
}

fn methodology() -> Methodology {
    Methodology {
        throughput: "request completions divided by the interval from the first request start to the last request completion; successful throughput uses HTTP-successful completions",
        errors: "transport errors are failed client/proxy exchanges; HTTP errors are transport-successful responses outside 200-399; error rate is their sum divided by all requests",
        latency_percentiles: "end-to-end latency for all generated requests, including failures, using linear interpolation between sorted samples (R-7 / NumPy default quantile method)",
        slo: "when slo_target_ms is declared, attainment counts HTTP-successful requests with latency_us <= slo_target_ms * 1,000; misses are all other requests, rates use all generated requests as the denominator, and legacy runs without a target retain null derived fields",
        scheduling_lag: "request scheduling lag is started_offset_us - scheduled_offset_us in microseconds; p50, p95, p99, and maximum use the same linear interpolation as latency",
        fairness: "Jain's fairness index over per-backend request-attempt deltas; weighted fairness applies the index to requests divided by configured backend weight",
        resource_usage: "100 ms samples retain separate proxy-process CPU/memory and whole-host CPU/memory values; unsupported host metrics remain null",
        windows: "named half-open windows [start_ms, end_ms) select requests by started_offset_us / 1,000 and report allocation and latency without changing run-level totals",
        environment_limits: "a run is flagged when host average CPU is at least 80%, or when scheduling-lag p95 exceeds 5 ms and is more than 25% above the matching three-backend control; flagged runs remain in raw results and aggregates but receive an explicit warning",
        recovery_time: "elapsed time from a recovery action's start to the first sequence of five HTTP-successful completions from its explicitly targeted backend; actions without an explicit target report no recovery latency",
        aggregates: "arithmetic mean, sample standard deviation, and a two-sided Student's t 95% confidence interval across eligible repetitions",
    }
}

fn discover_run_directories(experiment_directory: &Path) -> Result<Vec<PathBuf>, AnalysisError> {
    let mut directories = fs::read_dir(experiment_directory)?
        .filter_map(Result::ok)
        .filter_map(|entry| {
            entry
                .file_type()
                .ok()
                .filter(|file_type| file_type.is_dir())
                .map(|_| entry.path())
        })
        .filter(|path| path.join("metadata.json").is_file())
        .collect::<Vec<_>>();
    directories.sort();
    Ok(directories)
}

fn analyze_run(
    run_directory: &Path,
    include_failed: bool,
) -> Result<(RunAnalysis, Vec<BackendAnalysis>), AnalysisError> {
    let metadata: MetadataInput = read_json(run_directory.join("metadata.json"))?;
    let request_path = run_directory.join("requests.jsonl");
    let requests: Vec<RequestInput> = if request_path.is_file() {
        read_json_lines(&request_path)?
    } else {
        Vec::new()
    };
    let mut warnings = Vec::new();
    if requests.is_empty() {
        warnings.push("requests.jsonl is missing or contains no request measurements".into());
    }
    let included_in_aggregates =
        !requests.is_empty() && (metadata.status == "completed" || include_failed);
    if metadata.status != "completed" {
        warnings.push(if include_failed && !requests.is_empty() {
            format!(
                "run status is '{}'; metrics are included in aggregates because --include-failed was used",
                metadata.status
            )
        } else {
            format!(
                "run status is '{}'; metrics are excluded from aggregates unless --include-failed is used",
                metadata.status
            )
        });
    }

    let total_requests = requests.len();
    let successful_requests = requests
        .iter()
        .filter(|request| request.http_success)
        .count();
    let transport_errors = requests
        .iter()
        .filter(|request| !request.transport_success)
        .count();
    let http_errors = requests
        .iter()
        .filter(|request| request.transport_success && !request.http_success)
        .count();
    let measurement_duration_s = measurement_duration_seconds(&requests);
    let throughput_requests_per_s = measurement_duration_s
        .filter(|duration| *duration > 0.0)
        .map(|duration| total_requests as f64 / duration);
    let successful_throughput_requests_per_s = measurement_duration_s
        .filter(|duration| *duration > 0.0)
        .map(|duration| successful_requests as f64 / duration);
    let error_rate = (total_requests > 0)
        .then_some((transport_errors + http_errors) as f64 / total_requests as f64);
    let latency = latency_analysis(&requests);
    let slo = slo_analysis(&requests, metadata.scenario.slo_target_ms);
    let scheduling_lag = scheduling_lag_analysis(&requests);
    let workloads = workload_analysis(&requests, metadata.scenario.slo_target_ms);
    let windows = window_analysis(&requests, &metadata.scenario.analysis_windows);

    let (fairness, backends) = backend_analysis(run_directory, &metadata, &requests)?;
    if fairness.jain_index.is_none() {
        warnings.push("backend load data is unavailable; fairness could not be calculated".into());
    }
    let resource_usage = resource_analysis(run_directory)?;
    if resource_usage.samples == 0 {
        warnings.push(
            "resource-samples.jsonl is unavailable; resource metrics are null for this legacy or partial run"
                .into(),
        );
    }
    if let Some(host_warning) = &resource_usage.host_metrics_warning {
        warnings.push(format!(
            "whole-host resource metrics are unavailable: {host_warning}"
        ));
    }
    let recoveries = recovery_analysis(run_directory, &requests)?;
    let environment_limited = resource_usage
        .host_average_cpu_percent
        .is_some_and(|cpu| cpu >= 80.0);
    if environment_limited {
        warnings.push(
            "run is environment-limited by host CPU; exclude it from policy claims unless analyzed as a saturation study"
                .into(),
        );
    }

    Ok((
        RunAnalysis {
            run_id: metadata.run_id,
            scenario: metadata.scenario.id,
            algorithm: metadata.algorithm,
            runtime: metadata.runtime,
            repetition: metadata.repetition,
            run_seed: metadata.run_seed,
            slo_target_ms: metadata.scenario.slo_target_ms,
            run_status: metadata.status,
            included_in_aggregates,
            raw_directory: run_directory.to_path_buf(),
            measurement_duration_s,
            total_requests,
            successful_requests,
            transport_errors,
            http_errors,
            error_rate,
            throughput_requests_per_s,
            successful_throughput_requests_per_s,
            latency,
            slo,
            scheduling_lag,
            fairness,
            resource_usage,
            workloads,
            windows,
            environment_limited,
            recoveries,
            warnings,
        },
        backends,
    ))
}

fn apply_environment_limits(runs: &mut [RunAnalysis]) {
    let mut control_lags = BTreeMap::<(String, String), Vec<f64>>::new();
    for run in runs.iter() {
        if run.fairness.backend_count == 3 {
            if let Some(lag) = run.scheduling_lag.p95_us {
                control_lags
                    .entry((run.algorithm.clone(), run.runtime.clone()))
                    .or_default()
                    .push(lag);
            }
        }
    }
    let control_lags = control_lags
        .into_iter()
        .filter_map(|(key, values)| {
            (!values.is_empty()).then_some((key, values.iter().sum::<f64>() / values.len() as f64))
        })
        .collect::<BTreeMap<_, _>>();

    for run in runs {
        let host_limited = run
            .resource_usage
            .host_average_cpu_percent
            .is_some_and(|cpu| cpu >= 80.0);
        let lag_limited = run.scheduling_lag.p95_us.is_some_and(|lag| {
            lag > 5_000.0
                && control_lags
                    .get(&(run.algorithm.clone(), run.runtime.clone()))
                    .is_some_and(|control| lag > *control * 1.25)
        });
        run.environment_limited = host_limited || lag_limited;
        if lag_limited && !host_limited {
            run.warnings.push(
                "run is environment-limited by scheduling lag relative to the three-backend control; exclude it from policy claims unless analyzed as a saturation study".into(),
            );
        }
    }
}

fn measurement_duration_seconds(requests: &[RequestInput]) -> Option<f64> {
    let started = requests
        .iter()
        .map(|request| request.started_offset_us)
        .min()?;
    let completed = requests
        .iter()
        .map(|request| request.started_offset_us.saturating_add(request.latency_us))
        .max()?;
    (completed > started).then_some((completed - started) as f64 / 1_000_000.0)
}

fn latency_analysis(requests: &[RequestInput]) -> LatencyAnalysis {
    let mut values = requests
        .iter()
        .map(|request| request.latency_us)
        .collect::<Vec<_>>();
    if values.is_empty() {
        return LatencyAnalysis::default();
    }
    values.sort_unstable();
    LatencyAnalysis {
        samples: values.len(),
        mean_us: Some(values.iter().map(|value| *value as f64).sum::<f64>() / values.len() as f64),
        min_us: values.first().copied(),
        p50_us: percentile(&values, 0.50),
        p90_us: percentile(&values, 0.90),
        p95_us: percentile(&values, 0.95),
        p99_us: percentile(&values, 0.99),
        p999_us: percentile(&values, 0.999),
        max_us: values.last().copied(),
    }
}

fn slo_analysis(requests: &[RequestInput], target_ms: Option<u64>) -> SloAnalysis {
    let Some(target_ms) = target_ms else {
        return SloAnalysis::default();
    };
    let target_us = target_ms.saturating_mul(1_000);
    let successful_within_slo = requests
        .iter()
        .filter(|request| request.http_success && request.latency_us <= target_us)
        .count();
    let miss_count = requests.len().saturating_sub(successful_within_slo);
    let attainment_rate =
        (!requests.is_empty()).then_some(successful_within_slo as f64 / requests.len() as f64);
    SloAnalysis {
        target_ms: Some(target_ms),
        eligible_requests: Some(requests.len()),
        successful_within_slo: Some(successful_within_slo),
        miss_count: Some(miss_count),
        attainment_rate,
        miss_rate: attainment_rate.map(|rate| 1.0 - rate),
    }
}

fn scheduling_lag_analysis(requests: &[RequestInput]) -> SchedulingLagAnalysis {
    let mut values = requests
        .iter()
        .map(|request| {
            request
                .started_offset_us
                .saturating_sub(request.scheduled_offset_us)
        })
        .collect::<Vec<_>>();
    if values.is_empty() {
        return SchedulingLagAnalysis::default();
    }
    values.sort_unstable();
    SchedulingLagAnalysis {
        samples: values.len(),
        mean_us: Some(values.iter().map(|value| *value as f64).sum::<f64>() / values.len() as f64),
        p50_us: percentile(&values, 0.50),
        p95_us: percentile(&values, 0.95),
        p99_us: percentile(&values, 0.99),
        max_us: values.last().copied(),
    }
}

fn workload_analysis(
    requests: &[RequestInput],
    slo_target_ms: Option<u64>,
) -> Vec<WorkloadAnalysis> {
    let mut grouped = BTreeMap::<String, Vec<RequestInput>>::new();
    for request in requests {
        grouped
            .entry(request.workload_id.clone())
            .or_default()
            .push(request.clone());
    }
    grouped
        .into_iter()
        .map(|(workload_id, requests)| {
            let total_requests = requests.len();
            let successful_requests = requests
                .iter()
                .filter(|request| request.http_success)
                .count();
            let transport_errors = requests
                .iter()
                .filter(|request| !request.transport_success)
                .count();
            let http_errors = requests
                .iter()
                .filter(|request| request.transport_success && !request.http_success)
                .count();
            WorkloadAnalysis {
                workload_id,
                total_requests,
                successful_requests,
                transport_errors,
                http_errors,
                error_rate: (total_requests > 0)
                    .then_some((transport_errors + http_errors) as f64 / total_requests as f64),
                latency: latency_analysis(&requests),
                slo: slo_analysis(&requests, slo_target_ms),
                scheduling_lag: scheduling_lag_analysis(&requests),
            }
        })
        .collect()
}

fn window_analysis(
    requests: &[RequestInput],
    windows: &[AnalysisWindowInput],
) -> Vec<WindowAnalysis> {
    windows
        .iter()
        .map(|window| {
            let selected = requests
                .iter()
                .filter(|request| {
                    let started_ms = request.started_offset_us / 1_000;
                    started_ms >= window.start_ms && started_ms < window.end_ms
                })
                .collect::<Vec<_>>();
            let mut backend_requests = BTreeMap::new();
            for request in &selected {
                if let Some(backend_id) = &request.backend_id {
                    *backend_requests.entry(backend_id.clone()).or_insert(0) += 1;
                }
            }
            WindowAnalysis {
                window_id: window.id.clone(),
                start_ms: window.start_ms,
                end_ms: window.end_ms,
                total_requests: selected.len(),
                successful_requests: selected
                    .iter()
                    .filter(|request| request.http_success)
                    .count(),
                latency: latency_analysis(
                    &selected
                        .iter()
                        .map(|request| (*request).clone())
                        .collect::<Vec<_>>(),
                ),
                backend_requests,
            }
        })
        .collect()
}

fn percentile(sorted: &[u64], probability: f64) -> Option<f64> {
    if sorted.is_empty() || !(0.0..=1.0).contains(&probability) {
        return None;
    }
    let position = probability * (sorted.len() - 1) as f64;
    let lower = position.floor() as usize;
    let upper = position.ceil() as usize;
    let fraction = position - lower as f64;
    Some(sorted[lower] as f64 + (sorted[upper] as f64 - sorted[lower] as f64) * fraction)
}

fn backend_analysis(
    run_directory: &Path,
    metadata: &MetadataInput,
    requests: &[RequestInput],
) -> Result<(FairnessAnalysis, Vec<BackendAnalysis>), AnalysisError> {
    let before_path = run_directory.join("metrics-before.json");
    let after_path = run_directory.join("metrics-after.json");
    let loads = if before_path.is_file() && after_path.is_file() {
        let before: Value = read_json(before_path)?;
        let after: Value = read_json(after_path)?;
        backend_load_deltas(&before, &after)
    } else {
        Vec::new()
    };
    let loads = if loads.is_empty() {
        let mut response_counts = BTreeMap::<String, u64>::new();
        for backend_id in requests
            .iter()
            .filter_map(|request| request.backend_id.as_ref())
        {
            *response_counts.entry(backend_id.clone()).or_default() += 1;
        }
        response_counts
            .into_iter()
            .map(|(backend_id, requests)| BackendLoad {
                service_id: "response_headers".into(),
                backend_id,
                weight: 1,
                requests,
                successful_requests: requests,
                failed_requests: 0,
            })
            .collect()
    } else {
        loads
    };

    let values = loads
        .iter()
        .map(|backend| backend.requests as f64)
        .collect::<Vec<_>>();
    let weighted_values = loads
        .iter()
        .filter(|backend| backend.weight > 0)
        .map(|backend| backend.requests as f64 / backend.weight as f64)
        .collect::<Vec<_>>();
    let fairness = FairnessAnalysis {
        backend_count: loads.len(),
        jain_index: jain_index(&values),
        weighted_jain_index: jain_index(&weighted_values),
    };
    let rows = loads
        .into_iter()
        .map(|backend| BackendAnalysis {
            run_id: metadata.run_id.clone(),
            scenario: metadata.scenario.id.clone(),
            algorithm: metadata.algorithm.clone(),
            runtime: metadata.runtime.clone(),
            service_id: backend.service_id,
            backend_id: backend.backend_id,
            weight: backend.weight,
            requests: backend.requests,
            successful_requests: backend.successful_requests,
            failed_requests: backend.failed_requests,
            normalized_load: (backend.weight > 0)
                .then_some(backend.requests as f64 / backend.weight as f64),
        })
        .collect();
    Ok((fairness, rows))
}

#[derive(Debug)]
struct BackendLoad {
    service_id: String,
    backend_id: String,
    weight: u64,
    requests: u64,
    successful_requests: u64,
    failed_requests: u64,
}

fn backend_load_deltas(before: &Value, after: &Value) -> Vec<BackendLoad> {
    let mut before_metrics = BTreeMap::<(String, String), (u64, u64, u64)>::new();
    for service in before
        .get("services")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let service_id = service
            .get("service_id")
            .and_then(Value::as_str)
            .unwrap_or("");
        for backend in service
            .get("backends")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let backend_id = backend.get("id").and_then(Value::as_str).unwrap_or("");
            let metrics = backend.get("metrics").unwrap_or(&Value::Null);
            before_metrics.insert(
                (service_id.into(), backend_id.into()),
                (
                    json_u64(metrics, "total_requests"),
                    json_u64(metrics, "successful_requests"),
                    json_u64(metrics, "failed_requests"),
                ),
            );
        }
    }

    let mut loads = Vec::new();
    for service in after
        .get("services")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let service_id = service
            .get("service_id")
            .and_then(Value::as_str)
            .unwrap_or("");
        for backend in service
            .get("backends")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let backend_id = backend.get("id").and_then(Value::as_str).unwrap_or("");
            if backend_id.is_empty() {
                continue;
            }
            let metrics = backend.get("metrics").unwrap_or(&Value::Null);
            let baseline = before_metrics
                .get(&(service_id.into(), backend_id.into()))
                .copied()
                .unwrap_or_default();
            loads.push(BackendLoad {
                service_id: service_id.into(),
                backend_id: backend_id.into(),
                weight: backend.get("weight").and_then(Value::as_u64).unwrap_or(1),
                requests: json_u64(metrics, "total_requests").saturating_sub(baseline.0),
                successful_requests: json_u64(metrics, "successful_requests")
                    .saturating_sub(baseline.1),
                failed_requests: json_u64(metrics, "failed_requests").saturating_sub(baseline.2),
            });
        }
    }
    loads
}

fn json_u64(value: &Value, key: &str) -> u64 {
    value.get(key).and_then(Value::as_u64).unwrap_or_default()
}

fn jain_index(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let sum = values.iter().sum::<f64>();
    let squares = values.iter().map(|value| value * value).sum::<f64>();
    (squares > 0.0).then_some(sum * sum / (values.len() as f64 * squares))
}

fn resource_analysis(run_directory: &Path) -> Result<ResourceAnalysis, AnalysisError> {
    let path = run_directory.join("resource-samples.jsonl");
    if !path.is_file() {
        return Ok(ResourceAnalysis::default());
    }
    let samples: Vec<ResourceInput> = read_json_lines(&path)?;
    let host_metrics_warning = samples.iter().find_map(|sample| {
        sample
            .error
            .as_deref()
            .and_then(|error| error.strip_prefix("host: "))
            .map(str::to_string)
    });
    let valid = samples
        .iter()
        .filter(|sample| {
            sample.cpu_time_us.is_some()
                || sample.resident_memory_bytes.is_some()
                || sample.virtual_memory_bytes.is_some()
                || sample.host_cpu_percent.is_some()
                || sample.host_memory_bytes.is_some()
        })
        .collect::<Vec<_>>();
    if valid.is_empty() {
        return Ok(ResourceAnalysis {
            host_metrics_warning,
            ..ResourceAnalysis::default()
        });
    }
    let cpu_samples = valid
        .iter()
        .filter_map(|sample| sample.cpu_time_us.map(|cpu| (sample.elapsed_us, cpu)))
        .collect::<Vec<_>>();
    let (cpu_time_ms, average_cpu_percent) = match (cpu_samples.first(), cpu_samples.last()) {
        (Some((first_elapsed, first_cpu)), Some((last_elapsed, last_cpu)))
            if last_elapsed > first_elapsed && last_cpu >= first_cpu =>
        {
            let cpu_delta = last_cpu - first_cpu;
            (
                Some(cpu_delta as f64 / 1_000.0),
                Some(cpu_delta as f64 / (last_elapsed - first_elapsed) as f64 * 100.0),
            )
        }
        _ => (None, None),
    };
    let resident = valid
        .iter()
        .filter_map(|sample| sample.resident_memory_bytes)
        .collect::<Vec<_>>();
    let virtual_memory = valid
        .iter()
        .filter_map(|sample| sample.virtual_memory_bytes)
        .collect::<Vec<_>>();
    let host_cpu = valid
        .iter()
        .filter_map(|sample| sample.host_cpu_percent)
        .filter(|value| value.is_finite())
        .collect::<Vec<_>>();
    let host_memory = valid
        .iter()
        .filter_map(|sample| sample.host_memory_bytes)
        .collect::<Vec<_>>();
    Ok(ResourceAnalysis {
        samples: valid.len(),
        average_cpu_percent,
        cpu_time_ms,
        mean_resident_memory_bytes: mean_u64(&resident),
        peak_resident_memory_bytes: resident.iter().max().copied(),
        mean_virtual_memory_bytes: mean_u64(&virtual_memory),
        peak_virtual_memory_bytes: virtual_memory.iter().max().copied(),
        host_samples: host_cpu.len().max(host_memory.len()),
        host_average_cpu_percent: (!host_cpu.is_empty())
            .then_some(host_cpu.iter().sum::<f64>() / host_cpu.len() as f64),
        host_mean_memory_bytes: mean_u64(&host_memory),
        host_peak_memory_bytes: host_memory.iter().max().copied(),
        host_metrics_warning,
    })
}

fn mean_u64(values: &[u64]) -> Option<f64> {
    (!values.is_empty())
        .then(|| values.iter().map(|value| *value as f64).sum::<f64>() / values.len() as f64)
}

fn recovery_analysis(
    run_directory: &Path,
    requests: &[RequestInput],
) -> Result<Vec<RecoveryAnalysis>, AnalysisError> {
    let path = run_directory.join("events.jsonl");
    if !path.is_file() {
        return Ok(Vec::new());
    }
    let events: Vec<EventInput> = read_json_lines(&path)?;
    Ok(events
        .iter()
        .enumerate()
        .filter(|(_, event)| {
            event.event == "failure_completed"
                && event.success == Some(true)
                && is_recovery_action(event)
        })
        .filter_map(|(idx, event)| {
            let action_completed = event.elapsed_us?;
            let action_started = events[..idx]
                .iter()
                .rev()
                .find(|candidate| {
                    candidate.event == "failure_started" && candidate.label == event.label
                })
                .and_then(|candidate| candidate.elapsed_us);
            let clock_start = action_started.unwrap_or(action_completed);

            let target = event
                .details
                .get("target_backend_id")
                .and_then(Value::as_str)
                .filter(|target| !target.is_empty());
            let mut completions = requests
                .iter()
                .filter(|request| {
                    target.is_some_and(|target_id| request.backend_id.as_deref() == Some(target_id))
                })
                .map(|request| {
                    (
                        request.started_offset_us.saturating_add(request.latency_us),
                        request.http_success,
                    )
                })
                .collect::<Vec<_>>();
            completions.sort_by_key(|(completed, _)| *completed);

            let after = completions
                .iter()
                .copied()
                .filter(|(completed, _)| *completed >= clock_start)
                .collect::<Vec<_>>();
            let first_success = after
                .iter()
                .find(|(_, success)| *success)
                .map(|(completed, _)| *completed);
            let stable_success = first_stable_success(&after, STABLE_RECOVERY_SUCCESSES);
            Some(RecoveryAnalysis {
                action: event.label.clone(),
                target_backend_id: target.map(str::to_owned),
                action_started_offset_ms: action_started.map(|value| value as f64 / 1_000.0),
                action_completed_offset_ms: action_completed as f64 / 1_000.0,
                first_success_offset_ms: first_success.map(|value| value as f64 / 1_000.0),
                time_to_first_success_ms: first_success
                    .map(|value| value.saturating_sub(clock_start) as f64 / 1_000.0),
                stable_success_offset_ms: stable_success.map(|value| value as f64 / 1_000.0),
                time_to_stable_success_ms: stable_success
                    .map(|value| value.saturating_sub(clock_start) as f64 / 1_000.0),
                stable_successes_required: STABLE_RECOVERY_SUCCESSES,
            })
        })
        .collect())
}

fn is_recovery_word(word: &str) -> bool {
    matches!(
        word,
        "recover"
            | "recovery"
            | "restore"
            | "restart"
            | "resume"
            | "enable"
            | "start"
            | "heal"
            | "up"
    )
}

fn is_recovery_action(event: &EventInput) -> bool {
    let label = event.label.to_ascii_lowercase();
    if label
        .split(|character: char| !character.is_ascii_alphanumeric())
        .any(is_recovery_word)
    {
        return true;
    }

    // Labels intentionally use compound names such as "restore-one". Command
    // arguments must instead match whole words: a project or file containing
    // "failure-recovery" does not turn `docker ... stop` into a recovery action.
    event
        .details
        .get("command")
        .and_then(Value::as_str)
        .is_some_and(|command| {
            command
                .to_ascii_lowercase()
                .split_ascii_whitespace()
                .any(is_recovery_word)
        })
}

fn first_stable_success(completions: &[(u64, bool)], required: usize) -> Option<u64> {
    if required == 0 {
        return completions.first().map(|(completed, _)| *completed);
    }
    let mut consecutive = 0_usize;
    let mut start = 0_u64;
    for (completed, success) in completions {
        if *success {
            if consecutive == 0 {
                start = *completed;
            }
            consecutive += 1;
            if consecutive >= required {
                return Some(start);
            }
        } else {
            consecutive = 0;
        }
    }
    None
}

fn aggregate_runs(runs: &[RunAnalysis]) -> Vec<GroupAnalysis> {
    let mut grouped = BTreeMap::<GroupKey, Vec<&RunAnalysis>>::new();
    for run in runs.iter().filter(|run| run.included_in_aggregates) {
        grouped
            .entry(GroupKey {
                scenario: run.scenario.clone(),
                algorithm: run.algorithm.clone(),
                runtime: run.runtime.clone(),
            })
            .or_default()
            .push(run);
    }
    grouped
        .into_iter()
        .map(|(key, runs)| GroupAnalysis {
            scenario: key.scenario,
            algorithm: key.algorithm,
            runtime: key.runtime,
            runs: runs.len(),
            throughput_requests_per_s: summarize_values(
                runs.iter().filter_map(|run| run.throughput_requests_per_s),
            ),
            successful_throughput_requests_per_s: summarize_values(
                runs.iter()
                    .filter_map(|run| run.successful_throughput_requests_per_s),
            ),
            error_rate: summarize_values(runs.iter().filter_map(|run| run.error_rate)),
            latency_p50_us: summarize_values(runs.iter().filter_map(|run| run.latency.p50_us)),
            latency_p95_us: summarize_values(runs.iter().filter_map(|run| run.latency.p95_us)),
            latency_p99_us: summarize_values(runs.iter().filter_map(|run| run.latency.p99_us)),
            slo_attainment_rate: summarize_values(
                runs.iter().filter_map(|run| run.slo.attainment_rate),
            ),
            slo_miss_rate: summarize_values(runs.iter().filter_map(|run| run.slo.miss_rate)),
            scheduling_lag_p95_us: summarize_values(
                runs.iter().filter_map(|run| run.scheduling_lag.p95_us),
            ),
            fairness_jain_index: summarize_values(
                runs.iter().filter_map(|run| run.fairness.jain_index),
            ),
            weighted_fairness_jain_index: summarize_values(
                runs.iter()
                    .filter_map(|run| run.fairness.weighted_jain_index),
            ),
            average_cpu_percent: summarize_values(
                runs.iter()
                    .filter_map(|run| run.resource_usage.average_cpu_percent),
            ),
            peak_resident_memory_bytes: summarize_values(runs.iter().filter_map(|run| {
                run.resource_usage
                    .peak_resident_memory_bytes
                    .map(|value| value as f64)
            })),
            recovery_time_ms: summarize_values(runs.iter().flat_map(|run| {
                run.recoveries
                    .iter()
                    .filter_map(|recovery| recovery.time_to_stable_success_ms)
            })),
        })
        .collect()
}

fn aggregate_windows(runs: &[RunAnalysis]) -> Vec<GroupWindowAnalysis> {
    let mut grouped = BTreeMap::<(GroupKey, String, u64, u64), Vec<&WindowAnalysis>>::new();
    for run in runs.iter().filter(|run| run.included_in_aggregates) {
        for window in &run.windows {
            grouped
                .entry((
                    GroupKey {
                        scenario: run.scenario.clone(),
                        algorithm: run.algorithm.clone(),
                        runtime: run.runtime.clone(),
                    },
                    window.window_id.clone(),
                    window.start_ms,
                    window.end_ms,
                ))
                .or_default()
                .push(window);
        }
    }
    grouped
        .into_iter()
        .map(|((key, window_id, start_ms, end_ms), windows)| {
            let mut backend_requests = BTreeMap::new();
            for window in &windows {
                for (backend_id, requests) in &window.backend_requests {
                    *backend_requests.entry(backend_id.clone()).or_insert(0) += requests;
                }
            }
            GroupWindowAnalysis {
                scenario: key.scenario,
                algorithm: key.algorithm,
                runtime: key.runtime,
                window_id,
                start_ms,
                end_ms,
                runs: windows.len(),
                total_requests: summarize_values(
                    windows.iter().map(|window| window.total_requests as f64),
                ),
                successful_requests: summarize_values(
                    windows
                        .iter()
                        .map(|window| window.successful_requests as f64),
                ),
                latency_p95_us: summarize_values(
                    windows.iter().filter_map(|window| window.latency.p95_us),
                ),
                backend_requests,
            }
        })
        .collect()
}

fn summarize_values(values: impl Iterator<Item = f64>) -> StatisticalSummary {
    let values = values.filter(|value| value.is_finite()).collect::<Vec<_>>();
    if values.is_empty() {
        return StatisticalSummary::default();
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let standard_deviation = if values.len() > 1 {
        Some(
            (values
                .iter()
                .map(|value| (value - mean).powi(2))
                .sum::<f64>()
                / (values.len() - 1) as f64)
                .sqrt(),
        )
    } else {
        None
    };
    let margin = standard_deviation.map(|deviation| {
        student_t_critical_95(values.len() - 1) * deviation / (values.len() as f64).sqrt()
    });
    StatisticalSummary {
        samples: values.len(),
        mean: Some(mean),
        standard_deviation,
        confidence_interval_95_low: margin.map(|margin| mean - margin),
        confidence_interval_95_high: margin.map(|margin| mean + margin),
        min: values.iter().copied().reduce(f64::min),
        max: values.iter().copied().reduce(f64::max),
    }
}

fn student_t_critical_95(degrees_of_freedom: usize) -> f64 {
    const CRITICAL: [f64; 30] = [
        12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365, 2.306, 2.262, 2.228, 2.201, 2.179, 2.160,
        2.145, 2.131, 2.120, 2.110, 2.101, 2.093, 2.086, 2.080, 2.074, 2.069, 2.064, 2.060, 2.056,
        2.052, 2.048, 2.045, 2.042,
    ];
    degrees_of_freedom
        .checked_sub(1)
        .and_then(|index| CRITICAL.get(index))
        .copied()
        .unwrap_or(1.96)
}

fn write_outputs(
    report: &AnalysisReport,
    backends: &[BackendAnalysis],
) -> Result<(), AnalysisError> {
    fs::create_dir_all(&report.analysis_directory)?;
    write_json(report.analysis_directory.join("analysis.json"), report)?;
    write_json(
        report.analysis_directory.join("backend-fairness.json"),
        backends,
    )?;
    write_run_csv(report)?;
    write_group_csv(report)?;
    write_group_statistics_csv(report)?;
    write_backend_csv(report, backends)?;
    write_recovery_csv(report)?;
    write_resource_csv(report)?;
    write_source_csv(report)?;
    write_workload_csv(report)?;
    write_window_csv(report)?;
    write_group_window_csv(report)?;
    write_plots(report)?;
    Ok(())
}

fn write_run_csv(report: &AnalysisReport) -> Result<(), AnalysisError> {
    let mut writer = csv_writer(report.analysis_directory.join("run-summary.csv"))?;
    csv_row(
        &mut writer,
        &[
            "run_id",
            "scenario",
            "algorithm",
            "runtime",
            "repetition",
            "run_seed",
            "run_status",
            "included_in_aggregates",
            "measurement_duration_s",
            "total_requests",
            "successful_requests",
            "transport_errors",
            "http_errors",
            "error_rate",
            "throughput_requests_per_s",
            "successful_throughput_requests_per_s",
            "latency_mean_us",
            "latency_p50_us",
            "latency_p90_us",
            "latency_p95_us",
            "latency_p99_us",
            "latency_p999_us",
            "fairness_jain_index",
            "weighted_fairness_jain_index",
            "resource_samples",
            "average_cpu_percent",
            "cpu_time_ms",
            "mean_resident_memory_bytes",
            "peak_resident_memory_bytes",
            "mean_virtual_memory_bytes",
            "peak_virtual_memory_bytes",
            "recovery_time_ms",
            "slo_target_ms",
            "slo_successful_within_slo",
            "slo_attainment_rate",
            "slo_miss_count",
            "slo_miss_rate",
            "scheduling_lag_samples",
            "scheduling_lag_mean_us",
            "scheduling_lag_p50_us",
            "scheduling_lag_p95_us",
            "scheduling_lag_p99_us",
            "scheduling_lag_max_us",
            "environment_limited",
            "host_samples",
            "host_average_cpu_percent",
            "host_mean_memory_bytes",
            "host_peak_memory_bytes",
        ],
    )?;
    for run in &report.runs {
        let recovery = run
            .recoveries
            .iter()
            .filter_map(|recovery| recovery.time_to_stable_success_ms)
            .reduce(f64::min);
        csv_row(
            &mut writer,
            &[
                run.run_id.clone(),
                run.scenario.clone(),
                run.algorithm.clone(),
                run.runtime.clone(),
                run.repetition.to_string(),
                run.run_seed.to_string(),
                run.run_status.clone(),
                run.included_in_aggregates.to_string(),
                optional_number(run.measurement_duration_s),
                run.total_requests.to_string(),
                run.successful_requests.to_string(),
                run.transport_errors.to_string(),
                run.http_errors.to_string(),
                optional_number(run.error_rate),
                optional_number(run.throughput_requests_per_s),
                optional_number(run.successful_throughput_requests_per_s),
                optional_number(run.latency.mean_us),
                optional_number(run.latency.p50_us),
                optional_number(run.latency.p90_us),
                optional_number(run.latency.p95_us),
                optional_number(run.latency.p99_us),
                optional_number(run.latency.p999_us),
                optional_number(run.fairness.jain_index),
                optional_number(run.fairness.weighted_jain_index),
                run.resource_usage.samples.to_string(),
                optional_number(run.resource_usage.average_cpu_percent),
                optional_number(run.resource_usage.cpu_time_ms),
                optional_number(run.resource_usage.mean_resident_memory_bytes),
                run.resource_usage
                    .peak_resident_memory_bytes
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
                optional_number(run.resource_usage.mean_virtual_memory_bytes),
                run.resource_usage
                    .peak_virtual_memory_bytes
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
                optional_number(recovery),
                run.slo
                    .target_ms
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
                run.slo
                    .successful_within_slo
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
                optional_number(run.slo.attainment_rate),
                run.slo
                    .miss_count
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
                optional_number(run.slo.miss_rate),
                run.scheduling_lag.samples.to_string(),
                optional_number(run.scheduling_lag.mean_us),
                optional_number(run.scheduling_lag.p50_us),
                optional_number(run.scheduling_lag.p95_us),
                optional_number(run.scheduling_lag.p99_us),
                run.scheduling_lag
                    .max_us
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
                run.environment_limited.to_string(),
                run.resource_usage.host_samples.to_string(),
                optional_number(run.resource_usage.host_average_cpu_percent),
                optional_number(run.resource_usage.host_mean_memory_bytes),
                run.resource_usage
                    .host_peak_memory_bytes
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
            ],
        )?;
    }
    writer.flush()?;
    Ok(())
}

fn write_group_csv(report: &AnalysisReport) -> Result<(), AnalysisError> {
    let mut writer = csv_writer(report.analysis_directory.join("group-summary.csv"))?;
    csv_row(
        &mut writer,
        &[
            "scenario",
            "algorithm",
            "runtime",
            "runs",
            "throughput_mean_requests_per_s",
            "throughput_ci95_low",
            "throughput_ci95_high",
            "successful_throughput_mean_requests_per_s",
            "error_rate_mean",
            "latency_p50_mean_us",
            "latency_p95_mean_us",
            "latency_p99_mean_us",
            "fairness_mean",
            "weighted_fairness_mean",
            "average_cpu_percent_mean",
            "peak_resident_memory_mean_bytes",
            "recovery_time_mean_ms",
            "slo_attainment_rate_mean",
            "slo_miss_rate_mean",
            "scheduling_lag_p95_mean_us",
        ],
    )?;
    for group in &report.groups {
        csv_row(
            &mut writer,
            &[
                group.scenario.clone(),
                group.algorithm.clone(),
                group.runtime.clone(),
                group.runs.to_string(),
                optional_number(group.throughput_requests_per_s.mean),
                optional_number(group.throughput_requests_per_s.confidence_interval_95_low),
                optional_number(group.throughput_requests_per_s.confidence_interval_95_high),
                optional_number(group.successful_throughput_requests_per_s.mean),
                optional_number(group.error_rate.mean),
                optional_number(group.latency_p50_us.mean),
                optional_number(group.latency_p95_us.mean),
                optional_number(group.latency_p99_us.mean),
                optional_number(group.fairness_jain_index.mean),
                optional_number(group.weighted_fairness_jain_index.mean),
                optional_number(group.average_cpu_percent.mean),
                optional_number(group.peak_resident_memory_bytes.mean),
                optional_number(group.recovery_time_ms.mean),
                optional_number(group.slo_attainment_rate.mean),
                optional_number(group.slo_miss_rate.mean),
                optional_number(group.scheduling_lag_p95_us.mean),
            ],
        )?;
    }
    writer.flush()?;
    Ok(())
}

fn write_group_statistics_csv(report: &AnalysisReport) -> Result<(), AnalysisError> {
    let mut writer = csv_writer(report.analysis_directory.join("group-statistics.csv"))?;
    csv_row(
        &mut writer,
        &[
            "scenario",
            "algorithm",
            "runtime",
            "metric",
            "unit",
            "samples",
            "mean",
            "standard_deviation",
            "confidence_interval_95_low",
            "confidence_interval_95_high",
            "min",
            "max",
        ],
    )?;
    for group in &report.groups {
        let metrics = [
            (
                "throughput",
                "requests_per_second",
                &group.throughput_requests_per_s,
            ),
            (
                "successful_throughput",
                "requests_per_second",
                &group.successful_throughput_requests_per_s,
            ),
            ("error_rate", "fraction", &group.error_rate),
            ("latency_p50", "microseconds", &group.latency_p50_us),
            ("latency_p95", "microseconds", &group.latency_p95_us),
            ("latency_p99", "microseconds", &group.latency_p99_us),
            (
                "slo_attainment_rate",
                "fraction",
                &group.slo_attainment_rate,
            ),
            ("slo_miss_rate", "fraction", &group.slo_miss_rate),
            (
                "scheduling_lag_p95",
                "microseconds",
                &group.scheduling_lag_p95_us,
            ),
            ("fairness_jain_index", "index", &group.fairness_jain_index),
            (
                "weighted_fairness_jain_index",
                "index",
                &group.weighted_fairness_jain_index,
            ),
            (
                "average_cpu",
                "percent_of_one_logical_cpu",
                &group.average_cpu_percent,
            ),
            (
                "peak_resident_memory",
                "bytes",
                &group.peak_resident_memory_bytes,
            ),
            ("recovery_time", "milliseconds", &group.recovery_time_ms),
        ];
        for (metric, unit, summary) in metrics {
            csv_row(
                &mut writer,
                &[
                    group.scenario.clone(),
                    group.algorithm.clone(),
                    group.runtime.clone(),
                    metric.into(),
                    unit.into(),
                    summary.samples.to_string(),
                    optional_number(summary.mean),
                    optional_number(summary.standard_deviation),
                    optional_number(summary.confidence_interval_95_low),
                    optional_number(summary.confidence_interval_95_high),
                    optional_number(summary.min),
                    optional_number(summary.max),
                ],
            )?;
        }
    }
    writer.flush()?;
    Ok(())
}

fn write_backend_csv(
    report: &AnalysisReport,
    backends: &[BackendAnalysis],
) -> Result<(), AnalysisError> {
    let mut writer = csv_writer(report.analysis_directory.join("backend-fairness.csv"))?;
    csv_row(
        &mut writer,
        &[
            "run_id",
            "scenario",
            "algorithm",
            "runtime",
            "service_id",
            "backend_id",
            "weight",
            "requests",
            "successful_requests",
            "failed_requests",
            "normalized_load",
        ],
    )?;
    for backend in backends {
        csv_row(
            &mut writer,
            &[
                backend.run_id.clone(),
                backend.scenario.clone(),
                backend.algorithm.clone(),
                backend.runtime.clone(),
                backend.service_id.clone(),
                backend.backend_id.clone(),
                backend.weight.to_string(),
                backend.requests.to_string(),
                backend.successful_requests.to_string(),
                backend.failed_requests.to_string(),
                optional_number(backend.normalized_load),
            ],
        )?;
    }
    writer.flush()?;
    Ok(())
}

fn write_recovery_csv(report: &AnalysisReport) -> Result<(), AnalysisError> {
    let mut writer = csv_writer(report.analysis_directory.join("recovery.csv"))?;
    csv_row(
        &mut writer,
        &[
            "run_id",
            "scenario",
            "algorithm",
            "runtime",
            "action",
            "target_backend_id",
            "action_started_offset_ms",
            "action_completed_offset_ms",
            "first_success_offset_ms",
            "time_to_first_success_ms",
            "stable_success_offset_ms",
            "time_to_stable_success_ms",
            "stable_successes_required",
        ],
    )?;
    for run in &report.runs {
        for recovery in &run.recoveries {
            csv_row(
                &mut writer,
                &[
                    run.run_id.clone(),
                    run.scenario.clone(),
                    run.algorithm.clone(),
                    run.runtime.clone(),
                    recovery.action.clone(),
                    recovery.target_backend_id.clone().unwrap_or_default(),
                    optional_number(recovery.action_started_offset_ms),
                    format_number(recovery.action_completed_offset_ms),
                    optional_number(recovery.first_success_offset_ms),
                    optional_number(recovery.time_to_first_success_ms),
                    optional_number(recovery.stable_success_offset_ms),
                    optional_number(recovery.time_to_stable_success_ms),
                    recovery.stable_successes_required.to_string(),
                ],
            )?;
        }
    }
    writer.flush()?;
    Ok(())
}

fn write_resource_csv(report: &AnalysisReport) -> Result<(), AnalysisError> {
    let mut writer = csv_writer(report.analysis_directory.join("resource-usage.csv"))?;
    csv_row(
        &mut writer,
        &[
            "run_id",
            "scenario",
            "algorithm",
            "runtime",
            "samples",
            "average_cpu_percent",
            "cpu_time_ms",
            "mean_resident_memory_bytes",
            "peak_resident_memory_bytes",
            "mean_virtual_memory_bytes",
            "peak_virtual_memory_bytes",
            "host_samples",
            "host_average_cpu_percent",
            "host_mean_memory_bytes",
            "host_peak_memory_bytes",
        ],
    )?;
    for run in &report.runs {
        let usage = &run.resource_usage;
        csv_row(
            &mut writer,
            &[
                run.run_id.clone(),
                run.scenario.clone(),
                run.algorithm.clone(),
                run.runtime.clone(),
                usage.samples.to_string(),
                optional_number(usage.average_cpu_percent),
                optional_number(usage.cpu_time_ms),
                optional_number(usage.mean_resident_memory_bytes),
                usage
                    .peak_resident_memory_bytes
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
                optional_number(usage.mean_virtual_memory_bytes),
                usage
                    .peak_virtual_memory_bytes
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
                usage.host_samples.to_string(),
                optional_number(usage.host_average_cpu_percent),
                optional_number(usage.host_mean_memory_bytes),
                usage
                    .host_peak_memory_bytes
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
            ],
        )?;
    }
    writer.flush()?;
    Ok(())
}

fn write_source_csv(report: &AnalysisReport) -> Result<(), AnalysisError> {
    let mut writer = csv_writer(report.analysis_directory.join("raw-inputs.csv"))?;
    csv_row(&mut writer, &["relative_path", "bytes", "sha256"])?;
    for source in &report.source_artifacts {
        csv_row(
            &mut writer,
            &[
                source.relative_path.clone(),
                source.bytes.to_string(),
                source.sha256.clone(),
            ],
        )?;
    }
    writer.flush()?;
    Ok(())
}

fn write_workload_csv(report: &AnalysisReport) -> Result<(), AnalysisError> {
    let mut writer = csv_writer(report.analysis_directory.join("workload-summary.csv"))?;
    csv_row(
        &mut writer,
        &[
            "run_id",
            "scenario",
            "algorithm",
            "runtime",
            "workload_id",
            "total_requests",
            "successful_requests",
            "transport_errors",
            "http_errors",
            "error_rate",
            "latency_p50_us",
            "latency_p95_us",
            "slo_target_ms",
            "slo_successful_within_slo",
            "slo_attainment_rate",
            "scheduling_lag_p95_us",
        ],
    )?;
    for run in &report.runs {
        for workload in &run.workloads {
            csv_row(
                &mut writer,
                &[
                    run.run_id.clone(),
                    run.scenario.clone(),
                    run.algorithm.clone(),
                    run.runtime.clone(),
                    workload.workload_id.clone(),
                    workload.total_requests.to_string(),
                    workload.successful_requests.to_string(),
                    workload.transport_errors.to_string(),
                    workload.http_errors.to_string(),
                    optional_number(workload.error_rate),
                    optional_number(workload.latency.p50_us),
                    optional_number(workload.latency.p95_us),
                    workload
                        .slo
                        .target_ms
                        .map(|value| value.to_string())
                        .unwrap_or_default(),
                    workload
                        .slo
                        .successful_within_slo
                        .map(|value| value.to_string())
                        .unwrap_or_default(),
                    optional_number(workload.slo.attainment_rate),
                    optional_number(workload.scheduling_lag.p95_us),
                ],
            )?;
        }
    }
    writer.flush()?;
    Ok(())
}

fn write_window_csv(report: &AnalysisReport) -> Result<(), AnalysisError> {
    let mut writer = csv_writer(report.analysis_directory.join("window-summary.csv"))?;
    csv_row(
        &mut writer,
        &[
            "run_id",
            "scenario",
            "algorithm",
            "runtime",
            "window_id",
            "start_ms",
            "end_ms",
            "total_requests",
            "successful_requests",
            "latency_p50_us",
            "latency_p95_us",
            "backend_requests_json",
        ],
    )?;
    for run in &report.runs {
        for window in &run.windows {
            csv_row(
                &mut writer,
                &[
                    run.run_id.clone(),
                    run.scenario.clone(),
                    run.algorithm.clone(),
                    run.runtime.clone(),
                    window.window_id.clone(),
                    window.start_ms.to_string(),
                    window.end_ms.to_string(),
                    window.total_requests.to_string(),
                    window.successful_requests.to_string(),
                    optional_number(window.latency.p50_us),
                    optional_number(window.latency.p95_us),
                    serde_json::to_string(&window.backend_requests)?,
                ],
            )?;
        }
    }
    writer.flush()?;
    Ok(())
}

fn write_group_window_csv(report: &AnalysisReport) -> Result<(), AnalysisError> {
    let mut writer = csv_writer(report.analysis_directory.join("group-window-summary.csv"))?;
    csv_row(
        &mut writer,
        &[
            "scenario",
            "algorithm",
            "runtime",
            "window_id",
            "start_ms",
            "end_ms",
            "runs",
            "total_requests_mean",
            "successful_requests_mean",
            "latency_p95_mean_us",
            "backend_requests_json",
        ],
    )?;
    for window in &report.group_windows {
        csv_row(
            &mut writer,
            &[
                window.scenario.clone(),
                window.algorithm.clone(),
                window.runtime.clone(),
                window.window_id.clone(),
                window.start_ms.to_string(),
                window.end_ms.to_string(),
                window.runs.to_string(),
                optional_number(window.total_requests.mean),
                optional_number(window.successful_requests.mean),
                optional_number(window.latency_p95_us.mean),
                serde_json::to_string(&window.backend_requests)?,
            ],
        )?;
    }
    writer.flush()?;
    Ok(())
}

fn write_plots(report: &AnalysisReport) -> Result<(), AnalysisError> {
    let directory = report.analysis_directory.join("plots");
    fs::create_dir_all(&directory)?;
    let labels = report.groups.iter().map(group_label).collect::<Vec<_>>();
    bar_chart(
        &directory.join("throughput.svg"),
        "Successful throughput",
        "requests / second",
        &labels,
        &[(
            "successful throughput",
            report
                .groups
                .iter()
                .map(|group| group.successful_throughput_requests_per_s.mean)
                .collect(),
        )],
    )?;
    bar_chart(
        &directory.join("error-rate.svg"),
        "End-to-end error rate",
        "percent",
        &labels,
        &[(
            "errors",
            report
                .groups
                .iter()
                .map(|group| group.error_rate.mean.map(|value| value * 100.0))
                .collect(),
        )],
    )?;
    bar_chart(
        &directory.join("latency-percentiles.svg"),
        "End-to-end latency percentiles",
        "milliseconds",
        &labels,
        &[
            (
                "p50",
                report
                    .groups
                    .iter()
                    .map(|group| group.latency_p50_us.mean.map(|value| value / 1_000.0))
                    .collect(),
            ),
            (
                "p95",
                report
                    .groups
                    .iter()
                    .map(|group| group.latency_p95_us.mean.map(|value| value / 1_000.0))
                    .collect(),
            ),
            (
                "p99",
                report
                    .groups
                    .iter()
                    .map(|group| group.latency_p99_us.mean.map(|value| value / 1_000.0))
                    .collect(),
            ),
        ],
    )?;
    bar_chart(
        &directory.join("fairness.svg"),
        "Backend allocation fairness",
        "Jain index",
        &labels,
        &[
            (
                "unweighted",
                report
                    .groups
                    .iter()
                    .map(|group| group.fairness_jain_index.mean)
                    .collect(),
            ),
            (
                "weight-adjusted",
                report
                    .groups
                    .iter()
                    .map(|group| group.weighted_fairness_jain_index.mean)
                    .collect(),
            ),
        ],
    )?;
    bar_chart(
        &directory.join("cpu-usage.svg"),
        "Server process CPU usage",
        "percent of one logical CPU",
        &labels,
        &[(
            "average CPU",
            report
                .groups
                .iter()
                .map(|group| group.average_cpu_percent.mean)
                .collect(),
        )],
    )?;
    bar_chart(
        &directory.join("peak-memory.svg"),
        "Peak server resident memory",
        "MiB",
        &labels,
        &[(
            "peak RSS",
            report
                .groups
                .iter()
                .map(|group| {
                    group
                        .peak_resident_memory_bytes
                        .mean
                        .map(|value| value / 1_048_576.0)
                })
                .collect(),
        )],
    )?;
    bar_chart(
        &directory.join("recovery-time.svg"),
        "Stable recovery time",
        "milliseconds",
        &labels,
        &[(
            "time to five successes",
            report
                .groups
                .iter()
                .map(|group| group.recovery_time_ms.mean)
                .collect(),
        )],
    )?;
    Ok(())
}

fn group_label(group: &GroupAnalysis) -> String {
    format!(
        "{} / {} / {}",
        group.scenario, group.algorithm, group.runtime
    )
}

fn bar_chart(
    path: &Path,
    title: &str,
    y_label: &str,
    labels: &[String],
    series: &[(&str, Vec<Option<f64>>)],
) -> Result<(), AnalysisError> {
    const COLORS: [&str; 6] = [
        "#0072B2", "#D55E00", "#009E73", "#CC79A7", "#E69F00", "#56B4E9",
    ];
    let width = (220 + labels.len().max(1) * 125).max(900) as f64;
    let height = 620.0;
    let left = 100.0;
    let right = 35.0;
    let top = 90.0;
    let bottom = 175.0;
    let plot_width = width - left - right;
    let plot_height = height - top - bottom;
    let maximum = series
        .iter()
        .flat_map(|(_, values)| values.iter().flatten().copied())
        .filter(|value| value.is_finite() && *value >= 0.0)
        .reduce(f64::max)
        .unwrap_or(1.0)
        .max(f64::EPSILON);
    let axis_max = nice_axis_max(maximum);

    let mut svg = String::new();
    writeln!(
        svg,
        "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"{width:.0}\" height=\"{height:.0}\" viewBox=\"0 0 {width:.0} {height:.0}\">"
    )
    .unwrap();
    writeln!(svg, "<rect width=\"100%\" height=\"100%\" fill=\"white\"/>").unwrap();
    writeln!(
        svg,
        "<style>text{{font-family:Arial,Helvetica,sans-serif;fill:#222}} .grid{{stroke:#d9d9d9;stroke-width:1}} .axis{{stroke:#333;stroke-width:1.5}}</style>"
    )
    .unwrap();
    writeln!(
        svg,
        "<text x=\"{:.1}\" y=\"38\" text-anchor=\"middle\" font-size=\"22\" font-weight=\"600\">{}</text>",
        width / 2.0,
        xml_escape(title)
    )
    .unwrap();
    writeln!(
        svg,
        "<text transform=\"translate(25 {:.1}) rotate(-90)\" text-anchor=\"middle\" font-size=\"14\">{}</text>",
        top + plot_height / 2.0,
        xml_escape(y_label)
    )
    .unwrap();

    for tick in 0..=5 {
        let ratio = tick as f64 / 5.0;
        let y = top + plot_height * (1.0 - ratio);
        let value = axis_max * ratio;
        writeln!(
            svg,
            "<line class=\"grid\" x1=\"{left:.1}\" y1=\"{y:.1}\" x2=\"{:.1}\" y2=\"{y:.1}\"/><text x=\"{:.1}\" y=\"{:.1}\" text-anchor=\"end\" font-size=\"12\">{}</text>",
            width - right,
            left - 10.0,
            y + 4.0,
            compact_number(value)
        )
        .unwrap();
    }
    writeln!(
        svg,
        "<line class=\"axis\" x1=\"{left:.1}\" y1=\"{top:.1}\" x2=\"{left:.1}\" y2=\"{:.1}\"/><line class=\"axis\" x1=\"{left:.1}\" y1=\"{:.1}\" x2=\"{:.1}\" y2=\"{:.1}\"/>",
        top + plot_height,
        top + plot_height,
        width - right,
        top + plot_height
    )
    .unwrap();

    if labels.is_empty() {
        writeln!(
            svg,
            "<text x=\"{:.1}\" y=\"{:.1}\" text-anchor=\"middle\" font-size=\"16\" fill=\"#666\">No eligible runs</text>",
            left + plot_width / 2.0,
            top + plot_height / 2.0
        )
        .unwrap();
    } else {
        let group_width = plot_width / labels.len() as f64;
        let series_count = series.len().max(1);
        let bar_width = (group_width * 0.72 / series_count as f64).min(42.0);
        for (group_index, label) in labels.iter().enumerate() {
            let center = left + group_width * (group_index as f64 + 0.5);
            for (series_index, (_, values)) in series.iter().enumerate() {
                let x = center - (bar_width * series_count as f64) / 2.0
                    + bar_width * series_index as f64;
                if let Some(value) = values.get(group_index).copied().flatten() {
                    let bar_height = (value.max(0.0) / axis_max * plot_height).min(plot_height);
                    let y = top + plot_height - bar_height;
                    writeln!(
                        svg,
                        "<rect x=\"{x:.2}\" y=\"{y:.2}\" width=\"{:.2}\" height=\"{bar_height:.2}\" fill=\"{}\"/>",
                        (bar_width - 2.0).max(1.0),
                        COLORS[series_index % COLORS.len()]
                    )
                    .unwrap();
                }
            }
            writeln!(
                svg,
                "<text transform=\"translate({:.1} {:.1}) rotate(-38)\" text-anchor=\"end\" font-size=\"11\">{}</text>",
                center,
                top + plot_height + 18.0,
                xml_escape(label)
            )
            .unwrap();
        }
    }

    if series.len() > 1 {
        let legend_width = series.len() as f64 * 150.0;
        let start = (width - legend_width) / 2.0;
        for (index, (name, _)) in series.iter().enumerate() {
            let x = start + index as f64 * 150.0;
            writeln!(
                svg,
                "<rect x=\"{x:.1}\" y=\"55\" width=\"14\" height=\"14\" fill=\"{}\"/><text x=\"{:.1}\" y=\"67\" font-size=\"12\">{}</text>",
                COLORS[index % COLORS.len()],
                x + 20.0,
                xml_escape(name)
            )
            .unwrap();
        }
    }
    writeln!(svg, "</svg>").unwrap();
    fs::write(path, svg)?;
    Ok(())
}

fn nice_axis_max(maximum: f64) -> f64 {
    if maximum <= 0.0 || !maximum.is_finite() {
        return 1.0;
    }
    let rough_step = maximum / 5.0;
    let magnitude = 10_f64.powf(rough_step.log10().floor());
    let normalized = rough_step / magnitude;
    let nice = if normalized <= 1.0 {
        1.0
    } else if normalized <= 2.0 {
        2.0
    } else if normalized <= 5.0 {
        5.0
    } else {
        10.0
    };
    (nice * magnitude * 5.0).max(maximum)
}

fn compact_number(value: f64) -> String {
    if value.abs() >= 1_000.0 {
        format!("{value:.0}")
    } else if value.abs() >= 10.0 {
        format!("{value:.1}")
    } else {
        format!("{value:.2}")
    }
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn csv_writer(path: PathBuf) -> Result<BufWriter<File>, AnalysisError> {
    Ok(BufWriter::new(File::create(path)?))
}

fn csv_row<S: AsRef<str>>(writer: &mut impl Write, values: &[S]) -> Result<(), AnalysisError> {
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            writer.write_all(b",")?;
        }
        let value = value.as_ref();
        if value.contains([',', '"', '\n', '\r']) {
            writer.write_all(b"\"")?;
            writer.write_all(value.replace('"', "\"\"").as_bytes())?;
            writer.write_all(b"\"")?;
        } else {
            writer.write_all(value.as_bytes())?;
        }
    }
    writer.write_all(b"\n")?;
    Ok(())
}

fn optional_number(value: Option<f64>) -> String {
    value.map(format_number).unwrap_or_default()
}

fn format_number(value: f64) -> String {
    format!("{value:.6}")
}

fn read_json<T: for<'de> Deserialize<'de>>(path: PathBuf) -> Result<T, AnalysisError> {
    let file = File::open(&path).map_err(|error| {
        AnalysisError::new(format!("failed to open {}: {error}", path.display()))
    })?;
    serde_json::from_reader(file)
        .map_err(|error| AnalysisError::new(format!("failed to parse {}: {error}", path.display())))
}

fn read_json_lines<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<Vec<T>, AnalysisError> {
    let file = File::open(path).map_err(|error| {
        AnalysisError::new(format!("failed to open {}: {error}", path.display()))
    })?;
    let mut values = Vec::new();
    for (index, line) in BufReader::new(file).lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        values.push(serde_json::from_str(&line).map_err(|error| {
            AnalysisError::new(format!(
                "failed to parse {} line {}: {error}",
                path.display(),
                index + 1
            ))
        })?);
    }
    Ok(values)
}

fn write_json(path: PathBuf, value: &(impl Serialize + ?Sized)) -> Result<(), AnalysisError> {
    let mut writer = BufWriter::new(File::create(&path)?);
    serde_json::to_writer_pretty(&mut writer, value)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}

fn absolute_without_requiring_existence(path: &Path) -> Result<PathBuf, AnalysisError> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

fn inventory_sources(
    experiment_directory: &Path,
    run_directories: &[PathBuf],
    analysis_directory: &Path,
) -> Result<Vec<SourceArtifact>, AnalysisError> {
    let mut paths = BTreeSet::new();
    for entry in fs::read_dir(experiment_directory)? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            paths.insert(entry.path());
        }
    }
    for directory in run_directories {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                paths.insert(entry.path());
            }
        }
    }
    let mut artifacts = Vec::new();
    for path in paths {
        if path.starts_with(analysis_directory) {
            continue;
        }
        let metadata = fs::metadata(&path)?;
        artifacts.push(SourceArtifact {
            relative_path: relative_path(experiment_directory, &path),
            bytes: metadata.len(),
            sha256: sha256_file(&path)?,
        });
    }
    artifacts.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    Ok(artifacts)
}

fn relative_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn sha256_file(path: &Path) -> Result<String, AnalysisError> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher
        .finish()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

struct Sha256 {
    state: [u32; 8],
    buffer: Vec<u8>,
    length_bytes: u64,
}

impl Sha256 {
    fn new() -> Self {
        Self {
            state: [
                0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
                0x5be0cd19,
            ],
            buffer: Vec::with_capacity(64),
            length_bytes: 0,
        }
    }

    fn update(&mut self, mut bytes: &[u8]) {
        self.length_bytes = self.length_bytes.saturating_add(bytes.len() as u64);
        if !self.buffer.is_empty() {
            let needed = 64 - self.buffer.len();
            let take = needed.min(bytes.len());
            self.buffer.extend_from_slice(&bytes[..take]);
            bytes = &bytes[take..];
            if self.buffer.len() == 64 {
                let block: [u8; 64] = self.buffer.as_slice().try_into().unwrap();
                self.compress(&block);
                self.buffer.clear();
            }
        }
        while bytes.len() >= 64 {
            let block: &[u8; 64] = bytes[..64].try_into().unwrap();
            self.compress(block);
            bytes = &bytes[64..];
        }
        self.buffer.extend_from_slice(bytes);
    }

    fn finish(mut self) -> [u8; 32] {
        let bit_length = self.length_bytes.wrapping_mul(8);
        self.buffer.push(0x80);
        while self.buffer.len() % 64 != 56 {
            self.buffer.push(0);
        }
        self.buffer.extend_from_slice(&bit_length.to_be_bytes());
        let blocks = self.buffer.clone();
        for block in blocks.chunks_exact(64) {
            self.compress(block.try_into().unwrap());
        }
        let mut output = [0_u8; 32];
        for (chunk, value) in output.chunks_exact_mut(4).zip(self.state) {
            chunk.copy_from_slice(&value.to_be_bytes());
        }
        output
    }

    fn compress(&mut self, block: &[u8; 64]) {
        const K: [u32; 64] = [
            0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
            0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
            0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
            0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
            0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
            0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
            0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
            0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
            0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
            0xc67178f2,
        ];
        let mut words = [0_u32; 64];
        for (index, chunk) in block.chunks_exact(4).take(16).enumerate() {
            words[index] = u32::from_be_bytes(chunk.try_into().unwrap());
        }
        for index in 16..64 {
            let s0 = words[index - 15].rotate_right(7)
                ^ words[index - 15].rotate_right(18)
                ^ (words[index - 15] >> 3);
            let s1 = words[index - 2].rotate_right(17)
                ^ words[index - 2].rotate_right(19)
                ^ (words[index - 2] >> 10);
            words[index] = words[index - 16]
                .wrapping_add(s0)
                .wrapping_add(words[index - 7])
                .wrapping_add(s1);
        }
        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut h] = self.state;
        for index in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let choice = (e & f) ^ ((!e) & g);
            let temp1 = h
                .wrapping_add(s1)
                .wrapping_add(choice)
                .wrapping_add(K[index])
                .wrapping_add(words[index]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let majority = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(majority);
            h = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }
        for (state, value) in self.state.iter_mut().zip([a, b, c, d, e, f, g, h]) {
            *state = state.wrapping_add(value);
        }
    }
}

fn unix_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}
#[cfg(test)]
#[path = "analysis_tests.rs"]
mod tests;
