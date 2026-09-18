use std::{
    collections::BTreeMap,
    error::Error,
    fmt::{Display, Formatter},
    path::PathBuf,
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const ANALYSIS_SCHEMA_VERSION: u32 = 2;
pub const STABLE_RECOVERY_SUCCESSES: usize = 5;

#[derive(Debug)]
pub struct AnalysisError(pub String);

impl AnalysisError {
    pub fn new(message: impl Into<String>) -> Self {
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
pub(crate) struct MetadataInput {
    pub(crate) run_id: String,
    pub(crate) status: String,
    pub(crate) scenario: ScenarioInput,
    pub(crate) algorithm: String,
    pub(crate) runtime: String,
    pub(crate) repetition: usize,
    pub(crate) run_seed: u64,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ScenarioInput {
    pub(crate) id: String,
    #[serde(default)]
    pub(crate) slo_target_ms: Option<u64>,
    #[serde(default)]
    pub(crate) analysis_windows: Vec<AnalysisWindowInput>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct AnalysisWindowInput {
    pub(crate) id: String,
    pub(crate) start_ms: u64,
    pub(crate) end_ms: u64,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct RequestInput {
    #[serde(default)]
    pub(crate) workload_id: String,
    #[serde(default)]
    pub(crate) scheduled_offset_us: u64,
    pub(crate) started_offset_us: u64,
    pub(crate) latency_us: u64,
    pub(crate) transport_success: bool,
    pub(crate) http_success: bool,
    #[serde(default)]
    pub(crate) backend_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct EventInput {
    pub(crate) event: String,
    pub(crate) label: String,
    pub(crate) success: Option<bool>,
    pub(crate) elapsed_us: Option<u64>,
    #[serde(default)]
    pub(crate) details: Value,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ResourceInput {
    pub(crate) elapsed_us: u64,
    pub(crate) cpu_time_us: Option<u64>,
    pub(crate) resident_memory_bytes: Option<u64>,
    pub(crate) virtual_memory_bytes: Option<u64>,
    #[serde(default)]
    pub(crate) host_cpu_percent: Option<f64>,
    #[serde(default)]
    pub(crate) host_memory_bytes: Option<u64>,
    #[serde(default)]
    pub(crate) error: Option<String>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct GroupKey {
    pub(crate) scenario: String,
    pub(crate) algorithm: String,
    pub(crate) runtime: String,
}

#[derive(Clone, Debug)]
pub(crate) struct BackendLoad {
    pub(crate) service_id: String,
    pub(crate) backend_id: String,
    pub(crate) weight: u64,
    pub(crate) requests: u64,
    pub(crate) successful_requests: u64,
    pub(crate) failed_requests: u64,
}
