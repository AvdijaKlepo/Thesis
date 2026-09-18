use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use super::models::{AnalysisError, AnalysisReport, BackendAnalysis};

pub(crate) fn write_run_csv(report: &AnalysisReport) -> Result<(), AnalysisError> {
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

pub(crate) fn write_group_csv(report: &AnalysisReport) -> Result<(), AnalysisError> {
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

pub(crate) fn write_group_statistics_csv(report: &AnalysisReport) -> Result<(), AnalysisError> {
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

pub(crate) fn write_backend_csv(
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

pub(crate) fn write_recovery_csv(report: &AnalysisReport) -> Result<(), AnalysisError> {
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

pub(crate) fn write_resource_csv(report: &AnalysisReport) -> Result<(), AnalysisError> {
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

pub(crate) fn write_source_csv(report: &AnalysisReport) -> Result<(), AnalysisError> {
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

pub(crate) fn write_workload_csv(report: &AnalysisReport) -> Result<(), AnalysisError> {
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

pub(crate) fn write_window_csv(report: &AnalysisReport) -> Result<(), AnalysisError> {
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

pub(crate) fn write_group_window_csv(report: &AnalysisReport) -> Result<(), AnalysisError> {
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
