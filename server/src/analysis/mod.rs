use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub mod csv;
pub mod models;
pub mod plots;
pub mod stats;

pub use models::{
    AnalysisError, AnalysisOptions, AnalysisReport, BackendAnalysis, FairnessAnalysis,
    GroupAnalysis, GroupWindowAnalysis, LatencyAnalysis, Methodology, RecoveryAnalysis,
    ResourceAnalysis, RunAnalysis, SchedulingLagAnalysis, SloAnalysis, SourceArtifact,
    StatisticalSummary, WindowAnalysis, WorkloadAnalysis,
};

use self::csv::*;
use self::models::*;
use self::plots::*;
use self::stats::*;

const ANALYSIS_SCHEMA_VERSION: u32 = 2;
const STABLE_RECOVERY_SUCCESSES: usize = 5;

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
#[path = "tests/analysis_tests.rs"]
mod tests;
