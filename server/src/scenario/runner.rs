use std::{
    collections::BTreeMap,
    error::Error,
    fmt::{Display, Formatter},
    fs::{self, File},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

use serde::Serialize;
use serde_json::{Value, json};

use crate::{
    algorithms::AlgorithmKind,
    config::AppConfig,
    management::{HttpRequest, ManagementClient, send_http},
    proxy::RuntimeMode,
};

use super::{
    manifest::{
        CollectionEndpoint, CollectionPhase, ExecutionOrder, ExperimentManifest, ExternalCommand,
        ScenarioManifest,
    },
    resources::start_resource_monitor,
    workload::{RequestLog, RequestMeasurement, stable_hash, unix_timestamp_ms},
};

#[derive(Debug)]
pub struct RunnerError(String);

impl RunnerError {
    pub(super) fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl Display for RunnerError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for RunnerError {}

impl From<std::io::Error> for RunnerError {
    fn from(value: std::io::Error) -> Self {
        Self(value.to_string())
    }
}

impl From<serde_json::Error> for RunnerError {
    fn from(value: serde_json::Error) -> Self {
        Self(value.to_string())
    }
}

#[derive(Clone, Debug, Default)]
pub struct RunnerOptions {
    pub scenarios: Vec<String>,
    pub algorithms: Vec<AlgorithmKind>,
    pub runtimes: Vec<RuntimeMode>,
    pub repetitions: Option<usize>,
    pub output_directory: Option<PathBuf>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PlannedRun {
    pub ordinal: usize,
    pub total: usize,
    pub scenario: String,
    pub algorithm: AlgorithmKind,
    pub runtime: RuntimeMode,
    pub repetition: usize,
    pub seed: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct RunReport {
    pub run_id: String,
    pub directory: PathBuf,
    pub scenario: String,
    pub algorithm: AlgorithmKind,
    pub runtime: RuntimeMode,
    pub repetition: usize,
    pub seed: u64,
    pub status: RunStatus,
    pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Completed,
    Failed,
}

#[derive(Clone, Debug, Serialize)]
pub struct ExperimentReport {
    pub schema_version: u32,
    pub name: String,
    pub started_unix_ms: u64,
    pub completed_unix_ms: u64,
    pub experiment_directory: PathBuf,
    pub completed_runs: usize,
    pub failed_runs: usize,
    pub runs: Vec<RunReport>,
}

pub struct ScenarioRunner {
    manifest: ExperimentManifest,
    options: RunnerOptions,
}

impl ScenarioRunner {
    pub fn new(manifest: ExperimentManifest, options: RunnerOptions) -> Result<Self, RunnerError> {
        if options.repetitions == Some(0) {
            return Err(RunnerError::new(
                "repetition override must be greater than zero",
            ));
        }
        validate_filters(&manifest, &options)?;
        Ok(Self { manifest, options })
    }

    pub fn manifest(&self) -> &ExperimentManifest {
        &self.manifest
    }

    pub fn plan(&self) -> Vec<PlannedRun> {
        let scenarios = self
            .manifest
            .scenarios
            .iter()
            .filter(|scenario| {
                self.options.scenarios.is_empty()
                    || self.options.scenarios.iter().any(|id| id == &scenario.id)
            })
            .collect::<Vec<_>>();
        let algorithms = self
            .manifest
            .algorithms
            .iter()
            .copied()
            .filter(|algorithm| {
                self.options.algorithms.is_empty() || self.options.algorithms.contains(algorithm)
            })
            .collect::<Vec<_>>();
        let runtimes = self
            .manifest
            .runtimes
            .iter()
            .copied()
            .filter(|runtime| {
                self.options.runtimes.is_empty() || self.options.runtimes.contains(runtime)
            })
            .collect::<Vec<_>>();
        let repetitions = self
            .options
            .repetitions
            .unwrap_or(self.manifest.repetitions);
        let total = scenarios.len() * algorithms.len() * runtimes.len() * repetitions;
        let mut runs = Vec::with_capacity(total);

        match self.manifest.execution_order {
            ExecutionOrder::Declared => {
                for scenario in scenarios {
                    for algorithm in &algorithms {
                        for runtime in &runtimes {
                            for repetition in 1..=repetitions {
                                push_planned_run(
                                    &mut runs,
                                    total,
                                    &self.manifest,
                                    scenario,
                                    *algorithm,
                                    *runtime,
                                    repetition,
                                );
                            }
                        }
                    }
                }
            }
            ExecutionOrder::BlockedRandomized => {
                for scenario in scenarios {
                    for runtime in &runtimes {
                        for repetition in 1..=repetitions {
                            let mut block = algorithms.clone();
                            block.sort_by_key(|algorithm| {
                                deterministic_order_key(
                                    self.manifest.seed,
                                    &scenario.id,
                                    *runtime,
                                    repetition,
                                    *algorithm,
                                )
                            });
                            for algorithm in block {
                                push_planned_run(
                                    &mut runs,
                                    total,
                                    &self.manifest,
                                    scenario,
                                    algorithm,
                                    *runtime,
                                    repetition,
                                );
                            }
                        }
                    }
                }
            }
        }
        runs
    }

    pub fn run(&self) -> Result<ExperimentReport, RunnerError> {
        let started_unix_ms = unix_timestamp_ms();
        let root = self
            .options
            .output_directory
            .as_ref()
            .unwrap_or(&self.manifest.output_directory);
        fs::create_dir_all(root).map_err(|error| {
            RunnerError::new(format!(
                "failed to create result root {}: {error}",
                root.display()
            ))
        })?;
        let experiment_directory =
            create_experiment_directory(root, &self.manifest.name, started_unix_ms)?;
        copy_manifest_artifacts(&self.manifest, &experiment_directory)?;
        let plan = self.plan();
        write_json(experiment_directory.join("plan.json"), &plan)?;

        let mut reports = Vec::with_capacity(plan.len());
        for planned in plan {
            eprintln!(
                "[{}/{}] scenario={} algorithm={} runtime={} repetition={} seed={}",
                planned.ordinal,
                planned.total,
                planned.scenario,
                planned.algorithm.as_str(),
                planned.runtime.as_str(),
                planned.repetition,
                planned.seed
            );
            let scenario = self
                .manifest
                .scenarios
                .iter()
                .find(|scenario| scenario.id == planned.scenario)
                .expect("planned scenario came from the manifest");
            let report = self.run_one(&experiment_directory, &planned, scenario);
            reports.push(report);
            write_json(experiment_directory.join("runs.json"), &reports)?;
        }

        let completed_runs = reports
            .iter()
            .filter(|report| report.status == RunStatus::Completed)
            .count();
        let failed_runs = reports.len() - completed_runs;
        let report = ExperimentReport {
            schema_version: 1,
            name: self.manifest.name.clone(),
            started_unix_ms,
            completed_unix_ms: unix_timestamp_ms(),
            experiment_directory,
            completed_runs,
            failed_runs,
            runs: reports,
        };
        write_json(report.experiment_directory.join("experiment.json"), &report)?;
        Ok(report)
    }

    fn run_one(
        &self,
        experiment_directory: &Path,
        planned: &PlannedRun,
        scenario: &ScenarioManifest,
    ) -> RunReport {
        let run_id = format!(
            "{:04}-{}-{}-{}-r{:03}-s{}",
            planned.ordinal,
            sanitize_component(&planned.scenario),
            planned.algorithm.as_str(),
            planned.runtime.as_str(),
            planned.repetition,
            planned.seed
        );
        let run_directory = experiment_directory.join(&run_id);
        let report_base = || RunReport {
            run_id: run_id.clone(),
            directory: run_directory.clone(),
            scenario: planned.scenario.clone(),
            algorithm: planned.algorithm,
            runtime: planned.runtime,
            repetition: planned.repetition,
            seed: planned.seed,
            status: RunStatus::Failed,
            error: None,
        };
        if let Err(error) = fs::create_dir_all(&run_directory) {
            let mut report = report_base();
            report.error = Some(format!("failed to create run directory: {error}"));
            return report;
        }

        let started_unix_ms = unix_timestamp_ms();
        let run_started = Instant::now();
        let events = match EventLog::create(&run_directory) {
            Ok(events) => Arc::new(events),
            Err(error) => {
                let mut report = report_base();
                report.error = Some(error.to_string());
                return report;
            }
        };
        let context = PlaceholderContext {
            manifest_directory: self
                .manifest
                .manifest_path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .to_path_buf(),
            server_config: scenario.server_config.clone(),
            run_directory: run_directory.clone(),
            seed: planned.seed,
            scenario: planned.scenario.clone(),
            algorithm: planned.algorithm.as_str().into(),
            runtime: planned.runtime.as_str().into(),
            repetition: planned.repetition,
        };
        if let Err(error) = fs::copy(
            &scenario.server_config,
            run_directory.join("server-config.toml"),
        ) {
            let mut report = report_base();
            report.error = Some(format!("failed to preserve server configuration: {error}"));
            return report;
        }

        let source_revision_value = source_revision(&context.manifest_directory);
        let source_dirty_value = source_dirty(&context.manifest_directory);
        let metadata_start = RunMetadata {
            schema_version: 1,
            run_id: &run_id,
            experiment_name: &self.manifest.name,
            status: "running",
            started_unix_ms,
            completed_unix_ms: None,
            duration_ms: None,
            scenario,
            algorithm: planned.algorithm,
            runtime: planned.runtime,
            repetition: planned.repetition,
            experiment_seed: self.manifest.seed,
            run_seed: planned.seed,
            paired_workload_seeds: self.manifest.paired_workload_seeds,
            execution_order: self.manifest.execution_order,
            runner_version: env!("CARGO_PKG_VERSION"),
            server_version: self
                .manifest
                .server
                .version
                .as_deref()
                .unwrap_or(env!("CARGO_PKG_VERSION")),
            source_revision: source_revision_value.as_deref(),
            source_dirty: source_dirty_value,
            platform: std::env::consts::OS,
            architecture: std::env::consts::ARCH,
            error: None,
            summary: None,
            raw_artifacts: raw_artifacts(),
        };
        if let Err(error) = write_json(run_directory.join("metadata.json"), &metadata_start) {
            let mut report = report_base();
            report.error = Some(error.to_string());
            return report;
        }

        let mut server_process: Option<Child> = None;
        let execution = (|| -> Result<RunSummary, RunnerError> {
            let server_config = AppConfig::load(&scenario.server_config)
                .map_err(|error| RunnerError::new(error.to_string()))?;
            let admin_address = server_config
                .server
                .admin_address
                .parse()
                .map_err(|_| RunnerError::new("server config has an invalid admin address"))?;
            let proxy_address = server_config
                .server
                .proxy_address
                .parse()
                .map_err(|_| RunnerError::new("server config has an invalid proxy address"))?;
            for (index, action) in scenario.setup.iter().enumerate() {
                run_checked_command(
                    action,
                    &context,
                    &events,
                    None,
                    "setup",
                    &format!("setup-{}", index + 1),
                    None,
                )?;
            }

            server_process = Some(start_server(
                &self.manifest,
                scenario,
                &context,
                &run_directory,
                &events,
            )?);
            let management = ManagementClient::new(
                admin_address,
                Duration::from_millis(self.manifest.server.request_timeout_ms),
            );
            wait_for_server(
                server_process
                    .as_mut()
                    .expect("server process was assigned"),
                &management,
                Duration::from_millis(self.manifest.server.startup_timeout_ms),
            )?;
            events.record(
                "server_ready",
                "server",
                Some(true),
                json!({"admin_address": management.address()}),
                None,
            );

            management
                .set_service_algorithm(&server_config.server.default_service, planned.algorithm)
                .map_err(|error| RunnerError::new(error.to_string()))?;
            management
                .set_runtime(planned.runtime)
                .map_err(|error| RunnerError::new(error.to_string()))?;
            let observed_runtime = management
                .runtime()
                .map_err(|error| RunnerError::new(error.to_string()))?;
            if observed_runtime != planned.runtime {
                return Err(RunnerError::new(format!(
                    "server reported runtime {} after selecting {}",
                    observed_runtime.as_str(),
                    planned.runtime.as_str()
                )));
            }
            events.record(
                "server_configured",
                "management",
                Some(true),
                json!({
                    "service_id": server_config.server.default_service,
                    "algorithm": planned.algorithm,
                    "runtime": planned.runtime,
                }),
                None,
            );

            let metrics_before = management
                .metrics()
                .map_err(|error| RunnerError::new(error.to_string()))?;
            verify_algorithm(
                &metrics_before,
                &server_config.server.default_service,
                planned.algorithm,
            )?;
            write_json(run_directory.join("metrics-before.json"), &metrics_before)?;
            write_json(
                run_directory.join("adaptive-diagnostics-before.json"),
                &adaptive_diagnostics(&metrics_before),
            )?;
            collect_endpoints(
                scenario,
                CollectionPhase::Before,
                &run_directory,
                self.manifest.server.request_timeout_ms,
                &events,
            )?;

            let workload_origin = Instant::now();
            events.record(
                "workloads_started",
                "all",
                Some(true),
                json!({"count": scenario.workloads.len(), "seed": planned.seed}),
                Some(workload_origin),
            );
            let request_log = Arc::new(RequestLog::create(&run_directory)?);
            let resource_monitor = start_resource_monitor(
                server_process
                    .as_ref()
                    .expect("server process was assigned")
                    .id(),
                &run_directory,
                workload_origin,
            )?;
            let failure_handles = start_failures(
                scenario,
                context.clone(),
                Arc::clone(&events),
                workload_origin,
            );
            let fixture_change_handles = start_fixture_changes(
                scenario,
                workload_origin,
                Arc::clone(&events),
                self.manifest.server.request_timeout_ms,
            );
            let default_timeout = Duration::from_millis(self.manifest.server.request_timeout_ms);
            let workload_handles = scenario
                .workloads
                .iter()
                .cloned()
                .map(|workload| {
                    let events = Arc::clone(&events);
                    let workload_id = workload.id.clone();
                    let workload_seed = if self.manifest.paired_workload_seeds {
                        derive_workload_seed(
                            self.manifest.seed,
                            scenario.workload_seed_group.as_deref().unwrap_or("default"),
                            planned.runtime,
                            planned.repetition,
                            &workload.id,
                        )
                    } else {
                        planned.seed ^ stable_hash(&workload.id)
                    };
                    let request_log = Arc::clone(&request_log);
                    thread::spawn(move || {
                        let measurements = super::workload::run_workload(
                            workload,
                            proxy_address,
                            default_timeout,
                            workload_seed,
                            workload_origin,
                            request_log,
                        );
                        events.record(
                            "workload_completed",
                            &workload_id,
                            Some(true),
                            json!({"measurements": measurements.len()}),
                            Some(workload_origin),
                        );
                        measurements
                    })
                })
                .collect::<Vec<_>>();

            let mut measurements = Vec::new();
            let mut workload_errors = Vec::new();
            for handle in workload_handles {
                match handle.join() {
                    Ok(mut result) => measurements.append(&mut result),
                    Err(_) => workload_errors.push("workload worker panicked".to_string()),
                }
            }
            measurements.sort_by(|left, right| {
                left.started_offset_us
                    .cmp(&right.started_offset_us)
                    .then_with(|| left.workload_id.cmp(&right.workload_id))
                    .then_with(|| left.request_index.cmp(&right.request_index))
            });
            let mut failure_errors = Vec::new();
            for handle in failure_handles {
                match handle.join() {
                    Ok(Some(error)) => failure_errors.push(error),
                    Ok(None) => {}
                    Err(_) => failure_errors.push("failure scheduler panicked".into()),
                }
            }
            for handle in fixture_change_handles {
                match handle.join() {
                    Ok(Some(error)) => failure_errors.push(error),
                    Ok(None) => {}
                    Err(_) => failure_errors.push("fixture change scheduler panicked".into()),
                }
            }
            let resource_error = resource_monitor.stop().err();
            events.record(
                "workloads_completed",
                "all",
                Some(true),
                json!({"measurements": measurements.len()}),
                Some(workload_origin),
            );
            request_log.finish()?;
            if !workload_errors.is_empty() {
                return Err(RunnerError::new(workload_errors.join("; ")));
            }
            if let Some(error) = resource_error {
                return Err(error);
            }

            let metrics_after = management
                .metrics()
                .map_err(|error| RunnerError::new(error.to_string()))?;
            write_json(run_directory.join("metrics-after.json"), &metrics_after)?;
            write_json(
                run_directory.join("adaptive-diagnostics-after.json"),
                &adaptive_diagnostics(&metrics_after),
            )?;
            collect_endpoints(
                scenario,
                CollectionPhase::After,
                &run_directory,
                self.manifest.server.request_timeout_ms,
                &events,
            )?;

            let summary = summarize(&measurements, &metrics_before, &metrics_after);
            write_json(run_directory.join("summary.json"), &summary)?;
            if !failure_errors.is_empty() {
                return Err(RunnerError::new(failure_errors.join("; ")));
            }
            Ok(summary)
        })();

        if let Some(mut child) = server_process.take() {
            stop_server(&mut child, &events);
        }
        let mut cleanup_errors = Vec::new();
        for (index, action) in scenario.teardown.iter().enumerate() {
            if let Err(error) = run_checked_command(
                action,
                &context,
                &events,
                None,
                "teardown",
                &format!("teardown-{}", index + 1),
                None,
            ) {
                cleanup_errors.push(error.to_string());
            }
        }

        let mut execution = match (execution, cleanup_errors.is_empty()) {
            (Ok(summary), true) => Ok(summary),
            (Ok(_), false) => Err(RunnerError::new(cleanup_errors.join("; "))),
            (Err(error), true) => Err(error),
            (Err(error), false) => Err(RunnerError::new(format!(
                "{error}; cleanup: {}",
                cleanup_errors.join("; ")
            ))),
        };
        if let Err(event_error) = events.finish() {
            execution = match execution {
                Ok(_) => Err(event_error),
                Err(error) => Err(RunnerError::new(format!(
                    "{error}; event log: {event_error}"
                ))),
            };
        }

        let completed_unix_ms = unix_timestamp_ms();
        let duration_ms = run_started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        let source_revision_value = source_revision(&context.manifest_directory);
        let source_dirty_value = source_dirty(&context.manifest_directory);
        let (status, error, summary) = match &execution {
            Ok(summary) => ("completed", None, Some(summary)),
            Err(error) => ("failed", Some(error.to_string()), None),
        };
        let metadata = RunMetadata {
            schema_version: 1,
            run_id: &run_id,
            experiment_name: &self.manifest.name,
            status,
            started_unix_ms,
            completed_unix_ms: Some(completed_unix_ms),
            duration_ms: Some(duration_ms),
            scenario,
            algorithm: planned.algorithm,
            runtime: planned.runtime,
            repetition: planned.repetition,
            experiment_seed: self.manifest.seed,
            run_seed: planned.seed,
            paired_workload_seeds: self.manifest.paired_workload_seeds,
            execution_order: self.manifest.execution_order,
            runner_version: env!("CARGO_PKG_VERSION"),
            server_version: self
                .manifest
                .server
                .version
                .as_deref()
                .unwrap_or(env!("CARGO_PKG_VERSION")),
            source_revision: source_revision_value.as_deref(),
            source_dirty: source_dirty_value,
            platform: std::env::consts::OS,
            architecture: std::env::consts::ARCH,
            error: error.as_deref(),
            summary,
            raw_artifacts: raw_artifacts(),
        };
        let metadata_error = write_json(run_directory.join("metadata.json"), &metadata).err();

        let mut report = report_base();
        match (execution, metadata_error) {
            (Ok(_), None) => report.status = RunStatus::Completed,
            (Ok(_), Some(error)) => report.error = Some(error.to_string()),
            (Err(error), _) => report.error = Some(error.to_string()),
        }
        report
    }
}

fn validate_filters(
    manifest: &ExperimentManifest,
    options: &RunnerOptions,
) -> Result<(), RunnerError> {
    for scenario in &options.scenarios {
        if !manifest
            .scenarios
            .iter()
            .any(|candidate| candidate.id == *scenario)
        {
            return Err(RunnerError::new(format!(
                "unknown scenario filter: {scenario}"
            )));
        }
    }
    for algorithm in &options.algorithms {
        if !manifest.algorithms.contains(algorithm) {
            return Err(RunnerError::new(format!(
                "algorithm filter '{}' is not in the manifest",
                algorithm.as_str()
            )));
        }
    }
    for runtime in &options.runtimes {
        if !manifest.runtimes.contains(runtime) {
            return Err(RunnerError::new(format!(
                "runtime filter '{}' is not in the manifest",
                runtime.as_str()
            )));
        }
    }
    Ok(())
}

fn derive_run_seed(
    base: u64,
    scenario: &str,
    algorithm: AlgorithmKind,
    runtime: RuntimeMode,
    repetition: usize,
) -> u64 {
    let dimensions = format!(
        "{scenario}\0{}\0{}\0{repetition}",
        algorithm.as_str(),
        runtime.as_str()
    );
    let mut value = base ^ stable_hash(&dimensions);
    value = value.wrapping_add(0x9E3779B97F4A7C15);
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D049BB133111EB);
    value ^ (value >> 31)
}

fn derive_workload_seed(
    base: u64,
    workload_seed_group: &str,
    runtime: RuntimeMode,
    repetition: usize,
    workload_id: &str,
) -> u64 {
    let dimensions = format!(
        "{workload_seed_group}\0{}\0{repetition}\0{workload_id}",
        runtime.as_str()
    );
    let mut value = base ^ stable_hash(&dimensions);
    value = value.wrapping_add(0x9E3779B97F4A7C15);
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D049BB133111EB);
    value ^ (value >> 31)
}

fn deterministic_order_key(
    seed: u64,
    scenario: &str,
    runtime: RuntimeMode,
    repetition: usize,
    algorithm: AlgorithmKind,
) -> u64 {
    let dimensions = format!(
        "{seed}\0{scenario}\0{}\0{repetition}\0{}",
        runtime.as_str(),
        algorithm.as_str()
    );
    let mut value = stable_hash(&dimensions).wrapping_add(seed);
    value = value.wrapping_add(0x9E3779B97F4A7C15);
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D049BB133111EB);
    value ^ (value >> 31)
}

fn push_planned_run(
    runs: &mut Vec<PlannedRun>,
    total: usize,
    manifest: &ExperimentManifest,
    scenario: &ScenarioManifest,
    algorithm: AlgorithmKind,
    runtime: RuntimeMode,
    repetition: usize,
) {
    let ordinal = runs.len() + 1;
    runs.push(PlannedRun {
        ordinal,
        total,
        scenario: scenario.id.clone(),
        algorithm,
        runtime,
        repetition,
        seed: derive_run_seed(manifest.seed, &scenario.id, algorithm, runtime, repetition),
    });
}

fn copy_manifest_artifacts(
    manifest: &ExperimentManifest,
    experiment_directory: &Path,
) -> Result<(), RunnerError> {
    fs::copy(
        &manifest.manifest_path,
        experiment_directory.join("manifest.toml"),
    )
    .map_err(|error| RunnerError::new(format!("failed to preserve manifest: {error}")))?;
    write_json(
        experiment_directory.join("manifest.resolved.json"),
        manifest,
    )
}

fn create_experiment_directory(
    root: &Path,
    name: &str,
    started_unix_ms: u64,
) -> Result<PathBuf, RunnerError> {
    let base = format!("{}-{started_unix_ms}", sanitize_component(name));
    for suffix in 0..10_000_u32 {
        let component = if suffix == 0 {
            base.clone()
        } else {
            format!("{base}-{suffix}")
        };
        let candidate = root.join(component);
        match fs::create_dir(&candidate) {
            Ok(()) => return Ok(candidate),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(RunnerError::new(format!(
                    "failed to create experiment directory {}: {error}",
                    candidate.display()
                )));
            }
        }
    }
    Err(RunnerError::new(
        "could not allocate a unique experiment directory",
    ))
}

fn start_server(
    manifest: &ExperimentManifest,
    scenario: &ScenarioManifest,
    context: &PlaceholderContext,
    run_directory: &Path,
    events: &EventLog,
) -> Result<Child, RunnerError> {
    let launch = &manifest.server;
    let mut command = Command::new(expand(&launch.command, context));
    command.args(launch.args.iter().map(|argument| expand(argument, context)));
    if let Some(directory) = &launch.working_directory {
        command.current_dir(directory);
    }
    for (key, value) in &launch.environment {
        command.env(key, expand(value, context));
    }
    let stdout = File::create(run_directory.join("server.stdout.log"))?;
    let stderr = File::create(run_directory.join("server.stderr.log"))?;
    command.stdout(Stdio::from(stdout));
    command.stderr(Stdio::from(stderr));
    let rendered = render_command(&launch.command, &launch.args, context);
    let child = command.spawn().map_err(|error| {
        RunnerError::new(format!(
            "failed to start server for config {} using {rendered}: {error}",
            scenario.server_config.display()
        ))
    })?;
    events.record(
        "server_started",
        "server",
        Some(true),
        json!({"command": rendered, "pid": child.id()}),
        None,
    );
    Ok(child)
}

fn wait_for_server(
    child: &mut Child,
    management: &ManagementClient,
    timeout: Duration,
) -> Result<(), RunnerError> {
    let started = Instant::now();
    let mut last_error = String::new();
    while started.elapsed() < timeout {
        if let Some(status) = child.try_wait()? {
            return Err(RunnerError::new(format!(
                "server exited before becoming ready with {status}"
            )));
        }
        match management.runtime() {
            Ok(_) => return Ok(()),
            Err(error) => last_error = error.to_string(),
        }
        thread::sleep(Duration::from_millis(50));
    }
    Err(RunnerError::new(format!(
        "server did not become ready within {} ms: {last_error}",
        timeout.as_millis()
    )))
}

fn stop_server(child: &mut Child, events: &EventLog) {
    let status = match child.try_wait() {
        Ok(Some(status)) => Some(status.to_string()),
        Ok(None) => {
            let kill_error = child.kill().err().map(|error| error.to_string());
            let waited = child.wait().ok().map(|status| status.to_string());
            events.record(
                "server_stopped",
                "server",
                Some(kill_error.is_none()),
                json!({"status": waited, "error": kill_error}),
                None,
            );
            return;
        }
        Err(error) => {
            events.record(
                "server_stopped",
                "server",
                Some(false),
                json!({"error": error.to_string()}),
                None,
            );
            return;
        }
    };
    events.record(
        "server_stopped",
        "server",
        Some(true),
        json!({"status": status}),
        None,
    );
}

fn start_failures(
    scenario: &ScenarioManifest,
    context: PlaceholderContext,
    events: Arc<EventLog>,
    origin: Instant,
) -> Vec<thread::JoinHandle<Option<String>>> {
    scenario
        .failures
        .iter()
        .cloned()
        .map(|failure| {
            let context = context.clone();
            let events = Arc::clone(&events);
            thread::spawn(move || {
                sleep_until(origin + Duration::from_millis(failure.at_ms));
                run_checked_command(
                    &failure.action,
                    &context,
                    &events,
                    Some(origin),
                    "failure",
                    &failure.id,
                    failure.target_backend_id.as_deref(),
                )
                .err()
                .map(|error| error.to_string())
            })
        })
        .collect()
}

fn start_fixture_changes(
    scenario: &ScenarioManifest,
    origin: Instant,
    events: Arc<EventLog>,
    default_timeout_ms: u64,
) -> Vec<thread::JoinHandle<Option<String>>> {
    scenario
        .fixture_changes
        .iter()
        .cloned()
        .map(|change| {
            let events = Arc::clone(&events);
            thread::spawn(move || {
                sleep_until(origin + Duration::from_millis(change.at_ms));
                let address = match change.socket() {
                    Ok(address) => address,
                    Err(error) => {
                        let message = error.to_string();
                        events.record(
                            "fixture_change_started",
                            &change.id,
                            Some(false),
                            json!({"address": &change.address, "error": &message}),
                            Some(origin),
                        );
                        events.record(
                            "fixture_change_completed",
                            &change.id,
                            Some(false),
                            json!({"address": &change.address, "error": &message}),
                            Some(origin),
                        );
                        return (!change.allow_failure).then_some(message);
                    }
                };
                let body = match serde_json::to_vec(&change.patch) {
                    Ok(body) => body,
                    Err(error) => {
                        let message = error.to_string();
                        events.record(
                            "fixture_change_started",
                            &change.id,
                            Some(false),
                            json!({"address": &change.address, "error": &message}),
                            Some(origin),
                        );
                        events.record(
                            "fixture_change_completed",
                            &change.id,
                            Some(false),
                            json!({"address": &change.address, "error": &message}),
                            Some(origin),
                        );
                        return (!change.allow_failure).then_some(message);
                    }
                };
                let headers = BTreeMap::new();
                let before = match send_http(
                    address,
                    &HttpRequest {
                        method: "GET",
                        path: "/config",
                        host: &address.to_string(),
                        headers: &headers,
                        body: &[],
                        timeout: Duration::from_millis(default_timeout_ms),
                        max_response_bytes: 8 * 1024 * 1024,
                    },
                ) {
                    Ok(response) if (200..300).contains(&response.status_code) => {
                        serde_json::from_slice::<Value>(&response.body)
                            .unwrap_or_else(|_| Value::String(response.body_text()))
                    }
                    Ok(response) => {
                        let message = format!(
                            "fixture '{}' configuration returned HTTP {} before change",
                            change.id, response.status_code
                        );
                        events.record(
                            "fixture_change_started",
                            &change.id,
                            Some(false),
                            json!({
                                "address": &change.address,
                                "patch": &change.patch,
                                "error": &message,
                            }),
                            Some(origin),
                        );
                        events.record(
                            "fixture_change_completed",
                            &change.id,
                            Some(false),
                            json!({"address": &change.address, "error": &message}),
                            Some(origin),
                        );
                        return (!change.allow_failure).then_some(message);
                    }
                    Err(error) => {
                        let message = format!(
                            "fixture '{}' configuration could not be read before change: {error}",
                            change.id
                        );
                        events.record(
                            "fixture_change_started",
                            &change.id,
                            Some(false),
                            json!({
                                "address": &change.address,
                                "patch": &change.patch,
                                "error": &message,
                            }),
                            Some(origin),
                        );
                        events.record(
                            "fixture_change_completed",
                            &change.id,
                            Some(false),
                            json!({"address": &change.address, "error": &message}),
                            Some(origin),
                        );
                        return (!change.allow_failure).then_some(message);
                    }
                };
                events.record(
                    "fixture_change_started",
                    &change.id,
                    None,
                    json!({
                        "address": &change.address,
                        "patch": &change.patch,
                        "effective_before": &before,
                    }),
                    Some(origin),
                );
                let started = Instant::now();
                let mut headers = BTreeMap::new();
                headers.insert("Content-Type".into(), "application/json".into());
                let response = send_http(
                    address,
                    &HttpRequest {
                        method: "POST",
                        path: "/control",
                        host: &address.to_string(),
                        headers: &headers,
                        body: &body,
                        timeout: Duration::from_millis(default_timeout_ms),
                        max_response_bytes: 8 * 1024 * 1024,
                    },
                );
                let (success, details, error) = match response {
                    Ok(response) => {
                        let success = (200..300).contains(&response.status_code);
                        let effective_after = serde_json::from_slice::<Value>(&response.body)
                            .unwrap_or_else(|_| Value::String(response.body_text()));
                        let details = json!({
                            "address": address,
                            "patch": &change.patch,
                            "allow_failure": change.allow_failure,
                            "effective_before": &before,
                            "status_code": response.status_code,
                            "headers": response.headers,
                            "effective_after": &effective_after,
                            "body": &effective_after,
                            "bytes_received": response.bytes_received,
                            "truncated": response.truncated,
                            "duration_ms": started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
                        });
                        let error = (!success && !change.allow_failure).then(|| {
                            format!(
                                "fixture change '{}' returned HTTP {}",
                                change.id, response.status_code
                            )
                        });
                        (success, details, error)
                    }
                    Err(error) => (
                        false,
                        json!({
                            "address": address,
                            "patch": &change.patch,
                            "allow_failure": change.allow_failure,
                            "duration_ms": started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
                            "error": error.to_string(),
                        }),
                        (!change.allow_failure)
                            .then_some(format!("fixture change '{}' failed: {error}", change.id)),
                    ),
                };
                events.record(
                    "fixture_change_completed",
                    &change.id,
                    Some(success),
                    details,
                    Some(origin),
                );
                error
            })
        })
        .collect()
}

fn sleep_until(deadline: Instant) {
    let now = Instant::now();
    if deadline > now {
        thread::sleep(deadline.duration_since(now));
    }
}

fn run_checked_command(
    action: &ExternalCommand,
    context: &PlaceholderContext,
    events: &EventLog,
    origin: Option<Instant>,
    kind: &str,
    label: &str,
    target_backend_id: Option<&str>,
) -> Result<(), RunnerError> {
    let rendered = render_command(&action.command, &action.args, context);
    let started_details = match target_backend_id {
        Some(target) => json!({"command": &rendered, "target_backend_id": target}),
        None => json!({"command": &rendered}),
    };
    events.record(
        &format!("{kind}_started"),
        label,
        None,
        started_details,
        origin,
    );
    let started = Instant::now();
    let mut command = Command::new(expand(&action.command, context));
    command.args(action.args.iter().map(|argument| expand(argument, context)));
    if let Some(directory) = &action.working_directory {
        command.current_dir(directory);
    }
    for (key, value) in &action.environment {
        command.env(key, expand(value, context));
    }
    let result = match command.output() {
        Ok(output) => CommandExecution {
            command: rendered,
            success: output.status.success(),
            exit_code: output.status.code(),
            duration_ms: started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            error: None,
        },
        Err(error) => CommandExecution {
            command: rendered,
            success: false,
            exit_code: None,
            duration_ms: started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
            stdout: String::new(),
            stderr: String::new(),
            error: Some(error.to_string()),
        },
    };
    let mut completed_details = serde_json::to_value(&result).unwrap_or(Value::Null);
    if let (Some(target), Value::Object(details)) = (target_backend_id, &mut completed_details) {
        details.insert("target_backend_id".into(), Value::String(target.into()));
    }
    events.record(
        &format!("{kind}_completed"),
        label,
        Some(result.success),
        completed_details,
        origin,
    );
    if result.success || action.allow_failure {
        Ok(())
    } else {
        Err(RunnerError::new(format!(
            "{kind} command '{}' failed{}{}",
            result.command,
            result
                .exit_code
                .map(|code| format!(" with exit code {code}"))
                .unwrap_or_default(),
            result
                .error
                .as_ref()
                .map(|error| format!(": {error}"))
                .unwrap_or_default()
        )))
    }
}

fn collect_endpoints(
    scenario: &ScenarioManifest,
    phase: CollectionPhase,
    run_directory: &Path,
    default_timeout_ms: u64,
    events: &EventLog,
) -> Result<(), RunnerError> {
    for endpoint in scenario
        .collect
        .iter()
        .filter(|endpoint| endpoint.phases.contains(&phase))
    {
        collect_endpoint(endpoint, phase, run_directory, default_timeout_ms, events)?;
    }
    Ok(())
}

fn collect_endpoint(
    endpoint: &CollectionEndpoint,
    phase: CollectionPhase,
    run_directory: &Path,
    default_timeout_ms: u64,
    events: &EventLog,
) -> Result<(), RunnerError> {
    let phase_name = match phase {
        CollectionPhase::Before => "before",
        CollectionPhase::After => "after",
    };
    let prefix = format!("collection-{phase_name}-{}", endpoint.id);
    let address = endpoint
        .socket()
        .map_err(|error| RunnerError::new(error.to_string()))?;
    let headers = BTreeMap::new();
    let response = send_http(
        address,
        &HttpRequest {
            method: "GET",
            path: &endpoint.path,
            host: &address.to_string(),
            headers: &headers,
            body: &[],
            timeout: Duration::from_millis(endpoint.timeout_ms.unwrap_or(default_timeout_ms)),
            max_response_bytes: 8 * 1024 * 1024,
        },
    )
    .map_err(|error| RunnerError::new(format!("collection '{}': {error}", endpoint.id)))?;
    fs::write(run_directory.join(format!("{prefix}.body")), &response.body)?;
    let record = json!({
        "schema_version": 1,
        "id": endpoint.id,
        "phase": phase,
        "address": endpoint.address,
        "path": endpoint.path,
        "status_code": response.status_code,
        "headers": response.headers,
        "bytes_received": response.bytes_received,
        "truncated": response.truncated,
        "body_file": format!("{prefix}.body"),
    });
    write_json(run_directory.join(format!("{prefix}.json")), &record)?;
    events.record(
        "collection_completed",
        &endpoint.id,
        Some((200..300).contains(&response.status_code)),
        record,
        None,
    );
    if !(200..300).contains(&response.status_code) {
        return Err(RunnerError::new(format!(
            "collection '{}' returned HTTP {}",
            endpoint.id, response.status_code
        )));
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct PlaceholderContext {
    manifest_directory: PathBuf,
    server_config: PathBuf,
    run_directory: PathBuf,
    seed: u64,
    scenario: String,
    algorithm: String,
    runtime: String,
    repetition: usize,
}

fn expand(value: &str, context: &PlaceholderContext) -> String {
    value
        .replace(
            "{manifest_dir}",
            &context.manifest_directory.to_string_lossy(),
        )
        .replace("{server_config}", &context.server_config.to_string_lossy())
        .replace("{run_dir}", &context.run_directory.to_string_lossy())
        .replace("{seed}", &context.seed.to_string())
        .replace("{scenario}", &context.scenario)
        .replace("{algorithm}", &context.algorithm)
        .replace("{runtime}", &context.runtime)
        .replace("{repetition}", &context.repetition.to_string())
}

fn render_command(command: &str, args: &[String], context: &PlaceholderContext) -> String {
    std::iter::once(command)
        .chain(args.iter().map(String::as_str))
        .map(|part| shell_quote(&expand(part, context)))
        .collect::<Vec<_>>()
        .join(" ")
}

fn shell_quote(value: &str) -> String {
    if value.bytes().all(|byte| {
        byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'\\' | b'.' | b'-' | b'_' | b':')
    }) {
        value.into()
    } else {
        format!("{:?}", value)
    }
}

fn sanitize_component(value: &str) -> String {
    let sanitized = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '-'
            }
        })
        .collect::<String>();
    sanitized.trim_matches('-').to_string()
}

#[derive(Debug, Serialize)]
struct CommandExecution {
    command: String,
    success: bool,
    exit_code: Option<i32>,
    duration_ms: u64,
    stdout: String,
    stderr: String,
    error: Option<String>,
}

#[derive(Debug, Serialize)]
struct EventRecord {
    schema_version: u32,
    sequence: u64,
    timestamp_unix_ms: u64,
    elapsed_us: Option<u64>,
    event: String,
    label: String,
    success: Option<bool>,
    details: Value,
}

struct EventLog {
    state: Mutex<EventLogState>,
}

struct EventLogState {
    sequence: u64,
    writer: BufWriter<File>,
    error: Option<String>,
}

impl EventLog {
    fn create(run_directory: &Path) -> Result<Self, RunnerError> {
        let path = run_directory.join("events.jsonl");
        let file = File::create(&path).map_err(|error| {
            RunnerError::new(format!("failed to create {}: {error}", path.display()))
        })?;
        Ok(Self {
            state: Mutex::new(EventLogState {
                sequence: 0,
                writer: BufWriter::new(file),
                error: None,
            }),
        })
    }

    fn record(
        &self,
        event: &str,
        label: &str,
        success: Option<bool>,
        details: Value,
        origin: Option<Instant>,
    ) {
        let mut state = self.state.lock().unwrap();
        if state.error.is_some() {
            return;
        }
        let record = EventRecord {
            schema_version: 1,
            sequence: state.sequence,
            timestamp_unix_ms: unix_timestamp_ms(),
            elapsed_us: origin
                .map(|origin| origin.elapsed().as_micros().min(u128::from(u64::MAX)) as u64),
            event: event.into(),
            label: label.into(),
            success,
            details,
        };
        let result = (|| -> Result<(), String> {
            let mut line = serde_json::to_vec(&record).map_err(|error| error.to_string())?;
            line.push(b'\n');
            state
                .writer
                .write_all(&line)
                .map_err(|error| error.to_string())?;
            state.writer.flush().map_err(|error| error.to_string())
        })();
        match result {
            Ok(()) => state.sequence = state.sequence.saturating_add(1),
            Err(error) => state.error = Some(error.to_string()),
        }
    }

    fn finish(&self) -> Result<(), RunnerError> {
        let mut state = self.state.lock().unwrap();
        state.writer.flush()?;
        match &state.error {
            Some(error) => Err(RunnerError::new(format!(
                "failed to stream events.jsonl: {error}"
            ))),
            None => Ok(()),
        }
    }
}

#[derive(Debug, Serialize)]
struct RunSummary {
    total_requests: usize,
    transport_successes: usize,
    http_successes: usize,
    transport_errors: usize,
    http_errors: usize,
    metrics_before: Value,
    metrics_after: Value,
}

fn summarize(
    measurements: &[RequestMeasurement],
    metrics_before: &Value,
    metrics_after: &Value,
) -> RunSummary {
    let transport_successes = measurements
        .iter()
        .filter(|measurement| measurement.transport_success)
        .count();
    let http_successes = measurements
        .iter()
        .filter(|measurement| measurement.http_success)
        .count();
    RunSummary {
        total_requests: measurements.len(),
        transport_successes,
        http_successes,
        transport_errors: measurements.len() - transport_successes,
        http_errors: transport_successes - http_successes,
        metrics_before: metrics_before.clone(),
        metrics_after: metrics_after.clone(),
    }
}

fn verify_algorithm(
    metrics: &Value,
    default_service: &str,
    expected: AlgorithmKind,
) -> Result<(), RunnerError> {
    let observed = metrics
        .get("services")
        .and_then(Value::as_array)
        .and_then(|services| {
            services.iter().find(|service| {
                service.get("service_id").and_then(Value::as_str) == Some(default_service)
            })
        })
        .and_then(|service| service.get("algorithm"))
        .and_then(Value::as_str);
    if observed == Some(expected.as_str()) {
        Ok(())
    } else {
        Err(RunnerError::new(format!(
            "server reported algorithm {} for default service '{default_service}' after selecting {}",
            observed.unwrap_or("<missing>"),
            expected.as_str()
        )))
    }
}

fn adaptive_diagnostics(metrics: &Value) -> Value {
    let snapshots = metrics
        .get("services")
        .and_then(Value::as_array)
        .map(|services| {
            services
                .iter()
                .filter_map(|service| {
                    let diagnostics = service.get("adaptive_diagnostics")?;
                    if diagnostics.is_null() {
                        return None;
                    }
                    Some(json!({
                        "service_id": service.get("service_id"),
                        "diagnostics": diagnostics,
                    }))
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    Value::Array(snapshots)
}

#[derive(Serialize)]
struct RunMetadata<'a> {
    schema_version: u32,
    run_id: &'a str,
    experiment_name: &'a str,
    status: &'a str,
    started_unix_ms: u64,
    completed_unix_ms: Option<u64>,
    duration_ms: Option<u64>,
    scenario: &'a ScenarioManifest,
    algorithm: AlgorithmKind,
    runtime: RuntimeMode,
    repetition: usize,
    experiment_seed: u64,
    run_seed: u64,
    paired_workload_seeds: bool,
    execution_order: ExecutionOrder,
    runner_version: &'static str,
    server_version: &'a str,
    source_revision: Option<&'a str>,
    source_dirty: Option<bool>,
    platform: &'static str,
    architecture: &'static str,
    error: Option<&'a str>,
    summary: Option<&'a RunSummary>,
    raw_artifacts: Vec<&'static str>,
}

fn raw_artifacts() -> Vec<&'static str> {
    vec![
        "server-config.toml",
        "server.stdout.log",
        "server.stderr.log",
        "events.jsonl",
        "requests.jsonl",
        "resource-samples.jsonl",
        "metrics-before.json",
        "metrics-after.json",
        "adaptive-diagnostics-before.json",
        "adaptive-diagnostics-after.json",
        "collection-*.body",
        "collection-*.json",
    ]
}

fn source_revision(directory: &Path) -> Option<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(directory)
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let revision = String::from_utf8_lossy(&output.stdout).trim().to_string();
    (!revision.is_empty()).then_some(revision)
}

fn source_dirty(directory: &Path) -> Option<bool> {
    let output = Command::new("git")
        .arg("-C")
        .arg(directory)
        .args(["status", "--porcelain", "--untracked-files=all"])
        .output()
        .ok()?;
    output.status.success().then_some(!output.stdout.is_empty())
}

fn write_json(path: PathBuf, value: &impl Serialize) -> Result<(), RunnerError> {
    let file = File::create(&path).map_err(|error| {
        RunnerError::new(format!("failed to create {}: {error}", path.display()))
    })?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, value)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}
#[cfg(test)]
#[path = "tests/runner_tests.rs"]
mod tests;
