use std::{env, path::PathBuf, process::ExitCode};

use server::{
    algorithms::AlgorithmKind,
    proxy::RuntimeMode,
    scenario::{ExperimentManifest, RunnerOptions, ScenarioRunner},
};

const HELP: &str = r#"Manifest-driven load-balancing experiment runner

Usage:
  scenario-runner validate MANIFEST
  scenario-runner run MANIFEST [OPTIONS]
  scenario-runner MANIFEST [OPTIONS]

Options:
  --scenario ID        Run only this scenario (repeatable)
  --algorithm NAME     Run only this algorithm (repeatable)
  --runtime NAME       Run only this runtime (repeatable)
  --repetitions N      Override the manifest repetition count
  --output DIRECTORY   Override the manifest output directory
  --dry-run            Validate and print the expanded matrix without executing it
  -h, --help           Show this help

Algorithms: round_robin, weighted_round_robin, least_connections,
            least_response_time, adaptive_balancing
Runtimes:   thread_pool, async
"#;

fn main() -> ExitCode {
    match run() {
        Ok(code) => code,
        Err(error) => {
            eprintln!("scenario-runner: {error}");
            ExitCode::from(2)
        }
    }
}

fn run() -> Result<ExitCode, String> {
    let mut arguments = env::args().skip(1).collect::<Vec<_>>();
    if arguments.is_empty()
        || arguments
            .iter()
            .any(|argument| argument == "-h" || argument == "--help")
    {
        print!("{HELP}");
        return Ok(ExitCode::SUCCESS);
    }

    let validate_only = arguments.first().map(String::as_str) == Some("validate");
    if validate_only || arguments.first().map(String::as_str) == Some("run") {
        arguments.remove(0);
    }
    let manifest_path = arguments
        .first()
        .filter(|argument| !argument.starts_with('-'))
        .map(PathBuf::from)
        .ok_or_else(|| "a manifest path is required".to_string())?;
    arguments.remove(0);
    let manifest = ExperimentManifest::load(&manifest_path).map_err(|error| error.to_string())?;

    if validate_only {
        if !arguments.is_empty() {
            return Err("validate accepts only a manifest path".into());
        }
        let runner = ScenarioRunner::new(manifest, RunnerOptions::default())
            .map_err(|error| error.to_string())?;
        println!(
            "valid: {} ({} planned runs)",
            runner.manifest().name,
            runner.plan().len()
        );
        return Ok(ExitCode::SUCCESS);
    }

    let mut options = RunnerOptions::default();
    let mut dry_run = false;
    let mut index = 0;
    while index < arguments.len() {
        match arguments[index].as_str() {
            "--scenario" => {
                options.scenarios.push(required_value(&arguments, index)?);
                index += 2;
            }
            "--algorithm" => {
                let value = required_value(&arguments, index)?;
                options.algorithms.push(
                    AlgorithmKind::from_str_name(&value)
                        .ok_or_else(|| format!("unknown algorithm: {value}"))?,
                );
                index += 2;
            }
            "--runtime" => {
                let value = required_value(&arguments, index)?;
                options.runtimes.push(
                    RuntimeMode::from_str_name(&value)
                        .ok_or_else(|| format!("unknown runtime: {value}"))?,
                );
                index += 2;
            }
            "--repetitions" => {
                let value = required_value(&arguments, index)?;
                options.repetitions = Some(
                    value
                        .parse::<usize>()
                        .map_err(|_| "--repetitions needs a positive integer".to_string())?,
                );
                index += 2;
            }
            "--output" => {
                options.output_directory = Some(PathBuf::from(required_value(&arguments, index)?));
                index += 2;
            }
            "--dry-run" => {
                dry_run = true;
                index += 1;
            }
            argument => return Err(format!("unknown argument: {argument}")),
        }
    }

    let runner = ScenarioRunner::new(manifest, options).map_err(|error| error.to_string())?;
    if dry_run {
        println!(
            "{}",
            serde_json::to_string_pretty(&runner.plan()).map_err(|error| error.to_string())?
        );
        return Ok(ExitCode::SUCCESS);
    }

    let report = runner.run().map_err(|error| error.to_string())?;
    println!(
        "experiment complete: {} succeeded, {} failed; results: {}",
        report.completed_runs,
        report.failed_runs,
        report.experiment_directory.display()
    );
    Ok(if report.failed_runs == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    })
}

fn required_value(arguments: &[String], index: usize) -> Result<String, String> {
    arguments
        .get(index + 1)
        .filter(|value| !value.starts_with("--"))
        .cloned()
        .ok_or_else(|| format!("{} needs a value", arguments[index]))
}
