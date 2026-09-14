use std::{env, path::PathBuf, process::ExitCode};

use server::analysis::{AnalysisOptions, analyze_experiment};

const HELP: &str = r#"Analyze a scenario-runner experiment directory

Usage:
  analyze-results EXPERIMENT_DIRECTORY [OPTIONS]

Options:
  --output DIRECTORY   Write derived artifacts here (default: EXPERIMENT_DIRECTORY/analysis)
  --include-failed     Include analyzable failed/partial runs in aggregate statistics
  -h, --help           Show this help

The command never modifies raw run artifacts. It emits JSON, CSV, SHA-256 input
inventory, and vector SVG plots in a separate analysis directory.
"#;

fn main() -> ExitCode {
    match run() {
        Ok(code) => code,
        Err(error) => {
            eprintln!("analyze-results: {error}");
            ExitCode::from(2)
        }
    }
}

fn run() -> Result<ExitCode, String> {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    if arguments.is_empty()
        || arguments
            .iter()
            .any(|argument| argument == "-h" || argument == "--help")
    {
        print!("{HELP}");
        return Ok(ExitCode::SUCCESS);
    }
    let experiment_directory = arguments
        .first()
        .filter(|argument| !argument.starts_with('-'))
        .map(PathBuf::from)
        .ok_or_else(|| "an experiment directory is required".to_string())?;
    let mut options = AnalysisOptions::default();
    let mut index = 1;
    while index < arguments.len() {
        match arguments[index].as_str() {
            "--output" => {
                options.output_directory = Some(PathBuf::from(required_value(&arguments, index)?));
                index += 2;
            }
            "--include-failed" => {
                options.include_failed = true;
                index += 1;
            }
            argument => return Err(format!("unknown argument: {argument}")),
        }
    }

    let report =
        analyze_experiment(&experiment_directory, &options).map_err(|error| error.to_string())?;
    let included = report
        .runs
        .iter()
        .filter(|run| run.included_in_aggregates)
        .count();
    println!(
        "analysis complete: {} runs inspected, {} included in {} groups; outputs: {}",
        report.runs.len(),
        included,
        report.groups.len(),
        report.analysis_directory.display()
    );
    Ok(ExitCode::SUCCESS)
}

fn required_value(arguments: &[String], index: usize) -> Result<String, String> {
    arguments
        .get(index + 1)
        .filter(|value| !value.starts_with("--"))
        .cloned()
        .ok_or_else(|| format!("{} needs a value", arguments[index]))
}
