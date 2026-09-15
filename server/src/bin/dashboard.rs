use std::{env, path::PathBuf, process::ExitCode};

use server::dashboard::DashboardServer;

const HELP: &str = r#"View live and saved scenario-runner experiments

Usage:
  dashboard [OPTIONS]

Options:
  --results DIRECTORY  Scenario-runner results root (default: ../results)
  --address ADDRESS    Dashboard listen address (default: 127.0.0.1:7890)
  -h, --help           Show this help
"#;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("dashboard: {error}");
            ExitCode::from(2)
        }
    }
}

fn run() -> Result<(), String> {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    if arguments
        .iter()
        .any(|argument| argument == "-h" || argument == "--help")
    {
        print!("{HELP}");
        return Ok(());
    }
    let mut results = PathBuf::from("../results");
    let mut address = "127.0.0.1:7890".to_string();
    let mut index = 0;
    while index < arguments.len() {
        match arguments[index].as_str() {
            "--results" => {
                results = PathBuf::from(required_value(&arguments, index)?);
                index += 2;
            }
            "--address" => {
                address = required_value(&arguments, index)?;
                index += 2;
            }
            argument => return Err(format!("unknown argument: {argument}")),
        }
    }
    DashboardServer::new(address, results).run()
}

fn required_value(arguments: &[String], index: usize) -> Result<String, String> {
    arguments
        .get(index + 1)
        .filter(|value| !value.starts_with("--"))
        .cloned()
        .ok_or_else(|| format!("{} needs a value", arguments[index]))
}
