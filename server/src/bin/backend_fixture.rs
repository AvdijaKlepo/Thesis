use std::{env, process::ExitCode};

use server::fixture::{FixtureConfig, FixtureServer, healthcheck};

const HELP: &str = r#"Controlled backend fixture for load-balancing experiments

Configuration is read from FIXTURE_* environment variables and may be overridden by flags:
  --id ID
  --listen IP:PORT
  --capacity N
  --latency-ms N
  --latency-jitter-ms N
  --processing-ms N
  --error-rate 0.0..1.0
  --error-mode status|disconnect
  --seed N

Utility:
  --healthcheck HOST:PORT
  --help
"#;

fn main() -> ExitCode {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    if arguments.iter().any(|argument| argument == "--help") {
        print!("{HELP}");
        return ExitCode::SUCCESS;
    }
    if arguments.first().map(String::as_str) == Some("--healthcheck") {
        let Some(address) = arguments.get(1) else {
            eprintln!("--healthcheck requires HOST:PORT");
            return ExitCode::from(2);
        };
        if arguments.len() != 2 {
            eprintln!("--healthcheck accepts exactly one HOST:PORT");
            return ExitCode::from(2);
        }
        return match healthcheck(address) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("Fixture healthcheck failed: {error}");
                ExitCode::FAILURE
            }
        };
    }

    let mut config = match FixtureConfig::from_environment() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("Invalid fixture environment: {error}");
            return ExitCode::from(2);
        }
    };
    if let Err(error) = config.apply_arguments(arguments) {
        eprintln!("Invalid fixture arguments: {error}");
        return ExitCode::from(2);
    }

    match serde_json::to_string(&config) {
        Ok(config) => eprintln!("Fixture configuration: {config}"),
        Err(error) => eprintln!("Unable to serialize fixture configuration: {error}"),
    }
    let server = match FixtureServer::new(config) {
        Ok(server) => server,
        Err(error) => {
            eprintln!("Invalid fixture configuration: {error}");
            return ExitCode::from(2);
        }
    };
    match server.run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Backend fixture stopped: {error}");
            ExitCode::FAILURE
        }
    }
}
