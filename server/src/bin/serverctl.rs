use std::{
    env, fs,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    process::ExitCode,
    time::Duration,
};

use serde::Serialize;
use server::{
    Backend, RouteMatcher,
    algorithms::AlgorithmKind,
    management::{
        BackendPatch, BackendProbeResult, BackendStatus, ManagedServiceKind, ManagementClient,
        ServiceDefinition, ServicePatch, ServiceProbeResult, discover_loopback_ports,
    },
    proxy::RuntimeMode,
};

const HELP: &str = r#"Manage a running web server

Usage:
  serverctl [GLOBAL OPTIONS] service list
  serverctl [GLOBAL OPTIONS] service add ID --kind proxy|static --route PATH [OPTIONS]
  serverctl [GLOBAL OPTIONS] service update ID [OPTIONS]
  serverctl [GLOBAL OPTIONS] service remove ID
  serverctl [GLOBAL OPTIONS] service probe ID [--probe-timeout-ms N]
  serverctl [GLOBAL OPTIONS] backend list SERVICE
  serverctl [GLOBAL OPTIONS] backend add SERVICE ID ADDRESS [--weight N]
  serverctl [GLOBAL OPTIONS] backend update SERVICE ID [--address ADDRESS] [--weight N]
  serverctl [GLOBAL OPTIONS] backend remove SERVICE ID
  serverctl [GLOBAL OPTIONS] backend probe SERVICE ID [--probe-timeout-ms N]
  serverctl [GLOBAL OPTIONS] algorithm set SERVICE NAME
  serverctl [GLOBAL OPTIONS] runtime get|set [NAME]
  serverctl [GLOBAL OPTIONS] metrics
  serverctl [--json] discover ports --start PORT --end PORT [OPTIONS]

Global options:
  --admin ADDRESS       Management address (default: WEB_SERVER_ADMIN or 127.0.0.1:7880)
  --timeout-ms N        Management request timeout (default: 5000)
  --json                Emit machine-readable JSON
  -h, --help            Show this help

Service options:
  --route PATH          Add a host-agnostic route; repeatable
  --host-route HOST PATH
                        Add a host-specific route; repeatable
  --algorithm NAME      Proxy policy (default on add: round_robin)
  --fail-open           Permit unhealthy backends when no healthy one exists
  --fail-closed         Exclude unhealthy backends
  --root DIRECTORY      Static root; relative paths are resolved by this CLI

Discovery options:
  --host IP             Must be a loopback IP (default: 127.0.0.1)
  --connect-timeout-ms N
                        Per-port timeout from 1 through 500 (default: 25)

Port discovery scans at most 256 loopback ports and never registers results.
"#;

fn main() -> ExitCode {
    match run() {
        Ok(code) => code,
        Err(error) => {
            eprintln!("serverctl: {error}");
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

    let mut cursor = Arguments::new(arguments);
    let mut admin_address = env::var("WEB_SERVER_ADMIN")
        .unwrap_or_else(|_| "127.0.0.1:7880".into())
        .parse::<SocketAddr>()
        .map_err(|_| "WEB_SERVER_ADMIN must be an IP socket address".to_string())?;
    let mut request_timeout = Duration::from_millis(5_000);
    let mut json = false;
    loop {
        match cursor.peek() {
            Some("--admin") => {
                cursor.next();
                let value = cursor.required("--admin")?;
                admin_address = value
                    .parse()
                    .map_err(|_| "--admin must be an IP socket address".to_string())?;
            }
            Some("--timeout-ms") => {
                cursor.next();
                request_timeout = Duration::from_millis(positive_u64(
                    &cursor.required("--timeout-ms")?,
                    "--timeout-ms",
                )?);
            }
            Some("--json") => {
                cursor.next();
                json = true;
            }
            _ => break,
        }
    }

    let resource = cursor.required("resource")?;
    if resource == "discover" {
        return discover_command(&mut cursor, json);
    }
    let client = ManagementClient::new(admin_address, request_timeout);
    let result = match resource.as_str() {
        "service" | "services" => service_command(&client, &mut cursor, json),
        "backend" | "backends" => backend_command(&client, &mut cursor, json),
        "algorithm" => algorithm_command(&client, &mut cursor, json),
        "runtime" => runtime_command(&client, &mut cursor, json),
        "metrics" => {
            cursor.finish()?;
            let metrics = client.metrics().map_err(|error| error.to_string())?;
            print_json(&metrics)?;
            Ok(ExitCode::SUCCESS)
        }
        _ => Err(format!("unknown resource: {resource}")),
    }?;
    Ok(result)
}

fn service_command(
    client: &ManagementClient,
    cursor: &mut Arguments,
    json: bool,
) -> Result<ExitCode, String> {
    match cursor.required("service operation")?.as_str() {
        "list" => {
            cursor.finish()?;
            let services = client.list_services().map_err(|error| error.to_string())?;
            if json {
                print_json(&services)?;
            } else {
                print_services(&services);
            }
            Ok(ExitCode::SUCCESS)
        }
        "add" => {
            let id = cursor.required("service id")?;
            let options = parse_service_options(cursor, true)?;
            let kind = options
                .kind
                .ok_or_else(|| "service add requires --kind proxy|static".to_string())?;
            if options.routes.is_empty() {
                return Err("service add requires at least one --route or --host-route".into());
            }
            let definition = ServiceDefinition {
                id,
                kind,
                routes: options.routes,
                algorithm: match kind {
                    ManagedServiceKind::Proxy => {
                        Some(options.algorithm.unwrap_or(AlgorithmKind::RoundRobin))
                    }
                    ManagedServiceKind::Static => options.algorithm,
                },
                fail_open: options.fail_open.unwrap_or(false),
                backends: Vec::new(),
                root: options.root,
            };
            let service = client
                .add_service(&definition)
                .map_err(|error| error.to_string())?;
            print_service(&service, json)?;
            Ok(ExitCode::SUCCESS)
        }
        "update" => {
            let id = cursor.required("service id")?;
            let options = parse_service_options(cursor, false)?;
            if options.kind.is_some() {
                return Err("service kind cannot be changed at runtime".into());
            }
            let patch = ServicePatch {
                routes: (!options.routes.is_empty()).then_some(options.routes),
                algorithm: options.algorithm,
                fail_open: options.fail_open,
                root: options.root,
            };
            if patch.is_empty() {
                return Err("service update requires at least one change".into());
            }
            let service = client
                .update_service(&id, &patch)
                .map_err(|error| error.to_string())?;
            print_service(&service, json)?;
            Ok(ExitCode::SUCCESS)
        }
        "remove" => {
            let id = cursor.required("service id")?;
            cursor.finish()?;
            let service = client
                .remove_service(&id)
                .map_err(|error| error.to_string())?;
            print_service(&service, json)?;
            Ok(ExitCode::SUCCESS)
        }
        "probe" => {
            let id = cursor.required("service id")?;
            let timeout = parse_probe_timeout(cursor)?;
            let probe = client
                .probe_service(&id, timeout)
                .map_err(|error| error.to_string())?;
            print_service_probe(&probe, json)?;
            Ok(if probe.available {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            })
        }
        operation => Err(format!("unknown service operation: {operation}")),
    }
}

fn backend_command(
    client: &ManagementClient,
    cursor: &mut Arguments,
    json: bool,
) -> Result<ExitCode, String> {
    match cursor.required("backend operation")?.as_str() {
        "list" => {
            let service_id = cursor.required("service id")?;
            cursor.finish()?;
            let backends = client
                .list_backends(&service_id)
                .map_err(|error| error.to_string())?;
            if json {
                print_json(&backends)?;
            } else {
                print_backends(&backends);
            }
            Ok(ExitCode::SUCCESS)
        }
        "add" => {
            let service_id = cursor.required("service id")?;
            let id = cursor.required("backend id")?;
            let address = cursor.required("backend address")?;
            let mut weight = 1;
            while let Some(option) = cursor.next() {
                match option.as_str() {
                    "--weight" => {
                        weight = positive_usize(&cursor.required("--weight")?, "--weight")?
                    }
                    _ => return Err(format!("unknown backend add option: {option}")),
                }
            }
            let backend = client
                .add_backend(
                    &service_id,
                    &Backend {
                        id,
                        address,
                        weight,
                    },
                )
                .map_err(|error| error.to_string())?;
            print_backend(&backend, json)?;
            Ok(ExitCode::SUCCESS)
        }
        "update" => {
            let service_id = cursor.required("service id")?;
            let backend_id = cursor.required("backend id")?;
            let mut patch = BackendPatch::default();
            while let Some(option) = cursor.next() {
                match option.as_str() {
                    "--address" => patch.address = Some(cursor.required("--address")?),
                    "--weight" => {
                        patch.weight =
                            Some(positive_usize(&cursor.required("--weight")?, "--weight")?)
                    }
                    _ => return Err(format!("unknown backend update option: {option}")),
                }
            }
            if patch.is_empty() {
                return Err("backend update requires --address or --weight".into());
            }
            let backend = client
                .update_backend(&service_id, &backend_id, &patch)
                .map_err(|error| error.to_string())?;
            print_backend(&backend, json)?;
            Ok(ExitCode::SUCCESS)
        }
        "remove" => {
            let service_id = cursor.required("service id")?;
            let backend_id = cursor.required("backend id")?;
            cursor.finish()?;
            let backend = client
                .remove_backend(&service_id, &backend_id)
                .map_err(|error| error.to_string())?;
            print_backend(&backend, json)?;
            Ok(ExitCode::SUCCESS)
        }
        "probe" => {
            let service_id = cursor.required("service id")?;
            let backend_id = cursor.required("backend id")?;
            let timeout = parse_probe_timeout(cursor)?;
            let probe = client
                .probe_backend(&service_id, &backend_id, timeout)
                .map_err(|error| error.to_string())?;
            print_backend_probe(&probe, json)?;
            Ok(if probe.reachable {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            })
        }
        operation => Err(format!("unknown backend operation: {operation}")),
    }
}

fn algorithm_command(
    client: &ManagementClient,
    cursor: &mut Arguments,
    json: bool,
) -> Result<ExitCode, String> {
    let operation = cursor.required("algorithm operation")?;
    if operation != "set" {
        return Err("algorithm supports only the set operation".into());
    }
    let service_id = cursor.required("service id")?;
    let name = cursor.required("algorithm name")?;
    cursor.finish()?;
    let algorithm =
        AlgorithmKind::from_str_name(&name).ok_or_else(|| format!("unknown algorithm: {name}"))?;
    let service = client
        .set_service_algorithm(&service_id, algorithm)
        .map_err(|error| error.to_string())?;
    print_service(&service, json)?;
    Ok(ExitCode::SUCCESS)
}

fn runtime_command(
    client: &ManagementClient,
    cursor: &mut Arguments,
    json: bool,
) -> Result<ExitCode, String> {
    match cursor.required("runtime operation")?.as_str() {
        "get" => {
            cursor.finish()?;
            let runtime = client.runtime().map_err(|error| error.to_string())?;
            if json {
                print_json(&serde_json::json!({"runtime": runtime}))?;
            } else {
                println!("{}", runtime.as_str());
            }
        }
        "set" => {
            let name = cursor.required("runtime name")?;
            cursor.finish()?;
            let runtime = RuntimeMode::from_str_name(&name)
                .ok_or_else(|| format!("unknown runtime: {name}"))?;
            client
                .set_runtime(runtime)
                .map_err(|error| error.to_string())?;
            if json {
                print_json(&serde_json::json!({"runtime": runtime}))?;
            } else {
                println!("runtime={}", runtime.as_str());
            }
        }
        operation => return Err(format!("unknown runtime operation: {operation}")),
    }
    Ok(ExitCode::SUCCESS)
}

fn discover_command(cursor: &mut Arguments, json: bool) -> Result<ExitCode, String> {
    if cursor.required("discovery resource")? != "ports" {
        return Err("discover supports only the ports resource".into());
    }
    let mut host: IpAddr = "127.0.0.1".parse().unwrap();
    let mut start = None;
    let mut end = None;
    let mut timeout = Duration::from_millis(25);
    while let Some(option) = cursor.next() {
        match option.as_str() {
            "--host" => {
                host = cursor
                    .required("--host")?
                    .parse()
                    .map_err(|_| "--host must be an IP address".to_string())?
            }
            "--start" => start = Some(port(&cursor.required("--start")?, "--start")?),
            "--end" => end = Some(port(&cursor.required("--end")?, "--end")?),
            "--connect-timeout-ms" => {
                timeout = Duration::from_millis(positive_u64(
                    &cursor.required("--connect-timeout-ms")?,
                    "--connect-timeout-ms",
                )?)
            }
            _ => return Err(format!("unknown discovery option: {option}")),
        }
    }
    let discovered = discover_loopback_ports(
        host,
        start.ok_or_else(|| "discover ports requires --start".to_string())?,
        end.ok_or_else(|| "discover ports requires --end".to_string())?,
        timeout,
    )
    .map_err(|error| error.to_string())?;
    if json {
        print_json(&discovered)?;
    } else if discovered.is_empty() {
        println!("No listening ports found in the requested loopback range.");
    } else {
        println!("{:<24} CONNECT_US", "ADDRESS");
        for port in discovered {
            println!("{:<24} {}", port.address, port.connect_latency_us);
        }
    }
    Ok(ExitCode::SUCCESS)
}

#[derive(Default)]
struct ServiceOptions {
    kind: Option<ManagedServiceKind>,
    routes: Vec<RouteMatcher>,
    algorithm: Option<AlgorithmKind>,
    fail_open: Option<bool>,
    root: Option<PathBuf>,
}

fn parse_service_options(
    cursor: &mut Arguments,
    allow_kind: bool,
) -> Result<ServiceOptions, String> {
    let mut options = ServiceOptions::default();
    while let Some(option) = cursor.next() {
        match option.as_str() {
            "--kind" if allow_kind => {
                options.kind = Some(match cursor.required("--kind")?.as_str() {
                    "proxy" => ManagedServiceKind::Proxy,
                    "static" => ManagedServiceKind::Static,
                    value => return Err(format!("unknown service kind: {value}")),
                });
            }
            "--kind" => return Err("service kind cannot be changed at runtime".into()),
            "--route" => options.routes.push(RouteMatcher {
                host: None,
                path_prefix: cursor.required("--route")?,
            }),
            "--host-route" => options.routes.push(RouteMatcher {
                host: Some(cursor.required("--host-route host")?),
                path_prefix: cursor.required("--host-route path")?,
            }),
            "--algorithm" => {
                let name = cursor.required("--algorithm")?;
                options.algorithm = Some(
                    AlgorithmKind::from_str_name(&name)
                        .ok_or_else(|| format!("unknown algorithm: {name}"))?,
                );
            }
            "--fail-open" => set_fail_mode(&mut options.fail_open, true)?,
            "--fail-closed" => set_fail_mode(&mut options.fail_open, false)?,
            "--root" => {
                let path = PathBuf::from(cursor.required("--root")?);
                let root = fs::canonicalize(&path).map_err(|error| {
                    format!("unable to resolve static root {}: {error}", path.display())
                })?;
                if !root.is_dir() {
                    return Err(format!(
                        "static root is not a directory: {}",
                        root.display()
                    ));
                }
                options.root = Some(root);
            }
            _ => return Err(format!("unknown service option: {option}")),
        }
    }
    Ok(options)
}

fn set_fail_mode(target: &mut Option<bool>, value: bool) -> Result<(), String> {
    if target.is_some_and(|current| current != value) {
        return Err("--fail-open and --fail-closed are mutually exclusive".into());
    }
    *target = Some(value);
    Ok(())
}

fn parse_probe_timeout(cursor: &mut Arguments) -> Result<Duration, String> {
    let mut timeout = Duration::from_millis(500);
    while let Some(option) = cursor.next() {
        match option.as_str() {
            "--probe-timeout-ms" => {
                timeout = Duration::from_millis(positive_u64(
                    &cursor.required("--probe-timeout-ms")?,
                    "--probe-timeout-ms",
                )?)
            }
            _ => return Err(format!("unknown probe option: {option}")),
        }
    }
    if timeout > Duration::from_secs(10) {
        return Err("--probe-timeout-ms cannot exceed 10000".into());
    }
    Ok(timeout)
}

fn print_services(services: &[ServiceDefinition]) {
    println!("{:<24} {:<8} {:<24} ROUTES", "ID", "KIND", "TARGET");
    for service in services {
        let target = match service.kind {
            ManagedServiceKind::Proxy => format!(
                "{} ({} backends)",
                service
                    .algorithm
                    .map(|algorithm| algorithm.as_str())
                    .unwrap_or("unknown"),
                service.backends.len()
            ),
            ManagedServiceKind::Static => service
                .root
                .as_ref()
                .map(|root| root.display().to_string())
                .unwrap_or_default(),
        };
        println!(
            "{:<24} {:<8} {:<24} {}",
            service.id,
            kind_name(service.kind),
            target,
            routes_text(&service.routes)
        );
    }
}

fn print_backends(backends: &[BackendStatus]) {
    println!("{:<24} {:<24} {:<8} HEALTHY", "ID", "ADDRESS", "WEIGHT");
    for backend in backends {
        println!(
            "{:<24} {:<24} {:<8} {}",
            backend.id, backend.address, backend.weight, backend.healthy
        );
    }
}

fn print_service(service: &ServiceDefinition, json: bool) -> Result<(), String> {
    if json {
        print_json(service)
    } else {
        print_services(std::slice::from_ref(service));
        Ok(())
    }
}

fn print_backend(backend: &BackendStatus, json: bool) -> Result<(), String> {
    if json {
        print_json(backend)
    } else {
        print_backends(std::slice::from_ref(backend));
        Ok(())
    }
}

fn print_service_probe(probe: &ServiceProbeResult, json: bool) -> Result<(), String> {
    if json {
        print_json(probe)
    } else {
        println!(
            "service={} kind={} available={}",
            probe.service_id,
            kind_name(probe.kind),
            probe.available
        );
        if let Some(error) = &probe.error {
            println!("error={error}");
        }
        print_backend_probes(&probe.backend_probes);
        Ok(())
    }
}

fn print_backend_probe(probe: &BackendProbeResult, json: bool) -> Result<(), String> {
    if json {
        print_json(probe)
    } else {
        print_backend_probes(std::slice::from_ref(probe));
        Ok(())
    }
}

fn print_backend_probes(probes: &[BackendProbeResult]) {
    if probes.is_empty() {
        return;
    }
    println!(
        "{:<20} {:<20} {:<24} {:<10} {:<10} LATENCY_US",
        "SERVICE", "BACKEND", "ADDRESS", "HEALTHY", "REACHABLE"
    );
    for probe in probes {
        println!(
            "{:<20} {:<20} {:<24} {:<10} {:<10} {}",
            probe.service_id,
            probe.backend_id,
            probe.address,
            probe.configured_healthy,
            probe.reachable,
            probe.latency_us
        );
        if let Some(error) = &probe.error {
            println!("  error: {error}");
        }
    }
}

fn routes_text(routes: &[RouteMatcher]) -> String {
    routes
        .iter()
        .map(|route| match &route.host {
            Some(host) => format!("{host}{}", route.path_prefix),
            None => route.path_prefix.clone(),
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn kind_name(kind: ManagedServiceKind) -> &'static str {
    match kind {
        ManagedServiceKind::Proxy => "proxy",
        ManagedServiceKind::Static => "static",
    }
}

fn print_json(value: &impl Serialize) -> Result<(), String> {
    println!(
        "{}",
        serde_json::to_string_pretty(value).map_err(|error| error.to_string())?
    );
    Ok(())
}

fn positive_u64(value: &str, label: &str) -> Result<u64, String> {
    value
        .parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| format!("{label} needs a positive integer"))
}

fn positive_usize(value: &str, label: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| format!("{label} needs a positive integer"))
}

fn port(value: &str, label: &str) -> Result<u16, String> {
    value
        .parse::<u16>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| format!("{label} needs a port from 1 through 65535"))
}

struct Arguments {
    values: Vec<String>,
    index: usize,
}

impl Arguments {
    fn new(values: Vec<String>) -> Self {
        Self { values, index: 0 }
    }

    fn peek(&self) -> Option<&str> {
        self.values.get(self.index).map(String::as_str)
    }

    fn next(&mut self) -> Option<String> {
        let value = self.values.get(self.index).cloned()?;
        self.index += 1;
        Some(value)
    }

    fn required(&mut self, label: &str) -> Result<String, String> {
        self.next().ok_or_else(|| format!("missing {label}"))
    }

    fn finish(&self) -> Result<(), String> {
        if let Some(value) = self.peek() {
            Err(format!("unexpected argument: {value}"))
        } else {
            Ok(())
        }
    }
}
#[cfg(test)]
#[path = "serverctl/tests.rs"]
mod tests;
