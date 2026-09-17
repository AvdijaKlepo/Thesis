use std::{
    collections::{BTreeMap, HashSet},
    error::Error,
    fmt::{Display, Formatter},
    fs, io,
    net::SocketAddr,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::{algorithms::AlgorithmKind, config::AppConfig, proxy::RuntimeMode};

pub const MANIFEST_SCHEMA_VERSION: u32 = 1;

#[derive(Debug)]
pub enum ManifestError {
    Read { path: PathBuf, source: io::Error },
    Parse(toml::de::Error),
    Invalid(String),
}

impl Display for ManifestError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Read { path, source } => {
                write!(
                    formatter,
                    "failed to read manifest {}: {source}",
                    path.display()
                )
            }
            Self::Parse(source) => write!(formatter, "invalid experiment TOML: {source}"),
            Self::Invalid(message) => write!(formatter, "invalid experiment manifest: {message}"),
        }
    }
}

impl Error for ManifestError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Read { source, .. } => Some(source),
            Self::Parse(source) => Some(source),
            Self::Invalid(_) => None,
        }
    }
}

impl From<toml::de::Error> for ManifestError {
    fn from(value: toml::de::Error) -> Self {
        Self::Parse(value)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExperimentManifest {
    pub schema_version: u32,
    pub name: String,
    pub seed: u64,
    pub repetitions: usize,
    pub output_directory: PathBuf,
    pub algorithms: Vec<AlgorithmKind>,
    pub runtimes: Vec<RuntimeMode>,
    pub server: ServerLaunch,
    pub scenarios: Vec<ScenarioManifest>,
    #[serde(skip)]
    pub manifest_path: PathBuf,
}

impl ExperimentManifest {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ManifestError> {
        let path = path.as_ref();
        let contents = fs::read_to_string(path).map_err(|source| ManifestError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        let base = absolute_path(path.parent().unwrap_or_else(|| Path::new(".")))?;
        let mut manifest = Self::from_toml(&contents)?;
        manifest.manifest_path = absolute_path(path)?;
        manifest.resolve_paths(&base);
        manifest.validate_files()?;
        Ok(manifest)
    }

    pub fn from_toml(contents: &str) -> Result<Self, ManifestError> {
        let manifest: Self = toml::from_str(contents)?;
        manifest.validate()?;
        Ok(manifest)
    }

    fn resolve_paths(&mut self, base: &Path) {
        resolve_path(&mut self.output_directory, base);
        if let Some(directory) = &mut self.server.working_directory {
            resolve_path(directory, base);
        }
        for scenario in &mut self.scenarios {
            resolve_path(&mut scenario.server_config, base);
            for command in scenario
                .setup
                .iter_mut()
                .chain(scenario.teardown.iter_mut())
                .chain(
                    scenario
                        .failures
                        .iter_mut()
                        .map(|failure| &mut failure.action),
                )
            {
                if let Some(directory) = &mut command.working_directory {
                    resolve_path(directory, base);
                }
            }
        }
    }

    fn validate(&self) -> Result<(), ManifestError> {
        if self.schema_version != MANIFEST_SCHEMA_VERSION {
            return Err(ManifestError::Invalid(format!(
                "unsupported schema_version {}; expected {MANIFEST_SCHEMA_VERSION}",
                self.schema_version
            )));
        }
        if self.name.trim().is_empty() {
            return Err(ManifestError::Invalid("name must not be empty".into()));
        }
        if self.repetitions == 0 {
            return Err(ManifestError::Invalid(
                "repetitions must be greater than zero".into(),
            ));
        }
        if self.algorithms.is_empty() || self.runtimes.is_empty() || self.scenarios.is_empty() {
            return Err(ManifestError::Invalid(
                "algorithms, runtimes, and scenarios must not be empty".into(),
            ));
        }
        if self.output_directory.as_os_str().is_empty() {
            return Err(ManifestError::Invalid(
                "output_directory must not be empty".into(),
            ));
        }
        self.server.validate()?;

        ensure_unique(
            self.algorithms.iter().map(|value| value.as_str()),
            "algorithm",
        )?;
        ensure_unique(self.runtimes.iter().map(RuntimeMode::as_str), "runtime")?;
        ensure_unique(
            self.scenarios.iter().map(|value| value.id.as_str()),
            "scenario",
        )?;
        for scenario in &self.scenarios {
            scenario.validate()?;
        }
        Ok(())
    }

    fn validate_files(&self) -> Result<(), ManifestError> {
        for scenario in &self.scenarios {
            if !scenario.server_config.is_file() {
                return Err(ManifestError::Invalid(format!(
                    "server_config for scenario '{}' is not a file: {}",
                    scenario.id,
                    scenario.server_config.display()
                )));
            }
            let config = AppConfig::load(&scenario.server_config).map_err(|error| {
                ManifestError::Invalid(format!(
                    "server_config for scenario '{}' is invalid: {error}",
                    scenario.id
                ))
            })?;
            for failure in &scenario.failures {
                let Some(target) = failure.target_backend_id.as_deref() else {
                    continue;
                };
                let exists = config
                    .services
                    .iter()
                    .flat_map(|service| &service.backends)
                    .any(|backend| backend.id == target);
                if !exists {
                    return Err(ManifestError::Invalid(format!(
                        "failure '{}' targets backend '{}' which is not present in scenario '{}'",
                        failure.id, target, scenario.id
                    )));
                }
            }
        }
        if let Some(directory) = &self.server.working_directory
            && !directory.is_dir()
        {
            return Err(ManifestError::Invalid(format!(
                "server working_directory is not a directory: {}",
                directory.display()
            )));
        }
        Ok(())
    }
}

fn absolute_path(path: &Path) -> Result<PathBuf, ManifestError> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        std::env::current_dir()
            .map(|directory| directory.join(path))
            .map_err(|source| ManifestError::Read {
                path: path.to_path_buf(),
                source,
            })
    }
}

fn resolve_path(path: &mut PathBuf, base: &Path) {
    if path.is_relative() {
        *path = base.join(&*path);
    }
}

fn ensure_unique<'a>(
    values: impl IntoIterator<Item = &'a str>,
    label: &str,
) -> Result<(), ManifestError> {
    let mut seen = HashSet::new();
    for value in values {
        if !seen.insert(value) {
            return Err(ManifestError::Invalid(format!(
                "duplicate {label}: {value}"
            )));
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ServerLaunch {
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    pub working_directory: Option<PathBuf>,
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
    #[serde(default = "default_startup_timeout_ms")]
    pub startup_timeout_ms: u64,
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,
    pub version: Option<String>,
}

impl ServerLaunch {
    fn validate(&self) -> Result<(), ManifestError> {
        if self.command.trim().is_empty() {
            return Err(ManifestError::Invalid(
                "server.command must not be empty".into(),
            ));
        }
        if self.startup_timeout_ms == 0 || self.request_timeout_ms == 0 {
            return Err(ManifestError::Invalid(
                "server timeouts must be greater than zero".into(),
            ));
        }
        validate_environment(&self.environment, "server environment")
    }
}

fn default_startup_timeout_ms() -> u64 {
    15_000
}

fn default_request_timeout_ms() -> u64 {
    5_000
}

fn parse_address(label: &str, value: &str) -> Result<SocketAddr, ManifestError> {
    value.parse::<SocketAddr>().map_err(|_| {
        ManifestError::Invalid(format!("{label} must be an IP socket address: {value}"))
    })
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScenarioManifest {
    pub id: String,
    #[serde(default)]
    pub description: String,
    pub server_config: PathBuf,
    #[serde(default)]
    pub setup: Vec<ExternalCommand>,
    #[serde(default)]
    pub teardown: Vec<ExternalCommand>,
    pub workloads: Vec<WorkloadManifest>,
    #[serde(default)]
    pub failures: Vec<FailureEvent>,
    #[serde(default)]
    pub collect: Vec<CollectionEndpoint>,
}

impl ScenarioManifest {
    fn validate(&self) -> Result<(), ManifestError> {
        validate_id("scenario", &self.id)?;
        if self.server_config.as_os_str().is_empty() {
            return Err(ManifestError::Invalid(format!(
                "scenario '{}' needs server_config",
                self.id
            )));
        }
        if self.workloads.is_empty() {
            return Err(ManifestError::Invalid(format!(
                "scenario '{}' needs at least one workload",
                self.id
            )));
        }
        ensure_unique(
            self.workloads.iter().map(|value| value.id.as_str()),
            "workload id",
        )?;
        ensure_unique(
            self.failures.iter().map(|value| value.id.as_str()),
            "failure id",
        )?;
        ensure_unique(
            self.collect.iter().map(|value| value.id.as_str()),
            "collection id",
        )?;
        for command in self.setup.iter().chain(&self.teardown) {
            command.validate()?;
        }
        for workload in &self.workloads {
            workload.validate()?;
        }
        for failure in &self.failures {
            failure.validate()?;
        }
        for endpoint in &self.collect {
            endpoint.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalCommand {
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    pub working_directory: Option<PathBuf>,
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
    #[serde(default)]
    pub allow_failure: bool,
}

impl ExternalCommand {
    fn validate(&self) -> Result<(), ManifestError> {
        if self.command.trim().is_empty() {
            return Err(ManifestError::Invalid(
                "external command must not be empty".into(),
            ));
        }
        validate_environment(&self.environment, "command environment")
    }
}

fn validate_environment(
    environment: &BTreeMap<String, String>,
    label: &str,
) -> Result<(), ManifestError> {
    if let Some(key) = environment
        .keys()
        .find(|key| key.is_empty() || key.contains('=') || key.contains('\0'))
    {
        return Err(ManifestError::Invalid(format!(
            "{label} contains invalid key '{key}'"
        )));
    }
    Ok(())
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkloadManifest {
    pub id: String,
    pub requests: usize,
    pub concurrency: usize,
    #[serde(default = "default_method")]
    pub method: String,
    #[serde(default = "default_path")]
    pub path: String,
    pub host: Option<String>,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default)]
    pub body: String,
    #[serde(default)]
    pub start_after_ms: u64,
    pub requests_per_second: Option<f64>,
    #[serde(default)]
    pub jitter_ms: u64,
    pub timeout_ms: Option<u64>,
    #[serde(default = "default_max_response_bytes")]
    pub max_response_bytes: usize,
}

impl WorkloadManifest {
    fn validate(&self) -> Result<(), ManifestError> {
        validate_id("workload", &self.id)?;
        if self.requests == 0 || self.concurrency == 0 {
            return Err(ManifestError::Invalid(format!(
                "workload '{}' requests and concurrency must be greater than zero",
                self.id
            )));
        }
        if self.concurrency > self.requests {
            return Err(ManifestError::Invalid(format!(
                "workload '{}' concurrency cannot exceed requests",
                self.id
            )));
        }
        if self.path.is_empty() || !self.path.starts_with('/') || self.path.contains(['\r', '\n']) {
            return Err(ManifestError::Invalid(format!(
                "workload '{}' path must start with '/' and contain no line breaks",
                self.id
            )));
        }
        if self.method.is_empty()
            || !self
                .method
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte == b'-')
        {
            return Err(ManifestError::Invalid(format!(
                "workload '{}' method must be an uppercase HTTP token",
                self.id
            )));
        }
        if self
            .host
            .as_ref()
            .is_some_and(|host| host.is_empty() || host.contains(['\r', '\n']))
        {
            return Err(ManifestError::Invalid(format!(
                "workload '{}' has an invalid host header",
                self.id
            )));
        }
        if let Some(rate) = self.requests_per_second
            && (!rate.is_finite() || rate <= 0.0)
        {
            return Err(ManifestError::Invalid(format!(
                "workload '{}' requests_per_second must be finite and positive",
                self.id
            )));
        }
        if self.timeout_ms == Some(0) || self.max_response_bytes == 0 {
            return Err(ManifestError::Invalid(format!(
                "workload '{}' timeout and response limit must be positive",
                self.id
            )));
        }
        for (name, value) in &self.headers {
            if !is_http_token(name) || value.contains(['\r', '\n']) {
                return Err(ManifestError::Invalid(format!(
                    "workload '{}' has an invalid header",
                    self.id
                )));
            }
        }
        Ok(())
    }
}

fn is_http_token(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    byte,
                    b'!' | b'#'
                        | b'$'
                        | b'%'
                        | b'&'
                        | b'\''
                        | b'*'
                        | b'+'
                        | b'-'
                        | b'.'
                        | b'^'
                        | b'_'
                        | b'`'
                        | b'|'
                        | b'~'
                )
        })
}

fn default_method() -> String {
    "GET".into()
}

fn default_path() -> String {
    "/".into()
}

fn default_max_response_bytes() -> usize {
    1024 * 1024
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FailureEvent {
    pub id: String,
    pub at_ms: u64,
    pub target_backend_id: Option<String>,
    pub action: ExternalCommand,
}

impl FailureEvent {
    fn validate(&self) -> Result<(), ManifestError> {
        validate_id("failure", &self.id)?;
        if self
            .target_backend_id
            .as_ref()
            .is_some_and(|id| id.trim().is_empty())
        {
            return Err(ManifestError::Invalid(format!(
                "failure '{}' target_backend_id must not be empty",
                self.id
            )));
        }
        self.action.validate()
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CollectionPhase {
    Before,
    After,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CollectionEndpoint {
    pub id: String,
    pub address: String,
    #[serde(default = "default_path")]
    pub path: String,
    #[serde(default = "default_collection_phases")]
    pub phases: Vec<CollectionPhase>,
    pub timeout_ms: Option<u64>,
}

impl CollectionEndpoint {
    pub fn socket(&self) -> Result<SocketAddr, ManifestError> {
        parse_address("collection address", &self.address)
    }

    fn validate(&self) -> Result<(), ManifestError> {
        validate_id("collection", &self.id)?;
        self.socket()?;
        if !self.path.starts_with('/') || self.path.contains(['\r', '\n']) {
            return Err(ManifestError::Invalid(format!(
                "collection '{}' has an invalid path",
                self.id
            )));
        }
        if self.phases.is_empty() {
            return Err(ManifestError::Invalid(format!(
                "collection '{}' needs at least one phase",
                self.id
            )));
        }
        ensure_unique(
            self.phases.iter().map(|phase| match phase {
                CollectionPhase::Before => "before",
                CollectionPhase::After => "after",
            }),
            "collection phase",
        )?;
        if self.timeout_ms == Some(0) {
            return Err(ManifestError::Invalid(format!(
                "collection '{}' timeout must be positive",
                self.id
            )));
        }
        Ok(())
    }
}

fn default_collection_phases() -> Vec<CollectionPhase> {
    vec![CollectionPhase::Before, CollectionPhase::After]
}

fn validate_id(label: &str, value: &str) -> Result<(), ManifestError> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(ManifestError::Invalid(format!(
            "{label} id '{value}' must use only letters, digits, '-' and '_'"
        )));
    }
    Ok(())
}
#[cfg(test)]
#[path = "manifest_tests.rs"]
mod tests;
