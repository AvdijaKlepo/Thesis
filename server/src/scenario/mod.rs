//! External, manifest-driven experiment orchestration.

mod manifest;
mod runner;
mod workload;

pub use manifest::{
    CollectionEndpoint, CollectionPhase, ExperimentManifest, ExternalCommand, FailureEvent,
    ScenarioManifest, ServerLaunch, WorkloadManifest,
};
pub use runner::{ExperimentReport, PlannedRun, RunnerOptions, ScenarioRunner};
