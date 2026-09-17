//! External, manifest-driven experiment orchestration.

mod manifest;
mod resources;
mod runner;
mod workload;

pub use manifest::{
    AnalysisWindow, CollectionEndpoint, CollectionPhase, ExecutionOrder, ExperimentManifest,
    ExternalCommand, FailureEvent, FixtureChange, ScenarioManifest, ServerLaunch, WorkloadManifest,
};
pub use runner::{ExperimentReport, PlannedRun, RunnerOptions, ScenarioRunner};
