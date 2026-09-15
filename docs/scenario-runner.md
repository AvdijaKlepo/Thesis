# Scenario runner

The scenario runner is the experiment boundary. Server TOML files describe listeners, services, routes, health checking, backends, and initial policies. Experiment manifests describe what to run against that server: the algorithm/runtime matrix, workloads, repetitions, seeds, external fixture setup, timed failures, recovery actions, and collection endpoints. The runner reads its proxy and management addresses from the referenced server TOML; those settings are not duplicated in the experiment manifest.

The web server does not invoke Docker or interpret an experiment manifest. The runner starts a new server process for each matrix cell so counters and adaptive algorithm state cannot leak between repetitions.

For ready-to-run fairness, heterogeneous-capacity, variable-latency, saturation,
burst, retry, and recovery studies, see the [fixture experiment suite](experiment-scenarios.md).

## Build and validate

Build the server and runner before an experiment. Launching the built server directly also lets the runner terminate the exact process it started.

```text
cd server
cargo build --release --bin server --bin scenario-runner --bin backend-fixture
cargo run --bin scenario-runner -- validate ../experiments/failure-recovery.toml
```

Inspect the expanded matrix without starting the server, Docker, or workloads:

```text
cargo run --bin scenario-runner -- ../experiments/failure-recovery.toml --dry-run
```

Run the complete manifest:

```text
cargo run --release --bin scenario-runner -- run ../experiments/failure-recovery.toml
```

For a short smoke run, restrict any matrix dimension and override repetitions:

```text
cargo run --release --bin scenario-runner -- run ../experiments/failure-recovery.toml --scenario failure-recovery --algorithm round_robin --runtime async --repetitions 1
```

`--scenario`, `--algorithm`, and `--runtime` are repeatable. `--output` overrides the results root.

## Manifest model

Relative paths are resolved from the manifest directory. Commands are executed directly, without a shell. The following placeholders are expanded independently in command names, arguments, and environment values:

- `{manifest_dir}`
- `{server_config}`
- `{run_dir}`
- `{seed}`
- `{scenario}`
- `{algorithm}`
- `{runtime}`
- `{repetition}`

Each workload has a fixed request count and concurrency. `requests_per_second` adds deterministic pacing; `jitter_ms` adds deterministic, seed-derived positive jitter. Multiple `[[scenarios.workloads]]` entries begin from the same monotonic clock and therefore run simultaneously. Timed `[[scenarios.failures]]` actions use that same clock. A non-zero external command fails its run unless `allow_failure = true` is set on that command.

The example's Compose setup and recovery commands wait for fixture health before returning, keeping container startup races out of the measurement window.

Collection endpoints are plain HTTP address/path pairs and can run `before`, `after`, or in both phases. They are suited to fixture `/config` and `/metrics` endpoints; the checked-in example captures effective fixture configuration so its seeds and workload parameters remain attached to the result. Server-wide structured metrics are always captured before and after the workload through the management API.

The runner uses the same typed `server::management::ManagementClient` as `serverctl`. Runtime and default-service algorithm changes therefore exercise the same management surface documented in [management-cli.md](management-cli.md); the runner does not depend on CLI output or spawn the CLI as a subprocess.

## Reproducibility artifacts

An experiment directory contains the original manifest, a resolved JSON snapshot, the fully expanded plan, a live `runs.json` index, and the final `experiment.json` report. Every run has its own directory containing:

- `metadata.json`: status, experiment and derived run seeds, runner/server versions, source revision and dirty flag, platform, selected parameters, and the complete scenario definition.
- `server-config.toml`: the exact operational configuration used for that run.
- `requests.jsonl`: one raw end-to-end measurement per generated request, including planned and actual start offsets, timestamps, latency, status, transport errors, byte count, and fixture/backend identifiers.
- `resource-samples.jsonl`: raw 100 ms server-process CPU-time and memory samples during the measured workload.
- `events.jsonl`: ordered server, workload, setup, teardown, failure, recovery, and collection events, including command output and exit status.
- `metrics-before.json` and `metrics-after.json`: untouched structured server snapshots.
- `collection-*.body` and `collection-*.json`: untouched endpoint bodies plus response metadata.
- `server.stdout.log` and `server.stderr.log`: exact process output, including per-request JSON events when server request logging is enabled.

`summary.json` is only a convenience count. Scientific analysis should remain derivable from the raw artifacts rather than treating the convenience summary as source data.

`events.jsonl`, `requests.jsonl`, and `resource-samples.jsonl` are flushed as
observations happen. The [live dashboard](live-dashboard.md) follows those same
files while a run is active; no separate telemetry collector or metric schema is
introduced for the UI.

## Analyze an experiment

Build and run the analyzer against a completed experiment directory:

```text
cd server
cargo run --release --bin analyze-results -- ../results/failure-recovery-EXPERIMENT_ID
```

By default, failed and partial runs remain visible in `run-summary.csv` and
`analysis.json` but are excluded from cross-repetition aggregates. Use
`--include-failed` only when the experimental protocol permits partial data.
`--output DIRECTORY` selects a different derived-output directory.

The analyzer leaves the experiment and every run directory untouched. Its
`analysis/` directory contains:

- `analysis.json`: run-level results, grouped statistics, calculation definitions,
  and a SHA-256 inventory of every raw input.
- `run-summary.csv`, `group-summary.csv`, and long-form `group-statistics.csv`:
  end-to-end throughput, transport and
  HTTP errors, interpolated p50/p90/p95/p99/p99.9 latency, and 95% confidence
  intervals across repetitions.
- `backend-fairness.csv` and `backend-fairness.json`: request-attempt deltas per
  backend, configured weights, normalized load, and Jain fairness indices.
- `resource-usage.csv`: server-process CPU time/utilization and resident/virtual
  memory summaries from `resource-samples.jsonl`.
- `recovery.csv`: time from a successful restore/recover/restart/resume/enable/start/heal/up
  action to the first successful response and to five consecutive successes.
- `raw-inputs.csv`: relative path, byte count, and SHA-256 digest for each source
  artifact used by the analysis.
- `plots/*.svg`: editable, vector, color-blind-safe plots suitable for print or
  direct thesis inclusion.

Recovery actions are identified by keywords in their event labels or standalone
command words. A command argument containing a compound project/file name such
as `failure-recovery` is not itself a recovery action. Studies without restoration
events still produce the other analyses; their recovery output is empty.

New experiment runs sample the server process every 100 ms during the workload
and preserve those observations as `resource-samples.jsonl`. Older runs remain
analyzable; their CPU and memory fields are explicitly null rather than inferred.
