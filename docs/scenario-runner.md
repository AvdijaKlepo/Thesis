# Scenario runner

The scenario runner is the experiment boundary. Server TOML files describe listeners, services, routes, health checking, backends, and initial policies. Experiment manifests describe what to run against that server: the algorithm/runtime matrix, workloads, repetitions, seeds, external fixture setup, timed failures, recovery actions, and collection endpoints. The runner reads its proxy and management addresses from the referenced server TOML; those settings are not duplicated in the experiment manifest.

The web server does not invoke Docker or interpret an experiment manifest. The runner starts a new server process for each matrix cell so counters and adaptive algorithm state cannot leak between repetitions.

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

## Reproducibility artifacts

An experiment directory contains the original manifest, a resolved JSON snapshot, the fully expanded plan, a live `runs.json` index, and the final `experiment.json` report. Every run has its own directory containing:

- `metadata.json`: status, experiment and derived run seeds, runner/server versions, source revision and dirty flag, platform, selected parameters, and the complete scenario definition.
- `server-config.toml`: the exact operational configuration used for that run.
- `requests.jsonl`: one raw end-to-end measurement per generated request, including planned and actual start offsets, timestamps, latency, status, transport errors, byte count, and fixture/backend identifiers.
- `events.jsonl`: ordered server, workload, setup, teardown, failure, recovery, and collection events, including command output and exit status.
- `metrics-before.json` and `metrics-after.json`: untouched structured server snapshots.
- `collection-*.body` and `collection-*.json`: untouched endpoint bodies plus response metadata.
- `server.stdout.log` and `server.stderr.log`: exact process output, including per-request JSON events when server request logging is enabled.

`summary.json` is only a convenience count. Scientific analysis should remain derivable from the raw artifacts rather than treating the convenience summary as source data.
