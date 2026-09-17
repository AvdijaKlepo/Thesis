# Fixture experiment suite

The original manifests remain the three-backend baseline. Phase 5 adds an
independent scaled-fixture suite that reuses the same backend image, proxy,
scenario runner, analyzer, and dashboard. Each study answers a different
load-balancing or host-bias question; failure recovery remains a separate
availability study.

## Choose a study

The baseline manifests compare four classical policies across both runtimes
with three repetitions by default; the adaptive baseline adds v1 on async. The
scaled suite uses async first, compares the four classical policies plus
`adaptive_balancing` and `adaptive_balancing_v2`, and uses paired workload
seeds with deterministic blocked randomization.

| Manifest | Scenario IDs | Workload design | Main observations | Full matrix |
| --- | --- | --- | --- | --- |
| [equal-capacity.toml](../experiments/equal-capacity.toml) | `equal-capacity` | Three identical backends; 600 GETs at 60 requests/s, concurrency 16. | Baseline latency, throughput, allocation fairness, runtime overhead. | 24 runs |
| [heterogeneous-capacity.toml](../experiments/heterogeneous-capacity.toml) | `heterogeneous-capacity` | Backend capacities and proxy weights 1:4:8; 1,600 GETs at 160 requests/s, concurrency 64. | Capacity-normalized fairness, queueing on the small backend, weighted/adaptive allocation. | 24 runs |
| [variable-latency.toml](../experiments/variable-latency.toml) | `variable-latency` | Fast, seeded-variable, and slow backends; 1,000 GETs at 100 requests/s, concurrency 64. | p95/p99 latency, slow-backend allocation, latency-aware scheduling. | 24 runs |
| [adaptive-balancing.toml](../experiments/adaptive-balancing.toml) | `adaptive-load-shift` | Five policies on the variable-latency profile; async only, three repetitions. | v1 adaptive learning and allocation during a load shift. | 15 runs |
| [saturation-sweep.toml](../experiments/saturation-sweep.toml) | `saturation-light`, `saturation-medium`, `saturation-overload` | Identical backends; 600 / 1,800 / 3,600 GETs at 60 / 180 / 360 requests/s. Concurrency stays 128 in all three cases. | Throughput plateau, tail-latency growth, errors, CPU/RSS, load-generator scheduling lag. | 72 runs |
| [burst-load.toml](../experiments/burst-load.toml) | `burst-load` | 1,200 background GETs at 60 requests/s, concurrency 16; another 1,200 GETs at 300 requests/s, concurrency 96, scheduled from second 6. | Tail latency under a transient spike, backlog draining, allocation and resource usage over time. | 24 runs |
| [retry-behavior.toml](../experiments/retry-behavior.toml) | `retry-get`, `retry-post` | Healthy, 35%-disconnecting, and always-disconnecting backends; separate GET and POST runs, each 400 requests at 40 requests/s, concurrency 16. | Client-visible errors versus upstream attempts, retry latency cost, method-dependent retry behavior. | 48 runs |
| [failure-recovery.toml](../experiments/failure-recovery.toml) | `failure-recovery` | 1,200 GETs at 150 requests/s; stop the healthy container at 2 s, restore at 4.5 s. | Outage impact and post-restore success timing. | 24 runs |

The Phase 5 suite contains 19 scenarios and 492 matrix cells. The original
manifests and their result directories are separate baselines. Run a smoke
subset first; these defaults are protocols for comparison, not a claim that
one repetition or a short arrival window is sufficient for every thesis
conclusion.

## Phase 5 scaled protocols

These manifests use `compose.scaled-fixtures.yml` and the matching files under
`server/config/fixtures`. Their setup and teardown commands use a distinct
Compose project name and non-overlapping loopback ports. Run profiles
sequentially because published ports are shared by the profiles.

| Manifest | Scenario IDs | Protocol | Full matrix |
| --- | --- | --- | ---: |
| [backend-cardinality-calibration.toml](../experiments/backend-cardinality-calibration.toml) | `calibration-3`, `calibration-6`, `calibration-12` | Round-robin async low-load controls at 24 GET/s; equal total fixture capacity 12. | 3 |
| [adaptive-v2-tuning.toml](../experiments/adaptive-v2-tuning.toml) | `tuning-balanced`, `tuning-deadline`, `tuning-latency` | Three predeclared v2 setting variants on six variable-latency backends; pilot seeds only. | 9 |
| [adaptive-cardinality.toml](../experiments/adaptive-cardinality.toml) | `cardinality-3`, `cardinality-6`, `cardinality-12` | Six policies, async, ten paired repetitions; variable-latency cohorts with total fixture capacity 12. | 180 |
| [equal-capacity-12.toml](../experiments/equal-capacity-12.toml) | `equal-capacity-12` | Twelve identical unit-capacity backends at 180 GET/s. | 30 |
| [heterogeneous-capacity-13.toml](../experiments/heterogeneous-capacity-13.toml) | `heterogeneous-capacity-13` | Thirteen unit-capacity replicas in 1/4/8 cohorts at 240 GET/s. | 30 |
| [variable-latency-12.toml](../experiments/variable-latency-12.toml) | `variable-latency-12` | Four replicas of each latency class at 180 GET/s. | 30 |
| [saturation-sweep-12.toml](../experiments/saturation-sweep-12.toml) | `saturation-light`, `saturation-medium`, `saturation-overload` | Twelve equal backends at 60, 180, and 360 GET/s; concurrency 128. | 90 |
| [burst-load-12.toml](../experiments/burst-load-12.toml) | `burst-load-12` | 60 GET/s background plus a 300 GET/s burst from 6-10 s. | 30 |
| [retry-behavior-12.toml](../experiments/retry-behavior-12.toml) | `retry-get`, `retry-post` | Separate idempotent GET and non-idempotent POST retry workloads on healthy/flaky/failing cohorts. | 60 |
| [adaptive-phase-change-12.toml](../experiments/adaptive-phase-change-12.toml) | `adaptive-phase-change-12` | Sixteen scheduled fast/slow cohort mutations at 5 s and restoration at 11 s. | 30 |

The cardinality study is the final ten-repetition comparison. The tuning,
fairness, heterogeneous, variable-latency, saturation, burst, retry, and
phase-change studies use five repetitions, while calibration uses one smoke
repetition. The tuning configurations are intentionally pilot-only; freeze a
choice before collecting final cardinality repetitions.

## Build, inspect, and run

Prerequisites: Rust, Docker with a running engine, and Docker Compose supporting `up --wait` and `start --wait`. Run the following from the repository root, then remain in `server/` for the other examples:

```powershell
cd server
cargo build --release --bins
```

Do not manually launch the proxy or fixtures for these automated experiments. The runner owns their startup and teardown. Keep proxy/control/admin ports 7879/7878/7880 and the selected profile's fixture ports free. Baseline profiles use 5101-5403; scaled profiles use 6101-6906. If you already run that profile manually, stop its specific containers yourself or make an isolated copy with different ports in both the Compose file and server/collection configuration.

Each baseline scenario uses a dedicated Compose project named `webserver-benchmark-{scenario}`; scaled scenarios use `webserver-scaled-{scenario}`. Setup builds and recreates only the selected fixtures, waits for them, then starts a fresh proxy. Teardown removes that scenario's Compose project, without targeting your normal fixture project. Project names isolate lifecycle, **not published ports**: run these experiments sequentially. An existing listener on a required port is an error, not a request to stop another application.

Validate and inspect a complete plan without starting Docker or sending traffic:

```powershell
./target/release/scenario-runner validate ../experiments/saturation-sweep.toml
./target/release/scenario-runner ../experiments/saturation-sweep.toml --dry-run
```

Run one baseline cell:

```powershell
./target/release/scenario-runner run ../experiments/equal-capacity.toml --algorithm round_robin --runtime async --repetitions 1
```

Compare all algorithms on the heterogeneous fixture using only the async runtime:

```powershell
./target/release/scenario-runner run ../experiments/heterogeneous-capacity.toml --runtime async
```

Run just the overloaded saturation case, or both retry cases:

```powershell
./target/release/scenario-runner run ../experiments/saturation-sweep.toml --scenario saturation-overload --runtime async --repetitions 1
./target/release/scenario-runner run ../experiments/retry-behavior.toml --runtime async
./target/release/scenario-runner run ../experiments/adaptive-cardinality.toml --scenario cardinality-3 --runtime async --repetitions 1
```

Omit filters to run all declared algorithms, runtimes, and repetitions in that manifest. `--scenario`, `--algorithm`, and `--runtime` can be repeated to select several values. `--output DIRECTORY` selects a different results root.

To run the original suite sequentially in PowerShell:

```powershell
$studies = @(
    'equal-capacity', 'heterogeneous-capacity', 'variable-latency',
    'adaptive-balancing', 'saturation-sweep', 'burst-load',
    'retry-behavior', 'failure-recovery'
)
foreach ($study in $studies) {
    ./target/release/scenario-runner run "../experiments/$study.toml"
    if ($LASTEXITCODE -ne 0) { throw "Experiment failed: $study" }
}
```

To run the Phase 5 scaled suite sequentially, use a separate results root so
the baseline history remains untouched:

```powershell
$scaledStudies = @(
    'backend-cardinality-calibration', 'adaptive-v2-tuning',
    'adaptive-cardinality', 'equal-capacity-12',
    'heterogeneous-capacity-13', 'variable-latency-12',
    'saturation-sweep-12', 'burst-load-12', 'retry-behavior-12',
    'adaptive-phase-change-12'
)
foreach ($study in $scaledStudies) {
    ./target/release/scenario-runner run "../experiments/$study.toml" --output ../results-phase5
    if ($LASTEXITCODE -ne 0) { throw "Experiment failed: $study" }
}
```

## Analyze and view

The runner prints the new experiment directory. Use that exact directory (replace `EXPERIMENT_ID` below), not the top-level results root, for the analyzer:

```powershell
./target/release/analyze-results ../results/heterogeneous-capacity-EXPERIMENT_ID
./target/release/dashboard --results ../results
```

Open the dashboard at `http://127.0.0.1:7890`. It can follow an active experiment or replay recorded requests and events. With `--output` on the runner, point the dashboard at that same results root. See [live-dashboard.md](live-dashboard.md) for controls.

The existing analyzer produces CSV/JSON summaries and vector SVG plots under the experiment's `analysis/` directory. Use throughput/error and percentile plots for all studies, backend-fairness outputs for allocation, and resource-usage outputs for proxy cost. Three separate saturation scenario IDs keep those rates in distinct groups. GET and POST are also grouped separately. The burst's two workload IDs remain distinguishable in raw requests and dashboard replay, but its standard summary and percentiles aggregate both workloads for the whole run.

Recovery metrics require a successful restoration action followed by eligible responses. The new scenarios deliberately contain no outage/restoration actions: an empty recovery table or recovery plot with no eligible observations is expected for them. It does not invalidate their throughput, latency, fairness, or resource results. The burst study does **not** automatically calculate a queue-settling-time metric; use its request timeline to study the post-burst interval.

By default the analyzer excludes failed/partial runs from grouped results. If every throughput/latency plot says there are no eligible runs, check `runs.json`, per-run `metadata.json`, and setup/collection/teardown errors in `events.jsonl`. Fix the cause and rerun; `--include-failed` is an explicit analysis choice, not a repair for a broken experiment. The failure-recovery manifest now uses an immediate stop (`--timeout 0`) so Docker's graceful-stop timeout does not overlap its independently scheduled restore action.

## Interpretation and experimental controls

### Equal-capacity fairness

This is the low-load control: identical configured timing and capacity remove deliberate backend differences. Compare plain Jain fairness, latency, and achieved throughput. Weighted round robin has equal weights here, so it is a useful control rather than a capacity-aware advantage. Adaptive algorithms need not produce exactly equal counts at low concurrency; report measurements instead of assuming a winner.

### Heterogeneous capacity

All three backends take nominally 50 ms per executing request, with 1, 4, and 8 execution slots. Their idealized bounds are approximately 20, 80, and 160 requests/s, before overhead. Equal distribution at a target aggregate rate of 160 requests/s would overload the smallest backend while leaving capacity elsewhere. Weights 1:4:8 express the intended capacity proportions.

Inspect both plain fairness and capacity-normalized fairness (attempts divided by configured weight). Equal counts are not the goal here. These weights are configuration, not automatic capacity discovery, and proxy/runtime constraints can still prevent reaching the fixture bounds.

### Variable latency

Fast service time is 25 ms, variable service time is 30-170 ms, and slow service time is 140 ms, each with four slots. At 100 target requests/s, equal allocation can queue on the slow backend. Compare whether latency-aware or least-connections scheduling changes allocation and tail latency. All configured weights are 1; lower equal-count fairness can be a sensible tradeoff for faster responses. Inspect startup behavior too: adaptive state is fresh for every run, and these manifests do not discard a warm-up interval.

### Scaled cardinality and calibration

The calibration manifest uses identical sleeping fixtures at three, six, and
twelve backends while keeping total fixture capacity at twelve. Its low arrival
rate is a host and Docker control; it is not a policy benchmark. The adaptive
cardinality manifest then uses the same total capacity with one, two, and four
replicas per latency class. Paired workload seeds hold the request schedule
constant across cardinalities, and ten repetitions provide the final
comparison. Interpret backend-count effects only within this constant-capacity
matrix.

The equal-capacity-12, heterogeneous-capacity-13, and variable-latency-12
manifests are separate secondary studies. The 13-backend study represents the
1/4/8 capacity tiers with unit replicas, so configured proxy weights remain
one and cohort size carries the capacity difference.

### Adaptive tuning and phase changes

`adaptive-v2-tuning.toml` is a pilot grid with three server configurations. Use
its results to select and freeze v2 settings; do not reuse those repetitions as
final evidence. `adaptive-phase-change-12.toml` changes the four fast and four
slow fixtures at 5 s, then restores both cohorts at 11 s. Inspect the fixture
change events, allocation windows, adaptive diagnostics, and scheduling lag to
separate learning time from host saturation.

### Saturation and bursts

The equal fixture's idealized total bound is approximately 240 requests/s (12 slots / 0.05 s); the 60/180/360 sweep deliberately spans that estimate. It is not a guaranteed observed capacity. Use achieved throughput and measured latency rather than treating the configured arrival rate as achieved load.

The burst schedule is:

| Scheduled interval | Background target | Additional burst target | Total target |
| --- | --- | --- | --- |
| 0-6 s | 60 requests/s | 0 | 60 requests/s |
| 6-10 s | 60 requests/s | 300 requests/s | 360 requests/s |
| 10-20 s | 60 requests/s | 0 new scheduled burst arrivals | 60 requests/s, plus any remaining backlog |

These are planned arrival windows. The runner has bounded worker concurrency; when requests occupy every worker, later actual starts slip and the workload can finish after its nominal window. Compare `scheduled_offset_us` with `started_offset_us` in `requests.jsonl` to quantify scheduling lag. Reported request latency is measured from actual start, so it does not by itself include waiting for a load-generator worker. This is paced bounded-concurrency testing, not a guaranteed open-loop generator. Burst backlog can extend into the nominal settling interval.

The thread-pool configuration has 16 proxy workers, while async can admit more in-flight work. That is part of this system comparison and can dominate high-load results; do not attribute every difference solely to backend selection. Keep runtime, concurrency, and other settings constant for algorithm-only comparisons.

### Retry behavior without a container outage

The failure fixture deliberately disconnects workload requests after receiving them. GET can be retried after such a failure; POST is not replayed once writing has started. Connection failures before sending any request bytes are a different case and can safely be retried for either method. TCP health checks do not necessarily mark a backend unhealthy merely because it disconnects workload requests, so this tests request-level failures separately from container disappearance.

Compare client success/error counts and latency against backend attempt counts from metrics-before/after and fixture counters. A successful client request can still have consumed multiple upstream attempts. The fixture treats POST as a simulated workload, not a real write: this study demonstrates retry policy, not transactional correctness or application-level deduplication.

## Reproducibility and extending a study

- Each matrix cell starts fresh fixture containers and a fresh proxy. This resets counters, fixture sequences, and adaptive algorithm state.
- The manifest/run seed, effective fixture `/config`, before/after fixture `/metrics`, proxy metrics, raw request/event/resource streams, and server config are retained. The analyzer adds a SHA-256 inventory and never rewrites raw observations. See [scenario-runner.md](scenario-runner.md) for the complete artifact contract.
- Seeds make sampled timing/failure sequences repeatable, not wall-clock timing or concurrent request ordering. Container startup, host load, connection scheduling, and retry-dependent sampling still vary.
- Resource samples cover the proxy process only, not Docker, fixtures, the runner, or the entire host. Sleeping fixtures model service delay; they are not CPU-bound production workloads.
- Preserve the original experiments and results. For a new protocol, copy a manifest, give it a new name/scenario ID, change one factor at a time, and validate it. Relative paths resolve from the manifest's directory; moving it elsewhere requires adjusting paths.
- Keep rate and concurrency consistent when comparing algorithms. For longer observations at the same rate, increase request counts proportionally. For a longer burst study, adjust background count, burst count, and `start_after_ms` deliberately.
- Before thesis runs, choose repetitions and duration appropriate to the variability observed in smoke runs, control host load, record the protocol, and report uncertainty. One smoke run verifies integration; it cannot establish statistical superiority.
