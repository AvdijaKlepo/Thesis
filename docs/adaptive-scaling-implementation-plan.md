# Adaptive balancing and backend-scaling implementation plan

## Outcome

Build a second policy named `adaptive_balancing_v2` and an additive
experiment suite that measures how every load-balancing policy behaves as the
backend set grows. The work must preserve the existing manifests, fixture
profiles, server configurations, result directories, and the behavior of
`adaptive_balancing` v1 so the current three-backend results remain a valid
baseline and v1/v2 can run in the same matrix.

The first scaling target is 3, 6, and 12 backends. Twelve is large enough to
expose exploration, state-lock, health-check, and selection-scan costs without
making the experiment primarily a Docker Desktop stress test.

## Questions this plan must answer

1. Does backend cardinality change throughput, tail latency, SLO attainment,
   allocation quality, retry behavior, or proxy cost when aggregate backend
   capacity is held constant?
2. Does the adaptive policy react faster than the existing policies when a
   backend cohort changes performance during a run?
3. Which adaptive reward, recovery-probe, and capacity settings improve the current
   p50/tail-latency tradeoff without hiding exploration cost?
4. At what backend count does the test host or load generator begin to bias the
   result?

Winning a benchmark is not an implementation acceptance criterion. Producing a
controlled and explainable comparison is.

## Starting evidence

The current three-backend result set does not show a clear v1 advantage. Its
mean throughput is about 119.9 requests/s, p50 is 227 ms, and p95/p99 are about
1.30/1.36 s. Least response time reaches about 119.8 requests/s with a slower
325 ms p50 but materially lower 0.84/0.98 s p95/p99. V1 sends roughly 73% of
requests to the fast fixture, compared with about 78% for least response time.

The v1 binary deadline reward treats every successful sub-200 ms observation
equally. Adding backends therefore increases cold-start and exploration cost
without improving the signal. The larger matrix is a diagnostic for v1 and a
controlled comparison for v2, rather than a reason to expect v1 to improve by
itself.

## Fixed experimental decisions

- Add new files; do not rewrite the checked-in baseline manifests or configs.
- Run the initial cardinality matrix with the async runtime. Add the thread-pool
  runtime only after the policy comparison is stable, because its 16-worker
  admission limit is a separate factor.
- Use one repetition for smoke runs, at least five for secondary studies, and
  ten paired repetitions for the final cardinality comparison.
- Keep aggregate execution capacity at 12 fixture workers in the 3/6/12
  cardinality study:

  | Backend count | Replicas per latency class | Capacity per backend | Total capacity |
  | ---: | ---: | ---: | ---: |
  | 3 | 1 fast, 1 variable, 1 slow | 4 | 12 |
  | 6 | 2 fast, 2 variable, 2 slow | 2 | 12 |
  | 12 | 4 fast, 4 variable, 4 slow | 1 | 12 |

- Keep request schedules, latency-class proportions, seeds, health settings,
  and total capacity identical across those three scenarios.
- Treat cardinality scaling and capacity scaling as different experiments. A
  later large-capacity suite may raise arrival rate with replica count, but its
  results must not be mixed with the constant-capacity study.

## Phase 1: implement `adaptive_balancing_v2`

### Configuration model

Add an optional v2-policy block to proxy services in `server/src/config.rs`.
Existing services omit it and v1 continues to use its compiled constants.
Define and validate at least:

- `deadline_ms`, default `200`;
- `ewma_alpha`, default `0.2`, constrained to `(0, 1]`;
- `slo_weight`, default `0.7`, constrained to `[0, 1]`;
- `probe_interval_per_backend`, default `32`, greater than zero;
- `in_flight_penalty`, default `0.05`, non-negative;

Represent these settings with one shared `AdaptiveV2Settings` type rather than
copying constants between configuration and balancer code. Add a distinct
`AlgorithmKind::AdaptiveBalancingV2`; do not route new behavior through the v1
algorithm name.

Likely files:

- `server/src/config.rs` and `server/src/config_tests.rs`
- `server/src/algorithms/mod.rs` and `server/src/algorithms/tests.rs`
- `server/src/algorithms/balancers.rs` and
  `server/src/algorithms/balancers_tests.rs`
- `server/src/backend/pool.rs` and `server/src/backend/pool_tests.rs`
- `server/src/management.rs`, `server/src/management/api.rs`, and their tests

`BackendPool` must retain the settings so an algorithm change or backend
membership change rebuilds the balancer with the same configuration. The
management API must expose effective settings before it is allowed to update
them at runtime.

### V2 state and score

Maintain two EWMAs per backend: deadline-hit probability and continuous latency
utility. Failed attempts contribute zero to both. For successful attempts:

```text
latency_utility = 1 / (1 + latency / deadline)

score =
    slo_weight * deadline_hit_ewma
  + (1 - slo_weight) * latency_utility_ewma
  + bounded_staleness_bonus
  - in_flight_penalty * in_flight / configured_weight
```

Freeze the formula and floating-point boundary behavior in unit tests. It must
remain finite and bounded for zero and very large latency values.

Normalize the in-flight term by configured backend weight so a backend that
represents more capacity is not penalized as if it had weight one. Replace v1's
cumulative UCB exploration in v2 with a bounded staleness probe: each healthy
backend becomes eligible for reconsideration after approximately
`probe_interval_per_backend * eligible_backend_count` selections. This bounds
exploration as cardinality grows while still detecting recovery. Cold start
reserves every backend once as v1 does.

### Concurrency decision

Keep the current short adaptive-state mutex for the first 12-backend study.
Instrument it before replacing it. Refactor to per-backend atomics or a
snapshot-based state only if selection-time or CPU measurements cross the
acceptance gates below. This avoids optimizing an unmeasured bottleneck.

### Phase 1 acceptance

- Existing configurations parse unchanged and v1 selection traces remain
  unchanged.
- Deterministic tests cover validation, both EWMA values, reward bounds,
  capacity normalization, cold-start distribution, retry exclusion, bounded
  probing, and backend recovery.
- V2 ranks successful under-deadline backends by latency, changes preference
  after their performance reverses, and revisits a recovered backend within
  the configured bound.
- Algorithm changes and backend add/update/remove operations preserve settings.

## Phase 2: add the measurements the policy actually optimizes

### Analyzer additions

Add an optional `slo_target_ms` to each scenario manifest. Record it in resolved
metadata and calculate:

- successful completions within the SLO;
- SLO attainment rate;
- SLO miss count and miss ratio;
- request scheduling lag, `started_offset_us - scheduled_offset_us`, including
  p50, p95, p99, and maximum;
- summaries by `workload_id` so background and burst traffic are not collapsed;
- fixed-width time-window allocation and latency summaries for convergence and
  post-change recovery.

Also add optional named `analysis_windows`, an opt-in
`paired_workload_seeds = true`, a scenario `workload_seed_group`, and
`execution_order = "blocked_randomized"`. For paired runs, derive a workload
seed from experiment seed, workload-seed group, runtime, repetition, and
workload ID while excluding algorithm and concrete scenario ID; retain a
separate unique run seed. Giving the 3/6/12 scenarios the same group then
produces the same arrival schedule. Randomize algorithm order deterministically
inside each scenario/runtime/repetition block. Existing manifests default to
their current declared ordering and seed derivation.

Move the derived analysis report to schema version 2, retain support for old raw
artifacts, and add long-form CSV outputs such as `workload-summary.csv`,
`window-summary.csv`, and `group-window-summary.csv`. Keep all existing columns
and files backward compatible; old runs without `slo_target_ms` should produce
null SLO fields rather than inferred values.

Likely files:

- `server/src/scenario/manifest.rs` and `manifest_tests.rs`
- `server/src/scenario/runner.rs`
- `server/src/analysis.rs` and `analysis_tests.rs`
- `docs/scenario-runner.md`

### Adaptive diagnostics

Add a read-only balancer diagnostic snapshot containing, per backend:

- observations, selections, and last-selection generation;
- deadline-hit EWMA, latency-utility EWMA, and staleness bonus;
- in-flight count and final selection score;
- configured adaptive settings and snapshot timestamp.

Expose the snapshot through structured management metrics. The experiment
runner should collect it before and after a run; time-window request allocation
remains the source for behavior during the workload.

### Host-bias measurements

Extend resource samples with optional whole-host CPU and memory data while
retaining the existing proxy-process fields. Implement host sampling directly
for Windows and Linux; unsupported platforms emit null values and a warning.
Do not make Docker CLI output part of the core artifact schema.

Use scheduling lag as the primary load-generator saturation signal. Mark a run
environment-limited when either condition holds:

- sustained host CPU is at least 80%; or
- p95 scheduling lag exceeds both 5 ms and 25% above the matching three-backend
  control.

Environment-limited runs remain in raw results but are excluded from policy
claims unless explicitly analyzed as host-saturation experiments.

### Phase 2 acceptance

- Synthetic analyzer tests verify SLO, scheduling-lag, workload, and time-window
  calculations at boundary values.
- Old result directories remain analyzable.
- JSON, CSV, and methodology text agree on units and eligibility rules.
- Host metrics and proxy metrics are visibly distinct.

## Phase 3: support a real performance shift

The current adaptive experiment changes offered load but keeps each backend's
latency distribution fixed. Add a typed fixture-control operation so an
experiment can change latency, jitter, or error rate at a scheduled time
without rebuilding a container.

Implement a fixture control endpoint backed by synchronized mutable workload
settings. Listener address, fixture ID, and worker-pool capacity remain
immutable. Add a typed `[[scenarios.fixture_changes]]` manifest entry with an
address, time, and validated patch. The runner records started/completed events
and the before/after effective fixture configuration.

Likely files:

- `server/src/fixture.rs` and `fixture_tests.rs`
- `server/src/scenario/manifest.rs`, `runner.rs`, and their tests
- `docs/backend-fixtures.md` and `docs/scenario-runner.md`

The principal phase-change scenario swaps the fast and slow cohorts after a
warm-up interval, then restores them. This tests learning and recovery rather
than health-check removal.

### Phase 3 acceptance

- Control requests cannot change identity, listener, or capacity.
- A seeded fixture produces repeatable samples before and after a mutation.
- Mutation events and effective values are present in run artifacts.
- A failed mutation fails the run unless explicitly marked allowable.

## Phase 4: add scaling fixtures and server configurations

Use explicit Compose services and ports so every resolved run remains easy to
audit. Reuse YAML anchors, but do not generate Compose or server TOML at run
time.

Create a separate `compose.scaled-fixtures.yml` so expansion cannot alter the
environment used by the original experiment suite.

Add profiles and matching server configurations for:

- variable latency at 3, 6, and 12 backends with total capacity 12;
- 12 identical unit-capacity backends for equal, saturation, and burst studies;
- 13 unit-capacity backends split into 1/4/8 replica cohorts for the existing
  heterogeneous 1:4:8 capacity comparison;
- 12 unit-capacity failure fixtures split into four healthy, four flaky, and
  four always-failing backends for retry studies.

Likely files:

- `compose.scaled-fixtures.yml`
- `server/config/fixtures/cardinality-3.toml`
- `server/config/fixtures/cardinality-6.toml`
- `server/config/fixtures/cardinality-12.toml`
- `server/config/fixtures/equal-capacity-12.toml`
- `server/config/fixtures/heterogeneous-replicas-13.toml`
- `server/config/fixtures/failure-12.toml`
- `server/config/fixtures/variable-latency-12.toml`
- `server/src/config_tests.rs`

Assign non-overlapping loopback port ranges per profile. Document that profiles
must run sequentially because Compose project names do not isolate published
ports.

### Phase 4 acceptance

- Every configuration loads through `AppConfig` tests.
- Compose configuration validation passes for every new profile.
- `/config`, `/health`, and `/metrics` collections succeed for every backend.
- Low-load round-robin calibration shows no material latency or scheduling-lag
  increase caused solely by moving from 3 to 12 sleeping fixtures.

## Phase 5: add manifests without replacing baselines

Add these manifests:

1. `experiments/backend-cardinality-calibration.toml` — low-load 3/6/12
   equal-backend checks used only to detect host and Docker bias.
2. `experiments/adaptive-v2-tuning.toml` — a small, predeclared parameter grid
   on six variable-latency backends using pilot seeds that are excluded from
   final evaluation.
3. `experiments/adaptive-cardinality.toml` — the controlled 3/6/12 v1/v2 and
   classical-policy matrix that answers the backend-count question.
4. `experiments/equal-capacity-12.toml` — large equal-backend fairness control.
5. `experiments/heterogeneous-capacity-13.toml` — the 1/4/8 capacity tiers
   represented as unit replicas.
6. `experiments/variable-latency-12.toml` — four replicas of each latency class.
7. `experiments/saturation-sweep-12.toml` — light, medium, and overload rates
   against the large equal pool.
8. `experiments/burst-load-12.toml` — transient overload and queue draining.
9. `experiments/retry-behavior-12.toml` — distinct-backend retry behavior with
   larger healthy/flaky/failing cohorts.
10. `experiments/adaptive-phase-change-12.toml` — warm-up, latency-cohort swap,
   recovery, and settling windows.

The cardinality and phase-change manifests compare the relevant existing
algorithms with both `adaptive_balancing` and `adaptive_balancing_v2`.

Enable paired workload seeds and seeded block randomization in the new primary
manifests. Execution position must not change workload samples.

Update `docs/experiment-scenarios.md` with the new protocols and exact matrix
sizes.

### Execution sequence

1. Validate and dry-run every manifest.
2. Run one async round-robin smoke cell for every new fixture profile.
3. Run the 3/6/12 low-load host calibration.
4. Run one repetition of the full cardinality and phase-change matrices.
5. Inspect host limits, scheduling lag, collection completeness, and adaptive
   diagnostics.
6. Freeze settings, then run ten final repetitions for the cardinality study
   and at least five for each secondary large-backend study.
7. Analyze into a new results directory; never overwrite the original eight
   experiment result sets.

## Required comparisons

Report, with 95% confidence intervals where repetitions permit:

- throughput and successful throughput;
- transport and HTTP error rates;
- p50, p95, and p99 end-to-end latency;
- SLO attainment and miss ratio;
- plain and capacity-normalized Jain fairness;
- allocation per latency/capacity cohort;
- time to adapt after a cohort change;
- p95 scheduling lag;
- proxy CPU/RSS and whole-host CPU/memory;
- adaptive diagnostic state at collection points.

Compare backend-count effects only within constant-capacity scenarios. Compare
scaled-capacity scenarios separately.

## Final acceptance criteria

- Existing manifests, configs, and historical results remain intact and valid.
- All new manifests validate and all smoke cells complete without collection
  gaps.
- Unit and integration tests cover settings, mutations, analysis, selection,
  retries, and backward compatibility.
- `cargo fmt --all -- --check`, `cargo test`, and release builds pass, apart
  from any explicitly documented pre-existing environment-dependent test.
- No final policy conclusion uses an environment-limited run.
- The report can explain an adaptive win or loss through reward state,
  exploration, allocation, SLO results, and host/load-generator measurements.

## Stop conditions

Pause scale-up above 12 backends if host CPU, memory pressure, scheduling lag,
or control-collection overhead crosses the environment gates. Do not refactor
the balancer lock unless measurement identifies it as material. Do not tune
adaptive constants on the same repetitions later used as final evidence; use a
pilot set, freeze the configuration, and create a fresh final result set.
