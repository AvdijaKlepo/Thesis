# Quantum optimization implementation plan

## Outcome

Add a solver-neutral QUBO routing optimizer that periodically calculates a
backend weight plan. The server applies that immutable plan with a cheap local
selection operation. The first production-capable solver is deterministic
classical simulated annealing; real quantum or hybrid solvers consume the same
serialized problem later through an optional out-of-process adapter.

This is a control-plane optimizer. No request may construct a QUBO, wait for a
solver, access a network service, or require quantum credentials.

## Research claim and boundaries

The experiment tests whether periodically solving a coupled allocation problem
improves SLO attainment, tail latency, capacity use, or plan stability relative
to local greedy policies. It does not assume or claim quantum advantage.

The useful quantum analogy is Wi-Fi access-point selection: formulate a
periodic selection/allocation problem, solve it outside the packet path, and
apply the result locally. Current gate-model QAOA and hosted hybrid solvers have
execution and queue times that are unsuitable for individual web requests.

Non-goals for the first implementation:

- routing each request through a remote solver;
- adding vendor SDKs or credentials to the server binary;
- treating physical-qubit count as the number of supported backends;
- replacing health checks, retries, or fail-closed behavior;
- claiming that a quantum label makes a simple minimum scan more efficient.

## Optimization model

### Decision variables

Give every eligible backend three binary variables:

```text
y_i = x_i0 + 2*x_i1 + 4*x_i2
w_i = 1 + y_i
```

`w_i` is a dynamic routing weight from 1 through 8. Configured backend weight
remains the capacity/provenance value and is never overwritten by an optimizer.
Unhealthy backends are omitted from a new problem and are always rejected by
request-path health filtering even if they appear in an older plan.

With three bits per backend, the principal scales are:

| Backends | Logical variables | Use |
| ---: | ---: | --- |
| 3 | 9 | exhaustive correctness checks |
| 6 | 18 | largest routine exact-solver oracle |
| 8 | 24 | first nontrivial optimizer experiment |
| 12 | 36 | primary project target |
| 16 | 48 | optional host/QPU scaling study |

The number of logical variables, quadratic couplings, coefficient range, and
hardware embedding determine QPU fit. There is no required physical-backend
count for quantum optimization.

### Objective

For eligible backend `i`, derive a bounded cost `c_i` from:

- capped latency divided by the configured SLO;
- transport-failure deltas during the latest observation window;
- active connections divided by configured capacity weight.

For target total dynamic weight `W = units_per_backend * N`, minimize:

```text
P * (sum_i(w_i) - W)^2
+ sum_i(c_i * w_i)
+ C * sum_i(w_i^2 / capacity_i)
+ S * sum_i((w_i - previous_w_i)^2)
```

All terms expand into a QUBO. `P` is calculated from an upper bound on every
soft term so violating the total-weight constraint cannot appear beneficial.
Normalize coefficients for solver adapters while retaining the original scale
and constant offset in the artifact.

The capacity term discourages concentration beyond configured capacity. The
stability term controls plan churn. The one-unit floor provides continued
sampling of every healthy backend; health and retry exclusion still take
precedence. Record raw and normalized objectives separately.

### Validation and repair

Never trust a solver response directly. For every result:

1. verify problem ID, generation, variable count, and backend-membership hash;
2. reject non-binary or stale values;
3. recompute energy locally from the submitted QUBO;
4. check hard constraints;
5. greedily repair total-weight infeasibility within the solve budget;
6. record raw and repaired energy, feasibility, and repair count;
7. publish only a validated plan.

## Phase 1: implement an offline solver-neutral core

Create:

- `server/src/optimization/mod.rs`
- `server/src/optimization/qubo.rs`
- `server/src/optimization/features.rs`
- `server/src/optimization/solver.rs`
- `server/src/optimization/exact.rs`
- `server/src/optimization/annealing.rs`
- `server/src/optimization/tests.rs`
- `server/src/bin/qubo_optimize.rs`

Core models should resemble:

```rust
pub struct QuboProblem {
    pub schema_version: u32,
    pub problem_id: String,
    pub generation: u64,
    pub membership_hash: String,
    pub variables: Vec<VariableLabel>,
    pub constant: f64,
    pub linear: Vec<f64>,
    pub quadratic: Vec<QuboTerm>,
}

pub struct SolveLimits {
    pub deadline: Instant,
    pub seed: u64,
}

pub trait QuboSolver: Send + Sync {
    fn name(&self) -> &'static str;
    fn solve(
        &self,
        problem: &QuboProblem,
        limits: SolveLimits,
    ) -> Result<QuboSolution, SolverError>;
}
```

Implement two local solvers:

- exact enumeration for at most 18 variables, used as a correctness oracle;
- deterministic seeded simulated annealing followed by local bit-flip search
  for 24–48 variables.

The CLI reads a versioned problem JSON file and writes a versioned solution
JSON file. It supports generate/evaluate/solve workflows without running the
proxy. Do not add a vendor dependency in this phase.

### Phase 1 tests and gates

- Symbolic QUBO expansion and direct objective evaluation agree for randomized
  small inputs.
- Exact solutions match hand-calculated 3-backend cases.
- The same seed and input produce the same annealing result.
- Timeout checks terminate both solvers within a bounded overrun.
- Validation and repair produce a feasible allocation for every generated test
  problem.
- On a fixed 6-backend corpus, the annealer's median objective gap from exact is
  no greater than 2%.
- Benchmark, without making CI timing-flaky, that the 12-backend/36-variable
  p95 local solve is at most 100 ms on the reference machine.

## Phase 2: add a background optimizer and request-path policy

Register `qubo_balancing` as a new `AlgorithmKind`; do not rename any existing
algorithm. Add `server/src/algorithms/qubo_balancing.rs`.

`QuboBalancing` owns a background controller and an atomically replaceable
`RoutingPlan`:

```rust
pub struct RoutingPlan {
    pub generation: u64,
    pub membership_hash: String,
    pub weights: Vec<u8>,
    pub schedule: Box<[usize]>,
    pub cursor: AtomicUsize,
}
```

Every optimization interval the controller:

1. snapshots health and cumulative backend metrics;
2. converts counter deltas into bounded features;
3. waits for a full observation interval and a minimum sample count after
   construction or an algorithm switch;
4. constructs and solves one QUBO within a strict budget;
5. validates and repairs the result;
6. atomically publishes a complete routing plan.

Capturing metric baselines at construction prevents traffic from a previously
selected algorithm and the initial 1 ms latency estimate from contaminating the
first optimization. Counter decreases start a new baseline.

The request path only reads the current plan, advances an atomic schedule
cursor, and skips unhealthy or retry-excluded backends. Until the first valid
plan exists, it uses configured-weight routing. On timeout or solver error it
retains the last good plan. If that plan is stale or contains no eligible
backend, it falls back to configured-weight routing.

Use `QuboBalancing::with_solver` and `Arc<dyn QuboSolver>` so tests can inject
exact, slow, failing, malformed, and stale solvers. Stop and join the controller
when its balancer snapshot is dropped. Existing `BackendSelection` ownership
ensures in-flight feedback remains attached to the balancer that selected it.

Likely integration files:

- `server/src/algorithms/mod.rs` and tests
- `server/src/algorithms/balancers.rs`
- `server/src/backend/pool.rs` and tests
- `server/src/proxy/behavior.rs` and behavior tests
- `server/src/lib.rs`
- `server/Cargo.toml`

### Phase 2 concurrency and failure tests

- Concurrent selection continues during a deliberate 500 ms solver stall.
- Plan swaps are atomic and every observed plan is internally consistent.
- Health and retry exclusions override dynamic weights.
- Slow, failed, infeasible, malformed, and stale solutions preserve valid
  routing and increment the correct diagnostic counter.
- Backend membership changes invalidate mismatched solutions.
- Weighted selection matches the published plan over a complete schedule.
- Algorithm changes shut down the previous controller without leaking threads.

## Phase 3: configuration, management, and observability

Add optional per-service settings with conservative defaults:

```toml
[services.qubo]
solver = "simulated_annealing"
interval_ms = 2000
solve_budget_ms = 100
bits_per_backend = 3
units_per_backend = 4
slo_ms = 200
seed = 20260917
minimum_samples = 32
max_plan_age_ms = 6000
```

Keep objective coefficients as versioned implementation constants for the first
study. This avoids turning a small experiment into an uncontrolled coefficient
tuning exercise. Expose them in diagnostics and artifacts.

Permit the settings on a proxy service even if its initial algorithm is another
policy, because the scenario runner changes algorithms at runtime. Preserve
existing constructors through a new defaulted balancer-settings wrapper.

Likely files:

- `server/src/config.rs` and `config_tests.rs`
- `server/src/backend/pool.rs`
- `server/src/management.rs`, `management/api.rs`, and tests
- `server/src/bin/serverctl.rs`
- `docs/management-cli.md`

Add a default `diagnostics()` method to `LoadBalancer` and expose at least:

- generation, plan age, dynamic weights, and membership hash;
- solver and encoding versions;
- logical variable and quadratic-coupler counts;
- feature window and sample count;
- solve, validation, repair, and queue durations;
- raw/repaired energy and feasibility;
- churn from the previous plan;
- solve attempts, timeouts, errors, stale results, repairs, and fallbacks.

Emit one low-rate structured `optimization_decision` record per controller
cycle. Preserve it in `server.stdout.log` and extract it into
`optimization-events.jsonl` during experiment collection. Never log credentials
or vendor tokens.

Extend analysis with:

- SLO attainment and miss ratio;
- warm-up, steady-state, burst, and post-change windows;
- plan churn, age, feasibility, and objective;
- solve-time distribution and controller duty cycle;
- solver timeout/error/fallback counts;
- allocation, fairness, retry, throughput, and latency metrics already used by
  the classical policies.

The local solver runs inside the proxy and is included in existing proxy CPU
measurements. A later remote adapter requires its own resource and timing
telemetry.

## Phase 4: local shadow and active experiments

This phase depends on the 3/6/12 fixture and measurement infrastructure from
the adaptive scaling plan.

Add:

1. `experiments/qubo-cardinality.toml`
   - 3, 6, and 12 backends at constant aggregate capacity;
   - async runtime;
   - weighted round robin, least response time, adaptive v1/v2, and QUBO;
   - at least five repetitions with paired workload seeds.
2. `experiments/qubo-dynamics.toml`
   - 12 backends;
   - burst plus explicit backend performance or availability change;
   - 30–60 seconds, providing 15–30 two-second optimization windows;
   - separate warm-up, active-change, and recovery summaries.

Run modes in order:

1. **Offline:** solve a fixed captured corpus and compare exact versus annealing.
2. **Shadow:** build and solve QUBOs, but route with the selected classical
   baseline. Verify feature quality, stability, overhead, and feasibility.
3. **Active local:** publish simulated-annealing plans and compare policies.

### Engineering acceptance

- Every applied plan passes local validation.
- No request waits for the solver or a controller lock.
- Deliberate solver and adapter outages preserve valid routing.
- Local solve duty cycle stays below 5% of one controller interval.
- Stale-plan and fallback behavior is exercised in tests and a fault-injection
  run.
- Request errors and p99 latency do not regress because of controller mechanics.

The scientific result may show that the QUBO policy loses to a simpler policy.
Report paired effects and confidence intervals rather than making superiority a
definition-of-done requirement.

## Phase 5: optional real-QPU and hybrid-solver boundary

Define a versioned, vendor-neutral JSON contract.

Problem fields include:

- problem ID, generation, membership hash, encoding/objective versions;
- ordered variable labels;
- sparse upper-triangular QUBO and constant offset;
- coefficient scale, seed, and solve budget;
- feature snapshot and previous plan needed for audit.

Solution fields include:

- problem ID and generation;
- returned bits, reported energy, and solver feasibility;
- provider, hardware, solver, and embedding identifiers;
- queue, wall-clock, QPU-access, validation, and repair times;
- sample count and provider metadata needed for reproduction.

Start with offline replay of saved problems through a new `qubo-replay` CLI.
Provider SDKs and credentials live in a sidecar or research tool outside the
proxy. Compare exact/local/remote solutions on objective value, feasibility,
total wall time including queueing, and cost.

Gate-model QAOA should begin with recorded 9–18-variable problems. Annealers
and hybrid solvers may attempt the 24–36-variable corpus, subject to the
provider's current topology and embedding. Dense logical QUBOs can consume many
physical qubits through minor embedding, so report the actual embedding rather
than translating a backend count directly into qubits.

Only after offline and shadow validation may an asynchronous remote client be
considered. It must enforce one job in flight, strict size/time limits,
generation and membership checks, local energy recomputation, stale-result
rejection, a circuit breaker, and local/last-plan fallback. Remote availability
can never become a serving dependency.

Relevant external references:

- [D-Wave BQM solver parameters](https://docs.dwavequantum.com/en/latest/industrial_optimization/solver_bqm_parameters.html)
- [D-Wave minor-embedding guidance](https://docs.dwavequantum.com/en/latest/quantum_research/embedding_guidance.html)
- [IBM QAOA workflow](https://quantum.cloud.ibm.com/docs/en/tutorials/quantum-approximate-optimization-algorithm)
- [Quantum-annealing access-point selection](https://arxiv.org/abs/2407.08943)

## Final deliverables

- Versioned QUBO problem and solution schemas.
- Exact and deterministic simulated-annealing solvers.
- `qubo_optimize` and `qubo-replay` offline tools.
- Background QUBO controller and atomic request-path routing plans.
- Configuration, management, diagnostics, and analysis support.
- Shadow and active local experiment result sets at 3/6/12 backends.
- Optional provider replay results kept separate from serving benchmarks.
- Documentation that distinguishes classical QUBO, quantum-inspired solving,
  simulated quantum execution, and real QPU execution.

## Stop conditions

Do not activate remote solutions if queue time routinely exceeds plan age, if
membership changes make results stale, or if repaired solutions dominate raw
solutions. Do not increase problem size until coefficient scaling and
feasibility remain stable. Do not claim an online latency improvement from QPU
execution when the QPU work occurred offline or excluded queue and adapter
time. If the local QUBO policy cannot beat or explain its tradeoff against a
simple classical baseline, keep it as an offline research comparison rather
than adding operational complexity to the proxy.
