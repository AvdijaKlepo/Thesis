# Controlled backend fixtures

Point 7 provides deterministic backend processes for load-balancing experiments. They are deployment fixtures, not children of the proxy server: Docker Compose or the operator owns their entire lifecycle.

## Workload model

Each container runs the `backend-fixture` binary with these controls:

| Variable | Meaning |
| --- | --- |
| `FIXTURE_CAPACITY` | Number of requests that can execute concurrently. Additional accepted connections wait in the fixture's work queue. |
| `FIXTURE_LATENCY_MS` | Minimum delay applied to every workload request. |
| `FIXTURE_LATENCY_JITTER_MS` | Seeded additional delay in the inclusive range `0..N`. |
| `FIXTURE_PROCESSING_MS` | Fixed service-processing duration after the latency delay. |
| `FIXTURE_ERROR_RATE` | Seeded failure probability from `0.0` through `1.0`. |
| `FIXTURE_ERROR_MODE` | `status` returns HTTP 500; `disconnect` closes without a response so proxy retry behavior is exercised. |
| `FIXTURE_SEED` | Makes the latency and failure sequence repeatable. |

The observed response time is queueing time plus sampled latency plus processing duration. Sleeping is used instead of CPU spinning so a fixture changes timing without deliberately exhausting the host.

The binary accepts equivalent flags such as `--capacity`, `--latency-ms`, and `--error-rate`. Run `cargo run --bin backend-fixture -- --help` for the complete list. Command-line values override environment values.

## Introspection endpoints

- `GET /health` reports liveness and bypasses configured workload failures.
- `GET /config` returns the effective configuration as JSON.
- `GET /metrics` returns workload totals plus current and maximum active requests.
- `POST /control` applies a typed JSON patch to latency, jitter, processing
  delay, or error behavior and returns the new effective configuration.
- Every other path executes the configured workload.

Successful and HTTP-error workload responses include `X-Fixture-Backend` and `X-Fixture-Request-Id` headers and a JSON body describing the sampled delay. Health, configuration, and metrics requests are excluded from workload counters.

## Server configurations for fixture profiles

[compose.fixtures.yml](../compose.fixtures.yml) defines four independent Compose profiles. Matching server configurations live under [`server/config/fixtures`](../server/config/fixtures). These files describe the server and its backend addresses; they do not describe experimental workloads or fault schedules.

| Profile | Published ports | Controlled difference |
| --- | --- | --- |
| `equal-capacity` | 5101-5103 | Three identical 4-worker, 50 ms total-service fixtures. |
| `heterogeneous` | 5201-5203 | Capacities 1, 4, and 8 with identical request timing. Proxy weights mirror those capacities. |
| `variable-latency` | 5301-5303 | Fast fixed, seeded variable, and slow fixed latency with equal capacity. |
| `failure` | 5401-5403 | Healthy, 35% disconnecting, and always-disconnecting fixtures. |

Seeds are fixed in the Compose file. Changing only the proxy runtime or load-balancing algorithm therefore leaves the fixture definition constant.

## Scaled fixture profiles

`compose.scaled-fixtures.yml` is an additive profile set for backend-cardinality
and host-bias studies. Its ports are separate from the original profiles:

| Profile | Ports | Composition | Server configuration |
| --- | --- | --- | --- |
| `cardinality-3` | 6101-6103 | One fast, variable, and slow backend; four workers each. | `cardinality-3.toml` |
| `cardinality-6` | 6201-6206 | Two replicas per latency class; two workers each. | `cardinality-6.toml` |
| `cardinality-12` | 6301-6312 | Four replicas per latency class; one worker each. | `cardinality-12.toml` |
| `equal-capacity-3` | 6801-6803 | Three identical 50 ms backends; four workers each. | `equal-capacity-3.toml` |
| `equal-capacity-6` | 6901-6906 | Six identical 50 ms backends; two workers each. | `equal-capacity-6.toml` |
| `equal-capacity-12` | 6401-6412 | Twelve identical one-worker backends. | `equal-capacity-12.toml` |
| `heterogeneous-replicas-13` | 6501-6513 | One, four, and eight one-worker cohorts. | `heterogeneous-replicas-13.toml` |
| `failure-12` | 6601-6612 | Four healthy, four flaky, and four always-failing backends. | `failure-12.toml` |
| `variable-latency-12` | 6701-6712 | Four replicas per latency class; one worker each. | `variable-latency-12.toml` |

Start and stop one scaled profile at a time because published ports are shared
by the profiles and are intentionally easy to audit:

```text
docker compose -f compose.scaled-fixtures.yml --profile cardinality-12 up --build -d
docker compose -f compose.scaled-fixtures.yml --profile cardinality-12 down
```

The matching server TOML files live under
[`server/config/fixtures`](../server/config/fixtures). They use the same proxy,
control, and admin ports as the original suite; run the profiles sequentially
when those listeners are also in use.

## Scheduled runtime changes

Phase-three scenario manifests can schedule a fixture change without restarting
the process:

```toml
[[scenarios.fixture_changes]]
id = "swap-fast-to-slow"
at_ms = 2000
address = "127.0.0.1:5101"
patch = { latency_ms = 250, latency_jitter_ms = 20 }
```

The patch schema deliberately excludes `id`, `listen_address`, `capacity`, and
`seed`. The runner records the effective `/config` response before the patch and
the effective configuration returned by `/control` after it in
`fixture_change_started` and `fixture_change_completed` events. A failed change
fails the run unless `allow_failure = true` is set on that change.

## Running a fixture profile manually

From the repository root, start only the selected external fixture profile:

```text
docker compose -f compose.fixtures.yml --profile equal-capacity up --build -d
```

Then start the proxy from `server/` with the matching host-side configuration:

```text
cargo run -- --config config/fixtures/equal-capacity.toml
```

Inspect a fixture before a run:

```text
curl http://127.0.0.1:5101/config
curl http://127.0.0.1:5101/metrics
```

Stop containers explicitly when the experiment ends:

```text
docker compose -f compose.fixtures.yml --profile equal-capacity down
```

The proxy never invokes these commands, talks to the Docker daemon, or attempts to restart a fixture. To test whole-container disappearance, stop a fixture from Compose while the workload is running; that external action is intentionally outside the web server.

For repeatable matrix runs, timed fault injection, and raw-result capture, use the external [scenario runner](scenario-runner.md).

The [fixture experiment suite](experiment-scenarios.md) supplies runnable manifests
for all four profiles, including fairness, capacity and latency differences,
saturation, bursts, method-dependent retries, and failure recovery.
