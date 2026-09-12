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
- Every other path executes the configured workload.

Successful and HTTP-error workload responses include `X-Fixture-Backend` and `X-Fixture-Request-Id` headers and a JSON body describing the sampled delay. Health, configuration, and metrics requests are excluded from workload counters.

## Scenarios

[compose.fixtures.yml](../compose.fixtures.yml) defines four independent Compose profiles. Matching proxy configurations live under [`server/config/fixtures`](../server/config/fixtures).

| Profile | Published ports | Controlled difference |
| --- | --- | --- |
| `equal-capacity` | 5101-5103 | Three identical 4-worker, 50 ms total-service fixtures. |
| `heterogeneous` | 5201-5203 | Capacities 1, 4, and 8 with identical request timing. Proxy weights mirror those capacities. |
| `variable-latency` | 5301-5303 | Fast fixed, seeded variable, and slow fixed latency with equal capacity. |
| `failure` | 5401-5403 | Healthy, 35% disconnecting, and always-disconnecting fixtures. |

Seeds are fixed in the Compose file. Changing only the proxy runtime or load-balancing algorithm therefore leaves the fixture definition constant.

## Running a scenario

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
