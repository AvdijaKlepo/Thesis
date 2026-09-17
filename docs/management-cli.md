# Management CLI

`serverctl` manages the live service registry through the server's versioned management API. Server TOML remains the durable description of listeners, initial services, routes, backends, health checking, and runtime defaults. CLI changes are intentionally in-memory and disappear when the server restarts.

The scenario runner and CLI both use `server::management::ManagementClient`; they do not duplicate HTTP endpoint handling. Neither the API nor the CLI starts containers or interprets experiment manifests.

## Build and connect

```text
cd server
cargo build --release --bin server --bin serverctl
cargo run --release --bin server -- --config config/server.toml
cargo run --release --bin serverctl -- service list
```

The default management address is `127.0.0.1:7880`. Set `WEB_SERVER_ADMIN` or put `--admin IP:PORT` before the resource name to select another address. The management listener has no authentication, so it should only be exposed on a trusted interface.

Use `--json` before the resource name for stable machine-readable output:

```text
cargo run --bin serverctl -- --json service list
cargo run --bin serverctl -- --json metrics
```

## Services

Create and update proxy services:

```text
serverctl service add catalog --kind proxy --route /catalog --algorithm least_connections
serverctl service add regional --kind proxy --host-route eu.example.test /api --fail-closed
serverctl service update catalog --route /v2/catalog --algorithm weighted_round_robin --fail-open
serverctl service probe catalog --probe-timeout-ms 250
serverctl service remove catalog
```

Proxy service responses include the effective `adaptive_v2` settings. A JSON
`PATCH /services/{id}` request can replace them while the service is running,
for example:

```json
{"adaptive_v2":{"deadline_ms":150,"ewma_alpha":0.2,"slo_weight":0.7,"probe_interval_per_backend":32,"in_flight_penalty":0.05}}
```

Create a static service with an existing directory. Relative roots are resolved by `serverctl` before they are sent, so use an absolute server-local path when managing another host.

```text
serverctl service add assets --kind static --route /assets --root C:\srv\assets
serverctl service update assets --root C:\srv\new-assets
```

Every service needs at least one route. `--route PATH` is host-agnostic; `--host-route HOST PATH` is host-specific. Both options may be repeated. Updating routes replaces the service's complete route set. Service and backend IDs are restricted to letters, digits, `.`, `_`, and `-` so they remain safe URL path components.

The configured default service can be updated but cannot be removed while the process is running. A service probe checks whether a static root still exists or whether at least one proxy backend accepts a TCP connection. It does not change health-check state.

## Backends and policies

Backend commands are scoped to a proxy service:

```text
serverctl backend list catalog
serverctl backend add catalog catalog-a catalog-a.internal:8080 --weight 2
serverctl backend update catalog catalog-a --address 127.0.0.1:9101 --weight 3
serverctl backend probe catalog catalog-a --probe-timeout-ms 250
serverctl backend remove catalog catalog-a
serverctl algorithm set catalog least_connections
```

Addresses may use an IP address or hostname with a port. Backend updates preserve that backend's counters and current health flag. Adding, updating, or removing a backend rebuilds only the affected service's balancer. The registry-wide health checker also discovers services and backends added after startup.

Probe output reports both `healthy`, the current state maintained by background health checks, and `reachable`, the result of this one TCP probe. Probe commands exit with status 1 when the target is unavailable and status 2 for invalid input or an API/transport error.

Runtime mode and current metrics are available through the same client:

```text
serverctl runtime get
serverctl runtime set async
serverctl metrics
```

The structured `/metrics` snapshot includes `adaptive_diagnostics` for each
adaptive proxy service. The read-only snapshot reports per-backend observations,
deadline and latency EWMAs, bounded staleness, in-flight reservations, final
score, effective settings, and its capture timestamp; non-adaptive services
return `null`.

## Restricted port discovery

Port discovery is separate from backend registration and is opt-in. It accepts only a loopback IP, at most 256 ports, and a per-port timeout from 1 through 500 ms. It only reports listening TCP ports; it never creates backend records.

```text
serverctl discover ports --start 9000 --end 9050
serverctl --json discover ports --host ::1 --start 9000 --end 9050 --connect-timeout-ms 20
```

Register a discovered address explicitly with `backend add` after verifying what owns the listener.

## HTTP API

The typed client uses these JSON endpoints:

| Method | Path | Purpose |
| --- | --- | --- |
| `GET`, `POST` | `/v1/services` | List or add services |
| `GET`, `PATCH`, `DELETE` | `/v1/services/{service}` | Inspect, update, or remove one service |
| `GET` | `/v1/services/{service}/probe?timeout_ms=N` | Probe a service |
| `GET`, `POST` | `/v1/services/{service}/backends` | List or add backends |
| `PATCH`, `DELETE` | `/v1/services/{service}/backends/{backend}` | Update or remove a backend |
| `GET` | `/v1/services/{service}/backends/{backend}/probe?timeout_ms=N` | Probe a backend |
| `GET` | `/metrics` | Read the shared metrics snapshot |
| `GET`, `POST` | `/runtime` | Read or change the live runtime |

Errors use a consistent JSON envelope with `error.code` and `error.message`. The existing `/algorithm`, `/runtime`, `/metrics`, and read-only `/backends` routes remain available for legacy integrations and dashboard compatibility. The old endpoint that guessed and automatically registered numbered localhost backends has been removed.
