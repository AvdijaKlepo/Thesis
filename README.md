# Web Server & Load Balancing Benchmark Suite

A lightweight HTTP reverse proxy and experimental testbed written in Rust. It was built to evaluate and compare load balancing algorithms and server execution models (`thread_pool` vs. `async` via Tokio) under controlled network conditions.

The repository includes the proxy server, mock backend services, a scenario-based test runner, and analysis tools.

---

## What It Does

1. **HTTP Reverse Proxy (`server`)**:
   - Forwards client HTTP/1.1 requests to upstream backend servers.
   - Supports two concurrency runtimes:
     - **Thread Pool (`thread_pool`)**: A bounded pool of operating system threads handling connections synchronously.
     - **Async (`async`)**: An asynchronous event loop powered by Tokio.
   - Supports multiple load balancing algorithms:
     - Round Robin (`round_robin`)
     - Weighted Round Robin (`weighted_round_robin`)
     - Least Connections (`least_connections`)
     - Least Response Time (`least_response_time`)
   - Includes basic active health checks, connection retries for idempotent requests, and an administrative control API.

2. **Backend Fixtures (`backend-fixture`)**:
   - Small mock HTTP servers designed to run in Docker containers.
   - Can simulate specific processing delays, network jitter, concurrency limits, and error rates (e.g. 500 status codes or TCP disconnects).

3. **Experiment Runner (`scenario-runner`)**:
   - Automates benchmark suites defined in TOML manifests (`experiments/*.toml`).
   - Starts Docker backend profiles, configures the proxy, generates paced traffic to prevent coordinated omission, runs timed chaos actions (stopping/starting containers), and records detailed request logs.

4. **Management CLI (`serverctl`)**:
   - Command-line tool to query metrics and add, remove, or modify routes and services at runtime via the proxy's admin API.

5. **Result Analysis (`analyze-results`)**:
   - Aggregates raw request measurements (`requests.jsonl`) into summary CSV files and calculates percentiles (p50, p95, p99), error rates, scheduling lag, and Jain's fairness index.

---

## Project Structure

```text
├── experiments/             # TOML manifests describing benchmark scenarios
├── results/                 # Output directories containing run metrics and CSV summaries
├── docs/                    # Detailed technical documentation and experiment guides
├── compose.fixtures.yml     # Docker Compose profiles for baseline (3-node) backends
├── compose.scaled-fixtures.yml # Docker Compose profiles for scaled (12/13-node) backends
└── server/                  # Rust source code
    ├── Cargo.toml
    └── src/
        ├── main.rs          # Server entry point
        ├── bin/
        │   ├── scenario_runner.rs  # Automated benchmark runner
        │   ├── backend_fixture.rs  # Mock backend service
        │   ├── serverctl.rs        # Management CLI tool
        │   └── analyze_results.rs  # Post-run metrics aggregator
        ├── proxy/           # Proxy forwarding logic (sync & async)
        └── algorithms/      # Load balancing algorithms
```

---

## Prerequisites

- **Rust toolchain** (1.80+ recommended, edition 2024):
  ```bash
  rustc --version
  cargo --version
  ```
- **Docker & Docker Compose** (for running backend fixture containers).

---

## Building the Project

Navigate to the `server/` directory and build in release mode:

```bash
cd server
cargo build --release
```

This compiles all binaries into `server/target/release/`:
- `server`
- `scenario-runner`
- `backend-fixture`
- `serverctl`
- `analyze-results`

---

## Quick Start & Common Commands

All commands below assume you are inside the `server/` directory.

### 1. Running the Proxy Server Manually

Start the proxy using one of the fixture configuration files:

```bash
cargo run --release --bin server -- --config config/fixtures/equal-capacity.toml
```

By default:
- **Proxy listener**: `127.0.0.1:7879` (where clients send traffic)
- **Admin listener**: `127.0.0.1:7880` (for `serverctl` management)
- **Control listener**: `127.0.0.1:7878`

### 2. Using the Management CLI (`serverctl`)

While the server is running, you can inspect or change settings dynamically:

```bash
# List all configured services
cargo run --release --bin serverctl -- service list

# Check live proxy metrics (JSON format)
cargo run --release --bin serverctl -- --json metrics

# Switch runtime mode between thread_pool and async
cargo run --release --bin serverctl -- runtime set async
cargo run --release --bin serverctl -- runtime set thread_pool

# Change load balancing algorithm for a service
cargo run --release --bin serverctl -- service update fixture --algorithm least_connections
```

### 3. Running Mock Backends (Docker Compose)

From the project root directory, you can manually launch backend fixtures:

```bash
# Start the baseline 3-node equal-capacity cluster
docker compose -f compose.fixtures.yml --profile equal-capacity up -d

# Check running backend containers
docker compose -f compose.fixtures.yml ps

# Stop the containers
docker compose -f compose.fixtures.yml --profile equal-capacity down
```

### 4. Running Automated Experiments (`scenario-runner`)

The scenario runner handles Docker setup, proxy execution, traffic generation, and teardown automatically.

**Validate an experiment manifest:**
```bash
cargo run --bin scenario-runner -- validate ../experiments/equal-capacity.toml
```

**Dry run (print the planned matrix without running):**
```bash
cargo run --bin scenario-runner -- ../experiments/equal-capacity.toml --dry-run
```

**Execute a full experiment suite:**
```bash
cargo run --release --bin scenario-runner -- run ../experiments/equal-capacity.toml
```

**Run a single test combination (useful for testing):**
```bash
cargo run --release --bin scenario-runner -- run ../experiments/equal-capacity.toml \
  --scenario equal-capacity \
  --algorithm round_robin \
  --runtime thread_pool \
  --repetitions 1
```

Available flags:
- `--scenario <ID>`: Filter by scenario ID.
- `--algorithm <NAME>`: `round_robin`, `weighted_round_robin`, `least_connections`, `least_response_time`.
- `--runtime <NAME>`: `thread_pool`, `async`.
- `--repetitions <N>`: Override the repetition count.
- `--output <DIR>`: Output directory for test logs (defaults to `../results`).

### 5. Analyzing Experiment Results

Once an experiment finishes, you can re-run or inspect the analysis on a results folder:

```bash
cargo run --release --bin analyze-results -- ../results/equal-capacity-<timestamp>
```

This updates files in `results/<run-id>/analysis/`:
- `group-summary.csv`: Aggregated averages across runs (throughput, p50/p95/p99 latency, error rate, fairness, memory, scheduling lag).
- `run-summary.csv`: Per-run metric breakdown.
- `backend-fairness.csv`: Request counts and shares per backend node.
- `recovery.csv`: Time-to-recovery metrics for failure scenarios.

---

## Limitations

- **Not a production web server**: Built for research and comparative benchmarking rather than production edge deployment.
- **Protocol scope**: Implements HTTP/1.1 over plain TCP. It does not support HTTPS/TLS, HTTP/2, or WebSocket upgrades.
- **In-memory configuration**: Changes made via `serverctl` take effect immediately in memory but are not persisted back to the TOML configuration files on disk.
