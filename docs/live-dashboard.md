# Live experiment dashboard

The dashboard is a read-only view over scenario-runner results. It follows active
runs and can replay any saved run without importing or transforming the source
artifacts.

## Start it

From `server/`:

```text
cargo run --release --bin dashboard -- --results ../results
```

Open `http://127.0.0.1:7890`. Use `--address IP:PORT` to choose another listener.
The results root must already exist; `--results` can point at any directory
created by `scenario-runner`.

The dashboard may run before, during, or after the scenario runner. Active run
snapshots refresh every 750 ms and the experiment catalog refreshes every three
seconds, so a newly created experiment appears without restarting the dashboard.

## Live and replay behavior

- Live mode follows the newest flushed request, event, and 100 ms process-resource
  sample for the selected active run.
- Saved replay uses the raw observation timestamps as its clock. Play, pause,
  scrub, and 0.5x–10x speed controls do not modify result files.
- A run whose artifact metadata still says `running` but has had no artifact
  activity for 15 minutes is shown as interrupted instead of indefinitely live.
- The API aligns workload-relative request/resource offsets with absolute run
  timestamps, so setup, workload, failure, and teardown events share one replay
  timeline.

## Metric consistency

The run snapshot calls the same `analysis::analyze_run_snapshot` implementation
used by `analyze-results`. At the latest replay position, the cards use its
throughput, R-7 p95 latency, combined transport/HTTP error rate, and Jain fairness
values. During partial replay, those metrics are recomputed over the visible prefix
with the same definitions. Charts read these source artifacts directly:

- `requests.jsonl`
- `events.jsonl`
- `resource-samples.jsonl`
- `metrics-before.json` and `metrics-after.json`
- `metadata.json`

Large command stdout/stderr fields are omitted from dashboard responses so setup
logs cannot dominate browser memory. They remain untouched in `events.jsonl`.

## Read-only HTTP API

```text
GET /api/experiments
GET /api/experiments/{experiment_id}
GET /api/experiments/{experiment_id}/runs/{run_id}
```

Identifiers are restricted to direct child directory names containing letters,
digits, hyphens, or underscores. The server never writes to the results root.
