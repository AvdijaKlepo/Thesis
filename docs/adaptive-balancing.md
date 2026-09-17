# Adaptive balancing

`adaptive_balancing` is a deadline-aware policy intended for controlled comparison with the existing load balancers. Existing server configurations and experiment manifests remain unchanged.

The policy treats a backend attempt as a positive observation when the transport succeeds within 200 ms. It maintains an exponentially weighted success probability for each backend, adds a UCB-style exploration bonus for less-used backends, and subtracts a small penalty for attempts already in flight. Initial selections are spread across the eligible backends before normal scoring begins.

The policy is deterministic. Exploration comes from the confidence bonus rather than random sampling, so a seeded experiment does not acquire another source of randomness. A backend can still be probed after poor observations, allowing recovery to be detected.

To try the policy without editing the checked-in configuration, switch a running service through the management interface:

```text
serverctl algorithm set default adaptive_balancing
```

The v1 implementation intentionally holds the production target at 200 ms. `AdaptiveBalancing::with_deadline` accepts a different target for focused tests; v1 configuration remains unchanged.

`adaptive_balancing_v2` is available as a separate policy for the scaling study. It keeps a deadline-hit EWMA and a continuous latency utility, normalizes in-flight pressure by backend weight, and revisits stale healthy backends on a bounded probe interval. Its defaults are a 200 ms deadline, 0.2 EWMA alpha, 0.7 SLO weight, a 32-selection per-backend probe interval, and a 0.05 in-flight penalty. Configure these values under a proxy service's `adaptive_v2` block; the management API returns the effective block and accepts a replacement block at runtime.

The main comparison metrics are the proportion of requests completed within the target, p95 and p99 latency, achieved throughput, backend allocation, and proxy CPU cost. Warm-up behavior and the time required to react to a backend latency change should be reported separately.

The focused experiment is defined in [`experiments/adaptive-balancing.toml`](../experiments/adaptive-balancing.toml). It compares all five policies under the async runtime, with moderate background traffic, a four-second overload burst, and a post-burst settling interval. Run it from `server/` after building the release binaries:

```text
cargo build --release --bins
./target/release/scenario-runner run ../experiments/adaptive-balancing.toml
```
