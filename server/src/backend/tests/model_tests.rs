use super::*;

#[test]
fn test_backend_metrics_lifecycle() {
    let metrics = BackendMetrics::new();
    assert_eq!(
        metrics
            .active_connections
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );

    metrics.record_start();
    assert_eq!(
        metrics
            .active_connections
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    assert_eq!(
        metrics
            .total_requests
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );

    metrics.record_end(
        &Feedback {
            latency: Duration::from_micros(500),
            success: true,
        },
        100,
        250,
    );

    let snap = metrics.snapshot();
    assert_eq!(snap.active_connections, 0);
    assert_eq!(snap.total_requests, 1);
    assert_eq!(snap.successful_requests, 1);
    assert_eq!(snap.failed_requests, 0);
    assert_eq!(snap.total_bytes_sent, 100);
    assert_eq!(snap.total_bytes_received, 250);
    // (1.0 - 0.2) * 1000 + 0.2 * 500 = 800 + 100 = 900
    assert_eq!(snap.latency_us, 900);
}

#[test]
fn test_backend_metrics_failure() {
    let metrics = BackendMetrics::new();
    metrics.record_start();
    metrics.record_end(
        &Feedback {
            latency: Duration::from_millis(10),
            success: false,
        },
        50,
        0,
    );

    let snap = metrics.snapshot();
    assert_eq!(snap.active_connections, 0);
    assert_eq!(snap.total_requests, 1);
    assert_eq!(snap.successful_requests, 0);
    assert_eq!(snap.failed_requests, 1);
    assert_eq!(snap.total_bytes_sent, 50);
    assert_eq!(snap.total_bytes_received, 0);
    // (1.0 - 0.2) * 1000 + 0.2 * 10_000_000 = 800 + 2_000_000 = 2_000_800
    assert_eq!(snap.latency_us, 2_000_800);
}

#[test]
fn test_active_connections_underflow_prevention() {
    let metrics = BackendMetrics::new();
    // Record end without start should not underflow
    metrics.record_end(
        &Feedback {
            latency: Duration::from_millis(1),
            success: false,
        },
        0,
        0,
    );
    assert_eq!(
        metrics
            .active_connections
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}
