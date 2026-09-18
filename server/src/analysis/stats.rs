use serde_json::Value;

use super::models::{
    EventInput, LatencyAnalysis, RequestInput, SchedulingLagAnalysis, SloAnalysis,
    StatisticalSummary,
};

pub(crate) fn measurement_duration_seconds(requests: &[RequestInput]) -> Option<f64> {
    let started = requests
        .iter()
        .map(|request| request.started_offset_us)
        .min()?;
    let completed = requests
        .iter()
        .map(|request| request.started_offset_us.saturating_add(request.latency_us))
        .max()?;
    (completed > started).then_some((completed - started) as f64 / 1_000_000.0)
}

pub(crate) fn latency_analysis(requests: &[RequestInput]) -> LatencyAnalysis {
    let mut values = requests
        .iter()
        .map(|request| request.latency_us)
        .collect::<Vec<_>>();
    if values.is_empty() {
        return LatencyAnalysis::default();
    }
    values.sort_unstable();
    LatencyAnalysis {
        samples: values.len(),
        mean_us: Some(values.iter().map(|value| *value as f64).sum::<f64>() / values.len() as f64),
        min_us: values.first().copied(),
        p50_us: percentile(&values, 0.50),
        p90_us: percentile(&values, 0.90),
        p95_us: percentile(&values, 0.95),
        p99_us: percentile(&values, 0.99),
        p999_us: percentile(&values, 0.999),
        max_us: values.last().copied(),
    }
}

pub(crate) fn slo_analysis(requests: &[RequestInput], target_ms: Option<u64>) -> SloAnalysis {
    let Some(target_ms) = target_ms else {
        return SloAnalysis::default();
    };
    let target_us = target_ms.saturating_mul(1_000);
    let successful_within_slo = requests
        .iter()
        .filter(|request| request.http_success && request.latency_us <= target_us)
        .count();
    let miss_count = requests.len().saturating_sub(successful_within_slo);
    let attainment_rate =
        (!requests.is_empty()).then_some(successful_within_slo as f64 / requests.len() as f64);
    SloAnalysis {
        target_ms: Some(target_ms),
        eligible_requests: Some(requests.len()),
        successful_within_slo: Some(successful_within_slo),
        miss_count: Some(miss_count),
        attainment_rate,
        miss_rate: attainment_rate.map(|rate| 1.0 - rate),
    }
}

pub(crate) fn scheduling_lag_analysis(requests: &[RequestInput]) -> SchedulingLagAnalysis {
    let mut values = requests
        .iter()
        .map(|request| {
            request
                .started_offset_us
                .saturating_sub(request.scheduled_offset_us)
        })
        .collect::<Vec<_>>();
    if values.is_empty() {
        return SchedulingLagAnalysis::default();
    }
    values.sort_unstable();
    SchedulingLagAnalysis {
        samples: values.len(),
        mean_us: Some(values.iter().map(|value| *value as f64).sum::<f64>() / values.len() as f64),
        p50_us: percentile(&values, 0.50),
        p95_us: percentile(&values, 0.95),
        p99_us: percentile(&values, 0.99),
        max_us: values.last().copied(),
    }
}

pub(crate) fn percentile(sorted: &[u64], probability: f64) -> Option<f64> {
    if sorted.is_empty() || !(0.0..=1.0).contains(&probability) {
        return None;
    }
    let position = probability * (sorted.len() - 1) as f64;
    let lower = position.floor() as usize;
    let upper = position.ceil() as usize;
    let fraction = position - lower as f64;
    Some(sorted[lower] as f64 + (sorted[upper] as f64 - sorted[lower] as f64) * fraction)
}

pub(crate) fn jain_index(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let sum = values.iter().sum::<f64>();
    let squares = values.iter().map(|value| value * value).sum::<f64>();
    (squares > 0.0).then_some(sum * sum / (values.len() as f64 * squares))
}

pub(crate) fn mean_u64(values: &[u64]) -> Option<f64> {
    (!values.is_empty())
        .then(|| values.iter().map(|value| *value as f64).sum::<f64>() / values.len() as f64)
}

pub(crate) fn is_recovery_word(word: &str) -> bool {
    matches!(
        word,
        "recover"
            | "recovery"
            | "restore"
            | "restart"
            | "resume"
            | "enable"
            | "start"
            | "heal"
            | "up"
    )
}

pub(crate) fn is_recovery_action(event: &EventInput) -> bool {
    let label = event.label.to_ascii_lowercase();
    if label
        .split(|character: char| !character.is_ascii_alphanumeric())
        .any(is_recovery_word)
    {
        return true;
    }

    // Labels intentionally use compound names such as "restore-one". Command
    // arguments must instead match whole words: a project or file containing
    // "failure-recovery" does not turn `docker ... stop` into a recovery action.
    event
        .details
        .get("command")
        .and_then(Value::as_str)
        .is_some_and(|command| {
            command
                .to_ascii_lowercase()
                .split_ascii_whitespace()
                .any(is_recovery_word)
        })
}

pub(crate) fn first_stable_success(completions: &[(u64, bool)], required: usize) -> Option<u64> {
    if required == 0 {
        return completions.first().map(|(completed, _)| *completed);
    }
    let mut consecutive = 0_usize;
    let mut start = 0_u64;
    for (completed, success) in completions {
        if *success {
            if consecutive == 0 {
                start = *completed;
            }
            consecutive += 1;
            if consecutive >= required {
                return Some(start);
            }
        } else {
            consecutive = 0;
        }
    }
    None
}

pub(crate) fn summarize_values(values: impl Iterator<Item = f64>) -> StatisticalSummary {
    let values = values.filter(|value| value.is_finite()).collect::<Vec<_>>();
    if values.is_empty() {
        return StatisticalSummary::default();
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let standard_deviation = if values.len() > 1 {
        Some(
            (values
                .iter()
                .map(|value| (value - mean).powi(2))
                .sum::<f64>()
                / (values.len() - 1) as f64)
                .sqrt(),
        )
    } else {
        None
    };
    let margin = standard_deviation.map(|deviation| {
        student_t_critical_95(values.len() - 1) * deviation / (values.len() as f64).sqrt()
    });
    StatisticalSummary {
        samples: values.len(),
        mean: Some(mean),
        standard_deviation,
        confidence_interval_95_low: margin.map(|margin| mean - margin),
        confidence_interval_95_high: margin.map(|margin| mean + margin),
        min: values.iter().copied().reduce(f64::min),
        max: values.iter().copied().reduce(f64::max),
    }
}

pub(crate) fn student_t_critical_95(degrees_of_freedom: usize) -> f64 {
    const CRITICAL: [f64; 30] = [
        12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365, 2.306, 2.262, 2.228, 2.201, 2.179, 2.160,
        2.145, 2.131, 2.120, 2.110, 2.101, 2.093, 2.086, 2.080, 2.074, 2.069, 2.064, 2.060, 2.056,
        2.052, 2.048, 2.045, 2.042,
    ];
    degrees_of_freedom
        .checked_sub(1)
        .and_then(|index| CRITICAL.get(index))
        .copied()
        .unwrap_or(1.96)
}

