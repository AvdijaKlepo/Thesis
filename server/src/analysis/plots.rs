use std::fs;
use std::fmt::Write;
use std::path::Path;

use super::models::{AnalysisError, AnalysisReport, GroupAnalysis};

pub(crate) fn write_plots(report: &AnalysisReport) -> Result<(), AnalysisError> {
    let directory = report.analysis_directory.join("plots");
    fs::create_dir_all(&directory)?;
    let labels = report.groups.iter().map(group_label).collect::<Vec<_>>();
    bar_chart(
        &directory.join("throughput.svg"),
        "Successful throughput",
        "requests / second",
        &labels,
        &[(
            "successful throughput",
            report
                .groups
                .iter()
                .map(|group| group.successful_throughput_requests_per_s.mean)
                .collect(),
        )],
    )?;
    bar_chart(
        &directory.join("error-rate.svg"),
        "End-to-end error rate",
        "percent",
        &labels,
        &[(
            "errors",
            report
                .groups
                .iter()
                .map(|group| group.error_rate.mean.map(|value| value * 100.0))
                .collect(),
        )],
    )?;
    bar_chart(
        &directory.join("latency-percentiles.svg"),
        "End-to-end latency percentiles",
        "milliseconds",
        &labels,
        &[
            (
                "p50",
                report
                    .groups
                    .iter()
                    .map(|group| group.latency_p50_us.mean.map(|value| value / 1_000.0))
                    .collect(),
            ),
            (
                "p95",
                report
                    .groups
                    .iter()
                    .map(|group| group.latency_p95_us.mean.map(|value| value / 1_000.0))
                    .collect(),
            ),
            (
                "p99",
                report
                    .groups
                    .iter()
                    .map(|group| group.latency_p99_us.mean.map(|value| value / 1_000.0))
                    .collect(),
            ),
        ],
    )?;
    bar_chart(
        &directory.join("fairness.svg"),
        "Backend allocation fairness",
        "Jain index",
        &labels,
        &[
            (
                "unweighted",
                report
                    .groups
                    .iter()
                    .map(|group| group.fairness_jain_index.mean)
                    .collect(),
            ),
            (
                "weight-adjusted",
                report
                    .groups
                    .iter()
                    .map(|group| group.weighted_fairness_jain_index.mean)
                    .collect(),
            ),
        ],
    )?;
    bar_chart(
        &directory.join("cpu-usage.svg"),
        "Server process CPU usage",
        "percent of one logical CPU",
        &labels,
        &[(
            "average CPU",
            report
                .groups
                .iter()
                .map(|group| group.average_cpu_percent.mean)
                .collect(),
        )],
    )?;
    bar_chart(
        &directory.join("peak-memory.svg"),
        "Peak server resident memory",
        "MiB",
        &labels,
        &[(
            "peak RSS",
            report
                .groups
                .iter()
                .map(|group| {
                group
                    .peak_resident_memory_bytes
                    .mean
                    .map(|value| value / 1_048_576.0)
            })
            .collect(),
        )],
    )?;
    bar_chart(
        &directory.join("recovery-time.svg"),
        "Stable recovery time",
        "milliseconds",
        &labels,
        &[(
            "time to five successes",
            report
                .groups
                .iter()
                .map(|group| group.recovery_time_ms.mean)
                .collect(),
        )],
    )?;
    Ok(())
}

fn group_label(group: &GroupAnalysis) -> String {
    format!(
        "{} / {} / {}",
        group.scenario, group.algorithm, group.runtime
    )
}

fn bar_chart(
    path: &Path,
    title: &str,
    y_label: &str,
    labels: &[String],
    series: &[(&str, Vec<Option<f64>>)],
) -> Result<(), AnalysisError> {
    const COLORS: [&str; 6] = [
        "#0072B2", "#D55E00", "#009E73", "#CC79A7", "#E69F00", "#56B4E9",
    ];
    let width = (220 + labels.len().max(1) * 125).max(900) as f64;
    let height = 620.0;
    let left = 100.0;
    let right = 35.0;
    let top = 90.0;
    let bottom = 175.0;
    let plot_width = width - left - right;
    let plot_height = height - top - bottom;
    let maximum = series
        .iter()
        .flat_map(|(_, values)| values.iter().flatten().copied())
        .filter(|value| value.is_finite() && *value >= 0.0)
        .reduce(f64::max)
        .unwrap_or(1.0)
        .max(f64::EPSILON);
    let axis_max = nice_axis_max(maximum);

    let mut svg = String::new();
    writeln!(
        svg,
        "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"{width:.0}\" height=\"{height:.0}\" viewBox=\"0 0 {width:.0} {height:.0}\">"
    )
    .unwrap();
    writeln!(svg, "<rect width=\"100%\" height=\"100%\" fill=\"white\"/>").unwrap();
    writeln!(
        svg,
        "<style>text{{font-family:Arial,Helvetica,sans-serif;fill:#222}} .grid{{stroke:#d9d9d9;stroke-width:1}} .axis{{stroke:#333;stroke-width:1.5}}</style>"
    )
    .unwrap();
    writeln!(
        svg,
        "<text x=\"{:.1}\" y=\"38\" text-anchor=\"middle\" font-size=\"22\" font-weight=\"600\">{}</text>",
        width / 2.0,
        xml_escape(title)
    )
    .unwrap();
    writeln!(
        svg,
        "<text transform=\"translate(25 {:.1}) rotate(-90)\" text-anchor=\"middle\" font-size=\"14\">{}</text>",
        top + plot_height / 2.0,
        xml_escape(y_label)
    )
    .unwrap();

    for tick in 0..=5 {
        let ratio = tick as f64 / 5.0;
        let y = top + plot_height * (1.0 - ratio);
        let value = axis_max * ratio;
        writeln!(
            svg,
            "<line class=\"grid\" x1=\"{left:.1}\" y1=\"{y:.1}\" x2=\"{:.1}\" y2=\"{y:.1}\"/><text x=\"{:.1}\" y=\"{:.1}\" text-anchor=\"end\" font-size=\"12\">{}</text>",
            width - right,
            left - 10.0,
            y + 4.0,
            compact_number(value)
        )
        .unwrap();
    }
    writeln!(
        svg,
        "<line class=\"axis\" x1=\"{left:.1}\" y1=\"{top:.1}\" x2=\"{left:.1}\" y2=\"{:.1}\"/><line class=\"axis\" x1=\"{left:.1}\" y1=\"{:.1}\" x2=\"{:.1}\" y2=\"{:.1}\"/>",
        top + plot_height,
        top + plot_height,
        width - right,
        top + plot_height
    )
    .unwrap();

    if labels.is_empty() {
        writeln!(
            svg,
            "<text x=\"{:.1}\" y=\"{:.1}\" text-anchor=\"middle\" font-size=\"16\" fill=\"#666\">No eligible runs</text>",
            left + plot_width / 2.0,
            top + plot_height / 2.0
        )
        .unwrap();
    } else {
        let group_width = plot_width / labels.len() as f64;
        let series_count = series.len().max(1);
        let bar_width = (group_width * 0.72 / series_count as f64).min(42.0);
        for (group_index, label) in labels.iter().enumerate() {
            let center = left + group_width * (group_index as f64 + 0.5);
            for (series_index, (_, values)) in series.iter().enumerate() {
                let x = center - (bar_width * series_count as f64) / 2.0
                    + bar_width * series_index as f64;
                if let Some(value) = values.get(group_index).copied().flatten() {
                    let bar_height = (value.max(0.0) / axis_max * plot_height).min(plot_height);
                    let y = top + plot_height - bar_height;
                    writeln!(
                        svg,
                        "<rect x=\"{x:.2}\" y=\"{y:.2}\" width=\"{:.2}\" height=\"{bar_height:.2}\" fill=\"{}\"/>",
                        (bar_width - 2.0).max(1.0),
                        COLORS[series_index % COLORS.len()]
                    )
                    .unwrap();
                }
            }
            writeln!(
                svg,
                "<text transform=\"translate({:.1} {:.1}) rotate(-38)\" text-anchor=\"end\" font-size=\"11\">{}</text>",
                center,
                top + plot_height + 18.0,
                xml_escape(label)
            )
            .unwrap();
        }
    }

    if series.len() > 1 {
        let legend_width = series.len() as f64 * 150.0;
        let start = (width - legend_width) / 2.0;
        for (index, (name, _)) in series.iter().enumerate() {
            let x = start + index as f64 * 150.0;
            writeln!(
                svg,
                "<rect x=\"{x:.1}\" y=\"55\" width=\"14\" height=\"14\" fill=\"{}\"/><text x=\"{:.1}\" y=\"67\" font-size=\"12\">{}</text>",
                COLORS[index % COLORS.len()],
                x + 20.0,
                xml_escape(name)
            )
            .unwrap();
        }
    }
    writeln!(svg, "</svg>").unwrap();
    fs::write(path, svg)?;
    Ok(())
}

fn nice_axis_max(maximum: f64) -> f64 {
    if maximum <= 0.0 || !maximum.is_finite() {
        return 1.0;
    }
    let rough_step = maximum / 5.0;
    let magnitude = 10_f64.powf(rough_step.log10().floor());
    let normalized = rough_step / magnitude;
    let nice = if normalized <= 1.0 {
        1.0
    } else if normalized <= 2.0 {
        2.0
    } else if normalized <= 5.0 {
        5.0
    } else {
        10.0
    };
    (nice * magnitude * 5.0).max(maximum)
}

fn compact_number(value: f64) -> String {
    if value.abs() >= 1_000.0 {
        format!("{value:.0}")
    } else if value.abs() >= 10.0 {
        format!("{value:.1}")
    } else {
        format!("{value:.2}")
    }
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

