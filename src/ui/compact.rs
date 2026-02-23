use indicatif::{ProgressBar, ProgressStyle};
use std::time::Duration;

use crate::ui::SpeedProgressSample;

pub struct SpeedProgressBar {
    phase: String,
    total_seconds: u64,
    bar: ProgressBar,
}

pub fn render_phase(phase: &str) {
    println!("[compact] {phase}");
}

pub fn render_metric(label: &str, value: &str) {
    println!("[compact] {label}: {value}");
}

pub fn begin_speed_progress(phase: &str, seconds: u64) -> SpeedProgressBar {
    let total_seconds = seconds.max(1);
    let style = ProgressStyle::with_template(
        "{spinner:.green} {msg:>9} [{bar:32.cyan/blue}] {pos:>2}/{len:2}s {prefix}",
    )
    .unwrap_or_else(|_| ProgressStyle::default_bar())
    .progress_chars("=>-");

    let bar = ProgressBar::new(total_seconds);
    bar.set_style(style);
    bar.enable_steady_tick(Duration::from_millis(120));
    bar.set_message(phase.to_string());

    SpeedProgressBar {
        phase: phase.to_string(),
        total_seconds,
        bar,
    }
}

pub fn update_speed_progress(progress: &SpeedProgressBar, sample: SpeedProgressSample) {
    let elapsed_secs = sample.elapsed.as_secs().min(progress.total_seconds);
    progress.bar.set_position(elapsed_secs);
    progress.bar.set_message(progress.phase.clone());
    let latency_label = sample
        .latency_ms
        .filter(|value| value.is_finite() && *value >= 0.0)
        .map(|value| format!(" {value:.2}ms"))
        .unwrap_or_default();
    let jitter_label = sample
        .jitter_ms
        .filter(|value| value.is_finite() && *value >= 0.0)
        .map(|value| format!(" j{value:.2}"))
        .unwrap_or_default();
    progress.bar.set_prefix(format!(
        "{:7.2} Mbps {:.1} MB {} conn{latency_label}{jitter_label}",
        sample.mbps,
        sample.bytes as f64 / 1_000_000.0,
        sample.active_connections,
    ));
}

pub fn finish_speed_progress(progress: SpeedProgressBar, mbps: f64, bytes: u64) {
    progress.bar.set_position(progress.total_seconds);
    progress.bar.set_prefix(String::new());
    progress.bar.finish_with_message(format!(
        "{} done {mbps:.2} Mbps ({:.1} MB)",
        progress.phase,
        bytes as f64 / 1_000_000.0
    ));
}
