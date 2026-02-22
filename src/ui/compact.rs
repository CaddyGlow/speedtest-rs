use indicatif::{ProgressBar, ProgressStyle};
use std::time::Duration;

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

pub fn update_speed_progress(
    progress: &SpeedProgressBar,
    elapsed: Duration,
    mbps: f64,
    bytes: u64,
) {
    let elapsed_secs = elapsed.as_secs().min(progress.total_seconds);
    progress.bar.set_position(elapsed_secs);
    progress.bar.set_message(progress.phase.clone());
    progress.bar.set_prefix(format!(
        "{mbps:7.2} Mbps {:.1} MB",
        bytes as f64 / 1_000_000.0
    ));
}

pub fn finish_speed_progress(progress: SpeedProgressBar, mbps: f64, bytes: u64) {
    progress.bar.set_position(progress.total_seconds);
    progress.bar.finish_with_message(format!(
        "{} done {mbps:.2} Mbps ({:.1} MB)",
        progress.phase,
        bytes as f64 / 1_000_000.0
    ));
}
