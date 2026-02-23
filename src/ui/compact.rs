use std::time::Duration;

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

use crate::ui::SpeedProgressSample;

struct MetricLine {
    key: String,
    bar: ProgressBar,
    max_width: usize,
}

pub struct CompactUi {
    multi: MultiProgress,
    phase: ProgressBar,
    metrics: Vec<MetricLine>,
}

pub struct SpeedProgressBar {
    phase: String,
    total_seconds: u64,
    bar: ProgressBar,
}

impl CompactUi {
    pub fn new() -> Self {
        let multi = MultiProgress::new();

        let phase = multi.add(ProgressBar::new_spinner());
        phase.set_style(
            ProgressStyle::with_template("{spinner:.green} {msg:.bold.cyan}")
                .unwrap_or_else(|_| ProgressStyle::default_spinner()),
        );
        phase.enable_steady_tick(Duration::from_millis(120));
        phase.set_message("initializing".to_string());

        Self {
            multi,
            phase,
            metrics: Vec::new(),
        }
    }

    pub fn render_phase(&mut self, phase: &str) {
        self.phase.set_message(format!("phase: {phase}"));
    }

    pub fn render_metric(&mut self, label: &str, value: &str) {
        let message = format!("{label:<16} {value}");
        if let Some(metric) = self.metrics.iter_mut().find(|metric| metric.key == label) {
            metric.max_width = metric.max_width.max(message.len());
            metric
                .bar
                .set_message(format!("{:<width$}", message, width = metric.max_width));
            return;
        }

        let bar = self.multi.add(ProgressBar::new_spinner());
        bar.set_style(
            ProgressStyle::with_template("  {msg:.dim}")
                .unwrap_or_else(|_| ProgressStyle::default_spinner()),
        );
        let width = message.len();
        bar.set_message(message);
        self.metrics.push(MetricLine {
            key: label.to_string(),
            bar,
            max_width: width,
        });
    }

    pub fn begin_speed_progress(&self, phase: &str, seconds: u64) -> SpeedProgressBar {
        let total_seconds = seconds.max(1);
        let bar = self.multi.add(ProgressBar::new(total_seconds));
        bar.set_style(
            ProgressStyle::with_template(
                "{spinner:.green} {msg:.bold.yellow:>9} [{bar:32.cyan/blue}] {pos:>2}/{len:2}s {prefix}",
            )
            .unwrap_or_else(|_| ProgressStyle::default_bar())
            .progress_chars("=>-"),
        );
        bar.enable_steady_tick(Duration::from_millis(120));
        bar.set_message(phase.to_string());

        SpeedProgressBar {
            phase: phase.to_string(),
            total_seconds,
            bar,
        }
    }

    pub fn update_speed_progress(&self, progress: &SpeedProgressBar, sample: SpeedProgressSample) {
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

    pub fn finish_speed_progress(&self, progress: SpeedProgressBar, mbps: f64, bytes: u64) {
        progress.bar.set_position(progress.total_seconds);
        progress.bar.set_prefix(String::new());
        progress.bar.finish_with_message(format!(
            "{} done {mbps:.2} Mbps ({:.1} MB)",
            progress.phase,
            bytes as f64 / 1_000_000.0
        ));
    }

    pub fn shutdown(&mut self) {
        for metric in self.metrics.drain(..) {
            metric.bar.finish_and_clear();
        }
        self.phase.finish_and_clear();
    }
}
