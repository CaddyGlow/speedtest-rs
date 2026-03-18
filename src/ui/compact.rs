use std::cell::Cell;
use std::cell::RefCell;
use std::time::Duration;

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

use crate::ui::SpeedProgressSample;
use crate::ui::sparkline::Sparkline;

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
    sparkline_width: usize,
    bar: ProgressBar,
    sparkline_bar: ProgressBar,
    sparkline: RefCell<Sparkline>,
    last_sparkline_index: Cell<Option<usize>>,
    last_non_zero_mbps: Cell<f64>,
    last_rtt_ms: Cell<Option<f64>>,
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

        let term_width = console::Term::stdout().size().1 as usize;
        let sparkline_width = term_width.saturating_sub(2).max(20);

        let sparkline_bar = self.multi.insert_after(&bar, ProgressBar::new_spinner());
        sparkline_bar.set_style(
            ProgressStyle::with_template("  {msg}")
                .unwrap_or_else(|_| ProgressStyle::default_spinner()),
        );

        SpeedProgressBar {
            phase: phase.to_string(),
            total_seconds,
            sparkline_width,
            bar,
            sparkline_bar,
            sparkline: RefCell::new(Sparkline::new(sparkline_width)),
            last_sparkline_index: Cell::new(None),
            last_non_zero_mbps: Cell::new(0.0),
            last_rtt_ms: Cell::new(None),
        }
    }

    pub fn update_speed_progress(&self, progress: &SpeedProgressBar, sample: SpeedProgressSample) {
        let elapsed_secs = sample.elapsed.as_secs().min(progress.total_seconds);
        progress.bar.set_position(elapsed_secs);
        progress.bar.set_message(progress.phase.clone());
        let mut mbps = sample.mbps;
        if !mbps.is_finite() {
            mbps = 0.0;
        }
        let mbps = if mbps <= 0.0 {
            let previous = progress.last_non_zero_mbps.get();
            if previous > 0.0 { previous } else { 0.0 }
        } else {
            progress.last_non_zero_mbps.set(mbps);
            mbps
        };
        if let Some(rtt) = sample.rtt_ms.filter(|rtt| rtt.is_finite() && *rtt >= 0.0) {
            progress.last_rtt_ms.set(Some(rtt));
        }
        progress.bar.set_prefix(format!(
            "{:7.2} Mbps {:.1} MB{}",
            mbps,
            sample.bytes as f64 / 1_000_000.0,
            progress
                .last_rtt_ms
                .get()
                .map(|rtt| format!(" RTT {rtt:.2} ms"))
                .unwrap_or_default(),
        ));

        let sparkline_index = progress.sample_index(sample.elapsed);
        let mut sparkline = progress.sparkline.borrow_mut();
        if let Some(previous_index) = progress.last_sparkline_index.get()
            && sparkline_index > previous_index
        {
            for idx in previous_index + 1..sparkline_index {
                sparkline.set(idx, mbps);
            }
        }
        sparkline.set(sparkline_index, mbps);
        progress.last_sparkline_index.set(Some(sparkline_index));
        progress.sparkline_bar.set_message(sparkline.render());
    }

    pub fn finish_speed_progress(&self, progress: SpeedProgressBar, mbps: f64, bytes: u64) {
        progress.bar.set_position(progress.total_seconds);
        progress.bar.set_prefix(String::new());
        progress.bar.finish_with_message(format!(
            "{} done {mbps:.2} Mbps ({:.1} MB){}",
            progress.phase,
            bytes as f64 / 1_000_000.0,
            progress
                .last_rtt_ms
                .get()
                .map(|rtt| format!(" RTT {rtt:.2} ms"))
                .unwrap_or_default(),
        ));
        progress
            .sparkline_bar
            .finish_with_message(progress.sparkline.borrow().render());
    }

    pub fn shutdown(&mut self) {
        for metric in self.metrics.drain(..) {
            metric.bar.finish_and_clear();
        }
        self.phase.finish_and_clear();
    }
}

impl SpeedProgressBar {
    fn sample_index(&self, elapsed: Duration) -> usize {
        if self.sparkline_width <= 1 || self.total_seconds == 0 {
            return 0;
        }

        let total_nanos = u128::from(self.total_seconds) * 1_000_000_000;
        let elapsed_nanos = elapsed.as_nanos().min(total_nanos);
        let max_index = (self.sparkline_width - 1) as u128;
        ((elapsed_nanos * max_index) / total_nanos) as usize
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn compact_ui_tests_live_elsewhere() {}
}
