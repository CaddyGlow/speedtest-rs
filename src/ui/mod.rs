pub mod compact;

use std::time::Duration;

use crate::cli::TuiMode;

pub enum SpeedProgress {
    Disabled,
    Compact(compact::SpeedProgressBar),
}

#[derive(Debug, Clone, Copy)]
pub struct SpeedProgressSample {
    pub elapsed: Duration,
    pub mbps: f64,
    pub bytes: u64,
    pub active_connections: usize,
    pub latency_ms: Option<f64>,
    pub jitter_ms: Option<f64>,
}

pub struct Ui {
    enabled: bool,
    mode: TuiMode,
}

impl Ui {
    pub fn new(mode: TuiMode, enabled: bool) -> Self {
        Self { enabled, mode }
    }

    pub fn render_phase(&mut self, phase: &str) {
        if !self.enabled {
            return;
        }

        match self.mode {
            TuiMode::Compact => compact::render_phase(phase),
        }
    }

    pub fn render_metric(&mut self, label: &str, value: &str) {
        if !self.enabled {
            return;
        }

        match self.mode {
            TuiMode::Compact => compact::render_metric(label, value),
        }
    }

    pub fn begin_speed_progress(&mut self, phase: &str, seconds: u64) -> SpeedProgress {
        if !self.enabled {
            return SpeedProgress::Disabled;
        }

        match self.mode {
            TuiMode::Compact => {
                SpeedProgress::Compact(compact::begin_speed_progress(phase, seconds))
            }
        }
    }

    #[must_use]
    pub fn progress_interval(&self) -> Option<Duration> {
        if !self.enabled {
            return None;
        }

        Some(Duration::from_millis(250))
    }

    pub fn update_speed_progress(&mut self, progress: &SpeedProgress, sample: SpeedProgressSample) {
        if !self.enabled {
            return;
        }

        match progress {
            SpeedProgress::Disabled => {}
            SpeedProgress::Compact(bar) => compact::update_speed_progress(bar, sample),
        }
    }

    pub fn finish_speed_progress(
        &mut self,
        progress: SpeedProgress,
        _phase: &str,
        mbps: f64,
        bytes: u64,
    ) {
        if !self.enabled {
            return;
        }

        match progress {
            SpeedProgress::Disabled => {}
            SpeedProgress::Compact(bar) => compact::finish_speed_progress(bar, mbps, bytes),
        }
    }

    pub fn shutdown(&mut self) {}
}
