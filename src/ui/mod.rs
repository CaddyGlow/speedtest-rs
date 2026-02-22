pub mod compact;
pub mod fullscreen;

use std::time::Duration;

use crate::cli::TuiMode;

pub enum SpeedProgress {
    Disabled,
    Compact(compact::SpeedProgressBar),
    Fullscreen { phase: String, total_seconds: u64 },
}

pub struct Ui {
    enabled: bool,
    mode: TuiMode,
    fullscreen: Option<fullscreen::FullscreenUi>,
}

impl Ui {
    pub fn new(mode: TuiMode, enabled: bool) -> Self {
        if !enabled {
            return Self {
                enabled,
                mode,
                fullscreen: None,
            };
        }

        match mode {
            TuiMode::Compact => Self {
                enabled,
                mode,
                fullscreen: None,
            },
            TuiMode::Fullscreen => match fullscreen::FullscreenUi::start() {
                Ok(fullscreen) => Self {
                    enabled,
                    mode,
                    fullscreen: Some(fullscreen),
                },
                Err(error) => {
                    eprintln!(
                        "fullscreen tui unavailable, falling back to compact mode: {}",
                        error
                    );
                    Self {
                        enabled,
                        mode: TuiMode::Compact,
                        fullscreen: None,
                    }
                }
            },
        }
    }

    pub fn render_phase(&mut self, phase: &str) {
        if !self.enabled {
            return;
        }

        match self.mode {
            TuiMode::Compact => compact::render_phase(phase),
            TuiMode::Fullscreen => {
                if let Some(fullscreen) = self.fullscreen.as_mut()
                    && let Err(error) = fullscreen.render_phase(phase)
                {
                    self.fallback_to_compact(error.to_string(), phase);
                }
            }
        }
    }

    pub fn render_metric(&mut self, label: &str, value: &str) {
        if !self.enabled {
            return;
        }

        match self.mode {
            TuiMode::Compact => compact::render_metric(label, value),
            TuiMode::Fullscreen => {
                if let Some(fullscreen) = self.fullscreen.as_mut()
                    && let Err(error) = fullscreen.render_metric(label, value)
                {
                    self.fallback_to_compact(error.to_string(), "fullscreen draw failure");
                    compact::render_metric(label, value);
                }
            }
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
            TuiMode::Fullscreen => {
                if let Some(fullscreen) = self.fullscreen.as_mut()
                    && let Err(error) = fullscreen.begin_speed_progress(phase, seconds)
                {
                    self.fallback_to_compact(error.to_string(), phase);
                    return SpeedProgress::Compact(compact::begin_speed_progress(phase, seconds));
                }

                SpeedProgress::Fullscreen {
                    phase: phase.to_string(),
                    total_seconds: seconds.max(1),
                }
            }
        }
    }

    #[must_use]
    pub fn progress_interval(&self) -> Option<Duration> {
        if !self.enabled {
            return None;
        }

        match self.mode {
            TuiMode::Compact => Some(Duration::from_millis(250)),
            TuiMode::Fullscreen => Some(Duration::from_secs(1)),
        }
    }

    pub fn update_speed_progress(
        &mut self,
        progress: &SpeedProgress,
        elapsed: Duration,
        mbps: f64,
        bytes: u64,
    ) {
        if !self.enabled {
            return;
        }

        match progress {
            SpeedProgress::Disabled => {}
            SpeedProgress::Compact(bar) => {
                compact::update_speed_progress(bar, elapsed, mbps, bytes);
            }
            SpeedProgress::Fullscreen {
                phase,
                total_seconds,
            } => {
                if let Some(fullscreen) = self.fullscreen.as_mut()
                    && let Err(error) = fullscreen.update_speed_progress(
                        phase,
                        *total_seconds,
                        elapsed,
                        mbps,
                        bytes,
                    )
                {
                    self.fallback_to_compact(error.to_string(), phase);
                }
            }
        }
    }

    pub fn finish_speed_progress(
        &mut self,
        progress: SpeedProgress,
        phase: &str,
        mbps: f64,
        bytes: u64,
    ) {
        if !self.enabled {
            return;
        }

        match progress {
            SpeedProgress::Disabled => {}
            SpeedProgress::Compact(bar) => compact::finish_speed_progress(bar, mbps, bytes),
            SpeedProgress::Fullscreen { .. } => {
                if let Some(fullscreen) = self.fullscreen.as_mut()
                    && let Err(error) = fullscreen.finish_speed_progress(phase, mbps, bytes)
                {
                    self.fallback_to_compact(error.to_string(), phase);
                    compact::render_metric(phase, &format!("{mbps:.2} Mbps ({bytes} bytes)"));
                }
            }
        }
    }

    pub fn shutdown(&mut self) {
        if let Some(fullscreen) = self.fullscreen.as_mut() {
            let _ = fullscreen.shutdown();
        }
        self.fullscreen = None;
    }

    fn fallback_to_compact(&mut self, reason: String, phase: &str) {
        eprintln!(
            "fullscreen tui failed, falling back to compact mode: {}",
            reason
        );
        if let Some(fullscreen) = self.fullscreen.as_mut() {
            let _ = fullscreen.shutdown();
        }
        self.fullscreen = None;
        self.mode = TuiMode::Compact;
        compact::render_phase(phase);
    }
}

impl Drop for Ui {
    fn drop(&mut self) {
        self.shutdown();
    }
}
