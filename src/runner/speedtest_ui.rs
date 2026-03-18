use std::collections::HashMap;

use crate::speedtest::engine;
use crate::ui;

pub(super) struct SpeedtestUiController {
    ui: ui::Ui,
    download_seconds: u64,
    upload_seconds: u64,
    server_names: HashMap<u64, String>,
    download_progress: Option<ui::SpeedProgress>,
    upload_progress: Option<ui::SpeedProgress>,
    download_last: Option<(f64, u64)>,
    upload_last: Option<(f64, u64)>,
    probe_completed: usize,
    probe_failed: usize,
    probe_best: Option<(u64, String, f64, f64)>,
}

impl SpeedtestUiController {
    pub(super) fn new(
        render_ui: bool,
        download_seconds: u64,
        upload_seconds: u64,
        server_names: HashMap<u64, String>,
    ) -> Self {
        Self {
            ui: ui::Ui::new(render_ui),
            download_seconds,
            upload_seconds,
            server_names,
            download_progress: None,
            upload_progress: None,
            download_last: None,
            upload_last: None,
            probe_completed: 0,
            probe_failed: 0,
            probe_best: None,
        }
    }

    pub(super) fn progress_interval(&self) -> Option<std::time::Duration> {
        self.ui.progress_interval()
    }

    pub(super) fn set_server_names(&mut self, server_names: HashMap<u64, String>) {
        self.server_names = server_names;
    }

    pub(super) fn render_phase(&mut self, phase: &str) {
        self.ui.render_phase(phase);
    }

    pub(super) fn render_metric(&mut self, label: &str, value: &str) {
        self.ui.render_metric(label, value);
    }

    pub(super) fn handle_engine_event(&mut self, event: engine::EngineEvent) {
        match event {
            engine::EngineEvent::StageStarting(stage) => match stage {
                engine::EngineStage::ServerSelection => {
                    self.ui.render_phase("probing latency across candidates");
                }
                engine::EngineStage::Latency => {
                    self.ui.render_phase("selecting best latency server");
                }
                engine::EngineStage::Download => {
                    self.ui.render_phase("running download test");
                    self.download_progress = Some(
                        self.ui
                            .begin_speed_progress("download", self.download_seconds),
                    );
                    self.download_last = None;
                }
                engine::EngineStage::Upload => {
                    self.ui.render_phase("running upload test");
                    self.upload_progress =
                        Some(self.ui.begin_speed_progress("upload", self.upload_seconds));
                    self.upload_last = None;
                }
                engine::EngineStage::Save => {
                    self.ui.render_phase("building result payload");
                }
                engine::EngineStage::Finished => {
                    self.ui.render_phase("benchmark complete");
                }
            },
            engine::EngineEvent::CandidateProbed {
                index,
                total,
                server_id,
                average_ms,
                variance_ms,
                error,
            } => {
                self.probe_completed = self.probe_completed.saturating_add(1);
                let server_name = self
                    .server_names
                    .get(&server_id)
                    .map(String::as_str)
                    .unwrap_or("unknown");
                if let Some(avg) = average_ms {
                    let stddev = variance_ms.unwrap_or(0.0).max(0.0).sqrt();
                    self.ui.render_metric(
                        "probe_current",
                        &format!(
                            "{index}/{total} id={server_id} {server_name} avg={avg:.2}ms std={stddev:.2}"
                        ),
                    );

                    let better_than_best = match self.probe_best.as_ref() {
                        None => true,
                        Some((_, _, best_avg, best_stddev)) => {
                            let avg_better = avg < *best_avg;
                            let avg_tied = (avg - *best_avg).abs() <= 0.000_1;
                            avg_better || (avg_tied && stddev < *best_stddev)
                        }
                    };

                    if better_than_best {
                        self.probe_best = Some((server_id, server_name.to_string(), avg, stddev));
                    }

                    if let Some((best_id, best_name, best_avg, best_stddev)) =
                        self.probe_best.as_ref()
                    {
                        self.ui.render_metric(
                            "probe_best",
                            &format!(
                                "id={best_id} {best_name} avg={best_avg:.2}ms std={best_stddev:.2}"
                            ),
                        );
                    }
                } else {
                    self.probe_failed = self.probe_failed.saturating_add(1);
                    let reason = error.as_deref().unwrap_or("probe failed");
                    self.ui.render_metric(
                        "probe_current",
                        &format!("{index}/{total} id={server_id} {server_name} failed ({reason})"),
                    );
                }

                self.ui.render_metric(
                    "probe_progress",
                    &format!("{}/{total} complete ({} failed)", self.probe_completed, self.probe_failed),
                );
            }
            engine::EngineEvent::ServerSelected {
                server_id,
                average_ms,
                variance_ms,
            } => {
                let stddev = variance_ms.max(0.0).sqrt();
                let server_name = self
                    .server_names
                    .get(&server_id)
                    .map(String::as_str)
                    .unwrap_or("unknown");
                self.ui.render_metric(
                    "selected_server",
                    &format!("id={server_id} {server_name} avg={average_ms:.2}ms std={stddev:.2}"),
                );
            }
            engine::EngineEvent::StageProgress {
                stage,
                elapsed,
                mbps,
                bytes,
                active_connections,
                rtt_ms,
            } => {
                let _ = active_connections;
                let sample = ui::SpeedProgressSample {
                    elapsed,
                    mbps,
                    bytes,
                    rtt_ms,
                };
                match stage {
                    engine::EngineStage::Download => {
                        self.download_last = Some((mbps, bytes));
                        if let Some(progress) = self.download_progress.as_ref() {
                            self.ui.update_speed_progress(progress, sample);
                        }
                    }
                    engine::EngineStage::Upload => {
                        self.upload_last = Some((mbps, bytes));
                        if let Some(progress) = self.upload_progress.as_ref() {
                            self.ui.update_speed_progress(progress, sample);
                        }
                    }
                    _ => {}
                }
            }
            engine::EngineEvent::StageResult { stage, mbps, bytes } => match stage {
                engine::EngineStage::Download => {
                    self.download_last = Some((mbps, bytes));
                }
                engine::EngineStage::Upload => {
                    self.upload_last = Some((mbps, bytes));
                }
                _ => {}
            },
            engine::EngineEvent::StageFinished(stage) => match stage {
                engine::EngineStage::Download => {
                    if let Some(progress) = self.download_progress.take() {
                        let (mbps, bytes) = self.download_last.unwrap_or((0.0, 0));
                        self.ui.finish_speed_progress(progress, "download", mbps, bytes);
                    }
                }
                engine::EngineStage::Upload => {
                    if let Some(progress) = self.upload_progress.take() {
                        let (mbps, bytes) = self.upload_last.unwrap_or((0.0, 0));
                        self.ui.finish_speed_progress(progress, "upload", mbps, bytes);
                    }
                }
                _ => {}
            },
            engine::EngineEvent::SavePayloadBuilt { guid, hash } => {
                self.ui.render_metric(
                    "save",
                    &format!("guid={} hash={}", guid, &hash[..hash.len().min(12)]),
                );
            }
        }
    }

    pub(super) fn shutdown(&mut self) {
        self.ui.shutdown();
    }
}
