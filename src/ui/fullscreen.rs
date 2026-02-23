use std::io::stdout;
use std::time::{Duration, Instant};

use crossterm::cursor::{Hide, Show};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Gauge, List, ListItem, Paragraph};

use crate::ui::SpeedProgressSample;

#[derive(Debug, Clone)]
struct ProgressState {
    phase: String,
    total_seconds: u64,
    elapsed: Duration,
    mbps: f64,
    bytes: u64,
    active_connections: usize,
    latency_ms: Option<f64>,
    jitter_ms: Option<f64>,
}

pub struct FullscreenUi {
    terminal: Terminal<CrosstermBackend<std::io::Stdout>>,
    phase: String,
    metrics: Vec<(String, String)>,
    progress: Option<ProgressState>,
    download_result: Option<String>,
    upload_result: Option<String>,
    min_draw_interval: Duration,
    last_draw_at: Instant,
    active: bool,
}

impl FullscreenUi {
    pub fn start() -> anyhow::Result<Self> {
        enable_raw_mode()?;
        let mut stdout = stdout();
        execute!(stdout, EnterAlternateScreen, Hide)?;

        let backend = CrosstermBackend::new(stdout);
        let terminal = Terminal::new(backend)?;

        let mut ui = Self {
            terminal,
            phase: "initializing".to_string(),
            metrics: Vec::new(),
            progress: None,
            download_result: None,
            upload_result: None,
            min_draw_interval: Duration::from_millis(900),
            last_draw_at: Instant::now(),
            active: true,
        };
        ui.draw_now()?;
        Ok(ui)
    }

    pub fn render_phase(&mut self, phase: &str) -> anyhow::Result<()> {
        self.phase = phase.to_string();
        self.draw_now()
    }

    pub fn render_metric(&mut self, label: &str, value: &str) -> anyhow::Result<()> {
        match self.metrics.iter_mut().find(|(key, _)| key == label) {
            Some((_, current)) => *current = value.to_string(),
            None => self.metrics.push((label.to_string(), value.to_string())),
        }
        self.draw_now()
    }

    pub fn begin_speed_progress(&mut self, phase: &str, seconds: u64) -> anyhow::Result<()> {
        self.progress = Some(ProgressState {
            phase: phase.to_string(),
            total_seconds: seconds.max(1),
            elapsed: Duration::ZERO,
            mbps: 0.0,
            bytes: 0,
            active_connections: 0,
            latency_ms: None,
            jitter_ms: None,
        });
        self.draw_now()
    }

    pub fn update_speed_progress(
        &mut self,
        phase: &str,
        total_seconds: u64,
        sample: SpeedProgressSample,
    ) -> anyhow::Result<()> {
        self.progress = Some(ProgressState {
            phase: phase.to_string(),
            total_seconds: total_seconds.max(1),
            elapsed: sample.elapsed,
            mbps: sample.mbps,
            bytes: sample.bytes,
            active_connections: sample.active_connections,
            latency_ms: sample.latency_ms,
            jitter_ms: sample.jitter_ms,
        });
        self.draw_throttled()
    }

    pub fn finish_speed_progress(
        &mut self,
        phase: &str,
        mbps: f64,
        bytes: u64,
    ) -> anyhow::Result<()> {
        let summary = format!("{mbps:.2} Mbps ({:.1} MB)", bytes as f64 / 1_000_000.0);
        match phase {
            "download" => self.download_result = Some(summary),
            "upload" => self.upload_result = Some(summary),
            _ => {}
        }
        self.progress = None;
        self.draw_now()
    }

    pub fn shutdown(&mut self) -> anyhow::Result<()> {
        if !self.active {
            return Ok(());
        }

        disable_raw_mode()?;
        execute!(self.terminal.backend_mut(), Show, LeaveAlternateScreen)?;
        self.terminal.show_cursor()?;
        self.active = false;
        Ok(())
    }

    fn draw_throttled(&mut self) -> anyhow::Result<()> {
        if self.last_draw_at.elapsed() >= self.min_draw_interval {
            self.draw_now()?;
        }
        Ok(())
    }

    fn draw_now(&mut self) -> anyhow::Result<()> {
        if !self.active {
            return Ok(());
        }

        let phase = self.phase.clone();
        let metrics = self.metrics.clone();
        let progress = self.progress.clone();
        let download_result = self.download_result.clone();
        let upload_result = self.upload_result.clone();

        self.terminal.draw(|frame| {
            let area = frame.area();
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(3),
                    Constraint::Length(3),
                    Constraint::Length(8),
                    Constraint::Min(5),
                ])
                .split(area);

            let header = Paragraph::new(Line::from(vec![
                Span::styled(
                    "tunmux-speedtest",
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw("  phase: "),
                Span::styled(phase.clone(), Style::default().fg(Color::Yellow)),
            ]))
            .block(Block::default().title("Status").borders(Borders::ALL));
            frame.render_widget(header, chunks[0]);

            let (ratio, gauge_label) = if let Some(current) = &progress {
                let elapsed = current.elapsed.as_secs().min(current.total_seconds);
                let ratio = elapsed as f64 / current.total_seconds as f64;
                (
                    ratio,
                    format!(
                        "{} {:.2} Mbps {:.1} MB {} conn{}{}",
                        current.phase,
                        current.mbps,
                        current.bytes as f64 / 1_000_000.0,
                        current.active_connections,
                        current
                            .latency_ms
                            .filter(|value| value.is_finite() && *value >= 0.0)
                            .map(|value| format!(" {value:.2}ms"))
                            .unwrap_or_default(),
                        current
                            .jitter_ms
                            .filter(|value| value.is_finite() && *value >= 0.0)
                            .map(|value| format!(" j{value:.2}"))
                            .unwrap_or_default(),
                    ),
                )
            } else {
                (0.0, "idle".to_string())
            };

            let gauge = Gauge::default()
                .block(
                    Block::default()
                        .title("Live Throughput")
                        .borders(Borders::ALL),
                )
                .gauge_style(Style::default().fg(Color::LightBlue))
                .ratio(ratio.clamp(0.0, 1.0))
                .label(gauge_label);
            frame.render_widget(gauge, chunks[1]);

            let metrics_items = if metrics.is_empty() {
                vec![ListItem::new(Line::from("waiting for metrics"))]
            } else {
                metrics
                    .iter()
                    .map(|(label, value)| {
                        ListItem::new(Line::from(vec![
                            Span::styled(
                                format!("{label}: "),
                                Style::default()
                                    .fg(Color::White)
                                    .add_modifier(Modifier::BOLD),
                            ),
                            Span::raw(value.clone()),
                        ]))
                    })
                    .collect::<Vec<_>>()
            };

            let metrics_widget = List::new(metrics_items)
                .block(Block::default().title("Metrics").borders(Borders::ALL));
            frame.render_widget(metrics_widget, chunks[2]);

            let mut result_lines = Vec::new();
            if let Some(download) = download_result {
                result_lines.push(Line::from(vec![
                    Span::styled("download: ", Style::default().fg(Color::Green)),
                    Span::raw(download),
                ]));
            }
            if let Some(upload) = upload_result {
                result_lines.push(Line::from(vec![
                    Span::styled("upload: ", Style::default().fg(Color::Magenta)),
                    Span::raw(upload),
                ]));
            }
            if result_lines.is_empty() {
                result_lines.push(Line::from("benchmark results will appear here"));
            }

            let results = Paragraph::new(result_lines)
                .block(Block::default().title("Results").borders(Borders::ALL));
            frame.render_widget(results, chunks[3]);
        })?;

        self.last_draw_at = Instant::now();

        Ok(())
    }
}

impl Drop for FullscreenUi {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}
