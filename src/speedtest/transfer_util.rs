use std::future::Future;
use std::ops::Range;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tokio::task::JoinSet;
use tokio::sync::watch;
use tokio::time::timeout;

use crate::speedtest::servers::SpeedtestServer;
use crate::speedtest::throughput::{ThroughputCalculator, TransferConfig};

pub fn normalize_server_pool(
    selected_server: &SpeedtestServer,
    server_pool: &[SpeedtestServer],
) -> Vec<SpeedtestServer> {
    if server_pool.is_empty() {
        vec![selected_server.clone()]
    } else {
        server_pool.to_vec()
    }
}

pub struct ActiveConnectionGuard<'a> {
    counter: &'a AtomicUsize,
}

impl<'a> ActiveConnectionGuard<'a> {
    pub fn new(counter: &'a AtomicUsize) -> Self {
        counter.fetch_add(1, Ordering::Relaxed);
        Self { counter }
    }
}

impl Drop for ActiveConnectionGuard<'_> {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

pub struct TransferControl {
    pub stage_stop_tx: watch::Sender<bool>,
    pub stage_stop_rx: watch::Receiver<bool>,
    pub first_transfer_tx: watch::Sender<bool>,
    pub first_transfer_rx: watch::Receiver<bool>,
    pub first_byte_at: Arc<OnceLock<Instant>>,
    pub target_workers: Arc<AtomicUsize>,
    pub suggested_size: Arc<AtomicUsize>,
}

pub struct TransferLoopState {
    pub stage_end_at: Instant,
    pub progress_clock_start: Option<Instant>,
    pub transfer_started: bool,
    pub last_progress_at: Option<Instant>,
    pub spawned_count: usize,
}

pub struct TransferSample {
    pub sample_elapsed_ms: u64,
    pub sample_bytes: u64,
    pub progress_bytes: u64,
    pub progress_mbps: Option<f64>,
}

pub fn new_transfer_control(worker_count: usize, start_request_size: usize) -> TransferControl {
    let (stage_stop_tx, stage_stop_rx) = watch::channel(false);
    let (first_transfer_tx, first_transfer_rx) = watch::channel(false);

    TransferControl {
        stage_stop_tx,
        stage_stop_rx,
        first_transfer_tx,
        first_transfer_rx,
        first_byte_at: Arc::new(OnceLock::new()),
        target_workers: Arc::new(AtomicUsize::new(worker_count)),
        suggested_size: Arc::new(AtomicUsize::new(start_request_size)),
    }
}

pub async fn resolve_ready_server_pool<F, Fut>(
    server_pool: &[SpeedtestServer],
    default_guid: &str,
    probe: F,
) -> Vec<SpeedtestServer>
where
    F: Fn(SpeedtestServer, String) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = bool> + Send + 'static,
{
    let mut tasks = JoinSet::new();

    for server in server_pool.iter().cloned() {
        let guid = server
            .session_guid
            .clone()
            .unwrap_or_else(|| default_guid.to_string());
        let probe = probe.clone();
        tasks.spawn(async move {
            let server_id = server.id;
            let is_ready = probe(server, guid).await;
            (server_id, is_ready)
        });
    }

    let mut ready_ids = std::collections::HashSet::new();
    while let Some(joined) = tasks.join_next().await {
        if let Ok((server_id, true)) = joined {
            ready_ids.insert(server_id);
        }
    }

    server_pool
        .iter()
        .filter(|server| ready_ids.contains(&server.id))
        .cloned()
        .collect()
}

pub async fn drain_join_set(tasks: &mut JoinSet<()>, grace_period: Duration) {
    let drained = timeout(grace_period, async {
        while tasks.join_next().await.is_some() {}
    })
    .await;

    if drained.is_err() {
        tasks.abort_all();
        let _ = timeout(grace_period, async {
            while tasks.join_next().await.is_some() {}
        })
        .await;
    }
}

#[must_use]
pub fn elapsed_ms_since(start: Option<Instant>) -> u64 {
    start
        .map(|it| Instant::now().saturating_duration_since(it).as_millis() as u64)
        .unwrap_or(0)
}

pub fn sync_transfer_start(
    transfer_started: &mut bool,
    first_transfer_rx: &watch::Receiver<bool>,
    first_byte_at: &OnceLock<Instant>,
    max_seconds: u64,
    stage_end_at: &mut Instant,
    progress_clock_start: &mut Option<Instant>,
) {
    if *transfer_started || !*first_transfer_rx.borrow() {
        return;
    }

    if let Some(started_at) = first_byte_at.get().copied() {
        *transfer_started = true;
        *stage_end_at = started_at + Duration::from_secs(max_seconds);
        *progress_clock_start = Some(started_at);
    }
}

pub fn stage_deadline_reached(stage_end_at: Instant, stage_stop_tx: &watch::Sender<bool>) -> bool {
    if Instant::now() < stage_end_at {
        return false;
    }

    let _ = stage_stop_tx.send(true);
    true
}

pub fn should_emit_progress(
    last_progress_at: &mut Option<Instant>,
    now: Instant,
    interval: Duration,
) -> bool {
    let should_report = last_progress_at.is_none_or(|t| now.duration_since(t) >= interval);
    if should_report {
        *last_progress_at = Some(now);
    }
    should_report
}

pub async fn run_transfer_loop<Sample, Spawn, Report>(
    tasks: &mut JoinSet<()>,
    first_transfer_rx: &mut watch::Receiver<bool>,
    control: &TransferControl,
    loop_state: &mut TransferLoopState,
    poll_interval: Duration,
    config: &TransferConfig,
    active_connections: &AtomicUsize,
    calc: &mut ThroughputCalculator,
    mut sample_progress: Sample,
    mut spawn_workers: Spawn,
    mut report_progress: Report,
) where
    Sample: FnMut(Instant, Duration) -> TransferSample,
    Spawn: FnMut(Range<usize>, &mut JoinSet<()>),
    Report: FnMut(Duration, u64, f64, usize),
{
    loop {
        sync_transfer_start(
            &mut loop_state.transfer_started,
            first_transfer_rx,
            &control.first_byte_at,
            config.max_seconds,
            &mut loop_state.stage_end_at,
            &mut loop_state.progress_clock_start,
        );

        if stage_deadline_reached(loop_state.stage_end_at, &control.stage_stop_tx) {
            break;
        }

        tokio::select! {
            joined = tasks.join_next() => {
                if joined.is_none() {
                    break;
                }
            }
            changed = first_transfer_rx.changed() => {
                if changed.is_err() {
                    continue;
                }
            }
            _ = tokio::time::sleep(poll_interval) => {
                if let Some(clock_start) = loop_state.progress_clock_start {
                    let now = Instant::now();
                    let elapsed = now.saturating_duration_since(clock_start);
                    let sample = sample_progress(now, elapsed);
                    let blended_bps = calc.record_sample(sample.sample_elapsed_ms, sample.sample_bytes);

                    let elapsed_ms = elapsed.as_millis() as u64;
                    let desired = calc.desired_connections(config.connections);
                    control.target_workers.store(desired, Ordering::Relaxed);
                    if desired > loop_state.spawned_count {
                        spawn_workers(loop_state.spawned_count..desired, tasks);
                        loop_state.spawned_count = desired;
                    }

                    let time_remaining_ms = (config.max_seconds * 1000).saturating_sub(elapsed_ms);
                    let conns = active_connections.load(Ordering::Relaxed).max(1);
                    let size = calc.suggested_request_size(conns, time_remaining_ms, config);
                    control.suggested_size.store(size, Ordering::Relaxed);

                    if let Some(interval) = config.progress_interval
                        && should_emit_progress(&mut loop_state.last_progress_at, now, interval)
                    {
                        report_progress(
                            elapsed,
                            sample.progress_bytes,
                            sample.progress_mbps.unwrap_or(blended_bps * 8.0 / 1_000_000.0),
                            active_connections.load(Ordering::Relaxed),
                        );
                    }
                }
            }
        }
    }
}

pub fn spawn_worker_range<Spawn>(range: Range<usize>, tasks: &mut JoinSet<()>, mut spawn: Spawn)
where
    Spawn: FnMut(usize, &mut JoinSet<()>),
{
    for worker_index in range {
        spawn(worker_index, tasks);
    }
}
