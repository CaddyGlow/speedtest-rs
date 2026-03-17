use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use reqwest::Client;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout};
use tracing::debug;

use crate::speedtest::api::TransportProtocol;
use crate::speedtest::browser_protocol;
use crate::speedtest::modern_protocol;
use crate::speedtest::servers::SpeedtestServer;
use crate::speedtest::throughput::{ThroughputCalculator, ThroughputResult, TransferConfig};
use crate::speedtest::transfer_util::{ActiveConnectionGuard, normalize_server_pool};
use crate::util::clamp_worker_count;

const REMOTE_STATS_SAMPLE_INTERVAL_MS: u64 = 250;
const REMOTE_STATS_JOIN_WAIT_MS: u64 = 3_500;
const REMOTE_STATS_BACKGROUND_UPLOAD_BYTES: usize = 32 * 1024;
const REMOTE_STATS_BACKGROUND_UPLOAD_INTERVAL_MS: u64 = 500;
const STAGE_POLL_INTERVAL: Duration = Duration::from_millis(50);
const STAGE_JOIN_GRACE_PERIOD: Duration = Duration::from_millis(250);
const SERVER_READINESS_TIMEOUT: Duration = Duration::from_secs(8);
const UPLOAD_READINESS_BYTES: usize = 32 * 1024;
const DEFAULT_UPLOAD_SIZE: usize = 1_048_576;

#[derive(Debug, Clone)]
pub struct UploadStats {
    pub bytes: u64,
    pub mbps: f64,
    pub actual_duration_ms: u64,
    pub throughput: Option<ThroughputResult>,
    pub request_attempts: u64,
    pub request_successes: u64,
    pub request_http_errors: u64,
    pub request_transport_errors: u64,
    pub response_read_errors: u64,
    pub remote_samples: Vec<browser_protocol::UploadStatsSample>,
}

#[derive(Debug, Clone, Copy)]
pub struct UploadProgress {
    pub elapsed: Duration,
    pub bytes: u64,
    pub mbps: f64,
    pub active_connections: usize,
}

pub async fn run_upload_test<F>(
    client: &Client,
    selected_server: &SpeedtestServer,
    mode: TransportProtocol,
    server_pool: &[SpeedtestServer],
    config: &TransferConfig,
    mut on_progress: F,
) -> Result<UploadStats>
where
    F: FnMut(UploadProgress),
{
    let worker_count = clamp_worker_count(config.connections);
    let total_bytes = Arc::new(AtomicU64::new(0));
    let request_attempts = Arc::new(AtomicU64::new(0));
    let request_successes = Arc::new(AtomicU64::new(0));
    let request_http_errors = Arc::new(AtomicU64::new(0));
    let request_transport_errors = Arc::new(AtomicU64::new(0));
    let response_read_errors = Arc::new(AtomicU64::new(0));
    let active_connections = Arc::new(AtomicUsize::new(0));
    let transfer_pool = normalize_server_pool(selected_server, server_pool);
    let default_guid = selected_server
        .session_guid
        .clone()
        .or_else(|| {
            transfer_pool
                .iter()
                .find_map(|server| server.session_guid.clone())
        })
        .unwrap_or_else(|| "tunmux-speedtest".to_string());
    let ready_pool = resolve_ready_upload_pool(client, mode, &transfer_pool, &default_guid).await;
    if ready_pool.is_empty() {
        bail!("no speedtest servers became ready for upload stage");
    }
    let selected_server_ready = ready_pool
        .iter()
        .any(|server| server.id == selected_server.id);

    let start_at = Instant::now();
    let mut stage_end_at = start_at + Duration::from_secs(config.max_seconds);
    let mut upload_stats_task = None;
    let mut remote_stats_background_task = None;
    let mut remote_stats_background_stop = None;
    let mut upload_stats_rx = None;
    let mut latest_remote_sample = None;
    let mut remote_samples = Vec::new();
    let (stage_stop_tx, stage_stop_rx) = watch::channel(false);
    let (first_transfer_tx, mut first_transfer_rx) = watch::channel(false);

    if matches!(mode, TransportProtocol::Xhr)
        && selected_server_ready
        && let Some(guid) = selected_server
            .session_guid
            .clone()
            .or_else(|| Some(default_guid.clone()))
    {
        debug!(
            server_id = selected_server.id,
            guid = %guid,
            duration_seconds = config.max_seconds,
            sample_interval_ms = REMOTE_STATS_SAMPLE_INTERVAL_MS,
            "upload remote stats stream enabled"
        );
        let (tx, rx) = mpsc::unbounded_channel();
        let stats_server = selected_server.clone();
        let stats_guid = guid.clone();
        let ws_sender = tx.clone();
        let stats_duration_seconds = config.max_seconds;
        upload_stats_task = Some(tokio::spawn(async move {
            browser_protocol::stream_upload_stats_samples(
                &stats_server,
                &stats_guid,
                stats_duration_seconds,
                REMOTE_STATS_SAMPLE_INTERVAL_MS,
                ws_sender,
            )
            .await
        }));

        let background_stop = Arc::new(AtomicBool::new(false));
        let background_client = client.clone();
        let background_server = selected_server.clone();
        let background_guid = guid.clone();
        let background_stop_flag = Arc::clone(&background_stop);
        let background_total_bytes = Arc::clone(&total_bytes);
        remote_stats_background_task = Some(tokio::spawn(async move {
            run_remote_stats_background_upload(
                &background_client,
                &background_server,
                &background_guid,
                background_stop_flag,
                &background_total_bytes,
            )
            .await;
        }));
        remote_stats_background_stop = Some(background_stop);
        upload_stats_rx = Some(rx);
    }

    let target_workers = Arc::new(AtomicUsize::new(worker_count));
    let suggested_size = Arc::new(AtomicUsize::new(DEFAULT_UPLOAD_SIZE));

    let mut tasks = JoinSet::new();
    let mut spawned_count = worker_count;
    match mode {
        TransportProtocol::Xhr => {
            run_upload_workers_modern_sdk(
                client,
                &ready_pool,
                &default_guid,
                worker_count,
                &total_bytes,
                &request_attempts,
                &request_successes,
                &request_http_errors,
                &request_transport_errors,
                &response_read_errors,
                &active_connections,
                &target_workers,
                &suggested_size,
                &stage_stop_rx,
                &first_transfer_tx,
                &mut tasks,
            );
        }
        TransportProtocol::Tcp => {
            run_upload_workers_modern(
                &ready_pool,
                worker_count,
                &total_bytes,
                &request_attempts,
                &request_successes,
                &request_transport_errors,
                &response_read_errors,
                &active_connections,
                &target_workers,
                &suggested_size,
                &stage_stop_rx,
                &first_transfer_tx,
                &mut tasks,
            );
        }
    }

    let poll_interval = STAGE_POLL_INTERVAL;
    let mut calc = ThroughputCalculator::new(config.max_seconds * 1000);
    let mut progress_clock_start = None;
    let mut transfer_started = false;
    let mut last_progress_at: Option<Instant> = None;

    loop {
        if !transfer_started && *first_transfer_rx.borrow() {
            let now = Instant::now();
            transfer_started = true;
            stage_end_at = now + Duration::from_secs(config.max_seconds);
            progress_clock_start = Some(now);
        }

        if Instant::now() >= stage_end_at {
            let _ = stage_stop_tx.send(true);
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
            _ = sleep(poll_interval) => {
                if let Some(clock_start) = progress_clock_start {
                    if let Some(rx) = upload_stats_rx.as_mut() {
                        while let Ok(sample) = rx.try_recv() {
                            latest_remote_sample = Some(sample);
                            remote_samples.push(sample);
                        }
                    }

                    let now = Instant::now();
                    let local_bytes = total_bytes.load(Ordering::Relaxed);
                    let elapsed = now.saturating_duration_since(clock_start);
                    let blended_bps = calc.record_sample(elapsed.as_millis() as u64, local_bytes);

                    if let Some(sample) = latest_remote_sample {
                        let _ = remote_sample_rate(sample, elapsed);
                    }

                    let elapsed_ms = elapsed.as_millis() as u64;
                    let desired = calc.desired_connections(config.connections);
                    target_workers.store(desired, Ordering::Relaxed);
                    if desired > spawned_count && !ready_pool.is_empty() {
                        for idx in spawned_count..desired {
                            match mode {
                                TransportProtocol::Tcp => spawn_upload_worker_tcp(
                                    idx,
                                    &ready_pool,
                                    &total_bytes,
                                    &request_attempts,
                                    &request_successes,
                                    &request_transport_errors,
                                    &response_read_errors,
                                    &active_connections,
                                    &target_workers,
                                    &suggested_size,
                                    &stage_stop_rx,
                                    &first_transfer_tx,
                                    &mut tasks,
                                ),
                                TransportProtocol::Xhr => spawn_upload_worker_xhr(
                                    idx,
                                    client,
                                    &ready_pool,
                                    &default_guid,
                                    &total_bytes,
                                    &request_attempts,
                                    &request_successes,
                                    &request_http_errors,
                                    &request_transport_errors,
                                    &response_read_errors,
                                    &active_connections,
                                    &target_workers,
                                    &suggested_size,
                                    &stage_stop_rx,
                                    &first_transfer_tx,
                                    &mut tasks,
                                ),
                            }
                        }
                        spawned_count = desired;
                    }

                    let time_remaining_ms =
                        (config.max_seconds * 1000).saturating_sub(elapsed_ms);
                    let conns = active_connections.load(Ordering::Relaxed).max(1);
                    let size = calc.suggested_request_size(conns, time_remaining_ms);
                    suggested_size.store(size, Ordering::Relaxed);

                    if let Some(interval) = config.progress_interval {
                        let should_report = last_progress_at
                            .map_or(true, |t| now.duration_since(t) >= interval);
                        if should_report {
                            last_progress_at = Some(now);
                            on_progress(UploadProgress {
                                elapsed,
                                bytes: local_bytes,
                                mbps: blended_bps * 8.0 / 1_000_000.0,
                                active_connections: active_connections.load(Ordering::Relaxed),
                            });
                        }
                    }
                }
            }
        }
    }

    let drained = timeout(STAGE_JOIN_GRACE_PERIOD, async {
        while tasks.join_next().await.is_some() {}
    })
    .await;
    if drained.is_err() {
        tasks.abort_all();
        let _ = timeout(STAGE_JOIN_GRACE_PERIOD, async {
            while tasks.join_next().await.is_some() {}
        })
        .await;
    }

    if let Some(rx) = upload_stats_rx.as_mut() {
        while let Ok(sample) = rx.try_recv() {
            remote_samples.push(sample);
        }
    }

    remote_samples.sort_by_key(|sample| (sample.elapsed_ms, sample.index.unwrap_or(u64::MAX)));
    remote_samples.dedup_by(|left, right| {
        left.bytes == right.bytes
            && left.elapsed_ms == right.elapsed_ms
            && left.index == right.index
    });

    if let Some(mut handle) = upload_stats_task.take() {
        let await_budget = Duration::from_millis(REMOTE_STATS_JOIN_WAIT_MS);
        match timeout(await_budget, &mut handle).await {
            Ok(Ok(Ok(()))) => {
                debug!(
                    server_id = selected_server.id,
                    "upload remote stats stream completed"
                );
            }
            Ok(Ok(Err(error))) => {
                debug!(
                    server_id = selected_server.id,
                    error = %error,
                    "upload remote stats stream failed"
                );
            }
            Ok(Err(error)) => {
                debug!(
                    server_id = selected_server.id,
                    error = %error,
                    "upload remote stats stream task failed"
                );
            }
            Err(_) => {
                debug!(
                    server_id = selected_server.id,
                    timeout_ms = await_budget.as_millis(),
                    "upload remote stats stream did not finish in time"
                );
                handle.abort();
            }
        }
    }

    if let Some(rx) = upload_stats_rx.as_mut() {
        while let Ok(sample) = rx.try_recv() {
            remote_samples.push(sample);
        }
    }

    if let Some(stop_flag) = remote_stats_background_stop.take() {
        stop_flag.store(true, Ordering::Relaxed);
    }
    if let Some(mut background_task) = remote_stats_background_task.take() {
        let _ = timeout(Duration::from_millis(900), &mut background_task).await;
    }

    let throughput_result = calc.finish();
    let actual_duration_ms = progress_clock_start
        .map(|start| Instant::now().saturating_duration_since(start).as_millis() as u64)
        .unwrap_or(0);

    let min_remote_elapsed_ms = config.max_seconds.saturating_mul(800);
    let remote_final_sample = remote_samples
        .iter()
        .copied()
        .max_by_key(|sample| sample.elapsed_ms)
        .filter(|sample| sample.elapsed_ms >= min_remote_elapsed_ms);

    let local_bytes = total_bytes.load(Ordering::Relaxed);
    let local_mbps = throughput_result.blended_mbps();

    let (bytes, mbps) = if let Some(sample) = remote_final_sample {
        let elapsed_secs = (sample.elapsed_ms.max(1) as f64) / 1_000.0;
        let mbps = (sample.bytes as f64 * 8.0) / 1_000_000.0 / elapsed_secs;
        (sample.bytes, mbps)
    } else {
        (local_bytes, local_mbps)
    };

    debug!(
        server_id = selected_server.id,
        local_bytes,
        local_mbps,
        remote_samples = remote_samples.len(),
        remote_final_elapsed_ms = ?remote_final_sample.map(|sample| sample.elapsed_ms),
        remote_final_bytes = ?remote_final_sample.map(|sample| sample.bytes),
        effective_bytes = bytes,
        effective_mbps = mbps,
        upload_measurement_method = if remote_final_sample.is_some() { "remote" } else { "local" },
        "upload stage finalized"
    );

    Ok(UploadStats {
        bytes,
        mbps,
        actual_duration_ms,
        throughput: Some(throughput_result),
        request_attempts: request_attempts.load(Ordering::Relaxed),
        request_successes: request_successes.load(Ordering::Relaxed),
        request_http_errors: request_http_errors.load(Ordering::Relaxed),
        request_transport_errors: request_transport_errors.load(Ordering::Relaxed),
        response_read_errors: response_read_errors.load(Ordering::Relaxed),
        remote_samples,
    })
}

async fn resolve_ready_upload_pool(
    client: &Client,
    mode: TransportProtocol,
    server_pool: &[SpeedtestServer],
    default_guid: &str,
) -> Vec<SpeedtestServer> {
    let mut tasks = JoinSet::new();
    for server in server_pool.iter().cloned() {
        let probe_client = client.clone();
        let guid = server
            .session_guid
            .clone()
            .unwrap_or_else(|| default_guid.to_string());
        tasks.spawn(async move {
            let is_ready = match mode {
                TransportProtocol::Tcp => {
                    match timeout(SERVER_READINESS_TIMEOUT, modern_protocol::connect(&server)).await
                    {
                        Ok(Ok(mut stream)) => {
                            let _ = modern_protocol::quit(&mut stream).await;
                            true
                        }
                        _ => false,
                    }
                }
                TransportProtocol::Xhr => {
                    let probe_payload = vec![0x52_u8; UPLOAD_READINESS_BYTES];
                    matches!(
                        timeout(
                            SERVER_READINESS_TIMEOUT,
                            browser_protocol::upload(
                                &probe_client,
                                &server,
                                &guid,
                                &probe_payload,
                            ),
                        )
                        .await,
                        Ok(Ok(_))
                    )
                }
            };

            (server.id, is_ready)
        });
    }

    let mut ready_ids = HashSet::new();
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

#[allow(clippy::too_many_arguments)]
fn run_upload_workers_modern(
    server_pool: &[SpeedtestServer],
    worker_count: usize,
    total_bytes: &Arc<AtomicU64>,
    request_attempts: &Arc<AtomicU64>,
    request_successes: &Arc<AtomicU64>,
    request_transport_errors: &Arc<AtomicU64>,
    response_read_errors: &Arc<AtomicU64>,
    active_connections: &Arc<AtomicUsize>,
    target_workers: &Arc<AtomicUsize>,
    suggested_size: &Arc<AtomicUsize>,
    stop_rx: &watch::Receiver<bool>,
    first_transfer_tx: &watch::Sender<bool>,
    tasks: &mut JoinSet<()>,
) {
    if server_pool.is_empty() {
        return;
    }

    for worker in 0..worker_count {
        spawn_upload_worker_tcp(
            worker,
            server_pool,
            total_bytes,
            request_attempts,
            request_successes,
            request_transport_errors,
            response_read_errors,
            active_connections,
            target_workers,
            suggested_size,
            stop_rx,
            first_transfer_tx,
            tasks,
        );
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_upload_worker_tcp(
    worker_index: usize,
    server_pool: &[SpeedtestServer],
    total_bytes: &Arc<AtomicU64>,
    request_attempts: &Arc<AtomicU64>,
    request_successes: &Arc<AtomicU64>,
    request_transport_errors: &Arc<AtomicU64>,
    response_read_errors: &Arc<AtomicU64>,
    active_connections: &Arc<AtomicUsize>,
    target_workers: &Arc<AtomicUsize>,
    suggested_size: &Arc<AtomicUsize>,
    stop_rx: &watch::Receiver<bool>,
    first_transfer_tx: &watch::Sender<bool>,
    tasks: &mut JoinSet<()>,
) {
    let worker_server = server_pool[worker_index % server_pool.len()].clone();
    let worker_bytes = Arc::clone(total_bytes);
    let worker_attempts = Arc::clone(request_attempts);
    let worker_successes = Arc::clone(request_successes);
    let worker_transport_errors = Arc::clone(request_transport_errors);
    let worker_read_errors = Arc::clone(response_read_errors);
    let worker_active_connections = Arc::clone(active_connections);
    let worker_target = Arc::clone(target_workers);
    let worker_suggested_size = Arc::clone(suggested_size);
    let mut worker_stop = stop_rx.clone();
    let worker_first_transfer_tx = first_transfer_tx.clone();

    tasks.spawn(async move {
        let mut stream = None;
        let mut stream_connected = false;
        let mut signaled_first_transfer = false;

        while !*worker_stop.borrow_and_update() {
            if worker_index >= worker_target.load(Ordering::Relaxed) {
                break;
            }

            if stream.is_none() {
                let maybe_connected = tokio::select! {
                    connected = modern_protocol::connect(&worker_server) => Some(connected),
                    _ = worker_stop.changed() => None,
                };

                match maybe_connected {
                    Some(Ok(connected)) => {
                        stream = Some(connected);
                        if !stream_connected {
                            worker_active_connections.fetch_add(1, Ordering::Relaxed);
                            stream_connected = true;
                        }
                    }
                    Some(Err(_)) => {
                        worker_transport_errors.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    None => {
                        break;
                    }
                }
            }

            worker_attempts.fetch_add(1, Ordering::Relaxed);

            let Some(active_stream) = stream.as_mut() else {
                continue;
            };

            let request_size = worker_suggested_size.load(Ordering::Relaxed);
            let maybe_uploaded = tokio::select! {
                uploaded = modern_protocol::upload(active_stream, request_size) => Some(uploaded),
                _ = worker_stop.changed() => None,
            };

            match maybe_uploaded {
                Some(Ok(uploaded)) => {
                    worker_successes.fetch_add(1, Ordering::Relaxed);
                    worker_bytes.fetch_add(uploaded, Ordering::Relaxed);
                    if uploaded > 0 && !signaled_first_transfer {
                        let _ = worker_first_transfer_tx.send(true);
                        signaled_first_transfer = true;
                    }
                }
                Some(Err(_)) => {
                    worker_read_errors.fetch_add(1, Ordering::Relaxed);
                    stream = None;
                    if stream_connected {
                        worker_active_connections.fetch_sub(1, Ordering::Relaxed);
                        stream_connected = false;
                    }
                }
                None => break,
            }
        }

        if let Some(mut active_stream) = stream {
            let _ = modern_protocol::quit(&mut active_stream).await;
        }

        if stream_connected {
            worker_active_connections.fetch_sub(1, Ordering::Relaxed);
        }
    });
}

#[allow(clippy::too_many_arguments)]
fn run_upload_workers_modern_sdk(
    client: &Client,
    server_pool: &[SpeedtestServer],
    default_guid: &str,
    worker_count: usize,
    total_bytes: &Arc<AtomicU64>,
    request_attempts: &Arc<AtomicU64>,
    request_successes: &Arc<AtomicU64>,
    request_http_errors: &Arc<AtomicU64>,
    request_transport_errors: &Arc<AtomicU64>,
    response_read_errors: &Arc<AtomicU64>,
    active_connections: &Arc<AtomicUsize>,
    target_workers: &Arc<AtomicUsize>,
    suggested_size: &Arc<AtomicUsize>,
    stop_rx: &watch::Receiver<bool>,
    first_transfer_tx: &watch::Sender<bool>,
    tasks: &mut JoinSet<()>,
) {
    if server_pool.is_empty() {
        return;
    }

    for worker in 0..worker_count {
        spawn_upload_worker_xhr(
            worker,
            client,
            server_pool,
            default_guid,
            total_bytes,
            request_attempts,
            request_successes,
            request_http_errors,
            request_transport_errors,
            response_read_errors,
            active_connections,
            target_workers,
            suggested_size,
            stop_rx,
            first_transfer_tx,
            tasks,
        );
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_upload_worker_xhr(
    worker_index: usize,
    client: &Client,
    server_pool: &[SpeedtestServer],
    default_guid: &str,
    total_bytes: &Arc<AtomicU64>,
    request_attempts: &Arc<AtomicU64>,
    request_successes: &Arc<AtomicU64>,
    request_http_errors: &Arc<AtomicU64>,
    request_transport_errors: &Arc<AtomicU64>,
    response_read_errors: &Arc<AtomicU64>,
    active_connections: &Arc<AtomicUsize>,
    target_workers: &Arc<AtomicUsize>,
    suggested_size: &Arc<AtomicUsize>,
    stop_rx: &watch::Receiver<bool>,
    first_transfer_tx: &watch::Sender<bool>,
    tasks: &mut JoinSet<()>,
) {
    const MAX_UPLOAD_BUF: usize = 25 * 1024 * 1024;
    let worker_client = client.clone();
    let worker_server = server_pool[worker_index % server_pool.len()].clone();
    let worker_default_guid = default_guid.to_string();
    let worker_guid = worker_server
        .session_guid
        .clone()
        .unwrap_or(worker_default_guid);
    let worker_bytes = Arc::clone(total_bytes);
    let worker_attempts = Arc::clone(request_attempts);
    let worker_successes = Arc::clone(request_successes);
    let worker_http_errors = Arc::clone(request_http_errors);
    let worker_transport_errors = Arc::clone(request_transport_errors);
    let worker_read_errors = Arc::clone(response_read_errors);
    let worker_active_connections = Arc::clone(active_connections);
    let worker_target = Arc::clone(target_workers);
    let worker_suggested_size = Arc::clone(suggested_size);
    let mut worker_stop = stop_rx.clone();
    let worker_first_transfer_tx = first_transfer_tx.clone();

    tasks.spawn(async move {
        let upload_buf = vec![0x42_u8; MAX_UPLOAD_BUF];
        let mut signaled_first_transfer = false;

        while !*worker_stop.borrow_and_update() {
            if worker_index >= worker_target.load(Ordering::Relaxed) {
                break;
            }

            let size = worker_suggested_size
                .load(Ordering::Relaxed)
                .min(MAX_UPLOAD_BUF);
            worker_attempts.fetch_add(1, Ordering::Relaxed);

            let _active_guard = ActiveConnectionGuard::new(&worker_active_connections);

            let maybe_uploaded = tokio::select! {
                uploaded = browser_protocol::upload(
                    &worker_client,
                    &worker_server,
                    &worker_guid,
                    &upload_buf[..size],
                ) => Some(uploaded),
                _ = worker_stop.changed() => None,
            };

            match maybe_uploaded {
                Some(Ok(uploaded)) => {
                    worker_successes.fetch_add(1, Ordering::Relaxed);
                    worker_bytes.fetch_add(uploaded, Ordering::Relaxed);
                    if uploaded > 0 && !signaled_first_transfer {
                        let _ = worker_first_transfer_tx.send(true);
                        signaled_first_transfer = true;
                    }
                }
                Some(Err(error)) => match error {
                    browser_protocol::TransferRequestError::HttpStatus => {
                        worker_http_errors.fetch_add(1, Ordering::Relaxed);
                    }
                    browser_protocol::TransferRequestError::Transport
                    | browser_protocol::TransferRequestError::InvalidEndpoint => {
                        worker_transport_errors.fetch_add(1, Ordering::Relaxed);
                    }
                    browser_protocol::TransferRequestError::ResponseRead => {
                        worker_read_errors.fetch_add(1, Ordering::Relaxed);
                    }
                },
                None => break,
            }
        }
    });
}

fn remote_sample_rate(
    sample: browser_protocol::UploadStatsSample,
    elapsed: Duration,
) -> Option<(u64, f64)> {
    if sample.elapsed_ms == 0 {
        return None;
    }

    let elapsed_wall_ms = elapsed.as_millis() as u64;
    if sample.elapsed_ms.saturating_add(750) < elapsed_wall_ms {
        return None;
    }

    let elapsed_secs = (sample.elapsed_ms as f64) / 1_000.0;
    if elapsed_secs <= 0.0 || !elapsed_secs.is_finite() {
        return None;
    }

    let mbps = (sample.bytes as f64 * 8.0) / 1_000_000.0 / elapsed_secs;
    if !mbps.is_finite() || mbps < 0.0 {
        return None;
    }

    Some((sample.bytes, mbps))
}

async fn run_remote_stats_background_upload(
    client: &Client,
    server: &SpeedtestServer,
    guid: &str,
    stop_flag: Arc<AtomicBool>,
    total_bytes: &Arc<AtomicU64>,
) {
    let payload = vec![0x57_u8; REMOTE_STATS_BACKGROUND_UPLOAD_BYTES];

    while !stop_flag.load(Ordering::Relaxed) {
        if let Ok(uploaded) = browser_protocol::upload(client, server, guid, &payload).await {
            total_bytes.fetch_add(uploaded, Ordering::Relaxed);
        }
        sleep(Duration::from_millis(
            REMOTE_STATS_BACKGROUND_UPLOAD_INTERVAL_MS,
        ))
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::net::Ipv4Addr;

    use anyhow::Context;
    use tokio::io::AsyncReadExt;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::watch;
    use tokio::time::{Duration, Instant, sleep, timeout};

    fn upload_test_server(port: u16) -> SpeedtestServer {
        SpeedtestServer {
            id: 17,
            sponsor: "unit-test".to_string(),
            name: "local".to_string(),
            country: "local".to_string(),
            host: format!("127.0.0.1:{port}"),
            distance_km: 1.0,
            url: format!("http://127.0.0.1:{port}/").to_string(),
            session_guid: None,
            sdk_lat: None,
            sdk_lon: None,
            sdk_cc: None,
            sdk_preferred: None,
            sdk_isp_id: None,
            sdk_https_functional: None,
            sdk_hostname: None,
            sdk_port: None,
            sdk_force_ping_select: None,
        }
    }

    async fn read_line(stream: &mut TcpStream) -> anyhow::Result<String> {
        let mut line = String::new();
        let mut buf = [0_u8; 1];
        loop {
            let read = stream
                .read(&mut buf)
                .await
                .context("failed reading command")?;
            if read == 0 {
                anyhow::bail!("client closed before sending request line");
            }

            if buf[0] == b'\n' {
                break;
            }

            if buf[0] != b'\r' {
                line.push(buf[0] as char);
                if line.len() > 64 {
                    anyhow::bail!("command line exceeded expected limit");
                }
            }
        }

        Ok(line)
    }

    async fn handle_slow_upload(mut stream: TcpStream, mut stop: watch::Receiver<bool>) {
        let _ = read_line(&mut stream).await;
        let mut payload = [0_u8; 1024];

        loop {
            if *stop.borrow_and_update() {
                break;
            }

            if let Ok(read) = stream.read(&mut payload).await {
                if read == 0 {
                    break;
                }
            } else {
                break;
            }

            sleep(Duration::from_millis(2)).await;
        }
    }

    async fn spawn_slow_upload_server()
    -> anyhow::Result<(u16, watch::Sender<bool>, tokio::task::JoinHandle<()>)> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .context("bind upload server")?;
        let port = listener.local_addr().context("read server port")?.port();
        let (stop_tx, mut stop_rx) = watch::channel(false);

        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    changed = stop_rx.changed() => {
                        if changed.is_err() {
                            break;
                        }

                        if *stop_rx.borrow_and_update() {
                            break;
                        }
                    }
                    accepted = listener.accept() => {
                        let Ok((stream, _)) = accepted else {
                            continue;
                        };

                        let stop = stop_rx.clone();
                        tokio::spawn(async move {
                            handle_slow_upload(stream, stop).await;
                        });
                    }
                }
            }
        });

        Ok((port, stop_tx, handle))
    }

    #[tokio::test]
    async fn tcp_upload_stage_honors_stop_deadline() -> anyhow::Result<()> {
        let (port, stop_tx, server_handle) = spawn_slow_upload_server()
            .await
            .context("start slow upload test server")?;

        let server = upload_test_server(port);
        let client = reqwest::Client::new();
        let start = Instant::now();

        let test_config = crate::speedtest::throughput::TransferConfig {
            connections: 1,
            max_seconds: 1,
            progress_interval: None,
        };
        let result = timeout(
            Duration::from_secs(3),
            run_upload_test(
                &client,
                &server,
                TransportProtocol::Tcp,
                std::slice::from_ref(&server),
                &test_config,
                |_| {},
            ),
        )
        .await
        .context("upload run must finish inside timeout")?;

        let stats = result.context("upload stage must succeed")?;

        stop_tx.send_replace(true);
        let _ = timeout(Duration::from_millis(300), server_handle).await;

        assert!(
            start.elapsed() < Duration::from_millis(1800),
            "upload stage should stop near target duration"
        );
        assert_eq!(stats.bytes, 0);

        Ok(())
    }
}
