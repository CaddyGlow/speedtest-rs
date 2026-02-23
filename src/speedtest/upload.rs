use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use reqwest::Client;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout};
use tracing::debug;

use crate::speedtest::api::TransportProtocol;
use crate::speedtest::browser_protocol;
use crate::speedtest::modern_protocol;
use crate::speedtest::servers::SpeedtestServer;
use crate::util::{clamp_worker_count, mbps_from_bytes};

const REMOTE_STATS_SAMPLE_INTERVAL_MS: u64 = 250;
const REMOTE_STATS_JOIN_WAIT_MS: u64 = 3_500;
const REMOTE_STATS_BACKGROUND_UPLOAD_BYTES: usize = 32 * 1024;
const REMOTE_STATS_BACKGROUND_UPLOAD_INTERVAL_MS: u64 = 500;

#[derive(Debug, Clone)]
pub struct UploadStats {
    pub bytes: u64,
    pub mbps: f64,
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

#[allow(clippy::too_many_arguments)]
pub async fn run_upload_test<F>(
    client: &Client,
    selected_server: &SpeedtestServer,
    mode: TransportProtocol,
    server_pool: &[SpeedtestServer],
    connections: usize,
    seconds: u64,
    progress_interval: Option<Duration>,
    mut on_progress: F,
) -> Result<UploadStats>
where
    F: FnMut(UploadProgress),
{
    let worker_count = clamp_worker_count(connections);
    let start_at = Instant::now();
    let stop_at = start_at + Duration::from_secs(seconds);
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
    let mut upload_stats_task = None;
    let mut remote_stats_background_task = None;
    let mut remote_stats_background_stop = None;
    let mut upload_stats_rx = None;
    let mut latest_remote_sample = None;
    let mut remote_samples = Vec::new();

    if matches!(mode, TransportProtocol::Xhr)
        && let Some(guid) = selected_server
            .session_guid
            .clone()
            .or_else(|| Some(default_guid.clone()))
    {
        debug!(
            server_id = selected_server.id,
            guid = %guid,
            duration_seconds = seconds,
            sample_interval_ms = REMOTE_STATS_SAMPLE_INTERVAL_MS,
            "upload remote stats stream enabled"
        );
        let (tx, rx) = mpsc::unbounded_channel();
        let stats_server = selected_server.clone();
        let stats_guid = guid.clone();
        let ws_sender = tx.clone();
        upload_stats_task = Some(tokio::spawn(async move {
            browser_protocol::stream_upload_stats_samples(
                &stats_server,
                &stats_guid,
                seconds,
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

    let workers = async {
        match mode {
            TransportProtocol::Xhr => {
                run_upload_workers_modern_sdk(
                    client,
                    &transfer_pool,
                    &default_guid,
                    worker_count,
                    stop_at,
                    &total_bytes,
                    &request_attempts,
                    &request_successes,
                    &request_http_errors,
                    &request_transport_errors,
                    &response_read_errors,
                    &active_connections,
                )
                .await;
            }
            TransportProtocol::Tcp => {
                run_upload_workers_modern(
                    &transfer_pool,
                    worker_count,
                    stop_at,
                    &total_bytes,
                    &request_attempts,
                    &request_successes,
                    &request_transport_errors,
                    &response_read_errors,
                    &active_connections,
                )
                .await;
            }
        }
        Ok::<(), anyhow::Error>(())
    };
    tokio::pin!(workers);

    if let Some(interval) = progress_interval {
        loop {
            tokio::select! {
                result = &mut workers => {
                    result?;
                    break;
                }
                _ = sleep(interval) => {
                    if let Some(rx) = upload_stats_rx.as_mut() {
                        while let Ok(sample) = rx.try_recv() {
                            latest_remote_sample = Some(sample);
                            remote_samples.push(sample);
                        }
                    }

                    let elapsed = start_at.elapsed();
                    let elapsed_secs = elapsed.as_secs().max(1);
                    let local_bytes = total_bytes.load(Ordering::Relaxed);
                    let local_mbps = mbps_from_bytes(local_bytes, elapsed_secs);

                    if let Some(sample) = latest_remote_sample {
                        let _ = remote_sample_rate(sample, elapsed);
                    }

                    on_progress(UploadProgress {
                        elapsed,
                        bytes: local_bytes,
                        mbps: local_mbps,
                        active_connections: active_connections.load(Ordering::Relaxed),
                    });
                }
            }
        }
    } else {
        workers.await?;
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

    let min_remote_elapsed_ms = seconds.saturating_mul(800);
    let remote_final_sample = remote_samples
        .iter()
        .copied()
        .max_by_key(|sample| sample.elapsed_ms)
        .filter(|sample| sample.elapsed_ms >= min_remote_elapsed_ms);

    let local_bytes = total_bytes.load(Ordering::Relaxed);
    let local_mbps = mbps_from_bytes(local_bytes, seconds);

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
        request_attempts: request_attempts.load(Ordering::Relaxed),
        request_successes: request_successes.load(Ordering::Relaxed),
        request_http_errors: request_http_errors.load(Ordering::Relaxed),
        request_transport_errors: request_transport_errors.load(Ordering::Relaxed),
        response_read_errors: response_read_errors.load(Ordering::Relaxed),
        remote_samples,
    })
}

fn normalize_server_pool(
    selected_server: &SpeedtestServer,
    server_pool: &[SpeedtestServer],
) -> Vec<SpeedtestServer> {
    if server_pool.is_empty() {
        vec![selected_server.clone()]
    } else {
        server_pool.to_vec()
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_upload_workers_modern(
    server_pool: &[SpeedtestServer],
    worker_count: usize,
    stop_at: Instant,
    total_bytes: &Arc<AtomicU64>,
    request_attempts: &Arc<AtomicU64>,
    request_successes: &Arc<AtomicU64>,
    request_transport_errors: &Arc<AtomicU64>,
    response_read_errors: &Arc<AtomicU64>,
    active_connections: &Arc<AtomicUsize>,
) {
    if server_pool.is_empty() {
        return;
    }

    let mut tasks = JoinSet::new();
    for worker in 0..worker_count {
        let worker_server = server_pool[worker % server_pool.len()].clone();
        let worker_bytes = Arc::clone(total_bytes);
        let worker_attempts = Arc::clone(request_attempts);
        let worker_successes = Arc::clone(request_successes);
        let worker_transport_errors = Arc::clone(request_transport_errors);
        let worker_read_errors = Arc::clone(response_read_errors);
        let worker_active_connections = Arc::clone(active_connections);

        tasks.spawn(async move {
            const REQUEST_SIZE: usize = 25_000_000;
            let mut stream = None;

            while Instant::now() < stop_at {
                if stream.is_none() {
                    match modern_protocol::connect(&worker_server).await {
                        Ok(connected) => {
                            stream = Some(connected);
                        }
                        Err(_) => {
                            worker_transport_errors.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                    }
                }

                worker_attempts.fetch_add(1, Ordering::Relaxed);

                let Some(active_stream) = stream.as_mut() else {
                    continue;
                };

                let _active_guard = ActiveConnectionGuard::new(&worker_active_connections);

                match modern_protocol::upload(active_stream, REQUEST_SIZE).await {
                    Ok(uploaded) => {
                        worker_successes.fetch_add(1, Ordering::Relaxed);
                        worker_bytes.fetch_add(uploaded, Ordering::Relaxed);
                    }
                    Err(_) => {
                        worker_read_errors.fetch_add(1, Ordering::Relaxed);
                        stream = None;
                    }
                }
            }

            if let Some(mut active_stream) = stream {
                let _ = modern_protocol::quit(&mut active_stream).await;
            }
        });
    }

    while tasks.join_next().await.is_some() {}
}

#[allow(clippy::too_many_arguments)]
async fn run_upload_workers_modern_sdk(
    client: &Client,
    server_pool: &[SpeedtestServer],
    default_guid: &str,
    worker_count: usize,
    stop_at: Instant,
    total_bytes: &Arc<AtomicU64>,
    request_attempts: &Arc<AtomicU64>,
    request_successes: &Arc<AtomicU64>,
    request_http_errors: &Arc<AtomicU64>,
    request_transport_errors: &Arc<AtomicU64>,
    response_read_errors: &Arc<AtomicU64>,
    active_connections: &Arc<AtomicUsize>,
) {
    if server_pool.is_empty() {
        return;
    }

    let mut tasks = JoinSet::new();

    for worker in 0..worker_count {
        let worker_client = client.clone();
        let worker_server = server_pool[worker % server_pool.len()].clone();
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

        tasks.spawn(async move {
            const SIZES: [usize; 5] = [
                256 * 1024,
                512 * 1024,
                1024 * 1024,
                2 * 1024 * 1024,
                4 * 1024 * 1024,
            ];
            let mut cursor = worker % SIZES.len();

            while Instant::now() < stop_at {
                let size = SIZES[cursor];
                cursor = (cursor + 1) % SIZES.len();
                worker_attempts.fetch_add(1, Ordering::Relaxed);

                let _active_guard = ActiveConnectionGuard::new(&worker_active_connections);

                match browser_protocol::upload(
                    &worker_client,
                    &worker_server,
                    &worker_guid,
                    vec![0x42_u8; size],
                )
                .await
                {
                    Ok(uploaded) => {
                        worker_successes.fetch_add(1, Ordering::Relaxed);
                        worker_bytes.fetch_add(uploaded, Ordering::Relaxed);
                    }
                    Err(error) => match error {
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
                }
            }
        });
    }

    while tasks.join_next().await.is_some() {}
}

struct ActiveConnectionGuard<'a> {
    counter: &'a AtomicUsize,
}

impl<'a> ActiveConnectionGuard<'a> {
    fn new(counter: &'a AtomicUsize) -> Self {
        counter.fetch_add(1, Ordering::Relaxed);
        Self { counter }
    }
}

impl Drop for ActiveConnectionGuard<'_> {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
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
        if let Ok(uploaded) = browser_protocol::upload(client, server, guid, payload.clone()).await
        {
            total_bytes.fetch_add(uploaded, Ordering::Relaxed);
        }
        sleep(Duration::from_millis(
            REMOTE_STATS_BACKGROUND_UPLOAD_INTERVAL_MS,
        ))
        .await;
    }
}
