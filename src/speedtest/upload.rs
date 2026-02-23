use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use reqwest::Client;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::sleep;

use crate::speedtest::api::ResolvedSpeedtestApi;
use crate::speedtest::browser_protocol;
use crate::speedtest::modern_protocol;
use crate::speedtest::servers::SpeedtestServer;
use crate::util::{clamp_worker_count, mbps_from_bytes};

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
    api: ResolvedSpeedtestApi,
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
    let mut upload_stats_task = None;
    let mut upload_stats_rx = None;
    let mut latest_remote_sample = None;
    let mut remote_samples = Vec::new();

    if matches!(api, ResolvedSpeedtestApi::Modern)
        && let Some(guid) = selected_server.session_guid.clone()
    {
        let (tx, rx) = mpsc::unbounded_channel();
        let stats_server = selected_server.clone();
        upload_stats_task = Some(tokio::spawn(async move {
            browser_protocol::stream_upload_stats_samples(&stats_server, &guid, seconds, 50, tx)
                .await
        }));
        upload_stats_rx = Some(rx);
    }

    let workers = async {
        match api {
            ResolvedSpeedtestApi::Legacy => {
                run_upload_workers_legacy(
                    client,
                    selected_server,
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
                .await?;
            }
            ResolvedSpeedtestApi::Modern => {
                run_upload_workers_modern_sdk(
                    client,
                    &transfer_pool,
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
            ResolvedSpeedtestApi::ModernTcp => {
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
                    let (bytes, mbps) = latest_remote_sample
                        .and_then(|sample| remote_sample_rate(sample, elapsed))
                        .unwrap_or((local_bytes, mbps_from_bytes(local_bytes, elapsed_secs)));

                    on_progress(UploadProgress {
                        elapsed,
                        bytes,
                        mbps,
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

    if let Some(handle) = upload_stats_task.take()
        && !handle.is_finished()
    {
        handle.abort();
    }

    let min_remote_elapsed_ms = seconds.saturating_mul(800);
    let remote_final_sample = remote_samples
        .iter()
        .copied()
        .max_by_key(|sample| sample.elapsed_ms)
        .filter(|sample| sample.elapsed_ms >= min_remote_elapsed_ms);

    let (bytes, mbps) = if let Some(sample) = remote_final_sample {
        let elapsed_secs = (sample.elapsed_ms.max(1) as f64) / 1_000.0;
        let mbps = (sample.bytes as f64 * 8.0) / 1_000_000.0 / elapsed_secs;
        (sample.bytes, mbps)
    } else {
        let bytes = total_bytes.load(Ordering::Relaxed);
        (bytes, mbps_from_bytes(bytes, seconds))
    };

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
async fn run_upload_workers_legacy(
    client: &Client,
    server: &SpeedtestServer,
    worker_count: usize,
    stop_at: Instant,
    total_bytes: &Arc<AtomicU64>,
    request_attempts: &Arc<AtomicU64>,
    request_successes: &Arc<AtomicU64>,
    request_http_errors: &Arc<AtomicU64>,
    request_transport_errors: &Arc<AtomicU64>,
    response_read_errors: &Arc<AtomicU64>,
    active_connections: &Arc<AtomicUsize>,
) -> Result<()> {
    let payload = Arc::new(vec![0x42_u8; 256 * 1024]);
    let upload_url = server.upload_url()?;
    let mut tasks = JoinSet::new();
    for _ in 0..worker_count {
        let worker_client = client.clone();
        let worker_upload_url = upload_url.clone();
        let worker_payload = Arc::clone(&payload);
        let worker_bytes = Arc::clone(total_bytes);
        let worker_attempts = Arc::clone(request_attempts);
        let worker_successes = Arc::clone(request_successes);
        let worker_http_errors = Arc::clone(request_http_errors);
        let worker_transport_errors = Arc::clone(request_transport_errors);
        let worker_read_errors = Arc::clone(response_read_errors);
        let worker_active_connections = Arc::clone(active_connections);

        tasks.spawn(async move {
            while Instant::now() < stop_at {
                worker_attempts.fetch_add(1, Ordering::Relaxed);
                let _active_guard = ActiveConnectionGuard::new(&worker_active_connections);
                let response = match worker_client
                    .post(&worker_upload_url)
                    .header("Content-Type", "application/octet-stream")
                    .body((*worker_payload).clone())
                    .send()
                    .await
                {
                    Ok(response) => match response.error_for_status() {
                        Ok(response) => response,
                        Err(_) => {
                            worker_http_errors.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                    },
                    Err(_) => {
                        worker_transport_errors.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                };

                match response.bytes().await {
                    Ok(_) => {
                        worker_successes.fetch_add(1, Ordering::Relaxed);
                        worker_bytes.fetch_add(worker_payload.len() as u64, Ordering::Relaxed);
                    }
                    Err(_) => {
                        worker_read_errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });
    }

    while tasks.join_next().await.is_some() {}
    Ok(())
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
        let worker_guid = worker_server
            .session_guid
            .clone()
            .unwrap_or_else(|| "tunmux-speedtest".to_string());
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
