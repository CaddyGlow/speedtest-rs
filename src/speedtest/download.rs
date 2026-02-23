use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use reqwest::Client;
use tokio::task::JoinSet;
use tokio::time::sleep;

use crate::speedtest::api::ResolvedSpeedtestApi;
use crate::speedtest::browser_protocol;
use crate::speedtest::modern_protocol;
use crate::speedtest::servers::SpeedtestServer;
use crate::util::{clamp_worker_count, mbps_from_bytes};

#[derive(Debug, Clone)]
pub struct DownloadStats {
    pub bytes: u64,
    pub mbps: f64,
    pub request_attempts: u64,
    pub request_successes: u64,
    pub request_http_errors: u64,
    pub request_transport_errors: u64,
    pub response_read_errors: u64,
    pub per_server: Vec<PerServerDownloadStats>,
}

#[derive(Debug, Clone)]
pub struct PerServerDownloadStats {
    pub server_id: u64,
    pub bytes: u64,
    pub mbps: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct DownloadProgress {
    pub elapsed: Duration,
    pub bytes: u64,
    pub mbps: f64,
    pub active_connections: usize,
}

#[allow(clippy::too_many_arguments)]
pub async fn run_download_test<F>(
    client: &Client,
    selected_server: &SpeedtestServer,
    api: ResolvedSpeedtestApi,
    server_pool: &[SpeedtestServer],
    connections: usize,
    seconds: u64,
    progress_interval: Option<Duration>,
    mut on_progress: F,
) -> Result<DownloadStats>
where
    F: FnMut(DownloadProgress),
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
    let per_server_bytes = Arc::new(
        transfer_pool
            .iter()
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>(),
    );

    let workers = async {
        match api {
            ResolvedSpeedtestApi::Legacy => {
                run_download_workers_legacy(
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
                    &per_server_bytes,
                )
                .await;
            }
            ResolvedSpeedtestApi::Modern => {
                run_download_workers_modern_sdk(
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
                    &per_server_bytes,
                )
                .await;
            }
            ResolvedSpeedtestApi::ModernTcp => {
                run_download_workers_modern(
                    &transfer_pool,
                    worker_count,
                    stop_at,
                    &total_bytes,
                    &request_attempts,
                    &request_successes,
                    &request_transport_errors,
                    &response_read_errors,
                    &active_connections,
                    &per_server_bytes,
                )
                .await;
            }
        }
    };
    tokio::pin!(workers);

    if let Some(interval) = progress_interval {
        loop {
            tokio::select! {
                _ = &mut workers => break,
                _ = sleep(interval) => {
                    let elapsed = start_at.elapsed();
                    let elapsed_secs = elapsed.as_secs().max(1);
                    let bytes = total_bytes.load(Ordering::Relaxed);
                    on_progress(DownloadProgress {
                        elapsed,
                        bytes,
                        mbps: mbps_from_bytes(bytes, elapsed_secs),
                        active_connections: active_connections.load(Ordering::Relaxed),
                    });
                }
            }
        }
    } else {
        workers.await;
    }

    let bytes = total_bytes.load(Ordering::Relaxed);
    let per_server = transfer_pool
        .iter()
        .enumerate()
        .map(|(index, server)| {
            let bytes = per_server_bytes[index].load(Ordering::Relaxed);
            PerServerDownloadStats {
                server_id: server.id,
                bytes,
                mbps: mbps_from_bytes(bytes, seconds),
            }
        })
        .collect();

    Ok(DownloadStats {
        bytes,
        mbps: mbps_from_bytes(bytes, seconds),
        request_attempts: request_attempts.load(Ordering::Relaxed),
        request_successes: request_successes.load(Ordering::Relaxed),
        request_http_errors: request_http_errors.load(Ordering::Relaxed),
        request_transport_errors: request_transport_errors.load(Ordering::Relaxed),
        response_read_errors: response_read_errors.load(Ordering::Relaxed),
        per_server,
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
async fn run_download_workers_legacy(
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
    per_server_bytes: &Arc<Vec<AtomicU64>>,
) {
    let mut tasks = JoinSet::new();
    for worker in 0..worker_count {
        let worker_client = client.clone();
        let worker_server = server.clone();
        let worker_bytes = Arc::clone(total_bytes);
        let worker_attempts = Arc::clone(request_attempts);
        let worker_successes = Arc::clone(request_successes);
        let worker_http_errors = Arc::clone(request_http_errors);
        let worker_transport_errors = Arc::clone(request_transport_errors);
        let worker_read_errors = Arc::clone(response_read_errors);
        let worker_active_connections = Arc::clone(active_connections);
        let worker_per_server_bytes = Arc::clone(per_server_bytes);
        tasks.spawn(async move {
            const SIZES: [usize; 8] = [500, 750, 1000, 1500, 2000, 2500, 3000, 4000];
            let mut cursor = worker % SIZES.len();

            while Instant::now() < stop_at {
                let size = SIZES[cursor];
                cursor = (cursor + 1) % SIZES.len();

                let Ok(url) = worker_server.download_url(size) else {
                    break;
                };

                worker_attempts.fetch_add(1, Ordering::Relaxed);

                let _active_guard = ActiveConnectionGuard::new(&worker_active_connections);

                let response = match worker_client.get(url).send().await {
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
                    Ok(body) => {
                        worker_successes.fetch_add(1, Ordering::Relaxed);
                        worker_bytes.fetch_add(body.len() as u64, Ordering::Relaxed);
                        worker_per_server_bytes[0].fetch_add(body.len() as u64, Ordering::Relaxed);
                    }
                    Err(_) => {
                        worker_read_errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });
    }

    while tasks.join_next().await.is_some() {}
}

#[allow(clippy::too_many_arguments)]
async fn run_download_workers_modern(
    server_pool: &[SpeedtestServer],
    worker_count: usize,
    stop_at: Instant,
    total_bytes: &Arc<AtomicU64>,
    request_attempts: &Arc<AtomicU64>,
    request_successes: &Arc<AtomicU64>,
    request_transport_errors: &Arc<AtomicU64>,
    response_read_errors: &Arc<AtomicU64>,
    active_connections: &Arc<AtomicUsize>,
    per_server_bytes: &Arc<Vec<AtomicU64>>,
) {
    if server_pool.is_empty() {
        return;
    }

    let mut tasks = JoinSet::new();
    for worker in 0..worker_count {
        let server_index = worker % server_pool.len();
        let worker_server = server_pool[server_index].clone();
        let worker_bytes = Arc::clone(total_bytes);
        let worker_attempts = Arc::clone(request_attempts);
        let worker_successes = Arc::clone(request_successes);
        let worker_transport_errors = Arc::clone(request_transport_errors);
        let worker_read_errors = Arc::clone(response_read_errors);
        let worker_active_connections = Arc::clone(active_connections);
        let worker_per_server_bytes = Arc::clone(per_server_bytes);
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

                match modern_protocol::download(active_stream, REQUEST_SIZE).await {
                    Ok(downloaded) => {
                        worker_successes.fetch_add(1, Ordering::Relaxed);
                        worker_bytes.fetch_add(downloaded, Ordering::Relaxed);
                        worker_per_server_bytes[server_index]
                            .fetch_add(downloaded, Ordering::Relaxed);
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
async fn run_download_workers_modern_sdk(
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
    per_server_bytes: &Arc<Vec<AtomicU64>>,
) {
    if server_pool.is_empty() {
        return;
    }

    let mut tasks = JoinSet::new();

    for worker in 0..worker_count {
        let worker_client = client.clone();
        let server_index = worker % server_pool.len();
        let worker_server = server_pool[server_index].clone();
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
        let worker_per_server_bytes = Arc::clone(per_server_bytes);
        tasks.spawn(async move {
            const REQUEST_SIZE: usize = 25_000_000;

            while Instant::now() < stop_at {
                worker_attempts.fetch_add(1, Ordering::Relaxed);

                let _active_guard = ActiveConnectionGuard::new(&worker_active_connections);

                match browser_protocol::download(
                    &worker_client,
                    &worker_server,
                    &worker_guid,
                    REQUEST_SIZE,
                )
                .await
                {
                    Ok(downloaded) => {
                        worker_successes.fetch_add(1, Ordering::Relaxed);
                        worker_bytes.fetch_add(downloaded, Ordering::Relaxed);
                        worker_per_server_bytes[server_index]
                            .fetch_add(downloaded, Ordering::Relaxed);
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
