use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use reqwest::Client;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::timeout;

use crate::speedtest::api::TransportProtocol;
use crate::speedtest::browser_protocol;
use crate::speedtest::modern_protocol;
use crate::speedtest::servers::SpeedtestServer;
use crate::speedtest::throughput::{ThroughputCalculator, ThroughputResult, TransferConfig};
use crate::speedtest::transfer_util::{
    ActiveConnectionGuard, TransferLoopState, TransferSample, drain_join_set, elapsed_ms_since,
    new_transfer_control, normalize_server_pool, resolve_ready_server_pool, run_transfer_loop,
    spawn_worker_range,
};
use crate::util::{clamp_worker_count, mbps_from_bytes};

const STAGE_POLL_INTERVAL: Duration = Duration::from_millis(50);
const STAGE_JOIN_GRACE_PERIOD: Duration = Duration::from_millis(250);
const SERVER_READINESS_TIMEOUT: Duration = Duration::from_secs(8);
const DOWNLOAD_READINESS_BYTES: usize = 64 * 1024;
#[derive(Debug, Clone)]
pub struct DownloadStats {
    pub bytes: u64,
    pub mbps: f64,
    pub actual_duration_ms: u64,
    pub throughput: Option<ThroughputResult>,
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

pub async fn run_download_test<F>(
    client: &Client,
    selected_server: &SpeedtestServer,
    mode: TransportProtocol,
    server_pool: &[SpeedtestServer],
    config: &TransferConfig,
    mut on_progress: F,
) -> Result<DownloadStats>
where
    F: FnMut(DownloadProgress),
{
    let worker_count = clamp_worker_count(config.initial_connections());
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
    let ready_pool = resolve_ready_download_pool(client, mode, &transfer_pool, &default_guid).await;
    if ready_pool.is_empty() {
        bail!("no speedtest servers became ready for download stage");
    }

    let start_at = Instant::now();
    let stage_end_at = start_at + Duration::from_secs(config.max_seconds);
    let per_server_bytes = Arc::new(
        ready_pool
            .iter()
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>(),
    );

    let control = new_transfer_control(worker_count, config.start_request_size);
    let stage_stop_rx = control.stage_stop_rx.clone();
    let first_transfer_tx = control.first_transfer_tx.clone();
    let mut first_transfer_rx = control.first_transfer_rx.clone();
    let first_byte_at = Arc::clone(&control.first_byte_at);
    let target_workers = Arc::clone(&control.target_workers);
    let suggested_size = Arc::clone(&control.suggested_size);

    let mut tasks = JoinSet::new();
    let mut loop_state = TransferLoopState {
        stage_end_at,
        progress_clock_start: None,
        transfer_started: false,
        last_progress_at: None,
        spawned_count: worker_count,
    };
    spawn_worker_range(0..worker_count, &mut tasks, |idx, tasks| match mode {
        TransportProtocol::Tcp => spawn_download_worker_tcp(
            idx,
            &ready_pool,
            &total_bytes,
            &request_attempts,
            &request_successes,
            &request_transport_errors,
            &response_read_errors,
            &active_connections,
            &per_server_bytes,
            &target_workers,
            &suggested_size,
            &first_byte_at,
            &stage_stop_rx,
            &first_transfer_tx,
            tasks,
        ),
        TransportProtocol::Xhr => spawn_download_worker_xhr(
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
            &per_server_bytes,
            &target_workers,
            &suggested_size,
            &first_byte_at,
            &stage_stop_rx,
            &first_transfer_tx,
            tasks,
        ),
    });

    let mut calc = ThroughputCalculator::new(config.max_seconds * 1000);
    run_transfer_loop(
        &mut tasks,
        &mut first_transfer_rx,
        &control,
        &mut loop_state,
        STAGE_POLL_INTERVAL,
        config,
        &active_connections,
        &mut calc,
        |now, elapsed| {
            let bytes = total_bytes.load(Ordering::Relaxed);
            let _ = now;
            TransferSample {
                sample_elapsed_ms: elapsed.as_millis() as u64,
                sample_bytes: bytes,
                progress_bytes: bytes,
                progress_mbps: None,
            }
        },
        |range, tasks| {
            if ready_pool.is_empty() {
                return;
            }
            spawn_worker_range(range, tasks, |idx, tasks| match mode {
                TransportProtocol::Tcp => spawn_download_worker_tcp(
                    idx,
                    &ready_pool,
                    &total_bytes,
                    &request_attempts,
                    &request_successes,
                    &request_transport_errors,
                    &response_read_errors,
                    &active_connections,
                    &per_server_bytes,
                    &target_workers,
                    &suggested_size,
                    &first_byte_at,
                    &stage_stop_rx,
                    &first_transfer_tx,
                    tasks,
                ),
                TransportProtocol::Xhr => spawn_download_worker_xhr(
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
                    &per_server_bytes,
                    &target_workers,
                    &suggested_size,
                    &first_byte_at,
                    &stage_stop_rx,
                    &first_transfer_tx,
                    tasks,
                ),
            });
        },
        |elapsed, bytes, mbps, active_connections| {
            on_progress(DownloadProgress {
                elapsed,
                bytes,
                mbps,
                active_connections,
            });
        },
    )
    .await;

    drain_join_set(&mut tasks, STAGE_JOIN_GRACE_PERIOD).await;

    let throughput_result = calc.finish();
    let actual_duration_ms = elapsed_ms_since(loop_state.progress_clock_start);
    let bytes = total_bytes.load(Ordering::Relaxed);
    let per_server = ready_pool
        .iter()
        .enumerate()
        .map(|(index, server)| {
            let bytes = per_server_bytes[index].load(Ordering::Relaxed);
            PerServerDownloadStats {
                server_id: server.id,
                bytes,
                mbps: mbps_from_bytes(bytes, config.max_seconds),
            }
        })
        .collect();

    Ok(DownloadStats {
        bytes,
        mbps: throughput_result.blended_mbps(),
        actual_duration_ms,
        throughput: Some(throughput_result),
        request_attempts: request_attempts.load(Ordering::Relaxed),
        request_successes: request_successes.load(Ordering::Relaxed),
        request_http_errors: request_http_errors.load(Ordering::Relaxed),
        request_transport_errors: request_transport_errors.load(Ordering::Relaxed),
        response_read_errors: response_read_errors.load(Ordering::Relaxed),
        per_server,
    })
}

async fn resolve_ready_download_pool(
    client: &Client,
    mode: TransportProtocol,
    server_pool: &[SpeedtestServer],
    default_guid: &str,
) -> Vec<SpeedtestServer> {
    let probe_client = client.clone();
    resolve_ready_server_pool(server_pool, default_guid, move |server, guid| {
        let probe_client = probe_client.clone();
        async move {
            match mode {
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
                TransportProtocol::Xhr => matches!(
                    timeout(
                        SERVER_READINESS_TIMEOUT,
                        browser_protocol::download(
                            &probe_client,
                            &server,
                            &guid,
                            DOWNLOAD_READINESS_BYTES,
                        ),
                    )
                    .await,
                    Ok(Ok(_))
                ),
            }
        }
    })
    .await
}

#[allow(clippy::too_many_arguments)]
fn spawn_download_worker_tcp(
    worker_index: usize,
    server_pool: &[SpeedtestServer],
    total_bytes: &Arc<AtomicU64>,
    request_attempts: &Arc<AtomicU64>,
    request_successes: &Arc<AtomicU64>,
    request_transport_errors: &Arc<AtomicU64>,
    response_read_errors: &Arc<AtomicU64>,
    active_connections: &Arc<AtomicUsize>,
    per_server_bytes: &Arc<Vec<AtomicU64>>,
    target_workers: &Arc<AtomicUsize>,
    suggested_size: &Arc<AtomicUsize>,
    first_byte_at: &Arc<OnceLock<Instant>>,
    stop_rx: &watch::Receiver<bool>,
    first_transfer_tx: &watch::Sender<bool>,
    tasks: &mut JoinSet<()>,
) {
    let server_index = worker_index % server_pool.len();
    let worker_server = server_pool[server_index].clone();
    let worker_bytes = Arc::clone(total_bytes);
    let worker_attempts = Arc::clone(request_attempts);
    let worker_successes = Arc::clone(request_successes);
    let worker_transport_errors = Arc::clone(request_transport_errors);
    let worker_read_errors = Arc::clone(response_read_errors);
    let worker_active_connections = Arc::clone(active_connections);
    let worker_per_server_bytes = Arc::clone(per_server_bytes);
    let worker_target = Arc::clone(target_workers);
    let worker_suggested_size = Arc::clone(suggested_size);
    let worker_first_byte_at = Arc::clone(first_byte_at);
    let mut worker_stop = stop_rx.clone();
    let worker_first_transfer_tx = first_transfer_tx.clone();
    tasks.spawn(async move {
        let mut stream = None;
        let mut stream_connected = false;

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
            let counters: [&AtomicU64; 2] = [&worker_bytes, &worker_per_server_bytes[server_index]];
            let maybe_downloaded = tokio::select! {
                downloaded = modern_protocol::download(
                    active_stream,
                    request_size,
                    &counters,
                    &worker_first_byte_at,
                    &worker_first_transfer_tx,
                ) => Some(downloaded),
                _ = worker_stop.changed() => None,
            };

            match maybe_downloaded {
                Some(Ok(_)) => {
                    worker_successes.fetch_add(1, Ordering::Relaxed);
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
fn spawn_download_worker_xhr(
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
    per_server_bytes: &Arc<Vec<AtomicU64>>,
    target_workers: &Arc<AtomicUsize>,
    suggested_size: &Arc<AtomicUsize>,
    first_byte_at: &Arc<OnceLock<Instant>>,
    stop_rx: &watch::Receiver<bool>,
    first_transfer_tx: &watch::Sender<bool>,
    tasks: &mut JoinSet<()>,
) {
    let worker_client = client.clone();
    let server_index = worker_index % server_pool.len();
    let worker_server = server_pool[server_index].clone();
    let worker_default_guid = default_guid.to_string();
    let _worker_guid = worker_server
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
    let worker_target = Arc::clone(target_workers);
    let worker_suggested_size = Arc::clone(suggested_size);
    let worker_first_byte_at = Arc::clone(first_byte_at);
    let mut worker_stop = stop_rx.clone();
    let worker_first_transfer_tx = first_transfer_tx.clone();
    tasks.spawn(async move {
        while !*worker_stop.borrow_and_update() {
            if worker_index >= worker_target.load(Ordering::Relaxed) {
                break;
            }

            worker_attempts.fetch_add(1, Ordering::Relaxed);

            let _active_guard = ActiveConnectionGuard::new(&worker_active_connections);

            let request_size = worker_suggested_size.load(Ordering::Relaxed);
            let counters: [&AtomicU64; 2] =
                [&worker_bytes, &worker_per_server_bytes[server_index]];
            let maybe_downloaded = tokio::select! {
                downloaded = browser_protocol::download_streaming(
                    &worker_client,
                    &worker_server,
                    request_size,
                    &counters,
                    Some(&worker_first_byte_at),
                    Some(&worker_first_transfer_tx),
                ) => Some(downloaded),
                _ = worker_stop.changed() => None,
            };

            match maybe_downloaded {
                Some(Ok(_)) => {
                    worker_successes.fetch_add(1, Ordering::Relaxed);
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

#[cfg(test)]
mod tests {
    use super::*;

    use std::net::Ipv4Addr;

    use anyhow::Context;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::watch;
    use tokio::time::{Duration, Instant, sleep, timeout};

    fn download_test_server(port: u16) -> SpeedtestServer {
        SpeedtestServer {
            id: 11,
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

    async fn handle_slow_download(mut stream: TcpStream, mut stop: watch::Receiver<bool>) {
        let _ = read_line(&mut stream).await;

        loop {
            if *stop.borrow_and_update() {
                break;
            }

            if stream.write_all(&[0x44]).await.is_err() {
                break;
            }
            sleep(Duration::from_millis(2)).await;
        }
    }

    async fn spawn_slow_download_server()
    -> anyhow::Result<(u16, watch::Sender<bool>, tokio::task::JoinHandle<()>)> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .context("bind download server")?;
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
                            handle_slow_download(stream, stop).await;
                        });
                    }
                }
            }
        });

        Ok((port, stop_tx, handle))
    }

    #[tokio::test]
    async fn tcp_download_stage_honors_stop_deadline() -> anyhow::Result<()> {
        let (port, stop_tx, server_handle) = spawn_slow_download_server()
            .await
            .context("start slow download test server")?;

        let server = download_test_server(port);
        let client = reqwest::Client::new();
        let start = Instant::now();

        let test_config = crate::speedtest::throughput::TransferConfig {
            connections: 1,
            initial_connections: 1,
            max_seconds: 1,
            progress_interval: None,
            request_target_ms: 1_000,
            start_request_size: 25_000_000,
            min_request_size: 32 * 1024,
            max_request_size: 25 * 1024 * 1024,
        };
        let result = timeout(
            Duration::from_secs(3),
            run_download_test(
                &client,
                &server,
                TransportProtocol::Tcp,
                std::slice::from_ref(&server),
                &test_config,
                |_| {},
            ),
        )
        .await
        .context("download run must finish inside timeout")?;

        let stats = result.context("download stage must succeed")?;

        stop_tx.send_replace(true);
        let _ = timeout(Duration::from_millis(300), server_handle).await;

        assert!(
            start.elapsed() < Duration::from_millis(1800),
            "download stage should stop near target duration"
        );
        assert_eq!(stats.bytes, 0);

        Ok(())
    }
}
