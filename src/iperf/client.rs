use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout};

use crate::cli::IperfProtocol;
use crate::iperf::model::{
    IperfClientConfig, IperfDirection, IperfDirectionSummary, IperfProgress,
};
use crate::iperf::proxy::{self, ProxyScheme, ProxySpec, Socks5UdpAssociation};
use crate::iperf::udp_packet::{UdpReceiveMetrics, build_iperf_udp_packet, parse_iperf_udp_header};
use crate::util::{clamp_worker_count, mbps_from_bytes};

const TEST_START: i8 = 1;
const TEST_RUNNING: i8 = 2;
const TEST_END: i8 = 4;
const PARAM_EXCHANGE: i8 = 9;
const CREATE_STREAMS: i8 = 10;
const SERVER_TERMINATE: i8 = 11;
const EXCHANGE_RESULTS: i8 = 13;
const DISPLAY_RESULTS: i8 = 14;
const IPERF_DONE: i8 = 16;
const ACCESS_DENIED: i8 = -1;
const SERVER_ERROR: i8 = -2;

const COOKIE_SIZE: usize = 37;
const UDP_CONNECT_MSG: u32 = if cfg!(target_endian = "big") {
    0x3938_3736
} else {
    0x3637_3839
};
const UDP_CONNECT_REPLY: u32 = if cfg!(target_endian = "big") {
    0x3637_3839
} else {
    0x3938_3736
};
const LEGACY_UDP_CONNECT_REPLY: u32 = 987_654_321;

pub async fn run_direction<F>(
    config: &IperfClientConfig,
    direction: IperfDirection,
    progress_interval: Option<Duration>,
    mut on_progress: F,
) -> Result<IperfDirectionSummary>
where
    F: FnMut(IperfProgress),
{
    const MAX_ATTEMPTS: usize = 3;

    let mut last_error = None;
    for attempt in 1..=MAX_ATTEMPTS {
        match run_direction_once(config, direction, progress_interval, &mut on_progress).await {
            Ok(summary) => return Ok(summary),
            Err(error) if attempt < MAX_ATTEMPTS && is_transient_iperf_error(&error) => {
                last_error = Some(error);
                sleep(Duration::from_millis((attempt as u64) * 800)).await;
            }
            Err(error) => return Err(error),
        }
    }

    Err(last_error.expect("retry loop must return or capture an error"))
}

async fn run_direction_once<F>(
    config: &IperfClientConfig,
    direction: IperfDirection,
    progress_interval: Option<Duration>,
    on_progress: &mut F,
) -> Result<IperfDirectionSummary>
where
    F: FnMut(IperfProgress),
{
    proxy::ensure_compatible(config.protocol, config.proxy.as_ref())?;

    let parallel = clamp_worker_count(config.parallel);
    let seconds = config.seconds.max(1);
    let packet_size = match config.protocol {
        IperfProtocol::Tcp => 128 * 1024,
        IperfProtocol::Udp => 1200,
    };
    let bitrate_bps = match config.protocol {
        IperfProtocol::Tcp => None,
        IperfProtocol::Udp => Some(config.bitrate_bps.unwrap_or(1_000_000)),
    };

    let mut control = timeout(
        Duration::from_secs(8),
        proxy::connect_tcp_target(&config.host, config.port, config.proxy.as_ref()),
    )
    .await
    .context("iperf3 control connection timed out")??;

    let cookie = make_cookie();
    control
        .write_all(&cookie)
        .await
        .context("failed writing iperf3 cookie to control channel")?;

    let mut streams: Vec<DataStream> = Vec::new();
    let reverse = matches!(direction, IperfDirection::Download);

    loop {
        let state = read_state(&mut control).await?;
        match state {
            PARAM_EXCHANGE => {
                let params = build_parameters_json(
                    config.protocol,
                    seconds,
                    parallel,
                    packet_size,
                    bitrate_bps,
                    reverse,
                );
                write_json_frame(&mut control, &params).await?;
            }
            CREATE_STREAMS => {
                streams = create_streams(
                    &config.host,
                    config.port,
                    config.protocol,
                    parallel,
                    &cookie,
                    config.proxy.as_ref(),
                    reverse,
                )
                .await?;
            }
            TEST_START => {}
            TEST_RUNNING => break,
            ACCESS_DENIED => {
                bail!("iperf3 server is busy running a test; try again later")
            }
            SERVER_ERROR => return Err(read_server_error(&mut control).await),
            SERVER_TERMINATE => bail!("iperf3 server terminated the test unexpectedly"),
            other => bail!("unexpected iperf3 control state during setup: {other}"),
        }
    }

    let local_streams = run_stream_data(
        streams,
        config.protocol,
        direction,
        seconds,
        packet_size,
        bitrate_bps,
        progress_interval,
        on_progress,
    )
    .await?;

    write_state(&mut control, TEST_END).await?;

    let mut remote: Option<RemoteResults> = None;
    let mut saw_done = false;

    while !saw_done {
        let state = match read_state(&mut control).await {
            Ok(state) => state,
            Err(_) => break,
        };
        match state {
            EXCHANGE_RESULTS => {
                let local_payload = build_results_json(&local_streams, seconds);
                write_json_frame(&mut control, &local_payload).await?;
                let remote_json = read_json_frame(&mut control, None).await?;
                remote = Some(parse_remote_results(&remote_json));
            }
            DISPLAY_RESULTS => {
                let _ = write_state(&mut control, IPERF_DONE).await;
            }
            IPERF_DONE => saw_done = true,
            SERVER_ERROR => return Err(read_server_error(&mut control).await),
            SERVER_TERMINATE => bail!("iperf3 server terminated before finishing results exchange"),
            TEST_END | TEST_START | TEST_RUNNING | CREATE_STREAMS | PARAM_EXCHANGE => {}
            other => bail!("unexpected iperf3 control state while finishing: {other}"),
        }
    }

    Ok(summarize_direction(
        config.protocol,
        direction,
        seconds,
        local_streams,
        remote,
    ))
}

fn is_transient_iperf_error(error: &anyhow::Error) -> bool {
    let message = error.to_string().to_lowercase();
    message.contains("connection refused")
        || message.contains("connection reset")
        || message.contains("busy running a test")
        || message.contains("early eof")
        || message.contains("timed out")
        || message.contains("unexpectedly")
        || message.contains("control socket has closed")
}

fn build_parameters_json(
    protocol: IperfProtocol,
    seconds: u64,
    parallel: usize,
    packet_size: usize,
    bitrate_bps: Option<u64>,
    reverse: bool,
) -> Value {
    let mut payload = json!({
        "omit": 0,
        "time": seconds,
        "num": 0,
        "blockcount": 0,
        "parallel": parallel,
        "len": packet_size,
        "client_version": "3.20"
    });

    if matches!(protocol, IperfProtocol::Tcp) {
        payload["tcp"] = Value::Bool(true);
    } else {
        payload["udp"] = Value::Bool(true);
        if let Some(rate) = bitrate_bps {
            payload["bandwidth"] = Value::Number(rate.into());
        }
    }

    if reverse {
        payload["reverse"] = Value::Bool(true);
    }

    payload
}

async fn create_streams(
    host: &str,
    port: u16,
    protocol: IperfProtocol,
    parallel: usize,
    cookie: &[u8; COOKIE_SIZE],
    proxy_spec: Option<&ProxySpec>,
    reverse: bool,
) -> Result<Vec<DataStream>> {
    let mut streams = Vec::with_capacity(parallel);

    for _ in 0..parallel {
        match protocol {
            IperfProtocol::Tcp => {
                let mut stream = timeout(
                    Duration::from_secs(8),
                    proxy::connect_tcp_target(host, port, proxy_spec),
                )
                .await
                .context("timed out connecting iperf3 TCP data stream")??;

                stream
                    .write_all(cookie)
                    .await
                    .context("failed writing iperf3 cookie to TCP data stream")?;

                streams.push(DataStream::Tcp(stream));
            }
            IperfProtocol::Udp => {
                let mut stream = match proxy_spec.map(|proxy| proxy.scheme) {
                    None => {
                        let socket = proxy::connect_udp_socket_direct(host, port).await?;
                        DataStream::UdpDirect(socket)
                    }
                    Some(ProxyScheme::Socks5 | ProxyScheme::Socks5h) => {
                        let assoc = proxy::connect_udp_socket_socks5(
                            proxy_spec.expect("proxy spec must exist"),
                            host,
                            port,
                        )
                        .await?;
                        DataStream::UdpSocks(assoc)
                    }
                    Some(ProxyScheme::Http | ProxyScheme::Https) => {
                        bail!("UDP stream creation with HTTP/HTTPS proxy is not supported")
                    }
                };

                udp_connect_handshake(&mut stream, reverse).await?;
                streams.push(stream);
            }
        }
    }

    Ok(streams)
}

async fn run_stream_data<F>(
    streams: Vec<DataStream>,
    protocol: IperfProtocol,
    direction: IperfDirection,
    seconds: u64,
    packet_size: usize,
    bitrate_bps: Option<u64>,
    progress_interval: Option<Duration>,
    on_progress: &mut F,
) -> Result<Vec<LocalStreamResult>>
where
    F: FnMut(IperfProgress),
{
    let stream_count = streams.len().max(1);
    let total_bytes = Arc::new(AtomicU64::new(0));
    let start_at = Instant::now();
    let stop_at = start_at + Duration::from_secs(seconds);

    let mut workers = JoinSet::new();
    for (idx, stream) in streams.into_iter().enumerate() {
        let total_bytes_ref = Arc::clone(&total_bytes);
        workers.spawn(async move {
            run_one_stream(
                idx + 1,
                stream,
                protocol,
                direction,
                stop_at,
                packet_size,
                bitrate_bps,
                stream_count,
                total_bytes_ref,
            )
            .await
        });
    }

    if let Some(interval) = progress_interval {
        while Instant::now() < stop_at {
            sleep(interval).await;
            let elapsed = start_at.elapsed();
            let elapsed_secs = elapsed.as_secs().max(1);
            let bytes = total_bytes.load(Ordering::Relaxed);
            on_progress(IperfProgress {
                elapsed,
                bytes,
                mbps: mbps_from_bytes(bytes, elapsed_secs),
            });
        }
    }

    let mut out = Vec::new();
    while let Some(joined) = workers.join_next().await {
        out.push(joined.context("iperf worker task join failure")??);
    }

    Ok(out)
}

async fn run_one_stream(
    id: usize,
    mut stream: DataStream,
    protocol: IperfProtocol,
    direction: IperfDirection,
    stop_at: Instant,
    packet_size: usize,
    bitrate_bps: Option<u64>,
    stream_count: usize,
    total_bytes: Arc<AtomicU64>,
) -> Result<LocalStreamResult> {
    match protocol {
        IperfProtocol::Tcp => {
            run_one_stream_tcp(
                id,
                &mut stream,
                direction,
                stop_at,
                packet_size,
                total_bytes,
            )
            .await
        }
        IperfProtocol::Udp => {
            run_one_stream_udp(
                id,
                &mut stream,
                direction,
                stop_at,
                packet_size,
                bitrate_bps,
                stream_count,
                total_bytes,
            )
            .await
        }
    }
}

async fn run_one_stream_tcp(
    id: usize,
    stream: &mut DataStream,
    direction: IperfDirection,
    stop_at: Instant,
    packet_size: usize,
    total_bytes: Arc<AtomicU64>,
) -> Result<LocalStreamResult> {
    let DataStream::Tcp(socket) = stream else {
        bail!("internal TCP stream type mismatch")
    };

    let mut stats = LocalStreamResult::new(id);
    let payload = vec![0x55_u8; packet_size];
    let mut recv_buf = vec![0_u8; packet_size];

    while Instant::now() < stop_at {
        match direction {
            IperfDirection::Upload => {
                let write_result =
                    timeout(Duration::from_secs(2), socket.write_all(&payload)).await;
                match write_result {
                    Ok(Ok(())) => {
                        stats.bytes += payload.len() as u64;
                        total_bytes.fetch_add(payload.len() as u64, Ordering::Relaxed);
                    }
                    _ => break,
                }
            }
            IperfDirection::Download => {
                let read_result = timeout(Duration::from_secs(2), socket.read(&mut recv_buf)).await;
                match read_result {
                    Ok(Ok(0)) => break,
                    Ok(Ok(read)) => {
                        stats.bytes += read as u64;
                        total_bytes.fetch_add(read as u64, Ordering::Relaxed);
                    }
                    Ok(Err(_)) => break,
                    Err(_) => continue,
                }
            }
        }
    }

    Ok(stats)
}

async fn run_one_stream_udp(
    id: usize,
    stream: &mut DataStream,
    direction: IperfDirection,
    stop_at: Instant,
    packet_size: usize,
    bitrate_bps: Option<u64>,
    stream_count: usize,
    total_bytes: Arc<AtomicU64>,
) -> Result<LocalStreamResult> {
    let mut stats = LocalStreamResult::new(id);
    let mut recv_buf = vec![0_u8; packet_size.max(2048)];
    let mut rx_metrics = UdpReceiveMetrics::default();
    let mut sequence = 1_u32;

    let per_worker_bitrate = bitrate_bps
        .and_then(|value| value.checked_div(stream_count as u64))
        .filter(|value| *value > 0);
    let send_delay = per_worker_bitrate.map(|bps| {
        let bits = (packet_size as u128) * 8;
        let nanos = (bits * 1_000_000_000_u128) / u128::from(bps.max(1));
        Duration::from_nanos(nanos.max(1_000_000) as u64)
    });

    while Instant::now() < stop_at {
        let io = match direction {
            IperfDirection::Upload => {
                let packet = build_iperf_udp_packet(sequence, packet_size);
                sequence = sequence.wrapping_add(1);
                udp_send(stream, &packet).await
            }
            IperfDirection::Download => udp_recv(stream, &mut recv_buf).await,
        };

        if let Ok(size) = io {
            stats.bytes += size as u64;
            total_bytes.fetch_add(size as u64, Ordering::Relaxed);

            if matches!(direction, IperfDirection::Upload) {
                stats.packets = Some(stats.packets.unwrap_or(0) + 1);
            } else {
                let header = parse_iperf_udp_header(&recv_buf[..size]);
                rx_metrics.on_packet(header);
            }
        }

        if let Some(delay) = send_delay
            && matches!(direction, IperfDirection::Upload)
        {
            sleep(delay).await;
        }
    }

    if matches!(direction, IperfDirection::Download) {
        stats.packets = Some(rx_metrics.total_packets);
        stats.lost_packets = Some(rx_metrics.lost_packets);
        stats.loss_percent = rx_metrics.loss_percent();
        stats.jitter_ms = Some(rx_metrics.jitter_ms);
        stats.out_of_order = Some(rx_metrics.out_of_order);
    }

    Ok(stats)
}

async fn udp_send(stream: &mut DataStream, payload: &[u8]) -> Result<usize> {
    match stream {
        DataStream::UdpDirect(socket) => {
            Ok(timeout(Duration::from_secs(1), socket.send(payload)).await??)
        }
        DataStream::UdpSocks(assoc) => {
            Ok(timeout(Duration::from_secs(1), assoc.send_to_target(payload)).await??)
        }
        DataStream::Tcp(_) => bail!("internal UDP stream type mismatch"),
    }
}

async fn udp_recv(stream: &mut DataStream, buffer: &mut [u8]) -> Result<usize> {
    match stream {
        DataStream::UdpDirect(socket) => {
            match timeout(Duration::from_millis(400), socket.recv(buffer)).await {
                Ok(Ok(size)) => Ok(size),
                Ok(Err(error)) => Err(error.into()),
                Err(_) => bail!("udp receive timeout"),
            }
        }
        DataStream::UdpSocks(assoc) => {
            match timeout(Duration::from_millis(400), assoc.recv_from_target(buffer)).await {
                Ok(Ok(size)) => Ok(size),
                Ok(Err(error)) => Err(error),
                Err(_) => bail!("udp receive timeout"),
            }
        }
        DataStream::Tcp(_) => bail!("internal UDP stream type mismatch"),
    }
}

async fn udp_connect_handshake(stream: &mut DataStream, reverse: bool) -> Result<()> {
    let message = UDP_CONNECT_MSG.to_ne_bytes();
    let _ = udp_send(stream, &message).await?;

    let mut buffer = [0_u8; 2048];
    let deadline = Instant::now() + Duration::from_secs(8);
    let max_attempts = if reverse { 8 } else { 3 };
    let mut attempts = 0;

    while Instant::now() < deadline && attempts < max_attempts {
        attempts += 1;
        let received = timeout(Duration::from_secs(1), udp_recv(stream, &mut buffer)).await;
        let Ok(Ok(size)) = received else {
            continue;
        };
        if size < 4 {
            continue;
        }

        let value = u32::from_ne_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);
        if value == UDP_CONNECT_REPLY || value == LEGACY_UDP_CONNECT_REPLY {
            return Ok(());
        }
    }

    bail!("failed waiting for iperf3 UDP connect reply")
}

fn build_results_json(local: &[LocalStreamResult], seconds: u64) -> Value {
    let streams = local
        .iter()
        .map(|stream| {
            json!({
                "id": stream.id,
                "bytes": stream.bytes,
                "retransmits": -1,
                "jitter": stream.jitter_ms.unwrap_or(0.0),
                "errors": stream.lost_packets.unwrap_or(0),
                "omitted_errors": stream.lost_packets.unwrap_or(0),
                "packets": stream.packets.unwrap_or(0),
                "omitted_packets": stream.packets.unwrap_or(0),
                "start_time": 0.0,
                "end_time": seconds as f64
            })
        })
        .collect::<Vec<_>>();

    json!({
        "cpu_util_total": 0.0,
        "cpu_util_user": 0.0,
        "cpu_util_system": 0.0,
        "sender_has_retransmits": 0,
        "streams": streams
    })
}

fn parse_remote_results(json: &Value) -> RemoteResults {
    let mut streams = HashMap::new();
    let mut retransmits_capable = None;

    if let Some(value) = json.get("sender_has_retransmits").and_then(Value::as_i64) {
        retransmits_capable = Some(value > 0);
    }

    if let Some(items) = json.get("streams").and_then(Value::as_array) {
        for stream in items {
            let Some(id) = stream.get("id").and_then(Value::as_u64) else {
                continue;
            };
            let summary = RemoteStreamResult {
                bytes: stream.get("bytes").and_then(Value::as_u64).unwrap_or(0),
                jitter_ms: stream.get("jitter").and_then(Value::as_f64),
                errors: stream.get("errors").and_then(Value::as_u64),
                packets: stream.get("packets").and_then(Value::as_u64),
            };
            streams.insert(id as usize, summary);
        }
    }

    RemoteResults {
        retransmits_capable,
        streams,
    }
}

fn summarize_direction(
    protocol: IperfProtocol,
    direction: IperfDirection,
    seconds: u64,
    local: Vec<LocalStreamResult>,
    remote: Option<RemoteResults>,
) -> IperfDirectionSummary {
    let local_bytes = local.iter().map(|stream| stream.bytes).sum::<u64>();
    let local_packets = local
        .iter()
        .map(|stream| stream.packets.unwrap_or(0))
        .sum::<u64>();
    let local_lost = local
        .iter()
        .map(|stream| stream.lost_packets.unwrap_or(0))
        .sum::<u64>();
    let local_out_of_order = local
        .iter()
        .map(|stream| stream.out_of_order.unwrap_or(0))
        .sum::<u64>();

    let local_jitter_ms = weighted_local_jitter(&local);

    let remote_agg = remote.as_ref().map(RemoteResults::aggregate);

    let (bytes, packets, lost_packets, loss_percent, jitter_ms, out_of_order) =
        match (protocol, direction) {
            (IperfProtocol::Udp, IperfDirection::Upload) => {
                let bytes = remote_agg
                    .as_ref()
                    .map(|value| value.bytes)
                    .filter(|value| *value > 0)
                    .unwrap_or(local_bytes);
                let packets = remote_agg
                    .as_ref()
                    .and_then(|value| value.packets)
                    .or(Some(local_packets));
                let lost = remote_agg.as_ref().and_then(|value| value.errors);
                let loss = if let (Some(p), Some(l)) = (packets, lost) {
                    if p == 0 {
                        None
                    } else {
                        Some((l as f64 * 100.0) / p as f64)
                    }
                } else {
                    None
                };
                let jitter = remote_agg.as_ref().and_then(|value| value.jitter_ms);
                (bytes, packets, lost, loss, jitter, None)
            }
            (IperfProtocol::Udp, IperfDirection::Download) => {
                let loss = if local_packets == 0 {
                    None
                } else {
                    Some((local_lost as f64 * 100.0) / local_packets as f64)
                };
                (
                    local_bytes,
                    Some(local_packets),
                    Some(local_lost),
                    loss,
                    local_jitter_ms,
                    Some(local_out_of_order),
                )
            }
            (IperfProtocol::Tcp, _) => {
                let bytes = if local_bytes > 0 {
                    local_bytes
                } else {
                    remote_agg.as_ref().map(|value| value.bytes).unwrap_or(0)
                };
                (bytes, None, None, None, None, None)
            }
        };

    let _ = remote
        .as_ref()
        .and_then(|value| value.retransmits_capable)
        .unwrap_or(false);

    IperfDirectionSummary {
        bytes,
        mbps: mbps_from_bytes(bytes, seconds),
        duration_seconds: seconds,
        packets,
        lost_packets,
        loss_percent,
        jitter_ms,
        out_of_order,
    }
}

fn weighted_local_jitter(local: &[LocalStreamResult]) -> Option<f64> {
    let mut packets = 0_u64;
    let mut weighted = 0.0;

    for stream in local {
        let Some(jitter) = stream.jitter_ms else {
            continue;
        };
        let p = stream.packets.unwrap_or(0);
        packets += p;
        weighted += jitter * p as f64;
    }

    if packets == 0 {
        None
    } else {
        Some(weighted / packets as f64)
    }
}

async fn write_json_frame(stream: &mut TcpStream, value: &Value) -> Result<()> {
    let body = serde_json::to_vec(value)?;
    let len = u32::try_from(body.len()).context("JSON frame is too large")?;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(&body).await?;
    Ok(())
}

async fn read_json_frame(stream: &mut TcpStream, max_size: Option<usize>) -> Result<Value> {
    let mut size_bytes = [0_u8; 4];
    stream.read_exact(&mut size_bytes).await?;
    let size = u32::from_be_bytes(size_bytes) as usize;
    if size == 0 {
        bail!("received empty JSON frame from iperf3 control channel")
    }
    if let Some(max) = max_size
        && size > max
    {
        bail!("received oversized iperf3 JSON frame ({size} > {max})")
    }

    let mut body = vec![0_u8; size];
    stream.read_exact(&mut body).await?;
    Ok(serde_json::from_slice(&body).context("failed parsing iperf3 JSON frame")?)
}

async fn read_state(stream: &mut TcpStream) -> Result<i8> {
    let mut state = [0_u8; 1];
    stream.read_exact(&mut state).await?;
    Ok(state[0] as i8)
}

async fn write_state(stream: &mut TcpStream, state: i8) -> Result<()> {
    stream.write_all(&[state as u8]).await?;
    Ok(())
}

async fn read_server_error(stream: &mut TcpStream) -> anyhow::Error {
    let mut code = [0_u8; 4];
    let mut os = [0_u8; 4];

    let read_code = stream.read_exact(&mut code).await;
    let read_os = stream.read_exact(&mut os).await;

    match (read_code, read_os) {
        (Ok(_), Ok(_)) => {
            let code = i32::from_be_bytes(code);
            let os = i32::from_be_bytes(os);
            anyhow::anyhow!("iperf3 server returned SERVER_ERROR code={code} os_errno={os}")
        }
        _ => anyhow::anyhow!("iperf3 server returned SERVER_ERROR and closed connection"),
    }
}

fn make_cookie() -> [u8; COOKIE_SIZE] {
    let mut cookie = [0_u8; COOKIE_SIZE];
    let mut entropy = [0_u8; COOKIE_SIZE - 1];

    let mut seeded = false;
    if let Ok(mut file) = std::fs::File::open("/dev/urandom")
        && file.read_exact(&mut entropy).is_ok()
    {
        seeded = true;
    }

    if !seeded {
        let mut seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|value| value.as_nanos() as u64)
            .unwrap_or(0xA5A5_5A5A_1234_5678);
        for value in &mut entropy {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            *value = (seed & 0xFF) as u8;
        }
    }

    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz234567";
    for (index, value) in entropy.iter().enumerate() {
        cookie[index] = ALPHABET[*value as usize % ALPHABET.len()];
    }
    cookie[COOKIE_SIZE - 1] = 0;

    cookie
}

enum DataStream {
    Tcp(TcpStream),
    UdpDirect(UdpSocket),
    UdpSocks(Socks5UdpAssociation),
}

#[derive(Debug, Clone)]
struct LocalStreamResult {
    id: usize,
    bytes: u64,
    packets: Option<u64>,
    lost_packets: Option<u64>,
    loss_percent: Option<f64>,
    jitter_ms: Option<f64>,
    out_of_order: Option<u64>,
}

impl LocalStreamResult {
    fn new(id: usize) -> Self {
        Self {
            id,
            bytes: 0,
            packets: None,
            lost_packets: None,
            loss_percent: None,
            jitter_ms: None,
            out_of_order: None,
        }
    }
}

#[derive(Debug, Clone)]
struct RemoteStreamResult {
    bytes: u64,
    jitter_ms: Option<f64>,
    errors: Option<u64>,
    packets: Option<u64>,
}

#[derive(Debug, Clone, Default)]
struct RemoteResults {
    retransmits_capable: Option<bool>,
    streams: HashMap<usize, RemoteStreamResult>,
}

impl RemoteResults {
    fn aggregate(&self) -> RemoteAggregate {
        let mut bytes = 0_u64;
        let mut packets = 0_u64;
        let mut errors = 0_u64;
        let mut jitter_weighted = 0.0;
        let mut jitter_packets = 0_u64;
        let mut any_packets = false;
        let mut any_errors = false;

        for stream in self.streams.values() {
            bytes += stream.bytes;

            if let Some(value) = stream.packets {
                packets += value;
                any_packets = true;
                if let Some(jitter) = stream.jitter_ms {
                    jitter_weighted += jitter * value as f64;
                    jitter_packets += value;
                }
            }

            if let Some(value) = stream.errors {
                errors += value;
                any_errors = true;
            }
        }

        RemoteAggregate {
            bytes,
            packets: any_packets.then_some(packets),
            errors: any_errors.then_some(errors),
            jitter_ms: if jitter_packets == 0 {
                None
            } else {
                Some(jitter_weighted / jitter_packets as f64)
            },
        }
    }
}

#[derive(Debug, Clone, Default)]
struct RemoteAggregate {
    bytes: u64,
    packets: Option<u64>,
    errors: Option<u64>,
    jitter_ms: Option<f64>,
}
