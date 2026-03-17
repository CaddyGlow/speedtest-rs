use std::collections::HashSet;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde::Deserialize;
use thiserror::Error;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::watch;
use tokio::time::timeout;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tracing::{debug, trace};
use url::Url;

use crate::speedtest::servers::SpeedtestServer;

static NONCE_COUNTER: AtomicU64 = AtomicU64::new(0);
const WS_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const WS_IO_TIMEOUT: Duration = Duration::from_secs(2);
const WS_PROTOCOL_LEVEL: &str = "2";

#[derive(Debug, Clone, Copy)]
pub struct UploadStatsSample {
    pub bytes: u64,
    pub elapsed_ms: u64,
    pub index: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct RawUploadStatsSample {
    #[serde(rename = "b")]
    bytes: u64,
    #[serde(rename = "e")]
    elapsed_ms: u64,
    #[serde(rename = "i")]
    index: Option<u64>,
    #[serde(rename = "t")]
    sample_type: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum TransferRequestError {
    #[error("HTTP status error")]
    HttpStatus,
    #[error("request transport error")]
    Transport,
    #[error("response read error")]
    ResponseRead,
    #[error("invalid endpoint")]
    InvalidEndpoint,
}

pub async fn probe_latency_samples_websocket(
    server: &SpeedtestServer,
    samples: usize,
) -> Result<Vec<f64>> {
    if samples == 0 {
        bail!("latency samples must be greater than zero");
    }

    debug!(
        server_id = server.id,
        host = %server.host,
        samples,
        "starting websocket latency probe"
    );

    let endpoints = websocket_endpoints(server)?;
    let mut last_error = None;

    for endpoint in endpoints {
        debug!(server_id = server.id, endpoint = %endpoint, "trying websocket endpoint");
        match probe_latency_samples_over_websocket_endpoint(&endpoint, samples).await {
            Ok(latencies) => {
                debug!(
                    server_id = server.id,
                    endpoint = %endpoint,
                    successful_samples = latencies.len(),
                    "websocket latency probe succeeded"
                );
                return Ok(latencies);
            }
            Err(error) => {
                debug!(
                    server_id = server.id,
                    endpoint = %endpoint,
                    error = %error,
                    error_debug = ?error,
                    error_chain = %format!("{error:#}"),
                    "websocket endpoint failed"
                );
                last_error = Some((endpoint, error));
            }
        }
    }

    let Some((endpoint, error)) = last_error else {
        bail!("no websocket endpoint candidates available for latency probing");
    };
    Err(error).with_context(|| format!("websocket latency probe failed at {endpoint}"))
}

pub async fn probe_latency_samples_websocket_for_duration(
    server: &SpeedtestServer,
    duration: Duration,
) -> Result<Vec<f64>> {
    let target_duration = duration.max(Duration::from_secs(1));
    debug!(
        server_id = server.id,
        host = %server.host,
        duration_ms = target_duration.as_millis(),
        "starting websocket loaded latency probe"
    );

    let endpoints = websocket_endpoints(server)?;
    let mut last_error = None;

    for endpoint in endpoints {
        debug!(server_id = server.id, endpoint = %endpoint, "trying websocket endpoint");
        match probe_latency_samples_over_websocket_endpoint_for_duration(&endpoint, target_duration)
            .await
        {
            Ok(latencies) => {
                debug!(
                    server_id = server.id,
                    endpoint = %endpoint,
                    successful_samples = latencies.len(),
                    "websocket loaded latency probe succeeded"
                );
                return Ok(latencies);
            }
            Err(error) => {
                debug!(
                    server_id = server.id,
                    endpoint = %endpoint,
                    error = %error,
                    "websocket loaded latency endpoint failed"
                );
                last_error = Some((endpoint, error));
            }
        }
    }

    let Some((endpoint, error)) = last_error else {
        bail!("no websocket endpoint candidates available for loaded latency probing");
    };
    Err(error).with_context(|| format!("websocket loaded latency probe failed at {endpoint}"))
}

pub async fn stream_upload_stats_samples(
    server: &SpeedtestServer,
    guid: &str,
    duration_seconds: u64,
    sample_interval_ms: u64,
    sender: UnboundedSender<UploadStatsSample>,
) -> Result<()> {
    let endpoints = websocket_endpoints(server)?;
    let mut last_error = None;

    for endpoint in endpoints {
        match stream_upload_stats_over_endpoint(
            &endpoint,
            guid,
            duration_seconds,
            sample_interval_ms,
            &sender,
        )
        .await
        {
            Ok(()) => return Ok(()),
            Err(error) => {
                debug!(
                    server_id = server.id,
                    endpoint = %endpoint,
                    error = %error,
                    "stream upload stats websocket endpoint failed"
                );
                last_error = Some((endpoint, error));
            }
        }
    }

    let Some((endpoint, error)) = last_error else {
        bail!("no websocket endpoint candidates available for upload stats stream");
    };
    Err(error).with_context(|| format!("upload stats stream websocket failed at {endpoint}"))
}

pub async fn probe_latency_samples_http(
    client: &Client,
    server: &SpeedtestServer,
    guid: &str,
    samples: usize,
) -> Result<Vec<f64>> {
    if samples == 0 {
        bail!("latency samples must be greater than zero");
    }

    let mut successful_samples = Vec::with_capacity(samples);
    for _ in 0..samples {
        if let Ok(elapsed_ms) = ping_over_http_once(client, server, guid).await {
            successful_samples.push(elapsed_ms);
        }
    }

    debug!(
        server_id = server.id,
        host = %server.host,
        attempted_samples = samples,
        successful_samples = successful_samples.len(),
        "HTTP latency probe completed"
    );

    Ok(successful_samples)
}

async fn probe_latency_samples_over_websocket_endpoint(
    endpoint: &Url,
    samples: usize,
) -> Result<Vec<f64>> {
    let mut request = endpoint
        .as_str()
        .into_client_request()
        .with_context(|| format!("failed building websocket request for {endpoint}"))?;

    let headers = request.headers_mut();
    headers.insert("Origin", "https://www.speedtest.net".parse()?);
    headers.insert("User-Agent", "Mozilla/5.0".parse()?);
    headers.insert("Accept", "*/*".parse()?);
    headers.insert("Accept-Language", "en-US,en;q=0.9".parse()?);
    headers.insert("Cache-Control", "no-cache".parse()?);
    headers.insert("Pragma", "no-cache".parse()?);

    let (mut socket, _) = timeout(WS_CONNECT_TIMEOUT, connect_async(request))
        .await
        .with_context(|| format!("timed out connecting websocket {endpoint}"))?
        .with_context(|| format!("failed to open websocket {endpoint}"))?;

    debug!(endpoint = %endpoint, "websocket connected");

    debug!(endpoint = %endpoint, "sending HI");
    ws_send_text(&mut socket, &format!("HI\t{WS_PROTOCOL_LEVEL}\t"), "HI").await?;
    debug!(endpoint = %endpoint, "waiting HELLO");
    ws_expect_prefix(&mut socket, "HELLO", "HELLO handshake").await?;

    debug!(endpoint = %endpoint, "sending GETIP");
    ws_send_text(&mut socket, "GETIP", "GETIP").await?;
    debug!(endpoint = %endpoint, "waiting YOURIP");
    ws_expect_prefix(&mut socket, "YOURIP", "GETIP response").await?;

    debug!(endpoint = %endpoint, "sending CAPABILITIES");
    ws_send_text(&mut socket, "CAPABILITIES", "CAPABILITIES").await?;
    debug!(endpoint = %endpoint, "waiting CAPABILITIES response");
    ws_expect_prefix(&mut socket, "CAPABILITIES", "CAPABILITIES response").await?;

    let mut successful_samples = Vec::with_capacity(samples);
    for sample_index in 0..samples {
        let token = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before UNIX_EPOCH")?
            .as_millis();
        let command = format!("PING {token}");
        let start = Instant::now();
        if let Err(error) = ws_send_text(&mut socket, &command, "PING").await {
            debug!(
                endpoint = %endpoint,
                sample_index,
                error = %error,
                "websocket PING send failed"
            );
            break;
        }

        if let Err(error) = ws_expect_prefix(&mut socket, "PONG", "PING response").await {
            debug!(
                endpoint = %endpoint,
                sample_index,
                error = %error,
                "websocket PONG wait failed"
            );
            break;
        }

        successful_samples.push(start.elapsed().as_secs_f64() * 1_000.0);
        debug!(
            endpoint = %endpoint,
            sample_index,
            latency_ms = successful_samples.last().copied().unwrap_or_default(),
            "websocket latency sample recorded"
        );
    }

    let _ = socket.close(None).await;
    debug!(endpoint = %endpoint, successful_samples = successful_samples.len(), "websocket closed");
    Ok(successful_samples)
}

async fn probe_latency_samples_over_websocket_endpoint_for_duration(
    endpoint: &Url,
    duration: Duration,
) -> Result<Vec<f64>> {
    let mut request = endpoint
        .as_str()
        .into_client_request()
        .with_context(|| format!("failed building websocket request for {endpoint}"))?;

    let headers = request.headers_mut();
    headers.insert("Origin", "https://www.speedtest.net".parse()?);
    headers.insert("User-Agent", "Mozilla/5.0".parse()?);
    headers.insert("Accept", "*/*".parse()?);
    headers.insert("Accept-Language", "en-US,en;q=0.9".parse()?);
    headers.insert("Cache-Control", "no-cache".parse()?);
    headers.insert("Pragma", "no-cache".parse()?);

    let (mut socket, _) = timeout(WS_CONNECT_TIMEOUT, connect_async(request))
        .await
        .with_context(|| format!("timed out connecting websocket {endpoint}"))?
        .with_context(|| format!("failed to open websocket {endpoint}"))?;

    ws_send_text(&mut socket, &format!("HI\t{WS_PROTOCOL_LEVEL}\t"), "HI").await?;
    ws_expect_prefix(&mut socket, "HELLO", "HELLO handshake").await?;

    ws_send_text(&mut socket, "GETIP", "GETIP").await?;
    ws_expect_prefix(&mut socket, "YOURIP", "GETIP response").await?;

    ws_send_text(&mut socket, "CAPABILITIES", "CAPABILITIES").await?;
    ws_expect_prefix(&mut socket, "CAPABILITIES", "CAPABILITIES response").await?;

    let mut successful_samples = Vec::new();
    let deadline = Instant::now() + duration;
    let mut sample_index = 0_usize;

    while Instant::now() < deadline {
        let token = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before UNIX_EPOCH")?
            .as_millis();
        let command = format!("PING {token}");
        let start = Instant::now();

        if ws_send_text(&mut socket, &command, "PING").await.is_err() {
            break;
        }
        if ws_expect_prefix(&mut socket, "PONG", "PING response")
            .await
            .is_err()
        {
            break;
        }

        successful_samples.push(start.elapsed().as_secs_f64() * 1_000.0);
        trace!(
            endpoint = %endpoint,
            sample_index,
            latency_ms = successful_samples.last().copied().unwrap_or_default(),
            "ws loaded latency sample"
        );
        sample_index += 1;
    }

    let _ = socket.close(None).await;
    Ok(successful_samples)
}

async fn stream_upload_stats_over_endpoint(
    endpoint: &Url,
    guid: &str,
    duration_seconds: u64,
    sample_interval_ms: u64,
    sender: &UnboundedSender<UploadStatsSample>,
) -> Result<()> {
    let mut request = endpoint
        .as_str()
        .into_client_request()
        .with_context(|| format!("failed building websocket request for {endpoint}"))?;

    let headers = request.headers_mut();
    headers.insert("Origin", "https://www.speedtest.net".parse()?);
    headers.insert("User-Agent", "Mozilla/5.0".parse()?);
    headers.insert("Accept", "*/*".parse()?);
    headers.insert("Accept-Language", "en-US,en;q=0.9".parse()?);
    headers.insert("Cache-Control", "no-cache".parse()?);
    headers.insert("Pragma", "no-cache".parse()?);

    let (mut socket, _) = timeout(WS_CONNECT_TIMEOUT, connect_async(request))
        .await
        .with_context(|| format!("timed out connecting websocket {endpoint}"))?
        .with_context(|| format!("failed to open websocket {endpoint}"))?;

    let upload_stats_duration_ms = duration_seconds.saturating_mul(1_000);
    ws_send_text(&mut socket, &format!("HI {guid}"), "HI upload stats").await?;
    ws_send_text(
        &mut socket,
        &format!("UPLOAD_STATS {upload_stats_duration_ms} {sample_interval_ms} 0"),
        "UPLOAD_STATS",
    )
    .await?;

    let deadline = Instant::now() + Duration::from_millis(upload_stats_duration_ms + 2_000);
    while Instant::now() < deadline {
        let Some(text) = ws_next_text(&mut socket, "UPLOAD_STATS frame").await? else {
            continue;
        };

        if text.starts_with("HELLO") || text.starts_with("CAPABILITIES") {
            continue;
        }

        if text.starts_with("ERROR") {
            bail!("server returned websocket upload stats error");
        }

        if let Some(sample) = parse_upload_stats_sample(&text) {
            debug!(
                endpoint = %endpoint,
                bytes = sample.bytes,
                elapsed_ms = sample.elapsed_ms,
                sample_index = sample.index.unwrap_or(0),
                "streamed upload stats sample"
            );
            let _ = sender.send(sample);
        }
    }

    let _ = socket.close(None).await;
    Ok(())
}

async fn ping_over_http_once(client: &Client, server: &SpeedtestServer, guid: &str) -> Result<f64> {
    let mut last_error = None;

    for mut url in endpoint_urls(server, "hello")? {
        let nonce = next_nocache_token();
        url.query_pairs_mut()
            .append_pair("nocache", &nonce)
            .append_pair("guid", guid);

        let start = std::time::Instant::now();
        let result = async {
            let response = browser_headers(client.get(url.clone()))
                .send()
                .await?
                .error_for_status()?;
            let _body = response.bytes().await?;
            Ok::<f64, anyhow::Error>(start.elapsed().as_secs_f64() * 1_000.0)
        }
        .await;

        match result {
            Ok(latency_ms) => return Ok(latency_ms),
            Err(error) => {
                debug!(server_id = server.id, endpoint = %url, error = %error, "hello endpoint failed");
                last_error = Some(error);
            }
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow!("no hello endpoint candidates available")))
}

fn websocket_endpoints(server: &SpeedtestServer) -> Result<Vec<Url>> {
    let host_with_port = host_with_port(server)?;
    let secure = Url::parse(&format!("wss://{host_with_port}/ws"))
        .with_context(|| format!("invalid websocket endpoint host '{host_with_port}'"))?;
    let insecure = Url::parse(&format!("ws://{host_with_port}/ws"))
        .with_context(|| format!("invalid websocket endpoint host '{host_with_port}'"))?;
    Ok(vec![secure, insecure])
}

async fn ws_send_text(
    socket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    text: &str,
    action: &str,
) -> Result<()> {
    timeout(
        WS_IO_TIMEOUT,
        socket.send(Message::Text(text.to_string().into())),
    )
    .await
    .with_context(|| format!("timed out sending websocket {action}"))?
    .with_context(|| format!("failed sending websocket {action}"))
}

async fn ws_expect_prefix(
    socket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    expected_prefix: &str,
    action: &str,
) -> Result<()> {
    loop {
        let frame = timeout(WS_IO_TIMEOUT, socket.next())
            .await
            .with_context(|| format!("timed out waiting for websocket {action}"))?
            .context("websocket stream closed")?
            .context("websocket frame error")?;

        match frame {
            Message::Text(text) => {
                let text = text.trim();
                if text.starts_with(expected_prefix) {
                    return Ok(());
                }
                bail!("unexpected websocket message '{text}' while waiting for {action}");
            }
            Message::Binary(_) | Message::Pong(_) => continue,
            Message::Ping(payload) => {
                timeout(WS_IO_TIMEOUT, socket.send(Message::Pong(payload)))
                    .await
                    .with_context(|| format!("timed out replying websocket PONG for {action}"))?
                    .with_context(|| format!("failed replying websocket PONG for {action}"))?;
            }
            Message::Close(_) => bail!("websocket closed while waiting for {action}"),
            Message::Frame(_) => continue,
        }
    }
}

async fn ws_next_text(
    socket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    action: &str,
) -> Result<Option<String>> {
    let frame = timeout(WS_IO_TIMEOUT, socket.next())
        .await
        .with_context(|| format!("timed out waiting for websocket {action}"))?;

    let Some(frame) = frame else {
        return Ok(None);
    };

    match frame.context("websocket frame error")? {
        Message::Text(text) => Ok(Some(text.trim().to_string())),
        Message::Binary(_) | Message::Pong(_) => Ok(None),
        Message::Ping(payload) => {
            timeout(WS_IO_TIMEOUT, socket.send(Message::Pong(payload)))
                .await
                .with_context(|| format!("timed out replying websocket PONG for {action}"))?
                .with_context(|| format!("failed replying websocket PONG for {action}"))?;
            Ok(None)
        }
        Message::Close(_) => Ok(None),
        Message::Frame(_) => Ok(None),
    }
}

fn parse_upload_stats_sample(text: &str) -> Option<UploadStatsSample> {
    let parsed = serde_json::from_str::<RawUploadStatsSample>(text).ok()?;
    if let Some(sample_type) = parsed.sample_type.as_deref()
        && sample_type != "u"
    {
        return None;
    }

    Some(UploadStatsSample {
        bytes: parsed.bytes,
        elapsed_ms: parsed.elapsed_ms,
        index: parsed.index,
    })
}

pub async fn download(
    client: &Client,
    server: &SpeedtestServer,
    _guid: &str,
    size: usize,
) -> std::result::Result<u64, TransferRequestError> {
    download_streaming(client, server, size, &[], None, None).await
}

pub async fn download_streaming(
    client: &Client,
    server: &SpeedtestServer,
    size: usize,
    live_counters: &[&AtomicU64],
    first_byte_at: Option<&OnceLock<Instant>>,
    first_transfer_tx: Option<&watch::Sender<bool>>,
) -> std::result::Result<u64, TransferRequestError> {
    let mut last_error = None;

    for mut url in
        endpoint_urls(server, "download").map_err(|_| TransferRequestError::InvalidEndpoint)?
    {
        url.query_pairs_mut().append_pair("size", &size.to_string());

        let response = match browser_headers(client.get(url.clone())).send().await {
            Ok(response) => response,
            Err(error) => {
                debug!(server_id = server.id, endpoint = %url, error = %error, "download endpoint transport failed");
                last_error = Some(TransferRequestError::Transport);
                continue;
            }
        };

        if !response.status().is_success() {
            let status = response.status();
            match response.text().await {
                Ok(body) => debug!(
                    server_id = server.id,
                    endpoint = %url,
                    status = %status,
                    response_body = %truncate_for_log(&body),
                    "download endpoint returned HTTP error"
                ),
                Err(error) => debug!(
                    server_id = server.id,
                    endpoint = %url,
                    status = %status,
                    error = %error,
                    "download endpoint returned HTTP error and response body unavailable"
                ),
            }
            last_error = Some(TransferRequestError::HttpStatus);
            continue;
        }

        let mut total = 0_u64;
        let mut stream = response.bytes_stream();
        let mut had_error = false;
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => {
                    let len = bytes.len() as u64;
                    if let Some(first_byte_at) = first_byte_at
                        && first_byte_at.set(Instant::now()).is_ok()
                        && let Some(first_transfer_tx) = first_transfer_tx
                    {
                        let _ = first_transfer_tx.send(true);
                    }
                    total += len;
                    for counter in live_counters {
                        counter.fetch_add(len, Ordering::Relaxed);
                    }
                }
                Err(error) => {
                    debug!(server_id = server.id, endpoint = %url, error = %error, "download stream read failed");
                    had_error = true;
                    break;
                }
            }
        }

        if had_error && total == 0 {
            last_error = Some(TransferRequestError::ResponseRead);
            continue;
        }

        return Ok(total);
    }

    Err(last_error.unwrap_or(TransferRequestError::InvalidEndpoint))
}

pub async fn upload(
    client: &Client,
    server: &SpeedtestServer,
    _guid: &str,
    payload: &[u8],
) -> std::result::Result<u64, TransferRequestError> {
    let body_len = payload.len() as u64;
    let mut last_error = None;

    for url in endpoint_urls(server, "upload").map_err(|_| TransferRequestError::InvalidEndpoint)? {
        let response = match browser_headers(client.post(url.clone()))
            .header("Content-Type", "application/octet-stream")
            .body(payload.to_vec())
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => {
                debug!(server_id = server.id, endpoint = %url, error = %error, "upload endpoint transport failed");
                last_error = Some(TransferRequestError::Transport);
                continue;
            }
        };

        if !response.status().is_success() {
            let status = response.status();
            match response.text().await {
                Ok(body) => debug!(
                    server_id = server.id,
                    endpoint = %url,
                    status = %status,
                    response_body = %truncate_for_log(&body),
                    "upload endpoint returned HTTP error"
                ),
                Err(error) => debug!(
                    server_id = server.id,
                    endpoint = %url,
                    status = %status,
                    error = %error,
                    "upload endpoint returned HTTP error and response body unavailable"
                ),
            }
            last_error = Some(TransferRequestError::HttpStatus);
            continue;
        }

        match response.bytes().await {
            Ok(_) => return Ok(body_len),
            Err(error) => {
                debug!(server_id = server.id, endpoint = %url, error = %error, "upload response read failed");
                last_error = Some(TransferRequestError::ResponseRead);
            }
        }
    }

    Err(last_error.unwrap_or(TransferRequestError::InvalidEndpoint))
}

fn truncate_for_log(body: &str) -> String {
    const MAX_LEN: usize = 1024;
    if body.len() <= MAX_LEN {
        return body.to_string();
    }

    let mut cutoff = MAX_LEN;
    while !body.is_char_boundary(cutoff) {
        cutoff = cutoff.saturating_sub(1);
    }

    let mut text = body[..cutoff].to_string();
    text.push_str("...");
    text
}

fn endpoint_urls(server: &SpeedtestServer, path: &str) -> Result<Vec<Url>> {
    let host_with_port = host_with_port(server)?;
    let path_candidates = endpoint_path_candidates(server, path);

    let mut endpoints = Vec::new();
    let mut seen = HashSet::new();
    for scheme in ["https", "http"] {
        let base = Url::parse(&format!("{scheme}://{host_with_port}/")).with_context(|| {
            format!(
                "invalid speedtest browser endpoint host '{}'",
                host_with_port
            )
        })?;

        for candidate in &path_candidates {
            let mut url = base.clone();
            {
                let mut segments = url
                    .path_segments_mut()
                    .map_err(|_| anyhow!("endpoint URL is not hierarchical"))?;
                segments.clear();
                for segment in candidate.split('/').filter(|segment| !segment.is_empty()) {
                    segments.push(segment);
                }
            }

            if seen.insert(url.to_string()) {
                endpoints.push(url);
            }
        }
    }

    Ok(endpoints)
}

fn endpoint_path_candidates(server: &SpeedtestServer, path: &str) -> Vec<String> {
    let mut out = vec![path.to_string()];

    if let Ok(parsed) = Url::parse(&server.url)
        && let Some(segments) = parsed.path_segments()
    {
        let parsed_segments = segments
            .filter(|segment| !segment.is_empty())
            .collect::<Vec<_>>();

        if !parsed_segments.is_empty() {
            let mut parent = parsed_segments.clone();
            parent.pop();
            let parent_prefix = if parent.is_empty() {
                String::new()
            } else {
                format!("{}/", parent.join("/"))
            };

            out.push(format!("{parent_prefix}{path}"));
            out.push(format!("{parent_prefix}{path}.php"));

            if path == "upload" {
                out.push(parsed_segments.join("/"));
                out.push(format!("{parent_prefix}upload.php"));
            }
        }
    }

    let mut deduped = Vec::new();
    let mut seen = HashSet::new();
    for candidate in out {
        if seen.insert(candidate.clone()) {
            deduped.push(candidate);
        }
    }
    deduped
}

fn browser_headers(builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    builder
        .header("Origin", "https://www.speedtest.net")
        .header("Referer", "https://www.speedtest.net/")
        .header("User-Agent", "Mozilla/5.0")
        .header("Accept", "*/*")
        .header("Accept-Language", "en-US,en;q=0.9")
        .header("Cache-Control", "no-cache")
        .header("Pragma", "no-cache")
}

fn host_with_port(server: &SpeedtestServer) -> Result<String> {
    if server.host.trim().is_empty() {
        bail!("speedtest server host is empty");
    }

    if looks_like_host_with_port(&server.host) {
        Ok(server.host.clone())
    } else {
        let parsed = Url::parse(&server.url)
            .with_context(|| format!("invalid speedtest server URL '{}'", server.url))?;
        let host = parsed
            .host_str()
            .context("speedtest server URL is missing host")?;
        let port = parsed
            .port_or_known_default()
            .context("speedtest server URL is missing resolvable port")?;
        Ok(format!("{host}:{port}"))
    }
}

pub(crate) fn looks_like_host_with_port(host: &str) -> bool {
    if host.starts_with('[') && host.contains(":") && host.contains("]:") {
        return true;
    }
    host.rsplit_once(':')
        .and_then(|(_, port)| port.parse::<u16>().ok())
        .is_some()
}

fn next_nocache_token() -> String {
    let counter = NONCE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    format!("{now:x}-{counter:x}")
}

#[cfg(test)]
mod tests {
    use super::{endpoint_urls, websocket_endpoints};
    use crate::speedtest::servers::SpeedtestServer;

    #[test]
    fn builds_browser_endpoint() {
        let server = SpeedtestServer {
            id: 1,
            sponsor: "s".to_string(),
            name: "n".to_string(),
            country: "c".to_string(),
            host: "example.net:8080".to_string(),
            distance_km: 1.0,
            url: "https://example.net/speedtest/upload.php".to_string(),
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
        };

        let endpoints = endpoint_urls(&server, "hello").expect("must build endpoint");
        let endpoint_set = endpoints
            .iter()
            .map(url::Url::as_str)
            .collect::<std::collections::HashSet<_>>();
        assert!(endpoint_set.contains("https://example.net:8080/hello"));
        assert!(endpoint_set.contains("http://example.net:8080/hello"));
        assert!(endpoint_set.contains("https://example.net:8080/speedtest/hello"));

        let ws = websocket_endpoints(&server).expect("must build websocket endpoints");
        assert_eq!(ws[0].as_str(), "wss://example.net:8080/ws");
    }
}
