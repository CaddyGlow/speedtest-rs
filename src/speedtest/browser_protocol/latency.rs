use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use reqwest::Client;
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, trace};
use url::Url;

use crate::speedtest::servers::SpeedtestServer;

use super::endpoints::{browser_headers, endpoint_urls, websocket_endpoints};
use super::websocket::{
    connect_browser_websocket, perform_speedtest_ws_handshake, ws_expect_prefix, ws_next_text,
    ws_send_text,
};
use super::{RawUploadStatsSample, UploadStatsSample, next_nocache_token};

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

pub async fn probe_latency_samples_websocket_for_duration_with_sender(
    server: &SpeedtestServer,
    duration: Duration,
    sender: Option<UnboundedSender<f64>>,
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
        match probe_latency_samples_over_websocket_endpoint_for_duration(
            &endpoint,
            target_duration,
            sender.clone(),
        )
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
    let mut socket = connect_browser_websocket(endpoint).await?;

    debug!(endpoint = %endpoint, "websocket connected");
    perform_speedtest_ws_handshake(&mut socket).await?;

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
    debug!(
        endpoint = %endpoint,
        successful_samples = successful_samples.len(),
        "websocket closed"
    );
    Ok(successful_samples)
}

async fn probe_latency_samples_over_websocket_endpoint_for_duration(
    endpoint: &Url,
    duration: Duration,
    sender: Option<UnboundedSender<f64>>,
) -> Result<Vec<f64>> {
    let mut socket = connect_browser_websocket(endpoint).await?;
    perform_speedtest_ws_handshake(&mut socket).await?;

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
        if let Some(sender) = sender.as_ref() {
            let _ = sender.send(*successful_samples.last().unwrap_or(&0.0));
        }
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
    let mut socket = connect_browser_websocket(endpoint).await?;

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

        let start = Instant::now();
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
