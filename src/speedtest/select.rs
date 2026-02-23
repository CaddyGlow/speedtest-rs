use std::cmp::Ordering;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use reqwest::Client;
use tokio::task::JoinSet;
use tracing::{debug, warn};

use crate::speedtest::api::ResolvedSpeedtestApi;
use crate::speedtest::browser_protocol;
use crate::speedtest::modern_protocol;
use crate::speedtest::servers::SpeedtestServer;

#[derive(Debug, Clone)]
pub struct ServerLatency {
    pub server: SpeedtestServer,
    pub average_ms: f64,
    pub variance_ms: f64,
    pub samples_ms: Vec<f64>,
}

#[derive(Debug, Clone)]
pub struct LatencyMeasurement {
    pub average_ms: f64,
    pub variance_ms: f64,
    pub samples_ms: Vec<f64>,
}

pub fn select_nearest(servers: &[SpeedtestServer], count: usize) -> Vec<SpeedtestServer> {
    let mut out = servers.to_vec();
    out.sort_by(|a, b| {
        a.distance_km
            .partial_cmp(&b.distance_km)
            .unwrap_or(Ordering::Equal)
    });
    out.truncate(count);
    out
}

pub async fn probe_and_rank_candidates_with_progress<F>(
    client: &Client,
    servers: &[SpeedtestServer],
    api: ResolvedSpeedtestApi,
    candidate_count: usize,
    latency_samples: usize,
    mut on_probe: F,
) -> Result<Vec<ServerLatency>>
where
    F: FnMut(usize, usize, &SpeedtestServer, Option<LatencyMeasurement>, Option<String>),
{
    if latency_samples == 0 {
        bail!("latency_samples must be greater than zero");
    }

    let candidates = select_nearest(servers, candidate_count.max(1));
    if candidates.is_empty() {
        bail!("no speedtest servers available for latency probing");
    }

    let mut scored = Vec::new();
    let total_candidates = candidates.len();
    for (index, server) in candidates.into_iter().enumerate() {
        match probe_server_latency_detailed(client, &server, latency_samples, api).await {
            Ok(measurement) => {
                on_probe(
                    index + 1,
                    total_candidates,
                    &server,
                    Some(measurement.clone()),
                    None,
                );
                scored.push(ServerLatency {
                    server,
                    average_ms: measurement.average_ms,
                    variance_ms: measurement.variance_ms,
                    samples_ms: measurement.samples_ms,
                });
            }
            Err(error) => {
                on_probe(
                    index + 1,
                    total_candidates,
                    &server,
                    None,
                    Some(error.to_string()),
                );
            }
        }
    }

    if scored.is_empty() {
        bail!("all candidate server latency probes failed; try a different proxy or network");
    }

    scored.sort_by(|a, b| compare_latency(a, b).unwrap_or(Ordering::Equal));
    Ok(scored)
}

pub async fn probe_server_latency(
    client: &Client,
    server: &SpeedtestServer,
    samples: usize,
    api: ResolvedSpeedtestApi,
) -> Result<LatencyMeasurement> {
    probe_server_latency_detailed(client, server, samples, api).await
}

pub async fn collect_loaded_latency_samples(
    client: &Client,
    server: &SpeedtestServer,
    api: ResolvedSpeedtestApi,
    stage_seconds: u64,
) -> Vec<f64> {
    let duration = Duration::from_secs(stage_seconds.max(1));

    let outcome = match api {
        ResolvedSpeedtestApi::Legacy => {
            collect_latency_legacy_for_duration(client, server, duration).await
        }
        ResolvedSpeedtestApi::Modern => {
            let mut samples = collect_latency_modern_ws_for_duration(server, duration).await;

            if samples.is_empty() {
                let guid = server.session_guid.as_deref().unwrap_or("tunmux-speedtest");
                let fallback_samples = (duration.as_millis() / 100).max(10) as usize;
                if let Ok(mut fallback) = browser_protocol::probe_latency_samples_http(
                    client,
                    server,
                    guid,
                    fallback_samples,
                )
                .await
                {
                    samples.append(&mut fallback);
                }
            }

            Ok(samples)
        }
        ResolvedSpeedtestApi::ModernTcp => {
            collect_latency_modern_tcp_for_duration(server, duration).await
        }
    };

    match outcome {
        Ok(samples) => samples,
        Err(error) => {
            debug!(
                server_id = server.id,
                host = %server.host,
                stage_seconds,
                error = %error,
                "loaded latency sampling failed"
            );
            Vec::new()
        }
    }
}

async fn collect_latency_modern_ws_for_duration(
    server: &SpeedtestServer,
    duration: Duration,
) -> Vec<f64> {
    const LOADED_LATENCY_WORKERS: usize = 6;

    let mut tasks = JoinSet::new();
    for _ in 0..LOADED_LATENCY_WORKERS {
        let worker_server = server.clone();
        tasks.spawn(async move {
            browser_protocol::probe_latency_samples_websocket_for_duration(&worker_server, duration)
                .await
        });
    }

    let mut samples = Vec::new();
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(mut worker_samples)) => samples.append(&mut worker_samples),
            Ok(Err(error)) => {
                debug!(
                    server_id = server.id,
                    host = %server.host,
                    error = %error,
                    "loaded latency websocket worker failed"
                );
            }
            Err(error) => {
                debug!(
                    server_id = server.id,
                    host = %server.host,
                    error = %error,
                    "loaded latency websocket worker task failed"
                );
            }
        }
    }

    samples
}

async fn collect_latency_legacy_for_duration(
    client: &Client,
    server: &SpeedtestServer,
    duration: Duration,
) -> Result<Vec<f64>> {
    let endpoint = server.latency_url()?;
    let deadline = Instant::now() + duration;
    let mut samples = Vec::new();

    while Instant::now() < deadline {
        let start = Instant::now();
        if let Ok(response) = client.get(&endpoint).send().await
            && let Ok(response) = response.error_for_status()
            && response.bytes().await.is_ok()
        {
            samples.push(start.elapsed().as_secs_f64() * 1_000.0);
        }
    }

    Ok(samples)
}

async fn collect_latency_modern_tcp_for_duration(
    server: &SpeedtestServer,
    duration: Duration,
) -> Result<Vec<f64>> {
    let deadline = Instant::now() + duration;
    let mut stream = modern_protocol::connect(server).await?;
    let mut samples = Vec::new();

    while Instant::now() < deadline {
        if let Ok(elapsed_ms) = modern_protocol::ping(&mut stream).await {
            samples.push(elapsed_ms);
        }
    }

    let _ = modern_protocol::quit(&mut stream).await;
    Ok(samples)
}

async fn probe_server_latency_detailed(
    client: &Client,
    server: &SpeedtestServer,
    samples: usize,
    api: ResolvedSpeedtestApi,
) -> Result<LatencyMeasurement> {
    if samples == 0 {
        bail!("latency_samples must be greater than zero");
    }

    match api {
        ResolvedSpeedtestApi::Legacy => probe_server_latency_legacy(client, server, samples).await,
        ResolvedSpeedtestApi::Modern => {
            probe_server_latency_modern_sdk(client, server, samples).await
        }
        ResolvedSpeedtestApi::ModernTcp => probe_server_latency_modern_tcp(server, samples).await,
    }
}

async fn probe_server_latency_legacy(
    client: &Client,
    server: &SpeedtestServer,
    samples: usize,
) -> Result<LatencyMeasurement> {
    let endpoint = server.latency_url()?;
    let mut successful_samples = Vec::with_capacity(samples);

    for _ in 0..samples {
        let start = Instant::now();
        let response = client.get(&endpoint).send().await;
        if let Ok(response) = response
            && let Ok(response) = response.error_for_status()
            && response.bytes().await.is_ok()
        {
            let elapsed_ms = start.elapsed().as_secs_f64() * 1_000.0;
            successful_samples.push(elapsed_ms);
        }
    }

    if successful_samples.len() * 2 < samples {
        bail!(
            "insufficient latency samples for server id={} ({} of {})",
            server.id,
            successful_samples.len(),
            samples
        );
    }

    let (average_ms, variance_ms) = compute_latency_stats(&successful_samples)?;
    Ok(LatencyMeasurement {
        average_ms,
        variance_ms,
        samples_ms: successful_samples,
    })
}

async fn probe_server_latency_modern_sdk(
    client: &Client,
    server: &SpeedtestServer,
    samples: usize,
) -> Result<LatencyMeasurement> {
    let mut successful_samples =
        match browser_protocol::probe_latency_samples_websocket(server, samples).await {
            Ok(samples_ms) => samples_ms,
            Err(error) => {
                warn!(
                    server_id = server.id,
                    host = %server.host,
                    error = %error,
                    "websocket latency probing failed; falling back to HTTP hello latency"
                );
                Vec::new()
            }
        };

    if successful_samples.len() < samples {
        let fallback_count = samples - successful_samples.len();
        let guid = server.session_guid.as_deref().unwrap_or("tunmux-speedtest");
        debug!(
            server_id = server.id,
            host = %server.host,
            guid = %guid,
            fallback_count,
            "starting HTTP fallback latency samples"
        );
        let mut fallback_samples =
            browser_protocol::probe_latency_samples_http(client, server, guid, fallback_count)
                .await
                .with_context(|| {
                    format!(
                        "HTTP fallback latency probing failed for server id={}",
                        server.id
                    )
                })?;
        successful_samples.append(&mut fallback_samples);
    }

    debug!(
        server_id = server.id,
        host = %server.host,
        requested_samples = samples,
        successful_samples = successful_samples.len(),
        "modern latency probing completed"
    );

    if successful_samples.len() * 2 < samples {
        bail!(
            "insufficient latency samples for server id={} ({} of {})",
            server.id,
            successful_samples.len(),
            samples
        );
    }

    let (average_ms, variance_ms) = compute_latency_stats(&successful_samples)?;
    Ok(LatencyMeasurement {
        average_ms,
        variance_ms,
        samples_ms: successful_samples,
    })
}

async fn probe_server_latency_modern_tcp(
    server: &SpeedtestServer,
    samples: usize,
) -> Result<LatencyMeasurement> {
    let mut stream = modern_protocol::connect(server).await?;
    let mut successful_samples = Vec::with_capacity(samples);

    for _ in 0..samples {
        if let Ok(elapsed_ms) = modern_protocol::ping(&mut stream).await {
            successful_samples.push(elapsed_ms);
        }
    }

    let _ = modern_protocol::quit(&mut stream).await;

    if successful_samples.len() * 2 < samples {
        bail!(
            "insufficient latency samples for server id={} ({} of {})",
            server.id,
            successful_samples.len(),
            samples
        );
    }

    let (average_ms, variance_ms) = compute_latency_stats(&successful_samples)?;
    Ok(LatencyMeasurement {
        average_ms,
        variance_ms,
        samples_ms: successful_samples,
    })
}

fn compute_latency_stats(samples: &[f64]) -> Result<(f64, f64)> {
    if samples.is_empty() {
        bail!("latency sampling produced no successful measurements");
    }

    let average = samples.iter().sum::<f64>() / samples.len() as f64;
    let variance = samples
        .iter()
        .map(|sample| {
            let delta = sample - average;
            delta * delta
        })
        .sum::<f64>()
        / samples.len() as f64;

    Ok((average, variance))
}

pub fn select_best_latency(scored: &[ServerLatency]) -> Option<ServerLatency> {
    let mut ranked = scored.to_vec();
    ranked.sort_by(|a, b| compare_latency(a, b).unwrap_or(Ordering::Equal));
    ranked.into_iter().next()
}

fn compare_latency(a: &ServerLatency, b: &ServerLatency) -> Option<Ordering> {
    a.average_ms
        .partial_cmp(&b.average_ms)
        .and_then(|ordering| match ordering {
            Ordering::Equal => {
                a.variance_ms
                    .partial_cmp(&b.variance_ms)
                    .and_then(|variance_ordering| match variance_ordering {
                        Ordering::Equal => a.server.distance_km.partial_cmp(&b.server.distance_km),
                        _ => Some(variance_ordering),
                    })
            }
            _ => Some(ordering),
        })
}

#[cfg(test)]
mod tests {
    use super::{ServerLatency, select_best_latency, select_nearest};
    use crate::speedtest::servers::SpeedtestServer;

    #[test]
    fn nearest_servers_are_ranked_by_distance() {
        let servers = vec![
            server(1, 30.0),
            server(2, 10.0),
            server(3, 20.0),
            server(4, 5.0),
        ];

        let nearest = select_nearest(&servers, 2);
        assert_eq!(nearest.len(), 2);
        assert_eq!(nearest[0].id, 4);
        assert_eq!(nearest[1].id, 2);
    }

    #[test]
    fn best_latency_prefers_average_then_variance_then_distance() {
        let ranked = vec![
            ServerLatency {
                server: server(1, 10.0),
                average_ms: 20.0,
                variance_ms: 5.0,
                samples_ms: Vec::new(),
            },
            ServerLatency {
                server: server(2, 30.0),
                average_ms: 20.0,
                variance_ms: 3.0,
                samples_ms: Vec::new(),
            },
            ServerLatency {
                server: server(3, 5.0),
                average_ms: 22.0,
                variance_ms: 1.0,
                samples_ms: Vec::new(),
            },
            ServerLatency {
                server: server(4, 15.0),
                average_ms: 20.0,
                variance_ms: 3.0,
                samples_ms: Vec::new(),
            },
        ];

        let best = select_best_latency(&ranked).expect("should pick a server");
        assert_eq!(best.server.id, 4);
    }

    fn server(id: u64, distance_km: f64) -> SpeedtestServer {
        SpeedtestServer {
            id,
            sponsor: "s".to_string(),
            name: "n".to_string(),
            country: "c".to_string(),
            host: "h".to_string(),
            distance_km,
            url: "https://example.com/speedtest/upload.php".to_string(),
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
}
