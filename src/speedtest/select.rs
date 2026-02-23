use std::cmp::Ordering;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use reqwest::Client;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{debug, warn};

use crate::speedtest::browser_protocol;
use crate::speedtest::servers::SpeedtestServer;

const LATENCY_PROBE_WORKERS: usize = 8;
#[allow(dead_code)]
const LOADED_LATENCY_WORKERS: usize = 8;
#[allow(dead_code)]
const LOADED_LATENCY_MAX_SAMPLES_PER_SEC: usize = 25;
#[allow(dead_code)]
const LOADED_LATENCY_OUTLIER_LOWER_QUANTILE: f64 = 0.01;
#[allow(dead_code)]
const LOADED_LATENCY_OUTLIER_UPPER_QUANTILE: f64 = 0.99;

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

    let total_candidates = candidates.len();
    let semaphore = Arc::new(Semaphore::new(
        LATENCY_PROBE_WORKERS.min(total_candidates).max(1),
    ));
    let mut tasks = JoinSet::new();

    for (index, server) in candidates.into_iter().enumerate() {
        let probe_client = client.clone();
        let probe_server = server.clone();
        let semaphore = Arc::clone(&semaphore);
        tasks.spawn(async move {
            let permit = semaphore
                .acquire_owned()
                .await
                .context("latency probe worker semaphore closed")?;
            let result =
                probe_server_latency_detailed(&probe_client, &probe_server, latency_samples).await;
            drop(permit);
            Ok::<_, anyhow::Error>((index, probe_server, result))
        });
    }

    let mut scored = Vec::new();
    while let Some(joined) = tasks.join_next().await {
        let (index, server, outcome) = joined.context("latency probe worker task failed")??;
        match outcome {
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
) -> Result<LatencyMeasurement> {
    probe_server_latency_detailed(client, server, samples).await
}

#[allow(dead_code)]
pub async fn collect_loaded_latency_samples(
    client: &Client,
    server: &SpeedtestServer,
    stage_seconds: u64,
) -> Vec<f64> {
    let duration = Duration::from_secs(stage_seconds.max(1));
    let mut samples = collect_latency_modern_ws_for_duration(server, duration).await;
    if samples.is_empty() {
        let guid = server.session_guid.as_deref().unwrap_or("tunmux-speedtest");
        let fallback_samples = (duration.as_millis() / 100).max(10) as usize;
        if let Ok(mut fallback) =
            browser_protocol::probe_latency_samples_http(client, server, guid, fallback_samples)
                .await
        {
            samples.append(&mut fallback);
        }
    }
    let raw_samples = samples.len();
    let normalized = normalize_loaded_latency_samples(samples, stage_seconds);
    debug!(
        server_id = server.id,
        host = %server.host,
        stage_seconds,
        raw_samples,
        normalized_samples = normalized.len(),
        "loaded latency samples normalized"
    );
    normalized
}

#[allow(dead_code)]
async fn collect_latency_modern_ws_for_duration(
    server: &SpeedtestServer,
    duration: Duration,
) -> Vec<f64> {
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

#[allow(dead_code)]
fn normalize_loaded_latency_samples(mut samples: Vec<f64>, stage_seconds: u64) -> Vec<f64> {
    samples.retain(|sample| sample.is_finite() && *sample >= 0.0 && *sample <= 10_000.0);
    if samples.is_empty() {
        return samples;
    }

    if samples.len() >= 20 {
        let mut sorted = samples.clone();
        sorted.sort_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal));

        let last_index = sorted.len() - 1;
        let lower_index = ((last_index as f64) * LOADED_LATENCY_OUTLIER_LOWER_QUANTILE).floor();
        let upper_index = ((last_index as f64) * LOADED_LATENCY_OUTLIER_UPPER_QUANTILE).ceil();
        let lower_bound = sorted[lower_index as usize];
        let upper_bound = sorted[upper_index as usize];

        samples.retain(|sample| *sample >= lower_bound && *sample <= upper_bound);
    }

    let max_samples = stage_seconds.max(1) as usize * LOADED_LATENCY_MAX_SAMPLES_PER_SEC;
    if samples.len() <= max_samples {
        return samples;
    }

    let stride = samples.len() as f64 / max_samples as f64;
    let mut downsampled = Vec::with_capacity(max_samples);
    let mut cursor = 0.0_f64;

    while downsampled.len() < max_samples {
        let index = cursor.floor() as usize;
        downsampled.push(samples[index.min(samples.len() - 1)]);
        cursor += stride;
    }

    downsampled
}

async fn probe_server_latency_detailed(
    client: &Client,
    server: &SpeedtestServer,
    samples: usize,
) -> Result<LatencyMeasurement> {
    if samples == 0 {
        bail!("latency_samples must be greater than zero");
    }

    probe_server_latency_modern_sdk(client, server, samples).await
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
    use super::{
        ServerLatency, normalize_loaded_latency_samples, select_best_latency, select_nearest,
    };
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

    #[test]
    fn loaded_latency_normalization_filters_and_caps_samples() {
        let mut samples = vec![f64::NAN, -1.0, 0.0, 1.0, 2.0, 3.0, 1000.0, 10000.1];
        samples.extend((0..2_000).map(|value| 2.0 + (value % 5) as f64 * 0.01));

        let normalized = normalize_loaded_latency_samples(samples, 10);

        assert!(!normalized.is_empty());
        assert!(normalized.len() <= 250);
        assert!(normalized.iter().all(|sample| sample.is_finite()));
        assert!(normalized.iter().all(|sample| *sample >= 0.0));
        assert!(normalized.iter().all(|sample| *sample <= 10_000.0));
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
