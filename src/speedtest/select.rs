use std::cmp::Ordering;
use std::time::Instant;

use anyhow::{Result, bail};
use reqwest::Client;

use crate::speedtest::servers::SpeedtestServer;

#[derive(Debug, Clone)]
pub struct ServerLatency {
    pub server: SpeedtestServer,
    pub average_ms: f64,
    pub variance_ms: f64,
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

pub async fn probe_and_select_best(
    client: &Client,
    servers: &[SpeedtestServer],
    candidate_count: usize,
    latency_samples: usize,
) -> Result<ServerLatency> {
    if latency_samples == 0 {
        bail!("latency_samples must be greater than zero");
    }

    let candidates = select_nearest(servers, candidate_count.max(1));
    if candidates.is_empty() {
        bail!("no speedtest servers available for latency probing");
    }

    let mut scored = Vec::new();
    for server in candidates {
        if let Ok((average_ms, variance_ms)) =
            probe_server_latency(client, &server, latency_samples).await
        {
            scored.push(ServerLatency {
                server,
                average_ms,
                variance_ms,
            });
        }
    }

    select_best_latency(&scored).ok_or_else(|| {
        anyhow::anyhow!(
            "all candidate server latency probes failed; try a different proxy or network"
        )
    })
}

pub async fn probe_server_latency(
    client: &Client,
    server: &SpeedtestServer,
    samples: usize,
) -> Result<(f64, f64)> {
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

    let average = successful_samples.iter().sum::<f64>() / successful_samples.len() as f64;
    let variance = successful_samples
        .iter()
        .map(|sample| {
            let delta = sample - average;
            delta * delta
        })
        .sum::<f64>()
        / successful_samples.len() as f64;

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
            },
            ServerLatency {
                server: server(2, 30.0),
                average_ms: 20.0,
                variance_ms: 3.0,
            },
            ServerLatency {
                server: server(3, 5.0),
                average_ms: 22.0,
                variance_ms: 1.0,
            },
            ServerLatency {
                server: server(4, 15.0),
                average_ms: 20.0,
                variance_ms: 3.0,
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
        }
    }
}
