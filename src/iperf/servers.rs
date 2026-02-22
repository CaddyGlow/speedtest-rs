use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout};

use crate::cli::IperfProtocol;
use crate::iperf::proxy::{self, ProxySpec};

#[derive(Debug, Clone)]
pub struct IperfServerCandidate {
    pub host: String,
    pub port: u16,
    pub region: Option<String>,
    pub localization: Option<String>,
}

#[derive(Debug, Clone)]
pub struct IperfSelectedServer {
    pub host: String,
    pub port: u16,
    pub average_latency_ms: f64,
    pub region: Option<String>,
    pub localization: Option<String>,
}

pub fn load_candidates(
    file_path: &str,
    protocol: IperfProtocol,
    port_override: Option<u16>,
) -> Result<Vec<IperfServerCandidate>> {
    let path = Path::new(file_path);
    let body = std::fs::read_to_string(path)
        .with_context(|| format!("failed reading iperf server list: {}", path.display()))?;
    let list: ServerList = serde_json::from_str(&body)
        .with_context(|| format!("invalid JSON in {}", path.display()))?;

    let mut candidates = Vec::new();
    for item in list.servers {
        if !is_status_ok(item.status.as_deref()) {
            continue;
        }
        if !item.port.supports(protocol) {
            continue;
        }

        let port = port_override.unwrap_or_else(|| item.port.default_port());
        candidates.push(IperfServerCandidate {
            host: item.host,
            port,
            region: item.region,
            localization: item.localization,
        });
    }

    if candidates.is_empty() {
        bail!(
            "no usable iperf servers found in '{}' for protocol {:?}",
            file_path,
            protocol
        );
    }

    Ok(candidates)
}

pub async fn select_best_server(
    candidates: &[IperfServerCandidate],
    latency_samples: usize,
    proxy: Option<&ProxySpec>,
) -> Result<IperfSelectedServer> {
    if candidates.is_empty() {
        bail!("cannot select iperf server from an empty candidate list");
    }

    let samples = latency_samples.max(1);
    let mut joins = JoinSet::new();
    for candidate in candidates.iter().cloned() {
        let proxy = proxy.cloned();
        joins.spawn(async move {
            let latency = probe_candidate_latency(&candidate, samples, proxy.as_ref()).await;
            (candidate, latency)
        });
    }

    let mut measured = Vec::new();
    while let Some(joined) = joins.join_next().await {
        let (candidate, latency) = joined.context("iperf latency probe task failed")?;
        if let Some(average_latency_ms) = latency {
            measured.push(IperfSelectedServer {
                host: candidate.host,
                port: candidate.port,
                average_latency_ms,
                region: candidate.region,
                localization: candidate.localization,
            });
        }
    }

    if measured.is_empty() {
        bail!("all candidate iperf servers failed latency probe");
    }

    measured.sort_by(|a, b| a.average_latency_ms.total_cmp(&b.average_latency_ms));
    Ok(measured.remove(0))
}

async fn probe_candidate_latency(
    candidate: &IperfServerCandidate,
    samples: usize,
    proxy: Option<&ProxySpec>,
) -> Option<f64> {
    let mut success_count = 0usize;
    let mut sum_ms = 0.0;

    for _ in 0..samples {
        let start = Instant::now();
        let attempt = timeout(
            Duration::from_secs(2),
            proxy::connect_tcp_target(&candidate.host, candidate.port, proxy),
        )
        .await;

        if let Ok(Ok(_)) = attempt {
            success_count += 1;
            sum_ms += start.elapsed().as_secs_f64() * 1_000.0;
        }

        sleep(Duration::from_millis(40)).await;
    }

    if success_count == 0 {
        None
    } else {
        Some(sum_ms / success_count as f64)
    }
}

fn is_status_ok(status: Option<&str>) -> bool {
    status
        .map(str::trim)
        .map(|value| value.eq_ignore_ascii_case("ok"))
        .unwrap_or(false)
}

#[derive(Debug, Deserialize)]
struct ServerList {
    servers: Vec<ServerEntry>,
}

#[derive(Debug, Deserialize)]
struct ServerEntry {
    region: Option<String>,
    host: String,
    localization: Option<String>,
    status: Option<String>,
    port: PortDescriptor,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum PortDescriptor {
    Single {
        port: u16,
        protocol: String,
    },
    Range {
        from: u16,
        to: u16,
        protocol: String,
    },
}

impl PortDescriptor {
    fn supports(&self, protocol: IperfProtocol) -> bool {
        let support = match self {
            Self::Single { protocol, .. } => protocol,
            Self::Range { protocol, .. } => protocol,
        }
        .to_ascii_uppercase();

        match protocol {
            IperfProtocol::Tcp => support.contains("TCP"),
            IperfProtocol::Udp => support.contains("UDP"),
        }
    }

    fn default_port(&self) -> u16 {
        match self {
            Self::Single { port, .. } => *port,
            Self::Range { from, to, .. } => {
                if *from <= 5201 && 5201 <= *to {
                    5201
                } else {
                    *from
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::load_candidates;
    use crate::cli::IperfProtocol;

    #[test]
    fn parses_and_filters_candidates() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should work")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("tunmux-iperf-test-{unique}.json"));
        std::fs::write(
            &path,
            r#"{
                "servers": [
                  {"host":"a.example","status":"OK","region":"x","localization":"loc","port":{"from":9200,"to":9240,"protocol":"TCP/UDP"}},
                  {"host":"b.example","status":"OOS","region":"x","localization":"loc","port":{"port":5001,"protocol":"TCP/UDP"}},
                  {"host":"c.example","status":"OK","region":"x","localization":"loc","port":{"port":5002,"protocol":"TCP"}}
                ]
              }"#,
        )
        .expect("test JSON should be written");

        let tcp = load_candidates(path.to_str().expect("utf8 path"), IperfProtocol::Tcp, None)
            .expect("tcp candidates should parse");
        assert_eq!(tcp.len(), 2);

        let udp = load_candidates(path.to_str().expect("utf8 path"), IperfProtocol::Udp, None)
            .expect("udp candidates should parse");
        assert_eq!(udp.len(), 1);
        assert_eq!(udp[0].host, "a.example");
        assert_eq!(udp[0].port, 9200);

        let _ = std::fs::remove_file(path);
    }
}
