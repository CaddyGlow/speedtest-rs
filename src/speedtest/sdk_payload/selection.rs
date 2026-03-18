use serde::Serialize;

use crate::model::{ClientMeta, RunResult, Server};

#[derive(Debug, Serialize)]
pub(super) struct SdkServerSelection {
    #[serde(rename = "closestPingDetails")]
    pub(super) closest_ping_details: Vec<SdkClosestPingDetail>,
}

#[derive(Debug, Serialize)]
pub(super) struct SdkClosestPingDetail {
    pub(super) id: u64,
    pub(super) name: String,
    pub(super) sponsor: String,
    pub(super) host: String,
    pub(super) distance: f64,
    pub(super) ping: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) jitter: Option<f64>,
}

#[derive(Debug, Serialize)]
pub(super) struct SdkServerListEntry {
    pub(super) url: String,
    pub(super) lat: String,
    pub(super) lon: String,
    pub(super) distance: u64,
    pub(super) name: String,
    pub(super) country: String,
    pub(super) cc: String,
    pub(super) sponsor: String,
    pub(super) id: u64,
    pub(super) preferred: u8,
    #[serde(rename = "isp_id")]
    pub(super) isp_id: String,
    #[serde(rename = "httpsFunctional")]
    pub(super) https_functional: u8,
    pub(super) host: String,
    pub(super) hostname: String,
    pub(super) port: u16,
    #[serde(rename = "force_ping_select", skip_serializing_if = "Option::is_none")]
    pub(super) force_ping_select: Option<u8>,
}

pub(super) fn build_server_selection(
    result: &RunResult,
    selected: &Server,
    fallback_ping: f64,
) -> Option<SdkServerSelection> {
    let source_servers = result
        .server_pool
        .as_ref()
        .filter(|servers| !servers.is_empty())
        .cloned()
        .unwrap_or_else(|| vec![selected.clone()]);

    let mut details = source_servers
        .into_iter()
        .filter_map(|server| {
            let ping = server.latency_ms.unwrap_or(fallback_ping);
            if !ping.is_finite() || ping < 0.0 {
                return None;
            }
            Some(SdkClosestPingDetail {
                id: server.id,
                name: server.name,
                sponsor: server.sponsor,
                host: server.host,
                distance: server.distance_km,
                ping,
                jitter: server.latency_stddev_ms,
            })
        })
        .collect::<Vec<_>>();

    details.sort_by(|left, right| {
        left.ping
            .partial_cmp(&right.ping)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    if details.is_empty() {
        None
    } else {
        Some(SdkServerSelection {
            closest_ping_details: details,
        })
    }
}

pub(super) fn parse_host_and_port(host: &str, fallback_url: &str) -> (String, u16) {
    if let Some((name, port)) = split_host_port(host) {
        return (name.to_string(), port);
    }

    let parsed = url::Url::parse(fallback_url).ok();
    let hostname = parsed
        .as_ref()
        .and_then(url::Url::host_str)
        .unwrap_or("unknown")
        .to_string();
    let port = parsed
        .as_ref()
        .and_then(url::Url::port_or_known_default)
        .unwrap_or(8080);
    (hostname, port)
}

pub(super) fn build_server_list_entry(
    server: &Server,
    client: &ClientMeta,
) -> SdkServerListEntry {
    let fallback_url = server.url_fallback();
    let (hostname, port) = parse_host_and_port(&server.host, &fallback_url);

    SdkServerListEntry {
        url: server.sdk_url.clone().unwrap_or(fallback_url),
        lat: server
            .sdk_lat
            .clone()
            .unwrap_or_else(|| format!("{:.4}", client.latitude)),
        lon: server
            .sdk_lon
            .clone()
            .unwrap_or_else(|| format!("{:.4}", client.longitude)),
        distance: server.distance_km.round() as u64,
        name: server.name.clone(),
        country: server.country.clone(),
        cc: server
            .sdk_cc
            .clone()
            .unwrap_or_else(|| client.country.clone()),
        sponsor: server.sponsor.clone(),
        id: server.id,
        preferred: server.sdk_preferred.unwrap_or(0),
        isp_id: server.sdk_isp_id.clone().unwrap_or_else(|| "0".to_string()),
        https_functional: server.sdk_https_functional.unwrap_or(1),
        host: server.host.clone(),
        hostname: server.sdk_hostname.clone().unwrap_or(hostname),
        port: server.sdk_port.unwrap_or(port),
        force_ping_select: server.sdk_force_ping_select,
    }
}

trait ServerUrlFallback {
    fn url_fallback(&self) -> String;
}

impl ServerUrlFallback for Server {
    fn url_fallback(&self) -> String {
        format!("https://{}/speedtest/upload.php", self.host)
    }
}

fn split_host_port(host: &str) -> Option<(&str, u16)> {
    if host.starts_with('[') {
        let end = host.find(']')?;
        let name = &host[1..end];
        let rest = &host[end + 1..];
        let port = rest.strip_prefix(':')?.parse::<u16>().ok()?;
        return Some((name, port));
    }

    let (name, port) = host.rsplit_once(':')?;
    let parsed_port = port.parse::<u16>().ok()?;
    Some((name, parsed_port))
}
