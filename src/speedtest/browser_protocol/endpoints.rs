use std::collections::HashSet;

use anyhow::{Context, Result, anyhow, bail};
use reqwest::Url;

use crate::speedtest::servers::SpeedtestServer;

pub(super) fn websocket_endpoints(server: &SpeedtestServer) -> Result<Vec<Url>> {
    let host_with_port = host_with_port(server)?;
    let secure = Url::parse(&format!("wss://{host_with_port}/ws"))
        .with_context(|| format!("invalid websocket endpoint host '{host_with_port}'"))?;
    let insecure = Url::parse(&format!("ws://{host_with_port}/ws"))
        .with_context(|| format!("invalid websocket endpoint host '{host_with_port}'"))?;
    Ok(vec![secure, insecure])
}

pub(super) fn endpoint_urls(server: &SpeedtestServer, path: &str) -> Result<Vec<Url>> {
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

pub(super) fn browser_headers(builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
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
    if host.starts_with('[') && host.contains(':') && host.contains("]:") {
        return true;
    }
    host.rsplit_once(':')
        .and_then(|(_, port)| port.parse::<u16>().ok())
        .is_some()
}

pub(super) fn apply_browser_websocket_headers(
    request: &mut tokio_tungstenite::tungstenite::handshake::client::Request,
) -> Result<()> {
    let headers = request.headers_mut();
    headers.insert("Origin", "https://www.speedtest.net".parse()?);
    headers.insert("User-Agent", "Mozilla/5.0".parse()?);
    headers.insert("Accept", "*/*".parse()?);
    headers.insert("Accept-Language", "en-US,en;q=0.9".parse()?);
    headers.insert("Cache-Control", "no-cache".parse()?);
    headers.insert("Pragma", "no-cache".parse()?);
    Ok(())
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
