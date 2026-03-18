use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use reqwest::Client;
use reqwest::header::{COOKIE, SET_COOKIE};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::speedtest::session;

const CACHE_SUBDIR: &str = "speedtest-rs";
const CACHE_FILE_NAME: &str = "servers.json";
const MAX_CACHED_SERVERS: usize = 10_000;
const MODERN_SDK_CONFIG_URL: &str =
    "https://www.speedtest.net/api/js/config-sdk?engine=js&https_functional=true";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeedtestServer {
    pub id: u64,
    pub sponsor: String,
    pub name: String,
    pub country: String,
    pub host: String,
    pub distance_km: f64,
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_guid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sdk_lat: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sdk_lon: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sdk_cc: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sdk_preferred: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sdk_isp_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sdk_https_functional: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sdk_hostname: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sdk_port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sdk_force_ping_select: Option<u8>,
}

#[derive(Debug, Clone, Deserialize)]
struct RawSdkConfig {
    #[serde(rename = "ipAddress")]
    _ip_address: String,
    #[serde(rename = "ispName")]
    _isp_name: String,
    #[serde(rename = "ispId")]
    isp_id: Option<u64>,
    #[serde(rename = "providerHash")]
    provider_hash: Option<String>,
    #[serde(rename = "guid")]
    guid: Option<String>,
    #[serde(rename = "clientAuth")]
    client_auth: Option<RawSdkClientAuth>,
    servers: Vec<RawSdkServer>,
}

#[derive(Debug, Clone, Deserialize)]
struct RawSdkClientAuth {
    token: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct RawSdkServer {
    id: serde_json::Value,
    sponsor: String,
    name: String,
    country: String,
    cc: Option<String>,
    lat: Option<String>,
    lon: Option<String>,
    host: String,
    hostname: Option<String>,
    port: Option<u16>,
    preferred: Option<u8>,
    #[serde(rename = "isp_id")]
    isp_id: Option<String>,
    #[serde(rename = "httpsFunctional", alias = "https_functional")]
    https_functional: Option<u8>,
    #[serde(rename = "force_ping_select")]
    force_ping_select: Option<u8>,
    url: String,
    distance: serde_json::Value,
}

#[derive(Debug, Clone)]
struct ParsedSdkCatalog {
    servers: Vec<SpeedtestServer>,
    guid: Option<String>,
    client_auth_token: Option<String>,
    provider_isp_id: Option<u64>,
    provider_hash: Option<String>,
}

pub async fn fetch_servers(
    client: &Client,
    limit: usize,
    selected_server_id: Option<u64>,
) -> Result<Vec<SpeedtestServer>> {
    let fetched = fetch_modern_sdk_servers(client, limit).await?;
    if fetched.is_empty() {
        bail!("modern speedtest server catalog returned no usable servers");
    }
    merge_with_cache_and_persist(fetched, selected_server_id)
}

fn merge_with_cache_and_persist(
    fetched: Vec<SpeedtestServer>,
    selected_server_id: Option<u64>,
) -> Result<Vec<SpeedtestServer>> {
    let cached = load_cached_servers().unwrap_or_default();
    let (runtime_catalog, cache_copy) =
        build_runtime_and_cache_catalogs(fetched, cached, selected_server_id);

    if let Err(error) = write_cached_servers(&cache_copy) {
        eprintln!("failed to update speedtest server cache: {}", error);
    }

    Ok(runtime_catalog)
}

fn build_runtime_and_cache_catalogs(
    fetched: Vec<SpeedtestServer>,
    cached: Vec<SpeedtestServer>,
    selected_server_id: Option<u64>,
) -> (Vec<SpeedtestServer>, Vec<SpeedtestServer>) {
    let runtime_catalog = build_runtime_catalog(&fetched, &cached, selected_server_id);
    let cache_catalog = merge_server_catalog(fetched, cached)
        .into_iter()
        .map(|mut server| {
            server.session_guid = None;
            server
        })
        .collect::<Vec<_>>();

    (runtime_catalog, cache_catalog)
}

fn build_runtime_catalog(
    fetched: &[SpeedtestServer],
    cached: &[SpeedtestServer],
    selected_server_id: Option<u64>,
) -> Vec<SpeedtestServer> {
    let mut runtime_catalog = fetched.to_vec();
    let Some(server_id) = selected_server_id else {
        return runtime_catalog;
    };

    if runtime_catalog.iter().any(|server| server.id == server_id) {
        return runtime_catalog;
    }

    if let Some(cached_server) = cached.iter().find(|server| server.id == server_id).cloned() {
        runtime_catalog.push(cached_server);
    }

    runtime_catalog
}

async fn fetch_modern_sdk_servers(client: &Client, limit: usize) -> Result<Vec<SpeedtestServer>> {
    let mut session_state = match session::load_modern_session() {
        Ok(Some(state)) => {
            debug!(
                has_guid = state.guid.is_some(),
                has_token = state.client_auth_token.is_some(),
                cookies = state.cookies.len(),
                "loaded cached modern session"
            );
            state
        }
        Ok(None) => {
            debug!("no cached modern session found");
            session::ModernSession::default()
        }
        Err(error) => {
            warn!(error = %error, "failed loading cached modern session; continuing without it");
            session::ModernSession::default()
        }
    };

    let endpoint = format!("{MODERN_SDK_CONFIG_URL}&limit={limit}");
    let mut request = client
        .get(endpoint)
        .header("Referer", "https://www.speedtest.net/")
        .header("Origin", "https://www.speedtest.net");
    if let Some(cookie_header) = session_state.cookie_header_value() {
        request = request.header(COOKIE, cookie_header);
    }

    let response = request.send().await?.error_for_status()?;
    let set_cookie_headers = response
        .headers()
        .get_all(SET_COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let body = response.text().await?;

    for header in &set_cookie_headers {
        session_state.apply_set_cookie_header_line(header);
    }

    let mut parsed = parse_sdk_catalog_json(&body)?;

    if parsed.guid.is_none() {
        parsed.guid = session_state.guid.clone();
    }

    let effective_guid = parsed
        .guid
        .clone()
        .unwrap_or_else(session::generate_session_guid);
    for server in &mut parsed.servers {
        server.session_guid = Some(effective_guid.clone());
    }

    session_state.guid = Some(effective_guid);
    if let Some(token) = parsed.client_auth_token {
        session_state.client_auth_token = Some(token);
    }
    if let Some(provider_isp_id) = parsed.provider_isp_id {
        session_state.provider_isp_id = Some(provider_isp_id);
    }
    if let Some(provider_hash) = parsed.provider_hash {
        session_state.provider_hash = Some(provider_hash);
    }
    session_state.touch_saved_at();

    if let Err(error) = session::save_modern_session(&session_state) {
        warn!(error = %error, "failed saving modern session state");
    }

    debug!(
        servers = parsed.servers.len(),
        guid = %session_state.guid.as_deref().unwrap_or("none"),
        guid_bound_servers = parsed
            .servers
            .iter()
            .filter(|server| server.session_guid.is_some())
            .count(),
        "modern server catalog fetched"
    );

    Ok(parsed.servers)
}

#[cfg(test)]
pub fn parse_servers_sdk_json(body: &str) -> Result<Vec<SpeedtestServer>> {
    Ok(parse_sdk_catalog_json(body)?.servers)
}

fn parse_sdk_catalog_json(body: &str) -> Result<ParsedSdkCatalog> {
    let raw = serde_json::from_str::<RawSdkConfig>(body)
        .context("failed to parse speedtest config-sdk response")?;

    let servers = raw
        .servers
        .into_iter()
        .map(|server| {
            let id = value_to_u64(&server.id).with_context(|| {
                format!("invalid speedtest config-sdk server id '{}'", server.id)
            })?;
            let distance_km = value_to_f64(&server.distance).with_context(|| {
                format!(
                    "invalid speedtest config-sdk server distance '{}'",
                    server.distance
                )
            })?;

            let country = if server.country.is_empty() {
                server.cc.clone().unwrap_or_default()
            } else {
                server.country.clone()
            };

            Ok(SpeedtestServer {
                id,
                sponsor: server.sponsor,
                name: server.name,
                country,
                host: server.host,
                distance_km,
                url: server.url,
                session_guid: None,
                sdk_lat: server.lat,
                sdk_lon: server.lon,
                sdk_cc: server.cc,
                sdk_preferred: server.preferred,
                sdk_isp_id: server.isp_id,
                sdk_https_functional: server.https_functional,
                sdk_hostname: server.hostname,
                sdk_port: server.port,
                sdk_force_ping_select: server.force_ping_select,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(ParsedSdkCatalog {
        servers,
        guid: raw.guid,
        client_auth_token: raw.client_auth.and_then(|auth| auth.token),
        provider_isp_id: raw.isp_id,
        provider_hash: raw.provider_hash,
    })
}

fn value_to_u64(value: &serde_json::Value) -> Result<u64> {
    match value {
        serde_json::Value::Number(number) => number
            .as_u64()
            .context("numeric value is not an unsigned integer"),
        serde_json::Value::String(text) => text
            .parse::<u64>()
            .with_context(|| format!("cannot parse '{text}' as u64")),
        _ => bail!("unsupported value type for u64 conversion"),
    }
}

fn value_to_f64(value: &serde_json::Value) -> Result<f64> {
    let parsed = match value {
        serde_json::Value::Number(number) => number
            .as_f64()
            .context("numeric value is not representable as f64")?,
        serde_json::Value::String(text) => text
            .parse::<f64>()
            .with_context(|| format!("cannot parse '{text}' as f64"))?,
        _ => bail!("unsupported value type for f64 conversion"),
    };

    if !parsed.is_finite() {
        bail!("parsed f64 value is not finite");
    }
    Ok(parsed)
}

fn merge_server_catalog(
    fetched: Vec<SpeedtestServer>,
    cached: Vec<SpeedtestServer>,
) -> Vec<SpeedtestServer> {
    let mut seen = HashSet::new();
    let mut merged = Vec::with_capacity((fetched.len() + cached.len()).min(MAX_CACHED_SERVERS));

    for server in fetched.into_iter().chain(cached) {
        if seen.insert(server.id) {
            merged.push(server);
        }
        if merged.len() >= MAX_CACHED_SERVERS {
            break;
        }
    }

    merged
}

pub fn filter_servers<'a>(
    servers: &'a [SpeedtestServer],
    search: Option<&str>,
) -> Vec<&'a SpeedtestServer> {
    match search {
        None => servers.iter().collect(),
        Some(raw) => {
            let query = raw.trim().to_ascii_lowercase();
            if query.is_empty() {
                return servers.iter().collect();
            }

            servers
                .iter()
                .filter(|server| {
                    server.id.to_string().contains(&query)
                        || server.sponsor.to_ascii_lowercase().contains(&query)
                        || server.name.to_ascii_lowercase().contains(&query)
                        || server.country.to_ascii_lowercase().contains(&query)
                        || server.host.to_ascii_lowercase().contains(&query)
                })
                .collect()
        }
    }
}

pub fn load_cached_servers() -> Result<Vec<SpeedtestServer>> {
    let cache_path = cache_file_path()?;
    if !cache_path.exists() {
        return Ok(Vec::new());
    }

    let body = fs::read_to_string(&cache_path)
        .with_context(|| format!("failed reading cache file {}", cache_path.display()))?;
    let parsed = serde_json::from_str::<Vec<SpeedtestServer>>(&body)
        .with_context(|| format!("failed parsing cache file {}", cache_path.display()))?;
    Ok(parsed)
}

pub fn clear_cached_servers() -> Result<bool> {
    let cache_path = cache_file_path()?;
    if !cache_path.exists() {
        return Ok(false);
    }

    fs::remove_file(&cache_path)
        .with_context(|| format!("failed removing cache file {}", cache_path.display()))?;
    Ok(true)
}

fn write_cached_servers(servers: &[SpeedtestServer]) -> Result<()> {
    let cache_path = cache_file_path()?;
    if let Some(parent) = cache_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed creating cache directory {}", parent.display()))?;
    }

    let body = serde_json::to_string_pretty(servers)?;
    fs::write(&cache_path, body)
        .with_context(|| format!("failed writing cache file {}", cache_path.display()))
}

pub fn cache_file_path() -> Result<PathBuf> {
    if let Ok(cache_home) = std::env::var("XDG_CACHE_HOME") {
        return Ok(PathBuf::from(cache_home)
            .join(CACHE_SUBDIR)
            .join(CACHE_FILE_NAME));
    }

    let home = std::env::var("HOME").context("HOME is not set; cannot resolve cache path")?;
    Ok(PathBuf::from(home)
        .join(".cache")
        .join(CACHE_SUBDIR)
        .join(CACHE_FILE_NAME))
}

#[cfg(test)]
mod tests {
    use super::{
        SpeedtestServer, build_runtime_and_cache_catalogs, filter_servers, merge_server_catalog,
        parse_sdk_catalog_json, parse_servers_sdk_json,
    };

    #[test]
    fn parses_server_catalog_sdk_json() {
        let body = r#"{
  "ipAddress": "203.0.113.7",
  "ispName": "Example ISP",
  "guid": "abc-guid",
  "servers": [
    {
      "id": "61301",
      "sponsor": "ORANGE FRANCE",
      "name": "Marseille",
      "country": "France",
      "cc": "FR",
      "host": "marseille.example.net:8080",
      "url": "http://marseille.example.net:8080/speedtest/upload.php",
      "distance": 1
    }
  ]
}"#;

        let servers = parse_servers_sdk_json(body).expect("sdk json should parse");
        assert_eq!(servers.len(), 1);
        assert_eq!(servers[0].id, 61301);
        assert_eq!(servers[0].distance_km, 1.0);
        assert_eq!(servers[0].session_guid, None);
    }

    #[test]
    fn extracts_guid_and_client_auth_from_sdk_json() {
        let body = r#"{
  "ipAddress": "203.0.113.7",
  "ispName": "Example ISP",
  "guid": "abc-guid",
  "clientAuth": {
    "token": "token-123"
  },
  "servers": [
    {
      "id": "61301",
      "sponsor": "ORANGE FRANCE",
      "name": "Marseille",
      "country": "France",
      "cc": "FR",
      "host": "marseille.example.net:8080",
      "url": "http://marseille.example.net:8080/speedtest/upload.php",
      "distance": 1
    }
  ]
}"#;

        let parsed = parse_sdk_catalog_json(body).expect("sdk json should parse");
        assert_eq!(parsed.guid.as_deref(), Some("abc-guid"));
        assert_eq!(parsed.client_auth_token.as_deref(), Some("token-123"));
        assert_eq!(parsed.servers.len(), 1);
    }

    #[test]
    fn merge_prefers_fetched_and_includes_cached() {
        let fetched = vec![server(1, "fresh-1"), server(2, "fresh-2")];
        let cached = vec![server(2, "old-2"), server(3, "old-3")];

        let merged = merge_server_catalog(fetched, cached);
        assert_eq!(merged.len(), 3);
        assert_eq!(merged[0].id, 1);
        assert_eq!(merged[1].id, 2);
        assert_eq!(merged[1].sponsor, "fresh-2");
        assert_eq!(merged[2].id, 3);
    }

    #[test]
    fn runtime_catalog_uses_only_fetched_servers() {
        let mut fetched = server(1, "fresh-1");
        fetched.session_guid = Some("guid-1".to_string());
        let mut cached = server(2, "old-2");
        cached.session_guid = Some("guid-2".to_string());

        let (runtime, cache_catalog) =
            build_runtime_and_cache_catalogs(vec![fetched.clone()], vec![cached], None);

        assert_eq!(runtime.len(), 1);
        assert_eq!(runtime[0].id, 1);
        assert_eq!(runtime[0].session_guid.as_deref(), Some("guid-1"));

        assert_eq!(cache_catalog.len(), 2);
        assert_eq!(cache_catalog[0].id, 1);
        assert_eq!(cache_catalog[1].id, 2);
        assert!(
            cache_catalog
                .iter()
                .all(|server| server.session_guid.is_none())
        );
    }

    #[test]
    fn runtime_catalog_includes_cached_server_only_for_requested_id() {
        let fetched = vec![server(1, "fresh-1")];
        let mut cached = server(2, "old-2");
        cached.session_guid = Some("guid-2".to_string());

        let (runtime, _cache_catalog) =
            build_runtime_and_cache_catalogs(fetched, vec![cached], Some(2));

        assert_eq!(runtime.len(), 2);
        assert_eq!(runtime[0].id, 1);
        assert_eq!(runtime[1].id, 2);
        assert_eq!(runtime[1].session_guid.as_deref(), Some("guid-2"));
    }

    #[test]
    fn runtime_catalog_prefers_api_server_when_requested_id_exists() {
        let mut fetched_server = server(2, "fresh-2");
        fetched_server.host = "api-host".to_string();
        let mut cached_server = server(2, "old-2");
        cached_server.host = "cached-host".to_string();

        let (runtime, _cache_catalog) =
            build_runtime_and_cache_catalogs(vec![fetched_server], vec![cached_server], Some(2));

        assert_eq!(runtime.len(), 1);
        assert_eq!(runtime[0].id, 2);
        assert_eq!(runtime[0].host, "api-host");
    }

    #[test]
    fn filters_servers_by_case_insensitive_query() {
        let servers = vec![server(13791, "Dedi.zone"), server(61301, "ORANGE FRANCE")];

        let by_id = filter_servers(&servers, Some("13791"));
        assert_eq!(by_id.len(), 1);
        assert_eq!(by_id[0].id, 13791);

        let by_sponsor = filter_servers(&servers, Some("orange"));
        assert_eq!(by_sponsor.len(), 1);
        assert_eq!(by_sponsor[0].id, 61301);

        let all = filter_servers(&servers, Some("  "));
        assert_eq!(all.len(), 2);
    }

    fn server(id: u64, sponsor: &str) -> SpeedtestServer {
        SpeedtestServer {
            id,
            sponsor: sponsor.to_string(),
            name: "name".to_string(),
            country: "country".to_string(),
            host: "host".to_string(),
            distance_km: 1.0,
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
