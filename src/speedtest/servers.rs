use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result, anyhow, bail};
use reqwest::Client;
use reqwest::header::{COOKIE, SET_COOKIE};
use roxmltree::Document;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};
use url::Url;

use crate::speedtest::api::{ResolvedSpeedtestApi, SpeedtestApiMode};
use crate::speedtest::session;

const CACHE_SUBDIR: &str = "tunmux-speedtest";
const CACHE_FILE_NAME: &str = "servers.json";
const MAX_CACHED_SERVERS: usize = 10_000;
const LEGACY_SERVERS_URL: &str =
    "https://www.speedtest.net/api/js/servers?engine=js&https_functional=true";
const MODERN_SERVERS_URL: &str =
    "https://www.speedtest.net/speedtest-servers-static.php?x=whysosad";
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
struct RawSpeedtestServer {
    id: String,
    sponsor: String,
    name: String,
    country: String,
    host: String,
    #[serde(alias = "d", alias = "distance")]
    distance_km: f64,
    url: String,
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
    api_mode: SpeedtestApiMode,
    limit: usize,
    client_location: Option<(f64, f64)>,
) -> Result<(Vec<SpeedtestServer>, ResolvedSpeedtestApi)> {
    match api_mode {
        SpeedtestApiMode::Legacy => {
            let fetched = fetch_legacy_servers(client, limit).await?;
            merge_with_cache_and_persist(fetched, ResolvedSpeedtestApi::Legacy)
        }
        SpeedtestApiMode::ModernTcp => {
            let (latitude, longitude) = client_location
                .context("modern-tcp speedtest API requires client latitude/longitude")?;
            let fetched = fetch_modern_tcp_servers(client, limit, latitude, longitude).await?;
            if fetched.is_empty() {
                bail!("modern-tcp speedtest API returned no usable servers");
            }
            merge_with_cache_and_persist(fetched, ResolvedSpeedtestApi::ModernTcp)
        }
        SpeedtestApiMode::Modern => {
            let fetched = fetch_modern_sdk_servers(client, limit).await?;
            if fetched.is_empty() {
                bail!("modern speedtest API returned no usable servers");
            }
            merge_with_cache_and_persist(fetched, ResolvedSpeedtestApi::Modern)
        }
        SpeedtestApiMode::Auto => {
            if let Ok(fetched) = fetch_modern_sdk_servers(client, limit).await
                && !fetched.is_empty()
            {
                return merge_with_cache_and_persist(fetched, ResolvedSpeedtestApi::Modern);
            }

            if let Ok(fetched) = fetch_legacy_servers(client, limit).await
                && !fetched.is_empty()
            {
                return merge_with_cache_and_persist(fetched, ResolvedSpeedtestApi::Legacy);
            }

            let (latitude, longitude) = client_location
                .context("auto mode fallback to modern-tcp requires client latitude/longitude")?;
            let fetched = fetch_modern_tcp_servers(client, limit, latitude, longitude)
                .await
                .map_err(|error| {
                    anyhow!(
                        "speedtest API discovery failed for modern, legacy, and modern-tcp ({error})"
                    )
                })?;
            if fetched.is_empty() {
                bail!("all speedtest API catalogs returned no usable servers");
            }
            merge_with_cache_and_persist(fetched, ResolvedSpeedtestApi::ModernTcp)
        }
    }
}

pub async fn fetch_servers_for_api(
    client: &Client,
    api: ResolvedSpeedtestApi,
    limit: usize,
    client_location: Option<(f64, f64)>,
) -> Result<Vec<SpeedtestServer>> {
    let mode = match api {
        ResolvedSpeedtestApi::Legacy => SpeedtestApiMode::Legacy,
        ResolvedSpeedtestApi::Modern => SpeedtestApiMode::Modern,
        ResolvedSpeedtestApi::ModernTcp => SpeedtestApiMode::ModernTcp,
    };
    let (servers, _) = fetch_servers(client, mode, limit, client_location).await?;
    Ok(servers)
}

fn merge_with_cache_and_persist(
    fetched: Vec<SpeedtestServer>,
    resolved_api: ResolvedSpeedtestApi,
) -> Result<(Vec<SpeedtestServer>, ResolvedSpeedtestApi)> {
    let cached = load_cached_servers().unwrap_or_default();
    let merged = merge_server_catalog(fetched, cached);
    let cache_copy = merged
        .iter()
        .cloned()
        .map(|mut server| {
            server.session_guid = None;
            server
        })
        .collect::<Vec<_>>();

    if let Err(error) = write_cached_servers(&cache_copy) {
        eprintln!("failed to update speedtest server cache: {}", error);
    }

    Ok((merged, resolved_api))
}

async fn fetch_legacy_servers(client: &Client, limit: usize) -> Result<Vec<SpeedtestServer>> {
    let endpoint = format!("{LEGACY_SERVERS_URL}&limit={limit}");
    let response = client.get(endpoint).send().await?.error_for_status()?;
    let body = response.text().await?;
    parse_servers_json(&body)
}

async fn fetch_modern_tcp_servers(
    client: &Client,
    limit: usize,
    client_latitude: f64,
    client_longitude: f64,
) -> Result<Vec<SpeedtestServer>> {
    let response = client
        .get(MODERN_SERVERS_URL)
        .send()
        .await?
        .error_for_status()?;
    let body = response.text().await?;
    let mut servers = parse_servers_xml(&body, client_latitude, client_longitude)?;
    servers.sort_by(|a, b| {
        a.distance_km
            .partial_cmp(&b.distance_km)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    servers.truncate(limit);
    Ok(servers)
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

pub fn parse_servers_json(body: &str) -> Result<Vec<SpeedtestServer>> {
    let raw_servers = serde_json::from_str::<Vec<RawSpeedtestServer>>(body)
        .context("failed to parse speedtest servers response")?;

    raw_servers
        .into_iter()
        .map(|server| {
            let id = server
                .id
                .parse::<u64>()
                .with_context(|| format!("invalid speedtest server id '{}'", server.id))?;
            Ok(SpeedtestServer {
                id,
                sponsor: server.sponsor,
                name: server.name,
                country: server.country,
                host: server.host,
                distance_km: server.distance_km,
                url: server.url,
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
            })
        })
        .collect()
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

pub fn parse_servers_xml(
    body: &str,
    client_latitude: f64,
    client_longitude: f64,
) -> Result<Vec<SpeedtestServer>> {
    if !client_latitude.is_finite() || !client_longitude.is_finite() {
        bail!("client latitude/longitude must be finite");
    }

    let doc = Document::parse(body).context("failed to parse speedtest servers XML response")?;
    let mut servers = Vec::new();

    for node in doc.descendants().filter(|node| node.has_tag_name("server")) {
        let id = read_u64_attr(&node, "id")?;
        let sponsor = read_attr(&node, "sponsor")?;
        let name = read_attr(&node, "name")?;
        let country = node
            .attribute("country")
            .or_else(|| node.attribute("cc"))
            .unwrap_or("")
            .to_string();
        let host = read_attr(&node, "host")?;
        let latitude = read_f64_attr(&node, "lat")?;
        let longitude = read_f64_attr(&node, "lon")?;
        let Some(url) = preferred_server_url(&node) else {
            continue;
        };

        let distance_km = haversine_km(client_latitude, client_longitude, latitude, longitude);

        servers.push(SpeedtestServer {
            id,
            sponsor,
            name,
            country,
            host,
            distance_km,
            url,
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
        });
    }

    Ok(servers)
}

fn read_attr(node: &roxmltree::Node<'_, '_>, name: &str) -> Result<String> {
    node.attribute(name)
        .map(ToString::to_string)
        .with_context(|| format!("speedtest server XML missing '{name}' attribute"))
}

fn read_u64_attr(node: &roxmltree::Node<'_, '_>, name: &str) -> Result<u64> {
    let raw = read_attr(node, name)?;
    raw.parse::<u64>()
        .with_context(|| format!("speedtest server XML has invalid '{name}' value '{raw}'"))
}

fn read_f64_attr(node: &roxmltree::Node<'_, '_>, name: &str) -> Result<f64> {
    let raw = read_attr(node, name)?;
    let value = raw
        .parse::<f64>()
        .with_context(|| format!("speedtest server XML has invalid '{name}' value '{raw}'"))?;
    if !value.is_finite() {
        bail!("speedtest server XML has non-finite '{name}' value");
    }
    Ok(value)
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

fn preferred_server_url(node: &roxmltree::Node<'_, '_>) -> Option<String> {
    ["url2", "url"].into_iter().find_map(|name| {
        node.attribute(name)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string)
    })
}

fn haversine_km(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const EARTH_RADIUS_KM: f64 = 6_371.0;
    let d_lat = (lat2 - lat1).to_radians();
    let d_lon = (lon2 - lon1).to_radians();
    let lat1 = lat1.to_radians();
    let lat2 = lat2.to_radians();

    let a = (d_lat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (d_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    EARTH_RADIUS_KM * c
}

impl SpeedtestServer {
    pub fn latency_url(&self) -> Result<String> {
        build_sibling_url(&self.url, "latency.txt")
    }

    pub fn upload_url(&self) -> Result<String> {
        build_sibling_url(&self.url, "upload.php")
    }

    pub fn download_url(&self, size: usize) -> Result<String> {
        build_sibling_url(&self.url, &format!("random{size}x{size}.jpg"))
    }
}

fn build_sibling_url(base_url: &str, file_name: &str) -> Result<String> {
    let mut parsed = Url::parse(base_url)
        .with_context(|| format!("invalid speedtest server URL '{base_url}'"))?;
    {
        let mut segments = parsed
            .path_segments_mut()
            .map_err(|_| anyhow!("speedtest server URL is not hierarchical"))?;
        segments.pop_if_empty();
        segments.pop();
        segments.push(file_name);
    }
    Ok(parsed.into())
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
        SpeedtestServer, filter_servers, haversine_km, merge_server_catalog,
        parse_sdk_catalog_json, parse_servers_json, parse_servers_sdk_json, parse_servers_xml,
    };

    #[test]
    fn parses_server_catalog() {
        let body = r#"[
  {
    "id": "1234",
    "sponsor": "Example Sponsor",
    "name": "Example City",
    "country": "US",
    "host": "example.net:8080",
    "d": 42.8,
    "url": "https://example.net/speedtest/upload.php"
  }
]"#;

        let servers = parse_servers_json(body).expect("servers should parse");
        assert_eq!(servers.len(), 1);
        assert_eq!(servers[0].id, 1234);
        assert_eq!(servers[0].distance_km, 42.8);
    }

    #[test]
    fn rejects_non_numeric_server_id() {
        let body = r#"[{"id":"abc","sponsor":"x","name":"x","country":"x","host":"x","d":1.0,"url":"https://x/speedtest/upload.php"}]"#;
        let error = parse_servers_json(body).expect_err("invalid id should fail");
        assert!(error.to_string().contains("invalid speedtest server id"));
    }

    #[test]
    fn parses_server_catalog_xml() {
        let body = r#"<?xml version='1.0' encoding='UTF-8'?>
<settings>
  <servers>
    <server id='123' sponsor='Example' name='City' country='US' host='example.net:8080' lat='37.7749' lon='-122.4194' url='https://example.net/speedtest/upload.php' />
  </servers>
</settings>"#;

        let servers = parse_servers_xml(body, 37.7749, -122.4194).expect("xml should parse");
        assert_eq!(servers.len(), 1);
        assert_eq!(servers[0].id, 123);
        assert_eq!(servers[0].sponsor, "Example");
        assert_eq!(servers[0].url, "https://example.net/speedtest/upload.php");
        assert!(servers[0].distance_km < 0.01);
    }

    #[test]
    fn rejects_non_numeric_xml_server_id() {
        let body = r#"<settings><servers><server id='abc' sponsor='s' name='n' country='US' host='h:1' lat='1' lon='2' url='https://x/speedtest/upload.php'/></servers></settings>"#;
        let error = parse_servers_xml(body, 0.0, 0.0).expect_err("invalid xml id should fail");
        assert!(error.to_string().contains("invalid 'id' value"));
    }

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
    fn prefers_url2_when_present_in_xml() {
        let body = r#"<settings><servers><server id='1' sponsor='s' name='n' country='US' host='h:1' lat='1' lon='2' url='http://legacy/speedtest/upload.php' url2='https://modern/speedtest/upload.php'/></servers></settings>"#;
        let servers = parse_servers_xml(body, 0.0, 0.0).expect("xml should parse");
        assert_eq!(servers.len(), 1);
        assert_eq!(servers[0].url, "https://modern/speedtest/upload.php");
    }

    #[test]
    fn haversine_distance_is_symmetric() {
        let a_to_b = haversine_km(40.0, -74.0, 48.8566, 2.3522);
        let b_to_a = haversine_km(48.8566, 2.3522, 40.0, -74.0);
        assert!((a_to_b - b_to_a).abs() < 0.000_001);
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
