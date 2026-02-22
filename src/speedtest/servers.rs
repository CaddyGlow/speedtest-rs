use anyhow::{Context, Result, anyhow};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;
use url::Url;

const CACHE_SUBDIR: &str = "tunmux-speedtest";
const CACHE_FILE_NAME: &str = "servers.json";
const MAX_CACHED_SERVERS: usize = 10_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeedtestServer {
    pub id: u64,
    pub sponsor: String,
    pub name: String,
    pub country: String,
    pub host: String,
    pub distance_km: f64,
    pub url: String,
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

pub async fn fetch_servers(client: &Client, limit: usize) -> Result<Vec<SpeedtestServer>> {
    let cached = load_cached_servers().unwrap_or_default();
    let endpoint = format!(
        "https://www.speedtest.net/api/js/servers?engine=js&https_functional=true&limit={}",
        limit
    );

    let response = client.get(endpoint).send().await?.error_for_status()?;
    let body = response.text().await?;
    let fetched = parse_servers_json(&body)?;
    let merged = merge_server_catalog(fetched, cached);

    if let Err(error) = write_cached_servers(&merged) {
        eprintln!("failed to update speedtest server cache: {}", error);
    }

    Ok(merged)
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
            })
        })
        .collect()
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
    use super::{SpeedtestServer, filter_servers, merge_server_catalog, parse_servers_json};

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
        }
    }
}
