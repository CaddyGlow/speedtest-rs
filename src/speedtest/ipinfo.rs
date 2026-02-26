use std::time::Duration;

use anyhow::Result;
use reqwest::Client;
use serde::Deserialize;

const IPINFO_URL: &str = "https://ipinfo.io/json";
const IPINFO_TIMEOUT_SECONDS: u64 = 4;

#[derive(Debug, Clone)]
pub struct IpInfo {
    pub country: Option<String>,
    pub city: Option<String>,
    pub ip: Option<String>,
    pub org: Option<String>,
}

pub async fn fetch_ipinfo(client: &Client) -> Result<IpInfo> {
    let response = client
        .get(IPINFO_URL)
        .timeout(Duration::from_secs(IPINFO_TIMEOUT_SECONDS))
        .send()
        .await?
        .error_for_status()?;
    let payload: IpInfoPayload = response.json().await?;
    Ok(map_payload(payload))
}

#[derive(Debug, Deserialize)]
struct IpInfoPayload {
    country: Option<String>,
    city: Option<String>,
    ip: Option<String>,
    org: Option<String>,
}

fn map_payload(payload: IpInfoPayload) -> IpInfo {
    IpInfo {
        country: clean_optional(payload.country),
        city: clean_optional(payload.city),
        ip: clean_optional(payload.ip),
        org: clean_optional(payload.org),
    }
}

fn clean_optional(value: Option<String>) -> Option<String> {
    value.and_then(|raw| {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{IpInfoPayload, map_payload};

    #[test]
    fn maps_and_trims_ipinfo_fields() {
        let payload = IpInfoPayload {
            country: Some(" US ".to_string()),
            city: Some(" New York ".to_string()),
            ip: Some(" 203.0.113.7 ".to_string()),
            org: Some(" AS64500 Example Net ".to_string()),
        };

        let info = map_payload(payload);

        assert_eq!(info.country.as_deref(), Some("US"));
        assert_eq!(info.city.as_deref(), Some("New York"));
        assert_eq!(info.ip.as_deref(), Some("203.0.113.7"));
        assert_eq!(info.org.as_deref(), Some("AS64500 Example Net"));
    }

    #[test]
    fn drops_empty_ipinfo_fields() {
        let payload = IpInfoPayload {
            country: Some("  ".to_string()),
            city: None,
            ip: Some(String::new()),
            org: Some("\t".to_string()),
        };

        let info = map_payload(payload);

        assert!(info.country.is_none());
        assert!(info.city.is_none());
        assert!(info.ip.is_none());
        assert!(info.org.is_none());
    }
}
