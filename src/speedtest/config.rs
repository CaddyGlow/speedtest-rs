use anyhow::{Context, Result, bail};
use reqwest::Client;
use roxmltree::Document;

const SPEEDTEST_CONFIG_URL: &str = "https://www.speedtest.net/speedtest-config.php";

#[derive(Debug, Clone)]
pub struct SpeedtestClient {
    pub ip: String,
    pub isp: String,
    pub country: String,
    pub latitude: f64,
    pub longitude: f64,
}

#[derive(Debug, Clone)]
pub struct SpeedtestConfig {
    pub client: SpeedtestClient,
}

pub async fn fetch_config(client: &Client) -> Result<SpeedtestConfig> {
    let xml = fetch_config_xml(client).await?;
    parse_config_xml(&xml)
}

pub async fn fetch_config_xml(client: &Client) -> Result<String> {
    let response = client
        .get(SPEEDTEST_CONFIG_URL)
        .send()
        .await?
        .error_for_status()?;
    Ok(response.text().await?)
}

pub fn parse_config_xml(xml: &str) -> Result<SpeedtestConfig> {
    let doc = Document::parse(xml).context("failed to parse speedtest config XML")?;
    let client_node = doc
        .descendants()
        .find(|node| node.has_tag_name("client"))
        .context("speedtest config missing <client> node")?;

    let ip = read_attr(&client_node, "ip")?;
    let isp = read_attr(&client_node, "isp")?;
    let country = read_attr(&client_node, "country")?;
    let latitude = parse_f64_attr(&client_node, "lat")?;
    let longitude = parse_f64_attr(&client_node, "lon")?;

    Ok(SpeedtestConfig {
        client: SpeedtestClient {
            ip,
            isp,
            country,
            latitude,
            longitude,
        },
    })
}

fn read_attr(node: &roxmltree::Node<'_, '_>, name: &str) -> Result<String> {
    node.attribute(name)
        .map(ToString::to_string)
        .with_context(|| format!("speedtest config <client> missing '{name}' attribute"))
}

fn parse_f64_attr(node: &roxmltree::Node<'_, '_>, name: &str) -> Result<f64> {
    let value = read_attr(node, name)?;
    let parsed = value
        .parse::<f64>()
        .with_context(|| format!("speedtest config <client> has invalid '{name}' value"))?;
    if !parsed.is_finite() {
        bail!("speedtest config <client> has non-finite '{name}' value");
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::parse_config_xml;

    #[test]
    fn parses_client_metadata() {
        let xml = r#"<?xml version='1.0' encoding='UTF-8'?>
<settings>
  <client ip='203.0.113.9' lat='37.7749' lon='-122.4194' isp='Example ISP' country='US'/>
</settings>"#;

        let config = parse_config_xml(xml).expect("config should parse");

        assert_eq!(config.client.ip, "203.0.113.9");
        assert_eq!(config.client.isp, "Example ISP");
        assert_eq!(config.client.country, "US");
        assert!((config.client.latitude - 37.7749).abs() < f64::EPSILON);
        assert!((config.client.longitude + 122.4194).abs() < f64::EPSILON);
    }

    #[test]
    fn rejects_missing_client_node() {
        let xml = r#"<settings></settings>"#;
        let error = parse_config_xml(xml).expect_err("missing client node should fail");
        assert!(
            error
                .to_string()
                .contains("speedtest config missing <client> node")
        );
    }
}
