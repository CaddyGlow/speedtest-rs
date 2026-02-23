use std::cmp::min;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use url::Url;

use crate::speedtest::servers::SpeedtestServer;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const IO_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_LINE_BYTES: usize = 512;
const IO_BUFFER_BYTES: usize = 32 * 1024;
static DATA_BLOCK: [u8; IO_BUFFER_BYTES] = [0x42; IO_BUFFER_BYTES];

pub async fn connect(server: &SpeedtestServer) -> Result<TcpStream> {
    let endpoint = resolve_endpoint(server)?;
    let stream = timeout(CONNECT_TIMEOUT, TcpStream::connect(&endpoint))
        .await
        .with_context(|| format!("timed out connecting to modern speedtest server {endpoint}"))?
        .with_context(|| format!("failed connecting to modern speedtest server {endpoint}"))?;
    stream
        .set_nodelay(true)
        .context("failed setting TCP_NODELAY on modern speedtest stream")?;
    Ok(stream)
}

#[allow(dead_code)]
pub async fn ping(stream: &mut TcpStream) -> Result<f64> {
    let token = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before UNIX_EPOCH")?
        .as_millis();
    let command = format!("PING {token}\n");
    let start = Instant::now();
    write_all_with_timeout(stream, command.as_bytes(), "PING command").await?;
    let line = read_line_with_timeout(stream, "PING response").await?;
    validate_pong_line(&line)?;
    Ok(start.elapsed().as_secs_f64() * 1_000.0)
}

pub async fn download(stream: &mut TcpStream, bytes: usize) -> Result<u64> {
    let command = format!("DOWNLOAD {bytes}\n");
    write_all_with_timeout(stream, command.as_bytes(), "DOWNLOAD command").await?;

    let mut remaining = bytes;
    let mut buffer = [0_u8; IO_BUFFER_BYTES];
    while remaining > 0 {
        let read_limit = min(remaining, buffer.len());
        let read = read_with_timeout(stream, &mut buffer[..read_limit], "DOWNLOAD payload").await?;
        if read == 0 {
            bail!("modern DOWNLOAD stream closed before receiving all requested bytes");
        }
        remaining -= read;
    }

    Ok(bytes as u64)
}

pub async fn upload(stream: &mut TcpStream, total_size: usize) -> Result<u64> {
    let command = format!("UPLOAD {total_size} 0\n");
    write_all_with_timeout(stream, command.as_bytes(), "UPLOAD command").await?;

    let payload_size = total_size.saturating_sub(command.len());
    let mut remaining = payload_size;
    while remaining > 0 {
        let chunk = min(remaining, DATA_BLOCK.len());
        write_all_with_timeout(stream, &DATA_BLOCK[..chunk], "UPLOAD payload").await?;
        remaining -= chunk;
    }

    let line = read_line_with_timeout(stream, "UPLOAD response").await?;
    if !line.starts_with("OK ") {
        bail!("modern UPLOAD received invalid response '{line}'");
    }

    Ok(payload_size as u64)
}

pub async fn quit(stream: &mut TcpStream) -> Result<()> {
    write_all_with_timeout(stream, b"QUIT\n", "QUIT command").await
}

fn resolve_endpoint(server: &SpeedtestServer) -> Result<String> {
    if looks_like_host_with_port(&server.host) {
        return Ok(server.host.clone());
    }

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

fn looks_like_host_with_port(host: &str) -> bool {
    if host.starts_with('[') && host.contains(":") && host.contains("]:") {
        return true;
    }
    host.rsplit_once(':')
        .and_then(|(_, port)| port.parse::<u16>().ok())
        .is_some()
}

#[allow(dead_code)]
fn validate_pong_line(line: &str) -> Result<()> {
    let mut fields = line.split_whitespace();
    let command = fields
        .next()
        .context("modern PING response missing command token")?;
    if command != "PONG" {
        bail!("modern PING response did not start with PONG");
    }
    let token = fields
        .next()
        .context("modern PING response missing token")?;
    let _parsed: i64 = token
        .parse()
        .with_context(|| format!("modern PING response token is not numeric ('{token}')"))?;
    Ok(())
}

async fn write_all_with_timeout(stream: &mut TcpStream, body: &[u8], action: &str) -> Result<()> {
    timeout(IO_TIMEOUT, stream.write_all(body))
        .await
        .with_context(|| format!("timed out writing modern speedtest {action}"))?
        .with_context(|| format!("failed writing modern speedtest {action}"))
}

async fn read_with_timeout(stream: &mut TcpStream, body: &mut [u8], action: &str) -> Result<usize> {
    timeout(IO_TIMEOUT, stream.read(body))
        .await
        .with_context(|| format!("timed out reading modern speedtest {action}"))?
        .with_context(|| format!("failed reading modern speedtest {action}"))
}

async fn read_line_with_timeout(stream: &mut TcpStream, action: &str) -> Result<String> {
    let mut line = Vec::new();
    loop {
        if line.len() >= MAX_LINE_BYTES {
            bail!("modern speedtest {action} exceeded {MAX_LINE_BYTES} bytes");
        }

        let mut byte = [0_u8; 1];
        let read = read_with_timeout(stream, &mut byte, action).await?;
        if read == 0 {
            bail!("modern speedtest {action} closed before line terminator");
        }

        line.push(byte[0]);
        if byte[0] == b'\n' {
            break;
        }
    }

    Ok(String::from_utf8_lossy(&line)
        .trim_end_matches(['\r', '\n'])
        .to_string())
}

#[cfg(test)]
mod tests {
    use super::{looks_like_host_with_port, resolve_endpoint, validate_pong_line};
    use crate::speedtest::servers::SpeedtestServer;

    #[test]
    fn detects_host_with_port() {
        assert!(looks_like_host_with_port("example.com:8080"));
        assert!(looks_like_host_with_port("[2001:db8::1]:8080"));
        assert!(!looks_like_host_with_port("example.com"));
    }

    #[test]
    fn resolves_endpoint_from_host_or_url() {
        let with_host = SpeedtestServer {
            id: 1,
            sponsor: "s".to_string(),
            name: "n".to_string(),
            country: "c".to_string(),
            host: "example.com:8080".to_string(),
            distance_km: 1.0,
            url: "https://fallback.test/speedtest/upload.php".to_string(),
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
        assert_eq!(
            resolve_endpoint(&with_host).expect("must resolve"),
            "example.com:8080"
        );

        let without_port = SpeedtestServer {
            host: "example.com".to_string(),
            ..with_host
        };
        assert_eq!(
            resolve_endpoint(&without_port).expect("must resolve from URL"),
            "fallback.test:443"
        );
    }

    #[test]
    fn validates_pong_response() {
        validate_pong_line("PONG 123").expect("valid PONG should parse");
        assert!(validate_pong_line("PONG abc").is_err());
        assert!(validate_pong_line("NOPE 1").is_err());
    }
}
