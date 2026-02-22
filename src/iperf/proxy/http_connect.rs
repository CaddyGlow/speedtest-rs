use anyhow::{Context, Result, bail};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::ProxySpec;

pub async fn connect_via_http_proxy(
    proxy: &ProxySpec,
    target_host: &str,
    target_port: u16,
) -> Result<TcpStream> {
    let proxy_addr = format!("{}:{}", proxy.host, proxy.port);
    let mut stream = TcpStream::connect(proxy_addr)
        .await
        .context("failed connecting to HTTP proxy")?;

    let connect_target = format!("{target_host}:{target_port}");
    let request = format!(
        "CONNECT {connect_target} HTTP/1.1\r\nHost: {connect_target}\r\nProxy-Connection: Keep-Alive\r\n\r\n"
    );
    stream
        .write_all(request.as_bytes())
        .await
        .context("failed writing HTTP CONNECT request")?;

    let mut response = Vec::with_capacity(1024);
    let mut chunk = [0_u8; 512];
    loop {
        let read = stream
            .read(&mut chunk)
            .await
            .context("failed reading HTTP CONNECT response")?;
        if read == 0 {
            bail!("HTTP proxy closed connection before CONNECT was acknowledged");
        }
        response.extend_from_slice(&chunk[..read]);
        if response.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
        if response.len() > 16 * 1024 {
            bail!("HTTP CONNECT response headers exceed safe size limit");
        }
    }

    let response_text = String::from_utf8_lossy(&response);
    let status_line = response_text.lines().next().unwrap_or_default();
    if !(status_line.starts_with("HTTP/1.1 200") || status_line.starts_with("HTTP/1.0 200")) {
        bail!("HTTP CONNECT failed: {status_line}");
    }

    Ok(stream)
}
