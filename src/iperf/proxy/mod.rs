mod http_connect;
mod socks5;

use anyhow::{Context, Result, bail};
use tokio::net::TcpStream;
use url::Url;

use crate::cli::IperfProtocol;

pub use socks5::Socks5UdpAssociation;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProxyScheme {
    Http,
    Https,
    Socks5,
    Socks5h,
}

impl ProxyScheme {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Https => "https",
            Self::Socks5 => "socks5",
            Self::Socks5h => "socks5h",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ProxySpec {
    pub raw_url: String,
    pub scheme: ProxyScheme,
    pub host: String,
    pub port: u16,
}

pub fn parse_proxy(proxy_url: &str) -> Result<ProxySpec> {
    let parsed =
        Url::parse(proxy_url).with_context(|| format!("invalid proxy URL '{proxy_url}'"))?;
    let scheme = match parsed.scheme() {
        "http" => ProxyScheme::Http,
        "https" => ProxyScheme::Https,
        "socks5" => ProxyScheme::Socks5,
        "socks5h" => ProxyScheme::Socks5h,
        other => bail!("unsupported proxy scheme '{other}' (use http, https, socks5, or socks5h)"),
    };

    let host = parsed
        .host_str()
        .context("proxy URL missing host component")?
        .to_string();
    let port = parsed
        .port_or_known_default()
        .context("proxy URL missing port and no default is available")?;

    Ok(ProxySpec {
        raw_url: proxy_url.to_string(),
        scheme,
        host,
        port,
    })
}

pub fn ensure_compatible(protocol: IperfProtocol, proxy: Option<&ProxySpec>) -> Result<()> {
    let Some(proxy) = proxy else {
        return Ok(());
    };

    match (protocol, proxy.scheme) {
        (_, ProxyScheme::Https) => {
            bail!(
                "HTTPS proxy is not yet supported by native iperf command; use http, socks5, or socks5h"
            )
        }
        (IperfProtocol::Udp, ProxyScheme::Http) => {
            bail!("UDP is not supported over HTTP proxy; use socks5 or socks5h")
        }
        _ => Ok(()),
    }
}

pub async fn connect_tcp_target(
    target_host: &str,
    target_port: u16,
    proxy: Option<&ProxySpec>,
) -> Result<TcpStream> {
    match proxy {
        None => {
            let addr = format!("{target_host}:{target_port}");
            Ok(TcpStream::connect(addr).await?)
        }
        Some(spec) => match spec.scheme {
            ProxyScheme::Http | ProxyScheme::Https => {
                http_connect::connect_via_http_proxy(spec, target_host, target_port).await
            }
            ProxyScheme::Socks5 | ProxyScheme::Socks5h => {
                socks5::connect_via_socks5_proxy(spec, target_host, target_port).await
            }
        },
    }
}

pub async fn connect_udp_socket_direct(
    target_host: &str,
    target_port: u16,
) -> Result<tokio::net::UdpSocket> {
    let socket = tokio::net::UdpSocket::bind("0.0.0.0:0").await?;
    socket
        .connect(format!("{target_host}:{target_port}"))
        .await?;
    Ok(socket)
}

pub async fn connect_udp_socket_socks5(
    proxy: &ProxySpec,
    target_host: &str,
    target_port: u16,
) -> Result<Socks5UdpAssociation> {
    socks5::open_udp_association(proxy, target_host, target_port).await
}

#[cfg(test)]
mod tests {
    use super::{ensure_compatible, parse_proxy};
    use crate::cli::IperfProtocol;

    #[test]
    fn rejects_udp_over_http_proxy() {
        let proxy = parse_proxy("http://127.0.0.1:8080").expect("proxy should parse");
        let error = ensure_compatible(IperfProtocol::Udp, Some(&proxy))
            .expect_err("udp over http should fail");
        assert!(
            error
                .to_string()
                .contains("UDP is not supported over HTTP proxy")
        );
    }

    #[test]
    fn allows_udp_over_socks5() {
        let proxy = parse_proxy("socks5://127.0.0.1:1080").expect("proxy should parse");
        let result = ensure_compatible(IperfProtocol::Udp, Some(&proxy));
        assert!(result.is_ok());
    }

    #[test]
    fn rejects_https_proxy_for_native_iperf() {
        let proxy = parse_proxy("https://127.0.0.1:8443").expect("proxy should parse");
        let error = ensure_compatible(IperfProtocol::Tcp, Some(&proxy))
            .expect_err("https proxy should fail for native iperf");
        assert!(
            error
                .to_string()
                .contains("HTTPS proxy is not yet supported")
        );
    }
}
