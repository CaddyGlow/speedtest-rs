use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use anyhow::{Context, Result, bail};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket, lookup_host};

use super::{ProxyScheme, ProxySpec};

#[derive(Debug, Clone)]
enum SocksTarget {
    Ip(IpAddr),
    Domain(String),
}

pub struct Socks5UdpAssociation {
    udp_socket: UdpSocket,
    control_stream: TcpStream,
    target: SocksTarget,
    target_port: u16,
}

impl Socks5UdpAssociation {
    pub async fn send_to_target(&self, payload: &[u8]) -> Result<usize> {
        let mut packet = Vec::with_capacity(payload.len() + 64);
        packet.extend_from_slice(&[0x00, 0x00, 0x00]);
        append_target_header(&mut packet, &self.target, self.target_port);
        packet.extend_from_slice(payload);

        self.udp_socket.send(&packet).await?;
        let _ = self.control_stream.peer_addr();
        Ok(payload.len())
    }

    pub async fn recv_from_target(&self, payload_buffer: &mut [u8]) -> Result<usize> {
        let mut packet_buffer = vec![0_u8; payload_buffer.len() + 512];
        let packet_size = self.udp_socket.recv(&mut packet_buffer).await?;

        if packet_size < 4 {
            bail!("received malformed SOCKS5 UDP packet");
        }
        if packet_buffer[2] != 0x00 {
            bail!("fragmented SOCKS5 UDP packets are not supported");
        }

        let mut cursor = 3;
        let atyp = packet_buffer[cursor];
        cursor += 1;

        match atyp {
            0x01 => {
                if packet_size < cursor + 4 + 2 {
                    bail!("received malformed SOCKS5 IPv4 UDP packet");
                }
                cursor += 4;
            }
            0x04 => {
                if packet_size < cursor + 16 + 2 {
                    bail!("received malformed SOCKS5 IPv6 UDP packet");
                }
                cursor += 16;
            }
            0x03 => {
                if packet_size < cursor + 1 {
                    bail!("received malformed SOCKS5 domain UDP packet");
                }
                let len = packet_buffer[cursor] as usize;
                cursor += 1;
                if packet_size < cursor + len + 2 {
                    bail!("received malformed SOCKS5 domain UDP packet");
                }
                cursor += len;
            }
            _ => bail!("unsupported SOCKS5 UDP atyp value {atyp}"),
        }

        if packet_size < cursor + 2 {
            bail!("received malformed SOCKS5 UDP packet (missing port)");
        }
        cursor += 2;

        if packet_size < cursor {
            bail!("received malformed SOCKS5 UDP packet payload");
        }

        let payload_len = packet_size - cursor;
        payload_buffer[..payload_len].copy_from_slice(&packet_buffer[cursor..packet_size]);
        Ok(payload_len)
    }
}

pub async fn connect_via_socks5_proxy(
    proxy: &ProxySpec,
    target_host: &str,
    target_port: u16,
) -> Result<TcpStream> {
    let mut stream = connect_to_proxy(proxy).await?;
    perform_socks5_handshake(&mut stream).await?;

    let target = encode_target(proxy.scheme, target_host, target_port).await?;
    send_command(&mut stream, 0x01, &target).await?;
    read_socks5_reply(&mut stream).await?;
    Ok(stream)
}

pub async fn open_udp_association(
    proxy: &ProxySpec,
    target_host: &str,
    target_port: u16,
) -> Result<Socks5UdpAssociation> {
    let mut control_stream = connect_to_proxy(proxy).await?;
    perform_socks5_handshake(&mut control_stream).await?;

    let any_addr_request = vec![0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
    send_command(&mut control_stream, 0x03, &any_addr_request).await?;

    let relay_addr = read_socks5_reply(&mut control_stream).await?;
    let udp_socket = UdpSocket::bind("0.0.0.0:0").await?;
    udp_socket.connect(relay_addr).await?;

    let target = match proxy.scheme {
        ProxyScheme::Socks5h => SocksTarget::Domain(target_host.to_string()),
        ProxyScheme::Socks5 => {
            let mut resolved = lookup_host(format!("{target_host}:{target_port}"))
                .await
                .with_context(|| format!("failed resolving target host '{target_host}'"))?;
            let first = resolved
                .next()
                .context("target host resolved to no addresses")?;
            SocksTarget::Ip(first.ip())
        }
        _ => bail!("SOCKS5 UDP association requires socks5 or socks5h proxy"),
    };

    Ok(Socks5UdpAssociation {
        udp_socket,
        control_stream,
        target,
        target_port,
    })
}

async fn connect_to_proxy(proxy: &ProxySpec) -> Result<TcpStream> {
    let addr = format!("{}:{}", proxy.host, proxy.port);
    TcpStream::connect(addr)
        .await
        .context("failed connecting to SOCKS5 proxy")
}

async fn perform_socks5_handshake(stream: &mut TcpStream) -> Result<()> {
    stream.write_all(&[0x05, 0x01, 0x00]).await?;

    let mut method_selection = [0_u8; 2];
    stream.read_exact(&mut method_selection).await?;
    if method_selection[0] != 0x05 {
        bail!("proxy returned invalid SOCKS version during method selection");
    }
    if method_selection[1] != 0x00 {
        bail!(
            "proxy requires unsupported SOCKS5 authentication method {}",
            method_selection[1]
        );
    }

    Ok(())
}

async fn encode_target(
    scheme: ProxyScheme,
    target_host: &str,
    target_port: u16,
) -> Result<Vec<u8>> {
    match scheme {
        ProxyScheme::Socks5h => {
            if target_host.len() > u8::MAX as usize {
                bail!("target host is too long for SOCKS5 domain encoding");
            }
            let mut out = Vec::with_capacity(target_host.len() + 4);
            out.push(0x03);
            out.push(target_host.len() as u8);
            out.extend_from_slice(target_host.as_bytes());
            out.extend_from_slice(&target_port.to_be_bytes());
            Ok(out)
        }
        ProxyScheme::Socks5 => {
            let mut resolved = lookup_host(format!("{target_host}:{target_port}"))
                .await
                .with_context(|| format!("failed resolving target host '{target_host}'"))?;
            let first = resolved
                .next()
                .context("target host resolved to no addresses")?;

            let mut out = Vec::with_capacity(22);
            match first.ip() {
                IpAddr::V4(ip) => {
                    out.push(0x01);
                    out.extend_from_slice(&ip.octets());
                }
                IpAddr::V6(ip) => {
                    out.push(0x04);
                    out.extend_from_slice(&ip.octets());
                }
            }
            out.extend_from_slice(&target_port.to_be_bytes());
            Ok(out)
        }
        _ => bail!("SOCKS5 target encoding requires socks5 or socks5h scheme"),
    }
}

async fn send_command(stream: &mut TcpStream, command: u8, encoded_target: &[u8]) -> Result<()> {
    let mut request = Vec::with_capacity(encoded_target.len() + 3);
    request.extend_from_slice(&[0x05, command, 0x00]);
    request.extend_from_slice(encoded_target);
    stream.write_all(&request).await?;
    Ok(())
}

async fn read_socks5_reply(stream: &mut TcpStream) -> Result<SocketAddr> {
    let mut header = [0_u8; 4];
    stream.read_exact(&mut header).await?;

    if header[0] != 0x05 {
        bail!("proxy returned invalid SOCKS version in command reply");
    }
    if header[1] != 0x00 {
        bail!("SOCKS5 command failed with error code {}", header[1]);
    }

    let atyp = header[3];
    match atyp {
        0x01 => {
            let mut ipv4 = [0_u8; 4];
            stream.read_exact(&mut ipv4).await?;
            let mut port = [0_u8; 2];
            stream.read_exact(&mut port).await?;
            Ok(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(ipv4[0], ipv4[1], ipv4[2], ipv4[3])),
                u16::from_be_bytes(port),
            ))
        }
        0x04 => {
            let mut ipv6 = [0_u8; 16];
            stream.read_exact(&mut ipv6).await?;
            let mut port = [0_u8; 2];
            stream.read_exact(&mut port).await?;
            Ok(SocketAddr::new(
                IpAddr::from(ipv6),
                u16::from_be_bytes(port),
            ))
        }
        0x03 => {
            let mut len = [0_u8; 1];
            stream.read_exact(&mut len).await?;
            let domain_len = len[0] as usize;

            let mut domain = vec![0_u8; domain_len];
            stream.read_exact(&mut domain).await?;

            let mut port = [0_u8; 2];
            stream.read_exact(&mut port).await?;

            let domain = String::from_utf8(domain).context("SOCKS5 reply domain is not UTF-8")?;
            let port = u16::from_be_bytes(port);
            let mut resolved = lookup_host(format!("{domain}:{port}"))
                .await
                .with_context(|| format!("failed resolving SOCKS5 UDP relay '{domain}:{port}'"))?;
            resolved
                .next()
                .context("SOCKS5 UDP relay resolved to no addresses")
        }
        _ => bail!("SOCKS5 command reply used unsupported atyp value {atyp}"),
    }
}

fn append_target_header(packet: &mut Vec<u8>, target: &SocksTarget, target_port: u16) {
    match target {
        SocksTarget::Ip(IpAddr::V4(ip)) => {
            packet.push(0x01);
            packet.extend_from_slice(&ip.octets());
        }
        SocksTarget::Ip(IpAddr::V6(ip)) => {
            packet.push(0x04);
            packet.extend_from_slice(&ip.octets());
        }
        SocksTarget::Domain(domain) => {
            packet.push(0x03);
            packet.push(domain.len() as u8);
            packet.extend_from_slice(domain.as_bytes());
        }
    }
    packet.extend_from_slice(&target_port.to_be_bytes());
}
