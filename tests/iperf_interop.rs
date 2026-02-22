use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::process::{Child, Command, Stdio};
use std::sync::OnceLock;

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::task::JoinHandle;
use tokio::time::{Duration, timeout};

fn binary_path() -> &'static str {
    env!("CARGO_BIN_EXE_tunmux-speedtest")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn iperf_tcp_direct_upload_json_smoke() {
    if !iperf3_available() {
        return;
    }

    let port = reserve_tcp_port();
    let mut server = start_iperf3_server(port);

    let output = run_iperf_command(&[
        "iperf",
        "--host",
        "127.0.0.1",
        "--port",
        &port.to_string(),
        "--seconds",
        "1",
        "--parallel",
        "1",
        "--upload-only",
        "--json",
    ])
    .await;

    let _ = server.wait();

    assert!(output.status.success(), "command failed: {:?}", output);

    let body: Value = serde_json::from_slice(&output.stdout).expect("json output should parse");
    assert_eq!(body["schema"], "tunmux.iperf.v1");
    assert!(body["results"]["upload"]["bytes"].as_u64().unwrap_or(0) > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn iperf_tcp_http_proxy_upload_json_smoke() {
    if !iperf3_available() {
        return;
    }

    let port = reserve_tcp_port();
    let mut server = start_iperf3_server(port);
    let (proxy_port, proxy_handle) = start_http_connect_proxy().await;

    let proxy_url = format!("http://127.0.0.1:{proxy_port}");
    let output = run_iperf_command(&[
        "iperf",
        "--host",
        "127.0.0.1",
        "--port",
        &port.to_string(),
        "--seconds",
        "1",
        "--parallel",
        "1",
        "--upload-only",
        "--proxy",
        &proxy_url,
        "--json",
    ])
    .await;

    proxy_handle.abort();
    let _ = server.wait();

    assert!(output.status.success(), "command failed: {:?}", output);
    let body: Value = serde_json::from_slice(&output.stdout).expect("json output should parse");
    assert_eq!(body["schema"], "tunmux.iperf.v1");
    assert!(body["results"]["upload"]["bytes"].as_u64().unwrap_or(0) > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn iperf_udp_socks5_proxy_upload_json_smoke() {
    if !iperf3_available() {
        return;
    }

    let port = reserve_tcp_port();
    let mut server = start_iperf3_server(port);
    let (proxy_port, proxy_handle) = start_socks5_proxy().await;

    let proxy_url = format!("socks5://127.0.0.1:{proxy_port}");
    let output = run_iperf_command(&[
        "iperf",
        "--host",
        "127.0.0.1",
        "--port",
        &port.to_string(),
        "--protocol",
        "udp",
        "--seconds",
        "1",
        "--parallel",
        "1",
        "--upload-only",
        "--proxy",
        &proxy_url,
        "--bitrate",
        "1000000",
        "--json",
    ])
    .await;

    proxy_handle.abort();
    let _ = server.wait();

    assert!(output.status.success(), "command failed: {:?}", output);
    let body: Value = serde_json::from_slice(&output.stdout).expect("json output should parse");
    assert_eq!(body["schema"], "tunmux.iperf.v1");
    assert!(body["results"]["upload"]["bytes"].as_u64().unwrap_or(0) > 0);
    assert!(body["results"]["upload"]["packets"].as_u64().unwrap_or(0) > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn iperf_udp_over_http_proxy_is_rejected() {
    let output = run_iperf_command(&[
        "iperf",
        "--host",
        "127.0.0.1",
        "--port",
        "5201",
        "--protocol",
        "udp",
        "--proxy",
        "http://127.0.0.1:8080",
        "--json",
    ])
    .await;

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("UDP is not supported over HTTP proxy"));
}

async fn run_iperf_command(args: &[&str]) -> std::process::Output {
    let owned_args = args
        .iter()
        .map(|arg| (*arg).to_string())
        .collect::<Vec<_>>();
    tokio::task::spawn_blocking(move || {
        Command::new(binary_path())
            .args(owned_args)
            .output()
            .expect("failed running command")
    })
    .await
    .expect("spawn_blocking should complete")
}

fn iperf3_available() -> bool {
    static AVAILABLE: OnceLock<bool> = OnceLock::new();
    *AVAILABLE.get_or_init(|| {
        Command::new("iperf3")
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    })
}

fn reserve_tcp_port() -> u16 {
    let listener =
        std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("should reserve local port");
    listener
        .local_addr()
        .expect("local addr should resolve")
        .port()
}

fn start_iperf3_server(port: u16) -> Child {
    let child = Command::new("iperf3")
        .args(["-s", "-1", "-p", &port.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn iperf3 server");

    std::thread::sleep(Duration::from_millis(300));
    child
}

async fn start_http_connect_proxy() -> (u16, JoinHandle<()>) {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("proxy listener should bind");
    let port = listener
        .local_addr()
        .expect("proxy address should resolve")
        .port();

    let handle = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let _ = handle_http_connect_client(stream).await;
            });
        }
    });

    (port, handle)
}

async fn handle_http_connect_client(mut inbound: TcpStream) -> anyhow::Result<()> {
    let mut request = Vec::new();
    let mut chunk = [0_u8; 512];
    loop {
        let read = inbound.read(&mut chunk).await?;
        if read == 0 {
            anyhow::bail!("client disconnected before CONNECT headers");
        }
        request.extend_from_slice(&chunk[..read]);
        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
        if request.len() > 16 * 1024 {
            anyhow::bail!("CONNECT request too large");
        }
    }

    let req_text = String::from_utf8_lossy(&request);
    let first_line = req_text.lines().next().unwrap_or_default();
    let Some(target) = first_line
        .strip_prefix("CONNECT ")
        .and_then(|line| line.split_whitespace().next())
    else {
        anyhow::bail!("invalid CONNECT request line: {first_line}");
    };

    let mut outbound = TcpStream::connect(target).await?;
    inbound
        .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        .await?;
    let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
    Ok(())
}

async fn start_socks5_proxy() -> (u16, JoinHandle<()>) {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("socks listener should bind");
    let port = listener
        .local_addr()
        .expect("socks address should resolve")
        .port();

    let handle = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let _ = handle_socks5_client(stream).await;
            });
        }
    });

    (port, handle)
}

async fn handle_socks5_client(mut control: TcpStream) -> anyhow::Result<()> {
    let mut greeting = [0_u8; 2];
    control.read_exact(&mut greeting).await?;
    if greeting[0] != 0x05 {
        anyhow::bail!("invalid SOCKS version");
    }
    let methods_len = greeting[1] as usize;
    let mut methods = vec![0_u8; methods_len];
    control.read_exact(&mut methods).await?;
    control.write_all(&[0x05, 0x00]).await?;

    let mut request_header = [0_u8; 4];
    control.read_exact(&mut request_header).await?;
    if request_header[0] != 0x05 {
        anyhow::bail!("invalid SOCKS request version");
    }
    let cmd = request_header[1];
    let atyp = request_header[3];

    let target = read_socks_address(&mut control, atyp).await?;

    match cmd {
        0x01 => {
            let mut outbound = TcpStream::connect(target).await?;
            let bind = outbound.local_addr()?;
            write_socks_success(&mut control, bind).await?;
            let _ = tokio::io::copy_bidirectional(&mut control, &mut outbound).await;
        }
        0x03 => {
            let relay = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await?;
            let relay_addr = relay.local_addr()?;
            write_socks_success(&mut control, relay_addr).await?;

            let mut client_addr: Option<SocketAddr> = None;
            let mut buf = vec![0_u8; 64 * 1024];

            loop {
                let received = timeout(Duration::from_secs(15), relay.recv_from(&mut buf)).await;
                let Ok(Ok((size, src))) = received else {
                    break;
                };

                let is_client_frame = if client_addr.is_none() || client_addr == Some(src) {
                    parse_socks_udp_destination(&buf[..size]).is_some()
                } else {
                    false
                };

                if is_client_frame {
                    client_addr = Some(src);
                    if let Some((dest, payload_start)) = parse_socks_udp_destination(&buf[..size]) {
                        let _ = relay.send_to(&buf[payload_start..size], dest).await;
                    }
                    continue;
                }

                if let Some(client) = client_addr {
                    let wrapped = wrap_socks_udp_response(src, &buf[..size]);
                    let _ = relay.send_to(&wrapped, client).await;
                }
            }
        }
        _ => {
            control
                .write_all(&[0x05, 0x07, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
                .await?;
        }
    }

    Ok(())
}

async fn read_socks_address(stream: &mut TcpStream, atyp: u8) -> anyhow::Result<SocketAddr> {
    match atyp {
        0x01 => {
            let mut addr = [0_u8; 4];
            stream.read_exact(&mut addr).await?;
            let mut port = [0_u8; 2];
            stream.read_exact(&mut port).await?;
            Ok(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(addr[0], addr[1], addr[2], addr[3])),
                u16::from_be_bytes(port),
            ))
        }
        0x03 => {
            let mut len = [0_u8; 1];
            stream.read_exact(&mut len).await?;
            let mut host = vec![0_u8; len[0] as usize];
            stream.read_exact(&mut host).await?;
            let mut port = [0_u8; 2];
            stream.read_exact(&mut port).await?;
            let host = String::from_utf8(host)?;
            let port = u16::from_be_bytes(port);
            let mut resolved = tokio::net::lookup_host(format!("{host}:{port}")).await?;
            resolved
                .next()
                .ok_or_else(|| anyhow::anyhow!("domain target resolved to no addresses"))
        }
        0x04 => {
            let mut addr = [0_u8; 16];
            stream.read_exact(&mut addr).await?;
            let mut port = [0_u8; 2];
            stream.read_exact(&mut port).await?;
            Ok(SocketAddr::new(
                IpAddr::V6(Ipv6Addr::from(addr)),
                u16::from_be_bytes(port),
            ))
        }
        _ => anyhow::bail!("unsupported atyp in test SOCKS server: {atyp}"),
    }
}

async fn write_socks_success(stream: &mut TcpStream, bind: SocketAddr) -> anyhow::Result<()> {
    match bind {
        SocketAddr::V4(v4) => {
            let mut response = vec![0x05, 0x00, 0x00, 0x01];
            response.extend_from_slice(&v4.ip().octets());
            response.extend_from_slice(&v4.port().to_be_bytes());
            stream.write_all(&response).await?;
        }
        SocketAddr::V6(v6) => {
            let mut response = vec![0x05, 0x00, 0x00, 0x04];
            response.extend_from_slice(&v6.ip().octets());
            response.extend_from_slice(&v6.port().to_be_bytes());
            stream.write_all(&response).await?;
        }
    }
    Ok(())
}

fn parse_socks_udp_destination(packet: &[u8]) -> Option<(SocketAddr, usize)> {
    if packet.len() < 4 || packet[0] != 0 || packet[1] != 0 || packet[2] != 0 {
        return None;
    }

    let mut cursor = 3;
    let atyp = *packet.get(cursor)?;
    cursor += 1;

    match atyp {
        0x01 => {
            let octets = packet.get(cursor..cursor + 4)?;
            cursor += 4;
            let port = packet.get(cursor..cursor + 2)?;
            cursor += 2;
            Some((
                SocketAddr::new(
                    IpAddr::V4(Ipv4Addr::new(octets[0], octets[1], octets[2], octets[3])),
                    u16::from_be_bytes([port[0], port[1]]),
                ),
                cursor,
            ))
        }
        0x03 => {
            let len = *packet.get(cursor)? as usize;
            cursor += 1;
            let host = packet.get(cursor..cursor + len)?;
            cursor += len;
            let port = packet.get(cursor..cursor + 2)?;
            cursor += 2;
            let host = std::str::from_utf8(host).ok()?;
            let port = u16::from_be_bytes([port[0], port[1]]);
            let addr = format!("{host}:{port}").parse::<SocketAddr>().ok()?;
            Some((addr, cursor))
        }
        _ => None,
    }
}

fn wrap_socks_udp_response(from: SocketAddr, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() + 32);
    out.extend_from_slice(&[0x00, 0x00, 0x00]);

    match from {
        SocketAddr::V4(v4) => {
            out.push(0x01);
            out.extend_from_slice(&v4.ip().octets());
            out.extend_from_slice(&v4.port().to_be_bytes());
        }
        SocketAddr::V6(v6) => {
            out.push(0x04);
            out.extend_from_slice(&v6.ip().octets());
            out.extend_from_slice(&v6.port().to_be_bytes());
        }
    }

    out.extend_from_slice(payload);
    out
}
