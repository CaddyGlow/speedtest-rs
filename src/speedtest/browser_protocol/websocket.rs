use std::time::Duration;

use anyhow::{Context, Result, bail};
use futures_util::{SinkExt, StreamExt};
use tokio::time::timeout;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use url::Url;

use super::WS_PROTOCOL_LEVEL;
use super::endpoints::apply_browser_websocket_headers;

const WS_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const WS_IO_TIMEOUT: Duration = Duration::from_secs(2);

pub(super) type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

pub(super) async fn connect_browser_websocket(endpoint: &Url) -> Result<WsStream> {
    let mut request = endpoint
        .as_str()
        .into_client_request()
        .with_context(|| format!("failed building websocket request for {endpoint}"))?;
    apply_browser_websocket_headers(&mut request)?;

    let (socket, _) = timeout(WS_CONNECT_TIMEOUT, connect_async(request))
        .await
        .with_context(|| format!("timed out connecting websocket {endpoint}"))?
        .with_context(|| format!("failed to open websocket {endpoint}"))?;
    Ok(socket)
}

pub(super) async fn perform_speedtest_ws_handshake(socket: &mut WsStream) -> Result<()> {
    ws_send_text(socket, &format!("HI\t{WS_PROTOCOL_LEVEL}\t"), "HI").await?;
    ws_expect_prefix(socket, "HELLO", "HELLO handshake").await?;

    ws_send_text(socket, "GETIP", "GETIP").await?;
    ws_expect_prefix(socket, "YOURIP", "GETIP response").await?;

    ws_send_text(socket, "CAPABILITIES", "CAPABILITIES").await?;
    ws_expect_prefix(socket, "CAPABILITIES", "CAPABILITIES response").await
}

pub(super) async fn ws_send_text(socket: &mut WsStream, text: &str, action: &str) -> Result<()> {
    timeout(
        WS_IO_TIMEOUT,
        socket.send(Message::Text(text.to_string().into())),
    )
    .await
    .with_context(|| format!("timed out sending websocket {action}"))?
    .with_context(|| format!("failed sending websocket {action}"))
}

pub(super) async fn ws_expect_prefix(
    socket: &mut WsStream,
    expected_prefix: &str,
    action: &str,
) -> Result<()> {
    loop {
        let frame = timeout(WS_IO_TIMEOUT, socket.next())
            .await
            .with_context(|| format!("timed out waiting for websocket {action}"))?
            .context("websocket stream closed")?
            .context("websocket frame error")?;

        match frame {
            Message::Text(text) => {
                let text = text.trim();
                if text.starts_with(expected_prefix) {
                    return Ok(());
                }
                bail!("unexpected websocket message '{text}' while waiting for {action}");
            }
            Message::Binary(_) | Message::Pong(_) => continue,
            Message::Ping(payload) => {
                timeout(WS_IO_TIMEOUT, socket.send(Message::Pong(payload)))
                    .await
                    .with_context(|| format!("timed out replying websocket PONG for {action}"))?
                    .with_context(|| format!("failed replying websocket PONG for {action}"))?;
            }
            Message::Close(_) => bail!("websocket closed while waiting for {action}"),
            Message::Frame(_) => continue,
        }
    }
}

pub(super) async fn ws_next_text(socket: &mut WsStream, action: &str) -> Result<Option<String>> {
    let frame = timeout(WS_IO_TIMEOUT, socket.next())
        .await
        .with_context(|| format!("timed out waiting for websocket {action}"))?;

    let Some(frame) = frame else {
        return Ok(None);
    };

    match frame.context("websocket frame error")? {
        Message::Text(text) => Ok(Some(text.trim().to_string())),
        Message::Binary(_) | Message::Pong(_) => Ok(None),
        Message::Ping(payload) => {
            timeout(WS_IO_TIMEOUT, socket.send(Message::Pong(payload)))
                .await
                .with_context(|| format!("timed out replying websocket PONG for {action}"))?
                .with_context(|| format!("failed replying websocket PONG for {action}"))?;
            Ok(None)
        }
        Message::Close(_) => Ok(None),
        Message::Frame(_) => Ok(None),
    }
}
