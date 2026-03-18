use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use futures_util::{StreamExt, stream};
use reqwest::{Body, Client};
use tokio::sync::watch;
use tracing::debug;

use crate::speedtest::servers::SpeedtestServer;

use super::endpoints::{browser_headers, endpoint_urls};
use super::{TransferRequestError, truncate_for_log};

pub async fn download(
    client: &Client,
    server: &SpeedtestServer,
    _guid: &str,
    size: usize,
) -> std::result::Result<u64, TransferRequestError> {
    download_streaming(client, server, size, &[], None, None).await
}

pub async fn download_streaming(
    client: &Client,
    server: &SpeedtestServer,
    size: usize,
    live_counters: &[&AtomicU64],
    first_byte_at: Option<&OnceLock<Instant>>,
    first_transfer_tx: Option<&watch::Sender<bool>>,
) -> std::result::Result<u64, TransferRequestError> {
    let mut last_error = None;

    for mut url in
        endpoint_urls(server, "download").map_err(|_| TransferRequestError::InvalidEndpoint)?
    {
        url.query_pairs_mut().append_pair("size", &size.to_string());

        let response = match browser_headers(client.get(url.clone())).send().await {
            Ok(response) => response,
            Err(error) => {
                debug!(server_id = server.id, endpoint = %url, error = %error, "download endpoint transport failed");
                last_error = Some(TransferRequestError::Transport);
                continue;
            }
        };

        if !response.status().is_success() {
            let status = response.status();
            match response.text().await {
                Ok(body) => debug!(
                    server_id = server.id,
                    endpoint = %url,
                    status = %status,
                    response_body = %truncate_for_log(&body),
                    "download endpoint returned HTTP error"
                ),
                Err(error) => debug!(
                    server_id = server.id,
                    endpoint = %url,
                    status = %status,
                    error = %error,
                    "download endpoint returned HTTP error and response body unavailable"
                ),
            }
            last_error = Some(TransferRequestError::HttpStatus);
            continue;
        }

        let mut total = 0_u64;
        let mut stream = response.bytes_stream();
        let mut had_error = false;
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => {
                    let len = bytes.len() as u64;
                    if let Some(first_byte_at) = first_byte_at
                        && first_byte_at.set(Instant::now()).is_ok()
                        && let Some(first_transfer_tx) = first_transfer_tx
                    {
                        let _ = first_transfer_tx.send(true);
                    }
                    total += len;
                    for counter in live_counters {
                        counter.fetch_add(len, Ordering::Relaxed);
                    }
                }
                Err(error) => {
                    debug!(server_id = server.id, endpoint = %url, error = %error, "download stream read failed");
                    had_error = true;
                    break;
                }
            }
        }

        if had_error && total == 0 {
            last_error = Some(TransferRequestError::ResponseRead);
            continue;
        }

        return Ok(total);
    }

    Err(last_error.unwrap_or(TransferRequestError::InvalidEndpoint))
}

pub async fn upload(
    client: &Client,
    server: &SpeedtestServer,
    _guid: &str,
    payload: &[u8],
) -> std::result::Result<u64, TransferRequestError> {
    upload_streaming(client, server, payload, None, None, None).await
}

pub async fn upload_streaming(
    client: &Client,
    server: &SpeedtestServer,
    payload: &[u8],
    live_counter: Option<Arc<AtomicU64>>,
    first_byte_at: Option<Arc<OnceLock<Instant>>>,
    first_transfer_tx: Option<watch::Sender<bool>>,
) -> std::result::Result<u64, TransferRequestError> {
    const UPLOAD_CHUNK_BYTES: usize = 32 * 1024;
    let body_len = payload.len() as u64;
    let payload = Arc::<[u8]>::from(payload.to_vec());
    let mut last_error = None;

    for url in endpoint_urls(server, "upload").map_err(|_| TransferRequestError::InvalidEndpoint)? {
        let payload = Arc::clone(&payload);
        let live_counter = live_counter.clone();
        let first_byte_at = first_byte_at.clone();
        let first_transfer_tx = first_transfer_tx.clone();
        let body = Body::wrap_stream(stream::unfold(0usize, move |offset| {
            let payload = Arc::clone(&payload);
            let live_counter = live_counter.clone();
            let first_byte_at = first_byte_at.clone();
            let first_transfer_tx = first_transfer_tx.clone();
            async move {
                if offset >= payload.len() {
                    return None;
                }

                let end = (offset + UPLOAD_CHUNK_BYTES).min(payload.len());
                let chunk = payload[offset..end].to_vec();
                let len = chunk.len() as u64;
                if let Some(first_byte_at) = first_byte_at
                    && first_byte_at.set(Instant::now()).is_ok()
                    && let Some(first_transfer_tx) = &first_transfer_tx
                {
                    let _ = first_transfer_tx.send(true);
                }
                if let Some(live_counter) = &live_counter {
                    live_counter.fetch_add(len, Ordering::Relaxed);
                }

                Some((Ok::<Vec<u8>, std::io::Error>(chunk), end))
            }
        }));

        let response = match browser_headers(client.post(url.clone()))
            .header("Content-Type", "application/octet-stream")
            .header("Content-Length", body_len)
            .body(body)
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => {
                debug!(server_id = server.id, endpoint = %url, error = %error, "upload endpoint transport failed");
                last_error = Some(TransferRequestError::Transport);
                continue;
            }
        };

        if !response.status().is_success() {
            let status = response.status();
            match response.text().await {
                Ok(body) => debug!(
                    server_id = server.id,
                    endpoint = %url,
                    status = %status,
                    response_body = %truncate_for_log(&body),
                    "upload endpoint returned HTTP error"
                ),
                Err(error) => debug!(
                    server_id = server.id,
                    endpoint = %url,
                    status = %status,
                    error = %error,
                    "upload endpoint returned HTTP error and response body unavailable"
                ),
            }
            last_error = Some(TransferRequestError::HttpStatus);
            continue;
        }

        match response.bytes().await {
            Ok(_) => return Ok(body_len),
            Err(error) => {
                debug!(server_id = server.id, endpoint = %url, error = %error, "upload response read failed");
                last_error = Some(TransferRequestError::ResponseRead);
            }
        }
    }

    Err(last_error.unwrap_or(TransferRequestError::InvalidEndpoint))
}
