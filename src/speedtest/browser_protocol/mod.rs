mod endpoints;
mod latency;
mod transfer;
mod websocket;

use std::sync::atomic::AtomicU64;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Deserialize;
use thiserror::Error;

pub(crate) use endpoints::looks_like_host_with_port;
pub use latency::{
    probe_latency_samples_http, probe_latency_samples_websocket,
    probe_latency_samples_websocket_for_duration_with_sender, stream_upload_stats_samples,
};
pub use transfer::{download, download_streaming, upload, upload_streaming};

static NONCE_COUNTER: AtomicU64 = AtomicU64::new(0);
const WS_PROTOCOL_LEVEL: &str = "2";

#[derive(Debug, Clone, Copy)]
pub struct UploadStatsSample {
    pub bytes: u64,
    pub elapsed_ms: u64,
    pub index: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct RawUploadStatsSample {
    #[serde(rename = "b")]
    bytes: u64,
    #[serde(rename = "e")]
    elapsed_ms: u64,
    #[serde(rename = "i")]
    index: Option<u64>,
    #[serde(rename = "t")]
    sample_type: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum TransferRequestError {
    #[error("HTTP status error")]
    HttpStatus,
    #[error("request transport error")]
    Transport,
    #[error("response read error")]
    ResponseRead,
    #[error("invalid endpoint")]
    InvalidEndpoint,
}

fn truncate_for_log(body: &str) -> String {
    const MAX_LEN: usize = 1024;
    if body.len() <= MAX_LEN {
        return body.to_string();
    }

    let mut cutoff = MAX_LEN;
    while !body.is_char_boundary(cutoff) {
        cutoff = cutoff.saturating_sub(1);
    }

    let mut text = body[..cutoff].to_string();
    text.push_str("...");
    text
}

fn next_nocache_token() -> String {
    let counter = NONCE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    format!("{now:x}-{counter:x}")
}
