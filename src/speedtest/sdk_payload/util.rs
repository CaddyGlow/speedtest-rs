use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Result, bail};

use crate::model::BenchmarkResult;

static GUID_COUNTER: AtomicU64 = AtomicU64::new(0);

#[must_use]
pub(crate) fn generate_sdk_guid() -> String {
    let counter = GUID_COUNTER.fetch_add(1, Ordering::Relaxed);
    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(0);

    let part1 = (now_nanos >> 32) as u32;
    let part2 = ((now_nanos >> 16) & 0xffff) as u16;
    let part3 = (now_nanos & 0xffff) as u16;
    let part4 = ((counter >> 48) & 0xffff) as u16;
    let part5 = (counter & 0xffffffffffff) ^ (now_nanos & 0xffffffffffff);

    format!("{part1:08x}-{part2:04x}-{part3:04x}-{part4:04x}-{part5:012x}")
}

pub(super) fn mbps_to_bps(result: &BenchmarkResult) -> Result<u64> {
    if !result.mbps.is_finite() || result.mbps < 0.0 {
        bail!("throughput Mbps must be a finite non-negative number");
    }
    Ok((result.mbps * 1_000_000.0).round() as u64)
}

pub(super) fn bps_to_sdk_units(bps: u64) -> u64 {
    ((bps as f64) / 125.0).round() as u64
}

pub(super) fn calculate_result_hash(
    ping: f64,
    upload: Option<u64>,
    download: Option<u64>,
) -> String {
    let ping = if ping.is_finite() { ping } else { 0.0 };
    let upload = upload.unwrap_or(0);
    let download = download.unwrap_or(0);
    let hash_input = format!("{ping}-{upload}-{download}-817d699764d33f89c");
    format!("{:x}", md5::compute(hash_input))
}
