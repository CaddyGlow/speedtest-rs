use anyhow::Result;
use url::Url;

use crate::error::AppError;

pub const MAX_BENCHMARK_WORKERS: usize = 64;

pub fn validate_proxy_scheme(proxy: &str) -> Result<()> {
    let url = Url::parse(proxy)?;
    match url.scheme() {
        "http" | "https" | "socks5" | "socks5h" => Ok(()),
        other => Err(AppError::InvalidArgument(format!(
            "unsupported proxy scheme '{other}' (use http, https, socks5, or socks5h)"
        ))
        .into()),
    }
}

#[must_use]
pub fn clamp_worker_count(requested: usize) -> usize {
    requested.clamp(1, MAX_BENCHMARK_WORKERS)
}

#[must_use]
pub fn mbps_from_bytes(bytes: u64, seconds: u64) -> f64 {
    if seconds == 0 {
        return 0.0;
    }
    let bits = bytes as f64 * 8.0;
    bits / seconds as f64 / 1_000_000.0
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_BENCHMARK_WORKERS, clamp_worker_count, mbps_from_bytes, validate_proxy_scheme,
    };

    #[test]
    fn allows_expected_proxy_schemes() {
        for scheme in ["http", "https", "socks5", "socks5h"] {
            let proxy = format!("{scheme}://127.0.0.1:8080");
            assert!(validate_proxy_scheme(&proxy).is_ok());
        }
    }

    #[test]
    fn rejects_unknown_proxy_scheme() {
        let error = validate_proxy_scheme("ftp://127.0.0.1:21").expect_err("must reject ftp");
        assert!(error.to_string().contains("unsupported proxy scheme"));
    }

    #[test]
    fn clamps_worker_count_into_safe_range() {
        assert_eq!(clamp_worker_count(0), 1);
        assert_eq!(clamp_worker_count(8), 8);
        assert_eq!(
            clamp_worker_count(MAX_BENCHMARK_WORKERS + 10),
            MAX_BENCHMARK_WORKERS
        );
    }

    #[test]
    fn converts_bytes_per_second_to_mbps() {
        let mbps = mbps_from_bytes(125_000_000, 10);
        assert!((mbps - 100.0).abs() < 0.001);
    }
}
