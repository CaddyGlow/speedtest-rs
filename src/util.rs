use anyhow::Result;
use std::env;
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

pub fn resolve_proxy_url(cli_proxy: Option<&str>) -> Option<String> {
    if let Some(cli_proxy) = cli_proxy {
        return Some(cli_proxy.to_string());
    }

    for key in [
        "HTTPS_PROXY",
        "https_proxy",
        "HTTP_PROXY",
        "http_proxy",
        "ALL_PROXY",
        "all_proxy",
    ] {
        if let Ok(value) = env::var(key) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }

    None
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
    use std::env;
    use std::sync::{Mutex, OnceLock};

    use super::{
        MAX_BENCHMARK_WORKERS, clamp_worker_count, mbps_from_bytes, resolve_proxy_url,
        validate_proxy_scheme,
    };

    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    fn with_env_lock(body: impl FnOnce()) {
        let lock = ENV_LOCK.get_or_init(|| Mutex::new(()));
        let _guard = lock.lock().unwrap();
        body();
    }

    fn with_temp_env(name: &str, value: Option<&str>, body: impl FnOnce()) {
        let previous = env::var_os(name);

        match value {
            Some(value) => unsafe { env::set_var(name, value) },
            None => unsafe { env::remove_var(name) },
        }

        body();

        match previous {
            Some(previous) => unsafe { env::set_var(name, previous) },
            None => unsafe { env::remove_var(name) },
        }
    }

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

    #[test]
    fn resolves_proxy_from_cli_before_env() {
        with_env_lock(|| {
            with_temp_env("HTTPS_PROXY", Some("http://env.example:3128"), || {
                assert_eq!(
                    resolve_proxy_url(Some("http://cli.example:8080")),
                    Some("http://cli.example:8080".to_string())
                );
            });
        });
    }

    #[test]
    fn resolves_proxy_from_env_precedence() {
        with_env_lock(|| {
            with_temp_env("ALL_PROXY", Some("http://all.example:1080"), || {
                with_temp_env("HTTPS_PROXY", Some("https://https.example:444"), || {
                    with_temp_env("http_proxy", Some("http://http.example:8080"), || {
                        assert_eq!(
                            resolve_proxy_url(None),
                            Some("https://https.example:444".to_string())
                        );
                    });
                });
            });
        });
    }
}
