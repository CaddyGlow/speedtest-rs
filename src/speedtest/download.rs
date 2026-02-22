use anyhow::Result;
use reqwest::Client;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::task::JoinSet;
use tokio::time::sleep;

use crate::speedtest::servers::SpeedtestServer;
use crate::util::{clamp_worker_count, mbps_from_bytes};

#[derive(Debug, Clone)]
pub struct DownloadStats {
    pub bytes: u64,
    pub mbps: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct DownloadProgress {
    pub elapsed: Duration,
    pub bytes: u64,
    pub mbps: f64,
}

pub async fn run_download_test<F>(
    client: &Client,
    server: &SpeedtestServer,
    connections: usize,
    seconds: u64,
    progress_interval: Option<Duration>,
    mut on_progress: F,
) -> Result<DownloadStats>
where
    F: FnMut(DownloadProgress),
{
    let worker_count = clamp_worker_count(connections);
    let start_at = Instant::now();
    let stop_at = start_at + Duration::from_secs(seconds);
    let total_bytes = Arc::new(AtomicU64::new(0));

    let mut tasks = JoinSet::new();
    for worker in 0..worker_count {
        let worker_client = client.clone();
        let worker_server = server.clone();
        let worker_bytes = Arc::clone(&total_bytes);
        tasks.spawn(async move {
            const SIZES: [usize; 8] = [500, 750, 1000, 1500, 2000, 2500, 3000, 4000];
            let mut cursor = worker % SIZES.len();

            while Instant::now() < stop_at {
                let size = SIZES[cursor];
                cursor = (cursor + 1) % SIZES.len();

                let Ok(url) = worker_server.download_url(size) else {
                    break;
                };

                let response = match worker_client.get(url).send().await {
                    Ok(response) => match response.error_for_status() {
                        Ok(response) => response,
                        Err(_) => continue,
                    },
                    Err(_) => continue,
                };

                if let Ok(body) = response.bytes().await {
                    worker_bytes.fetch_add(body.len() as u64, Ordering::Relaxed);
                }
            }
        });
    }

    if let Some(interval) = progress_interval {
        while Instant::now() < stop_at {
            sleep(interval).await;
            let elapsed = start_at.elapsed();
            let elapsed_secs = elapsed.as_secs().max(1);
            let bytes = total_bytes.load(Ordering::Relaxed);
            on_progress(DownloadProgress {
                elapsed,
                bytes,
                mbps: mbps_from_bytes(bytes, elapsed_secs),
            });
        }
    }

    while tasks.join_next().await.is_some() {}

    let bytes = total_bytes.load(Ordering::Relaxed);
    Ok(DownloadStats {
        bytes,
        mbps: mbps_from_bytes(bytes, seconds),
    })
}
