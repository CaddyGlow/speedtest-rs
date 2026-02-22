use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout};

use crate::iperf::control::NegotiatedParameters;
use crate::iperf::model::{
    IperfClientConfig, IperfDirection, IperfDirectionSummary, IperfProgress,
};
use crate::iperf::proxy;
use crate::util::mbps_from_bytes;

pub async fn run_tcp_direction<F>(
    config: &IperfClientConfig,
    negotiated: NegotiatedParameters,
    direction: IperfDirection,
    progress_interval: Option<Duration>,
    mut on_progress: F,
) -> Result<IperfDirectionSummary>
where
    F: FnMut(IperfProgress),
{
    let worker_count = negotiated.parallel;
    let start_at = Instant::now();
    let stop_at = start_at + Duration::from_secs(negotiated.seconds);
    let total_bytes = Arc::new(AtomicU64::new(0));

    let mut workers = JoinSet::new();
    for _ in 0..worker_count {
        let host = config.host.clone();
        let port = config.port;
        let proxy = config.proxy.clone();
        let bytes_counter = Arc::clone(&total_bytes);
        workers.spawn(async move {
            let upload_payload = vec![0x55_u8; negotiated.packet_size];
            let mut download_buffer = vec![0_u8; negotiated.packet_size];

            while Instant::now() < stop_at {
                let mut stream = match timeout(
                    Duration::from_secs(5),
                    proxy::connect_tcp_target(&host, port, proxy.as_ref()),
                )
                .await
                {
                    Ok(Ok(stream)) => stream,
                    _ => {
                        sleep(Duration::from_millis(200)).await;
                        continue;
                    }
                };

                while Instant::now() < stop_at {
                    match direction {
                        IperfDirection::Upload => {
                            let write_result =
                                timeout(Duration::from_secs(2), stream.write_all(&upload_payload))
                                    .await;

                            match write_result {
                                Ok(Ok(())) => {
                                    bytes_counter
                                        .fetch_add(upload_payload.len() as u64, Ordering::Relaxed);
                                }
                                _ => break,
                            }
                        }
                        IperfDirection::Download => {
                            let read_result =
                                timeout(Duration::from_secs(2), stream.read(&mut download_buffer))
                                    .await;

                            match read_result {
                                Ok(Ok(0)) => break,
                                Ok(Ok(read)) => {
                                    bytes_counter.fetch_add(read as u64, Ordering::Relaxed);
                                }
                                Ok(Err(_)) => break,
                                Err(_) => continue,
                            }
                        }
                    }
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
            on_progress(IperfProgress {
                elapsed,
                bytes,
                mbps: mbps_from_bytes(bytes, elapsed_secs),
            });
        }
    }

    while workers.join_next().await.is_some() {}

    let bytes = total_bytes.load(Ordering::Relaxed);
    Ok(IperfDirectionSummary {
        bytes,
        mbps: mbps_from_bytes(bytes, negotiated.seconds),
        duration_seconds: negotiated.seconds,
        packets: None,
        lost_packets: None,
        loss_percent: None,
        jitter_ms: None,
        out_of_order: None,
    })
}
