use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout};

use crate::iperf::control::NegotiatedParameters;
use crate::iperf::model::{
    IperfClientConfig, IperfDirection, IperfDirectionSummary, IperfProgress,
};
use crate::iperf::proxy::{self, ProxyScheme};
use crate::iperf::udp_packet::{UdpReceiveMetrics, build_packet, parse_header};
use crate::util::mbps_from_bytes;

pub async fn run_udp_direction<F>(
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

    let mut workers = JoinSet::new();
    for worker_id in 0..worker_count {
        let host = config.host.clone();
        let port = config.port;
        let proxy = config.proxy.clone();
        let bitrate_bps = negotiated.bitrate_bps;
        let packet_size = negotiated.packet_size;

        workers.spawn(async move {
            let mut recv_buffer = vec![0_u8; packet_size.max(2048)];
            let mut result = WorkerUdpResult::default();
            let mut next_sequence = ((worker_id as u64) << 32) + 1;

            let per_worker_bitrate = bitrate_bps
                .and_then(|value| value.checked_div(worker_count as u64))
                .filter(|value| *value > 0);

            let sleep_between_packets = per_worker_bitrate.map(|bps| {
                let bits_per_packet = (packet_size as u128) * 8;
                let nanos = (bits_per_packet * 1_000_000_000_u128) / u128::from(bps.max(1));
                Duration::from_nanos(nanos.max(1_000_000) as u64)
            });

            match proxy.as_ref().map(|spec| spec.scheme) {
                None => {
                    let socket = match proxy::connect_udp_socket_direct(&host, port).await {
                        Ok(socket) => socket,
                        Err(_) => return result,
                    };

                    run_udp_worker_loop(
                        direction,
                        stop_at,
                        packet_size,
                        &mut next_sequence,
                        sleep_between_packets,
                        &mut recv_buffer,
                        &mut result,
                        UdpSendRecv::Direct(socket),
                    )
                    .await;
                }
                Some(ProxyScheme::Socks5 | ProxyScheme::Socks5h) => {
                    let association = match proxy::connect_udp_socket_socks5(
                        proxy.as_ref().unwrap(),
                        &host,
                        port,
                    )
                    .await
                    {
                        Ok(association) => association,
                        Err(_) => return result,
                    };

                    run_udp_worker_loop(
                        direction,
                        stop_at,
                        packet_size,
                        &mut next_sequence,
                        sleep_between_packets,
                        &mut recv_buffer,
                        &mut result,
                        UdpSendRecv::Socks(association),
                    )
                    .await;
                }
                Some(ProxyScheme::Http | ProxyScheme::Https) => {
                    return result;
                }
            }

            result
        });
    }

    let mut aggregate_bytes = 0_u64;
    let aggregate_metrics = Arc::new(tokio::sync::Mutex::new(UdpReceiveMetrics::default()));

    if let Some(interval) = progress_interval {
        while Instant::now() < stop_at {
            sleep(interval).await;
            let elapsed = start_at.elapsed();
            let elapsed_secs = elapsed.as_secs().max(1);
            let bytes = aggregate_bytes;
            on_progress(IperfProgress {
                elapsed,
                bytes,
                mbps: mbps_from_bytes(bytes, elapsed_secs),
            });

            while let Some(joined) = workers.try_join_next() {
                if let Ok(worker) = joined {
                    aggregate_bytes += worker.bytes;
                    if matches!(direction, IperfDirection::Download) {
                        let mut guard = aggregate_metrics.lock().await;
                        guard.merge(&worker.receive_metrics);
                    }
                }
            }
        }
    }

    while let Some(joined) = workers.join_next().await {
        if let Ok(worker) = joined {
            aggregate_bytes += worker.bytes;
            if matches!(direction, IperfDirection::Download) {
                let mut guard = aggregate_metrics.lock().await;
                guard.merge(&worker.receive_metrics);
            }
        }
    }

    let final_metrics = aggregate_metrics.lock().await.clone();

    let (packets, lost_packets, loss_percent, jitter_ms, out_of_order) =
        if matches!(direction, IperfDirection::Download) {
            (
                Some(final_metrics.total_packets),
                Some(final_metrics.lost_packets),
                final_metrics.loss_percent(),
                Some(final_metrics.jitter_ms),
                Some(final_metrics.out_of_order),
            )
        } else {
            (None, None, None, None, None)
        };

    Ok(IperfDirectionSummary {
        bytes: aggregate_bytes,
        mbps: mbps_from_bytes(aggregate_bytes, negotiated.seconds),
        duration_seconds: negotiated.seconds,
        packets,
        lost_packets,
        loss_percent,
        jitter_ms,
        out_of_order,
    })
}

#[derive(Default)]
struct WorkerUdpResult {
    bytes: u64,
    receive_metrics: UdpReceiveMetrics,
}

enum UdpSendRecv {
    Direct(tokio::net::UdpSocket),
    Socks(proxy::Socks5UdpAssociation),
}

async fn run_udp_worker_loop(
    direction: IperfDirection,
    stop_at: Instant,
    packet_size: usize,
    next_sequence: &mut u64,
    sleep_between_packets: Option<Duration>,
    recv_buffer: &mut [u8],
    result: &mut WorkerUdpResult,
    mut transport: UdpSendRecv,
) {
    while Instant::now() < stop_at {
        let io_result = match direction {
            IperfDirection::Upload => {
                let payload = build_packet(*next_sequence, packet_size);
                let sent = udp_send(&mut transport, &payload).await;
                *next_sequence += 1;
                sent
            }
            IperfDirection::Download => udp_recv(&mut transport, recv_buffer).await,
        };

        if let Ok(size) = io_result {
            result.bytes += size as u64;
            if matches!(direction, IperfDirection::Download) {
                let header = parse_header(&recv_buffer[..size]);
                result.receive_metrics.on_packet(header);
            }
        }

        if let Some(delay) = sleep_between_packets
            && matches!(direction, IperfDirection::Upload)
        {
            sleep(delay).await;
        }
    }
}

async fn udp_send(transport: &mut UdpSendRecv, payload: &[u8]) -> Result<usize> {
    match transport {
        UdpSendRecv::Direct(socket) => {
            let sent = timeout(Duration::from_secs(1), socket.send(payload)).await??;
            Ok(sent)
        }
        UdpSendRecv::Socks(association) => {
            let sent =
                timeout(Duration::from_secs(1), association.send_to_target(payload)).await??;
            Ok(sent)
        }
    }
}

async fn udp_recv(transport: &mut UdpSendRecv, buffer: &mut [u8]) -> Result<usize> {
    match transport {
        UdpSendRecv::Direct(socket) => {
            let received = timeout(Duration::from_millis(400), socket.recv(buffer)).await;
            match received {
                Ok(Ok(size)) => Ok(size),
                Ok(Err(error)) => Err(error.into()),
                Err(_) => bail!("timeout"),
            }
        }
        UdpSendRecv::Socks(association) => {
            let received = timeout(
                Duration::from_millis(400),
                association.recv_from_target(buffer),
            )
            .await;
            match received {
                Ok(Ok(size)) => Ok(size),
                Ok(Err(error)) => Err(error),
                Err(_) => bail!("timeout"),
            }
        }
    }
}
