use anyhow::Result;
use serde::Serialize;

use crate::iperf::schema::{IperfDirectionOut, IperfJsonV1, IperfProtocolOut};
use crate::model::RunResult;

pub fn print_human(result: &RunResult) {
    println!("timestamp: {}", result.timestamp);

    if let Some(api) = result.speedtest_api.as_deref() {
        println!("speedtest_api: {}", api);
    }

    if let Some(server) = &result.server {
        let latency = match (server.latency_ms, server.latency_stddev_ms) {
            (Some(avg), Some(std)) => format!("avg={avg:.2}ms std={std:.2}ms"),
            (Some(avg), None) => format!("avg={avg:.2}ms"),
            _ => "n/a".to_string(),
        };

        println!(
            "server: {} ({}, {}, {}, host={}, distance={:.2} km, latency={})",
            server.id,
            server.sponsor,
            server.name,
            server.country,
            server.host,
            server.distance_km,
            latency,
        );
    } else {
        println!("server: not selected");
    }

    if let Some(pool) = &result.server_pool
        && !pool.is_empty()
    {
        println!("server_pool ({}):", pool.len());
        for server in pool {
            let latency = match (server.latency_ms, server.latency_stddev_ms) {
                (Some(avg), Some(std)) => format!("avg={avg:.2}ms std={std:.2}ms"),
                (Some(avg), None) => format!("avg={avg:.2}ms"),
                _ => "n/a".to_string(),
            };
            let download = match (server.download_avg_mbps, server.download_bytes) {
                (Some(mbps), Some(bytes)) => format!("avg={mbps:.2}Mbps bytes={bytes}"),
                _ => "n/a".to_string(),
            };
            println!(
                "  - id={} name={} host={} latency={} download={}",
                server.id, server.name, server.host, latency, download
            );
        }
    }

    if let Some(client) = &result.client {
        println!(
            "client: ip={} isp={} country={} location={:.4},{:.4}",
            client.ip, client.isp, client.country, client.latitude, client.longitude
        );
    }

    if let Some(ping) = result.ping_ms {
        println!("ping: {:.2} ms", ping);
    }
    if let Some(download) = &result.download {
        println!(
            "download: {:.2} Mbps (bytes={} duration={}s workers={})",
            download.mbps, download.bytes, download.duration_seconds, download.connections
        );
    }
    if let Some(upload) = &result.upload {
        println!(
            "upload: {:.2} Mbps (bytes={} duration={}s workers={})",
            upload.mbps, upload.bytes, upload.duration_seconds, upload.connections
        );
    }
    println!("proxy: {}", result.proxy.as_deref().unwrap_or("none"));
}

pub fn print_json<T>(result: &T) -> Result<()>
where
    T: Serialize,
{
    let body = serde_json::to_string_pretty(result)?;
    println!("{}", body);
    Ok(())
}

pub fn print_iperf_human(result: &IperfJsonV1) {
    let protocol = match result.protocol {
        IperfProtocolOut::Tcp => "tcp",
        IperfProtocolOut::Udp => "udp",
    };

    println!("schema: {}", result.schema);
    println!("timestamp: {}", result.timestamp);
    println!(
        "target: {}:{} protocol={}",
        result.target.host, result.target.port, protocol
    );

    if let Some(proxy) = &result.proxy {
        println!("proxy: {} ({})", proxy.url, proxy.scheme);
    } else {
        println!("proxy: none");
    }

    println!(
        "config: seconds={} parallel={} bitrate_bps={}",
        result.config.seconds,
        result.config.parallel,
        result
            .config
            .bitrate_bps
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".to_string())
    );

    print_iperf_direction("upload", result.results.upload.as_ref());
    print_iperf_direction("download", result.results.download.as_ref());
}

pub fn print_iperf_json(result: &IperfJsonV1) -> Result<()> {
    let body = serde_json::to_string_pretty(result)?;
    println!("{}", body);
    Ok(())
}

fn print_iperf_direction(label: &str, result: Option<&IperfDirectionOut>) {
    let Some(result) = result else {
        println!("{}: skipped", label);
        return;
    };

    println!(
        "{}: {:.2} Mbps (bytes={} duration={}s)",
        label, result.mbps, result.bytes, result.duration_seconds
    );

    if let Some(packets) = result.packets {
        println!(
            "{} udp: packets={} lost={:?} loss_percent={:?} jitter_ms={:?} out_of_order={:?}",
            label,
            packets,
            result.lost_packets,
            result.loss_percent,
            result.jitter_ms,
            result.out_of_order
        );
    }
}
