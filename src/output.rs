use anyhow::Result;

use crate::model::RunResult;

pub fn print_human(result: &RunResult) {
    println!("timestamp: {}", result.timestamp);

    if let Some(server) = &result.server {
        println!(
            "server: {} ({}, {}, {}, host={}, distance={:.2} km)",
            server.id, server.sponsor, server.name, server.country, server.host, server.distance_km
        );
    } else {
        println!("server: not selected");
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

pub fn print_json(result: &RunResult) -> Result<()> {
    let body = serde_json::to_string_pretty(result)?;
    println!("{}", body);
    Ok(())
}
