use anyhow::Result;
use serde::Serialize;
use std::env;
use std::io::{self, IsTerminal};

use crate::iperf::schema::{IperfDirectionOut, IperfJsonV1, IperfProtocolOut};
use crate::model::RunResult;

pub fn print_human(result: &RunResult) {
    let theme = Theme::detect();

    println!("{}", theme.section("Speedtest Result"));
    println!(
        "{} {}",
        theme.key("timestamp:"),
        theme.muted(&result.timestamp)
    );

    if let Some(api) = result.speedtest_api.as_deref() {
        println!("{} {}", theme.key("mode:"), theme.paint(api, "35"));
    }

    if let Some(server) = &result.server {
        let latency = match (server.latency_ms, server.latency_stddev_ms) {
            (Some(avg), Some(std)) => format!(
                "avg={} std={}",
                theme.paint(&format!("{avg:.2}ms"), latency_color(avg)),
                theme.muted(&format!("{std:.2}ms"))
            ),
            (Some(avg), None) => theme.paint(&format!("avg={avg:.2}ms"), latency_color(avg)),
            _ => "n/a".to_string(),
        };

        println!(
            "{} {} ({}, {}, {}, host={}, distance={}, latency={})",
            theme.key("server:"),
            theme.paint(&server.id.to_string(), "1;37"),
            server.sponsor,
            server.name,
            server.country,
            server.host,
            theme.muted(&format!("{:.2} km", server.distance_km)),
            latency,
        );
    } else {
        println!("{} {}", theme.key("server:"), theme.muted("not selected"));
    }

    if let Some(pool) = &result.server_pool
        && !pool.is_empty()
    {
        println!(
            "{} {}",
            theme.key("server_pool:"),
            theme.muted(&format!("{} servers", pool.len()))
        );
        for server in pool {
            let latency = match (server.latency_ms, server.latency_stddev_ms) {
                (Some(avg), Some(std)) => {
                    format!("avg={avg:.2}ms std={std:.2}ms")
                }
                (Some(avg), None) => format!("avg={avg:.2}ms"),
                _ => "n/a".to_string(),
            };
            let download = match (server.download_avg_mbps, server.download_bytes) {
                (Some(mbps), Some(bytes)) => format!(
                    "avg={} bytes={}",
                    theme.paint(&format!("{mbps:.2}Mbps"), throughput_color(mbps)),
                    theme.muted(&format_bytes(bytes))
                ),
                _ => "n/a".to_string(),
            };
            println!(
                "  - id={} name={} host={} latency={} download={}",
                theme.paint(&server.id.to_string(), "1;37"),
                server.name,
                server.host,
                latency,
                download
            );
        }
    }

    if let Some(client) = &result.client {
        println!(
            "{} ip={} isp={} country={} location={:.4},{:.4}",
            theme.key("client:"),
            client.ip,
            client.isp,
            client.country,
            client.latitude,
            client.longitude
        );
    }

    if let Some(ping) = result.ping_ms {
        println!(
            "{} {}",
            theme.key("ping:"),
            theme.paint(&format!("{ping:.2} ms"), latency_color(ping))
        );
    }
    if let Some(download) = &result.download {
        println!(
            "{} {} ({}, {}s, workers={})",
            theme.key("download:"),
            theme.paint(
                &format!("{:.2} Mbps", download.mbps),
                throughput_color(download.mbps)
            ),
            format_bytes(download.bytes),
            download.duration_seconds,
            download.connections
        );
    }
    if let Some(upload) = &result.upload {
        println!(
            "{} {} ({}, {}s, workers={})",
            theme.key("upload:"),
            theme.paint(
                &format!("{:.2} Mbps", upload.mbps),
                throughput_color(upload.mbps)
            ),
            format_bytes(upload.bytes),
            upload.duration_seconds,
            upload.connections
        );
    }
    println!(
        "{} {}",
        theme.key("proxy:"),
        result
            .proxy
            .as_deref()
            .map(|value| theme.paint(value, "36"))
            .unwrap_or_else(|| theme.muted("none"))
    );
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
    let theme = Theme::detect();
    let protocol = match result.protocol {
        IperfProtocolOut::Tcp => "tcp",
        IperfProtocolOut::Udp => "udp",
    };

    println!("{}", theme.section("iPerf Result"));
    println!("{} {}", theme.key("schema:"), theme.muted(&result.schema));
    println!(
        "{} {}",
        theme.key("timestamp:"),
        theme.muted(&result.timestamp)
    );
    println!(
        "{} {}:{} protocol={}",
        theme.key("target:"),
        result.target.host,
        result.target.port,
        protocol
    );

    if let Some(proxy) = &result.proxy {
        println!("{} {} ({})", theme.key("proxy:"), proxy.url, proxy.scheme);
    } else {
        println!("{} {}", theme.key("proxy:"), theme.muted("none"));
    }

    println!(
        "{} seconds={} parallel={} bitrate_bps={}",
        theme.key("config:"),
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

struct Theme {
    color: bool,
}

impl Theme {
    fn detect() -> Self {
        Self {
            color: io::stdout().is_terminal() && env::var_os("NO_COLOR").is_none(),
        }
    }

    fn paint(&self, text: &str, code: &str) -> String {
        if self.color {
            format!("\x1b[{code}m{text}\x1b[0m")
        } else {
            text.to_string()
        }
    }

    fn section(&self, text: &str) -> String {
        self.paint(text, "1;36")
    }

    fn key(&self, text: &str) -> String {
        self.paint(text, "1;34")
    }

    fn muted(&self, text: &str) -> String {
        self.paint(text, "2")
    }
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];

    let mut value = bytes as f64;
    let mut unit = 0_usize;
    while value >= 1000.0 && unit < UNITS.len() - 1 {
        value /= 1000.0;
        unit += 1;
    }

    if unit == 0 {
        format!("{} {}", bytes, UNITS[unit])
    } else {
        format!("{value:.2} {}", UNITS[unit])
    }
}

fn throughput_color(mbps: f64) -> &'static str {
    if mbps >= 1000.0 {
        "1;32"
    } else if mbps >= 250.0 {
        "32"
    } else if mbps >= 50.0 {
        "33"
    } else {
        "31"
    }
}

fn latency_color(ms: f64) -> &'static str {
    if ms <= 8.0 {
        "1;32"
    } else if ms <= 20.0 {
        "33"
    } else {
        "31"
    }
}
