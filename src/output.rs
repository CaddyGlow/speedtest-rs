use anyhow::Result;
use serde::Serialize;
use std::env;
use std::io::{self, IsTerminal};

use crate::iperf::schema::{IperfDirectionOut, IperfJsonV1, IperfProtocolOut};
use crate::model::RunResult;

pub fn print_human(result: &RunResult) {
    let theme = Theme::detect();

    println!("{}", theme.section("=== Speedtest Result ==="));
    println!(
        "{}  {}",
        theme.key("timestamp"),
        theme.muted(&result.timestamp)
    );

    if let Some(api) = result.speedtest_api.as_deref() {
        println!("{}       {}", theme.key("mode"), theme.paint(api, "1;35"));
    }
    println!();

    if let Some(server) = &result.server {
        println!("{}", theme.section("Server"));
        println!(
            "  {} {} ({})",
            theme.key("id"),
            theme.paint(&server.id.to_string(), "1;37"),
            server.sponsor
        );
        println!("  {} {}", theme.key("location"), server.name);
        println!("  {} {}", theme.key("country"), server.country);
        println!("  {} {}", theme.key("host"), server.host);
        println!(
            "  {} {}",
            theme.key("distance"),
            theme.muted(&format!("{:.2} km", server.distance_km))
        );
        if let Some(avg) = server.latency_ms {
            let stddev = server
                .latency_stddev_ms
                .map(|value| format!(" (std {})", theme.muted(&format!("{value:.2}ms"))))
                .unwrap_or_default();
            println!(
                "  {} {}{}",
                theme.key("latency"),
                theme.paint(&format!("{avg:.2}ms"), latency_color(avg)),
                stddev
            );
        }
    } else {
        println!("{} {}", theme.key("server"), theme.muted("not selected"));
    }
    println!();

    if let Some(pool) = &result.server_pool
        && !pool.is_empty()
    {
        println!(
            "{} {}",
            theme.section("Server Pool"),
            theme.muted(&format!("({})", pool.len()))
        );
        for server in pool {
            let latency = match (server.latency_ms, server.latency_stddev_ms) {
                (Some(avg), Some(std)) => {
                    format!("lat {avg:.2}ms (std {std:.2}ms)")
                }
                (Some(avg), None) => format!("lat {avg:.2}ms"),
                _ => "lat n/a".to_string(),
            };
            let download = match (server.download_avg_mbps, server.download_bytes) {
                (Some(mbps), Some(bytes)) => format!(
                    "dl {} / {}",
                    theme.paint(&format!("{mbps:.2}Mbps"), throughput_color(mbps)),
                    theme.muted(&format_bytes(bytes))
                ),
                _ => "dl n/a".to_string(),
            };
            println!(
                "  - id={} {:<14} {:<26} | {} | {}",
                theme.paint(&server.id.to_string(), "1;37"),
                server.name,
                server.host,
                latency,
                download
            );
        }
        println!();
    }

    if let Some(client) = &result.client {
        println!("{}", theme.section("Client"));
        println!("  {} {}", theme.key("ip"), client.ip,);
        println!("  {} {}", theme.key("isp"), client.isp,);
        println!("  {} {}", theme.key("country"), client.country,);
        println!(
            "  {} {:.4},{:.4}",
            theme.key("location"),
            client.latitude,
            client.longitude
        );
        println!();
    }

    println!("{}", theme.section("Results"));
    if let Some(ping) = result.ping_ms {
        let jitter_suffix = result
            .jitter_ms
            .map(|j| format!(" (jitter {})", theme.paint(&format!("{j:.2}ms"), latency_color(j))))
            .unwrap_or_default();
        println!(
            "  {} {}{}",
            theme.badge("PING", latency_color(ping)),
            theme.paint(&format!("{ping:.2} ms"), latency_color(ping)),
            jitter_suffix
        );
    }
    if let Some(download) = &result.download {
        let latency_suffix = result
            .download_latency_ms
            .map(|l| {
                format!(
                    " latency {}",
                    theme.paint(&format!("{l:.2}ms"), latency_color(l))
                )
            })
            .unwrap_or_default();
        let duration_str = format_stage_duration(
            download.duration_seconds,
            download.actual_duration_seconds,
        );
        println!(
            "  {} {}  {} in {} ({} workers){}",
            theme.badge("DOWN", throughput_color(download.mbps)),
            theme.paint(
                &format!("{:.2} Mbps", download.mbps),
                throughput_color(download.mbps)
            ),
            format_bytes(download.bytes),
            duration_str,
            download.connections,
            latency_suffix
        );
    }
    if let Some(upload) = &result.upload {
        let latency_suffix = result
            .upload_latency_ms
            .map(|l| {
                format!(
                    " latency {}",
                    theme.paint(&format!("{l:.2}ms"), latency_color(l))
                )
            })
            .unwrap_or_default();
        let duration_str = format_stage_duration(
            upload.duration_seconds,
            upload.actual_duration_seconds,
        );
        println!(
            "  {} {}  {} in {} ({} workers){}",
            theme.badge("UP", throughput_color(upload.mbps)),
            theme.paint(
                &format!("{:.2} Mbps", upload.mbps),
                throughput_color(upload.mbps)
            ),
            format_bytes(upload.bytes),
            duration_str,
            upload.connections,
            latency_suffix
        );
    }
    println!(
        "  {} {}",
        theme.key("proxy"),
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

    fn badge(&self, text: &str, color: &str) -> String {
        self.paint(&format!("[{text}]"), color)
    }
}

fn format_stage_duration(configured: u64, actual: Option<f64>) -> String {
    if let Some(actual) = actual {
        if actual > 0.0 && actual < (configured as f64 - 0.5) {
            return format!("{actual:.1}s");
        }
    }
    format!("{configured}s")
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
