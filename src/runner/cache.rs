use anyhow::Result;
use serde::Serialize;

use crate::cli::CacheCommand;
use crate::model::Server;
use crate::speedtest;
use crate::speedtest::servers::SpeedtestServer;

#[derive(Debug, Serialize)]
struct CacheShowOutput {
    cache_path: String,
    total_cached: usize,
    filtered: usize,
    shown: usize,
    servers: Vec<Server>,
}

pub(super) fn run_cache_command(command: CacheCommand) -> Result<()> {
    match command {
        CacheCommand::Path => {
            let path = speedtest::servers::cache_file_path()?;
            println!("{}", path.display());
            Ok(())
        }
        CacheCommand::Clear => {
            let path = speedtest::servers::cache_file_path()?;
            let removed = speedtest::servers::clear_cached_servers()?;
            if removed {
                println!("cleared cache file: {}", path.display());
            } else {
                println!("cache file not found: {}", path.display());
            }
            Ok(())
        }
        CacheCommand::Show(show) => {
            let path = speedtest::servers::cache_file_path()?;
            let cached = speedtest::servers::load_cached_servers()?;
            let filtered = speedtest::servers::filter_servers(&cached, show.search.as_deref());
            let filtered_count = filtered.len();
            let displayed = filtered.into_iter().take(show.limit).collect::<Vec<_>>();

            let servers = displayed
                .into_iter()
                .map(cache_server_to_output)
                .collect::<Vec<_>>();

            if show.json {
                let body = CacheShowOutput {
                    cache_path: path.display().to_string(),
                    total_cached: cached.len(),
                    filtered: filtered_count,
                    shown: servers.len(),
                    servers,
                };
                println!("{}", serde_json::to_string_pretty(&body)?);
                return Ok(());
            }

            println!("cache: {}", path.display());
            println!("total cached: {}", cached.len());
            if let Some(search) = show.search.as_deref() {
                println!("search: {}", search);
                println!("filtered: {}", filtered_count);
            }
            println!("showing: {}", servers.len());

            if servers.is_empty() {
                println!("no cached servers match");
                return Ok(());
            }

            for server in servers {
                println!(
                    "- {} | {} | {}, {} | {}",
                    server.id, server.sponsor, server.name, server.country, server.host
                );
            }

            Ok(())
        }
    }
}

fn cache_server_to_output(server: &SpeedtestServer) -> Server {
    Server {
        id: server.id,
        sponsor: server.sponsor.clone(),
        name: server.name.clone(),
        country: server.country.clone(),
        host: server.host.clone(),
        distance_km: server.distance_km,
        latency_ms: None,
        latency_stddev_ms: None,
        download_avg_mbps: None,
        download_bytes: None,
        sdk_url: None,
        sdk_lat: None,
        sdk_lon: None,
        sdk_cc: None,
        sdk_preferred: None,
        sdk_isp_id: None,
        sdk_https_functional: None,
        sdk_hostname: None,
        sdk_port: None,
        sdk_force_ping_select: None,
    }
}
