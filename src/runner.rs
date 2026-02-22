use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde::Serialize;

use crate::cli::{CacheCommand, Cli, Command, IperfArgs, IperfProtocol, RunArgs};
use crate::http;
use crate::iperf;
use crate::iperf::schema::{
    IPERF_SCHEMA_V1, IperfConfigOut, IperfDirectionOut, IperfJsonV1, IperfProtocolOut,
    IperfProxyOut, IperfResultsOut, IperfTarget,
};
use crate::model::{BenchmarkResult, ClientMeta, RunResult, Server};
use crate::output;
use crate::speedtest;
use crate::ui;
use crate::util::clamp_worker_count;

pub async fn run(cli: Cli) -> Result<()> {
    match cli
        .command
        .unwrap_or_else(|| Cli::default().command.unwrap())
    {
        Command::Plan => {
            let plan = include_str!("../PLAN.md");
            println!("{}", plan);
            Ok(())
        }
        Command::Cache(cache) => run_cache_command(cache.command),
        Command::Run(args) => run_speedtest(args).await,
        Command::Iperf(args) => run_iperf(args).await,
    }
}

#[derive(Debug, Serialize)]
struct CacheShowOutput {
    cache_path: String,
    total_cached: usize,
    filtered: usize,
    shown: usize,
    servers: Vec<Server>,
}

fn run_cache_command(command: CacheCommand) -> Result<()> {
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
                .map(|server| Server {
                    id: server.id,
                    sponsor: server.sponsor.clone(),
                    name: server.name.clone(),
                    country: server.country.clone(),
                    host: server.host.clone(),
                    distance_km: server.distance_km,
                    latency_ms: None,
                })
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

async fn run_speedtest(args: RunArgs) -> Result<()> {
    let mut effective_args = args.clone();
    effective_args.download_connections = clamp_worker_count(effective_args.download_connections);
    effective_args.upload_connections = clamp_worker_count(effective_args.upload_connections);
    let render_ui = !effective_args.json;
    let mut ui = ui::Ui::new(effective_args.tui, render_ui);

    ui.render_phase("building HTTP client");
    let client = http::build_client(effective_args.proxy.as_deref())?;

    ui.render_phase("fetching speedtest config");
    let config = with_ctrl_c(speedtest::config::fetch_config(&client)).await?;

    ui.render_phase("fetching speedtest server catalog");
    let fetch_limit = if effective_args.server_id.is_some() {
        effective_args.candidate_servers.max(200)
    } else {
        effective_args.candidate_servers.max(25)
    };
    let servers = with_ctrl_c(speedtest::servers::fetch_servers(&client, fetch_limit)).await?;

    let selected = if let Some(requested_id) = effective_args.server_id {
        let mut server = servers
            .iter()
            .find(|candidate| candidate.id == requested_id)
            .cloned();

        if server.is_none() {
            ui.render_phase("expanding server catalog for explicit server-id");
            let expanded = with_ctrl_c(speedtest::servers::fetch_servers(&client, 5_000)).await?;
            server = expanded
                .into_iter()
                .find(|candidate| candidate.id == requested_id);
        }

        let server = server.with_context(|| {
            let proxy_hint = if effective_args.proxy.is_some() {
                " and current proxy route"
            } else {
                ""
            };
            format!(
                "server id {} not found in current speedtest catalog for this network context{}",
                requested_id, proxy_hint
            )
        })?;

        let (average_ms, variance_ms) = speedtest::select::probe_server_latency(
            &client,
            &server,
            effective_args.latency_samples,
        )
        .await?;
        speedtest::select::ServerLatency {
            server,
            average_ms,
            variance_ms,
        }
    } else {
        ui.render_phase("probing latency across candidates");
        with_ctrl_c(speedtest::select::probe_and_select_best(
            &client,
            &servers,
            effective_args.candidate_servers,
            effective_args.latency_samples,
        ))
        .await?
    };

    ui.render_metric(
        "selected_server",
        &format!(
            "id={} host={} ping={:.2}ms variance={:.2}",
            selected.server.id, selected.server.host, selected.average_ms, selected.variance_ms
        ),
    );

    let download = if effective_args.upload_only {
        None
    } else {
        let progress_interval = ui.progress_interval();
        let mut progress =
            Some(ui.begin_speed_progress("download", effective_args.download_seconds));
        let stats = with_ctrl_c(speedtest::download::run_download_test(
            &client,
            &selected.server,
            effective_args.download_connections,
            effective_args.download_seconds,
            progress_interval,
            |snapshot| {
                if let Some(progress) = progress.as_ref() {
                    ui.update_speed_progress(
                        progress,
                        snapshot.elapsed,
                        snapshot.mbps,
                        snapshot.bytes,
                    );
                }
            },
        ))
        .await?;

        if let Some(progress) = progress.take() {
            ui.finish_speed_progress(progress, "download", stats.mbps, stats.bytes);
        }

        Some(BenchmarkResult {
            mbps: stats.mbps,
            bytes: stats.bytes,
            duration_seconds: effective_args.download_seconds,
            connections: effective_args.download_connections,
        })
    };

    let upload = if effective_args.download_only {
        None
    } else {
        let progress_interval = ui.progress_interval();
        let mut progress = Some(ui.begin_speed_progress("upload", effective_args.upload_seconds));
        let stats = with_ctrl_c(speedtest::upload::run_upload_test(
            &client,
            &selected.server,
            effective_args.upload_connections,
            effective_args.upload_seconds,
            progress_interval,
            |snapshot| {
                if let Some(progress) = progress.as_ref() {
                    ui.update_speed_progress(
                        progress,
                        snapshot.elapsed,
                        snapshot.mbps,
                        snapshot.bytes,
                    );
                }
            },
        ))
        .await?;

        if let Some(progress) = progress.take() {
            ui.finish_speed_progress(progress, "upload", stats.mbps, stats.bytes);
        }

        Some(BenchmarkResult {
            mbps: stats.mbps,
            bytes: stats.bytes,
            duration_seconds: effective_args.upload_seconds,
            connections: effective_args.upload_connections,
        })
    };

    let result = RunResult {
        timestamp: current_timestamp()?,
        client: Some(ClientMeta {
            ip: config.client.ip,
            isp: config.client.isp,
            country: config.client.country,
            latitude: config.client.latitude,
            longitude: config.client.longitude,
        }),
        server: Some(Server {
            id: selected.server.id,
            sponsor: selected.server.sponsor,
            name: selected.server.name,
            country: selected.server.country,
            host: selected.server.host,
            distance_km: selected.server.distance_km,
            latency_ms: Some(selected.average_ms),
        }),
        ping_ms: Some(selected.average_ms),
        download,
        upload,
        proxy: effective_args.proxy,
    };

    ui.shutdown();

    if effective_args.json {
        output::print_json(&result)?;
    } else {
        output::print_human(&result);
    }

    Ok(())
}

async fn run_iperf(args: IperfArgs) -> Result<()> {
    let proxy_spec = args
        .proxy
        .as_deref()
        .map(iperf::proxy::parse_proxy)
        .transpose()?;

    iperf::proxy::ensure_compatible(args.protocol, proxy_spec.as_ref())?;

    let render_ui = !args.json;
    let mut ui = ui::Ui::new(args.tui, render_ui);
    ui.render_phase("preparing native iperf client");
    ui.render_metric("target", &format!("{}:{}", args.host, args.port));
    ui.render_metric(
        "protocol",
        match args.protocol {
            IperfProtocol::Tcp => "tcp",
            IperfProtocol::Udp => "udp",
        },
    );
    ui.render_metric("parallel", &args.parallel.to_string());
    if let Some(proxy) = proxy_spec.as_ref() {
        ui.render_metric("proxy", &proxy.raw_url);
    }

    let config = iperf::IperfClientConfig {
        host: args.host.clone(),
        port: args.port,
        protocol: args.protocol,
        seconds: args.seconds,
        parallel: args.parallel,
        bitrate_bps: args.bitrate,
        proxy: proxy_spec.clone(),
    };

    let directions = if args.upload_only {
        vec![iperf::IperfDirection::Upload]
    } else if args.download_only {
        vec![iperf::IperfDirection::Download]
    } else {
        vec![
            iperf::IperfDirection::Upload,
            iperf::IperfDirection::Download,
        ]
    };

    let mut upload = None;
    let mut download = None;

    for direction in directions {
        let phase = direction.label();
        ui.render_phase(&format!("running iperf {phase}"));
        let progress_interval = ui.progress_interval();
        let mut progress = Some(ui.begin_speed_progress(phase, args.seconds));
        let stats = with_ctrl_c(iperf::run_direction(
            &config,
            direction,
            progress_interval,
            |snapshot| {
                if let Some(progress) = progress.as_ref() {
                    ui.update_speed_progress(
                        progress,
                        snapshot.elapsed,
                        snapshot.mbps,
                        snapshot.bytes,
                    );
                }
            },
        ))
        .await?;

        if let Some(progress) = progress.take() {
            ui.finish_speed_progress(progress, phase, stats.mbps, stats.bytes);
        }

        let out = IperfDirectionOut {
            bytes: stats.bytes,
            mbps: stats.mbps,
            duration_seconds: stats.duration_seconds,
            packets: stats.packets,
            lost_packets: stats.lost_packets,
            loss_percent: stats.loss_percent,
            jitter_ms: stats.jitter_ms,
            out_of_order: stats.out_of_order,
        };

        match direction {
            iperf::IperfDirection::Upload => upload = Some(out),
            iperf::IperfDirection::Download => download = Some(out),
        }
    }

    ui.shutdown();

    let result = IperfJsonV1 {
        schema: IPERF_SCHEMA_V1.to_string(),
        timestamp: current_timestamp()?,
        target: IperfTarget {
            host: args.host,
            port: args.port,
        },
        protocol: match args.protocol {
            IperfProtocol::Tcp => IperfProtocolOut::Tcp,
            IperfProtocol::Udp => IperfProtocolOut::Udp,
        },
        proxy: proxy_spec.as_ref().map(|proxy| IperfProxyOut {
            url: proxy.raw_url.clone(),
            scheme: proxy.scheme.as_str().to_string(),
        }),
        config: IperfConfigOut {
            seconds: args.seconds,
            parallel: args.parallel,
            bitrate_bps: args.bitrate,
        },
        results: IperfResultsOut { upload, download },
    };

    if args.json {
        output::print_iperf_json(&result)?;
    } else {
        output::print_iperf_human(&result);
    }

    Ok(())
}

async fn with_ctrl_c<F, T>(future: F) -> Result<T>
where
    F: std::future::Future<Output = Result<T>>,
{
    tokio::select! {
        output = future => output,
        _ = tokio::signal::ctrl_c() => bail!("benchmark interrupted by Ctrl-C"),
    }
}

fn current_timestamp() -> Result<String> {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    Ok(timestamp.to_string())
}
