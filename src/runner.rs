use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use chrono::{SecondsFormat, Utc};
use serde::Serialize;

use crate::cli::{CacheCommand, Cli, Command, IperfArgs, IperfProtocol, RunArgs};
use crate::http;
use crate::iperf;
use crate::iperf::schema::{
    IPERF_SCHEMA_V1, IperfConfigOut, IperfDetailsOut, IperfDirectionDetailsOut, IperfDirectionOut,
    IperfIntervalOut, IperfIntervalResultsOut, IperfJsonV1, IperfProtocolOut, IperfProxyOut,
    IperfResultsOut, IperfTarget,
};
use crate::model::Server;
use crate::output;
use crate::speedtest;
use crate::speedtest::engine::{self, EngineSettings as StageEngineSettings};
use crate::ui;
use crate::util::{clamp_worker_count, resolve_proxy_url};

pub async fn run(cli: Cli) -> Result<()> {
    match cli
        .command
        .unwrap_or_else(|| Cli::default().command.unwrap())
    {
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
    run_speedtest_with_stage_engine(args).await
}

async fn run_speedtest_with_stage_engine(args: RunArgs) -> Result<()> {
    let mut effective_args = args.clone();
    effective_args.download_connections = clamp_worker_count(effective_args.download_connections);
    effective_args.upload_connections = clamp_worker_count(effective_args.upload_connections);

    let render_ui = !effective_args.json && !effective_args.no_progress;
    let mut ui = ui::Ui::new(render_ui);

    ui.render_phase("building HTTP client");

    let proxy = resolve_proxy_url(effective_args.proxy.as_deref());
    if let Some(proxy) = proxy.as_deref() {
        ui.render_metric("proxy", proxy);
    }

    let client = http::build_client(proxy.as_deref())?;

    ui.render_metric("mode", &effective_args.mode.to_string());
    ui.render_metric("pool_size", &effective_args.pool_size.to_string());

    if render_ui {
        ui.render_phase("fetching connectivity info");
        match speedtest::ipinfo::fetch_ipinfo(&client).await {
            Ok(ipinfo) => {
                ui.render_metric("country", ipinfo.country.as_deref().unwrap_or("unknown"));
                ui.render_metric("city", ipinfo.city.as_deref().unwrap_or("unknown"));
                ui.render_metric("ip", ipinfo.ip.as_deref().unwrap_or("unknown"));
                ui.render_metric("org", ipinfo.org.as_deref().unwrap_or("unknown"));
            }
            Err(_) => {
                ui.render_metric("country", "unknown");
                ui.render_metric("city", "unknown");
                ui.render_metric("ip", "unknown");
                ui.render_metric("org", "unknown");
            }
        }
    }

    ui.render_phase("fetching speedtest config");
    let config = with_ctrl_c(speedtest::config::fetch_config(&client)).await?;

    ui.render_phase("fetching speedtest server catalog");

    let fetch_limit = if effective_args.server_id.is_some() {
        effective_args.candidate_servers.max(200)
    } else {
        effective_args.candidate_servers.max(25)
    };
    let servers = with_ctrl_c(speedtest::servers::fetch_servers(
        &client,
        fetch_limit,
        effective_args.server_id,
    ))
    .await?;
    ui.render_metric("catalog_servers", &servers.len().to_string());

    let server_names = servers
        .iter()
        .map(|server| (server.id, server.name.clone()))
        .collect::<HashMap<_, _>>();

    let progress_interval = if render_ui {
        ui.progress_interval()
    } else {
        None
    };

    let transfer_mode = effective_args.mode.into();

    let settings = StageEngineSettings {
        server_id: effective_args.server_id,
        candidate_servers: effective_args.candidate_servers,
        modern_pool_size: effective_args.pool_size,
        latency_samples: effective_args.latency_samples,
        download_connections: effective_args.download_connections,
        upload_connections: effective_args.upload_connections,
        download_seconds: effective_args.download_seconds,
        upload_seconds: effective_args.upload_seconds,
        download_only: effective_args.download_only,
        upload_only: effective_args.upload_only,
        details: effective_args.details,
        progress_interval,
    };

    let mut download_progress = None;
    let mut upload_progress = None;
    let mut download_last = None;
    let mut upload_last = None;
    let mut probe_completed = 0_usize;
    let mut probe_failed = 0_usize;
    let mut probe_best: Option<(u64, String, f64, f64)> = None;

    let outcome = with_ctrl_c(engine::run_speedtest_engine(
        &client,
        &config,
        &servers,
        transfer_mode,
        &settings,
        |event| {
            match event {
                engine::EngineEvent::StageStarting(stage) => match stage {
                    engine::EngineStage::ServerSelection => {
                        ui.render_phase("probing latency across candidates");
                    }
                    engine::EngineStage::Latency => {
                        ui.render_phase("selecting best latency server");
                    }
                    engine::EngineStage::Download => {
                        ui.render_phase("running download test");
                        download_progress =
                            Some(ui.begin_speed_progress("download", effective_args.download_seconds));
                        download_last = None;

                        // keep behavior unchanged: live latency monitor was removed from this path.
                    }
                    engine::EngineStage::Upload => {
                        ui.render_phase("running upload test");
                        upload_progress =
                            Some(ui.begin_speed_progress("upload", effective_args.upload_seconds));
                        upload_last = None;
                    }
                    engine::EngineStage::Save => {
                        ui.render_phase("building result payload");
                    }
                    engine::EngineStage::Finished => {
                        ui.render_phase("benchmark complete");
                    }
                },
                engine::EngineEvent::CandidateProbed {
                    index,
                    total,
                    server_id,
                    average_ms,
                    variance_ms,
                    error,
                } => {
                    probe_completed = probe_completed.saturating_add(1);
                    let server_name = server_names
                        .get(&server_id)
                        .map(String::as_str)
                        .unwrap_or("unknown");
                    if let Some(avg) = average_ms {
                        let stddev = variance_ms.unwrap_or(0.0).max(0.0).sqrt();
                        ui.render_metric(
                            "probe_current",
                            &format!(
                                "{index}/{total} id={server_id} {server_name} avg={avg:.2}ms std={stddev:.2}"
                            ),
                        );

                        let better_than_best = match probe_best.as_ref() {
                            None => true,
                            Some((_, _, best_avg, best_stddev)) => {
                                let avg_better = avg < *best_avg;
                                let avg_tied = (avg - *best_avg).abs() <= 0.000_1;
                                avg_better || (avg_tied && stddev < *best_stddev)
                            }
                        };

                        if better_than_best {
                            probe_best = Some((server_id, server_name.to_string(), avg, stddev));
                        }

                        if let Some((best_id, best_name, best_avg, best_stddev)) = probe_best.as_ref()
                        {
                            ui.render_metric(
                                "probe_best",
                                &format!(
                                    "id={best_id} {best_name} avg={best_avg:.2}ms std={best_stddev:.2}"
                                ),
                            );
                        }
                    } else {
                        probe_failed = probe_failed.saturating_add(1);
                        let reason = error.as_deref().unwrap_or("probe failed");
                        ui.render_metric(
                            "probe_current",
                            &format!("{index}/{total} id={server_id} {server_name} failed ({reason})"),
                        );
                    }

                    ui.render_metric(
                        "probe_progress",
                        &format!("{probe_completed}/{total} complete ({probe_failed} failed)"),
                    );
                }
                engine::EngineEvent::ServerSelected {
                    server_id,
                    average_ms,
                    variance_ms,
                } => {
                    let stddev = variance_ms.max(0.0).sqrt();
                    let server_name = server_names
                        .get(&server_id)
                        .map(String::as_str)
                        .unwrap_or("unknown");
                    ui.render_metric(
                        "selected_server",
                        &format!("id={server_id} {server_name} avg={average_ms:.2}ms std={stddev:.2}"),
                    );
                }
                engine::EngineEvent::StageProgress {
                    stage,
                    elapsed,
                    mbps,
                    bytes,
                    active_connections,
                } => {
                    let _ = active_connections;
                    let sample = ui::SpeedProgressSample {
                        elapsed,
                        mbps,
                        bytes,
                    };
                    match stage {
                        engine::EngineStage::Download => {
                            download_last = Some((mbps, bytes));
                            if let Some(progress) = download_progress.as_ref() {
                                ui.update_speed_progress(progress, sample);
                            }
                        }
                        engine::EngineStage::Upload => {
                            upload_last = Some((mbps, bytes));
                            if let Some(progress) = upload_progress.as_ref() {
                                ui.update_speed_progress(progress, sample);
                            }
                        }
                        _ => {}
                    }
                }
                engine::EngineEvent::StageResult { stage, mbps, bytes } => match stage {
                    engine::EngineStage::Download => {
                        download_last = Some((mbps, bytes));
                    }
                    engine::EngineStage::Upload => {
                        upload_last = Some((mbps, bytes));
                    }
                    _ => {}
                },
                engine::EngineEvent::StageFinished(stage) => match stage {
                    engine::EngineStage::Download => {
                        if let Some(progress) = download_progress.take() {
                            let (mbps, bytes) = download_last.unwrap_or((0.0, 0));
                            ui.finish_speed_progress(progress, "download", mbps, bytes);
                        }
                    }
                    engine::EngineStage::Upload => {
                        if let Some(progress) = upload_progress.take() {
                            let (mbps, bytes) = upload_last.unwrap_or((0.0, 0));
                            ui.finish_speed_progress(progress, "upload", mbps, bytes);
                        }
                    }
                    _ => {}
                },
                engine::EngineEvent::SavePayloadBuilt { guid, hash } => {
                    ui.render_metric(
                        "save",
                        &format!("guid={} hash={}", guid, &hash[..hash.len().min(12)]),
                    );
                }
            }
        },
    ))
    .await;

    let outcome = outcome?;

    ui.shutdown();

    if let Some(output_path) = effective_args.sdk_json_out.as_deref() {
        let sdk_guid = outcome
            .selected_server
            .session_guid
            .clone()
            .unwrap_or_else(speedtest::sdk_payload::generate_sdk_guid);
        speedtest::sdk_payload::write_sdk_result_json_file(
            &outcome.result,
            Path::new(output_path),
            Some(&sdk_guid),
        )
        .with_context(|| format!("failed writing SDK JSON file {output_path}"))?;
    }

    if effective_args.json {
        output::print_json(&outcome.sdk_payload)?;
    } else {
        output::print_human(&outcome.result);
    }

    Ok(())
}

async fn run_iperf(args: IperfArgs) -> Result<()> {
    let proxy_url = resolve_proxy_url(args.proxy.as_deref());
    let proxy_spec = proxy_url
        .as_deref()
        .map(iperf::proxy::parse_proxy)
        .transpose()?;

    iperf::proxy::ensure_compatible(args.protocol, proxy_spec.as_ref())?;

    let details_interval_seconds = 1_u64;
    let details_progress_interval = args
        .details
        .then_some(Duration::from_secs(details_interval_seconds));
    let render_ui = !args.json && !args.no_progress;
    let mut ui = ui::Ui::new(render_ui);
    ui.render_phase("preparing native iperf client");
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

    let (target_host, target_port) = if args.auto_server {
        ui.render_phase("loading iperf server catalog");
        let mut candidates =
            iperf::servers::load_candidates(&args.servers_file, args.protocol, args.port)?;
        let limit = args.candidate_servers.min(candidates.len());
        candidates.truncate(limit);

        ui.render_metric("catalog", &args.servers_file);
        ui.render_metric("candidates", &limit.to_string());
        ui.render_phase("probing iperf latency across candidates");

        let selected = with_ctrl_c(iperf::servers::select_best_server(
            &candidates,
            args.latency_samples,
            proxy_spec.as_ref(),
        ))
        .await?;

        let mut label = format!(
            "{}:{} {:.2}ms",
            selected.host, selected.port, selected.average_latency_ms
        );
        if let Some(region) = selected.region.as_deref() {
            label.push_str(&format!(" [{region}]"));
        }
        if let Some(localization) = selected.localization.as_deref() {
            label.push_str(&format!(" {localization}"));
        }
        ui.render_metric("selected_server", &label);

        (selected.host, selected.port)
    } else {
        let host = args
            .host
            .clone()
            .context("--host is required unless --auto-server is set")?;
        let port = args.port.unwrap_or(5201);
        (host, port)
    };

    ui.render_metric("target", &format!("{}:{}", target_host, target_port));

    let config = iperf::IperfClientConfig {
        host: target_host.clone(),
        port: target_port,
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
    let mut upload_details = None;
    let mut download_details = None;

    for direction in directions {
        let phase = direction.label();
        ui.render_phase(&format!("running iperf {phase}"));
        let progress_interval = details_progress_interval.or_else(|| ui.progress_interval());
        let mut progress = Some(ui.begin_speed_progress(phase, args.seconds));
        let mut intervals = Vec::new();
        let stats = with_ctrl_c(iperf::run_direction(
            &config,
            direction,
            progress_interval,
            |snapshot| {
                if let Some(progress) = progress.as_ref() {
                    ui.update_speed_progress(
                        progress,
                        ui::SpeedProgressSample {
                            elapsed: snapshot.elapsed,
                            mbps: snapshot.mbps,
                            bytes: snapshot.bytes,
                        },
                    );
                }
                if args.details {
                    push_iperf_interval(
                        &mut intervals,
                        snapshot.elapsed.as_secs_f64().min(args.seconds as f64),
                        snapshot.bytes,
                        snapshot.mbps,
                    );
                }
            },
        ))
        .await?;

        if args.details {
            push_iperf_interval(&mut intervals, args.seconds as f64, stats.bytes, stats.mbps);
        }

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
            iperf::IperfDirection::Upload => {
                upload = Some(out);
                upload_details = args
                    .details
                    .then_some(IperfDirectionDetailsOut { intervals });
            }
            iperf::IperfDirection::Download => {
                download = Some(out);
                download_details = args
                    .details
                    .then_some(IperfDirectionDetailsOut { intervals });
            }
        }
    }

    ui.shutdown();

    let result = IperfJsonV1 {
        schema: IPERF_SCHEMA_V1.to_string(),
        timestamp: current_timestamp()?,
        target: IperfTarget {
            host: target_host,
            port: target_port,
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
        details: args.details.then_some(IperfDetailsOut {
            interval_seconds: details_interval_seconds,
            results: IperfIntervalResultsOut {
                upload: upload_details,
                download: download_details,
            },
        }),
    };

    if args.json {
        output::print_iperf_json(&result)?;
    } else {
        output::print_iperf_human(&result);
    }

    Ok(())
}

fn push_iperf_interval(
    intervals: &mut Vec<IperfIntervalOut>,
    elapsed_seconds: f64,
    bytes: u64,
    mbps: f64,
) {
    if let Some(last) = intervals.last()
        && (last.elapsed_seconds - elapsed_seconds).abs() < f64::EPSILON
    {
        if let Some(last_mut) = intervals.last_mut() {
            last_mut.bytes = bytes;
            last_mut.mbps = mbps;
        }
        return;
    }

    intervals.push(IperfIntervalOut {
        elapsed_seconds,
        bytes,
        mbps,
    });
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
    Ok(Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true))
}
