use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde::Serialize;

use crate::cli::{CacheCommand, Cli, Command, IperfArgs, IperfProtocol, RunArgs};
use crate::http;
use crate::iperf;
use crate::iperf::schema::{
    IPERF_SCHEMA_V1, IperfConfigOut, IperfDetailsOut, IperfDirectionDetailsOut, IperfDirectionOut,
    IperfIntervalOut, IperfIntervalResultsOut, IperfJsonV1, IperfProtocolOut, IperfProxyOut,
    IperfResultsOut, IperfTarget,
};
use crate::model::{
    BenchmarkResult, ClientMeta, DirectionDetails, RunDetails, RunResult,
    SelectedServerLatencyDetails, Server, ThroughputInterval,
};
use crate::output;
use crate::speedtest;
use crate::speedtest::api::{ModernTransportMode, ResolvedSpeedtestApi, SpeedtestApiMode};
use crate::speedtest::servers::SpeedtestServer;
use crate::ui;
use crate::util::clamp_worker_count;

#[derive(Debug, Clone, Copy)]
struct LiveLatencySnapshot {
    latency_ms: Option<f64>,
    jitter_ms: Option<f64>,
}

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
    let mut effective_args = args.clone();
    effective_args.download_connections = clamp_worker_count(effective_args.download_connections);
    effective_args.upload_connections = clamp_worker_count(effective_args.upload_connections);
    let details_interval_seconds = 1_u64;
    let details_progress_interval = effective_args
        .details
        .then_some(Duration::from_secs(details_interval_seconds));
    let collect_sdk_payload_metrics = effective_args.json || effective_args.sdk_json_out.is_some();
    let sdk_progress_interval = collect_sdk_payload_metrics.then_some(Duration::from_secs(1));
    let render_ui = !effective_args.json;
    let mut ui = ui::Ui::new(effective_args.tui, render_ui);

    ui.render_phase("building HTTP client");
    let client = http::build_client(effective_args.proxy.as_deref())?;

    ui.render_phase("fetching speedtest config");
    let config = with_ctrl_c(speedtest::config::fetch_config(&client)).await?;

    ui.render_phase("fetching speedtest server catalog");
    let requested_api_mode = match effective_args.speedtest_api {
        SpeedtestApiMode::ModernTcp => SpeedtestApiMode::Modern,
        mode => mode,
    };
    let fetch_limit = if effective_args.server_id.is_some() {
        effective_args.candidate_servers.max(200)
    } else {
        effective_args.candidate_servers.max(25)
    };
    let client_location = (config.client.latitude, config.client.longitude);
    let (servers, resolved_api) = with_ctrl_c(speedtest::servers::fetch_servers(
        &client,
        requested_api_mode,
        fetch_limit,
        Some(client_location),
    ))
    .await?;
    let transfer_api = match resolved_api {
        ResolvedSpeedtestApi::Modern
            if matches!(effective_args.modern_mode, ModernTransportMode::Tcp)
                || matches!(effective_args.speedtest_api, SpeedtestApiMode::ModernTcp) =>
        {
            ResolvedSpeedtestApi::ModernTcp
        }
        mode => mode,
    };
    let latency_candidates = if matches!(resolved_api, ResolvedSpeedtestApi::Modern) {
        let scoped = servers
            .iter()
            .filter(|server| server.session_guid.is_some())
            .cloned()
            .collect::<Vec<_>>();
        if scoped.is_empty() {
            servers.clone()
        } else {
            scoped
        }
    } else {
        servers.clone()
    };

    let (selected, transfer_pool, latency_by_server) =
        if let Some(requested_id) = effective_args.server_id {
            let mut server = servers
                .iter()
                .find(|candidate| candidate.id == requested_id)
                .cloned();

            if server.is_none() {
                ui.render_phase("expanding server catalog for explicit server-id");
                let expanded = with_ctrl_c(speedtest::servers::fetch_servers_for_api(
                    &client,
                    resolved_api,
                    5_000,
                    Some(client_location),
                ))
                .await?;
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

            let measurement = speedtest::select::probe_server_latency(
                &client,
                &server,
                effective_args.latency_samples,
                resolved_api,
            )
            .await?;
            (
                speedtest::select::ServerLatency {
                    server: server.clone(),
                    average_ms: measurement.average_ms,
                    variance_ms: measurement.variance_ms,
                    samples_ms: measurement.samples_ms.clone(),
                },
                vec![server],
                HashMap::from([(
                    requested_id,
                    (
                        measurement.average_ms,
                        measurement.variance_ms.max(0.0).sqrt(),
                    ),
                )]),
            )
        } else {
            ui.render_phase("probing latency across candidates");
            let ranked = with_ctrl_c(speedtest::select::probe_and_rank_candidates_with_progress(
                &client,
                &latency_candidates,
                resolved_api,
                effective_args.candidate_servers,
                effective_args.latency_samples,
                |index, total, server, outcome, error| match outcome {
                    Some(measurement) => {
                        let samples = measurement
                            .samples_ms
                            .iter()
                            .map(|sample| format!("{sample:.2}"))
                            .collect::<Vec<_>>()
                            .join(",");
                        ui.render_metric(
                            "latency_probe",
                            &format!(
                                "{index}/{total} id={} {} pings=[{}] avg={:.2}ms std={:.2}",
                                server.id,
                                server.name,
                                samples,
                                measurement.average_ms,
                                measurement.variance_ms.max(0.0).sqrt()
                            ),
                        );
                    }
                    None => {
                        let reason = error.unwrap_or_else(|| "probe failed".to_string());
                        ui.render_metric(
                            "latency_probe",
                            &format!(
                                "{index}/{total} id={} {} failed ({reason})",
                                server.id, server.name
                            ),
                        );
                    }
                },
            ))
            .await?;

            let selected = speedtest::select::select_best_latency(&ranked)
                .context("no ranked speedtest candidates were produced")?;
            let transfer_pool = build_transfer_pool(
                &ranked,
                resolved_api,
                effective_args.modern_pool_size.max(1),
            );
            let latency_by_server = ranked
                .iter()
                .map(|entry| {
                    (
                        entry.server.id,
                        (entry.average_ms, entry.variance_ms.max(0.0).sqrt()),
                    )
                })
                .collect::<HashMap<_, _>>();

            (selected, transfer_pool, latency_by_server)
        };

    ui.render_metric("server_pool", &format_server_pool_metric(&transfer_pool));

    ui.render_metric(
        "selected_server",
        &format_selected_server_metric(
            &selected.server,
            selected.average_ms,
            selected.variance_ms.max(0.0).sqrt(),
            &transfer_pool,
        ),
    );

    let (
        download,
        download_details,
        download_by_server,
        sdk_download_intervals,
        sdk_download_latency_samples_ms,
    ) = if effective_args.upload_only {
        (None, None, HashMap::new(), None, None)
    } else {
        let progress_interval = details_progress_interval
            .or_else(|| ui.progress_interval())
            .or(sdk_progress_interval);
        let mut progress =
            Some(ui.begin_speed_progress("download", effective_args.download_seconds));
        let mut intervals = Vec::new();
        let mut sdk_intervals = Vec::new();
        let mut sdk_latency_task = collect_sdk_payload_metrics.then(|| {
            let stage_client = client.clone();
            let stage_server = selected.server.clone();
            tokio::spawn(async move {
                speedtest::select::collect_loaded_latency_samples(
                    &stage_client,
                    &stage_server,
                    resolved_api,
                    effective_args.download_seconds,
                )
                .await
            })
        });
        let selected_stddev_ms = selected.variance_ms.max(0.0).sqrt();
        let (live_latency_state, mut live_latency_task) = if !effective_args.json {
            if let Some(interval) = progress_interval {
                let (state, task) = spawn_live_latency_monitor(
                    client.clone(),
                    selected.server.clone(),
                    resolved_api,
                    interval,
                    selected.average_ms,
                    selected_stddev_ms,
                );
                (Some(state), Some(task))
            } else {
                (None, None)
            }
        } else {
            (None, None)
        };
        let stats = with_ctrl_c(speedtest::download::run_download_test(
            &client,
            &selected.server,
            transfer_api,
            &transfer_pool,
            effective_args.download_connections,
            effective_args.download_seconds,
            progress_interval,
            |snapshot| {
                if let Some(progress) = progress.as_ref() {
                    let live_latency = live_latency_state
                        .as_ref()
                        .map(read_live_latency_snapshot)
                        .unwrap_or(LiveLatencySnapshot {
                            latency_ms: Some(selected.average_ms),
                            jitter_ms: Some(selected_stddev_ms),
                        });
                    ui.update_speed_progress(
                        progress,
                        ui::SpeedProgressSample {
                            elapsed: snapshot.elapsed,
                            mbps: snapshot.mbps,
                            bytes: snapshot.bytes,
                            active_connections: snapshot.active_connections,
                            latency_ms: live_latency.latency_ms,
                            jitter_ms: live_latency.jitter_ms,
                        },
                    );
                }
                if effective_args.details {
                    push_run_interval(
                        &mut intervals,
                        snapshot
                            .elapsed
                            .as_secs_f64()
                            .min(effective_args.download_seconds as f64),
                        snapshot.bytes,
                        snapshot.mbps,
                    );
                }
                push_run_interval(
                    &mut sdk_intervals,
                    snapshot
                        .elapsed
                        .as_secs_f64()
                        .min(effective_args.download_seconds as f64),
                    snapshot.bytes,
                    snapshot.mbps,
                );
            },
        ))
        .await?;

        if let Some(task) = live_latency_task.take() {
            task.abort();
            let _ = task.await;
        }

        let sdk_download_latency_samples_ms = if let Some(task) = sdk_latency_task.take() {
            match task.await {
                Ok(samples) if !samples.is_empty() => Some(samples),
                _ => None,
            }
        } else {
            None
        };

        if effective_args.details {
            push_run_interval(
                &mut intervals,
                effective_args.download_seconds as f64,
                stats.bytes,
                stats.mbps,
            );
        }
        push_run_interval(
            &mut sdk_intervals,
            effective_args.download_seconds as f64,
            stats.bytes,
            stats.mbps,
        );

        if let Some(progress) = progress.take() {
            ui.finish_speed_progress(progress, "download", stats.mbps, stats.bytes);
        }

        (
            Some(BenchmarkResult {
                mbps: stats.mbps,
                bytes: stats.bytes,
                duration_seconds: effective_args.download_seconds,
                connections: effective_args.download_connections,
            }),
            effective_args.details.then_some(DirectionDetails {
                request_attempts: stats.request_attempts,
                request_successes: stats.request_successes,
                request_http_errors: stats.request_http_errors,
                request_transport_errors: stats.request_transport_errors,
                response_read_errors: stats.response_read_errors,
                intervals,
                remote_intervals: None,
            }),
            stats
                .per_server
                .into_iter()
                .map(|entry| (entry.server_id, (entry.bytes, entry.mbps)))
                .collect::<HashMap<_, _>>(),
            (!sdk_intervals.is_empty()).then_some(sdk_intervals),
            sdk_download_latency_samples_ms,
        )
    };

    let (
        upload,
        upload_details,
        sdk_upload_intervals,
        sdk_upload_remote_intervals,
        sdk_upload_latency_samples_ms,
    ) = if effective_args.download_only {
        (None, None, None, None, None)
    } else {
        let progress_interval = details_progress_interval
            .or_else(|| ui.progress_interval())
            .or(sdk_progress_interval);
        let mut progress = Some(ui.begin_speed_progress("upload", effective_args.upload_seconds));
        let mut intervals = Vec::new();
        let mut sdk_intervals = Vec::new();
        let mut sdk_latency_task = collect_sdk_payload_metrics.then(|| {
            let stage_client = client.clone();
            let stage_server = selected.server.clone();
            tokio::spawn(async move {
                speedtest::select::collect_loaded_latency_samples(
                    &stage_client,
                    &stage_server,
                    resolved_api,
                    effective_args.upload_seconds,
                )
                .await
            })
        });
        let selected_stddev_ms = selected.variance_ms.max(0.0).sqrt();
        let (live_latency_state, mut live_latency_task) = if !effective_args.json {
            if let Some(interval) = progress_interval {
                let (state, task) = spawn_live_latency_monitor(
                    client.clone(),
                    selected.server.clone(),
                    resolved_api,
                    interval,
                    selected.average_ms,
                    selected_stddev_ms,
                );
                (Some(state), Some(task))
            } else {
                (None, None)
            }
        } else {
            (None, None)
        };
        let stats = with_ctrl_c(speedtest::upload::run_upload_test(
            &client,
            &selected.server,
            transfer_api,
            &transfer_pool,
            effective_args.upload_connections,
            effective_args.upload_seconds,
            progress_interval,
            |snapshot| {
                if let Some(progress) = progress.as_ref() {
                    let live_latency = live_latency_state
                        .as_ref()
                        .map(read_live_latency_snapshot)
                        .unwrap_or(LiveLatencySnapshot {
                            latency_ms: Some(selected.average_ms),
                            jitter_ms: Some(selected_stddev_ms),
                        });
                    ui.update_speed_progress(
                        progress,
                        ui::SpeedProgressSample {
                            elapsed: snapshot.elapsed,
                            mbps: snapshot.mbps,
                            bytes: snapshot.bytes,
                            active_connections: snapshot.active_connections,
                            latency_ms: live_latency.latency_ms,
                            jitter_ms: live_latency.jitter_ms,
                        },
                    );
                }
                if effective_args.details {
                    push_run_interval(
                        &mut intervals,
                        snapshot
                            .elapsed
                            .as_secs_f64()
                            .min(effective_args.upload_seconds as f64),
                        snapshot.bytes,
                        snapshot.mbps,
                    );
                }
                push_run_interval(
                    &mut sdk_intervals,
                    snapshot
                        .elapsed
                        .as_secs_f64()
                        .min(effective_args.upload_seconds as f64),
                    snapshot.bytes,
                    snapshot.mbps,
                );
            },
        ))
        .await?;

        if let Some(task) = live_latency_task.take() {
            task.abort();
            let _ = task.await;
        }

        let sdk_upload_latency_samples_ms = if let Some(task) = sdk_latency_task.take() {
            match task.await {
                Ok(samples) if !samples.is_empty() => Some(samples),
                _ => None,
            }
        } else {
            None
        };

        if effective_args.details {
            push_run_interval(
                &mut intervals,
                effective_args.upload_seconds as f64,
                stats.bytes,
                stats.mbps,
            );
        }
        push_run_interval(
            &mut sdk_intervals,
            effective_args.upload_seconds as f64,
            stats.bytes,
            stats.mbps,
        );

        let mut sdk_remote_intervals = Vec::new();
        for sample in &stats.remote_samples {
            if sample.elapsed_ms == 0 {
                continue;
            }
            let elapsed_seconds = (sample.elapsed_ms as f64) / 1_000.0;
            let elapsed_for_mbps = elapsed_seconds.max(0.001);
            let mbps = (sample.bytes as f64 * 8.0) / 1_000_000.0 / elapsed_for_mbps;
            if !mbps.is_finite() || mbps < 0.0 {
                continue;
            }
            push_run_interval(
                &mut sdk_remote_intervals,
                elapsed_seconds.min(effective_args.upload_seconds as f64),
                sample.bytes,
                mbps,
            );
        }
        let sdk_remote_intervals =
            (!sdk_remote_intervals.is_empty()).then_some(sdk_remote_intervals);
        let remote_intervals = if effective_args.details {
            sdk_remote_intervals.clone()
        } else {
            None
        };

        if let Some(progress) = progress.take() {
            ui.finish_speed_progress(progress, "upload", stats.mbps, stats.bytes);
        }

        (
            Some(BenchmarkResult {
                mbps: stats.mbps,
                bytes: stats.bytes,
                duration_seconds: effective_args.upload_seconds,
                connections: effective_args.upload_connections,
            }),
            effective_args.details.then_some(DirectionDetails {
                request_attempts: stats.request_attempts,
                request_successes: stats.request_successes,
                request_http_errors: stats.request_http_errors,
                request_transport_errors: stats.request_transport_errors,
                response_read_errors: stats.response_read_errors,
                intervals,
                remote_intervals,
            }),
            (!sdk_intervals.is_empty()).then_some(sdk_intervals),
            sdk_remote_intervals,
            sdk_upload_latency_samples_ms,
        )
    };

    let session_state = if matches!(resolved_api, ResolvedSpeedtestApi::Modern) {
        speedtest::session::load_modern_session().ok().flatten()
    } else {
        None
    };

    let result = RunResult {
        timestamp: current_timestamp()?,
        speedtest_api: Some(transfer_api.to_string()),
        client: Some(ClientMeta {
            ip: config.client.ip,
            isp: config.client.isp,
            country: config.client.country,
            latitude: config.client.latitude,
            longitude: config.client.longitude,
            isp_id: session_state
                .as_ref()
                .and_then(|state| state.provider_isp_id),
            provider_hash: session_state
                .as_ref()
                .and_then(|state| state.provider_hash.clone()),
        }),
        server: Some(Server {
            id: selected.server.id,
            sponsor: selected.server.sponsor.clone(),
            name: selected.server.name.clone(),
            country: selected.server.country.clone(),
            host: selected.server.host.clone(),
            distance_km: selected.server.distance_km,
            latency_ms: Some(selected.average_ms),
            latency_stddev_ms: Some(selected.variance_ms.max(0.0).sqrt()),
            download_avg_mbps: download_by_server
                .get(&selected.server.id)
                .map(|(_, mbps)| *mbps),
            download_bytes: download_by_server
                .get(&selected.server.id)
                .map(|(bytes, _)| *bytes),
            sdk_url: Some(selected.server.url.clone()),
            sdk_lat: selected.server.sdk_lat.clone(),
            sdk_lon: selected.server.sdk_lon.clone(),
            sdk_cc: selected.server.sdk_cc.clone(),
            sdk_preferred: selected.server.sdk_preferred,
            sdk_isp_id: selected.server.sdk_isp_id.clone(),
            sdk_https_functional: selected.server.sdk_https_functional,
            sdk_hostname: selected.server.sdk_hostname.clone(),
            sdk_port: selected.server.sdk_port,
            sdk_force_ping_select: selected.server.sdk_force_ping_select,
        }),
        server_pool: Some(
            transfer_pool
                .iter()
                .map(|server| {
                    let latency_stats = latency_by_server.get(&server.id).copied();
                    let download_stats = download_by_server.get(&server.id).copied();
                    map_server(server, latency_stats, download_stats)
                })
                .collect(),
        ),
        ping_ms: Some(selected.average_ms),
        download,
        upload,
        proxy: effective_args.proxy,
        sdk_selected_latency_samples_ms: (!selected.samples_ms.is_empty())
            .then(|| selected.samples_ms.clone()),
        sdk_download_intervals,
        sdk_upload_intervals,
        sdk_upload_remote_intervals,
        sdk_download_latency_samples_ms,
        sdk_upload_latency_samples_ms,
        details: effective_args.details.then_some(RunDetails {
            interval_seconds: details_interval_seconds,
            selected_server_latency: SelectedServerLatencyDetails {
                average_ms: selected.average_ms,
                variance_ms: selected.variance_ms,
                stddev_ms: Some(selected.variance_ms.max(0.0).sqrt()),
                samples_ms: (!selected.samples_ms.is_empty()).then(|| selected.samples_ms.clone()),
            },
            download: download_details,
            upload: upload_details,
        }),
    };

    ui.shutdown();

    let sdk_guid = selected
        .server
        .session_guid
        .clone()
        .unwrap_or_else(speedtest::sdk_payload::generate_sdk_guid);

    if let Some(output_path) = effective_args.sdk_json_out.as_deref() {
        speedtest::sdk_payload::write_sdk_result_json_file(
            &result,
            Path::new(output_path),
            Some(&sdk_guid),
        )
        .with_context(|| format!("failed generating SDK payload file at {output_path}"))?;
    }

    if effective_args.json {
        if effective_args.details {
            output::print_json(&result)?;
        } else {
            let payload = speedtest::sdk_payload::build_sdk_result_payload(&result, &sdk_guid)
                .context("failed generating SDK JSON output payload")?;
            output::print_json(&payload)?;
        }
    } else {
        output::print_human(&result);
    }

    Ok(())
}

fn build_transfer_pool(
    ranked: &[speedtest::select::ServerLatency],
    api: ResolvedSpeedtestApi,
    pool_size: usize,
) -> Vec<SpeedtestServer> {
    let limit = if matches!(api, ResolvedSpeedtestApi::Legacy) {
        1
    } else {
        pool_size.max(1)
    };

    ranked
        .iter()
        .take(limit)
        .map(|entry| entry.server.clone())
        .collect()
}

fn format_server_pool_metric(pool: &[SpeedtestServer]) -> String {
    if pool.is_empty() {
        return "none".to_string();
    }

    let entries = pool
        .iter()
        .map(|server| format!("{}:{}", server.id, server.name))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{} [{}]", pool.len(), entries)
}

fn format_selected_server_metric(
    selected_server: &SpeedtestServer,
    average_ms: f64,
    stddev_ms: f64,
    pool: &[SpeedtestServer],
) -> String {
    let base = format!(
        "id={} host={} avg={average_ms:.2}ms std={stddev_ms:.2}",
        selected_server.id, selected_server.host
    );

    if pool.len() <= 1 {
        return base;
    }

    let pool_entries = pool
        .iter()
        .map(|server| format!("{}:{}", server.id, server.name))
        .collect::<Vec<_>>()
        .join(", ");

    format!("{base} pool=[{pool_entries}]")
}

fn map_server(
    server: &SpeedtestServer,
    latency_stats: Option<(f64, f64)>,
    download_stats: Option<(u64, f64)>,
) -> Server {
    let (latency_ms, latency_stddev_ms) = latency_stats
        .map(|(average_ms, stddev_ms)| (Some(average_ms), Some(stddev_ms)))
        .unwrap_or((None, None));
    let (download_bytes, download_avg_mbps) = download_stats
        .map(|(bytes, mbps)| (Some(bytes), Some(mbps)))
        .unwrap_or((None, None));

    Server {
        id: server.id,
        sponsor: server.sponsor.clone(),
        name: server.name.clone(),
        country: server.country.clone(),
        host: server.host.clone(),
        distance_km: server.distance_km,
        latency_ms,
        latency_stddev_ms,
        download_avg_mbps,
        download_bytes,
        sdk_url: Some(server.url.clone()),
        sdk_lat: server.sdk_lat.clone(),
        sdk_lon: server.sdk_lon.clone(),
        sdk_cc: server.sdk_cc.clone(),
        sdk_preferred: server.sdk_preferred,
        sdk_isp_id: server.sdk_isp_id.clone(),
        sdk_https_functional: server.sdk_https_functional,
        sdk_hostname: server.sdk_hostname.clone(),
        sdk_port: server.sdk_port,
        sdk_force_ping_select: server.sdk_force_ping_select,
    }
}

async fn run_iperf(args: IperfArgs) -> Result<()> {
    let proxy_spec = args
        .proxy
        .as_deref()
        .map(iperf::proxy::parse_proxy)
        .transpose()?;

    iperf::proxy::ensure_compatible(args.protocol, proxy_spec.as_ref())?;

    let details_interval_seconds = 1_u64;
    let details_progress_interval = args
        .details
        .then_some(Duration::from_secs(details_interval_seconds));
    let render_ui = !args.json;
    let mut ui = ui::Ui::new(args.tui, render_ui);
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
                            active_connections: args.parallel,
                            latency_ms: None,
                            jitter_ms: None,
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

fn spawn_live_latency_monitor(
    client: reqwest::Client,
    server: SpeedtestServer,
    api: ResolvedSpeedtestApi,
    interval: Duration,
    initial_latency_ms: f64,
    initial_jitter_ms: f64,
) -> (Arc<Mutex<LiveLatencySnapshot>>, tokio::task::JoinHandle<()>) {
    let initial_latency = sanitize_latency_metric(initial_latency_ms);
    let initial_jitter = sanitize_latency_metric(initial_jitter_ms);
    let state = Arc::new(Mutex::new(LiveLatencySnapshot {
        latency_ms: initial_latency,
        jitter_ms: initial_jitter,
    }));

    let state_for_task = Arc::clone(&state);
    let task = tokio::spawn(async move {
        let poll_interval = interval.max(Duration::from_secs(1));
        let mut recent_samples = VecDeque::with_capacity(16);
        if let Some(value) = initial_latency {
            recent_samples.push_back(value);
        }

        loop {
            if let Ok(measurement) =
                speedtest::select::probe_server_latency(&client, &server, 1, api).await
                && let Some(sample_ms) = sanitize_latency_metric(measurement.average_ms)
            {
                recent_samples.push_back(sample_ms);
                while recent_samples.len() > 16 {
                    recent_samples.pop_front();
                }

                let live_jitter_ms = rolling_stddev_ms(&recent_samples);
                if let Ok(mut guard) = state_for_task.lock() {
                    guard.latency_ms = Some(sample_ms);
                    guard.jitter_ms = live_jitter_ms;
                }
            }

            tokio::time::sleep(poll_interval).await;
        }
    });

    (state, task)
}

fn read_live_latency_snapshot(state: &Arc<Mutex<LiveLatencySnapshot>>) -> LiveLatencySnapshot {
    state
        .lock()
        .map(|guard| *guard)
        .unwrap_or(LiveLatencySnapshot {
            latency_ms: None,
            jitter_ms: None,
        })
}

fn rolling_stddev_ms(samples: &VecDeque<f64>) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }

    let mean = samples.iter().sum::<f64>() / samples.len() as f64;
    let variance = samples
        .iter()
        .map(|sample| {
            let delta = *sample - mean;
            delta * delta
        })
        .sum::<f64>()
        / samples.len() as f64;

    sanitize_latency_metric(variance.sqrt())
}

fn sanitize_latency_metric(value: f64) -> Option<f64> {
    if value.is_finite() && value >= 0.0 {
        Some(value)
    } else {
        None
    }
}

fn push_run_interval(
    intervals: &mut Vec<ThroughputInterval>,
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

    intervals.push(ThroughputInterval {
        elapsed_seconds,
        bytes,
        mbps,
    });
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
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    Ok(timestamp.to_string())
}
