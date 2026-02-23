use std::fs;
use std::net::IpAddr;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde::Serialize;
use serde_json::{Value, json};
use tracing::debug;

use crate::model::{BenchmarkResult, DirectionDetails, RunResult, Server, ThroughputInterval};

static GUID_COUNTER: AtomicU64 = AtomicU64::new(0);

#[must_use]
pub fn generate_sdk_guid() -> String {
    let counter = GUID_COUNTER.fetch_add(1, Ordering::Relaxed);
    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(0);

    let part1 = (now_nanos >> 32) as u32;
    let part2 = ((now_nanos >> 16) & 0xffff) as u16;
    let part3 = (now_nanos & 0xffff) as u16;
    let part4 = ((counter >> 48) & 0xffff) as u16;
    let part5 = (counter & 0xffffffffffff) ^ (now_nanos & 0xffffffffffff);

    format!("{part1:08x}-{part2:04x}-{part3:04x}-{part4:04x}-{part5:012x}")
}

pub fn write_sdk_result_json_file(
    result: &RunResult,
    output_path: &Path,
    guid: Option<&str>,
) -> Result<String> {
    let guid = guid
        .map(ToString::to_string)
        .unwrap_or_else(generate_sdk_guid);
    let payload = build_sdk_result_payload(result, &guid)?;
    let body = serde_json::to_string_pretty(&payload)?;

    if let Some(parent) = output_path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed creating SDK JSON output directory {}",
                parent.display()
            )
        })?;
    }

    fs::write(output_path, body)
        .with_context(|| format!("failed writing SDK JSON file {}", output_path.display()))?;
    Ok(guid)
}

pub fn build_sdk_result_payload(result: &RunResult, guid: &str) -> Result<serde_json::Value> {
    let client = result
        .client
        .as_ref()
        .context("cannot build SDK payload without client metadata")?;
    let server = result
        .server
        .as_ref()
        .context("cannot build SDK payload without selected server")?;
    let ping_ms = result
        .ping_ms
        .context("cannot build SDK payload without ping result")?;
    if !ping_ms.is_finite() || ping_ms < 0.0 {
        bail!("ping must be a finite non-negative number");
    }

    let (hostname, port) = parse_host_and_port(&server.host, &server.url_fallback());
    let server_list_source = result
        .server_pool
        .as_ref()
        .filter(|servers| !servers.is_empty())
        .cloned()
        .unwrap_or_else(|| vec![server.clone()]);
    let server_list = server_list_source
        .iter()
        .map(|entry| build_server_list_entry(entry, client))
        .collect::<Vec<_>>();
    let download_bps = result.download.as_ref().map(mbps_to_bps).transpose()?;
    let upload_effective_bps = result.upload.as_ref().map(mbps_to_bps).transpose()?;
    let upload_local_bps = if upload_effective_bps.is_some() {
        local_upload_bps(result).or(upload_effective_bps)
    } else {
        None
    };
    let upload_remote_bps = remote_upload_bps(result);
    let protocols = infer_protocols(result.speedtest_api.as_deref());
    let pings = selected_latency_samples(result).unwrap_or_else(|| vec![ping_ms]);
    let ping = latency_stats(&pings)
        .map(|stats| stats.min)
        .unwrap_or(ping_ms);
    let jitter = calculate_jitter_ms(&pings);
    let download_latency_samples = result
        .sdk_download_latency_samples_ms
        .clone()
        .filter(|samples| !samples.is_empty())
        .unwrap_or_else(|| pings.clone());
    let upload_latency_samples = result
        .sdk_upload_latency_samples_ms
        .clone()
        .filter(|samples| !samples.is_empty())
        .unwrap_or_else(|| pings.clone());
    let download = download_bps.map(bps_to_sdk_units);
    let upload = upload_effective_bps.map(bps_to_sdk_units);
    let (clientip, ip6_address) = split_client_ips(&client.ip);
    let latency =
        build_latency_payload(&pings, jitter, protocols.latency_connection_protocol, true);
    let download_latency = result.download.as_ref().and_then(|_| {
        build_latency_payload(
            &download_latency_samples,
            calculate_jitter_ms(&download_latency_samples),
            protocols.download_connection_protocol,
            false,
        )
    });
    let upload_latency = result.upload.as_ref().and_then(|_| {
        build_latency_payload(
            &upload_latency_samples,
            calculate_jitter_ms(&upload_latency_samples),
            protocols.upload_connection_protocol,
            false,
        )
    });
    let supplemental_data = build_supplemental_data(result)?;
    let download_samples = if download_bps.is_some() {
        build_direction_samples(
            result.sdk_download_intervals.as_deref(),
            result
                .details
                .as_ref()
                .and_then(|details| details.download.as_ref()),
            result.download.as_ref(),
        )?
    } else {
        None
    };
    let upload_samples = if upload_effective_bps.is_some() {
        build_direction_samples(
            result.sdk_upload_intervals.as_deref(),
            result
                .details
                .as_ref()
                .and_then(|details| details.upload.as_ref()),
            result.upload.as_ref(),
        )?
    } else {
        None
    };
    let upload_remote_samples = result
        .sdk_upload_remote_intervals
        .as_deref()
        .and_then(direction_samples_from_intervals);
    let server_selection = build_server_selection(result, server, ping);
    let download_speed_profile = build_speed_profile(
        download_bps,
        download_samples.as_deref(),
        result.download.as_ref().map(|stats| stats.duration_seconds),
    );
    let upload_local_speed_profile = build_speed_profile(
        upload_local_bps,
        upload_samples.as_deref(),
        result.upload.as_ref().map(|stats| stats.duration_seconds),
    );
    let upload_remote_speed_profile = build_speed_profile(
        upload_remote_bps,
        upload_remote_samples.as_deref(),
        result.upload.as_ref().map(|stats| stats.duration_seconds),
    );
    let download = download_speed_profile
        .as_ref()
        .map(|profile| bps_to_sdk_units(profile.combined))
        .or(download);
    let upload = upload_remote_speed_profile
        .as_ref()
        .or(upload_local_speed_profile.as_ref())
        .map(|profile| bps_to_sdk_units(profile.combined))
        .or(upload);
    let upload_measurement_method = if upload_remote_speed_profile.is_some() {
        "remote"
    } else {
        "local"
    };

    debug!(
        upload_effective_bps = ?upload_effective_bps,
        upload_local_bps = ?upload_local_bps,
        upload_remote_bps = ?upload_remote_bps,
        upload_sample_count = upload_samples.as_ref().map(|samples| samples.len()).unwrap_or(0),
        upload_remote_sample_count = upload_remote_samples
            .as_ref()
            .map(|samples| samples.len())
            .unwrap_or(0),
        upload_measurement_method,
        "building sdk upload speed profiles"
    );

    let hash = calculate_result_hash(ping, upload, download);

    let payload = SdkPayload {
        app: SdkApp {
            sdk: SdkAppVersion {
                commit: "tunmux-generated".to_string(),
                version: "3.1.1".to_string(),
            },
        },
        serverid: server.id,
        testmethod: protocols.test_method.to_string(),
        source: "st4-js".to_string(),
        configs: SdkConfigs {
            remote_debugging: false,
            max_display_servers: 20,
            request_web_location: true,
            short_tests: false,
            automatic_stage_progression: false,
            event_skip_interval: 2,
            latency: SdkLatencyConfig { max_servers: 10 },
            js_engine: SdkJsEngine {
                save_content_type: "application/json".to_string(),
                save_type: "st4-js".to_string(),
            },
            stages_list: vec![
                "latency".to_string(),
                "download".to_string(),
                "upload".to_string(),
                "save".to_string(),
            ],
            loaded_latency: SdkLoadedLatency { enabled: true },
            swf: SdkSwf {
                engine: "/engine.swf".to_string(),
                express: "/expressInstall.swf".to_string(),
            },
            provider: SdkProvider {
                country_code: client.country.clone(),
                ip_address: client.ip.clone(),
                isp_name: client.isp.clone(),
                provider_name: client.isp.clone(),
                isp_id: client.isp_id,
                provider_hash: client.provider_hash.clone(),
            },
            vpn_detected: false,
            log_errors_to_server: SdkLogErrorsToServer {
                level: "warn".to_string(),
                use_costanza: true,
                max_per_client: 100,
                allow_during_test: false,
                expensive_stack_traces: true,
            },
            connections: SdkConnections {
                is_vpn: false,
                selection_method: "auto".to_string(),
                mode: "multi".to_string(),
            },
            server_list,
            latency_protocol: protocols.latency_protocol.to_string(),
            download_protocol: protocols.download_protocol.to_string(),
            upload_protocol: protocols.upload_protocol.to_string(),
            host: hostname,
            port,
            server_version: "2.11.1".to_string(),
            server_build: "tunmux-generated".to_string(),
        },
        location: Some(SdkLocation {
            country: client.country.clone(),
            country_code: client.country.clone(),
            lat: client.latitude,
            lon: client.longitude,
        }),
        isp_name: client.isp.clone(),
        ping,
        pings,
        jitter,
        latency,
        guid: guid.to_string(),
        server_selection_guid: guid.to_string(),
        server_selection_method: "auto".to_string(),
        server_selection,
        upload_measurement_method: upload_measurement_method.to_string(),
        upload,
        upload_speeds: upload_effective_bps.map(|_| SdkDirectionSpeeds {
            local: upload_local_speed_profile,
            remote: upload_remote_speed_profile,
        }),
        download,
        download_speeds: download_bps.map(|_| SdkDirectionSpeeds {
            local: download_speed_profile,
            remote: None,
        }),
        download_latency,
        upload_latency,
        supplemental_data,
        download_samples,
        upload_samples,
        spoofed: false,
        clientip,
        ip6_address,
        hash,
    };

    Ok(serde_json::to_value(payload)?)
}

fn mbps_to_bps(result: &BenchmarkResult) -> Result<u64> {
    if !result.mbps.is_finite() || result.mbps < 0.0 {
        bail!("throughput Mbps must be a finite non-negative number");
    }
    Ok((result.mbps * 1_000_000.0).round() as u64)
}

fn bps_to_sdk_units(bps: u64) -> u64 {
    ((bps as f64) / 125.0).round() as u64
}

fn remote_upload_bps(result: &RunResult) -> Option<u64> {
    if let Some(intervals) = result.sdk_upload_remote_intervals.as_ref()
        && let Some(last) = intervals.last()
        && last.mbps.is_finite()
        && last.mbps >= 0.0
    {
        return Some((last.mbps * 1_000_000.0).round() as u64);
    }

    let remote_mbps = result
        .details
        .as_ref()?
        .upload
        .as_ref()?
        .remote_intervals
        .as_ref()?
        .last()?
        .mbps;

    if !remote_mbps.is_finite() || remote_mbps < 0.0 {
        return None;
    }

    Some((remote_mbps * 1_000_000.0).round() as u64)
}

fn local_upload_bps(result: &RunResult) -> Option<u64> {
    if let Some(intervals) = result.sdk_upload_intervals.as_ref()
        && let Some(last) = intervals.last()
        && last.mbps.is_finite()
        && last.mbps >= 0.0
    {
        return Some((last.mbps * 1_000_000.0).round() as u64);
    }

    let local_mbps = result
        .details
        .as_ref()?
        .upload
        .as_ref()?
        .intervals
        .last()?
        .mbps;

    if !local_mbps.is_finite() || local_mbps < 0.0 {
        return None;
    }

    Some((local_mbps * 1_000_000.0).round() as u64)
}

fn infer_protocols(speedtest_api: Option<&str>) -> SdkProtocols {
    match speedtest_api {
        Some("modern-tcp") => SdkProtocols {
            test_method: "wss,tcps,tcps",
            latency_protocol: "ws",
            download_protocol: "tcp",
            upload_protocol: "tcp",
            latency_connection_protocol: "wss",
            download_connection_protocol: "tcps",
            upload_connection_protocol: "tcps",
        },
        _ => SdkProtocols {
            test_method: "wss,xhrs,xhrs",
            latency_protocol: "ws",
            download_protocol: "xhr",
            upload_protocol: "xhr",
            latency_connection_protocol: "wss",
            download_connection_protocol: "xhrs",
            upload_connection_protocol: "xhrs",
        },
    }
}

fn selected_latency_samples(result: &RunResult) -> Option<Vec<f64>> {
    let samples = result
        .sdk_selected_latency_samples_ms
        .as_ref()
        .or_else(|| {
            result
                .details
                .as_ref()
                .and_then(|details| details.selected_server_latency.samples_ms.as_ref())
        })?;

    let normalized = samples
        .iter()
        .copied()
        .filter(|sample| sample.is_finite() && *sample >= 0.0)
        .collect::<Vec<_>>();

    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

fn calculate_jitter_ms(samples: &[f64]) -> f64 {
    if samples.len() < 2 {
        return 0.0;
    }

    let mut total_delta = 0.0;
    for index in 1..samples.len() {
        total_delta += (samples[index] - samples[index - 1]).abs();
    }

    round_to(total_delta / (samples.len() - 1) as f64, 3)
}

fn split_client_ips(client_ip: &str) -> (Option<String>, Option<String>) {
    match client_ip.parse::<IpAddr>() {
        Ok(IpAddr::V4(_)) => (Some(client_ip.to_string()), None),
        Ok(IpAddr::V6(_)) => (Some(client_ip.to_string()), Some(client_ip.to_string())),
        Err(_) => (Some(client_ip.to_string()), None),
    }
}

fn build_latency_payload(
    samples: &[f64],
    jitter_ms: f64,
    connection_protocol: &str,
    include_sample_arrays: bool,
) -> Option<SdkLatencyPayload> {
    let stats = latency_stats(samples)?;
    let jitter = if jitter_ms.is_finite() && jitter_ms >= 0.0 {
        jitter_ms
    } else {
        calculate_jitter_ms(samples)
    };

    Some(SdkLatencyPayload {
        connection_protocol: connection_protocol.to_string(),
        tcp: SdkLatencyTcp {
            rtt: stats,
            jitter,
            count: samples.len() as u64,
            samples: include_sample_arrays.then(|| samples.to_vec()),
            graph_samples: include_sample_arrays.then(|| samples.to_vec()),
        },
    })
}

fn latency_stats(samples: &[f64]) -> Option<SdkRtt> {
    if samples.is_empty() {
        return None;
    }

    let mut values = samples
        .iter()
        .copied()
        .filter(|sample| sample.is_finite() && *sample >= 0.0)
        .collect::<Vec<_>>();
    if values.is_empty() {
        return None;
    }

    values.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let min = values[0];
    let max = values[values.len() - 1];
    let median = if values.len() <= 2 {
        mean
    } else {
        let middle = values.len() / 2;
        if values.len() % 2 == 1 {
            values[middle]
        } else {
            let left = values[middle];
            let right = values.get(middle + 1).copied().unwrap_or(left);
            (left + right) / 2.0
        }
    };
    let iqm = calculate_iqm(&values);

    Some(SdkRtt {
        iqm,
        mean,
        median,
        min,
        max,
    })
}

fn calculate_iqm(values: &[f64]) -> f64 {
    match values.len() {
        0 => 0.0,
        1 => values[0],
        2 => (values[0] + values[1]) / 2.0,
        len => {
            let lower = len as f64 / 4.0;
            let upper = 3.0 * len as f64 / 4.0;
            let start = lower.ceil() as usize;
            let end = upper.floor() as usize;
            let fraction = upper - upper.floor();
            let core_sum = if start < end {
                values[start..end].iter().sum::<f64>()
            } else {
                0.0
            };
            let edge_sum = if start > 0 && end < len {
                values[start - 1] + values[end]
            } else {
                0.0
            };
            (fraction * edge_sum + core_sum) / (len as f64 / 2.0)
        }
    }
}

fn round_to(value: f64, digits: i32) -> f64 {
    let scale = 10_f64.powi(digits);
    (value * scale).round() / scale
}

fn build_direction_samples(
    sdk_intervals: Option<&[ThroughputInterval]>,
    details: Option<&DirectionDetails>,
    fallback: Option<&BenchmarkResult>,
) -> Result<Option<Vec<SdkThroughputSample>>> {
    if let Some(sdk_samples) = sdk_intervals.and_then(direction_samples_from_intervals)
        && !sdk_samples.is_empty()
    {
        return Ok(Some(sdk_samples));
    }

    if let Some(details) = details {
        let samples = direction_samples_from_intervals(&details.intervals).unwrap_or_default();
        if !samples.is_empty() {
            return Ok(Some(samples));
        }
    }

    let Some(fallback) = fallback else {
        return Ok(None);
    };

    let bps = mbps_to_bps(fallback)?;
    Ok(Some(vec![SdkThroughputSample {
        elapsed: fallback.duration_seconds as f64,
        bytes: fallback.bytes,
        mbps: fallback.mbps,
        bps,
    }]))
}

fn direction_samples_from_intervals(
    intervals: &[ThroughputInterval],
) -> Option<Vec<SdkThroughputSample>> {
    let samples = intervals
        .iter()
        .filter_map(interval_to_sdk_sample)
        .collect::<Vec<_>>();
    if samples.is_empty() {
        None
    } else {
        Some(samples)
    }
}

fn interval_to_sdk_sample(interval: &ThroughputInterval) -> Option<SdkThroughputSample> {
    if !interval.elapsed_seconds.is_finite()
        || interval.elapsed_seconds < 0.0
        || !interval.mbps.is_finite()
        || interval.mbps < 0.0
    {
        return None;
    }

    Some(SdkThroughputSample {
        elapsed: interval.elapsed_seconds,
        bytes: interval.bytes,
        mbps: interval.mbps,
        bps: (interval.mbps * 1_000_000.0).round() as u64,
    })
}

fn build_speed_profile(
    combined_bps: Option<u64>,
    samples: Option<&[SdkThroughputSample]>,
    duration_seconds: Option<u64>,
) -> Option<SdkCombinedSpeed> {
    let combined = combined_bps?;
    let baseline = combined as f64;
    let transfer_samples = samples.map(to_transfer_samples).unwrap_or_default();
    if transfer_samples.is_empty() {
        return Some(SdkCombinedSpeed {
            combined,
            average: baseline,
            mst_66_20: baseline,
            mst_66_30: baseline,
            mst_75_30: baseline,
            superspeed: baseline,
        });
    }

    let duration_millis = duration_seconds
        .map(|seconds| seconds as f64 * 1_000.0)
        .unwrap_or_else(|| {
            transfer_samples
                .last()
                .map(|sample| sample.received_reply_at_ms)
                .unwrap_or(0.0)
                .max(1_000.0)
        });

    let average = calculate_average_speed_bps(&transfer_samples).unwrap_or(baseline);
    let mst_66_20 =
        calculate_mst_speed_bps(&transfer_samples, 2.0 / 3.0, 750.0, 600.0).unwrap_or(average);
    let mst_66_30 =
        calculate_mst_speed_bps(&transfer_samples, 2.0 / 3.0, 500.0, 500.0).unwrap_or(average);
    let mst_75_30 =
        calculate_mst_speed_bps(&transfer_samples, 0.75, 500.0, 500.0).unwrap_or(average);
    let superspeed = calculate_superspeed_bps(&transfer_samples, duration_millis, 500.0, 500.0)
        .unwrap_or(average);

    Some(SdkCombinedSpeed {
        combined,
        average,
        mst_66_20,
        mst_66_30,
        mst_75_30,
        superspeed,
    })
}

fn to_transfer_samples(samples: &[SdkThroughputSample]) -> Vec<SdkTransferSample> {
    let mut transfers = Vec::new();
    let mut previous_elapsed_ms = 0.0;
    let mut previous_bytes = 0_u64;

    for sample in samples {
        let elapsed_ms = sample.elapsed * 1_000.0;
        if !elapsed_ms.is_finite() || elapsed_ms <= previous_elapsed_ms {
            continue;
        }

        let bytes_delta = sample.bytes.saturating_sub(previous_bytes);
        transfers.push(SdkTransferSample {
            elapsed_ms,
            size: bytes_delta,
            sent_at_ms: previous_elapsed_ms,
            received_reply_at_ms: elapsed_ms,
        });

        previous_elapsed_ms = elapsed_ms;
        previous_bytes = sample.bytes;
    }

    transfers
}

fn calculate_average_speed_bps(samples: &[SdkTransferSample]) -> Option<f64> {
    let total_bytes = samples.iter().map(|sample| sample.size).sum::<u64>() as f64;
    let elapsed_ms = samples
        .last()
        .map(|sample| sample.elapsed_ms)
        .filter(|value| value.is_finite() && *value > 0.0)?;
    Some(total_bytes * 8.0 * 1_000.0 / elapsed_ms)
}

fn calculate_mst_speed_bps(
    samples: &[SdkTransferSample],
    kept_samples_percentage: f64,
    sample_length_ms: f64,
    minimum_sample_length_ms: f64,
) -> Option<f64> {
    let calculator =
        BucketedThroughputCalculator::new(sample_length_ms, minimum_sample_length_ms, None);
    let buckets = calculator.build(samples);
    let mut completed = buckets
        .buckets
        .iter()
        .filter_map(|bucket| (*bucket).to_bandwidth_sample())
        .collect::<Vec<_>>();

    let completed_count = completed.len();
    if completed_count < 4 {
        return None;
    }

    completed.sort_by(|left, right| {
        left.bandwidth_bps
            .partial_cmp(&right.bandwidth_bps)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    if completed.len() <= 2 {
        return None;
    }
    let mut trimmed = completed[1..completed.len() - 1].to_vec();
    if trimmed.is_empty() {
        return None;
    }

    let keep_ratio = kept_samples_percentage.clamp(0.0, 1.0);
    let drop_count = (((completed_count - 2) as f64) * (1.0 - keep_ratio)).floor() as usize;
    if drop_count >= trimmed.len() {
        trimmed = vec![trimmed[trimmed.len() - 1]];
    } else if drop_count > 0 {
        trimmed = trimmed.split_off(drop_count);
    }

    let aggregate = trimmed.iter().fold(
        AggregateBandwidthSample {
            duration_ms: 0.0,
            bytes_transferred: 0.0,
        },
        |acc, sample| AggregateBandwidthSample {
            duration_ms: acc.duration_ms + sample.duration_ms,
            bytes_transferred: acc.bytes_transferred + sample.bytes_transferred,
        },
    );

    if aggregate.duration_ms <= 0.0 {
        None
    } else {
        Some(aggregate.bytes_transferred * 8.0 * 1_000.0 / aggregate.duration_ms)
    }
}

fn calculate_superspeed_bps(
    samples: &[SdkTransferSample],
    test_duration_ms: f64,
    sample_length_ms: f64,
    minimum_sample_length_ms: f64,
) -> Option<f64> {
    if !test_duration_ms.is_finite() || test_duration_ms <= 0.0 {
        return None;
    }

    let calculator = BucketedThroughputCalculator::new(
        sample_length_ms,
        minimum_sample_length_ms,
        Some(test_duration_ms),
    );
    let buckets = calculator.build(samples);
    let first_good_index = buckets.first_good_end_sample_index?;
    let completed = buckets
        .buckets
        .iter()
        .filter(|bucket| bucket.stop_time_ms.is_some())
        .collect::<Vec<_>>();

    if first_good_index >= completed.len() {
        return None;
    }

    let minimum_window_ms = test_duration_ms / 2.0;
    let mut best_bps = 0.0;

    for start_index in 0..=first_good_index {
        for end_index in first_good_index..completed.len() {
            let start = completed[start_index];
            let end = completed[end_index];
            let Some(end_stop_ms) = end.stop_time_ms else {
                continue;
            };

            let window_ms = end_stop_ms - start.start_time_ms;
            if window_ms < minimum_window_ms || window_ms <= 0.0 {
                continue;
            }

            let bytes = end.total_bytes_transferred as f64
                - (start.total_bytes_transferred as f64 - start.bytes_transferred as f64);
            let speed_bps = bytes * 8.0 * 1_000.0 / window_ms;
            if speed_bps > best_bps {
                best_bps = speed_bps;
            }
        }
    }

    if best_bps > 0.0 { Some(best_bps) } else { None }
}

#[derive(Debug, Clone, Copy)]
struct SdkTransferSample {
    elapsed_ms: f64,
    size: u64,
    sent_at_ms: f64,
    received_reply_at_ms: f64,
}

#[derive(Debug, Clone, Copy)]
struct SdkThroughputBucket {
    bytes_transferred: u64,
    start_time_ms: f64,
    stop_time_ms: Option<f64>,
    total_bytes_transferred: u64,
}

impl SdkThroughputBucket {
    fn duration_ms(self) -> Option<f64> {
        let stop = self.stop_time_ms?;
        let duration = stop - self.start_time_ms;
        (duration.is_finite() && duration > 0.0).then_some(duration)
    }

    fn bandwidth_bps(self) -> Option<f64> {
        let duration_ms = self.duration_ms()?;
        Some(self.bytes_transferred as f64 * 8.0 * 1_000.0 / duration_ms)
    }

    fn to_bandwidth_sample(self) -> Option<BandwidthSample> {
        Some(BandwidthSample {
            bandwidth_bps: self.bandwidth_bps()?,
            duration_ms: self.duration_ms()?,
            bytes_transferred: self.bytes_transferred as f64,
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct BandwidthSample {
    bandwidth_bps: f64,
    duration_ms: f64,
    bytes_transferred: f64,
}

#[derive(Debug, Clone, Copy)]
struct AggregateBandwidthSample {
    duration_ms: f64,
    bytes_transferred: f64,
}

#[derive(Debug, Clone, Copy)]
struct FragmentedTransfer {
    target_bucket: usize,
    size: u64,
}

#[derive(Debug)]
struct BucketedThroughput {
    buckets: Vec<SdkThroughputBucket>,
    first_good_end_sample_index: Option<usize>,
}

#[derive(Debug, Clone, Copy)]
struct BucketedThroughputCalculator {
    sample_length_ms: f64,
    minimum_sample_length_ms: f64,
    test_duration_ms: Option<f64>,
}

impl BucketedThroughputCalculator {
    fn new(
        sample_length_ms: f64,
        minimum_sample_length_ms: f64,
        test_duration_ms: Option<f64>,
    ) -> Self {
        Self {
            sample_length_ms,
            minimum_sample_length_ms,
            test_duration_ms,
        }
    }

    fn build(self, samples: &[SdkTransferSample]) -> BucketedThroughput {
        let mut buckets = Vec::new();
        let mut first_good_end_sample_index = None;
        self.create_bucket(0.0, &mut buckets, &mut first_good_end_sample_index);

        let max_bucket = (15_000.0 / self.minimum_sample_length_ms).ceil() as usize;
        for sample in samples {
            if !sample.elapsed_ms.is_finite() || sample.elapsed_ms < 0.0 {
                continue;
            }

            let target_bucket = self
                .calculate_target_bucket(sample.elapsed_ms, &buckets)
                .min(max_bucket);

            while buckets.len() <= target_bucket {
                self.create_bucket(
                    sample.elapsed_ms,
                    &mut buckets,
                    &mut first_good_end_sample_index,
                );
            }

            let fragments = self.fragment_sample(sample, target_bucket, &buckets);
            for fragment in fragments {
                if fragment.target_bucket >= buckets.len() {
                    continue;
                }
                buckets[fragment.target_bucket].bytes_transferred = buckets[fragment.target_bucket]
                    .bytes_transferred
                    .saturating_add(fragment.size);

                for bucket in &mut buckets[fragment.target_bucket..] {
                    bucket.total_bytes_transferred =
                        bucket.total_bytes_transferred.saturating_add(fragment.size);
                }
            }
        }

        BucketedThroughput {
            buckets,
            first_good_end_sample_index,
        }
    }

    fn calculate_target_bucket(self, elapsed_ms: f64, buckets: &[SdkThroughputBucket]) -> usize {
        if (self.sample_length_ms - self.minimum_sample_length_ms).abs() < f64::EPSILON {
            return (elapsed_ms / self.sample_length_ms).floor() as usize;
        }

        let bucket_count = buckets.len();
        let expected_count = (elapsed_ms / self.sample_length_ms).floor() as usize + 1;
        let min_ready_at = buckets
            .last()
            .map(|bucket| bucket.start_time_ms + self.minimum_sample_length_ms)
            .unwrap_or(self.minimum_sample_length_ms);

        if bucket_count < expected_count && elapsed_ms >= min_ready_at {
            expected_count.saturating_sub(1)
        } else {
            bucket_count.saturating_sub(1)
        }
    }

    fn fragment_sample(
        self,
        sample: &SdkTransferSample,
        mut target_bucket: usize,
        buckets: &[SdkThroughputBucket],
    ) -> Vec<FragmentedTransfer> {
        if sample.size == 0 {
            return Vec::new();
        }

        let mut fragments = Vec::new();
        let mut remaining = sample.size;
        let mut received_reply_at_ms = sample.received_reply_at_ms;
        let sent_at_ms = sample.sent_at_ms;

        while remaining > 0
            && target_bucket > 0
            && sent_at_ms < buckets[target_bucket].start_time_ms
        {
            let overlap = received_reply_at_ms - buckets[target_bucket].start_time_ms;
            if overlap > 0.0 {
                let total = received_reply_at_ms - sent_at_ms;
                if total > 0.0 {
                    let ratio = overlap / total;
                    let fragment_size = ((remaining as f64 * ratio).round() as u64).min(remaining);
                    if fragment_size > 0 {
                        fragments.push(FragmentedTransfer {
                            target_bucket,
                            size: fragment_size,
                        });
                        remaining -= fragment_size;
                    }
                }
            }

            target_bucket -= 1;
            if let Some(stop_time_ms) = buckets[target_bucket].stop_time_ms {
                received_reply_at_ms = stop_time_ms;
            } else {
                break;
            }
        }

        if remaining > 0 {
            fragments.push(FragmentedTransfer {
                target_bucket,
                size: remaining,
            });
        }

        fragments
    }

    fn create_bucket(
        self,
        elapsed_ms: f64,
        buckets: &mut Vec<SdkThroughputBucket>,
        first_good_end_sample_index: &mut Option<usize>,
    ) {
        let mut start_time_ms = elapsed_ms;
        let mut total_bytes_transferred = 0_u64;

        if let Some(last_bucket) = buckets.last_mut() {
            let stop_time_ms = (last_bucket.start_time_ms + self.sample_length_ms).min(elapsed_ms);
            last_bucket.stop_time_ms = Some(stop_time_ms);
            start_time_ms = stop_time_ms;
            total_bytes_transferred = last_bucket.total_bytes_transferred;
        }

        buckets.push(SdkThroughputBucket {
            bytes_transferred: 0,
            start_time_ms,
            stop_time_ms: None,
            total_bytes_transferred,
        });

        if first_good_end_sample_index.is_none()
            && let Some(test_duration_ms) = self.test_duration_ms
            && start_time_ms > (test_duration_ms / 2.0)
        {
            *first_good_end_sample_index = buckets.len().checked_sub(1);
        }
    }
}

fn build_server_selection(
    result: &RunResult,
    selected: &Server,
    fallback_ping: f64,
) -> Option<SdkServerSelection> {
    let source_servers = result
        .server_pool
        .as_ref()
        .filter(|servers| !servers.is_empty())
        .cloned()
        .unwrap_or_else(|| vec![selected.clone()]);

    let mut details = source_servers
        .into_iter()
        .filter_map(|server| {
            let ping = server.latency_ms.unwrap_or(fallback_ping);
            if !ping.is_finite() || ping < 0.0 {
                return None;
            }
            Some(SdkClosestPingDetail {
                id: server.id,
                name: server.name,
                sponsor: server.sponsor,
                host: server.host,
                distance: server.distance_km,
                ping,
                jitter: server.latency_stddev_ms,
            })
        })
        .collect::<Vec<_>>();

    details.sort_by(|left, right| {
        left.ping
            .partial_cmp(&right.ping)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    if details.is_empty() {
        None
    } else {
        Some(SdkServerSelection {
            closest_ping_details: details,
        })
    }
}

fn build_supplemental_data(result: &RunResult) -> Result<Value> {
    let mut payload = serde_json::Map::new();
    payload.insert(
        "detailsIncluded".to_string(),
        json!(result.details.is_some()),
    );
    payload.insert("proxy".to_string(), json!(result.proxy));
    payload.insert("speedtestApi".to_string(), json!(result.speedtest_api));

    if let Some(details) = result.details.as_ref() {
        payload.insert("runDetails".to_string(), serde_json::to_value(details)?);
    }

    Ok(Value::Object(payload))
}

fn calculate_result_hash(ping: f64, upload: Option<u64>, download: Option<u64>) -> String {
    let ping = if ping.is_finite() { ping } else { 0.0 };
    let upload = upload.unwrap_or(0);
    let download = download.unwrap_or(0);
    let hash_input = format!("{ping}-{upload}-{download}-817d699764d33f89c");
    format!("{:x}", md5::compute(hash_input))
}

fn parse_host_and_port(host: &str, fallback_url: &str) -> (String, u16) {
    if let Some((name, port)) = split_host_port(host) {
        return (name.to_string(), port);
    }

    let parsed = url::Url::parse(fallback_url).ok();
    let hostname = parsed
        .as_ref()
        .and_then(url::Url::host_str)
        .unwrap_or("unknown")
        .to_string();
    let port = parsed
        .as_ref()
        .and_then(url::Url::port_or_known_default)
        .unwrap_or(8080);
    (hostname, port)
}

fn split_host_port(host: &str) -> Option<(&str, u16)> {
    if host.starts_with('[') {
        let end = host.find(']')?;
        let name = &host[1..end];
        let rest = &host[end + 1..];
        let port = rest.strip_prefix(':')?.parse::<u16>().ok()?;
        return Some((name, port));
    }

    let (name, port) = host.rsplit_once(':')?;
    let parsed_port = port.parse::<u16>().ok()?;
    Some((name, parsed_port))
}

fn build_server_list_entry(
    server: &Server,
    client: &crate::model::ClientMeta,
) -> SdkServerListEntry {
    let fallback_url = server.url_fallback();
    let (hostname, port) = parse_host_and_port(&server.host, &fallback_url);

    SdkServerListEntry {
        url: server.sdk_url.clone().unwrap_or(fallback_url),
        lat: server
            .sdk_lat
            .clone()
            .unwrap_or_else(|| format!("{:.4}", client.latitude)),
        lon: server
            .sdk_lon
            .clone()
            .unwrap_or_else(|| format!("{:.4}", client.longitude)),
        distance: server.distance_km.round() as u64,
        name: server.name.clone(),
        country: server.country.clone(),
        cc: server
            .sdk_cc
            .clone()
            .unwrap_or_else(|| client.country.clone()),
        sponsor: server.sponsor.clone(),
        id: server.id,
        preferred: server.sdk_preferred.unwrap_or(0),
        isp_id: server.sdk_isp_id.clone().unwrap_or_else(|| "0".to_string()),
        https_functional: server.sdk_https_functional.unwrap_or(1),
        host: server.host.clone(),
        hostname: server.sdk_hostname.clone().unwrap_or(hostname),
        port: server.sdk_port.unwrap_or(port),
        force_ping_select: server.sdk_force_ping_select,
    }
}

trait ServerUrlFallback {
    fn url_fallback(&self) -> String;
}

impl ServerUrlFallback for Server {
    fn url_fallback(&self) -> String {
        format!("https://{}/speedtest/upload.php", self.host)
    }
}

#[derive(Debug, Serialize)]
struct SdkPayload {
    app: SdkApp,
    serverid: u64,
    testmethod: String,
    source: String,
    configs: SdkConfigs,
    #[serde(skip_serializing_if = "Option::is_none")]
    location: Option<SdkLocation>,
    #[serde(rename = "ispName")]
    isp_name: String,
    ping: f64,
    pings: Vec<f64>,
    jitter: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    latency: Option<SdkLatencyPayload>,
    guid: String,
    #[serde(rename = "serverSelectionGuid")]
    server_selection_guid: String,
    #[serde(rename = "serverSelectionMethod")]
    server_selection_method: String,
    #[serde(rename = "serverSelection", skip_serializing_if = "Option::is_none")]
    server_selection: Option<SdkServerSelection>,
    #[serde(rename = "uploadMeasurementMethod")]
    upload_measurement_method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    upload: Option<u64>,
    #[serde(rename = "uploadSpeeds", skip_serializing_if = "Option::is_none")]
    upload_speeds: Option<SdkDirectionSpeeds>,
    #[serde(skip_serializing_if = "Option::is_none")]
    download: Option<u64>,
    #[serde(rename = "downloadSpeeds", skip_serializing_if = "Option::is_none")]
    download_speeds: Option<SdkDirectionSpeeds>,
    #[serde(rename = "downloadLatency", skip_serializing_if = "Option::is_none")]
    download_latency: Option<SdkLatencyPayload>,
    #[serde(rename = "uploadLatency", skip_serializing_if = "Option::is_none")]
    upload_latency: Option<SdkLatencyPayload>,
    #[serde(rename = "supplementalData")]
    supplemental_data: Value,
    #[serde(rename = "downloadSamples", skip_serializing_if = "Option::is_none")]
    download_samples: Option<Vec<SdkThroughputSample>>,
    #[serde(rename = "uploadSamples", skip_serializing_if = "Option::is_none")]
    upload_samples: Option<Vec<SdkThroughputSample>>,
    spoofed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    clientip: Option<String>,
    #[serde(rename = "ip6Address", skip_serializing_if = "Option::is_none")]
    ip6_address: Option<String>,
    hash: String,
}

#[derive(Debug, Clone, Copy)]
struct SdkProtocols {
    test_method: &'static str,
    latency_protocol: &'static str,
    download_protocol: &'static str,
    upload_protocol: &'static str,
    latency_connection_protocol: &'static str,
    download_connection_protocol: &'static str,
    upload_connection_protocol: &'static str,
}

#[derive(Debug, Serialize)]
struct SdkApp {
    sdk: SdkAppVersion,
}

#[derive(Debug, Serialize)]
struct SdkAppVersion {
    commit: String,
    version: String,
}

#[derive(Debug, Serialize)]
struct SdkConfigs {
    #[serde(rename = "remoteDebugging")]
    remote_debugging: bool,
    #[serde(rename = "maxDisplayServers")]
    max_display_servers: u64,
    #[serde(rename = "requestWebLocation")]
    request_web_location: bool,
    #[serde(rename = "shortTests")]
    short_tests: bool,
    #[serde(rename = "automaticStageProgression")]
    automatic_stage_progression: bool,
    #[serde(rename = "eventSkipInterval")]
    event_skip_interval: u64,
    latency: SdkLatencyConfig,
    #[serde(rename = "jsEngine")]
    js_engine: SdkJsEngine,
    #[serde(rename = "stagesList")]
    stages_list: Vec<String>,
    #[serde(rename = "loadedLatency")]
    loaded_latency: SdkLoadedLatency,
    swf: SdkSwf,
    provider: SdkProvider,
    #[serde(rename = "vpnDetected")]
    vpn_detected: bool,
    #[serde(rename = "logErrorsToServer")]
    log_errors_to_server: SdkLogErrorsToServer,
    connections: SdkConnections,
    #[serde(rename = "serverList")]
    server_list: Vec<SdkServerListEntry>,
    #[serde(rename = "latencyProtocol")]
    latency_protocol: String,
    #[serde(rename = "downloadProtocol")]
    download_protocol: String,
    #[serde(rename = "uploadProtocol")]
    upload_protocol: String,
    host: String,
    port: u16,
    #[serde(rename = "serverVersion")]
    server_version: String,
    #[serde(rename = "serverBuild")]
    server_build: String,
}

#[derive(Debug, Serialize)]
struct SdkLatencyConfig {
    #[serde(rename = "maxServers")]
    max_servers: u64,
}

#[derive(Debug, Serialize)]
struct SdkJsEngine {
    #[serde(rename = "saveContentType")]
    save_content_type: String,
    #[serde(rename = "saveType")]
    save_type: String,
}

#[derive(Debug, Serialize)]
struct SdkLoadedLatency {
    enabled: bool,
}

#[derive(Debug, Serialize)]
struct SdkSwf {
    engine: String,
    express: String,
}

#[derive(Debug, Serialize)]
struct SdkProvider {
    #[serde(rename = "countryCode")]
    country_code: String,
    #[serde(rename = "ipAddress")]
    ip_address: String,
    #[serde(rename = "ispName")]
    isp_name: String,
    #[serde(rename = "providerName")]
    provider_name: String,
    #[serde(rename = "ispId", skip_serializing_if = "Option::is_none")]
    isp_id: Option<u64>,
    #[serde(rename = "providerHash", skip_serializing_if = "Option::is_none")]
    provider_hash: Option<String>,
}

#[derive(Debug, Serialize)]
struct SdkLocation {
    country: String,
    #[serde(rename = "countryCode")]
    country_code: String,
    lat: f64,
    lon: f64,
}

#[derive(Debug, Serialize)]
struct SdkLatencyPayload {
    #[serde(rename = "connectionProtocol")]
    connection_protocol: String,
    tcp: SdkLatencyTcp,
}

#[derive(Debug, Serialize)]
struct SdkLatencyTcp {
    rtt: SdkRtt,
    jitter: f64,
    count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    samples: Option<Vec<f64>>,
    #[serde(rename = "graphSamples", skip_serializing_if = "Option::is_none")]
    graph_samples: Option<Vec<f64>>,
}

#[derive(Debug, Serialize)]
struct SdkRtt {
    iqm: f64,
    mean: f64,
    median: f64,
    min: f64,
    max: f64,
}

#[derive(Debug, Serialize)]
struct SdkServerSelection {
    #[serde(rename = "closestPingDetails")]
    closest_ping_details: Vec<SdkClosestPingDetail>,
}

#[derive(Debug, Serialize)]
struct SdkClosestPingDetail {
    id: u64,
    name: String,
    sponsor: String,
    host: String,
    distance: f64,
    ping: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    jitter: Option<f64>,
}

#[derive(Debug, Serialize)]
struct SdkLogErrorsToServer {
    level: String,
    #[serde(rename = "useCostanza")]
    use_costanza: bool,
    #[serde(rename = "maxPerClient")]
    max_per_client: u64,
    #[serde(rename = "allowDuringTest")]
    allow_during_test: bool,
    #[serde(rename = "expensiveStackTraces")]
    expensive_stack_traces: bool,
}

#[derive(Debug, Serialize)]
struct SdkConnections {
    #[serde(rename = "isVpn")]
    is_vpn: bool,
    #[serde(rename = "selectionMethod")]
    selection_method: String,
    mode: String,
}

#[derive(Debug, Serialize)]
struct SdkServerListEntry {
    url: String,
    lat: String,
    lon: String,
    distance: u64,
    name: String,
    country: String,
    cc: String,
    sponsor: String,
    id: u64,
    preferred: u8,
    #[serde(rename = "isp_id")]
    isp_id: String,
    #[serde(rename = "httpsFunctional")]
    https_functional: u8,
    host: String,
    hostname: String,
    port: u16,
    #[serde(rename = "force_ping_select", skip_serializing_if = "Option::is_none")]
    force_ping_select: Option<u8>,
}

#[derive(Debug, Serialize)]
struct SdkDirectionSpeeds {
    #[serde(skip_serializing_if = "Option::is_none")]
    local: Option<SdkCombinedSpeed>,
    #[serde(skip_serializing_if = "Option::is_none")]
    remote: Option<SdkCombinedSpeed>,
}

#[derive(Debug, Serialize)]
struct SdkCombinedSpeed {
    combined: u64,
    average: f64,
    #[serde(rename = "mst_66_20")]
    mst_66_20: f64,
    #[serde(rename = "mst_66_30")]
    mst_66_30: f64,
    #[serde(rename = "mst_75_30")]
    mst_75_30: f64,
    superspeed: f64,
}

#[derive(Debug, Serialize)]
struct SdkThroughputSample {
    elapsed: f64,
    bytes: u64,
    mbps: f64,
    bps: u64,
}

#[cfg(test)]
mod tests {
    use super::{build_sdk_result_payload, write_sdk_result_json_file};
    use crate::model::{BenchmarkResult, ClientMeta, RunResult, Server, ThroughputInterval};

    #[test]
    fn builds_sdk_payload_from_run_result() {
        let result = sample_result();
        let payload = build_sdk_result_payload(&result, "eca27dc9-e3dd-429a-85e7-8ee178facc6c")
            .expect("payload should build");

        assert_eq!(payload["source"], "st4-js");
        assert_eq!(payload["serverid"], 61301);
        assert_eq!(payload["configs"]["downloadProtocol"], "xhr");
        assert_eq!(payload["guid"], "eca27dc9-e3dd-429a-85e7-8ee178facc6c");
        assert_eq!(payload["configs"]["remoteDebugging"], false);
        assert_eq!(payload["configs"]["maxDisplayServers"], 20);
        assert_eq!(payload["configs"]["requestWebLocation"], true);
        assert_eq!(payload["configs"]["latency"]["maxServers"], 10);
        assert_eq!(payload["configs"]["jsEngine"]["saveType"], "st4-js");
        assert_eq!(payload["configs"]["stagesList"][0], "latency");
        assert_eq!(payload["configs"]["loadedLatency"]["enabled"], true);
        assert_eq!(payload["configs"]["swf"]["engine"], "/engine.swf");
        assert_eq!(payload["configs"]["vpnDetected"], false);
        assert_eq!(payload["configs"]["logErrorsToServer"]["level"], "warn");
        assert_eq!(payload["configs"]["logErrorsToServer"]["maxPerClient"], 100);
        assert_eq!(payload["location"]["countryCode"], "FR");
        assert_eq!(payload["latency"]["connectionProtocol"], "wss");
        assert_eq!(payload["jitter"], 0.0);
        assert_eq!(payload["latency"]["tcp"]["count"], 1);
        assert_eq!(payload["latency"]["tcp"]["samples"][0], 2.940037);
        assert_eq!(payload["latency"]["tcp"]["graphSamples"][0], 2.940037);
        assert_eq!(payload["latency"]["tcp"]["rtt"]["mean"], 2.940037);
        assert_eq!(payload["latency"]["tcp"]["rtt"]["median"], 2.940037);
        assert_eq!(payload["latency"]["tcp"]["rtt"]["iqm"], 2.940037);
        assert_eq!(payload["latency"]["tcp"]["rtt"]["min"], 2.940037);
        assert_eq!(payload["latency"]["tcp"]["rtt"]["max"], 2.940037);
        assert!(payload["downloadSamples"].is_array());
        assert!(payload["uploadSamples"].is_array());
        assert!(payload["downloadSpeeds"]["local"]["mst_66_20"].is_number());
        assert!(payload["downloadSpeeds"]["local"]["mst_66_30"].is_number());
        assert!(payload["downloadSpeeds"]["local"]["mst_75_30"].is_number());
        assert_eq!(payload["downloadSpeeds"]["local"]["combined"], 222162000);
        assert_eq!(payload["downloadSpeeds"]["local"]["average"], 222162000.0);
        assert_eq!(payload["uploadSpeeds"]["local"]["combined"], 98950000);
        assert_eq!(payload["uploadSpeeds"]["local"]["average"], 98950000.0);
        assert!(payload["serverSelection"]["closestPingDetails"].is_array());
        assert_eq!(payload["clientip"], "159.26.112.4");
        assert_eq!(payload["hash"].as_str().map(str::len), Some(32));
    }

    #[test]
    fn writes_sdk_payload_file() {
        let result = sample_result();
        let temp =
            std::env::temp_dir().join(format!("tunmux-speedtest-sdk-{}.json", std::process::id()));

        let guid = write_sdk_result_json_file(&result, &temp, Some("guid-123"))
            .expect("must write payload file");
        assert_eq!(guid, "guid-123");

        let content = std::fs::read_to_string(&temp).expect("must read payload file");
        assert!(content.contains("\"source\": \"st4-js\""));

        let _ = std::fs::remove_file(temp);
    }

    #[test]
    fn omits_missing_direction_metrics() {
        let mut result = sample_result();
        result.upload = None;

        let payload = build_sdk_result_payload(&result, "eca27dc9-e3dd-429a-85e7-8ee178facc6c")
            .expect("payload should build without upload");

        assert!(payload.get("upload").is_none());
        assert!(payload.get("uploadSpeeds").is_none());
        assert!(payload.get("uploadSamples").is_none());
        assert!(payload.get("uploadLatency").is_none());
        assert!(payload.get("download").is_some());
    }

    fn sample_result() -> RunResult {
        RunResult {
            timestamp: "1771795845".to_string(),
            speedtest_api: Some("modern".to_string()),
            client: Some(ClientMeta {
                ip: "159.26.112.4".to_string(),
                isp: "ProtonVPN".to_string(),
                country: "FR".to_string(),
                latitude: 43.2951,
                longitude: 5.3861,
                isp_id: Some(316789),
                provider_hash: Some("provider-hash".to_string()),
            }),
            server: Some(Server {
                id: 61301,
                sponsor: "ORANGE FRANCE".to_string(),
                name: "Marseille".to_string(),
                country: "France".to_string(),
                host: "marseille3.d2m.c2d.liveservices.fr.prod.hosts.ooklaserver.net:8080"
                    .to_string(),
                distance_km: 1.0,
                latency_ms: Some(2.8),
                latency_stddev_ms: Some(0.2),
                download_avg_mbps: None,
                download_bytes: None,
                sdk_url: Some(
                    "http://marseille3.d2m.c2d.liveservices.fr:8080/speedtest/upload.php"
                        .to_string(),
                ),
                sdk_lat: Some("43.2964".to_string()),
                sdk_lon: Some("5.3700".to_string()),
                sdk_cc: Some("FR".to_string()),
                sdk_preferred: Some(0),
                sdk_isp_id: Some("48496".to_string()),
                sdk_https_functional: Some(1),
                sdk_hostname: Some(
                    "marseille3.d2m.c2d.liveservices.fr.prod.hosts.ooklaserver.net".to_string(),
                ),
                sdk_port: Some(8080),
                sdk_force_ping_select: Some(1),
            }),
            server_pool: None,
            ping_ms: Some(2.8),
            download: Some(BenchmarkResult {
                mbps: 222.162,
                bytes: 0,
                duration_seconds: 10,
                connections: 8,
            }),
            upload: Some(BenchmarkResult {
                mbps: 98.95,
                bytes: 0,
                duration_seconds: 10,
                connections: 8,
            }),
            proxy: None,
            sdk_selected_latency_samples_ms: Some(vec![2.940037]),
            sdk_download_intervals: Some(vec![ThroughputInterval {
                elapsed_seconds: 10.0,
                bytes: 277702500,
                mbps: 222.162,
            }]),
            sdk_upload_intervals: Some(vec![ThroughputInterval {
                elapsed_seconds: 10.0,
                bytes: 123687500,
                mbps: 98.95,
            }]),
            sdk_upload_remote_intervals: None,
            sdk_download_latency_samples_ms: None,
            sdk_upload_latency_samples_ms: None,
            details: None,
        }
    }
}
