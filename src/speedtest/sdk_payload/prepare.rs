use anyhow::Result;
use tracing::debug;

use crate::model::{BenchmarkResult, DirectionDetails, RunResult, SdkArtifacts, Server};

use super::latency::{
    SdkLatencyPayload, build_latency_payload, calculate_jitter_ms, latency_stats,
    selected_latency_samples,
};
use super::metadata::{build_supplemental_data, infer_protocols, split_client_ips};
use super::selection::build_server_selection;
use super::throughput::{
    SdkDirectionSpeeds, SdkThroughputSample, build_direction_samples, build_speed_profile,
    direction_samples_from_intervals, local_upload_bps, remote_upload_bps,
};
use super::types::PreparedSdkMeasurements;
use super::util;

pub(super) fn prepare_sdk_measurements(
    result: &RunResult,
    sdk_artifacts: &SdkArtifacts,
    server: &Server,
    ping_ms: f64,
) -> Result<PreparedSdkMeasurements> {
    let protocols = infer_protocols(result.speedtest_api.as_deref());
    let pings = selected_latency_samples(result, sdk_artifacts).unwrap_or_else(|| vec![ping_ms]);
    let ping = latency_stats(&pings)
        .map(|stats| stats.min)
        .unwrap_or(ping_ms);
    let jitter = calculate_jitter_ms(&pings);
    let download_bps = result
        .download
        .as_ref()
        .map(util::mbps_to_bps)
        .transpose()?;
    let upload_effective_bps = result.upload.as_ref().map(util::mbps_to_bps).transpose()?;
    let upload_local_bps = if upload_effective_bps.is_some() {
        local_upload_bps(result, sdk_artifacts).or(upload_effective_bps)
    } else {
        None
    };
    let upload_remote_bps = remote_upload_bps(result, sdk_artifacts);
    let download_latency_samples = sdk_artifacts
        .download_latency_samples_ms
        .clone()
        .filter(|samples| !samples.is_empty())
        .unwrap_or_else(|| pings.clone());
    let upload_latency_samples = sdk_artifacts
        .upload_latency_samples_ms
        .clone()
        .filter(|samples| !samples.is_empty())
        .unwrap_or_else(|| pings.clone());
    let latency =
        build_latency_payload(&pings, jitter, protocols.latency_connection_protocol, true);
    let download_latency = build_optional_direction_latency(
        result.download.as_ref(),
        &download_latency_samples,
        protocols.download_connection_protocol,
    );
    let upload_latency = build_optional_direction_latency(
        result.upload.as_ref(),
        &upload_latency_samples,
        protocols.upload_connection_protocol,
    );
    let download_samples = build_optional_direction_samples(
        download_bps.is_some(),
        sdk_artifacts.download_intervals.as_deref(),
        result
            .details
            .as_ref()
            .and_then(|details| details.download.as_ref()),
        result.download.as_ref(),
    )?;
    let upload_samples = build_optional_direction_samples(
        upload_effective_bps.is_some(),
        sdk_artifacts.upload_intervals.as_deref(),
        result
            .details
            .as_ref()
            .and_then(|details| details.upload.as_ref()),
        result.upload.as_ref(),
    )?;
    let upload_remote_samples = sdk_artifacts
        .upload_remote_intervals
        .as_deref()
        .and_then(direction_samples_from_intervals);
    let download_speed_profile = build_speed_profile(
        download_bps,
        download_samples.as_deref(),
        direction_duration_seconds(result.download.as_ref()),
    );
    let upload_local_speed_profile = build_speed_profile(
        upload_local_bps,
        upload_samples.as_deref(),
        direction_duration_seconds(result.upload.as_ref()),
    );
    let upload_remote_speed_profile = build_speed_profile(
        upload_remote_bps,
        upload_remote_samples.as_deref(),
        direction_duration_seconds(result.upload.as_ref()),
    );
    let download = download_speed_profile
        .as_ref()
        .map(|profile| util::bps_to_sdk_units(profile.combined))
        .or(download_bps.map(util::bps_to_sdk_units));
    let upload = upload_remote_speed_profile
        .as_ref()
        .or(upload_local_speed_profile.as_ref())
        .map(|profile| util::bps_to_sdk_units(profile.combined))
        .or(upload_effective_bps.map(util::bps_to_sdk_units));
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

    let (clientip, ip6_address) = result
        .client
        .as_ref()
        .map(|client| split_client_ips(&client.ip))
        .unwrap_or((None, None));
    let server_selection = build_server_selection(result, server, ping);
    let supplemental_data = build_supplemental_data(result)?;
    let upload_speeds = upload_effective_bps.map(|_| SdkDirectionSpeeds {
        local: upload_local_speed_profile,
        remote: upload_remote_speed_profile,
    });
    let download_speeds = download_bps.map(|_| SdkDirectionSpeeds {
        local: download_speed_profile,
        remote: None,
    });
    let hash = util::calculate_result_hash(ping, upload, download);

    Ok(PreparedSdkMeasurements {
        protocols,
        ping,
        pings,
        jitter,
        latency,
        download_latency,
        upload_latency,
        download,
        upload,
        download_samples,
        upload_samples,
        download_speeds,
        upload_speeds,
        server_selection,
        upload_measurement_method,
        clientip,
        ip6_address,
        supplemental_data,
        hash,
    })
}

fn build_optional_direction_latency(
    result: Option<&BenchmarkResult>,
    latency_samples: &[f64],
    connection_protocol: &'static str,
) -> Option<SdkLatencyPayload> {
    result.and_then(|_| {
        build_latency_payload(
            latency_samples,
            calculate_jitter_ms(latency_samples),
            connection_protocol,
            false,
        )
    })
}

fn build_optional_direction_samples(
    enabled: bool,
    intervals: Option<&[crate::model::ThroughputInterval]>,
    details: Option<&DirectionDetails>,
    result: Option<&BenchmarkResult>,
) -> Result<Option<Vec<SdkThroughputSample>>> {
    if enabled {
        build_direction_samples(intervals, details, result)
    } else {
        Ok(None)
    }
}

fn direction_duration_seconds(result: Option<&BenchmarkResult>) -> Option<u64> {
    result.map(|stats| stats.duration_seconds)
}
