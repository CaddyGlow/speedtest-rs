mod latency;
mod metadata;
mod selection;
mod throughput;

use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde::Serialize;
use serde_json::Value;
use tracing::debug;

use crate::model::{BenchmarkResult, RunResult, SdkArtifacts};

use self::latency::{
    SdkLatencyPayload, calculate_jitter_ms, build_latency_payload, latency_stats,
    selected_latency_samples,
};
use self::metadata::{
    SdkApp, SdkConfigs, SdkLocation, build_sdk_app, build_sdk_configs, build_sdk_location,
    build_supplemental_data, infer_protocols, split_client_ips,
};
use self::selection::{
    SdkServerSelection, build_server_list_entry, build_server_selection, parse_host_and_port,
};
use self::throughput::{
    SdkDirectionSpeeds, SdkThroughputSample, build_direction_samples, build_speed_profile,
    direction_samples_from_intervals, local_upload_bps, remote_upload_bps,
};

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
    sdk_artifacts: &SdkArtifacts,
    output_path: &Path,
    guid: Option<&str>,
) -> Result<String> {
    let guid = guid
        .map(ToString::to_string)
        .unwrap_or_else(generate_sdk_guid);
    let payload = build_sdk_result_payload(result, sdk_artifacts, &guid)?;
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

pub fn build_sdk_result_payload(
    result: &RunResult,
    sdk_artifacts: &SdkArtifacts,
    guid: &str,
) -> Result<serde_json::Value> {
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

    let fallback_url = format!("https://{}/speedtest/upload.php", server.host);
    let (hostname, port) = parse_host_and_port(&server.host, &fallback_url);
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
    let prepared = prepare_sdk_measurements(result, sdk_artifacts, server, ping_ms)?;

    let payload = SdkPayload {
        app: build_sdk_app(),
        serverid: server.id,
        testmethod: prepared.protocols.test_method.to_string(),
        source: "st4-js".to_string(),
        configs: build_sdk_configs(client, prepared.protocols, server_list, hostname, port),
        location: Some(build_sdk_location(client)),
        isp_name: client.isp.clone(),
        ping: prepared.ping,
        pings: prepared.pings,
        jitter: prepared.jitter,
        latency: prepared.latency,
        guid: guid.to_string(),
        server_selection_guid: guid.to_string(),
        server_selection_method: "auto".to_string(),
        server_selection: prepared.server_selection,
        upload_measurement_method: prepared.upload_measurement_method.to_string(),
        upload: prepared.upload,
        upload_speeds: prepared.upload_speeds,
        download: prepared.download,
        download_speeds: prepared.download_speeds,
        download_latency: prepared.download_latency,
        upload_latency: prepared.upload_latency,
        supplemental_data: prepared.supplemental_data,
        download_samples: prepared.download_samples,
        upload_samples: prepared.upload_samples,
        spoofed: false,
        clientip: prepared.clientip,
        ip6_address: prepared.ip6_address,
        hash: prepared.hash,
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

fn calculate_result_hash(ping: f64, upload: Option<u64>, download: Option<u64>) -> String {
    let ping = if ping.is_finite() { ping } else { 0.0 };
    let upload = upload.unwrap_or(0);
    let download = download.unwrap_or(0);
    let hash_input = format!("{ping}-{upload}-{download}-817d699764d33f89c");
    format!("{:x}", md5::compute(hash_input))
}

fn prepare_sdk_measurements(
    result: &RunResult,
    sdk_artifacts: &SdkArtifacts,
    server: &crate::model::Server,
    ping_ms: f64,
) -> Result<PreparedSdkMeasurements> {
    let protocols = infer_protocols(result.speedtest_api.as_deref());
    let pings = selected_latency_samples(result, sdk_artifacts).unwrap_or_else(|| vec![ping_ms]);
    let ping = latency_stats(&pings)
        .map(|stats| stats.min)
        .unwrap_or(ping_ms);
    let jitter = calculate_jitter_ms(&pings);
    let download_bps = result.download.as_ref().map(mbps_to_bps).transpose()?;
    let upload_effective_bps = result.upload.as_ref().map(mbps_to_bps).transpose()?;
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
    let download_samples = if download_bps.is_some() {
        build_direction_samples(
            sdk_artifacts.download_intervals.as_deref(),
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
            sdk_artifacts.upload_intervals.as_deref(),
            result
                .details
                .as_ref()
                .and_then(|details| details.upload.as_ref()),
            result.upload.as_ref(),
        )?
    } else {
        None
    };
    let upload_remote_samples = sdk_artifacts
        .upload_remote_intervals
        .as_deref()
        .and_then(direction_samples_from_intervals);
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
        .or(download_bps.map(bps_to_sdk_units));
    let upload = upload_remote_speed_profile
        .as_ref()
        .or(upload_local_speed_profile.as_ref())
        .map(|profile| bps_to_sdk_units(profile.combined))
        .or(upload_effective_bps.map(bps_to_sdk_units));
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
    let hash = calculate_result_hash(ping, upload, download);

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

struct PreparedSdkMeasurements {
    protocols: self::metadata::SdkProtocols,
    ping: f64,
    pings: Vec<f64>,
    jitter: f64,
    latency: Option<SdkLatencyPayload>,
    download_latency: Option<SdkLatencyPayload>,
    upload_latency: Option<SdkLatencyPayload>,
    download: Option<u64>,
    upload: Option<u64>,
    download_samples: Option<Vec<SdkThroughputSample>>,
    upload_samples: Option<Vec<SdkThroughputSample>>,
    download_speeds: Option<SdkDirectionSpeeds>,
    upload_speeds: Option<SdkDirectionSpeeds>,
    server_selection: Option<SdkServerSelection>,
    upload_measurement_method: &'static str,
    clientip: Option<String>,
    ip6_address: Option<String>,
    supplemental_data: Value,
    hash: String,
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

#[cfg(test)]
mod tests {
    use super::{build_sdk_result_payload, write_sdk_result_json_file};
    use crate::model::{
        BenchmarkResult, ClientMeta, RunResult, SdkArtifacts, Server, ThroughputInterval,
    };

    #[test]
    fn builds_sdk_payload_from_run_result() {
        let (result, sdk_artifacts) = sample_result();
        let payload =
            build_sdk_result_payload(&result, &sdk_artifacts, "eca27dc9-e3dd-429a-85e7-8ee178facc6c")
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
        let (result, sdk_artifacts) = sample_result();
        let temp =
            std::env::temp_dir().join(format!("tunmux-speedtest-sdk-{}.json", std::process::id()));

        let guid = write_sdk_result_json_file(&result, &sdk_artifacts, &temp, Some("guid-123"))
            .expect("must write payload file");
        assert_eq!(guid, "guid-123");

        let content = std::fs::read_to_string(&temp).expect("must read payload file");
        assert!(content.contains("\"source\": \"st4-js\""));

        let _ = std::fs::remove_file(temp);
    }

    #[test]
    fn omits_missing_direction_metrics() {
        let (mut result, sdk_artifacts) = sample_result();
        result.upload = None;

        let payload =
            build_sdk_result_payload(&result, &sdk_artifacts, "eca27dc9-e3dd-429a-85e7-8ee178facc6c")
                .expect("payload should build without upload");

        assert!(payload.get("upload").is_none());
        assert!(payload.get("uploadSpeeds").is_none());
        assert!(payload.get("uploadSamples").is_none());
        assert!(payload.get("uploadLatency").is_none());
        assert!(payload.get("download").is_some());
    }

    fn sample_result() -> (RunResult, SdkArtifacts) {
        let result = RunResult {
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
            jitter_ms: None,
            download: Some(BenchmarkResult {
                mbps: 222.162,
                bytes: 0,
                duration_seconds: 10,
                connections: 8,
                actual_duration_seconds: None,
                average_mbps: None,
                mst_mbps: None,
            }),
            download_latency_ms: None,
            upload: Some(BenchmarkResult {
                mbps: 98.95,
                bytes: 0,
                duration_seconds: 10,
                connections: 8,
                actual_duration_seconds: None,
                average_mbps: None,
                mst_mbps: None,
            }),
            upload_latency_ms: None,
            proxy: None,
            details: None,
        };

        let sdk_artifacts = SdkArtifacts {
            selected_latency_samples_ms: Some(vec![2.940037]),
            download_intervals: Some(vec![ThroughputInterval {
                elapsed_seconds: 10.0,
                bytes: 277702500,
                mbps: 222.162,
            }]),
            upload_intervals: Some(vec![ThroughputInterval {
                elapsed_seconds: 10.0,
                bytes: 123687500,
                mbps: 98.95,
            }]),
            upload_remote_intervals: None,
            download_latency_samples_ms: None,
            upload_latency_samples_ms: None,
        };

        (result, sdk_artifacts)
    }
}
