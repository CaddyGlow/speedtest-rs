mod latency;
mod metadata;
mod prepare;
mod selection;
mod throughput;
mod types;
mod util;

use std::fs;
use std::path::Path;

use anyhow::{Context, Result, bail};

use crate::model::{RunResult, SdkArtifacts};

use self::metadata::{build_sdk_app, build_sdk_configs, build_sdk_location};
use self::prepare::prepare_sdk_measurements;
use self::selection::{build_server_list_entry, parse_host_and_port};
use self::types::SdkPayload;

pub(crate) use self::util::generate_sdk_guid;

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
#[cfg(test)]
mod tests {
    use super::{build_sdk_result_payload, write_sdk_result_json_file};
    use crate::model::{
        BenchmarkResult, ClientMeta, RunResult, SdkArtifacts, Server, ThroughputInterval,
    };

    #[test]
    fn builds_sdk_payload_from_run_result() {
        let (result, sdk_artifacts) = sample_result();
        let payload = build_sdk_result_payload(
            &result,
            &sdk_artifacts,
            "eca27dc9-e3dd-429a-85e7-8ee178facc6c",
        )
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
            std::env::temp_dir().join(format!("speedtest-rs-sdk-{}.json", std::process::id()));

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

        let payload = build_sdk_result_payload(
            &result,
            &sdk_artifacts,
            "eca27dc9-e3dd-429a-85e7-8ee178facc6c",
        )
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
