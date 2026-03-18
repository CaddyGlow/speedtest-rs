use anyhow::Result;
use serde::Serialize;
use serde_json::{Value, json};

use crate::model::{ClientMeta, RunResult};

use super::selection::SdkServerListEntry;

#[derive(Debug, Clone, Copy)]
pub(super) struct SdkProtocols {
    pub(super) test_method: &'static str,
    pub(super) latency_protocol: &'static str,
    pub(super) download_protocol: &'static str,
    pub(super) upload_protocol: &'static str,
    pub(super) latency_connection_protocol: &'static str,
    pub(super) download_connection_protocol: &'static str,
    pub(super) upload_connection_protocol: &'static str,
}

#[derive(Debug, Serialize)]
pub(super) struct SdkApp {
    pub(super) sdk: SdkAppVersion,
}

#[derive(Debug, Serialize)]
pub(super) struct SdkAppVersion {
    pub(super) commit: String,
    pub(super) version: String,
}

#[derive(Debug, Serialize)]
pub(super) struct SdkConfigs {
    #[serde(rename = "remoteDebugging")]
    pub(super) remote_debugging: bool,
    #[serde(rename = "maxDisplayServers")]
    pub(super) max_display_servers: u64,
    #[serde(rename = "requestWebLocation")]
    pub(super) request_web_location: bool,
    #[serde(rename = "shortTests")]
    pub(super) short_tests: bool,
    #[serde(rename = "automaticStageProgression")]
    pub(super) automatic_stage_progression: bool,
    #[serde(rename = "eventSkipInterval")]
    pub(super) event_skip_interval: u64,
    pub(super) latency: SdkLatencyConfig,
    #[serde(rename = "jsEngine")]
    pub(super) js_engine: SdkJsEngine,
    #[serde(rename = "stagesList")]
    pub(super) stages_list: Vec<String>,
    #[serde(rename = "loadedLatency")]
    pub(super) loaded_latency: SdkLoadedLatency,
    pub(super) swf: SdkSwf,
    pub(super) provider: SdkProvider,
    #[serde(rename = "vpnDetected")]
    pub(super) vpn_detected: bool,
    #[serde(rename = "logErrorsToServer")]
    pub(super) log_errors_to_server: SdkLogErrorsToServer,
    pub(super) connections: SdkConnections,
    #[serde(rename = "serverList")]
    pub(super) server_list: Vec<SdkServerListEntry>,
    #[serde(rename = "latencyProtocol")]
    pub(super) latency_protocol: String,
    #[serde(rename = "downloadProtocol")]
    pub(super) download_protocol: String,
    #[serde(rename = "uploadProtocol")]
    pub(super) upload_protocol: String,
    pub(super) host: String,
    pub(super) port: u16,
    #[serde(rename = "serverVersion")]
    pub(super) server_version: String,
    #[serde(rename = "serverBuild")]
    pub(super) server_build: String,
}

#[derive(Debug, Serialize)]
pub(super) struct SdkLatencyConfig {
    #[serde(rename = "maxServers")]
    pub(super) max_servers: u64,
}

#[derive(Debug, Serialize)]
pub(super) struct SdkJsEngine {
    #[serde(rename = "saveContentType")]
    pub(super) save_content_type: String,
    #[serde(rename = "saveType")]
    pub(super) save_type: String,
}

#[derive(Debug, Serialize)]
pub(super) struct SdkLoadedLatency {
    pub(super) enabled: bool,
}

#[derive(Debug, Serialize)]
pub(super) struct SdkSwf {
    pub(super) engine: String,
    pub(super) express: String,
}

#[derive(Debug, Serialize)]
pub(super) struct SdkProvider {
    #[serde(rename = "countryCode")]
    pub(super) country_code: String,
    #[serde(rename = "ipAddress")]
    pub(super) ip_address: String,
    #[serde(rename = "ispName")]
    pub(super) isp_name: String,
    #[serde(rename = "providerName")]
    pub(super) provider_name: String,
    #[serde(rename = "ispId", skip_serializing_if = "Option::is_none")]
    pub(super) isp_id: Option<u64>,
    #[serde(rename = "providerHash", skip_serializing_if = "Option::is_none")]
    pub(super) provider_hash: Option<String>,
}

#[derive(Debug, Serialize)]
pub(super) struct SdkLocation {
    pub(super) country: String,
    #[serde(rename = "countryCode")]
    pub(super) country_code: String,
    pub(super) lat: f64,
    pub(super) lon: f64,
}

#[derive(Debug, Serialize)]
pub(super) struct SdkLogErrorsToServer {
    pub(super) level: String,
    #[serde(rename = "useCostanza")]
    pub(super) use_costanza: bool,
    #[serde(rename = "maxPerClient")]
    pub(super) max_per_client: u64,
    #[serde(rename = "allowDuringTest")]
    pub(super) allow_during_test: bool,
    #[serde(rename = "expensiveStackTraces")]
    pub(super) expensive_stack_traces: bool,
}

#[derive(Debug, Serialize)]
pub(super) struct SdkConnections {
    #[serde(rename = "isVpn")]
    pub(super) is_vpn: bool,
    #[serde(rename = "selectionMethod")]
    pub(super) selection_method: String,
    pub(super) mode: String,
}

pub(super) fn build_sdk_app() -> SdkApp {
    SdkApp {
        sdk: SdkAppVersion {
            commit: "tunmux-generated".to_string(),
            version: "3.1.1".to_string(),
        },
    }
}

pub(super) fn build_sdk_configs(
    client: &ClientMeta,
    protocols: SdkProtocols,
    server_list: Vec<SdkServerListEntry>,
    hostname: String,
    port: u16,
) -> SdkConfigs {
    SdkConfigs {
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
    }
}

pub(super) fn build_sdk_location(client: &ClientMeta) -> SdkLocation {
    SdkLocation {
        country: client.country.clone(),
        country_code: client.country.clone(),
        lat: client.latitude,
        lon: client.longitude,
    }
}

pub(super) fn infer_protocols(speedtest_api: Option<&str>) -> SdkProtocols {
    match speedtest_api {
        Some("tcp") => SdkProtocols {
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

pub(super) fn split_client_ips(client_ip: &str) -> (Option<String>, Option<String>) {
    match client_ip.parse::<std::net::IpAddr>() {
        Ok(std::net::IpAddr::V4(_)) => (Some(client_ip.to_string()), None),
        Ok(std::net::IpAddr::V6(_)) => (Some(client_ip.to_string()), Some(client_ip.to_string())),
        Err(_) => (Some(client_ip.to_string()), None),
    }
}

pub(super) fn build_supplemental_data(result: &RunResult) -> Result<Value> {
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
