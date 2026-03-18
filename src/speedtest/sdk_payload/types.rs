use serde::Serialize;
use serde_json::Value;

use super::latency::SdkLatencyPayload;
use super::metadata::{SdkApp, SdkConfigs, SdkLocation, SdkProtocols};
use super::selection::SdkServerSelection;
use super::throughput::{SdkDirectionSpeeds, SdkThroughputSample};

pub(super) struct PreparedSdkMeasurements {
    pub(super) protocols: SdkProtocols,
    pub(super) ping: f64,
    pub(super) pings: Vec<f64>,
    pub(super) jitter: f64,
    pub(super) latency: Option<SdkLatencyPayload>,
    pub(super) download_latency: Option<SdkLatencyPayload>,
    pub(super) upload_latency: Option<SdkLatencyPayload>,
    pub(super) download: Option<u64>,
    pub(super) upload: Option<u64>,
    pub(super) download_samples: Option<Vec<SdkThroughputSample>>,
    pub(super) upload_samples: Option<Vec<SdkThroughputSample>>,
    pub(super) download_speeds: Option<SdkDirectionSpeeds>,
    pub(super) upload_speeds: Option<SdkDirectionSpeeds>,
    pub(super) server_selection: Option<SdkServerSelection>,
    pub(super) upload_measurement_method: &'static str,
    pub(super) clientip: Option<String>,
    pub(super) ip6_address: Option<String>,
    pub(super) supplemental_data: Value,
    pub(super) hash: String,
}

#[derive(Debug, Serialize)]
pub(super) struct SdkPayload {
    pub(super) app: SdkApp,
    pub(super) serverid: u64,
    pub(super) testmethod: String,
    pub(super) source: String,
    pub(super) configs: SdkConfigs,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) location: Option<SdkLocation>,
    #[serde(rename = "ispName")]
    pub(super) isp_name: String,
    pub(super) ping: f64,
    pub(super) pings: Vec<f64>,
    pub(super) jitter: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) latency: Option<SdkLatencyPayload>,
    pub(super) guid: String,
    #[serde(rename = "serverSelectionGuid")]
    pub(super) server_selection_guid: String,
    #[serde(rename = "serverSelectionMethod")]
    pub(super) server_selection_method: String,
    #[serde(rename = "serverSelection", skip_serializing_if = "Option::is_none")]
    pub(super) server_selection: Option<SdkServerSelection>,
    #[serde(rename = "uploadMeasurementMethod")]
    pub(super) upload_measurement_method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) upload: Option<u64>,
    #[serde(rename = "uploadSpeeds", skip_serializing_if = "Option::is_none")]
    pub(super) upload_speeds: Option<SdkDirectionSpeeds>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) download: Option<u64>,
    #[serde(rename = "downloadSpeeds", skip_serializing_if = "Option::is_none")]
    pub(super) download_speeds: Option<SdkDirectionSpeeds>,
    #[serde(rename = "downloadLatency", skip_serializing_if = "Option::is_none")]
    pub(super) download_latency: Option<SdkLatencyPayload>,
    #[serde(rename = "uploadLatency", skip_serializing_if = "Option::is_none")]
    pub(super) upload_latency: Option<SdkLatencyPayload>,
    #[serde(rename = "supplementalData")]
    pub(super) supplemental_data: Value,
    #[serde(rename = "downloadSamples", skip_serializing_if = "Option::is_none")]
    pub(super) download_samples: Option<Vec<SdkThroughputSample>>,
    #[serde(rename = "uploadSamples", skip_serializing_if = "Option::is_none")]
    pub(super) upload_samples: Option<Vec<SdkThroughputSample>>,
    pub(super) spoofed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) clientip: Option<String>,
    #[serde(rename = "ip6Address", skip_serializing_if = "Option::is_none")]
    pub(super) ip6_address: Option<String>,
    pub(super) hash: String,
}
