use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientMeta {
    pub ip: String,
    pub isp: String,
    pub country: String,
    pub latitude: f64,
    pub longitude: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub isp_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Server {
    pub id: u64,
    pub sponsor: String,
    pub name: String,
    pub country: String,
    pub host: String,
    pub distance_km: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_stddev_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub download_avg_mbps: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub download_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sdk_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sdk_lat: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sdk_lon: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sdk_cc: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sdk_preferred: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sdk_isp_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sdk_https_functional: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sdk_hostname: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sdk_port: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sdk_force_ping_select: Option<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkResult {
    pub mbps: f64,
    pub bytes: u64,
    pub duration_seconds: u64,
    pub connections: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThroughputInterval {
    pub elapsed_seconds: f64,
    pub bytes: u64,
    pub mbps: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectionDetails {
    pub request_attempts: u64,
    pub request_successes: u64,
    pub request_http_errors: u64,
    pub request_transport_errors: u64,
    pub response_read_errors: u64,
    pub intervals: Vec<ThroughputInterval>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remote_intervals: Option<Vec<ThroughputInterval>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectedServerLatencyDetails {
    pub average_ms: f64,
    pub variance_ms: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stddev_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub samples_ms: Option<Vec<f64>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunDetails {
    pub interval_seconds: u64,
    pub selected_server_latency: SelectedServerLatencyDetails,
    pub download: Option<DirectionDetails>,
    pub upload: Option<DirectionDetails>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunResult {
    pub timestamp: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub speedtest_api: Option<String>,
    pub client: Option<ClientMeta>,
    pub server: Option<Server>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_pool: Option<Vec<Server>>,
    pub ping_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jitter_ms: Option<f64>,
    pub download: Option<BenchmarkResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub download_latency_ms: Option<f64>,
    pub upload: Option<BenchmarkResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upload_latency_ms: Option<f64>,
    pub proxy: Option<String>,
    #[serde(skip)]
    pub sdk_selected_latency_samples_ms: Option<Vec<f64>>,
    #[serde(skip)]
    pub sdk_download_intervals: Option<Vec<ThroughputInterval>>,
    #[serde(skip)]
    pub sdk_upload_intervals: Option<Vec<ThroughputInterval>>,
    #[serde(skip)]
    pub sdk_upload_remote_intervals: Option<Vec<ThroughputInterval>>,
    #[serde(skip)]
    pub sdk_download_latency_samples_ms: Option<Vec<f64>>,
    #[serde(skip)]
    pub sdk_upload_latency_samples_ms: Option<Vec<f64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<RunDetails>,
}
