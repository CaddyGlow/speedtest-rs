use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientMeta {
    pub ip: String,
    pub isp: String,
    pub country: String,
    pub latitude: f64,
    pub longitude: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Server {
    pub id: u64,
    pub sponsor: String,
    pub name: String,
    pub country: String,
    pub host: String,
    pub distance_km: f64,
    pub latency_ms: Option<f64>,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectedServerLatencyDetails {
    pub average_ms: f64,
    pub variance_ms: f64,
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
    pub client: Option<ClientMeta>,
    pub server: Option<Server>,
    pub ping_ms: Option<f64>,
    pub download: Option<BenchmarkResult>,
    pub upload: Option<BenchmarkResult>,
    pub proxy: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<RunDetails>,
}
