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
pub struct RunResult {
    pub timestamp: String,
    pub client: Option<ClientMeta>,
    pub server: Option<Server>,
    pub ping_ms: Option<f64>,
    pub download: Option<BenchmarkResult>,
    pub upload: Option<BenchmarkResult>,
    pub proxy: Option<String>,
}
