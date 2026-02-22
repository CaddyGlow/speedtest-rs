use std::time::Duration;

use crate::cli::IperfProtocol;
use crate::iperf::proxy::ProxySpec;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IperfDirection {
    Upload,
    Download,
}

impl IperfDirection {
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Upload => "upload",
            Self::Download => "download",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct IperfProgress {
    pub elapsed: Duration,
    pub bytes: u64,
    pub mbps: f64,
}

#[derive(Debug, Clone)]
pub struct IperfDirectionSummary {
    pub bytes: u64,
    pub mbps: f64,
    pub duration_seconds: u64,
    pub packets: Option<u64>,
    pub lost_packets: Option<u64>,
    pub loss_percent: Option<f64>,
    pub jitter_ms: Option<f64>,
    pub out_of_order: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct IperfClientConfig {
    pub host: String,
    pub port: u16,
    pub protocol: IperfProtocol,
    pub seconds: u64,
    pub parallel: usize,
    pub bitrate_bps: Option<u64>,
    pub proxy: Option<ProxySpec>,
}
