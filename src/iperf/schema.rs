use serde::{Deserialize, Serialize};

pub const IPERF_SCHEMA_V1: &str = "speedtest-rs.iperf.v1";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IperfJsonV1 {
    pub schema: String,
    pub timestamp: String,
    pub target: IperfTarget,
    pub protocol: IperfProtocolOut,
    pub proxy: Option<IperfProxyOut>,
    pub config: IperfConfigOut,
    pub results: IperfResultsOut,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<IperfDetailsOut>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IperfTarget {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IperfProtocolOut {
    Tcp,
    Udp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IperfProxyOut {
    pub url: String,
    pub scheme: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IperfConfigOut {
    pub seconds: u64,
    pub parallel: usize,
    pub bitrate_bps: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IperfResultsOut {
    pub upload: Option<IperfDirectionOut>,
    pub download: Option<IperfDirectionOut>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IperfDirectionOut {
    pub bytes: u64,
    pub mbps: f64,
    pub duration_seconds: u64,
    pub packets: Option<u64>,
    pub lost_packets: Option<u64>,
    pub loss_percent: Option<f64>,
    pub jitter_ms: Option<f64>,
    pub out_of_order: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IperfDetailsOut {
    pub interval_seconds: u64,
    pub results: IperfIntervalResultsOut,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IperfIntervalResultsOut {
    pub upload: Option<IperfDirectionDetailsOut>,
    pub download: Option<IperfDirectionDetailsOut>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IperfDirectionDetailsOut {
    pub intervals: Vec<IperfIntervalOut>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IperfIntervalOut {
    pub elapsed_seconds: f64,
    pub bytes: u64,
    pub mbps: f64,
}

#[cfg(test)]
mod tests {
    use super::{
        IPERF_SCHEMA_V1, IperfConfigOut, IperfDirectionOut, IperfJsonV1, IperfProtocolOut,
        IperfResultsOut, IperfTarget,
    };

    #[test]
    fn serializes_schema_with_optional_directions() {
        let body = IperfJsonV1 {
            schema: IPERF_SCHEMA_V1.to_string(),
            timestamp: "1".to_string(),
            target: IperfTarget {
                host: "127.0.0.1".to_string(),
                port: 5201,
            },
            protocol: IperfProtocolOut::Tcp,
            proxy: None,
            config: IperfConfigOut {
                seconds: 10,
                parallel: 1,
                bitrate_bps: None,
            },
            results: IperfResultsOut {
                upload: Some(IperfDirectionOut {
                    bytes: 100,
                    mbps: 1.0,
                    duration_seconds: 10,
                    packets: None,
                    lost_packets: None,
                    loss_percent: None,
                    jitter_ms: None,
                    out_of_order: None,
                }),
                download: None,
            },
            details: None,
        };

        let json = serde_json::to_string(&body).expect("json serialization should succeed");
        assert!(json.contains("\"schema\":\"speedtest-rs.iperf.v1\""));
        assert!(json.contains("\"download\":null"));
    }
}
