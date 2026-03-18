use serde::Serialize;

use crate::model::{RunResult, SdkArtifacts};

#[derive(Debug, Serialize)]
pub(super) struct SdkLatencyPayload {
    #[serde(rename = "connectionProtocol")]
    pub(super) connection_protocol: String,
    pub(super) tcp: SdkLatencyTcp,
}

#[derive(Debug, Serialize)]
pub(super) struct SdkLatencyTcp {
    pub(super) rtt: SdkRtt,
    pub(super) jitter: f64,
    pub(super) count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) samples: Option<Vec<f64>>,
    #[serde(rename = "graphSamples", skip_serializing_if = "Option::is_none")]
    pub(super) graph_samples: Option<Vec<f64>>,
}

#[derive(Debug, Serialize)]
pub(super) struct SdkRtt {
    pub(super) iqm: f64,
    pub(super) mean: f64,
    pub(super) median: f64,
    pub(super) min: f64,
    pub(super) max: f64,
}

pub(super) fn selected_latency_samples(
    result: &RunResult,
    sdk_artifacts: &SdkArtifacts,
) -> Option<Vec<f64>> {
    let samples = sdk_artifacts
        .selected_latency_samples_ms
        .as_ref()
        .or_else(|| {
            result
                .details
                .as_ref()
                .and_then(|details| details.selected_server_latency.samples_ms.as_ref())
        })?;

    let normalized = samples
        .iter()
        .copied()
        .filter(|sample| sample.is_finite() && *sample >= 0.0)
        .collect::<Vec<_>>();

    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

pub(super) fn calculate_jitter_ms(samples: &[f64]) -> f64 {
    if samples.len() < 2 {
        return 0.0;
    }

    let mut total_delta = 0.0;
    for index in 1..samples.len() {
        total_delta += (samples[index] - samples[index - 1]).abs();
    }

    round_to(total_delta / (samples.len() - 1) as f64, 3)
}

pub(super) fn build_latency_payload(
    samples: &[f64],
    jitter_ms: f64,
    connection_protocol: &str,
    include_sample_arrays: bool,
) -> Option<SdkLatencyPayload> {
    let stats = latency_stats(samples)?;
    let jitter = if jitter_ms.is_finite() && jitter_ms >= 0.0 {
        jitter_ms
    } else {
        calculate_jitter_ms(samples)
    };

    Some(SdkLatencyPayload {
        connection_protocol: connection_protocol.to_string(),
        tcp: SdkLatencyTcp {
            rtt: stats,
            jitter,
            count: samples.len() as u64,
            samples: include_sample_arrays.then(|| samples.to_vec()),
            graph_samples: include_sample_arrays.then(|| samples.to_vec()),
        },
    })
}

pub(super) fn latency_stats(samples: &[f64]) -> Option<SdkRtt> {
    if samples.is_empty() {
        return None;
    }

    let mut values = samples
        .iter()
        .copied()
        .filter(|sample| sample.is_finite() && *sample >= 0.0)
        .collect::<Vec<_>>();
    if values.is_empty() {
        return None;
    }

    values.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let min = values[0];
    let max = values[values.len() - 1];
    let median = if values.len() <= 2 {
        mean
    } else {
        let middle = values.len() / 2;
        if values.len() % 2 == 1 {
            values[middle]
        } else {
            let left = values[middle];
            let right = values.get(middle + 1).copied().unwrap_or(left);
            (left + right) / 2.0
        }
    };
    let iqm = calculate_iqm(&values);

    Some(SdkRtt {
        iqm,
        mean,
        median,
        min,
        max,
    })
}

fn calculate_iqm(values: &[f64]) -> f64 {
    match values.len() {
        0 => 0.0,
        1 => values[0],
        2 => (values[0] + values[1]) / 2.0,
        len => {
            let lower = len as f64 / 4.0;
            let upper = 3.0 * len as f64 / 4.0;
            let start = lower.ceil() as usize;
            let end = upper.floor() as usize;
            let fraction = upper - upper.floor();
            let core_sum = if start < end {
                values[start..end].iter().sum::<f64>()
            } else {
                0.0
            };
            let edge_sum = if start > 0 && end < len {
                values[start - 1] + values[end]
            } else {
                0.0
            };
            (fraction * edge_sum + core_sum) / (len as f64 / 2.0)
        }
    }
}

fn round_to(value: f64, digits: i32) -> f64 {
    let scale = 10_f64.powi(digits);
    (value * scale).round() / scale
}
