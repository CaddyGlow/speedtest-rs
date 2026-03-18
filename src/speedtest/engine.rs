use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use chrono::{SecondsFormat, Utc};
#[cfg(test)]
use md5::compute as md5_compute;
use reqwest::Client;
use serde_json::Value;
use tokio::sync::watch;

use crate::model::{
    BenchmarkResult, ClientMeta, MstBucketOut, MstSpeedsOut, RunDetails, RunResult, SdkArtifacts,
    SelectedServerLatencyDetails, Server, ThroughputInterval,
};
use crate::speedtest::api::TransportProtocol;
use crate::speedtest::config::SpeedtestConfig;
use crate::speedtest::download;
use crate::speedtest::engine_stages::{
    finalize_engine_outcome, run_download_stage, run_upload_stage,
};
use crate::speedtest::select::{self, LatencyMeasurement, ServerLatency};
use crate::speedtest::servers::SpeedtestServer;

#[cfg(test)]
const RESULT_HASH_SALT: &str = "817d699764d33f89c";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineStage {
    ServerSelection,
    Latency,
    Download,
    Upload,
    Save,
    Finished,
}

#[derive(Debug, Clone)]
pub enum EngineEvent {
    StageStarting(EngineStage),
    CandidateProbed {
        index: usize,
        total: usize,
        server_id: u64,
        average_ms: Option<f64>,
        variance_ms: Option<f64>,
        error: Option<String>,
    },
    ServerSelected {
        server_id: u64,
        average_ms: f64,
        variance_ms: f64,
    },
    StageProgress {
        stage: EngineStage,
        elapsed: Duration,
        mbps: f64,
        bytes: u64,
        active_connections: usize,
        rtt_ms: Option<f64>,
    },
    StageResult {
        stage: EngineStage,
        mbps: f64,
        bytes: u64,
    },
    StageFinished(EngineStage),
    SavePayloadBuilt {
        guid: String,
        hash: String,
    },
}

#[derive(Debug, Clone)]
pub struct EngineSettings {
    pub server_id: Option<u64>,
    pub candidate_servers: usize,
    pub modern_pool_size: usize,
    pub latency_samples: usize,
    pub download_connections: usize,
    pub upload_connections: usize,
    pub download_seconds: u64,
    pub upload_seconds: u64,
    pub download_only: bool,
    pub upload_only: bool,
    pub details: bool,
    pub progress_interval: Option<Duration>,
}

impl Default for EngineSettings {
    fn default() -> Self {
        Self {
            server_id: None,
            candidate_servers: 10,
            modern_pool_size: 4,
            latency_samples: 10,
            download_connections: 24,
            upload_connections: 8,
            download_seconds: 15,
            upload_seconds: 15,
            download_only: false,
            upload_only: false,
            details: false,
            progress_interval: None,
        }
    }
}

impl EngineSettings {
    fn stage_order(&self) -> Vec<EngineStage> {
        if self.download_only {
            return vec![
                EngineStage::Latency,
                EngineStage::Download,
                EngineStage::Save,
            ];
        }
        if self.upload_only {
            return vec![EngineStage::Latency, EngineStage::Upload, EngineStage::Save];
        }
        vec![
            EngineStage::Latency,
            EngineStage::Download,
            EngineStage::Upload,
            EngineStage::Save,
        ]
    }

    fn validate(&self) -> Result<()> {
        if self.candidate_servers == 0 {
            bail!("candidate_servers must be greater than zero");
        }
        if self.latency_samples == 0 {
            bail!("latency_samples must be greater than zero");
        }
        if self.download_seconds == 0 || self.upload_seconds == 0 {
            bail!("download/upload durations must be greater than zero");
        }
        Ok(())
    }
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone)]
pub struct EngineOutcome {
    pub result: RunResult,
    pub sdk_artifacts: SdkArtifacts,
    pub sdk_payload: Value,
    pub selected_server: SpeedtestServer,
    #[allow(dead_code)]
    pub selected_latency: LatencyMeasurement,
    #[allow(dead_code)]
    pub transfer_pool: Vec<SpeedtestServer>,
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub struct RttSummary {
    pub iqm: f64,
    pub mean: f64,
    pub median: f64,
    pub min: f64,
    pub max: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LatencyRanking {
    pub server_id: u64,
    pub average_ms: f64,
    pub variance_ms: f64,
    pub distance_km: f64,
}

#[derive(Debug, Clone)]
struct StageMachine {
    remaining: VecDeque<EngineStage>,
}

pub(crate) struct EngineInputs<'a> {
    pub(crate) client: &'a Client,
    pub(crate) mode: TransportProtocol,
    pub(crate) settings: &'a EngineSettings,
    pub(crate) selection: &'a SelectionOutcome,
}

pub(crate) struct EngineState {
    pub(crate) result: RunResult,
    pub(crate) sdk_artifacts: SdkArtifacts,
}

impl StageMachine {
    fn new(stages: Vec<EngineStage>) -> Self {
        Self {
            remaining: VecDeque::from(stages),
        }
    }

    fn next(&mut self) -> Option<EngineStage> {
        self.remaining.pop_front()
    }
}

pub async fn run_speedtest_engine<F>(
    client: &Client,
    config: &SpeedtestConfig,
    servers: &[SpeedtestServer],
    mode: TransportProtocol,
    settings: &EngineSettings,
    mut on_event: F,
) -> Result<EngineOutcome>
where
    F: FnMut(EngineEvent),
{
    settings.validate()?;
    if servers.is_empty() {
        bail!("no speedtest servers available");
    }

    on_event(EngineEvent::StageStarting(EngineStage::ServerSelection));
    let selection = select_server(client, servers, settings, &mut on_event).await?;
    on_event(EngineEvent::ServerSelected {
        server_id: selection.selected.server.id,
        average_ms: selection.selected.average_ms,
        variance_ms: selection.selected.variance_ms,
    });

    let inputs = EngineInputs {
        client,
        mode,
        settings,
        selection: &selection,
    };
    let mut state = EngineState {
        result: build_initial_run_result(config, mode, settings, &selection)?,
        sdk_artifacts: build_initial_sdk_artifacts(&selection),
    };

    let mut stage_machine = StageMachine::new(settings.stage_order());
    while let Some(stage) = stage_machine.next() {
        on_event(EngineEvent::StageStarting(stage));
        match stage {
            EngineStage::Latency => {
                state.result.ping_ms = Some(selection.selected.average_ms);
                on_event(EngineEvent::StageFinished(EngineStage::Latency));
            }
            EngineStage::Download => {
                run_download_stage(&inputs, &mut state, &mut on_event).await?;
                on_event(EngineEvent::StageFinished(EngineStage::Download));
            }
            EngineStage::Upload => {
                run_upload_stage(&inputs, &mut state, &mut on_event).await?;
                on_event(EngineEvent::StageFinished(EngineStage::Upload));
            }
            EngineStage::Save => {
                return finalize_engine_outcome(state, selection, &mut on_event);
            }
            EngineStage::ServerSelection | EngineStage::Finished => {
                bail!("unexpected stage in stage machine: {stage:?}");
            }
        }
    }

    bail!("save stage did not execute")
}

pub(crate) struct SelectionOutcome {
    pub(crate) selected: ServerLatency,
    pub(crate) transfer_pool: Vec<SpeedtestServer>,
    pub(crate) latency_by_server: HashMap<u64, (f64, f64)>,
}

fn build_initial_run_result(
    config: &SpeedtestConfig,
    mode: TransportProtocol,
    settings: &EngineSettings,
    selection: &SelectionOutcome,
) -> Result<RunResult> {
    let jitter_ms = calculate_jitter(&selection.selected.samples_ms);

    Ok(RunResult {
        timestamp: current_timestamp()?,
        speedtest_api: Some(mode.to_string()),
        client: Some(ClientMeta {
            ip: config.client.ip.clone(),
            isp: config.client.isp.clone(),
            country: config.client.country.clone(),
            latitude: config.client.latitude,
            longitude: config.client.longitude,
            isp_id: None,
            provider_hash: None,
        }),
        server: Some(map_server(
            &selection.selected.server,
            Some((
                selection.selected.average_ms,
                selection.selected.variance_ms.sqrt(),
            )),
            None,
        )),
        server_pool: Some(
            selection
                .transfer_pool
                .iter()
                .map(|server| {
                    let latency = selection.latency_by_server.get(&server.id).copied();
                    map_server(server, latency, None)
                })
                .collect::<Vec<_>>(),
        ),
        ping_ms: Some(selection.selected.average_ms),
        jitter_ms: if jitter_ms > 0.0 {
            Some(jitter_ms)
        } else {
            None
        },
        download: None,
        download_latency_ms: None,
        upload: None,
        upload_latency_ms: None,
        proxy: None,
        details: settings.details.then_some(RunDetails {
            interval_seconds: 1,
            selected_server_latency: SelectedServerLatencyDetails {
                average_ms: selection.selected.average_ms,
                variance_ms: selection.selected.variance_ms,
                stddev_ms: Some(selection.selected.variance_ms.max(0.0).sqrt()),
                samples_ms: (!selection.selected.samples_ms.is_empty())
                    .then(|| selection.selected.samples_ms.clone()),
            },
            download: None,
            upload: None,
        }),
    })
}

fn build_initial_sdk_artifacts(selection: &SelectionOutcome) -> SdkArtifacts {
    SdkArtifacts {
        selected_latency_samples_ms: (!selection.selected.samples_ms.is_empty())
            .then(|| selection.selected.samples_ms.clone()),
        ..SdkArtifacts::default()
    }
}

pub(crate) fn spawn_loaded_latency_task(
    client: &Client,
    server: &SpeedtestServer,
    stage_seconds: u64,
) -> (
    watch::Receiver<Option<f64>>,
    tokio::task::JoinHandle<Vec<f64>>,
) {
    let latency_server = server.clone();
    let latency_client = client.clone();
    let (rtt_tx, rtt_rx) = watch::channel(None);
    let task = tokio::spawn(async move {
        select::collect_loaded_latency_samples_with_progress(
            &latency_client,
            &latency_server,
            stage_seconds,
            |iqm| {
                let _ = rtt_tx.send(Some(iqm));
            },
        )
        .await
    });

    (rtt_rx, task)
}

pub(crate) fn average_latency_ms(samples: &[f64]) -> Option<f64> {
    (!samples.is_empty()).then(|| samples.iter().sum::<f64>() / samples.len() as f64)
}

pub(crate) fn build_benchmark_result(
    mbps: f64,
    bytes: u64,
    duration_seconds: u64,
    connections: usize,
    actual_duration_ms: u64,
    throughput: Option<&crate::speedtest::throughput::ThroughputResult>,
) -> BenchmarkResult {
    BenchmarkResult {
        mbps,
        bytes,
        duration_seconds,
        connections,
        actual_duration_seconds: Some(actual_duration_ms as f64 / 1_000.0),
        average_mbps: throughput.map(|it| it.average_mbps()),
        mst_mbps: throughput.map(|it| it.mst_mbps()),
    }
}

pub(crate) fn build_mst_speeds(
    throughput: Option<&crate::speedtest::throughput::ThroughputResult>,
) -> Option<MstSpeedsOut> {
    throughput.map(|t| MstSpeedsOut {
        average: t.average_bps * 8.0 / 1_000_000.0,
        mst_66_20: t.mst_66_20_bps * 8.0 / 1_000_000.0,
        mst_66_30: t.mst_66_30_bps * 8.0 / 1_000_000.0,
        mst_75_30: t.mst_75_30_bps * 8.0 / 1_000_000.0,
        blended: t.blended_bps * 8.0 / 1_000_000.0,
        superspeed: t.superspeed_bps * 8.0 / 1_000_000.0,
    })
}

pub(crate) fn build_mst_buckets(
    throughput: Option<&crate::speedtest::throughput::ThroughputResult>,
) -> Option<Vec<MstBucketOut>> {
    throughput.map(|t| {
        t.buckets_500ms
            .iter()
            .map(|b| MstBucketOut {
                start_ms: b.start_ms,
                stop_ms: b.stop_ms,
                bytes: b.bytes,
                bandwidth_mbps: b.bandwidth_bytes_per_sec() * 8.0 / 1_000_000.0,
            })
            .collect()
    })
}

pub(crate) fn build_remote_upload_intervals(
    samples: &[crate::speedtest::browser_protocol::UploadStatsSample],
    stage_seconds: u64,
) -> Vec<ThroughputInterval> {
    let mut intervals = Vec::new();

    for sample in samples {
        if sample.elapsed_ms == 0 {
            continue;
        }
        let elapsed_seconds = sample.elapsed_ms as f64 / 1_000.0;
        let mbps = (sample.bytes as f64 * 8.0) / 1_000_000.0 / elapsed_seconds.max(0.001);
        if !mbps.is_finite() || mbps < 0.0 {
            continue;
        }
        push_interval(
            &mut intervals,
            elapsed_seconds.min(stage_seconds as f64),
            sample.bytes,
            mbps,
        );
    }

    intervals
}

pub(crate) fn apply_download_stats_to_server_pool(
    server_pool: &mut Option<Vec<Server>>,
    per_server: &[download::PerServerDownloadStats],
) {
    if let Some(server_pool) = server_pool.as_mut() {
        for server in server_pool {
            if let Some(entry) = per_server.iter().find(|entry| entry.server_id == server.id) {
                server.download_avg_mbps = Some(entry.mbps);
                server.download_bytes = Some(entry.bytes);
            }
        }
    }
}

async fn select_server<F>(
    client: &Client,
    servers: &[SpeedtestServer],
    settings: &EngineSettings,
    on_event: &mut F,
) -> Result<SelectionOutcome>
where
    F: FnMut(EngineEvent),
{
    if let Some(server_id) = settings.server_id {
        let server = servers
            .iter()
            .find(|candidate| candidate.id == server_id)
            .cloned()
            .with_context(|| format!("server id {server_id} not found in provided server list"))?;
        let measurement =
            select::probe_server_latency(client, &server, settings.latency_samples).await?;
        on_event(EngineEvent::CandidateProbed {
            index: 1,
            total: 1,
            server_id,
            average_ms: Some(measurement.average_ms),
            variance_ms: Some(measurement.variance_ms),
            error: None,
        });
        return Ok(SelectionOutcome {
            selected: ServerLatency {
                server: server.clone(),
                average_ms: measurement.average_ms,
                variance_ms: measurement.variance_ms,
                samples_ms: measurement.samples_ms,
            },
            transfer_pool: vec![server.clone()],
            latency_by_server: HashMap::from([(
                server.id,
                (measurement.average_ms, measurement.variance_ms.sqrt()),
            )]),
        });
    }

    let ranked = select::probe_and_rank_candidates_with_progress(
        client,
        servers,
        settings.candidate_servers,
        settings.latency_samples,
        |index, total, server, outcome, error| {
            on_event(EngineEvent::CandidateProbed {
                index,
                total,
                server_id: server.id,
                average_ms: outcome.as_ref().map(|it| it.average_ms),
                variance_ms: outcome.as_ref().map(|it| it.variance_ms),
                error,
            });
        },
    )
    .await?;

    let mut rankings = ranked
        .iter()
        .map(|entry| LatencyRanking {
            server_id: entry.server.id,
            average_ms: entry.average_ms,
            variance_ms: entry.variance_ms,
            distance_km: entry.server.distance_km,
        })
        .collect::<Vec<_>>();
    sort_latency_rankings(&mut rankings);

    let selected = select::select_best_latency(&ranked)
        .context("no ranked speedtest candidates were produced")?;

    let transfer_pool = build_transfer_pool(&ranked, settings.modern_pool_size.max(1));
    let latency_by_server = ranked
        .iter()
        .map(|entry| {
            (
                entry.server.id,
                (entry.average_ms, entry.variance_ms.max(0.0).sqrt()),
            )
        })
        .collect::<HashMap<_, _>>();

    Ok(SelectionOutcome {
        selected,
        transfer_pool,
        latency_by_server,
    })
}

fn build_transfer_pool(ranked: &[ServerLatency], pool_size: usize) -> Vec<SpeedtestServer> {
    let limit = pool_size.max(1);

    if ranked.is_empty() {
        return Vec::new();
    }

    let best_latency = ranked[0].average_ms;
    let threshold_ms = (best_latency + 8.0).max(best_latency * 2.5);
    let mut filtered = ranked
        .iter()
        .filter(|entry| entry.average_ms <= threshold_ms)
        .take(limit)
        .map(|entry| entry.server.clone())
        .collect::<Vec<_>>();

    if filtered.is_empty() {
        filtered = ranked
            .iter()
            .take(limit)
            .map(|entry| entry.server.clone())
            .collect();
    }

    filtered
}

fn map_server(
    server: &SpeedtestServer,
    latency_stats: Option<(f64, f64)>,
    download_stats: Option<(u64, f64)>,
) -> Server {
    let (latency_ms, latency_stddev_ms) = latency_stats
        .map(|(average_ms, stddev_ms)| (Some(average_ms), Some(stddev_ms)))
        .unwrap_or((None, None));
    let (download_bytes, download_avg_mbps) = download_stats
        .map(|(bytes, mbps)| (Some(bytes), Some(mbps)))
        .unwrap_or((None, None));

    Server {
        id: server.id,
        sponsor: server.sponsor.clone(),
        name: server.name.clone(),
        country: server.country.clone(),
        host: server.host.clone(),
        distance_km: server.distance_km,
        latency_ms,
        latency_stddev_ms,
        download_avg_mbps,
        download_bytes,
        sdk_url: Some(server.url.clone()),
        sdk_lat: server.sdk_lat.clone(),
        sdk_lon: server.sdk_lon.clone(),
        sdk_cc: server.sdk_cc.clone(),
        sdk_preferred: server.sdk_preferred,
        sdk_isp_id: server.sdk_isp_id.clone(),
        sdk_https_functional: server.sdk_https_functional,
        sdk_hostname: server.sdk_hostname.clone(),
        sdk_port: server.sdk_port,
        sdk_force_ping_select: server.sdk_force_ping_select,
    }
}

pub(crate) fn push_interval(
    intervals: &mut Vec<ThroughputInterval>,
    elapsed_seconds: f64,
    bytes: u64,
    mbps: f64,
) {
    if let Some(last) = intervals.last()
        && (last.elapsed_seconds - elapsed_seconds).abs() < f64::EPSILON
    {
        if let Some(last_mut) = intervals.last_mut() {
            last_mut.bytes = bytes;
            last_mut.mbps = mbps;
        }
        return;
    }

    intervals.push(ThroughputInterval {
        elapsed_seconds,
        bytes,
        mbps,
    });
}

fn current_timestamp() -> Result<String> {
    Ok(Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true))
}

#[cfg(test)]
pub fn calculate_rtt(samples: &[f64]) -> Option<RttSummary> {
    let mut values = samples
        .iter()
        .copied()
        .filter(|sample| sample.is_finite() && *sample >= 0.0)
        .collect::<Vec<_>>();
    if values.is_empty() {
        return None;
    }

    values.sort_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal));
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
            let left = values[middle - 1];
            let right = values[middle];
            (left + right) / 2.0
        }
    };

    Some(RttSummary {
        iqm: calculate_iqm(&values),
        mean,
        median,
        min,
        max,
    })
}

#[cfg(test)]
pub fn calculate_iqm(samples: &[f64]) -> f64 {
    match samples.len() {
        0 => 0.0,
        1 => samples[0],
        2 => (samples[0] + samples[1]) / 2.0,
        len => {
            let lower = len as f64 / 4.0;
            let upper = 3.0 * len as f64 / 4.0;
            let start = lower.ceil() as usize;
            let end = upper.floor() as usize;
            let fraction = upper - upper.floor();
            let core_sum = if start < end {
                samples[start..end].iter().sum::<f64>()
            } else {
                0.0
            };
            let edge_sum = if start > 0 && end < len {
                samples[start - 1] + samples[end]
            } else {
                0.0
            };
            (fraction * edge_sum + core_sum) / (len as f64 / 2.0)
        }
    }
}

pub fn calculate_jitter(samples: &[f64]) -> f64 {
    if samples.len() < 2 {
        return 0.0;
    }

    let mut total_delta = 0.0;
    for index in 1..samples.len() {
        total_delta += (samples[index] - samples[index - 1]).abs();
    }

    let jitter = total_delta / (samples.len() - 1) as f64;
    (jitter * 1_000.0).round() / 1_000.0
}

#[cfg(test)]
pub fn calculate_result_hash(ping: f64, upload: Option<u64>, download: Option<u64>) -> String {
    let ping = if ping.is_finite() { ping } else { 0.0 };
    let upload = upload.unwrap_or(0);
    let download = download.unwrap_or(0);
    let hash_input = format!("{ping}-{upload}-{download}-{RESULT_HASH_SALT}");
    format!("{:x}", md5_compute(hash_input))
}

pub fn sort_latency_rankings(rankings: &mut [LatencyRanking]) {
    rankings.sort_by(|left, right| {
        left.average_ms
            .partial_cmp(&right.average_ms)
            .unwrap_or(Ordering::Equal)
            .then_with(|| {
                left.variance_ms
                    .partial_cmp(&right.variance_ms)
                    .unwrap_or(Ordering::Equal)
            })
            .then_with(|| {
                left.distance_km
                    .partial_cmp(&right.distance_km)
                    .unwrap_or(Ordering::Equal)
            })
    });
}

#[cfg(test)]
mod tests {
    use super::{
        EngineSettings, EngineStage, LatencyRanking, StageMachine, build_transfer_pool,
        calculate_iqm, calculate_jitter, calculate_result_hash, calculate_rtt,
        sort_latency_rankings,
    };
    use crate::speedtest::select::ServerLatency;
    use crate::speedtest::servers::SpeedtestServer;

    #[test]
    fn calculates_iqm_for_sorted_samples() {
        let samples = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
        let iqm = calculate_iqm(&samples);
        assert!((iqm - 4.5).abs() < 1e-9);
    }

    #[test]
    fn calculates_jitter_from_adjacent_deltas() {
        let samples = vec![10.0, 11.0, 13.0, 12.0];
        let jitter = calculate_jitter(&samples);
        assert!((jitter - 1.333).abs() < 1e-9);
    }

    #[test]
    fn calculates_rtt_summary() {
        let samples = vec![15.0, 16.0, 17.0, 18.0];
        let summary = calculate_rtt(&samples).expect("summary should exist");
        assert!((summary.iqm - 16.5).abs() < 1e-9);
        assert!((summary.mean - 16.5).abs() < 1e-9);
        assert!((summary.min - 15.0).abs() < 1e-9);
        assert!((summary.max - 18.0).abs() < 1e-9);
        assert!((summary.median - 16.5).abs() < 1e-9);
    }

    #[test]
    fn sorts_rankings_by_latency_variance_distance() {
        let mut rankings = vec![
            LatencyRanking {
                server_id: 1,
                average_ms: 20.0,
                variance_ms: 4.0,
                distance_km: 10.0,
            },
            LatencyRanking {
                server_id: 2,
                average_ms: 20.0,
                variance_ms: 2.0,
                distance_km: 20.0,
            },
            LatencyRanking {
                server_id: 3,
                average_ms: 20.0,
                variance_ms: 2.0,
                distance_km: 5.0,
            },
            LatencyRanking {
                server_id: 4,
                average_ms: 15.0,
                variance_ms: 8.0,
                distance_km: 50.0,
            },
        ];

        sort_latency_rankings(&mut rankings);
        assert_eq!(rankings[0].server_id, 4);
        assert_eq!(rankings[1].server_id, 3);
        assert_eq!(rankings[2].server_id, 2);
        assert_eq!(rankings[3].server_id, 1);
    }

    #[test]
    fn stage_machine_advances_in_expected_order() {
        let settings = EngineSettings::default();
        let mut machine = StageMachine::new(settings.stage_order());

        assert_eq!(machine.next(), Some(EngineStage::Latency));
        assert_eq!(machine.next(), Some(EngineStage::Download));
        assert_eq!(machine.next(), Some(EngineStage::Upload));
        assert_eq!(machine.next(), Some(EngineStage::Save));
        assert_eq!(machine.next(), None);
    }

    #[test]
    fn result_hash_matches_known_vector() {
        let hash = calculate_result_hash(2.940037, Some(791600), Some(1777296));
        assert_eq!(hash, "80dd6bc7a9d55bd2d3d85acd0d863742");
    }

    #[test]
    fn transfer_pool_filters_high_latency_outliers() {
        let ranked = vec![
            ServerLatency {
                server: make_server(1),
                average_ms: 3.0,
                variance_ms: 0.1,
                samples_ms: vec![3.0],
            },
            ServerLatency {
                server: make_server(2),
                average_ms: 10.0,
                variance_ms: 0.2,
                samples_ms: vec![10.0],
            },
            ServerLatency {
                server: make_server(3),
                average_ms: 21.0,
                variance_ms: 0.3,
                samples_ms: vec![21.0],
            },
            ServerLatency {
                server: make_server(4),
                average_ms: 24.0,
                variance_ms: 0.4,
                samples_ms: vec![24.0],
            },
        ];

        let pool = build_transfer_pool(&ranked, 4);
        let ids = pool.iter().map(|server| server.id).collect::<Vec<_>>();

        assert_eq!(ids, vec![1, 2]);
    }

    fn make_server(id: u64) -> SpeedtestServer {
        SpeedtestServer {
            id,
            sponsor: "s".to_string(),
            name: "n".to_string(),
            country: "c".to_string(),
            host: format!("host{id}.example:8080"),
            distance_km: 1.0,
            url: format!("http://host{id}.example:8080/speedtest/upload.php"),
            session_guid: Some("guid".to_string()),
            sdk_lat: None,
            sdk_lon: None,
            sdk_cc: None,
            sdk_preferred: None,
            sdk_isp_id: None,
            sdk_https_functional: Some(1),
            sdk_hostname: None,
            sdk_port: Some(8080),
            sdk_force_ping_select: None,
        }
    }
}
