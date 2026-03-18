use anyhow::{Context, Result};
use serde_json::Value;

use crate::model::DirectionDetails;
use crate::speedtest::download::{self, DownloadProgress};
use crate::speedtest::sdk_payload;
use crate::speedtest::throughput::TransferConfig;
use crate::speedtest::select::LatencyMeasurement;
use crate::speedtest::upload::{self, UploadProgress};
use crate::util::clamp_worker_count;

use super::engine::{
    EngineEvent, EngineInputs, EngineOutcome, EngineStage, EngineState, SelectionOutcome,
    apply_download_stats_to_server_pool, average_latency_ms,
    build_benchmark_result, build_mst_buckets, build_mst_speeds, build_remote_upload_intervals,
    push_interval, spawn_loaded_latency_task,
};

pub(crate) fn finalize_engine_outcome<F>(
    state: EngineState,
    selection: SelectionOutcome,
    on_event: &mut F,
) -> Result<EngineOutcome>
where
    F: FnMut(EngineEvent),
{
    let guid = selection
        .selected
        .server
        .session_guid
        .clone()
        .unwrap_or_else(sdk_payload::generate_sdk_guid);
    let payload = sdk_payload::build_sdk_result_payload(&state.result, &state.sdk_artifacts, &guid)
        .context("failed building SDK payload during save stage")?;

    let hash = payload
        .get("hash")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();

    on_event(EngineEvent::SavePayloadBuilt { guid, hash });
    on_event(EngineEvent::StageFinished(EngineStage::Save));
    on_event(EngineEvent::StageStarting(EngineStage::Finished));
    on_event(EngineEvent::StageFinished(EngineStage::Finished));

    Ok(EngineOutcome {
        result: state.result,
        sdk_artifacts: state.sdk_artifacts,
        sdk_payload: payload,
        selected_server: selection.selected.server,
        selected_latency: LatencyMeasurement {
            average_ms: selection.selected.average_ms,
            variance_ms: selection.selected.variance_ms,
            samples_ms: selection.selected.samples_ms,
        },
        transfer_pool: selection.transfer_pool,
    })
}

pub(crate) async fn run_download_stage<F>(
    inputs: &EngineInputs<'_>,
    state: &mut EngineState,
    on_event: &mut F,
) -> Result<()>
where
    F: FnMut(EngineEvent),
{
    let mut intervals = Vec::new();
    let (download_rtt_rx, latency_task) = spawn_loaded_latency_task(
        inputs.client,
        &inputs.selection.selected.server,
        inputs.settings.download_seconds,
    );

    let download_config = TransferConfig {
        connections: clamp_worker_count(inputs.settings.download_connections),
        initial_connections: 4,
        max_seconds: inputs.settings.download_seconds,
        progress_interval: inputs.settings.progress_interval,
        request_target_ms: 5_000,
        start_request_size: 25_000_000,
        min_request_size: 25_000_000,
        max_request_size: 250_000_000,
    };
    let stats = download::run_download_test(
        inputs.client,
        &inputs.selection.selected.server,
        inputs.mode,
        &inputs.selection.transfer_pool,
        &download_config,
        |snapshot: DownloadProgress| {
            on_event(EngineEvent::StageProgress {
                stage: EngineStage::Download,
                elapsed: snapshot.elapsed,
                mbps: snapshot.mbps,
                bytes: snapshot.bytes,
                active_connections: snapshot.active_connections,
                rtt_ms: *download_rtt_rx.borrow(),
            });
            push_interval(
                &mut intervals,
                snapshot
                    .elapsed
                    .as_secs_f64()
                    .min(inputs.settings.download_seconds as f64),
                snapshot.bytes,
                snapshot.mbps,
            );
        },
    )
    .await?;

    let download_latency_samples = latency_task.await.unwrap_or_default();
    state.result.download_latency_ms = average_latency_ms(&download_latency_samples);
    state.sdk_artifacts.download_latency_samples_ms =
        (!download_latency_samples.is_empty()).then_some(download_latency_samples);

    push_interval(
        &mut intervals,
        inputs.settings.download_seconds as f64,
        stats.bytes,
        stats.mbps,
    );

    state.result.download = Some(build_benchmark_result(
        stats.mbps,
        stats.bytes,
        inputs.settings.download_seconds,
        download_config.connections,
        stats.actual_duration_ms,
        stats.throughput.as_ref(),
    ));
    state.sdk_artifacts.download_intervals = (!intervals.is_empty()).then_some(intervals.clone());

    if let Some(details) = state.result.details.as_mut() {
        details.download = Some(DirectionDetails {
            request_attempts: stats.request_attempts,
            request_successes: stats.request_successes,
            request_http_errors: stats.request_http_errors,
            request_transport_errors: stats.request_transport_errors,
            response_read_errors: stats.response_read_errors,
            intervals,
            remote_intervals: None,
            mst_speeds: build_mst_speeds(stats.throughput.as_ref()),
            mst_buckets: build_mst_buckets(stats.throughput.as_ref()),
        });
    }

    apply_download_stats_to_server_pool(&mut state.result.server_pool, &stats.per_server);

    on_event(EngineEvent::StageResult {
        stage: EngineStage::Download,
        mbps: stats.mbps,
        bytes: stats.bytes,
    });

    Ok(())
}

pub(crate) async fn run_upload_stage<F>(
    inputs: &EngineInputs<'_>,
    state: &mut EngineState,
    on_event: &mut F,
) -> Result<()>
where
    F: FnMut(EngineEvent),
{
    let mut intervals = Vec::new();
    let (upload_rtt_rx, latency_task) = spawn_loaded_latency_task(
        inputs.client,
        &inputs.selection.selected.server,
        inputs.settings.upload_seconds,
    );

    let upload_config = TransferConfig {
        connections: clamp_worker_count(inputs.settings.upload_connections),
        initial_connections: clamp_worker_count(inputs.settings.upload_connections),
        max_seconds: inputs.settings.upload_seconds,
        progress_interval: inputs.settings.progress_interval,
        request_target_ms: 1_000,
        start_request_size: 1_048_576,
        min_request_size: 32 * 1024,
        max_request_size: 25 * 1024 * 1024,
    };
    let stats = upload::run_upload_test(
        inputs.client,
        &inputs.selection.selected.server,
        inputs.mode,
        &inputs.selection.transfer_pool,
        &upload_config,
        |snapshot: UploadProgress| {
            on_event(EngineEvent::StageProgress {
                stage: EngineStage::Upload,
                elapsed: snapshot.elapsed,
                mbps: snapshot.mbps,
                bytes: snapshot.bytes,
                active_connections: snapshot.active_connections,
                rtt_ms: *upload_rtt_rx.borrow(),
            });
            push_interval(
                &mut intervals,
                snapshot
                    .elapsed
                    .as_secs_f64()
                    .min(inputs.settings.upload_seconds as f64),
                snapshot.bytes,
                snapshot.mbps,
            );
        },
    )
    .await?;

    let upload_latency_samples = latency_task.await.unwrap_or_default();
    state.result.upload_latency_ms = average_latency_ms(&upload_latency_samples);
    state.sdk_artifacts.upload_latency_samples_ms =
        (!upload_latency_samples.is_empty()).then_some(upload_latency_samples);

    push_interval(
        &mut intervals,
        inputs.settings.upload_seconds as f64,
        stats.bytes,
        stats.mbps,
    );

    let remote_intervals =
        build_remote_upload_intervals(&stats.remote_samples, inputs.settings.upload_seconds);

    state.result.upload = Some(build_benchmark_result(
        stats.mbps,
        stats.bytes,
        inputs.settings.upload_seconds,
        upload_config.connections,
        stats.actual_duration_ms,
        stats.throughput.as_ref(),
    ));
    state.sdk_artifacts.upload_intervals = (!intervals.is_empty()).then_some(intervals.clone());
    state.sdk_artifacts.upload_remote_intervals =
        (!remote_intervals.is_empty()).then_some(remote_intervals.clone());

    if let Some(details) = state.result.details.as_mut() {
        details.upload = Some(DirectionDetails {
            request_attempts: stats.request_attempts,
            request_successes: stats.request_successes,
            request_http_errors: stats.request_http_errors,
            request_transport_errors: stats.request_transport_errors,
            response_read_errors: stats.response_read_errors,
            intervals,
            remote_intervals: (!remote_intervals.is_empty()).then_some(remote_intervals),
            mst_speeds: build_mst_speeds(stats.throughput.as_ref()),
            mst_buckets: build_mst_buckets(stats.throughput.as_ref()),
        });
    }

    on_event(EngineEvent::StageResult {
        stage: EngineStage::Upload,
        mbps: stats.mbps,
        bytes: stats.bytes,
    });

    Ok(())
}
