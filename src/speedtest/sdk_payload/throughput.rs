use anyhow::Result;
use serde::Serialize;

use crate::model::{BenchmarkResult, DirectionDetails, RunResult, SdkArtifacts, ThroughputInterval};

#[derive(Debug, Serialize)]
pub(super) struct SdkDirectionSpeeds {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) local: Option<SdkCombinedSpeed>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) remote: Option<SdkCombinedSpeed>,
}

#[derive(Debug, Serialize)]
pub(super) struct SdkCombinedSpeed {
    pub(super) combined: u64,
    pub(super) average: f64,
    #[serde(rename = "mst_66_20")]
    pub(super) mst_66_20: f64,
    #[serde(rename = "mst_66_30")]
    pub(super) mst_66_30: f64,
    #[serde(rename = "mst_75_30")]
    pub(super) mst_75_30: f64,
    pub(super) superspeed: f64,
}

#[derive(Debug, Serialize)]
pub(super) struct SdkThroughputSample {
    pub(super) elapsed: f64,
    pub(super) bytes: u64,
    pub(super) mbps: f64,
    pub(super) bps: u64,
}

pub(super) fn remote_upload_bps(result: &RunResult, sdk_artifacts: &SdkArtifacts) -> Option<u64> {
    if let Some(intervals) = sdk_artifacts.upload_remote_intervals.as_ref()
        && let Some(last) = intervals.last()
        && last.mbps.is_finite()
        && last.mbps >= 0.0
    {
        return Some((last.mbps * 1_000_000.0).round() as u64);
    }

    let remote_mbps = result
        .details
        .as_ref()?
        .upload
        .as_ref()?
        .remote_intervals
        .as_ref()?
        .last()?
        .mbps;

    if !remote_mbps.is_finite() || remote_mbps < 0.0 {
        return None;
    }

    Some((remote_mbps * 1_000_000.0).round() as u64)
}

pub(super) fn local_upload_bps(result: &RunResult, sdk_artifacts: &SdkArtifacts) -> Option<u64> {
    if let Some(intervals) = sdk_artifacts.upload_intervals.as_ref()
        && let Some(last) = intervals.last()
        && last.mbps.is_finite()
        && last.mbps >= 0.0
    {
        return Some((last.mbps * 1_000_000.0).round() as u64);
    }

    let local_mbps = result
        .details
        .as_ref()?
        .upload
        .as_ref()?
        .intervals
        .last()?
        .mbps;

    if !local_mbps.is_finite() || local_mbps < 0.0 {
        return None;
    }

    Some((local_mbps * 1_000_000.0).round() as u64)
}

pub(super) fn build_direction_samples(
    sdk_intervals: Option<&[ThroughputInterval]>,
    details: Option<&DirectionDetails>,
    fallback: Option<&BenchmarkResult>,
) -> Result<Option<Vec<SdkThroughputSample>>> {
    if let Some(sdk_samples) = sdk_intervals.and_then(direction_samples_from_intervals)
        && !sdk_samples.is_empty()
    {
        return Ok(Some(sdk_samples));
    }

    if let Some(details) = details {
        let samples = direction_samples_from_intervals(&details.intervals).unwrap_or_default();
        if !samples.is_empty() {
            return Ok(Some(samples));
        }
    }

    let Some(fallback) = fallback else {
        return Ok(None);
    };

    let bps = super::util::mbps_to_bps(fallback)?;
    Ok(Some(vec![SdkThroughputSample {
        elapsed: fallback.duration_seconds as f64,
        bytes: fallback.bytes,
        mbps: fallback.mbps,
        bps,
    }]))
}

pub(super) fn direction_samples_from_intervals(
    intervals: &[ThroughputInterval],
) -> Option<Vec<SdkThroughputSample>> {
    let samples = intervals
        .iter()
        .filter_map(interval_to_sdk_sample)
        .collect::<Vec<_>>();
    if samples.is_empty() {
        None
    } else {
        Some(samples)
    }
}

fn interval_to_sdk_sample(interval: &ThroughputInterval) -> Option<SdkThroughputSample> {
    if !interval.elapsed_seconds.is_finite()
        || interval.elapsed_seconds < 0.0
        || !interval.mbps.is_finite()
        || interval.mbps < 0.0
    {
        return None;
    }

    Some(SdkThroughputSample {
        elapsed: interval.elapsed_seconds,
        bytes: interval.bytes,
        mbps: interval.mbps,
        bps: (interval.mbps * 1_000_000.0).round() as u64,
    })
}

pub(super) fn build_speed_profile(
    combined_bps: Option<u64>,
    samples: Option<&[SdkThroughputSample]>,
    duration_seconds: Option<u64>,
) -> Option<SdkCombinedSpeed> {
    let combined = combined_bps?;
    let baseline = combined as f64;
    let transfer_samples = samples.map(to_transfer_samples).unwrap_or_default();
    if transfer_samples.is_empty() {
        return Some(SdkCombinedSpeed {
            combined,
            average: baseline,
            mst_66_20: baseline,
            mst_66_30: baseline,
            mst_75_30: baseline,
            superspeed: baseline,
        });
    }

    let duration_millis = duration_seconds
        .map(|seconds| seconds as f64 * 1_000.0)
        .unwrap_or_else(|| {
            transfer_samples
                .last()
                .map(|sample| sample.received_reply_at_ms)
                .unwrap_or(0.0)
                .max(1_000.0)
        });

    let average = calculate_average_speed_bps(&transfer_samples).unwrap_or(baseline);
    let mst_66_20 =
        calculate_mst_speed_bps(&transfer_samples, 2.0 / 3.0, 750.0, 600.0).unwrap_or(average);
    let mst_66_30 =
        calculate_mst_speed_bps(&transfer_samples, 2.0 / 3.0, 500.0, 500.0).unwrap_or(average);
    let mst_75_30 =
        calculate_mst_speed_bps(&transfer_samples, 0.75, 500.0, 500.0).unwrap_or(average);
    let superspeed = calculate_superspeed_bps(&transfer_samples, duration_millis, 500.0, 500.0)
        .unwrap_or(average);

    Some(SdkCombinedSpeed {
        combined,
        average,
        mst_66_20,
        mst_66_30,
        mst_75_30,
        superspeed,
    })
}

fn to_transfer_samples(samples: &[SdkThroughputSample]) -> Vec<SdkTransferSample> {
    let mut transfers = Vec::new();
    let mut previous_elapsed_ms = 0.0;
    let mut previous_bytes = 0_u64;

    for sample in samples {
        let elapsed_ms = sample.elapsed * 1_000.0;
        if !elapsed_ms.is_finite() || elapsed_ms <= previous_elapsed_ms {
            continue;
        }

        let bytes_delta = sample.bytes.saturating_sub(previous_bytes);
        transfers.push(SdkTransferSample {
            elapsed_ms,
            size: bytes_delta,
            sent_at_ms: previous_elapsed_ms,
            received_reply_at_ms: elapsed_ms,
        });

        previous_elapsed_ms = elapsed_ms;
        previous_bytes = sample.bytes;
    }

    transfers
}

fn calculate_average_speed_bps(samples: &[SdkTransferSample]) -> Option<f64> {
    let total_bytes = samples.iter().map(|sample| sample.size).sum::<u64>() as f64;
    let elapsed_ms = samples
        .last()
        .map(|sample| sample.elapsed_ms)
        .filter(|value| value.is_finite() && *value > 0.0)?;
    Some(total_bytes * 8.0 * 1_000.0 / elapsed_ms)
}

fn calculate_mst_speed_bps(
    samples: &[SdkTransferSample],
    kept_samples_percentage: f64,
    sample_length_ms: f64,
    minimum_sample_length_ms: f64,
) -> Option<f64> {
    let calculator =
        BucketedThroughputCalculator::new(sample_length_ms, minimum_sample_length_ms, None);
    let buckets = calculator.build(samples);
    let mut completed = buckets
        .buckets
        .iter()
        .filter_map(|bucket| (*bucket).to_bandwidth_sample())
        .collect::<Vec<_>>();

    let completed_count = completed.len();
    if completed_count < 4 {
        return None;
    }

    completed.sort_by(|left, right| {
        left.bandwidth_bps
            .partial_cmp(&right.bandwidth_bps)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    if completed.len() <= 2 {
        return None;
    }
    let mut trimmed = completed[1..completed.len() - 1].to_vec();
    if trimmed.is_empty() {
        return None;
    }

    let keep_ratio = kept_samples_percentage.clamp(0.0, 1.0);
    let drop_count = (((completed_count - 2) as f64) * (1.0 - keep_ratio)).floor() as usize;
    if drop_count >= trimmed.len() {
        trimmed = vec![trimmed[trimmed.len() - 1]];
    } else if drop_count > 0 {
        trimmed = trimmed.split_off(drop_count);
    }

    let aggregate = trimmed.iter().fold(
        AggregateBandwidthSample {
            duration_ms: 0.0,
            bytes_transferred: 0.0,
        },
        |acc, sample| AggregateBandwidthSample {
            duration_ms: acc.duration_ms + sample.duration_ms,
            bytes_transferred: acc.bytes_transferred + sample.bytes_transferred,
        },
    );

    if aggregate.duration_ms <= 0.0 {
        None
    } else {
        Some(aggregate.bytes_transferred * 8.0 * 1_000.0 / aggregate.duration_ms)
    }
}

fn calculate_superspeed_bps(
    samples: &[SdkTransferSample],
    test_duration_ms: f64,
    sample_length_ms: f64,
    minimum_sample_length_ms: f64,
) -> Option<f64> {
    if !test_duration_ms.is_finite() || test_duration_ms <= 0.0 {
        return None;
    }

    let calculator = BucketedThroughputCalculator::new(
        sample_length_ms,
        minimum_sample_length_ms,
        Some(test_duration_ms),
    );
    let buckets = calculator.build(samples);
    let first_good_index = buckets.first_good_end_sample_index?;
    let completed = buckets
        .buckets
        .iter()
        .filter(|bucket| bucket.stop_time_ms.is_some())
        .collect::<Vec<_>>();

    if first_good_index >= completed.len() {
        return None;
    }

    let minimum_window_ms = test_duration_ms / 2.0;
    let mut best_bps = 0.0;

    for start_index in 0..=first_good_index {
        for end_index in first_good_index..completed.len() {
            let start = completed[start_index];
            let end = completed[end_index];
            let Some(end_stop_ms) = end.stop_time_ms else {
                continue;
            };

            let window_ms = end_stop_ms - start.start_time_ms;
            if window_ms < minimum_window_ms || window_ms <= 0.0 {
                continue;
            }

            let bytes = end.total_bytes_transferred as f64
                - (start.total_bytes_transferred as f64 - start.bytes_transferred as f64);
            let speed_bps = bytes * 8.0 * 1_000.0 / window_ms;
            if speed_bps > best_bps {
                best_bps = speed_bps;
            }
        }
    }

    if best_bps > 0.0 { Some(best_bps) } else { None }
}

#[derive(Debug, Clone, Copy)]
struct SdkTransferSample {
    elapsed_ms: f64,
    size: u64,
    sent_at_ms: f64,
    received_reply_at_ms: f64,
}

#[derive(Debug, Clone, Copy)]
struct SdkThroughputBucket {
    bytes_transferred: u64,
    start_time_ms: f64,
    stop_time_ms: Option<f64>,
    total_bytes_transferred: u64,
}

impl SdkThroughputBucket {
    fn duration_ms(self) -> Option<f64> {
        let stop = self.stop_time_ms?;
        let duration = stop - self.start_time_ms;
        (duration.is_finite() && duration > 0.0).then_some(duration)
    }

    fn bandwidth_bps(self) -> Option<f64> {
        let duration_ms = self.duration_ms()?;
        Some(self.bytes_transferred as f64 * 8.0 * 1_000.0 / duration_ms)
    }

    fn to_bandwidth_sample(self) -> Option<BandwidthSample> {
        Some(BandwidthSample {
            bandwidth_bps: self.bandwidth_bps()?,
            duration_ms: self.duration_ms()?,
            bytes_transferred: self.bytes_transferred as f64,
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct BandwidthSample {
    bandwidth_bps: f64,
    duration_ms: f64,
    bytes_transferred: f64,
}

#[derive(Debug, Clone, Copy)]
struct AggregateBandwidthSample {
    duration_ms: f64,
    bytes_transferred: f64,
}

#[derive(Debug, Clone, Copy)]
struct FragmentedTransfer {
    target_bucket: usize,
    size: u64,
}

#[derive(Debug)]
struct BucketedThroughput {
    buckets: Vec<SdkThroughputBucket>,
    first_good_end_sample_index: Option<usize>,
}

#[derive(Debug, Clone, Copy)]
struct BucketedThroughputCalculator {
    sample_length_ms: f64,
    minimum_sample_length_ms: f64,
    test_duration_ms: Option<f64>,
}

impl BucketedThroughputCalculator {
    fn new(
        sample_length_ms: f64,
        minimum_sample_length_ms: f64,
        test_duration_ms: Option<f64>,
    ) -> Self {
        Self {
            sample_length_ms,
            minimum_sample_length_ms,
            test_duration_ms,
        }
    }

    fn build(self, samples: &[SdkTransferSample]) -> BucketedThroughput {
        let mut buckets = Vec::new();
        let mut first_good_end_sample_index = None;
        self.create_bucket(0.0, &mut buckets, &mut first_good_end_sample_index);

        let max_bucket = (15_000.0 / self.minimum_sample_length_ms).ceil() as usize;
        for sample in samples {
            if !sample.elapsed_ms.is_finite() || sample.elapsed_ms < 0.0 {
                continue;
            }

            let target_bucket = self
                .calculate_target_bucket(sample.elapsed_ms, &buckets)
                .min(max_bucket);

            while buckets.len() <= target_bucket {
                self.create_bucket(
                    sample.elapsed_ms,
                    &mut buckets,
                    &mut first_good_end_sample_index,
                );
            }

            let fragments = self.fragment_sample(sample, target_bucket, &buckets);
            for fragment in fragments {
                if fragment.target_bucket >= buckets.len() {
                    continue;
                }
                buckets[fragment.target_bucket].bytes_transferred = buckets[fragment.target_bucket]
                    .bytes_transferred
                    .saturating_add(fragment.size);

                for bucket in &mut buckets[fragment.target_bucket..] {
                    bucket.total_bytes_transferred =
                        bucket.total_bytes_transferred.saturating_add(fragment.size);
                }
            }
        }

        BucketedThroughput {
            buckets,
            first_good_end_sample_index,
        }
    }

    fn calculate_target_bucket(self, elapsed_ms: f64, buckets: &[SdkThroughputBucket]) -> usize {
        if (self.sample_length_ms - self.minimum_sample_length_ms).abs() < f64::EPSILON {
            return (elapsed_ms / self.sample_length_ms).floor() as usize;
        }

        let bucket_count = buckets.len();
        let expected_count = (elapsed_ms / self.sample_length_ms).floor() as usize + 1;
        let min_ready_at = buckets
            .last()
            .map(|bucket| bucket.start_time_ms + self.minimum_sample_length_ms)
            .unwrap_or(self.minimum_sample_length_ms);

        if bucket_count < expected_count && elapsed_ms >= min_ready_at {
            expected_count.saturating_sub(1)
        } else {
            bucket_count.saturating_sub(1)
        }
    }

    fn fragment_sample(
        self,
        sample: &SdkTransferSample,
        mut target_bucket: usize,
        buckets: &[SdkThroughputBucket],
    ) -> Vec<FragmentedTransfer> {
        if sample.size == 0 {
            return Vec::new();
        }

        let mut fragments = Vec::new();
        let mut remaining = sample.size;
        let mut received_reply_at_ms = sample.received_reply_at_ms;
        let sent_at_ms = sample.sent_at_ms;

        while remaining > 0
            && target_bucket > 0
            && sent_at_ms < buckets[target_bucket].start_time_ms
        {
            let overlap = received_reply_at_ms - buckets[target_bucket].start_time_ms;
            if overlap > 0.0 {
                let total = received_reply_at_ms - sent_at_ms;
                if total > 0.0 {
                    let ratio = overlap / total;
                    let fragment_size = ((remaining as f64 * ratio).round() as u64).min(remaining);
                    if fragment_size > 0 {
                        fragments.push(FragmentedTransfer {
                            target_bucket,
                            size: fragment_size,
                        });
                        remaining -= fragment_size;
                    }
                }
            }

            target_bucket -= 1;
            if let Some(stop_time_ms) = buckets[target_bucket].stop_time_ms {
                received_reply_at_ms = stop_time_ms;
            } else {
                break;
            }
        }

        if remaining > 0 {
            fragments.push(FragmentedTransfer {
                target_bucket,
                size: remaining,
            });
        }

        fragments
    }

    fn create_bucket(
        self,
        elapsed_ms: f64,
        buckets: &mut Vec<SdkThroughputBucket>,
        first_good_end_sample_index: &mut Option<usize>,
    ) {
        let mut start_time_ms = elapsed_ms;
        let mut total_bytes_transferred = 0_u64;

        if let Some(last_bucket) = buckets.last_mut() {
            let stop_time_ms = (last_bucket.start_time_ms + self.sample_length_ms).min(elapsed_ms);
            last_bucket.stop_time_ms = Some(stop_time_ms);
            start_time_ms = stop_time_ms;
            total_bytes_transferred = last_bucket.total_bytes_transferred;
        }

        buckets.push(SdkThroughputBucket {
            bytes_transferred: 0,
            start_time_ms,
            stop_time_ms: None,
            total_bytes_transferred,
        });

        if first_good_end_sample_index.is_none()
            && let Some(test_duration_ms) = self.test_duration_ms
            && start_time_ms > (test_duration_ms / 2.0)
        {
            *first_good_end_sample_index = buckets.len().checked_sub(1);
        }
    }
}
