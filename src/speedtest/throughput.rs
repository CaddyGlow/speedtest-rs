//! Moving Sample Trimmed (MST) throughput calculator.
//!
//! Implements the speedtest.net JS SDK's measurement approach:
//! time-bucketed sampling with dual 500ms/750ms streams, warmup trimming,
//! outlier removal, blended output, superspeed windows, and CV-based early exit.
//!
//! All `*_bps` values throughout this module are in **bytes per second**.

use std::time::Duration;

/// Configuration for a transfer test stage.
#[derive(Debug, Clone)]
pub struct TransferConfig {
    pub connections: usize,
    pub initial_connections: usize,
    pub max_seconds: u64,
    pub progress_interval: Option<Duration>,
    pub request_target_ms: u64,
    pub start_request_size: usize,
    pub min_request_size: usize,
    pub max_request_size: usize,
}

impl TransferConfig {
    pub fn initial_connections(&self) -> usize {
        self.initial_connections.clamp(1, self.connections.max(1))
    }
}

/// A time bucket accumulating transferred bytes over a sampling interval.
#[derive(Debug, Clone)]
pub struct Bucket {
    pub start_ms: u64,
    pub stop_ms: u64,
    pub bytes: u64,
    pub total_bytes: u64,
}

impl Bucket {
    /// Bandwidth in bytes per second for this bucket.
    pub fn bandwidth_bytes_per_sec(&self) -> f64 {
        let duration_ms = self.stop_ms.saturating_sub(self.start_ms);
        if duration_ms == 0 {
            return 0.0;
        }
        self.bytes as f64 / (duration_ms as f64 / 1_000.0)
    }
}

/// Comprehensive throughput result from all MST algorithms.
#[derive(Debug, Clone)]
pub struct ThroughputResult {
    pub average_bps: f64,
    pub mst_66_30_bps: f64,
    pub mst_66_20_bps: f64,
    pub mst_75_30_bps: f64,
    pub blended_bps: f64,
    pub superspeed_bps: f64,
    pub buckets_500ms: Vec<Bucket>,
    pub total_bytes: u64,
    pub elapsed_ms: u64,
}

impl ThroughputResult {
    /// Blended speed in megabits per second.
    pub fn blended_mbps(&self) -> f64 {
        self.blended_bps * 8.0 / 1_000_000.0
    }

    /// Simple average speed in megabits per second.
    pub fn average_mbps(&self) -> f64 {
        self.average_bps * 8.0 / 1_000_000.0
    }

    /// Primary MST (66%/500ms) speed in megabits per second.
    pub fn mst_mbps(&self) -> f64 {
        self.mst_66_30_bps * 8.0 / 1_000_000.0
    }
}

/// MST algorithm parameters.
#[derive(Debug, Clone, Copy)]
struct MstAlgoConfig {
    keep_ratio: f64,
    warmup_buckets: usize,
}

/// Accumulates transfer samples into fixed-interval time buckets.
#[derive(Debug)]
struct BucketStream {
    sample_length_ms: u64,
    buckets: Vec<Bucket>,
    current_start_ms: u64,
    bytes_at_current_start: u64,
    started: bool,
}

impl BucketStream {
    fn new(sample_length_ms: u64) -> Self {
        Self {
            sample_length_ms,
            buckets: Vec::new(),
            current_start_ms: 0,
            bytes_at_current_start: 0,
            started: false,
        }
    }

    fn record(&mut self, elapsed_ms: u64, cumulative_bytes: u64) {
        if !self.started {
            self.started = true;
            self.current_start_ms = elapsed_ms;
            self.bytes_at_current_start = cumulative_bytes;
            return;
        }

        while elapsed_ms >= self.current_start_ms + self.sample_length_ms {
            let bucket_end = self.current_start_ms + self.sample_length_ms;
            let bytes = cumulative_bytes.saturating_sub(self.bytes_at_current_start);
            self.buckets.push(Bucket {
                start_ms: self.current_start_ms,
                stop_ms: bucket_end,
                bytes,
                total_bytes: cumulative_bytes,
            });
            self.current_start_ms = bucket_end;
            self.bytes_at_current_start = cumulative_bytes;
        }
    }

    fn buckets(&self) -> &[Bucket] {
        &self.buckets
    }
}

/// Throughput calculator using MST (Moving Sample Trimmed) algorithms.
///
/// Maintains two bucket streams (500ms and 750ms) and computes four algorithm
/// variants plus a blended output and superspeed window.
#[derive(Debug)]
pub struct ThroughputCalculator {
    duration_ms: u64,
    stream_500: BucketStream,
    stream_750: BucketStream,
    last_elapsed_ms: u64,
    last_cumulative_bytes: u64,
    locked_connections: Option<usize>,
}

impl ThroughputCalculator {
    /// Create a new calculator for a test of the given configured duration.
    pub fn new(duration_ms: u64) -> Self {
        Self {
            duration_ms,
            stream_500: BucketStream::new(500),
            stream_750: BucketStream::new(750),
            last_elapsed_ms: 0,
            last_cumulative_bytes: 0,
            locked_connections: None,
        }
    }

    /// Record a cumulative sample and return the current blended speed in bytes/sec.
    ///
    /// Called from the coordinator poll loop (typically every 50ms).
    pub fn record_sample(&mut self, elapsed_ms: u64, cumulative_bytes: u64) -> f64 {
        self.stream_500.record(elapsed_ms, cumulative_bytes);
        self.stream_750.record(elapsed_ms, cumulative_bytes);
        self.last_elapsed_ms = elapsed_ms;
        self.last_cumulative_bytes = cumulative_bytes;

        let average = self.compute_average();
        let mst = self.compute_mst_66_30();
        self.blend(average, mst, elapsed_ms)
    }

    /// Finalize all algorithms and return the comprehensive result.
    pub fn finish(&self) -> ThroughputResult {
        let average_bps = self.compute_average();
        let mst_66_30_bps = self.compute_mst_66_30();
        let mst_66_20_bps = self.compute_mst_66_20();
        let mst_75_30_bps = self.compute_mst_75_30();
        let blended_bps = self.blend(average_bps, mst_66_30_bps, self.last_elapsed_ms);
        let superspeed_bps = self.compute_superspeed();

        ThroughputResult {
            average_bps,
            mst_66_30_bps,
            mst_66_20_bps,
            mst_75_30_bps,
            blended_bps,
            superspeed_bps,
            buckets_500ms: self.stream_500.buckets().to_vec(),
            total_bytes: self.last_cumulative_bytes,
            elapsed_ms: self.last_elapsed_ms,
        }
    }

    /// Returns true if measurements are stable enough to stop the test early.
    ///
    /// Requires at least `min_duration_ms` elapsed and a coefficient of variation
    /// below 0.05 across the last 6 × 500ms buckets.
    pub fn should_stop_early(&self, min_duration_ms: u64) -> bool {
        if self.last_elapsed_ms < min_duration_ms {
            return false;
        }

        let buckets = self.stream_500.buckets();
        if buckets.len() < 6 {
            return false;
        }

        let last_6 = &buckets[buckets.len() - 6..];
        let bandwidths: Vec<f64> = last_6.iter().map(|b| b.bandwidth_bytes_per_sec()).collect();

        let mean = bandwidths.iter().sum::<f64>() / bandwidths.len() as f64;
        if mean <= 0.0 {
            return false;
        }

        let variance = bandwidths
            .iter()
            .map(|&bw| (bw - mean).powi(2))
            .sum::<f64>()
            / bandwidths.len() as f64;
        let cv = variance.sqrt() / mean;

        cv < 0.05
    }

    /// Compute desired number of connections based on current throughput.
    ///
    /// Formula: `min(max, round(speed_mbps / 6))`. Connection count is
    /// locked (frozen) once 50% of the configured test duration has elapsed.
    pub fn desired_connections(&mut self, max: usize) -> usize {
        if let Some(locked) = self.locked_connections {
            return locked;
        }

        let desired = self.compute_desired_connections(max);

        if self.last_elapsed_ms >= self.duration_ms / 2 {
            self.locked_connections = Some(desired);
        }

        desired
    }

    fn compute_desired_connections(&self, max: usize) -> usize {
        let speed_bps = self.compute_average();
        let speed_mbps = speed_bps * 8.0 / 1_000_000.0;
        let desired = (speed_mbps / 6.0).round() as usize;
        desired.clamp(1, max)
    }

    /// Suggest request size targeting the configured request length per connection.
    ///
    /// Near the end of the test (`time_remaining_ms < 2000`), the target
    /// shrinks to avoid large in-flight requests that distort the final
    /// measurement.
    pub fn suggested_request_size(
        &self,
        num_connections: usize,
        time_remaining_ms: u64,
        config: &TransferConfig,
    ) -> usize {
        let speed_bps = self.compute_average();
        if speed_bps <= 0.0 || num_connections == 0 {
            return config.min_request_size;
        }

        let target_ms = if time_remaining_ms > 0 && time_remaining_ms < 2000 {
            (time_remaining_ms / 2).max(100) as f64
        } else {
            config.request_target_ms as f64
        };

        let size = (target_ms / 1000.0 * speed_bps / num_connections as f64) as usize;
        size.clamp(config.min_request_size, config.max_request_size)
    }

    fn compute_average(&self) -> f64 {
        if self.last_elapsed_ms == 0 {
            return 0.0;
        }
        self.last_cumulative_bytes as f64 / (self.last_elapsed_ms as f64 / 1_000.0)
    }

    fn compute_mst_66_30(&self) -> f64 {
        compute_mst(
            self.stream_500.buckets(),
            &MstAlgoConfig {
                keep_ratio: 0.66,
                warmup_buckets: 2,
            },
        )
    }

    fn compute_mst_66_20(&self) -> f64 {
        compute_mst(
            self.stream_750.buckets(),
            &MstAlgoConfig {
                keep_ratio: 0.66,
                warmup_buckets: 2,
            },
        )
    }

    fn compute_mst_75_30(&self) -> f64 {
        compute_mst(
            self.stream_500.buckets(),
            &MstAlgoConfig {
                keep_ratio: 0.75,
                warmup_buckets: 2,
            },
        )
    }

    /// Blend average and MST based on how far through the test we are.
    ///
    /// Early in the test MST has few buckets so we lean on average.
    /// Late in the test we trust MST more — unless MST < average, in
    /// which case average is returned to avoid underreporting.
    fn blend(&self, average: f64, mst: f64, elapsed_ms: u64) -> f64 {
        if self.duration_ms == 0 || average <= 0.0 {
            return average;
        }

        let ratio = (elapsed_ms as f64 / self.duration_ms as f64).clamp(0.0, 1.0);

        if mst > average {
            ratio * mst + (1.0 - ratio) * average
        } else {
            average
        }
    }

    /// Find the maximum throughput window in the second half of the test.
    fn compute_superspeed(&self) -> f64 {
        let buckets = self.stream_500.buckets();
        if buckets.is_empty() || self.last_elapsed_ms == 0 {
            return 0.0;
        }

        let half_elapsed = self.last_elapsed_ms / 2;
        let half_duration = self.duration_ms / 2;
        let mut max_bps = 0.0_f64;

        for (i, start_bucket) in buckets.iter().enumerate() {
            if start_bucket.start_ms < half_elapsed {
                continue;
            }
            let bytes_before_start =
                start_bucket.total_bytes.saturating_sub(start_bucket.bytes);

            for end_bucket in &buckets[i..] {
                let window_ms = end_bucket.stop_ms.saturating_sub(start_bucket.start_ms);
                if window_ms < half_duration {
                    continue;
                }
                let window_bytes = end_bucket.total_bytes.saturating_sub(bytes_before_start);
                if window_ms > 0 {
                    let bps = window_bytes as f64 / (window_ms as f64 / 1_000.0);
                    max_bps = max_bps.max(bps);
                }
            }
        }

        max_bps
    }
}

/// Core MST algorithm: skip warmup buckets, sort by bandwidth, drop lowest outliers.
fn compute_mst(buckets: &[Bucket], config: &MstAlgoConfig) -> f64 {
    if buckets.len() <= config.warmup_buckets {
        return 0.0;
    }

    let active = &buckets[config.warmup_buckets..];
    if active.is_empty() {
        return 0.0;
    }

    let mut indexed: Vec<(f64, usize)> = active
        .iter()
        .enumerate()
        .map(|(i, b)| (b.bandwidth_bytes_per_sec(), i))
        .collect();
    indexed.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

    let total = indexed.len();
    let drop_count = ((1.0 - config.keep_ratio) * total as f64).round() as usize;
    let kept = &indexed[drop_count..];

    if kept.is_empty() {
        return 0.0;
    }

    let total_bytes: u64 = kept.iter().map(|&(_, idx)| active[idx].bytes).sum();
    let total_duration_ms: u64 = kept
        .iter()
        .map(|&(_, idx)| active[idx].stop_ms.saturating_sub(active[idx].start_ms))
        .sum();

    if total_duration_ms == 0 {
        return 0.0;
    }

    total_bytes as f64 / (total_duration_ms as f64 / 1_000.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_stream_closes_at_correct_boundaries() {
        let mut stream = BucketStream::new(500);
        for step in 0..=30 {
            let t: u64 = step * 50;
            stream.record(t, t * 1000);
        }
        let buckets = stream.buckets();
        assert_eq!(buckets.len(), 3);
        assert_eq!(buckets[0].start_ms, 0);
        assert_eq!(buckets[0].stop_ms, 500);
        assert_eq!(buckets[1].start_ms, 500);
        assert_eq!(buckets[1].stop_ms, 1000);
        assert_eq!(buckets[2].start_ms, 1000);
        assert_eq!(buckets[2].stop_ms, 1500);
    }

    #[test]
    fn bucket_stream_bytes_accumulate_correctly() {
        let mut stream = BucketStream::new(500);
        for step in 0..=20 {
            let t: u64 = step * 50;
            stream.record(t, t * 2000);
        }
        let buckets = stream.buckets();
        assert_eq!(buckets.len(), 2);
        assert_eq!(buckets[0].bytes, 1_000_000);
        assert_eq!(buckets[1].bytes, 1_000_000);
    }

    #[test]
    fn bucket_stream_750ms_boundaries() {
        let mut stream = BucketStream::new(750);
        for step in 0..=30 {
            let t: u64 = step * 50;
            stream.record(t, t * 1000);
        }
        let buckets = stream.buckets();
        assert_eq!(buckets.len(), 2);
        assert_eq!(buckets[0].start_ms, 0);
        assert_eq!(buckets[0].stop_ms, 750);
        assert_eq!(buckets[1].start_ms, 750);
        assert_eq!(buckets[1].stop_ms, 1500);
    }

    #[test]
    fn mst_skips_warmup_and_trims_outliers() {
        let buckets = vec![
            Bucket {
                start_ms: 0,
                stop_ms: 500,
                bytes: 100_000,
                total_bytes: 100_000,
            },
            Bucket {
                start_ms: 500,
                stop_ms: 1000,
                bytes: 200_000,
                total_bytes: 300_000,
            },
            Bucket {
                start_ms: 1000,
                stop_ms: 1500,
                bytes: 50_000,
                total_bytes: 350_000,
            },
            Bucket {
                start_ms: 1500,
                stop_ms: 2000,
                bytes: 500_000,
                total_bytes: 850_000,
            },
            Bucket {
                start_ms: 2000,
                stop_ms: 2500,
                bytes: 500_000,
                total_bytes: 1_350_000,
            },
            Bucket {
                start_ms: 2500,
                stop_ms: 3000,
                bytes: 500_000,
                total_bytes: 1_850_000,
            },
        ];
        let config = MstAlgoConfig {
            keep_ratio: 0.66,
            warmup_buckets: 2,
        };
        let result = compute_mst(&buckets, &config);
        assert!(
            (result - 1_000_000.0).abs() < 1.0,
            "expected ~1M B/s, got {result}"
        );
    }

    #[test]
    fn blend_early_favors_average() {
        let calc = ThroughputCalculator::new(10_000);
        let avg = 1_000_000.0;
        let mst = 1_500_000.0;
        let blended = calc.blend(avg, mst, 1000);
        assert!(
            (blended - 1_050_000.0).abs() < 1.0,
            "at 10% elapsed: expected 1_050_000, got {blended}"
        );
    }

    #[test]
    fn blend_late_favors_mst() {
        let calc = ThroughputCalculator::new(10_000);
        let avg = 1_000_000.0;
        let mst = 1_200_000.0;
        let blended = calc.blend(avg, mst, 9000);
        assert!(
            (blended - 1_180_000.0).abs() < 1.0,
            "at 90% elapsed: expected 1_180_000, got {blended}"
        );
    }

    #[test]
    fn blend_mst_below_average_returns_average() {
        let calc = ThroughputCalculator::new(10_000);
        let avg = 1_200_000.0;
        let mst = 800_000.0;
        let blended = calc.blend(avg, mst, 9000);
        assert!(
            (blended - avg).abs() < 1.0,
            "MST < average should return average, got {blended}"
        );
    }

    #[test]
    fn cv_stable_triggers_early_exit() {
        let mut calc = ThroughputCalculator::new(20_000);
        for step in 0..=80 {
            let t: u64 = step * 50;
            calc.record_sample(t, t * 1_000_000);
        }
        assert!(calc.stream_500.buckets().len() >= 6);
        assert!(
            calc.should_stop_early(3000),
            "stable throughput should trigger early exit"
        );
    }

    #[test]
    fn cv_noisy_prevents_early_exit() {
        let mut calc = ThroughputCalculator::new(20_000);
        let mut bytes: u64 = 0;
        for step in 0..=80 {
            let t: u64 = step * 50;
            let bucket_index = t / 500;
            let rate: u64 = if bucket_index % 2 == 0 {
                2_000_000
            } else {
                100_000
            };
            bytes += 50 * rate;
            calc.record_sample(t, bytes);
        }
        assert!(
            !calc.should_stop_early(3000),
            "noisy throughput should not trigger early exit"
        );
    }

    #[test]
    fn zero_bytes_returns_zero_speed() {
        let mut calc = ThroughputCalculator::new(10_000);
        for step in 0..=100 {
            let bps = calc.record_sample(step * 50, 0);
            assert_eq!(bps, 0.0);
        }
        let result = calc.finish();
        assert_eq!(result.blended_bps, 0.0);
        assert_eq!(result.average_bps, 0.0);
    }

    #[test]
    fn insufficient_buckets_falls_back_to_average() {
        let mut calc = ThroughputCalculator::new(10_000);
        for step in 0..=12 {
            let t: u64 = step * 50;
            calc.record_sample(t, t * 1000);
        }
        let result = calc.finish();
        assert!(result.blended_bps > 0.0);
        assert!(
            (result.blended_bps - result.average_bps).abs() < 1.0,
            "with insufficient buckets, blended should equal average"
        );
    }

    #[test]
    fn superspeed_finds_burst_window() {
        let mut calc = ThroughputCalculator::new(10_000);
        for step in 0..=120 {
            let t: u64 = step * 50;
            calc.record_sample(t, t * 1000);
        }
        let base_bytes: u64 = 6000 * 1000;
        for step in 121..=200 {
            let t: u64 = step * 50;
            let burst_delta = (t - 6000) * 5000;
            calc.record_sample(t, base_bytes + burst_delta);
        }
        let result = calc.finish();
        assert!(
            result.superspeed_bps > result.average_bps,
            "superspeed ({}) should exceed average ({})",
            result.superspeed_bps,
            result.average_bps
        );
    }

    #[test]
    fn should_stop_early_respects_min_duration() {
        let mut calc = ThroughputCalculator::new(20_000);
        for step in 0..=80 {
            let t: u64 = step * 50;
            calc.record_sample(t, t * 1_000_000);
        }
        assert!(
            !calc.should_stop_early(5000),
            "should not exit before min_duration"
        );
        assert!(
            calc.should_stop_early(3000),
            "should exit when past min_duration and stable"
        );
    }

    #[test]
    fn record_sample_returns_blended_speed() {
        let mut calc = ThroughputCalculator::new(10_000);
        let mut last_bps = 0.0;
        for step in 0..=100 {
            let t: u64 = step * 50;
            last_bps = calc.record_sample(t, t * 1_000_000);
        }
        assert!(last_bps > 0.0, "blended speed should be positive");
        assert!(
            (last_bps - 1_000_000_000.0).abs() / 1_000_000_000.0 < 0.1,
            "blended speed should be near 1GB/s, got {last_bps}"
        );
    }

    #[test]
    fn finish_returns_all_algorithm_results() {
        let mut calc = ThroughputCalculator::new(10_000);
        for step in 0..=200 {
            let t: u64 = step * 50;
            calc.record_sample(t, t * 500_000);
        }
        let result = calc.finish();
        assert!(result.average_bps > 0.0);
        assert!(result.mst_66_30_bps > 0.0);
        assert!(result.mst_66_20_bps > 0.0);
        assert!(result.mst_75_30_bps > 0.0);
        assert!(result.blended_bps > 0.0);
        assert!(result.total_bytes > 0);
        assert!(result.elapsed_ms > 0);
        assert!(!result.buckets_500ms.is_empty());
    }

    #[test]
    fn throughput_result_mbps_conversions() {
        let result = ThroughputResult {
            average_bps: 125_000_000.0,
            mst_66_30_bps: 130_000_000.0,
            mst_66_20_bps: 128_000_000.0,
            mst_75_30_bps: 127_000_000.0,
            blended_bps: 129_000_000.0,
            superspeed_bps: 140_000_000.0,
            buckets_500ms: vec![],
            total_bytes: 1_250_000_000,
            elapsed_ms: 10_000,
        };
        assert!((result.average_mbps() - 1000.0).abs() < 0.01);
        assert!((result.mst_mbps() - 1040.0).abs() < 0.01);
        assert!((result.blended_mbps() - 1032.0).abs() < 0.01);
    }

    #[test]
    fn desired_connections_scales_with_speed() {
        let mut calc = ThroughputCalculator::new(10_000);
        // Feed 1s of data at 12Mbps = 1_500_000 bytes/s
        for step in 0..=20 {
            let t: u64 = step * 50;
            calc.record_sample(t, t * 1_500);
        }
        // 12Mbps / 6Mbps = 2
        let desired = calc.desired_connections(16);
        assert_eq!(desired, 2);
    }

    #[test]
    fn desired_connections_locks_after_half_elapsed() {
        let mut calc = ThroughputCalculator::new(10_000);
        // Feed 6s of data
        for step in 0..=120 {
            let t: u64 = step * 50;
            calc.record_sample(t, t * 1_500);
        }
        // After 60% elapsed, connections should lock
        let locked = calc.desired_connections(16);
        // Change speed dramatically — shouldn't matter, it's locked
        calc.record_sample(7000, 7000 * 100_000);
        assert_eq!(calc.desired_connections(16), locked);
    }

    #[test]
    fn desired_connections_clamps_to_max() {
        let mut calc = ThroughputCalculator::new(10_000);
        // Feed very high speed: 1Gbps = 125_000_000 B/s
        for step in 0..=20 {
            let t: u64 = step * 50;
            calc.record_sample(t, t * 125_000_000);
        }
        assert_eq!(calc.desired_connections(8), 8);
    }

    #[test]
    fn suggested_request_size_scales_with_speed() {
        let mut calc = ThroughputCalculator::new(10_000);
        // Feed 100Mbps = 12_500_000 B/s for 1s
        for step in 0..=20 {
            let t: u64 = step * 50;
            calc.record_sample(t, t * 12_500);
        }
        // With 4 connections and 8s remaining: target 1s * 12.5MB/s / 4 ≈ 3.1MB
        let size = calc.suggested_request_size(4, 8000, &test_transfer_config());
        assert!(size > 2_000_000 && size < 5_000_000, "got {size}");
    }

    #[test]
    fn suggested_request_size_shrinks_near_end() {
        let mut calc = ThroughputCalculator::new(10_000);
        for step in 0..=20 {
            let t: u64 = step * 50;
            calc.record_sample(t, t * 12_500);
        }
        let config = test_transfer_config();
        let size_normal = calc.suggested_request_size(4, 8000, &config);
        let size_near_end = calc.suggested_request_size(4, 500, &config);
        assert!(
            size_near_end < size_normal,
            "near-end ({size_near_end}) should be smaller than normal ({size_normal})"
        );
    }

    #[test]
    fn suggested_request_size_returns_min_with_no_speed() {
        let calc = ThroughputCalculator::new(10_000);
        let size = calc.suggested_request_size(4, 8000, &test_transfer_config());
        assert_eq!(size, 32 * 1024);
    }

    #[test]
    fn suggested_request_size_honors_longer_download_target() {
        let mut calc = ThroughputCalculator::new(10_000);
        for step in 0..=20 {
            let t: u64 = step * 50;
            calc.record_sample(t, t * 12_500);
        }

        let mut config = test_transfer_config();
        config.request_target_ms = 5_000;
        config.min_request_size = 25_000_000;
        config.max_request_size = 250_000_000;

        let size = calc.suggested_request_size(4, 8_000, &config);
        assert_eq!(size, 25_000_000);
    }

    fn test_transfer_config() -> TransferConfig {
        TransferConfig {
            connections: 4,
            initial_connections: 4,
            max_seconds: 10,
            progress_interval: None,
            request_target_ms: 1_000,
            start_request_size: 1_048_576,
            min_request_size: 32 * 1024,
            max_request_size: 25 * 1024 * 1024,
        }
    }
}
