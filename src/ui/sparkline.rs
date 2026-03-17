use std::collections::VecDeque;

const BLOCKS: [char; 8] = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];

// ANSI color codes
const RED: &str = "\x1b[31m";
const YELLOW: &str = "\x1b[33m";
const GREEN: &str = "\x1b[32m";
const RESET: &str = "\x1b[0m";

pub struct Sparkline {
    samples: VecDeque<f64>,
    max_width: usize,
}

impl Sparkline {
    pub fn new(max_width: usize) -> Self {
        Self {
            samples: VecDeque::with_capacity(max_width),
            max_width,
        }
    }

    pub fn push(&mut self, value: f64) {
        if self.samples.len() >= self.max_width {
            self.samples.pop_front();
        }
        self.samples.push_back(value);
    }

    pub fn render(&self) -> String {
        if self.samples.is_empty() {
            return String::new();
        }

        let (lo, hi) = self.percentile_range();
        let range = hi - lo;

        let mut out = String::with_capacity(self.samples.len() * 16);
        for &val in &self.samples {
            // Map to 0..7 block index, clamping outliers to edges
            let idx = if range < f64::EPSILON {
                3 // mid-height when all same or single sample
            } else {
                let normalized = ((val - lo) / range).clamp(0.0, 1.0);
                ((normalized * 7.0).round() as usize).min(7)
            };

            // Color: bottom third red, middle yellow, top third green
            let color = match idx {
                0 | 1 => RED,
                2..=4 => YELLOW,
                _ => GREEN,
            };
            out.push_str(color);
            out.push(BLOCKS[idx]);
        }
        out.push_str(RESET);
        out
    }

    /// Returns (p5, p95) percentile bounds for robust scaling.
    /// Falls back to (min, max) with fewer than 4 samples.
    fn percentile_range(&self) -> (f64, f64) {
        let mut sorted: Vec<f64> = self.samples.iter().copied().collect();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let n = sorted.len();
        if n < 4 {
            return (sorted[0], sorted[n - 1]);
        }
        let lo_idx = (n as f64 * 0.05).floor() as usize;
        let hi_idx = (n as f64 * 0.95).ceil() as usize;
        let hi_idx = hi_idx.min(n - 1);
        (sorted[lo_idx], sorted[hi_idx])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strip_ansi(s: &str) -> String {
        let mut out = String::new();
        let mut chars = s.chars();
        while let Some(c) = chars.next() {
            if c == '\x1b' {
                for c2 in chars.by_ref() {
                    if c2 == 'm' {
                        break;
                    }
                }
            } else {
                out.push(c);
            }
        }
        out
    }

    #[test]
    fn empty_renders_empty() {
        let s = Sparkline::new(10);
        assert_eq!(s.render(), "");
    }

    #[test]
    fn single_sample_mid_height() {
        let mut s = Sparkline::new(10);
        s.push(100.0);
        let rendered = strip_ansi(&s.render());
        assert_eq!(rendered, "▄");
    }

    #[test]
    fn all_same_mid_height() {
        let mut s = Sparkline::new(10);
        for _ in 0..5 {
            s.push(50.0);
        }
        let rendered = strip_ansi(&s.render());
        assert_eq!(rendered, "▄▄▄▄▄");
    }

    #[test]
    fn ascending_values() {
        let mut s = Sparkline::new(8);
        for i in 0..8 {
            s.push(i as f64);
        }
        let rendered = strip_ansi(&s.render());
        assert_eq!(rendered, "▁▂▃▄▅▆▇█");
    }

    #[test]
    fn ring_buffer_drops_oldest() {
        let mut s = Sparkline::new(3);
        s.push(1.0);
        s.push(2.0);
        s.push(3.0);
        s.push(4.0); // should drop 1.0
        assert_eq!(s.samples.len(), 3);
        assert_eq!(s.samples[0], 2.0);
    }

    #[test]
    fn outlier_does_not_squash_graph() {
        let mut s = Sparkline::new(60);
        s.push(500.0); // single outlier
        for _ in 0..59 {
            s.push(100.0);
        }
        let rendered = strip_ansi(&s.render());
        let low_count = rendered.chars().filter(|&c| c == '▁').count();
        assert!(low_count <= 2, "too many low bars ({low_count}), outlier is squashing the graph");
    }

    #[test]
    fn zero_renders_lowest() {
        let mut s = Sparkline::new(5);
        s.push(0.0);
        s.push(100.0);
        let rendered = strip_ansi(&s.render());
        assert_eq!(rendered.chars().next().unwrap(), '▁');
        assert_eq!(rendered.chars().nth(1).unwrap(), '█');
    }
}
