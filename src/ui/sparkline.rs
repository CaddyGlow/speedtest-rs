const BLOCKS: [char; 8] = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];

// ANSI color codes
const RED: &str = "\x1b[31m";
const YELLOW: &str = "\x1b[33m";
const GREEN: &str = "\x1b[32m";
const RESET: &str = "\x1b[0m";

pub struct Sparkline {
    samples: Vec<Option<f64>>,
    max_width: usize,
}

impl Sparkline {
    pub fn new(max_width: usize) -> Self {
        Self {
            samples: vec![None; max_width],
            max_width,
        }
    }

    pub fn set(&mut self, index: usize, value: f64) {
        if self.max_width == 0 {
            return;
        }
        let index = index.min(self.max_width - 1);
        self.samples[index] = Some(value);
    }

    pub fn render(&self) -> String {
        if self.max_width == 0 {
            return String::new();
        }

        let Some((lo, hi)) = self.percentile_range() else {
            return " ".repeat(self.max_width);
        };
        let range = hi - lo;

        let mut out = String::with_capacity(self.max_width * 16);
        for sample in &self.samples {
            let Some(val) = sample else {
                out.push(' ');
                continue;
            };
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
    fn percentile_range(&self) -> Option<(f64, f64)> {
        let mut sorted: Vec<f64> = self.samples.iter().flatten().copied().collect();
        if sorted.is_empty() {
            return None;
        }
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let n = sorted.len();
        if n < 4 {
            return Some((sorted[0], sorted[n - 1]));
        }
        let lo_idx = (n as f64 * 0.05).floor() as usize;
        let hi_idx = (n as f64 * 0.95).ceil() as usize;
        let hi_idx = hi_idx.min(n - 1);
        Some((sorted[lo_idx], sorted[hi_idx]))
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
        assert_eq!(s.render(), "          ");
    }

    #[test]
    fn single_sample_mid_height() {
        let mut s = Sparkline::new(10);
        s.set(0, 100.0);
        let rendered = strip_ansi(&s.render());
        assert_eq!(rendered, "▄         ");
    }

    #[test]
    fn all_same_mid_height() {
        let mut s = Sparkline::new(10);
        for idx in 0..5 {
            s.set(idx, 50.0);
        }
        let rendered = strip_ansi(&s.render());
        assert_eq!(rendered, "▄▄▄▄▄     ");
    }

    #[test]
    fn ascending_values() {
        let mut s = Sparkline::new(8);
        for i in 0..8 {
            s.set(i, i as f64);
        }
        let rendered = strip_ansi(&s.render());
        assert_eq!(rendered, "▁▂▃▄▅▆▇█");
    }

    #[test]
    fn setting_sample_overwrites_slot() {
        let mut s = Sparkline::new(3);
        s.set(0, 1.0);
        s.set(1, 2.0);
        s.set(1, 4.0);
        assert_eq!(s.samples[0], Some(1.0));
        assert_eq!(s.samples[1], Some(4.0));
        assert_eq!(s.samples[2], None);
    }

    #[test]
    fn outlier_does_not_squash_graph() {
        let mut s = Sparkline::new(60);
        s.set(0, 500.0); // single outlier
        for idx in 1..60 {
            s.set(idx, 100.0);
        }
        let rendered = strip_ansi(&s.render());
        let low_count = rendered.chars().filter(|&c| c == '▁').count();
        assert!(
            low_count <= 2,
            "too many low bars ({low_count}), outlier is squashing the graph"
        );
    }

    #[test]
    fn zero_renders_lowest() {
        let mut s = Sparkline::new(5);
        s.set(0, 0.0);
        s.set(1, 100.0);
        let rendered = strip_ansi(&s.render());
        assert_eq!(rendered.chars().next().unwrap(), '▁');
        assert_eq!(rendered.chars().nth(1).unwrap(), '█');
    }
}
