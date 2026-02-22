use std::time::{Duration, SystemTime, UNIX_EPOCH};

const MAGIC: [u8; 4] = *b"TMUX";
const HEADER_LEN: usize = 4 + 8 + 8;

#[derive(Debug, Clone, Copy)]
pub struct UdpHeader {
    pub sequence: u64,
    pub sent_micros: u64,
}

#[must_use]
pub fn build_packet(sequence: u64, payload_len: usize) -> Vec<u8> {
    let mut packet = vec![0_u8; payload_len.max(HEADER_LEN)];
    packet[..4].copy_from_slice(&MAGIC);
    packet[4..12].copy_from_slice(&sequence.to_be_bytes());

    let now = now_micros();
    packet[12..20].copy_from_slice(&now.to_be_bytes());

    if payload_len > HEADER_LEN {
        packet[HEADER_LEN..].fill(0x31);
    }

    packet
}

#[must_use]
pub fn parse_header(packet: &[u8]) -> Option<UdpHeader> {
    if packet.len() < HEADER_LEN || packet[..4] != MAGIC {
        return None;
    }

    let mut seq = [0_u8; 8];
    seq.copy_from_slice(&packet[4..12]);

    let mut ts = [0_u8; 8];
    ts.copy_from_slice(&packet[12..20]);

    Some(UdpHeader {
        sequence: u64::from_be_bytes(seq),
        sent_micros: u64::from_be_bytes(ts),
    })
}

#[derive(Default, Debug, Clone)]
pub struct UdpReceiveMetrics {
    pub total_packets: u64,
    pub lost_packets: u64,
    pub out_of_order: u64,
    pub jitter_ms: f64,
    last_transit_ms: Option<f64>,
    expected_next_sequence: Option<u64>,
}

impl UdpReceiveMetrics {
    pub fn on_packet(&mut self, header: Option<UdpHeader>) {
        self.total_packets += 1;

        let Some(header) = header else {
            return;
        };

        match self.expected_next_sequence {
            None => {
                self.expected_next_sequence = Some(header.sequence + 1);
            }
            Some(expected) if header.sequence == expected => {
                self.expected_next_sequence = Some(expected + 1);
            }
            Some(expected) if header.sequence > expected => {
                self.lost_packets += header.sequence - expected;
                self.expected_next_sequence = Some(header.sequence + 1);
            }
            Some(_) => {
                self.out_of_order += 1;
            }
        }

        let now_micros = now_micros();
        if now_micros >= header.sent_micros {
            let transit_ms = (now_micros - header.sent_micros) as f64 / 1_000.0;
            if let Some(last) = self.last_transit_ms {
                let delta = (transit_ms - last).abs();
                self.jitter_ms += (delta - self.jitter_ms) / 16.0;
            }
            self.last_transit_ms = Some(transit_ms);
        }
    }

    #[must_use]
    pub fn loss_percent(&self) -> Option<f64> {
        if self.total_packets == 0 {
            return None;
        }
        Some((self.lost_packets as f64 * 100.0) / self.total_packets as f64)
    }

    pub fn merge(&mut self, other: &Self) {
        if other.total_packets == 0 {
            return;
        }

        let combined_packets = self.total_packets + other.total_packets;
        let weighted_jitter = if combined_packets == 0 {
            0.0
        } else {
            ((self.jitter_ms * self.total_packets as f64)
                + (other.jitter_ms * other.total_packets as f64))
                / combined_packets as f64
        };

        self.total_packets = combined_packets;
        self.lost_packets += other.lost_packets;
        self.out_of_order += other.out_of_order;
        self.jitter_ms = weighted_jitter;
        self.last_transit_ms = other.last_transit_ms.or(self.last_transit_ms);
    }
}

fn now_micros() -> u64 {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO);
    duration.as_micros().min(u64::MAX as u128) as u64
}

#[cfg(test)]
mod tests {
    use super::{UdpReceiveMetrics, build_packet, parse_header};

    #[test]
    fn parses_packet_header_round_trip() {
        let packet = build_packet(7, 1200);
        let header = parse_header(&packet).expect("header should parse");
        assert_eq!(header.sequence, 7);
    }

    #[test]
    fn tracks_loss_and_order() {
        let mut metrics = UdpReceiveMetrics::default();
        for seq in [1_u64, 2, 4, 3, 5] {
            let packet = build_packet(seq, 128);
            metrics.on_packet(parse_header(&packet));
        }

        assert_eq!(metrics.total_packets, 5);
        assert_eq!(metrics.lost_packets, 1);
        assert_eq!(metrics.out_of_order, 1);
    }
}
