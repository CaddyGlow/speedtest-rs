use std::time::{Duration, SystemTime, UNIX_EPOCH};

const IPERF_UDP_HEADER_LEN: usize = 12;

#[derive(Debug, Clone, Copy)]
pub struct IperfUdpHeader {
    pub sequence: u64,
    pub sent_micros: u64,
}

#[must_use]
pub fn build_iperf_udp_packet(sequence: u32, payload_len: usize) -> Vec<u8> {
    let size = payload_len.max(IPERF_UDP_HEADER_LEN);
    let mut packet = vec![0_u8; size];

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO);
    let sec = (now.as_secs().min(u32::MAX as u64) as u32).to_be_bytes();
    let usec = (now.subsec_micros()).to_be_bytes();
    let seq = sequence.to_be_bytes();

    packet[0..4].copy_from_slice(&sec);
    packet[4..8].copy_from_slice(&usec);
    packet[8..12].copy_from_slice(&seq);

    if size > IPERF_UDP_HEADER_LEN {
        packet[IPERF_UDP_HEADER_LEN..].fill(0x31);
    }

    packet
}

#[must_use]
pub fn parse_iperf_udp_header(packet: &[u8]) -> Option<IperfUdpHeader> {
    if packet.len() < IPERF_UDP_HEADER_LEN {
        return None;
    }

    let sec = u32::from_be_bytes([packet[0], packet[1], packet[2], packet[3]]);
    let usec = u32::from_be_bytes([packet[4], packet[5], packet[6], packet[7]]);
    let seq = u32::from_be_bytes([packet[8], packet[9], packet[10], packet[11]]);
    let sent_micros = sec as u64 * 1_000_000 + usec as u64;

    Some(IperfUdpHeader {
        sequence: seq as u64,
        sent_micros,
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
    pub fn on_packet(&mut self, header: Option<IperfUdpHeader>) {
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

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_micros()
            .min(u64::MAX as u128) as u64;

        if now >= header.sent_micros {
            let transit_ms = (now - header.sent_micros) as f64 / 1_000.0;
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
}

#[cfg(test)]
mod tests {
    use super::{UdpReceiveMetrics, build_iperf_udp_packet, parse_iperf_udp_header};

    #[test]
    fn parses_iperf_udp_header_round_trip() {
        let packet = build_iperf_udp_packet(7, 1200);
        let header = parse_iperf_udp_header(&packet).expect("header should parse");
        assert_eq!(header.sequence, 7);
    }

    #[test]
    fn tracks_loss_and_order() {
        let mut metrics = UdpReceiveMetrics::default();
        for seq in [1_u32, 2, 4, 3, 5] {
            let packet = build_iperf_udp_packet(seq, 64);
            metrics.on_packet(parse_iperf_udp_header(&packet));
        }

        assert_eq!(metrics.total_packets, 5);
        assert_eq!(metrics.lost_packets, 1);
        assert_eq!(metrics.out_of_order, 1);
    }
}
