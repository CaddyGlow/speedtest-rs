use anyhow::{Result, bail};

use crate::cli::IperfProtocol;
use crate::iperf::model::{IperfClientConfig, IperfDirection};
use crate::util::clamp_worker_count;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlState {
    Init,
    Connected,
    ParametersExchanged,
    StreamsCreated,
    TestStarted,
    TestRunning,
    ResultsExchanged,
    ResultsDisplayed,
    Done,
}

#[derive(Debug, Clone, Copy)]
pub struct NegotiatedParameters {
    pub seconds: u64,
    pub parallel: usize,
    pub packet_size: usize,
    pub bitrate_bps: Option<u64>,
}

pub struct ControlSession<'a> {
    state: ControlState,
    config: &'a IperfClientConfig,
    direction: IperfDirection,
}

impl<'a> ControlSession<'a> {
    #[must_use]
    pub fn new(config: &'a IperfClientConfig, direction: IperfDirection) -> Self {
        Self {
            state: ControlState::Init,
            config,
            direction,
        }
    }

    #[cfg(test)]
    #[must_use]
    pub fn state(&self) -> ControlState {
        self.state
    }

    pub fn start(&mut self) -> Result<NegotiatedParameters> {
        self.transition(ControlState::Connected)?;
        self.transition(ControlState::ParametersExchanged)?;

        let negotiated = self.negotiate_parameters()?;

        self.transition(ControlState::StreamsCreated)?;
        self.transition(ControlState::TestStarted)?;
        self.transition(ControlState::TestRunning)?;
        Ok(negotiated)
    }

    pub fn finish(&mut self) -> Result<()> {
        self.transition(ControlState::ResultsExchanged)?;
        self.transition(ControlState::ResultsDisplayed)?;
        self.transition(ControlState::Done)
    }

    fn negotiate_parameters(&self) -> Result<NegotiatedParameters> {
        let parallel = clamp_worker_count(self.config.parallel);
        let seconds = self.config.seconds.max(1);

        let packet_size = match self.config.protocol {
            IperfProtocol::Tcp => 128 * 1024,
            IperfProtocol::Udp => 1200,
        };

        let bitrate_bps = match self.config.protocol {
            IperfProtocol::Tcp => None,
            IperfProtocol::Udp => {
                let raw = self.config.bitrate_bps.or(Some(1_000_000));
                raw.filter(|value| *value > 0)
            }
        };

        if matches!(self.direction, IperfDirection::Download)
            && matches!(self.config.protocol, IperfProtocol::Udp)
            && bitrate_bps.is_none()
        {
            bail!("failed negotiating UDP bitrate for download direction");
        }

        Ok(NegotiatedParameters {
            seconds,
            parallel,
            packet_size,
            bitrate_bps,
        })
    }

    fn transition(&mut self, next: ControlState) -> Result<()> {
        let expected = match self.state {
            ControlState::Init => ControlState::Connected,
            ControlState::Connected => ControlState::ParametersExchanged,
            ControlState::ParametersExchanged => ControlState::StreamsCreated,
            ControlState::StreamsCreated => ControlState::TestStarted,
            ControlState::TestStarted => ControlState::TestRunning,
            ControlState::TestRunning => ControlState::ResultsExchanged,
            ControlState::ResultsExchanged => ControlState::ResultsDisplayed,
            ControlState::ResultsDisplayed => ControlState::Done,
            ControlState::Done => bail!("control session already finished"),
        };

        if next != expected {
            bail!(
                "invalid control state transition: {:?} -> {:?} (expected {:?})",
                self.state,
                next,
                expected
            );
        }

        self.state = next;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{ControlSession, ControlState};
    use crate::cli::IperfProtocol;
    use crate::iperf::model::{IperfClientConfig, IperfDirection};

    fn config(protocol: IperfProtocol) -> IperfClientConfig {
        IperfClientConfig {
            host: "127.0.0.1".to_string(),
            port: 5201,
            protocol,
            seconds: 10,
            parallel: 2,
            bitrate_bps: Some(2_000_000),
            proxy: None,
        }
    }

    #[test]
    fn control_session_transitions_to_done() {
        let cfg = config(IperfProtocol::Tcp);
        let mut session = ControlSession::new(&cfg, IperfDirection::Upload);
        let negotiated = session.start().expect("control start should succeed");
        assert_eq!(negotiated.parallel, 2);
        assert_eq!(session.state(), ControlState::TestRunning);
        session.finish().expect("control finish should succeed");
        assert_eq!(session.state(), ControlState::Done);
    }
}
