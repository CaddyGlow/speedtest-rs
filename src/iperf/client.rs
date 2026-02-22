use std::time::Duration;

use anyhow::Result;

use crate::cli::IperfProtocol;
use crate::iperf::control::ControlSession;
use crate::iperf::model::{
    IperfClientConfig, IperfDirection, IperfDirectionSummary, IperfProgress,
};
use crate::iperf::proxy;
use crate::iperf::{tcp, udp};

pub async fn run_direction<F>(
    config: &IperfClientConfig,
    direction: IperfDirection,
    progress_interval: Option<Duration>,
    on_progress: F,
) -> Result<IperfDirectionSummary>
where
    F: FnMut(IperfProgress),
{
    proxy::ensure_compatible(config.protocol, config.proxy.as_ref())?;

    let mut control = ControlSession::new(config, direction);
    let negotiated = control.start()?;

    let summary = match config.protocol {
        IperfProtocol::Tcp => {
            tcp::run_tcp_direction(
                config,
                negotiated,
                direction,
                progress_interval,
                on_progress,
            )
            .await
        }
        IperfProtocol::Udp => {
            udp::run_udp_direction(
                config,
                negotiated,
                direction,
                progress_interval,
                on_progress,
            )
            .await
        }
    }?;

    control.finish()?;
    Ok(summary)
}
