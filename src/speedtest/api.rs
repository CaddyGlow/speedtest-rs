use std::fmt;

use clap::ValueEnum;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum SpeedtestApiMode {
    Auto,
    Legacy,
    ModernTcp,
    Modern,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ModernTransportMode {
    Xhr,
    Tcp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResolvedSpeedtestApi {
    Legacy,
    Modern,
    ModernTcp,
}

impl fmt::Display for ResolvedSpeedtestApi {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Legacy => f.write_str("legacy"),
            Self::Modern => f.write_str("modern"),
            Self::ModernTcp => f.write_str("modern-tcp"),
        }
    }
}
