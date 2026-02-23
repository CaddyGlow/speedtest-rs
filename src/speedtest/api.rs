use std::fmt;

use clap::ValueEnum;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ModernTransportMode {
    Xhr,
    Tcp,
}

impl fmt::Display for ModernTransportMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Xhr => f.write_str("xhr"),
            Self::Tcp => f.write_str("tcp"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransportProtocol {
    Xhr,
    Tcp,
}

impl From<ModernTransportMode> for TransportProtocol {
    fn from(value: ModernTransportMode) -> Self {
        match value {
            ModernTransportMode::Xhr => Self::Xhr,
            ModernTransportMode::Tcp => Self::Tcp,
        }
    }
}

impl fmt::Display for TransportProtocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Xhr => f.write_str("xhr"),
            Self::Tcp => f.write_str("tcp"),
        }
    }
}
