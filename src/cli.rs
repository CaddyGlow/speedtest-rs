use clap::{Args, Parser, Subcommand, ValueEnum};

const APP_VERSION: &str = env!("TUNMUX_SPEEDTEST_VERSION");

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum TuiMode {
    Compact,
    Fullscreen,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum IperfProtocol {
    Tcp,
    Udp,
}

#[derive(Debug, Clone, Args)]
pub struct CacheShowArgs {
    /// Maximum number of entries to display
    #[arg(long, default_value_t = 20, value_parser = parse_positive_usize)]
    pub limit: usize,

    /// Emit machine-readable JSON output
    #[arg(long)]
    pub json: bool,

    /// Case-insensitive search term for id/host/location fields
    #[arg(long)]
    pub search: Option<String>,
}

#[derive(Debug, Clone, Subcommand)]
pub enum CacheCommand {
    /// Show resolved local cache file path
    Path,

    /// Show cached speedtest servers
    Show(CacheShowArgs),

    /// Clear local cache file
    Clear,
}

#[derive(Debug, Clone, Args)]
pub struct CacheArgs {
    #[command(subcommand)]
    pub command: CacheCommand,
}

#[derive(Debug, Clone, Args)]
pub struct RunArgs {
    /// Optional server id override
    #[arg(long)]
    pub server_id: Option<u64>,

    /// Number of candidate servers to probe for latency
    #[arg(long, default_value_t = 10, value_parser = parse_positive_usize)]
    pub candidate_servers: usize,

    /// Number of latency samples per candidate server
    #[arg(long, default_value_t = 3, value_parser = parse_positive_usize)]
    pub latency_samples: usize,

    /// Parallel download workers
    #[arg(long, default_value_t = 8, value_parser = parse_positive_usize)]
    pub download_connections: usize,

    /// Parallel upload workers
    #[arg(long, default_value_t = 6, value_parser = parse_positive_usize)]
    pub upload_connections: usize,

    /// Download phase duration in seconds
    #[arg(long, default_value_t = 10, value_parser = parse_positive_u64)]
    pub download_seconds: u64,

    /// Upload phase duration in seconds
    #[arg(long, default_value_t = 10, value_parser = parse_positive_u64)]
    pub upload_seconds: u64,

    /// Skip upload phase
    #[arg(long, conflicts_with = "upload_only")]
    pub download_only: bool,

    /// Skip download phase
    #[arg(long, conflicts_with = "download_only")]
    pub upload_only: bool,

    /// Optional HTTP/HTTPS/SOCKS5 proxy URL
    #[arg(long)]
    pub proxy: Option<String>,

    /// TUI mode
    #[arg(long, default_value = "compact")]
    pub tui: TuiMode,

    /// Emit machine-readable JSON result
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Clone, Args)]
pub struct IperfArgs {
    /// Target iperf host
    #[arg(long)]
    pub host: String,

    /// Target iperf port
    #[arg(long, default_value_t = 5201)]
    pub port: u16,

    /// iperf protocol mode
    #[arg(long, default_value = "tcp")]
    pub protocol: IperfProtocol,

    /// Test duration in seconds
    #[arg(long, default_value_t = 10, value_parser = parse_positive_u64)]
    pub seconds: u64,

    /// Parallel worker streams
    #[arg(long, default_value_t = 1, value_parser = parse_positive_usize)]
    pub parallel: usize,

    /// Optional target bitrate in bits per second (mainly for UDP)
    #[arg(long, value_parser = parse_positive_u64)]
    pub bitrate: Option<u64>,

    /// Optional HTTP/SOCKS5 proxy URL
    #[arg(long)]
    pub proxy: Option<String>,

    /// Skip download direction
    #[arg(long, conflicts_with = "download_only")]
    pub upload_only: bool,

    /// Skip upload direction
    #[arg(long, conflicts_with = "upload_only")]
    pub download_only: bool,

    /// TUI mode
    #[arg(long, default_value = "compact")]
    pub tui: TuiMode,

    /// Emit machine-readable JSON result
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Parser)]
#[command(name = "tunmux-speedtest")]
#[command(version = APP_VERSION)]
#[command(about = "Standalone Speedtest.net CLI/TUI")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run speed test flow
    Run(RunArgs),

    /// Run native iperf benchmark flow
    Iperf(IperfArgs),

    /// Manage local speedtest server cache
    Cache(CacheArgs),

    /// Print implementation plan
    Plan,
}

impl Default for Cli {
    fn default() -> Self {
        Self {
            command: Some(Command::Run(RunArgs {
                server_id: None,
                candidate_servers: 10,
                latency_samples: 3,
                download_connections: 8,
                upload_connections: 6,
                download_seconds: 10,
                upload_seconds: 10,
                download_only: false,
                upload_only: false,
                proxy: None,
                tui: TuiMode::Compact,
                json: false,
            })),
        }
    }
}

fn parse_positive_usize(value: &str) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|_| format!("invalid integer '{value}'"))?;
    if parsed == 0 {
        return Err("value must be greater than zero".to_string());
    }
    Ok(parsed)
}

fn parse_positive_u64(value: &str) -> Result<u64, String> {
    let parsed = value
        .parse::<u64>()
        .map_err(|_| format!("invalid integer '{value}'"))?;
    if parsed == 0 {
        return Err("value must be greater than zero".to_string());
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::{CacheCommand, Cli, Command};

    #[test]
    fn rejects_zero_connection_counts() {
        let parse = Cli::try_parse_from(["tunmux-speedtest", "run", "--download-connections", "0"]);

        assert!(parse.is_err());
    }

    #[test]
    fn rejects_zero_durations() {
        let parse = Cli::try_parse_from(["tunmux-speedtest", "run", "--upload-seconds", "0"]);

        assert!(parse.is_err());
    }

    #[test]
    fn rejects_download_only_and_upload_only_together() {
        let parse = Cli::try_parse_from([
            "tunmux-speedtest",
            "run",
            "--download-only",
            "--upload-only",
        ]);

        assert!(parse.is_err());
    }

    #[test]
    fn defaults_to_run_command() {
        let cli = Cli::default();
        assert!(matches!(cli.command, Some(Command::Run(_))));
    }

    #[test]
    fn rejects_iperf_upload_only_and_download_only_together() {
        let parse = Cli::try_parse_from([
            "tunmux-speedtest",
            "iperf",
            "--host",
            "127.0.0.1",
            "--upload-only",
            "--download-only",
        ]);

        assert!(parse.is_err());
    }

    #[test]
    fn iperf_requires_host() {
        let parse = Cli::try_parse_from(["tunmux-speedtest", "iperf"]);

        assert!(parse.is_err());
    }

    #[test]
    fn iperf_defaults_to_both_directions() {
        let cli = Cli::try_parse_from(["tunmux-speedtest", "iperf", "--host", "127.0.0.1"])
            .expect("iperf should parse");

        let Some(Command::Iperf(args)) = cli.command else {
            panic!("expected iperf command");
        };

        assert!(!args.upload_only);
        assert!(!args.download_only);
        assert_eq!(args.seconds, 10);
        assert_eq!(args.parallel, 1);
    }

    #[test]
    fn parses_cache_show_defaults() {
        let cli = Cli::try_parse_from(["tunmux-speedtest", "cache", "show"])
            .expect("cache show should parse");

        let Some(Command::Cache(cache)) = cli.command else {
            panic!("expected cache command");
        };

        let CacheCommand::Show(show) = cache.command else {
            panic!("expected cache show");
        };

        assert_eq!(show.limit, 20);
        assert!(!show.json);
        assert!(show.search.is_none());
    }

    #[test]
    fn parses_cache_show_search_and_limit() {
        let cli = Cli::try_parse_from([
            "tunmux-speedtest",
            "cache",
            "show",
            "--search",
            "marseille",
            "--limit",
            "7",
            "--json",
        ])
        .expect("cache show flags should parse");

        let Some(Command::Cache(cache)) = cli.command else {
            panic!("expected cache command");
        };

        let CacheCommand::Show(show) = cache.command else {
            panic!("expected cache show");
        };

        assert_eq!(show.limit, 7);
        assert!(show.json);
        assert_eq!(show.search.as_deref(), Some("marseille"));
    }
}
