use clap::{ArgGroup, Args, Parser, Subcommand, ValueEnum};

use crate::speedtest::api::ModernTransportMode;

const APP_VERSION: &str = env!("TUNMUX_SPEEDTEST_VERSION");

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
    /// Speedtest transfer transport mode
    #[arg(long = "mode", default_value = "xhr")]
    pub mode: ModernTransportMode,

    /// Optional server id override
    #[arg(long)]
    pub server_id: Option<u64>,

    /// Number of candidate servers to probe for latency
    #[arg(long, default_value_t = 10, value_parser = parse_positive_usize)]
    pub candidate_servers: usize,

    /// Number of servers to include in transfer pool
    #[arg(long = "pool-size", default_value_t = 4, value_parser = parse_positive_usize)]
    pub pool_size: usize,

    /// Number of latency samples per candidate server
    #[arg(long, default_value_t = 10, value_parser = parse_positive_usize)]
    pub latency_samples: usize,

    /// Parallel download workers
    #[arg(long, default_value_t = 8, value_parser = parse_positive_usize)]
    pub download_connections: usize,

    /// Parallel upload workers
    #[arg(long, default_value_t = 8, value_parser = parse_positive_usize)]
    pub upload_connections: usize,

    /// Download phase duration in seconds
    #[arg(long, default_value_t = 15, value_parser = parse_positive_u64)]
    pub download_seconds: u64,

    /// Upload phase duration in seconds
    #[arg(long, default_value_t = 15, value_parser = parse_positive_u64)]
    pub upload_seconds: u64,

    /// Minimum seconds before early exit is allowed (0 disables early exit)
    #[arg(long, default_value_t = 5)]
    pub min_seconds: u64,

    /// Skip upload phase
    #[arg(long, conflicts_with = "upload_only")]
    pub download_only: bool,

    /// Skip download phase
    #[arg(long, conflicts_with = "download_only")]
    pub upload_only: bool,

    /// Optional HTTP/HTTPS/SOCKS5 proxy URL (falls back to http_proxy/https_proxy/all_proxy env vars)
    ///
    /// `--proxy-local` is kept as an alias for compatibility.
    #[arg(long, alias = "proxy-local")]
    pub proxy: Option<String>,

    /// Disable live progress rendering
    #[arg(long)]
    pub no_progress: bool,

    /// Emit machine-readable JSON result
    #[arg(long)]
    pub json: bool,

    /// Include MST algorithm diagnostics in JSON output
    #[arg(long, requires = "json")]
    pub details: bool,

    /// Write SDK-compatible result JSON payload to file
    #[arg(long, value_name = "PATH")]
    pub sdk_json_out: Option<String>,
}

#[derive(Debug, Clone, Args)]
#[command(group(
    ArgGroup::new("iperf_target")
        .required(true)
        .args(["host", "auto_server"])
))]
pub struct IperfArgs {
    /// Target iperf host
    #[arg(long)]
    pub host: Option<String>,

    /// Pick the closest host from iperf3_servers.json by measured latency
    #[arg(long, conflicts_with = "host")]
    pub auto_server: bool,

    /// Path to iperf server list JSON (used with --auto-server)
    #[arg(long, default_value = "iperf3_servers.json")]
    pub servers_file: String,

    /// Target iperf port
    #[arg(long)]
    pub port: Option<u16>,

    /// Number of auto-selected candidates to probe
    #[arg(long, default_value_t = 10, value_parser = parse_positive_usize)]
    pub candidate_servers: usize,

    /// Latency samples per auto-selected candidate
    #[arg(long, default_value_t = 2, value_parser = parse_positive_usize)]
    pub latency_samples: usize,

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

    /// Optional HTTP/SOCKS5 proxy URL (falls back to http_proxy/https_proxy/all_proxy env vars)
    ///
    /// `--proxy-local` is kept as an alias for compatibility.
    #[arg(long, alias = "proxy-local")]
    pub proxy: Option<String>,

    /// Skip download direction
    #[arg(long, conflicts_with = "download_only")]
    pub upload_only: bool,

    /// Skip upload direction
    #[arg(long, conflicts_with = "upload_only")]
    pub download_only: bool,

    /// Disable live progress rendering
    #[arg(long)]
    pub no_progress: bool,

    /// Emit machine-readable JSON result
    #[arg(long)]
    pub json: bool,

    /// Include interval and diagnostic details in JSON output
    #[arg(long, requires = "json")]
    pub details: bool,
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
}

impl Default for Cli {
    fn default() -> Self {
        Self {
            command: Some(Command::Run(RunArgs {
                mode: ModernTransportMode::Xhr,
                server_id: None,
                candidate_servers: 10,
                pool_size: 4,
                latency_samples: 10,
                download_connections: 8,
                upload_connections: 8,
                download_seconds: 15,
                upload_seconds: 15,
                min_seconds: 5,
                download_only: false,
                upload_only: false,
                proxy: None,
                no_progress: false,
                json: false,
                details: false,
                sdk_json_out: None,
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
    fn parses_modern_transport_mode() {
        let cli = Cli::try_parse_from(["tunmux-speedtest", "run", "--mode", "tcp"])
            .expect("run --mode tcp should parse");

        let Some(Command::Run(args)) = cli.command else {
            panic!("expected run command");
        };

        assert!(matches!(args.mode, super::ModernTransportMode::Tcp));
    }

    #[test]
    fn parses_sdk_json_out_flag() {
        let cli = Cli::try_parse_from([
            "tunmux-speedtest",
            "run",
            "--sdk-json-out",
            "sdk-result.json",
        ])
        .expect("run --sdk-json-out should parse");

        let Some(Command::Run(args)) = cli.command else {
            panic!("expected run command");
        };

        assert_eq!(args.sdk_json_out.as_deref(), Some("sdk-result.json"));
    }

    #[test]
    fn parses_run_proxy_local_alias() {
        let cli = Cli::try_parse_from([
            "tunmux-speedtest",
            "run",
            "--proxy-local",
            "http://127.0.0.1:8080",
        ])
        .expect("run --proxy-local should parse");

        let Some(Command::Run(args)) = cli.command else {
            panic!("expected run command");
        };

        assert_eq!(args.proxy.as_deref(), Some("http://127.0.0.1:8080"));
    }

    #[test]
    fn defaults_to_run_command() {
        let cli = Cli::default();
        let Some(Command::Run(args)) = cli.command else {
            panic!("expected run command");
        };

        assert!(matches!(args.mode, super::ModernTransportMode::Xhr));
        assert_eq!(args.pool_size, 4);
        assert_eq!(args.min_seconds, 5);
        assert!(!args.details);
        assert!(!args.no_progress);
    }

    #[test]
    fn parses_run_min_seconds() {
        let cli = Cli::try_parse_from(["tunmux-speedtest", "run", "--min-seconds", "3"])
            .expect("run --min-seconds should parse");

        let Some(Command::Run(args)) = cli.command else {
            panic!("expected run command");
        };

        assert_eq!(args.min_seconds, 3);
    }

    #[test]
    fn accepts_run_details_with_json() {
        let cli = Cli::try_parse_from(["tunmux-speedtest", "run", "--json", "--details"])
            .expect("run --json --details should parse");

        let Some(Command::Run(args)) = cli.command else {
            panic!("expected run command");
        };

        assert!(args.json);
        assert!(args.details);
    }

    #[test]
    fn rejects_run_details_without_json() {
        let parse = Cli::try_parse_from(["tunmux-speedtest", "run", "--details"]);

        assert!(parse.is_err());
    }

    #[test]
    fn parses_run_no_progress_flag() {
        let cli = Cli::try_parse_from(["tunmux-speedtest", "run", "--no-progress"])
            .expect("run --no-progress should parse");

        let Some(Command::Run(args)) = cli.command else {
            panic!("expected run command");
        };

        assert!(args.no_progress);
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
    fn rejects_iperf_details_without_json() {
        let parse = Cli::try_parse_from([
            "tunmux-speedtest",
            "iperf",
            "--host",
            "127.0.0.1",
            "--details",
        ]);

        assert!(parse.is_err());
    }

    #[test]
    fn accepts_iperf_details_with_json() {
        let cli = Cli::try_parse_from([
            "tunmux-speedtest",
            "iperf",
            "--host",
            "127.0.0.1",
            "--json",
            "--details",
        ])
        .expect("iperf --json --details should parse");

        let Some(Command::Iperf(args)) = cli.command else {
            panic!("expected iperf command");
        };

        assert!(args.json);
        assert!(args.details);
    }

    #[test]
    fn iperf_requires_host() {
        let parse = Cli::try_parse_from(["tunmux-speedtest", "iperf"]);

        assert!(parse.is_err());
    }

    #[test]
    fn iperf_accepts_auto_server_without_host() {
        let cli = Cli::try_parse_from(["tunmux-speedtest", "iperf", "--auto-server"])
            .expect("iperf auto-server should parse");

        let Some(Command::Iperf(args)) = cli.command else {
            panic!("expected iperf command");
        };

        assert!(args.auto_server);
        assert!(args.host.is_none());
        assert_eq!(args.servers_file, "iperf3_servers.json");
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
        assert_eq!(args.host.as_deref(), Some("127.0.0.1"));
        assert!(!args.auto_server);
        assert_eq!(args.seconds, 10);
        assert_eq!(args.parallel, 1);
        assert!(!args.no_progress);
    }

    #[test]
    fn parses_iperf_no_progress_flag() {
        let cli = Cli::try_parse_from([
            "tunmux-speedtest",
            "iperf",
            "--host",
            "127.0.0.1",
            "--no-progress",
        ])
        .expect("iperf --no-progress should parse");

        let Some(Command::Iperf(args)) = cli.command else {
            panic!("expected iperf command");
        };

        assert!(args.no_progress);
    }

    #[test]
    fn parses_iperf_proxy_local_alias() {
        let cli = Cli::try_parse_from([
            "tunmux-speedtest",
            "iperf",
            "--host",
            "127.0.0.1",
            "--proxy-local",
            "socks5h://127.0.0.1:1080",
        ])
        .expect("iperf --proxy-local should parse");

        let Some(Command::Iperf(args)) = cli.command else {
            panic!("expected iperf command");
        };

        assert_eq!(args.proxy.as_deref(), Some("socks5h://127.0.0.1:1080"));
    }

    #[test]
    fn rejects_iperf_host_and_auto_server_together() {
        let parse = Cli::try_parse_from([
            "tunmux-speedtest",
            "iperf",
            "--host",
            "127.0.0.1",
            "--auto-server",
        ]);

        assert!(parse.is_err());
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
