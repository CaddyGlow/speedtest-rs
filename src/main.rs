mod cli;
mod error;
mod http;
mod iperf;
mod model;
mod output;
mod runner;
mod speedtest;
mod ui;
mod util;

use clap::Parser;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = cli::Cli::parse();
    runner::run(cli).await
}
