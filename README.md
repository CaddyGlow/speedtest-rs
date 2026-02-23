# tunmux-speedtest

`tunmux-speedtest` is a standalone Rust CLI/TUI crate for Speedtest.net compatible benchmarking.

It is intentionally separate from the `tunmux` workspace and has no dependency on `tunmux` internals.

## Features

- best server auto selection by measured latency
- configurable parallel download and upload workers
- proxy support: HTTP, HTTPS, SOCKS5, SOCKS5h
- multiple speedtest API modes: `legacy` and `modern` (browser SDK)
- modern transport switch: `--modern-mode xhr|tcp` (same modern SDK server discovery, transfer differs)
- modern pooled transfer across nearest measured servers (`--modern-pool-size`)
- modern session cache for cookies, guid, and client auth token
- native `iperf` command with dedicated JSON schema (`tunmux.iperf.v1`)
- compact TUI and fullscreen TUI modes
- human readable output plus SDK-compatible JSON output

## Status

The crate now implements end-to-end runtime flow:

- speedtest config and server catalog fetching
- latency-based server selection
- concurrent download and upload benchmarking
- compact live progress bars and fullscreen ratatui dashboard (with fallback to compact)

Remaining hardening and polish work is tracked in `PLAN.md`.

## Build

```bash
cargo build
```

## Usage

Run with defaults:

```bash
cargo run -- run
```

Show the implementation plan:

```bash
cargo run -- plan
```

Example runtime flags:

```bash
cargo run -- run --tui compact
cargo run -- run --tui fullscreen
cargo run -- run --proxy socks5h://127.0.0.1:1080
cargo run -- run --download-connections 8 --upload-connections 6
cargo run -- run --speedtest-api auto
cargo run -- run --speedtest-api legacy
cargo run -- run --speedtest-api modern
cargo run -- run --speedtest-api modern --modern-mode xhr
cargo run -- run --speedtest-api modern --modern-mode tcp
cargo run -- run --speedtest-api modern --modern-pool-size 4
cargo run -- run --speedtest-api modern-tcp
cargo run -- run --json
cargo run -- run --json --details
cargo run -- run --sdk-json-out speedtest-sdk-result.json
RUST_LOG=debug cargo run -- run --speedtest-api modern --modern-mode tcp --modern-pool-size 8 --
```

Modern session cache path:

- `run --json` emits a Speedtest SDK-style payload (`st4-js` model)
- `run --json --details` emits the native `RunResult` diagnostic schema with interval counters

- `${XDG_CACHE_HOME}/tunmux-speedtest/modern-session.json` when `XDG_CACHE_HOME` is set
- `~/.cache/tunmux-speedtest/modern-session.json` otherwise

Native iperf command examples:

```bash
cargo run -- iperf --host 127.0.0.1
cargo run -- iperf --host 127.0.0.1 --protocol tcp --upload-only
cargo run -- iperf --host 127.0.0.1 --protocol udp --proxy socks5://127.0.0.1:1080
cargo run -- iperf --host 127.0.0.1 --protocol tcp --proxy http://127.0.0.1:8080 --download-only
cargo run -- iperf --host 127.0.0.1 --json
cargo run -- iperf --host 127.0.0.1 --json --details
cargo run -- iperf --auto-server
cargo run -- iperf --auto-server --servers-file iperf3_servers.json --candidate-servers 12 --latency-samples 2
```

Iperf auto-selection mode:

- `--auto-server` picks the closest reachable host by measured TCP control-channel latency
- server list is read from `iperf3_servers.json` by default
- `--candidate-servers` limits how many list entries are probed
- `--latency-samples` controls probe samples per candidate
- optional `--port` overrides catalog ports during selection

Iperf proxy matrix:

- TCP: direct, HTTP proxy, SOCKS5/SOCKS5h proxy
- UDP: direct, SOCKS5/SOCKS5h proxy
- UDP over HTTP is rejected
- HTTPS proxy is currently rejected for the native iperf command

Cache helpers:

```bash
cargo run -- cache path
cargo run -- cache show
cargo run -- cache show --search marseille --limit 20
cargo run -- cache show --json
cargo run -- cache clear
```

## Project Structure

- `PLAN.md` - roadmap and milestone detail
- `CONTRIBUTING.md` - contribution workflow and Rust idioms
- `src/cli.rs` - clap arguments and validation surface
- `src/runner.rs` - execution orchestration entry
- `src/speedtest/` - benchmark and server selection modules
- `src/ui/` - compact and fullscreen TUI modules

## Development

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all
```

## License

MIT. See `LICENSE`.
