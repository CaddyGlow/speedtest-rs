# tunmux-speedtest

`tunmux-speedtest` is a standalone Rust CLI/TUI crate for Speedtest.net compatible benchmarking.

It is intentionally separate from the `tunmux` workspace and has no dependency on `tunmux` internals.

## Features

- best server auto selection by measured latency
- configurable parallel download and upload workers
- proxy support: HTTP, HTTPS, SOCKS5, SOCKS5h
- compact TUI and fullscreen TUI modes
- human readable and JSON output

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
cargo run -- run --json
```

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
