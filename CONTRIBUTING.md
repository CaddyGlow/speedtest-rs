# Contributing to tunmux-speedtest

Thanks for contributing.

## Goals

- keep the crate standalone
- keep behavior deterministic and observable
- keep code idiomatic and easy to maintain

## Modern Rust idioms expected

- use `Result<T, E>` and `?` for error propagation
- avoid `unwrap()` and `expect()` in non-test code
- use `thiserror` for domain errors and `anyhow` at the CLI boundary
- prefer strong types over ad-hoc strings for parsed values
- prefer borrowing over unnecessary cloning
- keep modules focused and shallow
- add `#[must_use]` where ignored return values would be bugs
- keep async logic timeout bounded and cancellation aware

## Style and tooling

- format with `rustfmt`
- keep `clippy` warnings at zero
- write clear names and small functions
- add comments only where intent is non-obvious

Required local checks before opening a PR:

```bash
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all
```

## Testing expectations

- add tests for parser logic and throughput math
- add tests for server selection behavior
- add tests for CLI argument validation
- keep tests fast and deterministic

## Pull request checklist

- explain why the change is needed
- explain tradeoffs in implementation choices
- include test evidence
- update docs when behavior or flags change

## Commit guidance

Prefer concise intent based commit messages, for example:

- `feat: add latency-based best server selection`
- `feat: add fullscreen ratatui dashboard scaffold`
- `fix: reject invalid proxy schemes`
- `docs: document benchmark flow and milestones`

## Security and privacy

- do not commit credentials, tokens, or private endpoints
- avoid logging sensitive identifiers unless required

## License

By contributing, you agree contributions are licensed under MIT.
