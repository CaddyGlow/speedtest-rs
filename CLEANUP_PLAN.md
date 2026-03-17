# Cleanup Plan

## Status

Repository review completed on March 17, 2026.

- `cargo check --all-targets` passes
- `cargo clippy` could not be run in this environment because `clippy` is not installed

## Primary Findings

### 1. Download and upload stage orchestration are heavily duplicated

The largest cleanup opportunity is shared logic across:

- `src/speedtest/download.rs`
- `src/speedtest/upload.rs`

Both modules repeat the same high-level flow:

- readiness probing
- worker spawning
- adaptive worker scaling
- progress polling
- graceful task shutdown
- throughput finalization

This is the highest-value simplification target.

### 2. Engine stage handling duplicates per-direction logic

`src/speedtest/engine.rs` contains nearly identical download and upload stage bodies:

- transfer config construction
- loaded-latency task wiring
- interval capture
- MST projection
- `RunResult` mutation
- event emission

The engine should delegate stage-specific work to smaller helpers instead of owning both large paths inline.

### 3. Browser/websocket protocol code has repeated handshake and fallback logic

`src/speedtest/browser_protocol.rs` repeats:

- websocket request creation
- common headers
- connect + handshake
- endpoint fallback loops
- text frame send/receive patterns

This should be collapsed behind a websocket session helper.

### 4. `RunResult` mixes public output with internal SDK build scratch data

`src/model.rs` currently stores both user-facing result fields and several internal `#[serde(skip)]` SDK-only artifacts.

That makes the output model do too much and weakens module boundaries.

### 5. `sdk_payload.rs` is too broad

`src/speedtest/sdk_payload.rs` currently handles:

- GUID generation
- payload assembly
- latency statistics
- throughput shaping
- server selection shaping
- hashing
- file writing

This file should be split by concern.

### 6. `runner.rs` still owns too many responsibilities

`src/runner.rs` currently mixes:

- command dispatch
- UI coordination
- engine event interpretation
- speedtest result output
- iperf result assembly

This is still manageable, but it is already larger than it should be.

### 7. CLI defaults and validation are more manual than necessary

`src/cli.rs` has a lot of boilerplate:

- manual `Default` construction
- duplicated positive integer parsing helpers
- repeated default constants embedded directly in field declarations

This is lower risk cleanup, but worth doing once structural refactors are complete.

### 8. Integration test helpers are embedded directly in the test file

`tests/iperf_interop.rs` includes substantial proxy server and protocol helper code inline.

That works, but it makes the test scenarios harder to scan and maintain.

## Recommended Refactor Plan

### Phase 1: Remove the largest duplication first

1. Extract a shared transfer coordinator used by both download and upload.
2. Keep direction-specific behavior behind small callbacks or typed strategy structs.
3. Reuse one lifecycle for:
   - readiness selection
   - worker management
   - adaptive scaling
   - progress emission
   - shutdown and finalization

Expected result:

- much smaller `download.rs`
- much smaller `upload.rs`
- less behavioral drift between stages

### Phase 2: Split engine stage bodies into helpers

1. Extract `run_download_stage` and `run_upload_stage` from `src/speedtest/engine.rs`.
2. Extract helpers for:
   - interval collection
   - latency averaging
   - `BenchmarkResult` construction
   - MST detail conversion
   - `RunDetails` mutation
3. Keep `run_speedtest_engine` focused on stage sequencing and event flow.

Expected result:

- improved readability
- easier testing of stage behavior
- fewer long mutable blocks

### Phase 3: Introduce a proper internal measurement context

1. Remove SDK-only scratch fields from `RunResult`.
2. Introduce an internal struct such as:
   - `SdkBuildContext`
   - or `RawMeasurementArtifacts`
3. Pass that internal context to SDK payload generation instead of storing it on the public result model.

Expected result:

- better separation between output schema and internal processing state
- more idiomatic Rust data modeling

### Phase 4: Split `sdk_payload.rs` by concern

Suggested module split:

- `speedtest/sdk/guid.rs`
- `speedtest/sdk/latency.rs`
- `speedtest/sdk/throughput_profile.rs`
- `speedtest/sdk/server_selection.rs`
- `speedtest/sdk/payload.rs`

Also review whether throughput math can reuse more of `src/speedtest/throughput.rs` instead of maintaining similar logic in two places.

Expected result:

- smaller files
- clearer ownership
- easier targeted tests

### Phase 5: Build a websocket/browser helper layer

Extract reusable helpers from `src/speedtest/browser_protocol.rs` for:

- websocket request creation
- standard headers
- connect with timeout
- Speedtest handshake
- text send/receive
- endpoint fallback

Expected result:

- less copy-paste
- easier protocol changes
- lower risk of inconsistent behavior across websocket paths

### Phase 6: Reduce `runner.rs` to orchestration only

1. Keep top-level command dispatch in `runner.rs`.
2. Move speedtest presentation/event handling into a dedicated presenter/controller.
3. Move iperf result assembly closer to the iperf module.

Expected result:

- narrower module responsibilities
- easier testing of output behavior separate from execution

### Phase 7: Low-risk idiomatic cleanup

After the structural work:

1. Simplify CLI default handling in `src/cli.rs`.
2. Collapse duplicated positive-integer parsers into one generic helper if it remains worthwhile.
3. Centralize proxy parsing/validation policy where practical.
4. Review public structs for builder/helper constructors where that removes repetitive assembly code.

### Phase 8: Extract integration test support helpers

Move proxy server support code from `tests/iperf_interop.rs` into `tests/support/`.

Expected result:

- shorter tests
- clearer intent
- reusable integration helpers

## Priority Order

### Highest priority

- shared transfer coordinator
- engine stage extraction
- websocket helper extraction

### Medium priority

- `RunResult` and SDK boundary cleanup
- split `sdk_payload.rs`

### Lower priority

- `runner.rs` slimming
- CLI cleanup
- integration test helper extraction

## Safe Execution Order

To minimize risk and keep the crate compiling after each step:

1. Extract small pure helpers from `engine.rs`.
2. Extract websocket request/handshake helpers from `browser_protocol.rs`.
3. Introduce a shared transfer coordinator without changing external behavior.
4. Migrate `download.rs` to the coordinator.
5. Migrate `upload.rs` to the coordinator.
6. Introduce internal SDK build context.
7. Remove SDK scratch fields from `RunResult`.
8. Split `sdk_payload.rs`.
9. Slim `runner.rs`.
10. Clean up CLI and tests.

## Notes

- There is an unrelated modified `flake.nix` in the worktree and it should be left alone.
- No code changes were made as part of this review.
