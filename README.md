# Deckr Rust implementation

This repository contains the `deckr` Rust crate: a reusable, controller-independent
implementation of the Deckr protocols and shared runtime.

The specifications, schemas, catalogs, fixtures, and conformance vectors are
maintained in [kws/deckr](https://github.com/kws/deckr), normally checked out at
`../deckr`. Protocol changes belong there first. This repository contains Rust
implementation code and tests; it does not define or generate the specification.

## Requirements

- Rust 1.88 or newer, with Cargo, rustfmt, and Clippy.
- A checkout of `kws/deckr` for the optional conformance tests.
- A JetStream-enabled NATS server when running an application with the `nats`
  feature. Ordinary tests do not require a running broker; the optional live
  NATS regression test does.

[mise](https://mise.jdx.dev/) users can install the configured toolchain with
`mise install`.

## Using the crate

For a sibling application, add a path dependency:

```toml
[dependencies]
deckr = { path = "../deckr-core-rust" }
```

Enable the optional NATS runtime and JetStream KV adapter with
`features = ["nats"]` on that dependency. The default features provide protocol
models, validation, discovery, agreement, hardware and service helpers, and state
store abstractions without the NATS dependency.

For example, parse an endpoint and derive its direct message subject:

```rust
use deckr::endpoint::EndpointAddress;
use deckr::lanes::direct_subject;

let endpoint = EndpointAddress::parse("controller:main")?;
assert_eq!(
    direct_subject("hardware_messages", &endpoint)?,
    "deckr.msg.hardware_messages.to.controller.main"
);
# Ok::<(), deckr::Error>(())
```

### Runtime lifetimes and state views

Materialized stores install complete snapshots and consume ordered observations.
Exact mutations wait for their committed revision to arrive through the watch;
they never inject state into the cache. Custom `StateStore` adapters used with
materialized views must implement `open_source` with an atomic snapshot/watch
boundary and `delete_revision` with the committed deletion revision. Source gaps
must be reported even if transport recovery succeeds immediately.

Prefer `MaterializedStateStore::watch_snapshots`, `Beacon::watch_state`, and
materialized `ConcordCoordinator::watch_state` for public state subscriptions.
Each item carries complete filtered membership, a version and currentness.
Changes coalesce for slow readers; more than 256 changed identities sets
`resnapshot_required`. Registration 257 fails explicitly. The older event APIs
remain compatibility adapters over these snapshots.

Concord participant handles permanently lose authority after required renewal
failure, token expiry, or loss of source continuity. Rebuilt views do not restore
those handles. Recovery requires a new contract. Hardware routing checks current
local authority at dispatch, and `HardwareManagerRuntime::stop` cancels its owned
workers, bounds remote cleanup to five seconds, and closes its sources. The
caller can drain the supplied `JoinSet` after stopping; it need not abort workers.

### API migration notes

- Beacon and Concord accept protocol-neutral endpoint families. Use
  `EndpointAddress::parse_protocol` for these identities; `parse` retains Deckr's
  endpoint restrictions for Deckr messages and profiles.
- Service-use agreements are termless. Replace the old service terms, terms-hash,
  and scope-index helpers with `service_use_contract`. It returns a fresh contract
  intent with both endpoint sessions. Keep namespace, operation, and view
  permissions in local `ServiceAccess`, constructed by `service_access`.
- Ordinary NATS startup opens the three participation buckets. Maintenance is an
  optional capability obtained explicitly with
  `runtime.nats().concord_maintenance().await?`; it is no longer a `RuntimeBucket`
  variant or a startup readiness dependency.

## Repository layout

```text
Cargo.toml             Crate metadata, features, and dependencies
Cargo.lock             Locked dependencies
src/
  lib.rs               Public module exports
  endpoint.rs          Endpoint identities
  lanes.rs             Message models, validation, and subject/header helpers
  beacon.rs            Discovery implementation
  concord.rs           Agreement implementation
  authority.rs         Contract pointers
  canonical_json.rs    Canonical serialization and terms hashing
  keys.rs              Storage key encoding
  state.rs             State stores, watches, and materialized views
  hardware.rs          Hardware manager runtime
  profiles/            Hardware discovery and claim models
  services.rs          Service discovery and selection
  nats.rs              Optional NATS runtime and JetStream KV adapter
tests/                 Rust integration and upstream conformance tests
spec-revision          Upstream spec commit used by CI
.github/workflows/     Rust validation
```

Keep reusable implementation here. Controller orchestration, rendering policy,
controller configuration, and device-manager-specific behavior belong in the
components that own them, such as
[deckr-controller](https://github.com/kws/deckr-controller).

## Development

Run standalone checks from this repository's root:

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets --features nats -- -D warnings
cargo test --locked
cargo test --locked --features nats
cargo build --locked --features nats
cargo doc --locked --no-deps --features nats
```

These commands need only this repository and its Cargo dependencies. The `nats`
feature is compiled and tested without opening a broker connection.

To exercise consumer cleanup, source recovery, and optional maintenance against
a disposable JetStream broker, run:

```bash
DECKR_NATS_TEST_URL=nats://127.0.0.1:4222 cargo test --locked --features nats --test nats_runtime -- --ignored
```

The test uses isolated bucket names and deletes its buckets after success.

## Upstream conformance tests

The `conformance` feature enables a separate test target that reads the upstream
fixtures and vectors at runtime. It does not add a runtime API or dependency.
The default spec checkout is `../deckr`; set `DECKR_SPEC_DIR` to use another
checkout. The variable names the repository root, which contains `contract/v1/`.
Missing or invalid artifacts fail the tests with the path that could not be read.

```bash
git clone https://github.com/kws/deckr.git ../deckr
git -C ../deckr checkout "$(cat spec-revision)"
cargo test --locked --all-features
```

For an existing checkout elsewhere:

```bash
DECKR_SPEC_DIR=/path/to/deckr cargo test --locked --features conformance --test conformance
```

CI checks out the commit recorded in `spec-revision` and runs both standalone and
conformance checks. When updating to a new spec revision, review the upstream
changes, update the commit in that file, and run the full checks against that
checkout. The upstream commit must be available in `kws/deckr` before CI can fetch
it. Passing the current test suite covers the implemented cases; it does not
claim complete protocol conformance.

## Releases

The crate version is owned by the root `Cargo.toml`. Keep `Cargo.lock` updated
when changing the version or dependencies. Run the checks above and the upstream
conformance tests before a release, and verify the distributable with:

```bash
cargo package --locked
```

Release artifacts contain the Rust library and its implementation documentation.
Specification releases are managed separately in `kws/deckr`.
