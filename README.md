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
  feature. The current test suite does not require a running broker.

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
