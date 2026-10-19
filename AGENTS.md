# AGENTS.md

This repository is the Rust-only Deckr implementation. Use [README.md](README.md)
for setup, crate usage, validation, and releases.

## Scope and source of truth

- Keep Rust implementation, Rust tests, and implementation documentation here.
- The specification is maintained in `kws/deckr`, normally at `../deckr`.
  Protocol changes belong there first; do not add local specification documents,
  schemas, catalogs, fixtures, or vector bundles.
- Do not introduce Python or TypeScript source, tooling, or package metadata.
- Keep controller orchestration, rendering policy, filesystem configuration, and
  device-manager-specific behavior in the components that own them.

## Layout

- Root `Cargo.toml` defines the `deckr` library crate and its features.
- `src/` contains protocol models, runtime helpers, and adapters.
- `tests/` contains standalone Rust integration tests and a separate `conformance`
  target that reads upstream artifacts.
- `spec-revision` pins the upstream spec commit for CI. Use `DECKR_SPEC_DIR` to
  select an alternate checkout when testing local upstream changes.

Preserve the controller-independent public API. Keep generic runtime and storage
utilities independent of hardware and service policy. Gate NATS integration
behind the existing `nats` feature.

## Validation

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked
cargo test --locked --all-features
cargo build --locked --all-features
cargo doc --locked --no-deps --all-features
```

The conformance target requires `../deckr` or `DECKR_SPEC_DIR`. Its fixtures and
vectors must be read from that checkout, never generated or copied into this
repository. Run default-feature tests as well so ordinary development remains
independent of a spec checkout and NATS.

Use Cargo SemVer versions in the root manifest and follow the README release
guidance. Specification versions and releases belong to the upstream repo.
