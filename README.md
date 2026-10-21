# Deckr Rust implementation

This repository contains the `deckr` Rust crate: a reusable, controller-independent
implementation of the Deckr protocols and shared runtime.

The specifications, schemas, catalogs, fixtures, and conformance vectors are
maintained in [kws/deckr](https://github.com/kws/deckr), normally checked out at
`../deckr`. Protocol changes belong there first. This repository contains Rust
implementation code and tests; it does not define or generate the specification.

## Place in the project

The [project repository map](https://github.com/kws/deckr#project-repositories)
describes the reference, Python/Rust/TypeScript cores, and integration harness.
This repository owns the Rust API and its implementation tests.
Applications own controller routing, rendering, configuration policy, device
drivers, and action or integration behavior.

The cores use shared contracts with language-specific APIs and independent
package versions and coverage.
[deckr-integration-tests](https://github.com/kws/deckr-integration-tests)
owns the real-broker cross-language workloads and their recorded evidence.
Use the [schema guide](https://github.com/kws/deckr/blob/main/docs/schema-guide.md)
for reference artifacts and the
[architecture proposal](https://github.com/kws/deckr/blob/main/docs/deckr-architecture.md)
for informative component boundaries.

## Action Runtime contract support

`action_runtime` implements page targets, page-session generations, and named
capability matches from the alpha contract at the Git revision in
[spec-revision](spec-revision). `DynamicPageBody`
carries a `PageCommandTarget`; `ActionMessage::ClosePage` carries
`ClosePageBody`. Replacement and closure require a page-session target.
`ActionMessage::from_envelope` requires the runtime service subject.

`match_capability_requirements` and `validate_binding_resources_scoped` accept
`CapabilityMatchingScope` observations for a negotiated device and control.
They enforce preference order, required/optional matching, views, provenance,
complete supported name sets, and exact projection sources. The existing
`validate_binding_resources` uses the entire negotiated control; use the scoped
variant when application configuration restricts candidates. No requirements
means no matches, and every result needs `requirement_name`.

Pass the same configured capability IDs to `validate_binding_output_scoped`
and `validate_capability_input_scoped` when checking later messages. This keeps
preference selection within the configured scope. Their unscoped counterparts
use the entire negotiated control. `None` means unrestricted; an empty slice
permits no capabilities. Both variants retain identity, generation, and payload
checks, and sending output still needs a current hardware claim.

`validate_page_command` checks a `PageCommandContext` and returns the retained
target. The application supplies current observations and repeats the check
while committing a serialized transition. The helper neither allocates pages
nor acquires authority. The conformance target executes all upstream Action
Runtime semantic cases through these public helpers.

Hardware candidates already expose manager labels and managed events deliver
authorized command rejections with correlation. `ServiceClientState.contract`
now exposes the observed agreement identity. Operations still check current
authority; retaining a state snapshot does not extend an agreement.

Rendering models include `RenderingSupport`, `BindingRenderBody`, and
`BindingRenderResultBody`. `ActionMessage` and `ActionConnection` carry render
commands and results through the managed service boundary, with request
correlation in `causation_id`. `DeckrMessage::wire_size_bytes` retains the received
UTF-8 envelope size; that observation is not serialized as a wire field.

`validate_rendering_support` checks named raster matches and capability aliases.
`validate_render_command` accepts current `RenderCommandContext` observations and
returns the retained binding and target or a stable `RenderError` code. The owner
must recheck authority and output state at final dispatch. Results can describe
stale or detached targets and do not grant output authority. Rendering fixtures,
admission vectors, and managed command/result delivery are covered by conformance
tests. Image decoding, rendering workers, and output ordering belong to controllers.

## Requirements

- Rust 1.88 or newer, with Cargo, rustfmt, and Clippy.
- A checkout of `kws/deckr` for the optional conformance tests.
- A JetStream-enabled NATS server when running an application with the `nats`
  feature. Ordinary tests do not require a running broker; the optional live
  NATS regression tests do.

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
`resnapshot_required`. Registration 257 fails explicitly. Cached event APIs are
compatibility adapters over snapshots. Explicit exact-store watches remain
low-level operations with a caller-owned lifetime.

Concord queries, validation and filtered watches share one lazy index and one
subscription per contract/token source. Filters start with the smallest indexed
candidate set; unrelated records are not parsed or evaluated. Validity is cached
until a relevant observation changes, including one recent session-assertion set
per selected contract. `view_stats` exposes index, cache, evaluation and subscriber
counts. Malformed selected observations remain visible as invalid states.
Cached compatibility notifications share this view and report membership removal
when a record leaves their filter. A removal carries its last observed revision;
guarded mutations still read exact state before acting.

Concord participant handles permanently lose authority after required renewal
failure, token expiry, or loss of source continuity. Rebuilt views do not restore
those handles. Recovery requires a new contract. Hardware routing checks current
local authority at dispatch, and `HardwareManagerRuntime::stop` cancels its owned
workers, bounds remote cleanup to five seconds, and closes its sources. The
caller can drain the supplied `JoinSet` after stopping; it need not abort workers.

Hosts can share Beacon and Concord views through `runtime::ProtocolRuntime`.
`HardwareManagerRuntime::with_concord` borrows the host's Concord sources; stopping
that manager leaves shared sources open. The NATS convenience constructors use
one lazy view per protocol and connection. Close dependent components before
calling `ProtocolRuntime::close` or `NatsDeckrRuntime::close_sources`.

Hardware device changes revoke local claims before remote cleanup. Cleanup
attempts cancellation and token withdrawal independently within five seconds.
Discovery failures and invalid incoming messages are available through
`HardwareManagerRuntime::diagnostics` and do not terminate healthy claims.
The manager completes exclusive Concord bootstrap and subscribes to its lane
before publishing. Managed Beacon and per-agreement Concord workers schedule
from store TTLs; the retained `maintenance_policy` field no longer controls
these timers, and no periodic claim-repair loop runs. Device callbacks cannot
block token renewal. `state`, `watch_state`, and `wait_for` expose complete
immutable inventory and lifecycle snapshots, including separate discovery and
participation readiness. `stop` is shared across callers and retains cleanup
failures in closed state even when an earlier device operation reported them.

Hardware descriptors preserve connections, sources, projections, units, control
relationships and every capability collection. `ValidatedDevice` compiles
descriptor schemas and validates capability lookup, operation support, values,
constraints, and core raster image bytes and dimensions. Numeric step and scaled
range checks retain decimal precision; exponent expansion is limited to 10,000
places. The manager performs schema and image work off the async scheduler and
checks current authority again before dispatch. Drivers doing asynchronous work
implement `handle_hardware_command` and call the supplied
`HardwareCommandContext::authorize` after waits. Its descriptor and agreement
stay fixed; device replacement or claim termination invalidates that context.

Use `HardwareCommandContext::reply(body)` for command and capability-state
responses. It retains the request's return route and correlation, validates the
response against the selected device and operation, and rechecks authority after
payload validation. Replies use the manager's own session identity. Generated
rejections use the same routing and correlation fields. `HardwareTransport`
adapts any `messaging::MessageTransport` for a hardware manager; existing
`HardwareLaneTransport` implementations can retain ordinary lane delivery, or
override the delivery/reply methods to support inbox routes.

`schema::SchemaRegistry` compiles embedded JSON Schemas without automatic file
or network retrieval. Supply referenced resources with `with_resource`, then
use `HardwareManagerRuntime::new_with_schemas` or `with_concord_and_schemas`.
Standard capability builders live in `deckr::hardware::capabilities`: button
activation/momentary, encoder relative, touch gesture, raster bitmap, and device
power. Hardware managers supply device dimensions and constraints rather than
maintaining local copies of those schemas.

Known pure references to core hardware payload schemas use the corresponding
Rust validators; compound and extension references use the host's registry.
No upstream schema bundle is copied into this crate.

The [implementation coverage map](docs/implementation-coverage.md) maps the
upstream informative architecture to reusable APIs, ownership boundaries and
validation evidence.

`hardware::client::HardwareClient::start` borrows shared Beacon, Concord and
messaging lifetimes. Its asynchronous `HardwareSelectionPolicy` chooses a
manager candidate and device instance counts. The client validates the selected
descriptors, negotiates a fresh claim, and exposes an immutable
`HardwareClientContext` through `wait_ready` or `context`. Use that context's
`send` and `request` for commands and state requests; it supplies the exact
pointer and participant sessions and validates operations and replies against
the claimed descriptors. `watch_events` returns validated, scoped hardware
events. Event subscribers have bounded queues and report overflow explicitly.

Discovery loss does not end a valid claim. Permanent authority loss cancels
pending requests and the client may negotiate a fresh successor; retained
contexts and events never gain the successor's authority. Published commands
are not replayed. Closing or dropping the last client handle revokes its
authority and initiates bounded cleanup without closing borrowed host resources.
Observe discovery, selected devices, agreement state and failures through
`state`, `watch_state`, and `wait_for`. Hardware lifetimes use Concord's injected
clock; supply the same clock to `MessageSession::with_options` when overriding
the default system clock.

`BeaconAdvertiser::manage` starts an independent advertisement lifetime. Its
worker derives heartbeat timing from the store TTL, publishes content updates,
recovers a missing owned key, and withdraws with owner and revision checks.
Use `wait_ready`, `update`, `watch_state`, and `close` on the returned handle.
Dropping its last handle also initiates bounded cleanup. State watches retain
interrupted currentness across recovery and reject registration 257 explicitly.
Predicate waits distinguish `Error::Deadline` from closure. Hosts can inject a
`lifecycle::Clock`; tests can advance scheduling without wall-clock sleeps.

`ConcordCoordinator::propose` creates one managed agreement from an
`AgreementIntent` and a neutral `session::EndpointSession`. `participate` runs an
asynchronous batch policy over one filtered subscription. Each selected agreement
has an independent attachment and renewal worker; a blocked selector or store
operation does not stop other held tokens. `AgreementContext::authorize` checks
current cached records, token identity, source continuity, and the local deadline
before protected work. Domain handlers should use that context instead of
retaining a raw participant handle as authority.

The older `ConcordParticipantLease` and `ConcordParticipantManager` are manually
driven compatibility utilities. They do not supply automatic worker supervision
or refresh scheduling; use the managed contexts for application boundaries.
`ConcordParticipantLease::adopt` updates a held token observation without extending
the local heartbeat deadline or reviving an expired lease. A raw stored token or
an earlier reconciliation result is never a substitute for current authorization.

These lifetimes expose `state`, `watch_state`, `wait_for`, and `close`.
`request_close` revokes authority synchronously; cleanup is shielded from a caller
that stops waiting and is bounded to five seconds. `EndpointSession::close` ends
its dependent contexts. Borrowed protocol sources remain owned by the host.
Proposals never create their own successors. Profile owners use `bootstrap` with
exclusive ownership policy when their role requires it; competing live sessions
are not treated as lost ownership.

`concord::evaluate_contract` evaluates fixed `ContractObservations` without I/O.
Cached and exact readers use the same evaluator and return `ValidityReason`
codes, the parsed contract, and tokens with valid key identities. Use
`Error::mutation_code` for guarded mutation outcomes; diagnostic text is not a
retry or authorization rule.

`concord::ConcordMaintenance::new(contracts, tokens, maintenance)` creates an
optional capability over dedicated raw stores. Construction performs no I/O and
starts no scans or watches. NATS hosts obtain it explicitly through
`DeckrRuntime::maintenance`; ordinary agreement participation never opens the
maintenance store. Use `scan(audit)` for one scan, or `manage(audit, interval)`
for a host-owned recurring worker at a low frequency. Managed scans expose state,
watches, predicate waits, explicit scan requests and closure. Closing or dropping
the last managed handle cancels pending work and leaves borrowed stores open.

Maintenance uses exact Concord observations and the shared evaluator. It records
staleness before cancellation, preserves pending contracts with a valid refresh
token, and applies the default 900-second stale grace and 3600-second retention
periods. `MaintenanceOptions` configures these periods and bounds each store/audit
operation to five seconds by default. Every cancellation and deletion uses a
revision guard. Invalid record identities never redirect operations away from
the requested canonical storage key.

The required `MaintenanceAudit` callback records contract metadata and token
summaries before deletion; the default audit event excludes full terms. A durable
cleanup marker fences the contract revision and terms hash before deleting a
retained record. Token cleanup makes at most eight immediate mutation passes and
requires an exact empty rescan plus guarded marker removal to complete. Later
scans resume pending markers independently of contract records, including after
an interrupted or unacknowledged delete. Reports separate listed token counts,
confirmed deletion counts, pending cleanup and bounded failures. Audit callbacks
must tolerate cancellation; their failure prevents the dependent contract delete.

`messaging::MessageSession` binds a host's `MessageTransport` to a neutral
`session::EndpointSession`. It sets sender identity, validates core lanes and
explicitly registered extensions, filters recipients and expiry, and owns
request inboxes and bounded subscription queues. `request` subscribes before
publishing and ignores invalid or unrelated replies. Timeout, cancellation,
endpoint closure and last-handle drop release the owned subscriptions. A queue
that exceeds 256 pending deliveries fails explicitly. Use `watch_state` to
observe active operations and rejected-message diagnostics.

For NATS, use `DeckrRuntime::messaging(session)` or construct
`nats::NatsMessageTransport` from a host-owned `async_nats::Client`. This adapter
requires no JetStream access. It preserves reply inbox routes and validates
subject/header hints, including paired contract headers, against the envelope.
The older `nats::EndpointSession` is a low-level compatibility helper; new managed
components should use the neutral session with `MessageSession`.

The host owns connection configuration and final transport shutdown.
`DeckrRuntimeConfig::reconnect_max_interval` defaults to 60 seconds and
`connection_timeout` to 5 seconds. Initial connection retries indefinitely with
bounded dials and a fixed configured wait; dropping the startup future cancels
those attempts. Established connections retry indefinitely with exponential
windows, half-to-full-window jitter, and the configured cap. A successful
connection resets the window. The runtime records every disconnect/reconnect
epoch for source continuity.
Recovery rebuilds sources but cannot restore terminated agreement authority.
Close managed dependents, then shared sources, then release the transport.

Attachment contention waits for the selected contract's revision to advance;
unrelated contracts and token traffic cannot trigger more CAS attempts. Malformed
JSON is retained as an invalid record observation rather than interrupting the
shared source and revoking unrelated agreements.

The `interop` example is a JSON-lines test driver for the
[native Concord runner](https://github.com/kws/deckr-integration-tests#native-concord-lifecycle-test)
in `deckr-integration-tests`, with the reference and three core repositories
checked out as siblings. It exercises managed lifetimes and JetStream alongside
Python and TypeScript. That harness also owns separate Compose smoke and recovery
workloads using each core's public managed service APIs.

Messaging validation does not grant application authority. Use
`authority::AgreementMessageGuard` with `send_authorized`, `request_authorized`,
and `reply_authorized` to enforce the selected agreement, lane/profile and
participant sessions. The guard checks the managed lifetime on each operation
and after awaited reply acceptance. Hardware and service boundaries additionally
enforce their descriptor, operation and resource rules. An application-specific
`MessageAuthority` can compose those checks. Retained handler contexts must
recheck authority after asynchronous application work.

`services::ServiceProtocol::from_text` loads an owner-supplied declaration with
operation definitions, message directions, exchanges, payload schemas and view
writers. Advertisement parsing compares all supplied message definitions and
view families with that declaration. `ValidatedServiceProtocol` compiles the
payload schemas once using an explicit `SchemaRegistry`; it performs no network
or filesystem retrieval. Generic validation checks supplied payload positions.
The owning protocol still decides which optional positions a particular message
requires and how application resource references are authorized.

`services::ServiceContext` binds an accepted agreement to its immutable service
descriptor, both participant sessions, and local `ServiceAccess`. Its message
guard checks identity, direction, exchange and allowed operations; asynchronous
`validate_message` additionally checks payload schemas outside protocol workers.
`authorize_view` checks the declared writer and permitted family prefixes. A view
store must also verify each record's complete fence before exposing payloads.
Contexts reject service-use records containing either `terms` or `termsHash`,
including explicit nulls. Profile authority loss closes the exact agreement;
retained contexts cannot resume work under a repaired record or a successor.

`services::ServiceViewStore` names a host-owned materialized source. Bind it to a
`ServiceContext`, an advertised view family and an optional compiled owner schema
to obtain a `ProtectedServiceView`. The binding checks all ten fence fields and
the physical storage key before exposing a record. Its `get` uses cached data;
`watch_state` exposes bounded, coalesced snapshots with complete authorized
membership. Changed keys missing from that membership represent removals. A
replacement belonging to another fence never exposes its payload, and deleting
an unseen record does not produce a removal identity.

Only the declared writer can `create`, `update` or `delete`. Writes return the
committed revision after ordered observations reach both the underlying source
and the protected projection; use that revision for guarded updates and deletes.
Application payload fields share the wire object's root with the fence, while
the store revision remains metadata. Owner schemas validate that complete flat
record outside protocol workers. Operations cancel on agreement or binding
closure, with a five-second deadline; cancellation cannot undo a remote commit
that already happened. View-source staleness makes access unavailable and clears
visible membership, but does not revoke Concord authority. Recovery installs a
complete replacement snapshot, including removal of missed deletes.

With NATS, `runtime.service_views(bucket, policy).await?` lazily opens an explicit
KV bucket and shares one source across its families and agreements. Requests for
the same bucket must use compatible TTL policies. Closing a binding or dropping
its last handle leaves that source and its agreement open. The host closes shared
sources with `runtime.nats().close_sources().await` after dependent lifetimes;
this also cancels pending source initialization and prevents reopening them.

`services::ServiceClient` owns service discovery, negotiations and successor
agreements. Supply a host-owned `ServiceDirectory`, compiled declaration,
materialized Concord runtime and endpoint `MessageSession` through
`ServiceClientSpec`. The directory keeps one indexed feature watch shared by its
borrowers; its snapshots, explicit closure and last-owner drop release that watch
without closing Beacon. `ServiceTarget::matching(query)` applies indexed filters;
an asynchronous `ServiceCandidatePolicy` returns local `ServiceAccess` permissions
for each admitted candidate or rejects it with `None`.

A race negotiates with up to 64 unique endpoint/session pairs concurrently and
includes later arrivals while pending. `ServiceTarget::exact(descriptor)` pins a
particular advertised candidate and still requires current discovery before a
new attempt. Each race has a configurable deadline, followed by cleanup and a
retry delay. Every attempt gets a fresh opaque contract ID. Before exposing a
winner, the client revokes all losers locally and joins their bounded cleanup.
An accepted connection survives discovery withdrawal, replacement and closure.
Authority loss publishes `ServiceClientStatus::Reconnecting` and starts a new
race when discovery is available; commands are never replayed.
Client state retains `cleanup_failures` and `last_cleanup_failure` after recovery.
`close` reports failures from cleanup pending or started during shutdown; an
earlier completed recovery failure does not fail a later successful shutdown.

`wait_ready` returns an immutable `ServiceConnection`. Build an envelope with
`message(body)`, add any owner-defined subject identifiers or trace fields, and
use `send` for one-way messages or `request` for request/reply exchanges. Replies
must match both the envelope and the declaration's message, direction and
payload schemas. `watch_messages` delivers authorized service messages, including
service-to-consumer requests whose `ServiceIncoming::reply` preserves the original
inbox route. Queued deliveries recheck authority and expiry. Each subscriber has
a 256-delivery limit and fails explicitly on overflow; registration is limited
to 256 subscribers. `connection.view` creates a protected binding under that exact
agreement. Retained connections, deliveries and views cannot gain a successor's
authority. Closing a client leaves the shared directory, messenger and sources
available to other borrowers.

`services::ServiceProvider` owns the provider boundary. `ServiceProviderSpec`
supplies the compiled declaration, immutable advertisement definition, borrowed
Concord and messaging resources, advertisement store, admission policy and
backend. Startup subscribes to the service lane, completes exclusive bootstrap,
then starts participation and managed publication. A competing live provider
session prevents publication. Startup retries unresolved ownership automatically;
`bootstrap_timeout` limits each attempt. Closing the provider cancels this wait.
Backend status and diagnostics update discovery;
discovery failure or withdrawal leaves healthy agreements intact.

`ServiceAdmissionPolicy` assesses a consumer endpoint/session and returns local
`ServicePermissions`, or `None` to decline consent. It receives no Concord status
classification work. Existing selections retain capacity; declined candidates
consume no slot. `set_backend` reports global consent and discovery metadata;
`set_consumer_consent` controls one consumer session. Withdrawing consent through
these synchronous setters revokes retained connections and views before returning,
including when admission or backend callbacks are blocked. `reconsider` requests
asynchronous reassessment. Assessment errors preserve an existing grant, while
changed permissions require a fresh agreement. A final selection fence prevents
an old admission decision from attaching after the domain facts change.

`ServiceBackend::connected` receives the scoped connection before message dispatch
and can publish through `connection.view`. `handle` receives a `ServiceIncoming`
only after exact authority, profile, operation, direction and payload checks.
Connections also support events and reverse requests. The boundary cancels
callbacks on authority loss and rechecks queued-message expiry. Callback errors
and panics are observable; connection-notification failure does not itself withdraw
domain consent. Backends report known withdrawal through the consent setters.
Callbacks must tolerate cancellation and use their scoped context for protected
work. `disconnected` runs after revocation; final `shutdown` can overlap disconnect
cleanup. Defaults bound provider connections to 256, admission assessments and
dispatch callbacks to 64 each, and pending messages to 256, with configurable
callback and bootstrap deadlines.

Provider snapshots distinguish participation, discovery, backend consent and
dispatch failures. Explicit close, endpoint closure and last-owner drop revoke
contexts and perform bounded subscription, token, advertisement and backend
cleanup. Cancelling a caller awaiting `close` does not abandon cleanup. Borrowed
messaging, protocol sources and view stores remain owned by the host.

The `action_runtime` module provides typed Action Runtime v1 payloads, including
the core messages, action requirements, binding and page metadata, dynamic
child selectors, availability and settings identities. Import `ActionModel` to
use strict `from_text`/`from_value` parsing and validated `to_value` serialization.
`ActionMessage::from_envelope` checks the one-way message shape and the direct
controller/runtime endpoint roles. It does not establish Concord authority or
resolve controller-owned live IDs. `validate_provider` checks payload identity
against the selected runtime instance and provider.

Routing checks accept current facts from the controller. They check action,
binding and page identity; current output/overlay generation; negotiated device,
capability and source references; requirement selectors; and hardware payload
schemas. `validate_binding_output` returns a hardware body that still needs the
separate hardware claim. It does not forward the service agreement's pointer or
send a hardware command. Dynamic-page checks validate supplied child resolutions;
they do not allocate action instances or decide placement. Settings helpers
validate mirrored identities and produce keys using the upstream key-token layout.

`ActionConnection::new(service_connection, provider_id, references)` adds these
checks to an existing managed service connection. It verifies the supplied core
declaration, derives the exact runtime instance and both endpoint sessions, and
requires an `ActionReferenceAuthority` supplied by the owner. That synchronous
check resolves current IDs, generations and resources in the supplied
`ActionScope`; the crate keeps no second routing registry. `send` rechecks those
facts after asynchronous schema validation and immediately before publication.
`accept` wraps a managed client's or provider backend's `ServiceIncoming`, checks
its payload and current references, and returns an immutable `ActionIncoming`.
Call its `authorize` again before an effect after awaiting other work.

These handles share the managed service lifetime and never adopt a successor
agreement. Reference failures and callback panics fail the operation without
withdrawing otherwise healthy service authority. Page commands carry an explicit
binding or page-session target and generation in their payload. The owner resolves
that retained target in its authorized scope and can use `validate_page_command`
for ownership, current generation, and device-scope checks. The controller
still sends hardware output separately through its current hardware claim.
`validate_action_runtime_protocol` checks the host-supplied declaration against
the core operation, direction, schema-reference and availability-family identities;
the declaration and referenced schemas must come from trusted upstream artifacts.

`ActionAvailability::bind(context, store)` creates a typed binding to the declared
`action_availability` family. Reads, writes and watches use the same service-view
lifetime, exact agreement/session fences and revision guards. Intrinsic action
validation runs before visibility and guarded mutation, so an invalid replacement
only removes previously visible availability. The provider publishes a flat wire
record; payload service identity must match its held context. View-source loss
affects availability independently of service authority. Closing the binding
leaves the agreement and host-owned store open. The conformance target reads all
Action Runtime examples and settings key vectors directly from the upstream repo.
The same conformance target exercises managed Action connections with the actual
upstream catalog and schemas, including reference changes while validation waits.

### API migration notes

- Beacon and Concord accept protocol-neutral endpoint families. Use
  `EndpointAddress::parse_protocol` for these identities; `parse` retains Deckr's
  endpoint restrictions for Deckr messages and profiles.
- Service-use agreements are termless. Replace the old service terms, terms-hash,
  and scope-index helpers with `service_use_contract`. It returns a fresh contract
  intent with both endpoint sessions. Keep namespace, operation, and view
  permissions in local `ServiceAccess`, constructed by `service_access`.
- `ServiceProtocol.operations` now maps names to `ServiceOperationDefinition`;
  `messages` retains full message definitions. The existing `new` constructor
  creates empty operation definitions and an empty message map; populate messages
  or load the owner's declaration before using managed message validation.
  Advertisements require `supportedMessages`, and serialized view families
  require `writer`. The view-family `new` helpers select the service writer;
  use `with_writer` for consumer-written views.
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
  session.rs           Neutral endpoint session lifetime
  lanes.rs             Message models, validation, and subject/header helpers
  lifecycle.rs         Neutral clocks, cancellation, failures, and state watches
  beacon.rs            Discovery implementation
  beacon/managed.rs    Managed advertisement publication and lifetime
  concord.rs           Agreement implementation
  concord/evaluator.rs Fixed-observation validation and typed reasons
  concord/managed/     Proposal and participant lifetimes
  authority.rs         Contract pointers and scoped message authority
  messaging.rs         Managed delivery, requests, subscriptions, and correlation
  json.rs              Strict wire JSON parsing and arbitrary-precision values
  canonical_json.rs    Canonical serialization and terms hashing
  keys.rs              Storage key encoding
  state.rs             State stores, watches, and materialized views
  runtime.rs           Host-owned shared protocol sources
  hardware.rs          Hardware manager runtime
  hardware/client.rs   Managed hardware discovery, claims and scoped operations
  hardware/validation.rs Descriptor-scoped hardware payload validation
  profiles/            Hardware discovery and claim models
  services.rs          Service profiles and selection helpers
  services/declaration.rs Service declarations and compiled payload validation
  services/directory.rs Shared indexed discovery and observable lifecycle
  services/client.rs   Managed service races, selection and successors
  services/provider.rs Managed provider admission, dispatch and backend lifecycle
  services/connection.rs Scoped messages, replies and protected-view access
  services/context.rs  Exact service-use authority and local access scopes
  services/views.rs    Fenced cached views and revision-guarded mutations
  action_runtime/     Typed action models/messages, routing checks and availability
  nats.rs              Optional NATS runtime and JetStream KV adapter
tests/                 Rust integration and upstream conformance tests
spec-revision          Upstream spec commit used by CI
.github/workflows/     Rust validation
```

Keep reusable implementation here. Controller orchestration, rendering policy,
layout persistence, and settings placement belong in application repositories
such as [deckr-controller](https://github.com/kws/deckr-controller). Concrete
drivers and device behavior belong in hardware-manager applications.

## Development

Run standalone checks from this repository's root:

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked
cargo test --locked --features nats
cargo build --locked --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps --all-features
```

These commands need only this repository and its Cargo dependencies. The `nats`
feature is compiled and tested without opening a broker connection. Compiling the
`conformance` feature does not read upstream artifacts; running its tests does.
These steps match the standalone CI job; CI also runs `cargo package --locked`.

To exercise consumer cleanup, source recovery, optional maintenance and the
three-host hardware/Action Runtime path against a disposable JetStream broker,
with the upstream checkout available, run:

```bash
DECKR_NATS_TEST_URL=nats://127.0.0.1:4222 cargo test --locked --all-features --test nats_runtime -- --ignored
```

The four tests delete their test buckets after success. Protocol buckets use
isolated names; the Action Runtime scenario also uses its canonical availability
bucket from the upstream binding.

## Upstream conformance tests

The `conformance` feature enables a separate test target that reads the upstream
fixtures and vectors at runtime. It does not add a runtime API or dependency.
The default spec checkout is `../deckr`; set `DECKR_SPEC_DIR` to use another
checkout. The variable names the repository root, which contains `contract/v1/`.
Missing or invalid artifacts fail the tests with the path that could not be read.

```bash
git clone https://github.com/kws/deckr.git .deckr-spec
git -C .deckr-spec checkout --detach "$(cat spec-revision)"
DECKR_SPEC_DIR="$PWD/.deckr-spec" cargo test --locked --all-features
```

This creates a separate ignored reference checkout without changing an existing
sibling. For an existing checkout elsewhere:

```bash
DECKR_SPEC_DIR=/path/to/deckr cargo test --locked --features conformance --test conformance
```

CI checks out the commit recorded in `spec-revision` and runs both standalone and
conformance checks. When updating to a new spec revision, review the upstream
changes, update the commit in that file, and run the full checks against that
checkout. The upstream commit must be available in `kws/deckr` before CI can fetch
it. Passing the current test suite covers the implemented cases; it does not
claim complete protocol conformance.

When validating a coordinated contract or runtime change, update the source commits in
`deckr-integration-tests/sources.lock.json` and follow its
[source-pin workflow](https://github.com/kws/deckr-integration-tests#source-pins-and-coordinated-changes).
All three core specification pins must agree with the harness's reference pin.

## Releases

The crate version is owned by the root `Cargo.toml`. Keep `Cargo.lock` updated
when changing the version or dependencies. Run the checks above and the upstream
conformance tests before a release, and verify the distributable with:

```bash
cargo package --locked
```

Release artifacts contain the Rust library and its implementation documentation.
Specification releases are managed separately in `kws/deckr`.
