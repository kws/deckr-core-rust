# Architecture implementation coverage

This maps the reusable Rust implementation to
[the upstream informative architecture](../../deckr/docs/deckr-architecture.md).
The reviewed upstream revision is `db0b84e968f6ec483b6f69547a37347dabaf4dce`,
also pinned by `spec-revision`. The linked upstream specifications remain
authoritative. This file records implementation coverage and test evidence;
it does not define wire contracts or certify every possible deployment.

## Validation checkpoint

On 2026-10-19, all six repository validation commands passed on Rust 1.88.0,
including the shared Concord index and compatibility fixes. Default-feature tests
also passed with `DECKR_SPEC_DIR` pointing to an absent checkout. The conformance
target passed all twenty tests, including five managed Action Runtime boundary
cases. Documentation built with warnings denied, and the crate packaged and
verified offline with the uncommitted implementation included.

All four opt-in NATS tests passed against a disposable localhost `nats:2.11`
JetStream container. They cover reply inboxes, independent endpoint lifetimes,
shared view buckets, source recovery and closure, isolated maintenance, audited
retained-record cleanup, and the three-host hardware/Action Runtime path. Cleanup
checks found no remaining owned tokens, advertisements or KV consumers for that
path. The test container was stopped and its removal verified.

## Architecture coverage

| Responsibility | Current implementation | Validation and ownership |
| --- | --- | --- |
| Exact stores and materialized sources | `StateStore`, `MaterializedStateStore`, memory and NATS adapters; atomic snapshots, ordered observations, permanent continuity epochs, bounded public watches. | Lifecycle regressions and live NATS recovery verify missed deletions, failed watches and closure. |
| Shared protocol views | `ProtocolRuntime`, lazy per-protocol NATS views, `HardwareManagerRuntime::with_concord`. Hosts own shared closure; endpoint shutdown leaves other borrowers operational. | Ownership tests verify shared subscriptions and independent endpoint closure. |
| Endpoint sessions, time, supervision, diagnostics | Neutral `EndpointSession`, injectable `Clock` with jitter, cancellation, typed failures, bounded lifecycle snapshots and predicate waits. | Controlled-clock, worker-panic, blocked-callback, cancelled-close and last-owner tests cover managed boundaries. Hosts own outer configuration and transport policy. |
| Beacon discovery and publication | Shared cached feature views, profile-independent identities, validated advertisement envelopes; `BeaconAdvertiser::manage` owns TTL-driven refresh, content updates, missing-key recovery and guarded withdrawal. | Beacon, hardware and service tests verify that discovery failure does not revoke independent agreements. Publication I/O does not hold authority locks. |
| Concord evaluation and index | `evaluate_contract` accepts fixed raw observations; cached/exact paths share typed reasons. One lazy shared index selects the smallest filtered candidate set and caches selected validity. Relevant changes invalidate the cache; malformed observations remain visible. | Evaluation parity, seven index tests, recovery regression and cached compatibility notifications. Guarded mutations expose `MutationCode`; queries do not scan native KV. |
| Concord participation | `propose` owns one fresh agreement; `participate` owns one filtered view and asynchronous batch selection with a final domain-decision fence. Per-agreement workers attach with at most eight CAS attempts, renew from TTL, preserve permanent loss, and perform bounded guarded cleanup. | Managed Concord tests cover pending admission, competing sessions, partial attachment, renewal failure, source interruption and domain consent. Hardware/service tests compose the same workers. |
| Optional Concord maintenance | Explicit exact-store `ConcordMaintenance`, one-shot and host-managed recurring scans, stale grace and retention, mandatory audit, revision guards, durable cleanup markers and bounded cleanup passes. | Fifteen failure/scheduling tests, upstream record conformance and live NATS cleanup. Ordinary participation has no maintenance readiness dependency. |
| Messaging | `MessageSession` and `MessageTransport` validate envelopes, lanes, identity, expiry, request/reply correlation, routing hints and bounded queues. `AgreementMessageGuard` composes exact managed authority. Strict JSON preserves application keys and numeric tokens. | Messaging regressions and live reply-inbox tests cover invalid replies, cancellation, expiry and endpoint independence. Host connection retries and final transport shutdown are separate responsibilities. |
| Hardware manager | Managed discovery, bootstrap, claim admission/capacity, descriptor and payload validation, scoped driver contexts/replies, per-claim renewal, device-change termination and bounded shutdown. | Thirty manager tests, descriptor/conformance tests and the live three-host routing scenario. Driver callbacks cannot delay protocol termination or renewal. |
| Hardware client | Shared discovery, asynchronous selection, multi-device claims, validated scoped operations, requests and bounded event watches; fresh successors with no command replay. | Thirteen client tests and live H/S routing verify independent discovery, endpoint ownership, reply routes and retained-context revocation. |
| Service client | One shared indexed directory, asynchronous local permissions, races of at most 64 unique endpoint/session candidates, exact selection, loser cleanup and fresh successors. | Sixteen client tests, declaration checks and live service negotiation. Advertisements cannot replace the owner declaration's message meaning or view fences. |
| Service provider | Bootstrap, subscription before publication, independent Beacon/Concord, bounded admission, explicit synchronous consent, final decision fencing, scoped dispatch, protected publication and supervised backend callbacks. | Nineteen provider tests and live consent withdrawal/recovery. Discovery metadata does not grant or withdraw domain consent. |
| Protected service views | Flat exact agreement/session fences, contract-scoped keys, authorized cached reads/watches, declared writers and revision-guarded writes that wait for ordered observations. | Ten view tests, upstream vectors, shared NATS bucket tests and live typed availability. View-source availability is independent of Concord authority. |
| Action Runtime support | Fourteen typed operations, action/binding/page/settings models, availability, declaration validation, and current-reference checks composed with managed service dispatch. | Model/conformance tests, awaited-validation races, and live input under H, action traffic under S, then a separately authorized output under H. Owner callbacks resolve current routing facts. |
| Application ownership | Controller orchestration, identity allocation, routing registries, rendering, filesystem configuration and device-specific behavior stay in their owning applications. | The three-host test supplies controller state explicitly; reusable validators allocate none of it. Settings helpers validate identities/keys without defining a configuration service. |

## Regression coverage added during implementation

`tests/hardware_manager_runtime.rs` now checks that:

- Advertisement replacement cannot revoke a healthy Concord claim.
- Malformed stream items and envelopes do not stop the manager.
- Material descriptor changes end claims even with an unchanged fingerprint.
- Requested instance counts cannot exceed advertised capacity.
- Failed remote cancellation still revokes local authority and attempts owned-token withdrawal.
- Stalled publication does not hold authority locks; cancelled close callers do not abandon cleanup.
- A competing live manager survives attempted bootstrap; stopping during startup cannot publish or spawn late workers.
- Cancelling a startup caller initiates owned cleanup without requiring another stop call.
- Watch-driven selection admits waiting claims when capacity becomes free, while retaining existing selections.
- Token renewal follows the store TTL even with a blocked driver callback and long legacy maintenance intervals.
- State watches expose inventory, claim authority, independent discovery failure and bounded closure; registration limits do not prevent shutdown.
- Scoped replies complete managed requests through their original inbox, preserve pointer/session/correlation, reject changed resources or operations and invalid state schemas, and stop after device replacement or manager closure.
- Expired or wrong-session requests produce neither driver work nor replies; all generated rejection paths retain return routes and correlation.

`tests/lifecycle_regressions.rs` checks that two endpoint lifetimes share one set
of protocol sources, and closing one endpoint leaves the other sources current.
Existing tests cover source recovery, permanent authority loss, bounded cleanup,
and snapshot membership and subscription limits.

`tests/hardware_descriptors.rs` checks complete descriptor round trips, defaults,
references and collection directions; embedded schema dialects and explicit
offline resolution; raster decoding, command selection and dimensions; extension
values, unit scales, decimal steps and large integer boundaries. The conformance
target reads the upstream positive and negative descriptor examples directly.
Hardware runtime tests check that invalid operations never reach the driver and
retained driver contexts lose authority after material metadata changes.
Hardware profile coverage also checks nested claim references, strict object
fields and advertisement identity; conformance includes the upstream capacity,
claim-device, claim-terms and advertisement payload examples.

`tests/managed_hardware_client.rs` exercises the managed client against the real
hardware manager through an in-memory message transport. It covers discovery,
claim identity and participant sessions, command/reply and event validation,
invalid reply candidates, multi-device claims, invalid selection recovery,
discovery disappearance and closure, descriptor replacement and successor
claims, pending-request cancellation, competing proposal timeouts and cleanup,
slow event subscribers, blocked selection, cancelled closure, endpoint closure,
and shared-resource ownership. A controlled-clock test verifies request expiry,
retained driver-context expiry and reply/event timestamps across both boundaries.

`tests/managed_beacon.rs` uses a controlled clock to exercise TTL scheduling,
immediate content changes, missing-key recovery, transient failures, ownership
changes, cancelled close, last-handle drop, and cleanup after a create commits
without acknowledgement. Publication and renewal perform no discovery scans.
Lifecycle unit tests cover atomic initial snapshots, coalescing, interrupted
currentness, registration limits, and distinct timeout/closure outcomes.

`tests/managed_messaging.rs` covers invalid/stale/unrelated replies, absent-pointer
correlation, application acceptance, inbox cleanup on every exit, expiry during
inbox setup and reply acceptance, sender ownership, reply routing, endpoint and
last-owner closure, shared transport independence, explicit queue overflow,
extension registration and exact managed authority. Wire checks exercise duplicate
keys, finite decimal input, version/target defaults, oversized TTLs and timeouts,
and preservation of application keys that resemble Serde internals. NATS unit
tests check lane versus inbox routing and contradictory/partial headers. The
conformance target reads positive and negative upstream messaging examples.

`tests/service_declarations.rs` checks declaration round trips, operation/direction
relationships, advertisement/schema tampering, view writers and prefixes,
request/reply direction, payload schema selection, offline references, strict
JSON and required fields. `tests/service_context.rs` checks exact service-use
identity and sessions, operation permissions, schema validation, restricted view
prefixes and writer roles, cancellation on authority loss, and rejection of
explicit null terms that generic Concord permits. The conformance target reads
the upstream service model examples, compiles the Action Runtime catalog using
upstream schema resources, and validates every catalog message's example payload.

`tests/protected_service_views.rs` checks all ten fences, removal-only replacement
visibility, suppressed unseen deletions and queued stale payloads, schema and
writer/prefix checks, 1,000 cached reads without native reads/scans/consumers,
ordered write/update/delete observations, revision races against unseen
replacements, missed-delete recovery while Concord remains current, authority
loss during a pending mutation, bounded slow watches, cancelled close, last-owner
drop and borrowed-source ownership. Conformance reads upstream view model
examples and service-view key vectors. The opt-in NATS test additionally checks
concurrent bucket sharing, TTL-policy conflicts and host source shutdown.

`tests/managed_service_client.rs` verifies simultaneous negotiation of 64 unique
candidates, duplicate advertisement deduplication, the admission limit, later
arrivals during blocked policy work, local query/permission checks, loser cleanup
before readiness, exact selection and current discovery, timeout cleanup and fresh
IDs, discovery-independent accepted authority, reconnecting snapshots and successor
sessions without command replay, pending-request cancellation, payload/reply
validation, reverse requests, protected-view integration, bounded slow message
readers and queued expiry, endpoint closure, cancelled close, policy panic cleanup,
last-owner drop and shared directory/source ownership. Directory registration tests
confirm that multiple clients still consume only one Beacon feature watch.

`tests/managed_service_provider.rs` exercises actual managed clients and providers:
bidirectional requests, events, protected publication, synchronous global and
consumer consent withdrawal, blocked handlers and stale admission, independent
discovery metadata and disappearance, invalid payloads/namespaces/operations and
senders, explicit null terms during blocked reassessment, capacity after declined
candidates, permission changes and fresh successors, transient admission errors,
callback panics/timeouts, bounded queues and queued expiry, stalled publication
and unacknowledged creates, controlled-clock TTL renewal during blocked callbacks,
independent authority when view sources close, competing sessions, startup closure, public-watch
limits, cancelled close, concurrent backend cleanup, remote cleanup failures,
endpoint closure, last-owner drop and borrowed-resource ownership. Concord tests
also cover the final selection fence and containment of synchronous fence panics.

`tests/action_runtime.rs` checks runtime identity, descriptor consistency, unique
requirements, strict JSON and arbitrary application keys/numbers, all fourteen
payload positions and endpoint roles, current binding and output generations,
negotiated capability/requirement/source checks, projected provenance, hardware
payload validation, controller-owned dynamic-page resolutions, and settings
identity/key layouts. `tests/action_availability.rs` publishes through actual
managed service providers and clients, checks flat exact fences, typed reads and
guarded writes, invalid replacement removal, retained-view revocation and
independent service authority when the availability source closes. Conformance
checks thirty reusable model entry points, all fourteen message shapes, stored
availability and settings-key vectors directly against upstream artifacts.
`tests/support/action_boundary.rs`, included by the conformance target, exercises
actual managed service clients/providers with the upstream Action Runtime catalog
and schemas. It checks both dispatch directions, immutable agreement/session
scopes, retained-message revocation, stale generations, provider identity,
page-opener resolution, contained owner-callback panics, declaration tampering,
and reference changes during awaited schema validation on both send and receive.

`tests/managed_concord.rs` covers automatic TTL renewal without discovery scans,
failed renewal, live competing sessions, unknown selector pointers, blocked
selectors and attachments, endpoint closure, cancelled close, unknown attachment
acknowledgements, bounded partial-attachment retries, creation collisions, domain-consent withdrawal, adapter panics, and required cancellation when a clean withdrawal observes an expired heartbeat.
`tests/concord_evaluation.rs` checks fixed-observation reason codes, retained token
identities, exact/cached parity, mutation guards, and filtered malformed observations. Managed contexts also have a
source-recovery regression where exact records remain valid while local authority
stays permanently terminated.

`tests/concord_maintenance.rs` checks lazy construction with no subscriptions,
stale and retention boundaries, pending live tokens, unavailable observations,
refresh arrival during a stale check, metadata-only audit, invalid contract
identities, malformed/foreign token lists, marker identity, guarded conflicts,
the eight-pass retry bound, unacknowledged deletion, independent cleanup resumption,
orphan removal, audit failure/panic/timeout, host-clock scheduling, cancelled close,
last-owner drop and borrowed-store lifetime. Conformance reads the upstream
stale-observation and token-cleanup examples. The live NATS maintenance test checks
audited deletion, exact token cleanup and absence of leftover consumers.

`tests/concord_index.rs` checks smallest-candidate filtering, no evaluation of
unrelated malformed contracts, cached session assertions, precise invalidation,
selector changes, malformed replacements, 256 mixed modern/compatibility
subscribers sharing one subscription per source, overflow, synchronous queries
outside runtime threads, and last-owner closure. A lifecycle regression verifies
that recovery removes missing members while retaining the intervening loss of
currentness. The compatibility lease regression verifies that adopting a newer
stored token observation neither extends a local heartbeat deadline nor revives
an expired lease.

`tests/support/nats_action_path.rs`, enabled by `nats` and `conformance`, exercises
three independent NATS hosts using the upstream Action Runtime catalog and
schemas. It follows hardware input under H, action input/output under S, and a
separate hardware command/reply under H. It checks protected availability, service
consent withdrawal while hardware remains usable, a fresh S without replay,
hardware closure while S remains usable, stale output generations, and shutdown
with no owned advertisements, tokens or remaining KV consumers.

## Scope of the completed audit

Every reusable responsibility in the informative architecture has an API and
observable validation coverage above. The audit also checked declaration hints,
legacy entry points, the spec pin and host ownership of connection retries and
shutdown. Low-level exact store operations and manually driven legacy lease
utilities remain available for compatibility and diagnostics; managed application
boundaries use the supervised contexts and shared views. No application routing
state, new wire API, copied upstream artifact bundle or controller policy was
introduced.

The architecture remains informative. These results demonstrate the exercised
contracts and failure boundaries; they do not establish performance limits,
hardware-driver correctness, or conformance of a different transport binding.
