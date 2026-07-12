# Changelog

All notable changes to the `stitch-sync` crate are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.1] - 2026-07-11

### Fixed

- `subscribe_entity` / `subscribe_scope_entity` (and the wasm
  `subscribeToEntity` / `subscribeToScope`) now fire when a scope is loaded
  (`load_scope` / `replace_scope`) or cleared (`clear_scope`). The event
  forwarder previously dropped `ScopeLoaded` / `ScopeCleared`, so reactive
  bindings never received a re-read signal after a scope opened. Scope signals
  are delivered as a `MutationEvent` with `data: None` (a "re-read everything"
  cue).
- `create` now derives its mutation `scope_id` from the resolved record
  (matching `update` / `delete`) instead of the raw scope argument, so all three
  operations land under the same `(scope_id, entity)` key. A child row whose
  scope field diverges from the `scope_id` argument is now announced under the
  record's own scope, so a scope-filtered subscriber observes the create.
  Top-level entities are announced under an empty scope, consistent with their
  `update` / `delete` events.

## [0.3.0] - 2026-07-11

### Added

- MQTT 5 last-will (testament) support. `StoreConfig` gains a `will:
  Option<WillConfig>` (topic, payload, qos, retain, will-delay-interval,
  content-type) and a configurable `keep_alive_secs` (default 60, previously
  hard-coded). The will is registered on every `connect`/`reconnect`; the broker
  publishes it only on an ungraceful disconnect and clears it on a normal
  `disconnect`, so it never fires on intentional logout.

### Fixed

- A `create` or `update` carrying an optional field set to `null` no longer
  fails the mqdb schema validator inside `persistence` (`expected type Number,
  got null`). `persistence::{create,update}` now strip null-valued keys before
  the durable write, mirroring `memory_store`. Previously the persistence error
  was discarded while the in-memory copy succeeded, so `list` returned empty
  until restart even though `read` saw the record. Note: as a consequence, a
  `null` in an update is dropped rather than clearing the field.
- The `(scope, entity)` version counter now stays consistent for top-level
  entities (`create` and `update`/`delete` bump the same key), is no longer
  advanced mid-batch before the buffered mutation is broadcast, invalidates the
  previous scope when `loadScope` switches scopes, and releases a scope's
  version entries on `clearScope`/scope-switch instead of growing unbounded.

## [0.2.3] - 2026-06-07

### Changed

- The crate is now published to crates.io as **`stitch-sync`** (the `stitch`
  name was taken by an unrelated 2018 crate). The library name is unchanged, so
  it is still imported as `use stitch::…`; only the dependency line differs
  (`stitch-sync = "0.2.3"`). Added crates.io package metadata (keywords,
  categories, MSRV `1.88`) and corrected the repository URL.

### Added

- **`stitch-wasm` now mirrors the framework-agnostic TS `@laboverwire/stitch`
  core Store API**: added `list`, `listRootEntities`, `getChildCount`,
  `getSnapshotAsMap`, `closeScope`, `disconnect`, `reconnect`, `isReconnecting`,
  `ready`, `beginBatch`/`endBatch`, `request`, `updateLocalState`,
  `resetForLogout`, `destroy`, `hasPersistence`/`hasRemote`, plus
  `subscribeToScope` and `subscribeToConnectionStatus`. The `subscribe*` methods
  now return an unsubscribe function, and `subscribeToEntity` delivers
  `(data, op)` to its callback. `Store::{disconnect,reconnect,request}` are now
  cross-platform; added `Store::{has_persistence,has_remote}`. (Deferred:
  `setSessionInvalidHandler`/`setReconnectValidator` on wasm, `loadScope`/
  `clearScope`, the sessionStorage user-cache helpers, and the React/Vue subpath
  bindings.)


- **MQTT v5 JWT enhanced-auth now works in the browser**: the wasm remote adapter
  sends the JWT as the CONNECT `authentication_data` (method `"JWT"`) and answers
  any broker `AUTH(Continue)` challenge with a no-op (matching native
  `JwtAuthHandler`'s `Success`). `RemoteConfig` gained a static `ticket: Option<String>`
  (used when no `get_ticket` provider is set), surfaced as
  `createStore(config, { remote: { url, ticket } })`. Per-connect token refresh
  via an async JS provider on wasm is a future enhancement.


- The **durable offline queue is now cross-platform** (`wasm32` too): writes made
  in the browser while disconnected are buffered (in IndexedDB via the persistent
  queue, or in memory) and replayed to the broker on reconnect. The
  `OfflineQueue`/`MutationSender` traits gained the `?Send` wasm split, the
  concrete queues route time/id through `rt::now_millis`/`rt::new_id`, and
  `rt::sleep` backs the flush-retry loop. `stitch-wasm` exposes
  `setAuthenticatedUser` (the queue scopes writes per user) and
  `pendingMutationCount(scopeId)`.


- `wasm32` builds now support **remote MQTT sync over WebSocket** via
  `mqtt5-wasm`. The MQTT client sits behind an `MqttClientApi` trait, so
  `SyncEngine`'s request/response, scope-fetch, and live-mutation logic is shared
  across native (`mqtt5::MqttClient`, TCP/TLS) and wasm
  (`mqtt5_wasm::WasmMqttClient`, `ws://`/`wss://`). This is the core sync path —
  connect/subscribe/publish, live mutation delivery into the cache, scope
  fetch/open, and reconnect. JWT enhanced-auth and the durable offline queue stay
  native-only for now (a connect-time ticket is ignored on wasm with a warning).
- `stitch-wasm`: `createStore(config, { remote: { url, clientId? } })` enables
  remote sync, plus a `connectionStatus()` getter. `initialize()` connects when a
  remote is configured.
- `wasm32` builds now support durable **IndexedDB persistence** via `mqdb-wasm`,
  matching the native fjall persistence layer. The record store sits behind a
  backend `Db` trait, so `PersistenceLayer` is shared across native and wasm; on
  wasm it opens `WasmDatabase::openPersistent` (plaintext) or `openEncrypted`
  (AES-GCM, when a passphrase is given). Remote MQTT sync remains native-only.
- `stitch-wasm`: `createStore(config, { persistence: { dbName, passphrase? } })`
  enables persistence, plus `readLocalState` and `replaceScope` bindings to read
  durable state and rehydrate a scope's snapshot after a reopen.

### Fixed

- `Store::create` now stamps a child row's scope field before writing, so the
  persisted record carries it (previously only the in-memory copy did),
  letting scope-filtered reads from persistence find the row after a reopen.

## [0.2.2] - 2026-05-30

### Fixed

- Mutations whose direct `sync_update` lost the create→update ordering race were
  parked in the offline queue and only retried on the next reconnect, starving
  throughput while the connection stayed up. A new notify-driven `flush_loop`
  task now drains the queue while connected: `create`/`update`/`delete` wake it
  whenever a direct sync leaves a mutation parked, and it re-flushes after a
  250ms backoff until the queue drains. `update` also treats a remote `NotFound`
  like `delete` does — silenced and re-queued for recreate-from-local on flush —
  removing the `remote update failed: entity not found` warning burst.
- `initial_sync_done` is now a within-session one-way latch. `on_connected` no
  longer resets it to `false` on every reconnect (only sets `true` after sync;
  `reset_for_logout` still clears it), so readiness gates built on it stop
  bouncing under broker churn.

### Changed

- `OfflineQueue::flush` returns `Result<usize>` (rows retained as transient) so
  the flush loop can distinguish a drained queue from one needing another pass.
  The trait is crate-internal, so no public-API break.

## [0.2.1] - 2026-05-30

### Fixed

- The remote-mutation cache mirror no longer drops out-of-order deliveries. An
  `Update` that arrives before its `Insert` (which surfaced as `Error::NotFound`
  from `MemoryStore::update` under high write throughput) now upserts the row
  from the update's data instead of logging a warning and dropping it, keeping
  the local cache convergent for cross-process coordination.
- A `Delete` mirror for an already-absent row is treated as converged
  (`Error::NotFound`) rather than logged as a failure, removing benign warning
  spam under load.

### Changed

- Extracted the cache-mirror logic into a `mirror_remote_to_memory` function so
  the upsert/idempotency paths are covered by in-process regression tests.

## [0.2.0] - 2026-05-29

### Changed

- Default `StoreConfig.event_channel_capacity` raised from 1024 to 4096 to give
  high-write scopes more headroom before the internal mutation bus lags.
- Bumped local-database dependencies to `mqdb-agent` 0.8.8 / `mqdb-core` 0.7.3,
  which add bounded retry-on-conflict (field-level last-writer-wins) and an
  atomic fjall commit on the embedded backend.

### Fixed

- mqdb CAS conflicts on `update`/`delete` are now mapped to the typed
  `Error::Conflict { entity, id }` (classifiable via `Error::is_conflict`)
  instead of being flattened into an opaque `Mqdb` error.
- The internal mutation forwarder no longer drops inbound remote deliveries on
  `RecvError::Lagged`. When still connected it runs `sync_root_entity_list` to
  re-fetch authoritative state, preventing local divergence and missed
  scheduler wakeups under high write throughput.
- Remote-mutation apply and cache-mirror failures are logged at `warn` level
  rather than silently discarded; intentional last-writer-wins / out-of-scope
  drops still return quietly.

### Added

- Internal `RemoteSyncOps` seam over the remote layer so the lag/resync path is
  covered by in-process regression tests without a live broker.
- Regression tests for concurrent same-key convergence and the
  lag-triggers-resync behavior (including the connection-status gate and the
  sustained-overflow re-lag case).

## [0.1.0]

- Initial release: reactive state-sync over an in-memory cache, fjall-backed
  local persistence, and MQTT-based remote sync.
