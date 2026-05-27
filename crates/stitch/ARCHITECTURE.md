# Architecture

Internal design of the Rust port of stitch. The conceptual model mirrors the TS
library — see the TS library's [`ARCHITECTURE.md`](https://github.com/LabOverWire/stitch/blob/HEAD/ARCHITECTURE.md) for the
canonical reference. This document focuses on what's specific to the Rust port:
the layer composition, deliberate deviations, and current gaps.

---

## Layer stack

```
┌─────────────────────────────────────────────────┐
│  Store (public facade)                          │
│  src/store.rs                                   │
└──────┬────────────┬───────────┬─────────┬───────┘
       │            │           │         │
       ▼            ▼           ▼         ▼
   MemoryStore  Persistence  OfflineQueue  RemoteSyncLayer
   (in-memory)  (fjall)      (pending_sync) (MQTT5 + scope ops)
                                              │
                                              ▼
                                          SyncEngine
                                          (mqtt5 client)
```

| Module | Role |
|---|---|
| `config.rs` | `StoreConfig`, `EntityDefinition`, `ScopeConfig`, `RemoteConfig`, `PersistenceConfig` |
| `error.rs` | `Error` enum with classifiers (`is_transient`, `is_not_found`, `is_conflict`, `is_ownership`, `is_corruption`) |
| `origin.rs` | `Origin::{Local, Remote, Load, Clear}` with `skips_persistence` / `skips_remote` |
| `types.rs` | `MutationEvent`, `StoreEvent`, `Operation`, `SyncMutation`, `ScopeBundle`, `ScopeState`, `PendingMutation`, `ConnectionStatus` |
| `db_helpers.rs` | `pub(crate)` shared open/register/value-conversion helpers used by `memory_store` and `persistence` |
| `memory_store.rs` | Hot reads for the active scope; held in an `mqdb_agent::Database` opened with `MemoryBackend`; emits `StoreEvent` via `broadcast::Sender`; supports `begin_batch`/`end_batch` deduplication |
| `persistence.rs` | Durable storage for all scopes; held in an `mqdb_agent::Database` on fjall; `set_suppress_notifications`; `recover()` for corruption recovery |
| `offline_queue.rs` | Persistent (`pending_sync`-backed) and in-memory implementations of the `OfflineQueue` trait; consolidation (insert + updates → insert with merged data, etc.); `MutationSender` trait that `RemoteSyncLayer` implements |
| `sync_engine.rs` | Thin layer over `mqtt5::MqttClient`; request-response correlation; per-scope subscriptions; root-wildcard subscription for cross-scope root events; top-level entity subscriptions; JWT enhanced-auth via `JwtAuthHandler` |
| `remote_sync.rs` | Routes CRUD by entity role (Root/Child/TopLevel); `apply_mutation_to_db` with version-based conflict resolution; `reconcile_children`; `sync_root_entity_list`; impls `MutationSender` for `OfflineQueue` to drain through |
| `store.rs` | Public facade; idempotent `initialize` via `OnceCell`; CRUD fan-out (memory + persistence + queue + remote); `replace_scope` orchestration; background tasks for remote-mutation forwarding and connection-status handling |

---

## Origin tags

Threaded explicitly through every mutation API instead of TS's class-field hack.

`Origin::Local` is `Default`. Helpers `skips_persistence()` and `skips_remote()`
gate the fan-out in `Store::create/update/delete`:

| Tag | `Store::create`/`update`/`delete` fan-out |
|---|---|
| `Local` | memory + persistence + queue + remote |
| `Remote` | memory only (persistence + remote skipped) |
| `Load` | memory only |
| `Clear` | memory only |

Note: when a real inbound MQTT mutation arrives, the Store's
`handle_remote_mutation` writes to persistence via a separate `LocalAccessor`
path (not through `Store::create`). The `Origin::Remote` flag on
`Store::create` is for app code that wants to push a "remote-shaped" mutation
through (e.g. for testing or for replaying a sync event without re-publishing).

---

## Data flow

### Local mutation

```
UI → Store::create(entity, scope_id, data, Local)
       ├─ memory.create(..)         ← UI re-render fires from memory bus
       ├─ persistence.create(..)    ← durable
       ├─ queue.queue(..)           ← user_id required (warn if missing)
       └─ if connected:
             remote.sync_create(..) ← MQTT publish, await response
             queue.remove(..)       ← on success
```

### Remote mutation

```
broker → MQTT publish on $DB/{root}/{scope}/{entity}/events/created
       → SyncEngine scope callback (or root-wildcard callback)
       → mutation_bus
       → Store::handle_remote_mutation
         ├─ remote.apply_mutation_to_db(..)   ← writes persistence via accessor
         └─ if in current scope: memory.create/update/delete(.., Remote)
```

No persistence-bridge component: `Store::handle_remote_mutation` orchestrates the
fan-out explicitly. This deviates from TS where a separate
`setupPersistenceSubscriptions` callback bridges persistence events into memory.

### Scope replacement

```
Store::replace_scope(scope_id)
  ├─ if same scope: no-op
  ├─ atomically replace current_scope with new id; await
  │  remote.close_scope(prev) to unsubscribe the prior scope's MQTT topics
  │  (errors suppressed; local data for prev scope is NOT cleared here —
  │  the eventual memory.load_scope swap below replaces it)
  ├─ if remote connected:
  │     persistence.set_suppress_notifications(true)
  │     remote.open_scope(scope_id)            ← subscribes, fetches root + children concurrently
  │     local upsert root + reconcile children
  │     replay buffered_mutations
  │     load bundle from persistence
  │     memory.load_scope(scope_id, bundle)    ← destructive: fresh inner DB, swap atomically
  │     persistence.set_suppress_notifications(false)
  └─ else (offline):
        load bundle from persistence
        memory.load_scope(scope_id, bundle)
```

`MemoryStore::load_scope` opens a fresh `mqdb_agent::Database` with
`MemoryBackend`, populates it, then atomically swaps the inner `Arc<Database>`
behind `tokio::sync::RwLock`. Subscribers see fresh data after the swap;
existing receivers stay connected to the bus.

---

## Connection lifecycle

MQTT5 persisted sessions do most of the heavy lifting. `clean_start: false` and
configurable `session_expiry_secs` (default 3600) mean reconnects resume
existing subscriptions without re-subscribing on the wire.

- `SyncEngine::connect` checks `ConnectResult.session_present`. If `true`, we
  trust the broker; if `false` (first connect or session expired), we re-subscribe
  the response topic, root wildcard, top-level patterns, and all
  `subscribed_scopes`.
- Pending requests stored in `pending_requests: Mutex<HashMap<request_id,
  oneshot::Sender>>` survive transient disconnects — the broker queues responses
  for delivery on resume.
- Explicit `Store::disconnect()` drains pending requests with
  `Err(ConnectionClosed)`; transient disconnects do not.
- `AuthFailure` from `ConnectionEvent::Disconnected { reason }` fires the
  session-invalid handler set via `Store::set_session_invalid_handler`.

---

## Offline queue and flush

Persistent queue rows live in a `pending_sync` entity in the same fjall database
as the rest of persistence. Schema:

```
id, op, entity, entityId, scopeId, userId, data?, createdAt
```

Indexes on `scopeId` and `entity`. Each row is keyed by the user who created it
(`userId`); calling `Store::set_authenticated_user(None)` then `queue` produces a
`tracing::warn!` and drops the row — same behavior as TS, with the silent failure
replaced by a log.

`flush_consolidated` collapses pending rows per `(entity, entityId)`:

- insert + delete → drop (no-op against server)
- insert + N updates → single insert with merged fields
- N updates → single update with merged fields
- update + delete → single delete

Sort by `min(created_at)` then op priority (`insert=0, update=1, delete=2`) for
FK-safe replay order. Outcomes per attempt:

| Sender result | Outcome |
|---|---|
| `Ok(())` | drop queue row |
| transient (`Mqtt`, `Timeout`, `ConnectionClosed`) | keep |
| `Ownership` | drop (silent) |
| `NotFound + Delete` | drop (already gone) |
| `NotFound + Update` on root | `sender.delete_entity` + drop |
| `NotFound + Update` on child | `read_entity` + `sync_create` + drop (upsert) |
| `Conflict + Insert` | switch to `sync_update` + drop |
| anything else | drop + log |

A private `on_connected` task (in `src/store.rs`) flushes the queue twice on
each `ConnectionStatus::Connected` event — first to replay, second to drain
anything the first left as transient — then runs `sync_root_entity_list` and
sets `initial_sync_done`.

---

## Subscriptions

`Store::subscribe()` exposes the **memory bus** — receives `StoreEvent::Mutation`
for the current scope plus `ScopeLoaded` / `ScopeCleared` events.

`Store::subscribe_persistence()` exposes the **persistence bus** — receives a
`StoreEvent::Mutation` for every persisted write, regardless of scope. Returns
`None` when persistence isn't configured. Use this for cross-scope observation
(e.g., a list of all projects, even ones whose scope isn't open).

Both buses are `tokio::sync::broadcast::Sender<StoreEvent>` with capacity
controlled by `StoreConfig.event_channel_capacity` (default 1024). Slow consumers
that fall behind get `RecvError::Lagged`.

### Batching

`Store::begin_batch()` / `Store::end_batch()` defer memory-bus notifications. While
the batch counter is non-zero, mutations replace entries in a `HashMap<(scope,
entity), MutationEvent>`, so rapid bursts collapse to one event per unique
`(scope, entity)`. The end of the outermost batch drains and emits. Persistence
bus is unaffected.

---

## Corruption recovery

`PersistenceLayer.db` is held in `arc_swap::ArcSwap<Database>` to allow hot swap.

`PersistenceLayer::recover()`:

1. Swap in a memory-backed placeholder so the fjall directory lock can release.
2. Send `shutdown()` to the old Database and drop the returned `Arc<Database>`.
3. Retry `open_persistent_db` up to 10 times with 50 ms backoff — fjall can take
   a tick to release the directory lock after the last `Arc` drops.
4. Re-register schemas. mqdb-agent loads persisted schemas (including
   `pending_sync`) automatically during open, so the offline queue keeps working.
5. Store the new Database into the `ArcSwap`.

`Error::is_corruption()` classifies wrapped `mqdb_core::Error::Corruption` and
`Error::Storage(fjall::Error)` so consumers know when to call `recover()`.

**Caveat:** callers must release any outstanding `Arc<Database>` clones obtained
from `PersistenceLayer::database()` before calling `recover()`. If any references
remain, the old Database stays alive, the fjall lock stays held, and the retry
loop exhausts.

---

## Deliberate deviations from TS

| TS | Rust port |
|---|---|
| `mqdb-wasm` `Database` (sync reads via WASM event loop tricks) | `mqdb-agent::Database` (async); same semantics, no sync-callback dance |
| `originTag` mutated on a class field around each WASM call | `Origin` threaded explicitly through API arguments |
| `persistence-bridge` callback subscribes to persistence and writes memory | `Store::handle_remote_mutation` writes both layers inline |
| Custom backoff: `min(1000*2^n, 30000)` ms with jitter, 5 attempts then 15 s | mqtt5's built-in `ReconnectConfig` |
| `clean_start: true` + explicit re-subscribe each connect | `clean_start: false` + session resume; re-subscribe only when `session_present=false` |
| `_opQueue` serializing all persistence ops with 10 s timeout | mqdb-agent's `Database` is already concurrency-safe internally |
| `sessionStorage` cached user / pending logout | Out of scope; caller owns this on native |
| React hooks (`useSyncScope`, `useEntitySnapshot`, …) | Out of scope |
| Vue composables (`useStore`, `useScopedEntities`, …) | Out of scope |

---

## Module visibility

The internal layer modules are marked `#[doc(hidden)] pub` so tests and advanced
consumers can reach them, but they're not part of the documented public surface.
Long-term they should narrow to `pub(crate)` once `Store` covers every needed
operation. The current public surface is the re-exports in `lib.rs`:

- `Store`, `StoreConfig`, `StoreOptions`, `PersistenceConfig`, `RemoteConfig`,
  `EntityDefinition`, `SchemaField`, `ForeignKeyDefinition`, `OnDeleteAction`,
  `ScopeConfig`, `TopLevelEntity`, `ReconnectValidator`
- `Origin`, `Operation`, `MutationEvent`, `StoreEvent`, `ScopeBundle`,
  `ScopeState`, `SyncMutation`, `PendingMutation`, `ListFilter`, `SortField`,
  `SortDirection`, `ConnectionStatus`, `Record`
- `Error`, `Result`

### `Store` method surface

CRUD: `create`, `read`, `update`, `delete`, `list` (with `ListFilter` for
scope/sort/projection), `list_root_entities` (with sort), `snapshot`,
`child_count`.

Subscriptions: `subscribe` (memory bus), `subscribe_persistence` (cross-scope
persistence bus), `subscribe_entity` and `subscribe_scope_entity`
(filtered `tokio::sync::mpsc::UnboundedReceiver<MutationEvent>` streams),
`subscribe_connection_status`.

Lifecycle / readiness: `initialize`, `ready`, `initial_sync_done`,
`is_reconnecting`, `connection_status`, `current_scope`, `replace_scope`,
`close_scope`, `disconnect`, `reconnect`, `shutdown`,
`recover_persistence`.

Auth / session: `set_authenticated_user`, `set_session_invalid_handler`,
`set_reconnect_validator`, `reset_for_logout`.

Direct broker access: `request` (ad-hoc RPC), `applied_version` (last bump
seen for a scope).

Local-only state: `read_local_state`, `update_local_state` (skip memory and
remote; hit persistence directly with upsert semantics).

Batching: `begin_batch`, `end_batch`.

---

## Testing

- **Unit/integration tests** (no broker): `tests/memory_store.rs`,
  `tests/persistence.rs`, `tests/offline_queue.rs`, `tests/remote_sync.rs` (mock
  `LocalAccessor`), `tests/store.rs`.
- **Wire tests** (real `mqdb-agent::MqdbAgent` broker fixture):
  `tests/wire.rs` + `tests/common/mod.rs`. The broker fixture allocates a free
  port via `TcpListener::bind`, starts the broker with anonymous auth, and
  shuts it down with a bounded `tokio::time::timeout` on the broker task
  (mqtt5 broker doesn't always exit cleanly on signal; abort on Drop).

```
cargo test                              # full suite, default parallel
cargo clippy --all-targets -- -D warnings   # CI-strict
```

Current: 90 tests, all passing, zero clippy warnings, zero build warnings.

---

## Known gaps

These exist deliberately or as deferred work:

- **`Store::set_authenticated_user` requires `initialize()` first** — returns
  `Error::NotInitialized` if called before init. TS allowed the reverse via a
  cached field. Native callers should init first.
- **No corruption auto-retry inside CRUD wrappers** — `Error::is_corruption()`
  classifies, but the user must call `PersistenceLayer::recover()` themselves
  via the layer module directly (`stitch::persistence::PersistenceLayer`, which
  is `#[doc(hidden)]` but reachable). `Store` does not yet expose a passthrough.
  Auto-retry could be added once a real failure mode emerges.
- **`reset_for_logout` doesn't tear down persistence or queue** — TS clears
  these handles and the consumer reinitializes. Rust's ownership model makes
  mid-flight teardown unsound (background tasks still hold `Arc` references),
  so the Rust port resets auth + sync state in place; callers wanting a full
  reset should drop the `Store` and construct a new one.
- **Top-level entity wire propagation has only one wire test** — a basic
  propagation case is covered in `tests/wire.rs`. Stress tests under
  high-fanout or contention would be a natural extension.

---

## Wire compatibility with TS clients

The Rust port targets bidirectional interop with TS stitch clients on the same
broker. The wire-significant choices:

- **`version_field` defaults to `"version"`** (`StoreConfig::new`), matching
  TS. A Rust+TS pair using defaults agrees on which payload field carries the
  scope version.
- **`bump_scope_version`** publishes `$DB/{root}/{scope_id}/update` with
  `{[version_field]: now_ms, [updated_at_field]: now_ms}` after every successful
  child create/update/delete. The root must already exist on the server, same
  precondition as TS. `SyncEngine::applied_version(scope_id)` exposes the last
  ms value the local client wrote, mirroring TS `getAppliedVersion`.
- **Scoped topic op derivation** matches TS: scoped messages map the topic
  suffix (`created` / `updated` / `deleted`) onto `Operation`. Top-level
  messages read the payload's `operation` field (`Create` / `Update` /
  `Delete`).

---

## File layout

```
src/
├── config.rs          public config types
├── db_helpers.rs      pub(crate) shared open/register helpers
├── error.rs           Error enum + classifiers
├── lib.rs             module declarations + re-exports
├── memory_store.rs    MemoryStore + begin/end batch
├── offline_queue.rs   OfflineQueue trait + persistent/in-memory impls
├── origin.rs          Origin enum + skip helpers
├── persistence.rs     PersistenceLayer + recover()
├── remote_sync.rs     RemoteSyncLayer + LocalAccessor trait
├── store.rs           Store facade + StoreInner + background tasks
├── sync_engine.rs     SyncEngine over mqtt5::MqttClient
└── types.rs           MutationEvent, StoreEvent, Operation, etc.

tests/
├── common/mod.rs      BrokerFixture for wire tests
├── memory_store.rs    9 tests
├── offline_queue.rs   13 tests
├── persistence.rs     9 tests
├── remote_sync.rs     8 tests
├── store.rs           15 tests
└── wire.rs            2 broker-backed tests
```
