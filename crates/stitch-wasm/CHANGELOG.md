# Changelog

All notable changes to the `stitch-wasm` crate are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.6.5] - 2026-10-29

### Fixed

- A `create`/`update`/`delete` the broker refused to publish — e.g. a `sale`
  write denied by a topic ACL (PUBACK reason `0x87` Not authorized) — resolved
  successfully, left the optimistic local change in place, and was re-sent roughly
  four times a second until the page closed. It now rejects: a `0x87` refusal
  surfaces as `err.kind = "notAuthorized"` (other terminal reason codes as
  `"publishRejected"`), the optimistic local change is reverted (`create` drops
  the phantom row, `update` restores the previous values, `delete` reinstates the
  row), and the offline queue stops re-sending. A refusal with a retryable reason
  code (e.g. quota exceeded) is still retried. Inherited from stitch-sync; see its
  changelog.

## [0.6.4] - 2026-10-28

### Fixed

- A row created with `create` while a scope was being opened via `replaceScope`
  could vanish from the local store when the scope snapshot arrived, while the
  server kept it and no error was raised. Inherited from stitch-sync: own-origin
  events for a scope awaiting its snapshot are now buffered and replayed after
  the reconcile, so a write acknowledged after the open began survives. See the
  stitch-sync changelog.

## [0.6.3] - 2026-10-28

### Fixed

- `update`/`delete` on a connected store resolved successfully when the broker
  answered `404` for a row the client still held locally (a stale copy that
  missed a delete), so the promise never rejected and the phantom row stayed in
  the store. They now reject with an `err.kind = "notFound"` error and drop the
  stale local row; `updateIf`/`deleteIf` likewise drop it, and the `notFound`
  message now names the entity and id instead of reading `entity not found: /`.
  Inherited from stitch-sync; see its changelog.

## [0.6.2] - 2026-10-26

### Fixed

- A second viewer opening a scope it did not create saw an empty view a large
  fraction of the time, even when the open was issued while connected. Inherited
  from stitch-sync: on wasm, a connected `replaceScope` now fetches the scope's
  root and children from the server instead of only loading local persistence,
  so a late subscriber converges without a reload. See the stitch-sync changelog.

## [0.6.1] - 2026-10-25

### Fixed

- A scope opened before the remote connected rebuilt empty and never backfilled
  on connect, leaving a permanently empty view until a reload. Inherited from
  stitch-sync: the store now re-opens the current scope on connect, so a scope
  opened during connection warmup converges. See the stitch-sync changelog.

## [0.6.0] - 2026-10-22

### Added

- `Store.updateIf` / `Store.deleteIf` — compare-and-set update/delete gated on a
  row's server `_version` (pass the value read from a prior row; `updateIf`
  resolves to the new version). A stale version rejects with an
  `err.kind = "preconditionFailed"` error and makes no local change. Mirrors
  stitch-sync `update_if`/`delete_if`; requires a connected remote.

## [0.5.1] - 2026-10-17

### Fixed

- Creating, updating or deleting a child row with a remote no longer panics with `time not implemented on this platform` and leaves the store unusable. Inherited from `stitch-sync` (LabOverWire/MQDB#187).

### Changed

- Built on stitch-sync with mqtt5-wasm 2.1.7. A store reloaded with the same client id now resumes its broker session instead of failing to connect; see the stitch-sync changelog. Minimum Rust version is 1.89.

## [0.5.0] - 2026-08-29

### Changed

- **Breaking:** a failed `Store` method now rejects with a structured JS `Error`
  instead of a plain string. The `Error` carries `message` (the previous text)
  and a stable `kind` discriminant — one of `conflict`, `ownership`, `notFound`,
  `timeout`, `connectionClosed`, `sessionInvalid`, `mqtt`, `corruption` (a
  corrupt local store — recover via `recoverPersistence`), `constraint` (a
  permanent local schema/unique/foreign-key violation), `mqdb`, `config`,
  `serde`, `io`, `notInitialized`, `alreadyInitialized`, `scopeNotActive`,
  `unknownEntity`, `invalidInput` (a malformed argument), or `internal` (an
  internal serialization fault) — plus `entity` and `id` on the variants that
  carry them. Branch on `err.kind === "conflict"` instead of matching message
  text. Callers that matched `String(err)` (e.g.
  `String(err).startsWith("conflict for")`) must switch to `err.kind` or
  `err.message`, since `String(err)` is now `"Error: conflict for …"`.

## [0.4.0] - 2026-08-26

### Fixed

- `Store.create` (the `create` binding) now rejects with the broker error when an
  insert loses a unique-key race (409 `Conflict`) or is denied (403 `Ownership`),
  rolling back the optimistic local row, instead of resolving with an id and
  leaving a phantom row. A browser client can now claim an exclusive key — e.g. a
  seat hold keyed by a UNIQUE constraint — and observe the loss via a rejected
  promise. Inherited from `stitch-sync`.

## [0.3.0] - 2026-08-11

### Added

- `createStore`'s `remote` options accept `autoConnect` (default `true`). Set
  `remote.autoConnect: false` to skip the connect that `initialize` otherwise
  performs, and drive the authenticated connect yourself via `reconnect`. This
  suits the dynamic-ticket pattern (a JWT minted per connection): without it,
  `initialize` fires a ticketless connect that an auth-requiring broker rejects
  before the app's `reconnect` succeeds.

## [0.2.2] - 2026-08-06

### Fixed

- `createStore`'s config now honors `responseTopicPrefix`, `syncTopicPrefix`,
  `versionField`, `updatedAtField`, and `userScopeField`, plus `topLevelEntities`
  and `localOnlyEntities`. These fields were absent from the config deserializer,
  so serde silently dropped them and the baked-in defaults (`$DB` sync prefix,
  `$DB/clients` response prefix, `version`, `updatedAt`) always won regardless of
  the override. Deployments overriding `responseTopicPrefix` — e.g. to keep RPC
  responses out of a broker's reserved `$DB` namespace — now take effect.

## [0.2.1] - 2026-07-17

### Fixed

- `subscribeToScope` and `subscribeToEntity` now fire when a scope is loaded or
  cleared via `replaceScope` / `loadScope` / `clearScope`, not only on remote
  mutations — so reactive bindings re-read the snapshot after a scope opens.

## [0.2.0] - 2026-07-11

### Added

- `createStore`'s `remote` options accept an MQTT 5 `will` (`{ topic, payload,
  qos?, retain?, willDelayIntervalSecs?, contentType? }`) and `keepAliveSecs`,
  registering a last-will/testament on the connection. The broker publishes the
  will on an ungraceful disconnect (crash, tab close, network loss) and clears
  it on a normal disconnect.

## [0.1.0]

- Initial release: browser (`wasm-bindgen`) bindings over `stitch-sync`. A
  `createStore` factory and `Store` class for JavaScript mirroring the
  framework-agnostic core of the TypeScript `@laboverwire/stitch`: CRUD, reads
  (`list`/`listRootEntities`/`getChildCount`/`getSnapshot`/`getSnapshotAsMap`),
  scope ops, subscriptions, connection management, batch, `request`,
  local-state, durable IndexedDB persistence, and remote MQTT sync over
  WebSocket with JWT enhanced-auth.
