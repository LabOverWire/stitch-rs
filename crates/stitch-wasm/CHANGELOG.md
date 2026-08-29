# Changelog

All notable changes to the `stitch-wasm` crate are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **Breaking:** a failed `Store` method now rejects with a structured JS `Error`
  instead of a plain string. The `Error` carries `message` (the previous text)
  and a stable `kind` discriminant — one of `conflict`, `ownership`, `notFound`,
  `timeout`, `connectionClosed`, `sessionInvalid`, `mqtt`, `mqdb`, `config`,
  `serde`, `io`, `notInitialized`, `alreadyInitialized`, `scopeNotActive`,
  `unknownEntity`, or `invalidInput` (for malformed arguments) — plus `entity`
  and `id` on the variants that carry them. Branch on `err.kind === "conflict"`
  instead of matching message text. Callers that matched `String(err)` (e.g.
  `String(err).startsWith("conflict for")`) must switch to `err.kind` or
  `err.message`, since `String(err)` is now `"Error: conflict for …"`.

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
