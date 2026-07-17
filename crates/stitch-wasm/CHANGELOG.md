# Changelog

All notable changes to the `stitch-wasm` crate are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
