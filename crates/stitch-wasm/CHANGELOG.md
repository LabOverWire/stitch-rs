# Changelog

All notable changes to the `stitch-wasm` crate are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0]

- Initial release: browser (`wasm-bindgen`) bindings over `stitch-sync`. A
  `createStore` factory and `Store` class for JavaScript mirroring the
  framework-agnostic core of the TypeScript `@laboverwire/stitch`: CRUD, reads
  (`list`/`listRootEntities`/`getChildCount`/`getSnapshot`/`getSnapshotAsMap`),
  scope ops, subscriptions, connection management, batch, `request`,
  local-state, durable IndexedDB persistence, and remote MQTT sync over
  WebSocket with JWT enhanced-auth.
