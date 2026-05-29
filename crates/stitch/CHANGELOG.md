# Changelog

All notable changes to the `stitch` crate are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
