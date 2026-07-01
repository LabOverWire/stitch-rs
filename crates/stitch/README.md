# stitch-sync

Rust port of [`@laboverwire/stitch`](https://github.com/LabOverWire/stitch). Reactive state-sync library
bridging an in-memory store, fjall-backed local persistence, and MQTT-based remote
sync into a single `Store` interface. Compiles for both native and `wasm32`.

```toml
[dependencies]
stitch-sync = "0.2.3"
```

The crate is published as `stitch-sync` (the name `stitch` was taken) but is
imported as `stitch`:

```rust
use stitch::{Origin, Store, StoreConfig, StoreOptions};
```

```rust
use std::collections::HashMap;
use serde_json::json;
use stitch::config::{EntityDefinition, FieldType, PersistenceConfig, SchemaField, ScopeConfig};
use stitch::types::StoreEvent;
use stitch::{Origin, Store, StoreConfig, StoreOptions};

#[tokio::main]
async fn main() -> stitch::Result<()> {
    let mut entities = HashMap::new();
    entities.insert(
        "project".into(),
        EntityDefinition {
            fields: vec![
                SchemaField {
                    name: "id".into(),
                    r#type: FieldType::String,
                    required: true,
                    default: None,
                },
                SchemaField {
                    name: "name".into(),
                    r#type: FieldType::String,
                    required: false,
                    default: None,
                },
            ],
            ..EntityDefinition::default()
        },
    );

    let config = StoreConfig::new(
        entities,
        ScopeConfig {
            root_entity: "project".into(),
            child_entities: vec!["task".into()],
            scope_field: "projectId".into(),
        },
    );

    let options = StoreOptions {
        persistence: Some(PersistenceConfig {
            db_path: "./stitch-data".into(),
            passphrase: None,
        }),
        ..StoreOptions::default()
    };

    let store = Store::new(config, options);
    store.initialize().await?;

    let mut events = store.subscribe()?;
    tokio::spawn(async move {
        while let Ok(StoreEvent::Mutation(m)) = events.recv().await {
            println!("{:?} {}/{}", m.operation, m.entity, m.id);
        }
    });

    let mut data = serde_json::Map::new();
    data.insert("id".into(), json!("p1"));
    data.insert("name".into(), json!("Alpha"));
    store.create("project", "p1", data, Origin::Local).await?;
    store.replace_scope("p1").await?;

    let project = store.read("project", "p1").await?.expect("just created");
    println!("loaded: {:?}", project);

    store.shutdown().await?;
    Ok(())
}
```

A complete runnable example lives at [`examples/quickstart.rs`](./examples/quickstart.rs).

## What it does

- **Synchronous-feeling reads** for UI via `Store::read` / `Store::list`
- **Durable local state** via `mqdb-agent::Database` on fjall
- **Live multi-device sync** via MQTT5 with persisted sessions
- **Offline tolerance** — mutations queue locally and drain with consolidation
  (insert + N updates → single insert, etc.); a parked mutation whose direct
  sync lost the create→update race is retried within ~250ms rather than waiting
  for the next reconnect

## Status

Implements the CRUD-sync surface of TS stitch for the offline-first,
single-scope-at-a-time use case, including wire-compat with TS clients on the
same broker (matching `version_field` default, `bump_scope_version`,
top-level entity propagation). See [ARCHITECTURE.md](./ARCHITECTURE.md) for
layer composition, deliberate deviations from TS, and known gaps.

## Documentation

- [README.md](./README.md) — this file
- [ARCHITECTURE.md](./ARCHITECTURE.md) — internal design, data flow, deviations
  from TS, known gaps.
- `cargo doc --open` — per-type and per-method rustdoc for the public surface.
- [`examples/quickstart.rs`](./examples/quickstart.rs) — minimal runnable example

## Dependencies

- `mqdb-agent` / `mqdb-core` — local database (in-memory and fjall-backed)
- `mqtt5` — MQTT5 client and broker
- `tokio` — async runtime

All three are LabOverWire-owned. On `wasm32` the equivalent browser stack
(`mqdb-wasm`, `mqtt5-wasm`) is used instead; see
[`stitch-wasm`](../stitch-wasm) for the JavaScript bindings.

## License

Apache-2.0 — see [LICENSE](./LICENSE).
