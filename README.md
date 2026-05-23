# stitch

Native Rust port of [`@laboverwire/stitch`](../stitch). Reactive state-sync library
bridging an in-memory store, fjall-backed local persistence, and MQTT-based remote
sync into a single `Store` interface.

```toml
[dependencies]
stitch = { path = "..." } # path/git dep — not on crates.io
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
- **Offline tolerance** — mutations queue locally, drain on reconnect with
  consolidation (insert + N updates → single insert, etc.)

## Status

Implements the core CRUD-sync surface of TS stitch for the offline-first,
single-scope-at-a-time use case. Roughly 80% of the TS feature set. See
[ARCHITECTURE.md](./ARCHITECTURE.md) for layer composition, deliberate
deviations from TS, and known gaps (top-level entity wire tests, no inline
rustdoc, no corruption auto-retry in CRUD wrappers, etc.).

## Documentation

- [README.md](./README.md) — this file
- [ARCHITECTURE.md](./ARCHITECTURE.md) — internal design, data flow, deviations
  from TS, known gaps. **No inline rustdoc** — these markdown files are the
  primary documentation for now.
- [`examples/quickstart.rs`](./examples/quickstart.rs) — minimal runnable example

## Dependencies

- `mqdb-agent` / `mqdb-core` — local database (in-memory and fjall-backed)
- `mqtt5` — MQTT5 client and broker
- `tokio` — async runtime

All three are LabOverWire-owned. No WASM, no JS bindings — pure Rust target.

## License

Apache-2.0 — see [LICENSE](./LICENSE).
