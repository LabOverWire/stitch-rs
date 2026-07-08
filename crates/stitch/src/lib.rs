//! Native Rust port of [`@laboverwire/stitch`]. Reactive state-sync library bridging
//! an in-memory store, fjall-backed local persistence, and MQTT-based remote sync
//! into a single [`Store`] interface.
//!
//! See [`README.md`](https://github.com/LabOverWire/stitch-rs/blob/main/crates/stitch/README.md) for
//! a runnable example and [`ARCHITECTURE.md`](https://github.com/LabOverWire/stitch-rs/blob/main/crates/stitch/ARCHITECTURE.md)
//! for the layer composition, data flow, and deliberate deviations from the TS
//! library.
//!
//! [`@laboverwire/stitch`]: https://github.com/LabOverWire/stitch
//!
//! # Quick start
//!
//! ```no_run
//! use std::collections::HashMap;
//! use serde_json::json;
//! use stitch::config::{EntityDefinition, FieldType, PersistenceConfig, SchemaField, ScopeConfig};
//! use stitch::{Origin, Store, StoreConfig, StoreOptions};
//!
//! # async fn run() -> stitch::Result<()> {
//! let mut entities = HashMap::new();
//! entities.insert("project".into(), EntityDefinition {
//!     fields: vec![SchemaField {
//!         name: "id".into(),
//!         r#type: FieldType::String,
//!         required: true,
//!         default: None,
//!     }],
//!     ..EntityDefinition::default()
//! });
//!
//! let config = StoreConfig::new(entities, ScopeConfig {
//!     root_entity: "project".into(),
//!     child_entities: vec![],
//!     scope_field: "projectId".into(),
//! });
//!
//! let store = Store::new(config, StoreOptions::default());
//! store.initialize().await?;
//!
//! let mut data = serde_json::Map::new();
//! data.insert("id".into(), json!("p1"));
//! store.create("project", "p1", data, Origin::Local).await?;
//! # Ok(())
//! # }
//! ```

pub mod config;
pub mod error;
pub mod types;

pub(crate) mod backend;
pub(crate) mod lock;
pub(crate) mod mqtt_client;
pub(crate) mod rt;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod db_helpers;

#[doc(hidden)]
pub mod memory_store;
#[doc(hidden)]
pub mod offline_queue;
#[doc(hidden)]
pub mod persistence;
#[doc(hidden)]
pub mod queue;
#[doc(hidden)]
pub mod remote_sync;
#[doc(hidden)]
pub mod sync_engine;

pub mod origin;
pub mod store;

pub use config::{
    EntityDefinition, ForeignKeyDefinition, OnDeleteAction, PersistenceConfig, RemoteConfig,
    SchemaField, ScopeConfig, StoreConfig, StoreOptions, TopLevelEntity, WillConfig,
};
pub use error::{Error, Result};
pub use origin::Origin;
pub use store::{ReconnectValidator, Store};
pub use types::{
    ConnectionStatus, ListFilter, MutationEvent, Operation, PendingMutation, Record, ScopeBundle,
    ScopeState, SortDirection, SortField, StoreEvent, SyncMutation,
};
