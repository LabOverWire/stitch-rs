//! Browser bindings for [`stitch`]. Exposes a `createStore` factory and a
//! `Store` class to JavaScript via `wasm-bindgen`. The store runs the
//! `mqdb-wasm` core: in-memory by default, or durable IndexedDB persistence
//! when `createStore` is given a `persistence` option. MQTT remote sync lands
//! in a later milestone.

use serde::Deserialize;
use std::collections::HashMap;
use stitch::config::{EntityDefinition, PersistenceConfig, ScopeConfig};
use stitch::types::Record;
use stitch::{Origin, Store as CoreStore, StoreConfig, StoreEvent, StoreOptions};
use tokio::sync::broadcast::error::RecvError;
use wasm_bindgen::prelude::*;

#[derive(Deserialize)]
struct ScopeDto {
    #[serde(rename = "rootEntity")]
    root_entity: String,
    #[serde(default, rename = "childEntities")]
    child_entities: Vec<String>,
    #[serde(rename = "scopeField")]
    scope_field: String,
}

#[derive(Deserialize)]
struct ConfigDto {
    entities: HashMap<String, EntityDefinition>,
    scope: ScopeDto,
}

#[derive(Deserialize)]
struct PersistenceDto {
    #[serde(rename = "dbName")]
    db_name: String,
    #[serde(default)]
    passphrase: Option<String>,
}

#[derive(Deserialize, Default)]
struct OptionsDto {
    #[serde(default)]
    persistence: Option<PersistenceDto>,
}

fn err<E: std::fmt::Display>(e: E) -> JsValue {
    JsValue::from_str(&e.to_string())
}

fn to_js(value: &serde_json::Value) -> Result<JsValue, JsValue> {
    let text = serde_json::to_string(value).map_err(err)?;
    js_sys::JSON::parse(&text)
}

fn json_from_js(js: &JsValue) -> Result<serde_json::Value, JsValue> {
    let text = js_sys::JSON::stringify(js)?;
    serde_json::from_str(&String::from(text)).map_err(err)
}

fn record_from_js(js: &JsValue) -> Result<Record, JsValue> {
    match json_from_js(js)? {
        serde_json::Value::Object(map) => Ok(map),
        other => Err(JsValue::from_str(&format!("expected object, got {other}"))),
    }
}

/// Reactive state-sync store, browser flavour. Construct with [`create_store`],
/// call [`Store::initialize`] once, then issue CRUD calls.
#[wasm_bindgen]
pub struct Store {
    inner: CoreStore,
}

/// Build an unopened [`Store`] from a config object
/// (`{ entities, scope: { rootEntity, childEntities, scopeField } }`).
///
/// Pass an optional second argument to enable durable IndexedDB persistence:
/// `{ persistence: { dbName, passphrase? } }`. With `passphrase` the store is
/// AES-GCM encrypted; without it, plaintext IndexedDB. Omit the argument for an
/// in-memory store.
///
/// # Errors
/// Returns an error if the config or options object is malformed.
#[wasm_bindgen(js_name = "createStore")]
pub fn create_store(config: JsValue, options: JsValue) -> Result<Store, JsValue> {
    let dto: ConfigDto = serde_json::from_value(json_from_js(&config)?).map_err(err)?;
    let opts: OptionsDto = if options.is_undefined() || options.is_null() {
        OptionsDto::default()
    } else {
        serde_json::from_value(json_from_js(&options)?).map_err(err)?
    };
    let cfg = StoreConfig::new(
        dto.entities,
        ScopeConfig {
            root_entity: dto.scope.root_entity,
            child_entities: dto.scope.child_entities,
            scope_field: dto.scope.scope_field,
        },
    );
    let store_options = StoreOptions {
        persistence: opts.persistence.map(|p| PersistenceConfig {
            db_path: p.db_name.into(),
            passphrase: p.passphrase,
        }),
        remote: None,
    };
    Ok(Store {
        inner: CoreStore::new(cfg, store_options),
    })
}

#[wasm_bindgen]
impl Store {
    /// Open the in-memory store. Idempotent.
    ///
    /// # Errors
    /// Returns an error if the underlying database fails to open.
    pub async fn initialize(&self) -> Result<(), JsValue> {
        self.inner.initialize().await.map_err(err)
    }

    /// Insert a row into `entity` under `scopeId`. Returns the new row's id.
    ///
    /// # Errors
    /// Returns an error if the write fails.
    pub async fn create(
        &self,
        entity: String,
        scope_id: String,
        data: JsValue,
    ) -> Result<String, JsValue> {
        let record = record_from_js(&data)?;
        self.inner
            .create(&entity, &scope_id, record, Origin::Local)
            .await
            .map_err(err)
    }

    /// Read a single row from the in-memory cache, or `null` if absent.
    ///
    /// # Errors
    /// Returns an error if the read fails.
    pub async fn read(&self, entity: String, id: String) -> Result<JsValue, JsValue> {
        match self.inner.read(&entity, &id).await.map_err(err)? {
            Some(record) => to_js(&serde_json::Value::Object(record)),
            None => Ok(JsValue::NULL),
        }
    }

    /// Partial-update a row.
    ///
    /// # Errors
    /// Returns an error if the write fails.
    pub async fn update(&self, entity: String, id: String, fields: JsValue) -> Result<(), JsValue> {
        let record = record_from_js(&fields)?;
        self.inner
            .update(&entity, &id, record, Origin::Local)
            .await
            .map_err(err)
    }

    /// Delete a row. No-op if absent.
    ///
    /// # Errors
    /// Returns an error if the write fails.
    pub async fn delete(&self, entity: String, id: String) -> Result<(), JsValue> {
        self.inner
            .delete(&entity, &id, Origin::Local)
            .await
            .map_err(err)
    }

    /// Read a row straight from durable persistence, bypassing the in-memory
    /// cache. Returns `null` if absent. Falls back to memory when the store has
    /// no persistence configured.
    ///
    /// # Errors
    /// Returns an error if the read fails.
    #[wasm_bindgen(js_name = "readLocalState")]
    pub async fn read_local_state(&self, entity: String, id: String) -> Result<JsValue, JsValue> {
        match self
            .inner
            .read_local_state(&entity, &id)
            .await
            .map_err(err)?
        {
            Some(record) => to_js(&serde_json::Value::Object(record)),
            None => Ok(JsValue::NULL),
        }
    }

    /// Switch the active scope, loading its rows from persistence into the
    /// in-memory cache. Use after reopening a persistent store to rehydrate a
    /// scope's snapshot.
    ///
    /// # Errors
    /// Returns an error if the load fails.
    #[wasm_bindgen(js_name = "replaceScope")]
    pub async fn replace_scope(&self, scope_id: String) -> Result<(), JsValue> {
        self.inner.replace_scope(&scope_id).await.map_err(err)
    }

    /// Snapshot of all rows for `entity` within `scopeId`. Equivalent to TS
    /// `getSnapshot`.
    ///
    /// # Errors
    /// Returns an error if the read fails.
    #[wasm_bindgen(js_name = "getSnapshot")]
    pub async fn snapshot(&self, entity: String, scope_id: String) -> Result<JsValue, JsValue> {
        let rows = self.inner.snapshot(&entity, &scope_id).await.map_err(err)?;
        let array = rows.into_iter().map(serde_json::Value::Object).collect();
        to_js(&serde_json::Value::Array(array))
    }

    /// Invoke `callback` (no args) whenever a mutation for `entity` lands on the
    /// memory bus.
    ///
    /// # Errors
    /// Returns an error if the store is not initialized.
    #[wasm_bindgen(js_name = "subscribeToEntity")]
    pub fn subscribe_to_entity(
        &self,
        entity: String,
        callback: js_sys::Function,
    ) -> Result<(), JsValue> {
        let mut rx = self.inner.subscribe().map_err(err)?;
        wasm_bindgen_futures::spawn_local(async move {
            loop {
                match rx.recv().await {
                    Ok(StoreEvent::Mutation(mutation)) if mutation.entity == entity => {
                        let _ = callback.call0(&JsValue::NULL);
                    }
                    Ok(_) => {}
                    Err(RecvError::Lagged(_)) => {}
                    Err(RecvError::Closed) => break,
                }
            }
        });
        Ok(())
    }
}
