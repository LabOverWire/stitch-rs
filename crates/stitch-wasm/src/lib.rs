//! Browser bindings for [`stitch`]. Exposes a `createStore` factory and a
//! `Store` class to JavaScript via `wasm-bindgen`. The store runs the
//! `mqdb-wasm` core: in-memory by default, or durable IndexedDB persistence
//! when `createStore` is given a `persistence` option. MQTT remote sync lands
//! in a later milestone.

use futures::channel::oneshot;
use futures::future::{Either, select};
use serde::Deserialize;
use std::collections::HashMap;
use stitch::config::{EntityDefinition, PersistenceConfig, RemoteConfig, ScopeConfig};
use stitch::types::{ListFilter, MutationEvent, Operation, Record, SortDirection, SortField};
use stitch::{Origin, Store as CoreStore, StoreConfig, StoreOptions};
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

#[derive(Deserialize)]
struct RemoteDto {
    url: String,
    #[serde(default, rename = "clientId")]
    client_id: Option<String>,
    #[serde(default)]
    ticket: Option<String>,
    #[serde(default)]
    username: Option<String>,
    #[serde(default)]
    password: Option<String>,
}

#[derive(Deserialize, Default)]
struct OptionsDto {
    #[serde(default)]
    persistence: Option<PersistenceDto>,
    #[serde(default)]
    remote: Option<RemoteDto>,
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

#[derive(Deserialize)]
struct SortFieldDto {
    field: String,
    #[serde(default)]
    direction: Option<String>,
}

impl From<SortFieldDto> for SortField {
    fn from(dto: SortFieldDto) -> Self {
        let direction = match dto.direction.as_deref() {
            Some("desc") => SortDirection::Desc,
            _ => SortDirection::Asc,
        };
        SortField {
            field: dto.field,
            direction,
        }
    }
}

#[derive(Deserialize, Default)]
struct ListFilterDto {
    #[serde(default, rename = "scopeId")]
    scope_id: Option<String>,
    #[serde(default)]
    sort: Vec<SortFieldDto>,
    #[serde(default)]
    projection: Vec<String>,
}

fn op_label(op: Operation) -> &'static str {
    match op {
        Operation::Insert => "insert",
        Operation::Update => "update",
        Operation::Delete => "delete",
    }
}

fn origin_from_tag(tag: Option<String>) -> Origin {
    match tag.as_deref() {
        Some("remote") => Origin::Remote,
        Some("load") => Origin::Load,
        Some("clear") => Origin::Clear,
        _ => Origin::Local,
    }
}

/// Spawn a forwarder that drives `on_event` for each entity mutation until the
/// returned unsubscribe function is called (or the channel closes). The returned
/// `JsValue` is a one-shot JS function; calling it cancels the subscription.
fn spawn_mutation_forwarder(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<MutationEvent>,
    on_event: impl Fn(MutationEvent) + 'static,
) -> JsValue {
    let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
    wasm_bindgen_futures::spawn_local(async move {
        futures::pin_mut!(cancel_rx);
        loop {
            let next = rx.recv();
            futures::pin_mut!(next);
            match select(next, &mut cancel_rx).await {
                Either::Left((Some(event), _)) => on_event(event),
                Either::Left((None, _)) | Either::Right(_) => break,
            }
        }
    });
    Closure::once_into_js(move || {
        let _ = cancel_tx.send(());
    })
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
/// Pass an optional second argument to enable durable IndexedDB persistence
/// and/or remote MQTT-over-WebSocket sync:
/// `{ persistence: { dbName, passphrase? }, remote: { url, clientId?, ticket?, username?, password? } }`.
/// With a persistence `passphrase` the store is AES-GCM encrypted. `remote.url`
/// must be a `ws://`/`wss://` endpoint; `remote.ticket` is a JWT used for MQTT v5
/// enhanced auth, and `remote.username` + `remote.password` drive classic MQTT
/// password auth when the broker isn't in JWT mode (ticket takes precedence).
/// `initialize` then connects and live
/// mutations flow through `subscribeToEntity`/`getSnapshot`. Omit the argument
/// for an in-memory store.
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
    let client_id = opts.remote.as_ref().and_then(|r| r.client_id.clone());
    let store_options = StoreOptions {
        persistence: opts.persistence.map(|p| PersistenceConfig {
            db_path: p.db_name.into(),
            passphrase: p.passphrase,
        }),
        remote: opts.remote.map(|r| {
            let mut cfg = RemoteConfig::new(r.url);
            cfg.ticket = r.ticket;
            cfg.username = r.username;
            cfg.password = r.password;
            cfg
        }),
    };
    let inner = match client_id {
        Some(id) => CoreStore::with_client_id(cfg, store_options, id),
        None => CoreStore::new(cfg, store_options),
    };
    Ok(Store { inner })
}

#[wasm_bindgen]
impl Store {
    /// Open the store (in-memory + persistence) and, when a remote is
    /// configured, connect to the broker. Idempotent.
    ///
    /// # Errors
    /// Returns an error if the underlying database fails to open.
    pub async fn initialize(&self) -> Result<(), JsValue> {
        self.inner.initialize().await.map_err(err)
    }

    /// Current remote connection status as a string (`"Offline"`,
    /// `"Connecting"`, `"Connected"`, `"Disconnected"`, `"Error"`). `"Offline"`
    /// when no remote is configured.
    ///
    /// # Errors
    /// Returns an error if the store is not initialized.
    #[wasm_bindgen(js_name = "connectionStatus")]
    pub fn connection_status(&self) -> Result<String, JsValue> {
        let status = self.inner.connection_status().map_err(err)?;
        Ok(format!("{status:?}"))
    }

    /// Set (or clear, with `null`/`undefined`) the authenticated user. Offline
    /// writes are scoped to this user and are dropped while it is unset, so call
    /// this before issuing writes you want queued for replay.
    ///
    /// # Errors
    /// Returns an error if the store is not initialized.
    #[wasm_bindgen(js_name = "setAuthenticatedUser")]
    pub fn set_authenticated_user(&self, user_id: Option<String>) -> Result<(), JsValue> {
        self.inner.set_authenticated_user(user_id).map_err(err)
    }

    /// Number of offline-queued mutations buffered for `scopeId` (the
    /// authenticated user's pending writes). `0` when no remote/queue is
    /// configured.
    ///
    /// # Errors
    /// Returns an error if the store is not initialized.
    #[wasm_bindgen(js_name = "pendingMutationCount")]
    pub async fn pending_mutation_count(&self, scope_id: String) -> Result<usize, JsValue> {
        self.inner.pending_count(&scope_id).await.map_err(err)
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
        tag: Option<String>,
    ) -> Result<String, JsValue> {
        let record = record_from_js(&data)?;
        self.inner
            .create(&entity, &scope_id, record, origin_from_tag(tag))
            .await
            .map_err(err)
    }

    /// Read a single row from the in-memory cache, or `null` if absent.
    ///
    /// # Errors
    /// Returns an error if the read fails.
    pub fn read(&self, entity: String, id: String) -> Result<JsValue, JsValue> {
        match self.inner.read_sync(&entity, &id).map_err(err)? {
            Some(record) => to_js(&serde_json::Value::Object(record)),
            None => Ok(JsValue::NULL),
        }
    }

    /// Partial-update a row.
    ///
    /// # Errors
    /// Returns an error if the write fails.
    pub async fn update(
        &self,
        entity: String,
        id: String,
        fields: JsValue,
        tag: Option<String>,
    ) -> Result<(), JsValue> {
        let record = record_from_js(&fields)?;
        self.inner
            .update(&entity, &id, record, origin_from_tag(tag))
            .await
            .map_err(err)
    }

    /// Delete a row. No-op if absent.
    ///
    /// # Errors
    /// Returns an error if the write fails.
    pub async fn delete(
        &self,
        entity: String,
        id: String,
        tag: Option<String>,
    ) -> Result<(), JsValue> {
        self.inner
            .delete(&entity, &id, origin_from_tag(tag))
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
    pub fn snapshot(&self, entity: String, scope_id: String) -> Result<JsValue, JsValue> {
        let rows = self.inner.snapshot_sync(&entity, &scope_id).map_err(err)?;
        let array = rows.into_iter().map(serde_json::Value::Object).collect();
        to_js(&serde_json::Value::Array(array))
    }

    /// List rows of `entity`, optionally filtered/sorted. `filter` is
    /// `{ scopeId?, sort?: [{ field, direction }], projection?: [string] }`.
    ///
    /// # Errors
    /// Returns an error if the read fails or `filter` is malformed.
    pub async fn list(&self, entity: String, filter: JsValue) -> Result<JsValue, JsValue> {
        let filter = if filter.is_undefined() || filter.is_null() {
            None
        } else {
            let dto: ListFilterDto = serde_json::from_value(json_from_js(&filter)?).map_err(err)?;
            Some(ListFilter {
                scope_id: dto.scope_id,
                sort: dto.sort.into_iter().map(SortField::from).collect(),
                projection: dto.projection,
            })
        };
        let rows = self.inner.list(&entity, filter).await.map_err(err)?;
        let array = rows.into_iter().map(serde_json::Value::Object).collect();
        to_js(&serde_json::Value::Array(array))
    }

    /// List all root entities across scopes, optionally sorted (`[{ field,
    /// direction }]`).
    ///
    /// # Errors
    /// Returns an error if the read fails or `sort` is malformed.
    #[wasm_bindgen(js_name = "listRootEntities")]
    pub async fn list_root_entities(&self, sort: JsValue) -> Result<JsValue, JsValue> {
        let sort: Vec<SortField> = if sort.is_undefined() || sort.is_null() {
            Vec::new()
        } else {
            let dtos: Vec<SortFieldDto> =
                serde_json::from_value(json_from_js(&sort)?).map_err(err)?;
            dtos.into_iter().map(SortField::from).collect()
        };
        let rows = self.inner.list_root_entities(sort).await.map_err(err)?;
        let array = rows.into_iter().map(serde_json::Value::Object).collect();
        to_js(&serde_json::Value::Array(array))
    }

    /// Count of rows of `entity` within `scopeId`.
    ///
    /// # Errors
    /// Returns an error if the read fails.
    #[wasm_bindgen(js_name = "getChildCount")]
    pub fn get_child_count(&self, entity: String, scope_id: String) -> Result<usize, JsValue> {
        self.inner.child_count_sync(&entity, &scope_id).map_err(err)
    }

    /// Monotonic mutation counter for `(scopeId, entity)`. Bumps on every
    /// mutation and on scope load/clear. Lets the JS layer cache snapshots and
    /// re-fetch only when this changes (referential stability for React).
    ///
    /// # Errors
    /// Returns an error if the store is not initialized.
    #[wasm_bindgen(js_name = "getVersion")]
    pub fn get_version(&self, scope_id: String, entity: String) -> Result<f64, JsValue> {
        self.inner
            .version(&scope_id, &entity)
            .map(|v| v as f64)
            .map_err(err)
    }

    /// Snapshot of `entity` within `scopeId` as an object keyed by row id.
    ///
    /// # Errors
    /// Returns an error if the read fails.
    #[wasm_bindgen(js_name = "getSnapshotAsMap")]
    pub fn get_snapshot_as_map(
        &self,
        entity: String,
        scope_id: String,
    ) -> Result<JsValue, JsValue> {
        let rows = self.inner.snapshot_sync(&entity, &scope_id).map_err(err)?;
        let mut map = serde_json::Map::new();
        for row in rows {
            let id = row.get("id").and_then(|v| v.as_str()).map(str::to_string);
            if let Some(id) = id {
                map.insert(id, serde_json::Value::Object(row));
            }
        }
        to_js(&serde_json::Value::Object(map))
    }

    /// Unsubscribe from a scope's remote topics and clear it from the active
    /// scope if it was current.
    ///
    /// # Errors
    /// Returns an error if the operation fails.
    #[wasm_bindgen(js_name = "closeScope")]
    pub async fn close_scope(&self, scope_id: String) -> Result<(), JsValue> {
        self.inner.close_scope(&scope_id).await.map_err(err)
    }

    /// `true` once [`Store::initialize`] has completed.
    #[must_use]
    pub fn ready(&self) -> bool {
        self.inner.ready()
    }

    /// `true` if durable IndexedDB persistence is configured.
    ///
    /// # Errors
    /// Returns an error if the store is not initialized.
    #[wasm_bindgen(js_name = "hasPersistence")]
    pub fn has_persistence(&self) -> Result<bool, JsValue> {
        self.inner.has_persistence().map_err(err)
    }

    /// `true` if remote sync is configured.
    ///
    /// # Errors
    /// Returns an error if the store is not initialized.
    #[wasm_bindgen(js_name = "hasRemote")]
    pub fn has_remote(&self) -> Result<bool, JsValue> {
        self.inner.has_remote().map_err(err)
    }

    /// `true` while the remote client is between connections.
    ///
    /// # Errors
    /// Returns an error if the store is not initialized.
    #[wasm_bindgen(js_name = "isReconnecting")]
    pub fn is_reconnecting(&self) -> Result<bool, JsValue> {
        self.inner.is_reconnecting().map_err(err)
    }

    /// Disconnect the remote client. Memory and persistence are unaffected.
    ///
    /// # Errors
    /// Returns an error if the disconnect fails.
    pub async fn disconnect(&self) -> Result<(), JsValue> {
        self.inner.disconnect().await.map_err(err)
    }

    /// Reconnect the remote client, optionally with a fresh JWT ticket or
    /// fresh username + password.
    ///
    /// # Errors
    /// Returns an error if no remote is configured or the reconnect fails.
    pub async fn reconnect(
        &self,
        server_url: String,
        ticket: Option<String>,
        username: Option<String>,
        password: Option<String>,
    ) -> Result<(), JsValue> {
        self.inner
            .reconnect(&server_url, ticket, username, password)
            .await
            .map_err(err)
    }

    /// Disconnect, clear auth + sync state. Persistence and the offline queue
    /// stay alive; drop the store to fully reset.
    ///
    /// # Errors
    /// Returns an error if the operation fails.
    #[wasm_bindgen(js_name = "resetForLogout")]
    pub async fn reset_for_logout(&self) -> Result<(), JsValue> {
        self.inner.reset_for_logout().await.map_err(err)
    }

    /// Disconnect the remote, close persistence, and abort background tasks.
    ///
    /// # Errors
    /// Returns an error if the operation fails.
    pub async fn destroy(&self) -> Result<(), JsValue> {
        self.inner.shutdown().await.map_err(err)
    }

    /// Defer memory-bus notifications until [`Store::end_batch`]; rapid bursts
    /// collapse to one event per `(scope, entity)`.
    ///
    /// # Errors
    /// Returns an error if the store is not initialized.
    #[wasm_bindgen(js_name = "beginBatch")]
    pub fn begin_batch(&self) -> Result<(), JsValue> {
        self.inner.begin_batch().map_err(err)
    }

    /// Flush the batch opened by [`Store::begin_batch`].
    ///
    /// # Errors
    /// Returns an error if the store is not initialized.
    #[wasm_bindgen(js_name = "endBatch")]
    pub fn end_batch(&self) -> Result<(), JsValue> {
        self.inner.end_batch().map_err(err)
    }

    /// Upsert into persistence directly, bypassing memory and remote.
    ///
    /// # Errors
    /// Returns an error if the write fails.
    #[wasm_bindgen(js_name = "updateLocalState")]
    pub async fn update_local_state(
        &self,
        entity: String,
        id: String,
        fields: JsValue,
    ) -> Result<(), JsValue> {
        let record = record_from_js(&fields)?;
        self.inner
            .update_local_state(&entity, &id, record)
            .await
            .map_err(err)
    }

    /// Send an arbitrary MQTT request and await the broker's response.
    ///
    /// # Errors
    /// Returns an error if no remote is configured or the request fails.
    pub async fn request(&self, topic: String, payload: JsValue) -> Result<JsValue, JsValue> {
        let payload = json_from_js(&payload)?;
        let record = self.inner.request(&topic, payload).await.map_err(err)?;
        to_js(&serde_json::Value::Object(record))
    }

    /// Subscribe to mutations for `entity`. The callback receives
    /// `(data | null, op)` where `op` is `"insert" | "update" | "delete"`.
    /// Returns an unsubscribe function.
    ///
    /// # Errors
    /// Returns an error if the store is not initialized.
    #[wasm_bindgen(js_name = "subscribeToEntity")]
    pub fn subscribe_to_entity(
        &self,
        entity: String,
        callback: js_sys::Function,
    ) -> Result<JsValue, JsValue> {
        let rx = self.inner.subscribe_entity(&entity).map_err(err)?;
        Ok(spawn_mutation_forwarder(rx, move |event| {
            let data = match event.data {
                Some(record) => to_js(&serde_json::Value::Object(record)).unwrap_or(JsValue::NULL),
                None => JsValue::NULL,
            };
            let _ = callback.call2(
                &JsValue::NULL,
                &data,
                &JsValue::from_str(op_label(event.operation)),
            );
        }))
    }

    /// Subscribe to mutations for `(scopeId, entity)`. The callback takes no
    /// args (a "scope changed" signal). Returns an unsubscribe function.
    ///
    /// # Errors
    /// Returns an error if the store is not initialized.
    #[wasm_bindgen(js_name = "subscribeToScope")]
    pub fn subscribe_to_scope(
        &self,
        scope_id: String,
        entity: String,
        callback: js_sys::Function,
    ) -> Result<JsValue, JsValue> {
        let rx = self
            .inner
            .subscribe_scope_entity(&scope_id, &entity)
            .map_err(err)?;
        Ok(spawn_mutation_forwarder(rx, move |_event| {
            let _ = callback.call0(&JsValue::NULL);
        }))
    }

    /// Subscribe to remote connection-status changes; the callback receives the
    /// status string. Returns an unsubscribe function (a no-op when no remote is
    /// configured).
    ///
    /// # Errors
    /// Returns an error if the store is not initialized.
    #[wasm_bindgen(js_name = "subscribeToConnectionStatus")]
    pub fn subscribe_to_connection_status(
        &self,
        callback: js_sys::Function,
    ) -> Result<JsValue, JsValue> {
        let Some(mut rx) = self.inner.subscribe_connection_status().map_err(err)? else {
            return Ok(Closure::once_into_js(|| {}));
        };
        let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
        wasm_bindgen_futures::spawn_local(async move {
            futures::pin_mut!(cancel_rx);
            loop {
                let next = rx.recv();
                futures::pin_mut!(next);
                match select(next, &mut cancel_rx).await {
                    Either::Left((Ok(status), _)) => {
                        let _ = callback
                            .call1(&JsValue::NULL, &JsValue::from_str(&format!("{status:?}")));
                    }
                    Either::Left((Err(RecvError::Lagged(_)), _)) => {}
                    Either::Left((Err(RecvError::Closed), _)) | Either::Right(_) => break,
                }
            }
        });
        Ok(Closure::once_into_js(move || {
            let _ = cancel_tx.send(());
        }))
    }
}
