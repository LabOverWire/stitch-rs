use crate::config::{StoreConfig, StoreOptions};
use crate::error::{Error, Result};
use crate::memory_store::MemoryStore;
use crate::offline_queue::{InMemoryOfflineQueue, OfflineQueue, PersistentOfflineQueue};
use crate::origin::Origin;
use crate::persistence::PersistenceLayer;
use crate::remote_sync::{LocalAccessor, RemoteSyncLayer};
use crate::sync_engine::MutationDelivery;
use crate::types::{
    ConnectionStatus, ListFilter, MutationEvent, Operation, Record, ScopeBundle, SortDirection,
    SortField, StoreEvent, SyncMutation,
};
use async_trait::async_trait;
use mqdb_core::types::{
    Filter, FilterOp, SortDirection as MqdbSortDirection, SortOrder as MqdbSortOrder,
};
use serde_json::Value;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio::sync::{OnceCell, broadcast};
use tokio::task::JoinHandle;
use uuid::Uuid;

/// Reactive state-sync facade over an in-memory cache, fjall persistence, and
/// MQTT5 remote sync. Construct with [`Store::new`], call [`Store::initialize`]
/// once, then issue CRUD calls and subscribe to mutation streams.
///
/// `Store` is `Send + Sync` and intended to be held in an `Arc` and shared
/// across tasks.
pub struct Store {
    config: Arc<StoreConfig>,
    options: StoreOptions,
    client_id: String,
    inner: OnceCell<Arc<StoreInner>>,
}

/// Async validator fired at the start of every successful (re)connect. Use it
/// to hit your own auth check before mutations flush. Returning an `Err` is
/// logged at `warn` level and does not abort the connect; pair this with
/// [`Store::set_session_invalid_handler`] for hard logout.
pub type ReconnectValidator =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync>;

struct StoreInner {
    config: Arc<StoreConfig>,
    memory: Arc<MemoryStore>,
    persistence: Option<Arc<PersistenceLayer>>,
    remote: Option<Arc<RemoteSyncLayer>>,
    queue: Option<Arc<dyn OfflineQueue>>,
    state: Mutex<StoreState>,
    reconnect_validator: Mutex<Option<ReconnectValidator>>,
    replace_scope_lock: tokio::sync::Mutex<()>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

struct StoreState {
    current_scope: Option<String>,
    initial_sync_done: bool,
    authenticated_user: Option<String>,
    last_connection_status: ConnectionStatus,
    has_been_connected: bool,
}

impl Store {
    /// Construct an unopened `Store` with a freshly generated client id. Call
    /// [`Store::initialize`] before issuing any other operation.
    #[must_use]
    pub fn new(config: StoreConfig, options: StoreOptions) -> Self {
        Self::with_client_id(config, options, Uuid::now_v7().to_string())
    }

    /// Like [`Store::new`] but with a caller-supplied MQTT client id. The id
    /// must not contain `+`, `#`, or `/`.
    #[must_use]
    pub fn with_client_id(config: StoreConfig, options: StoreOptions, client_id: String) -> Self {
        Self {
            config: Arc::new(config),
            options,
            client_id,
            inner: OnceCell::new(),
        }
    }

    /// Open the inner layers (memory + optional persistence + optional remote)
    /// and start background tasks. Idempotent: subsequent calls return `Ok(())`
    /// without re-initializing.
    pub async fn initialize(&self) -> Result<()> {
        self.inner
            .get_or_try_init(|| async {
                let inner =
                    StoreInner::open(Arc::clone(&self.config), &self.options, &self.client_id)
                        .await?;
                Ok::<Arc<StoreInner>, Error>(inner)
            })
            .await?;
        Ok(())
    }

    fn inner(&self) -> Result<&Arc<StoreInner>> {
        self.inner.get().ok_or(Error::NotInitialized)
    }

    /// Broadcast receiver for the memory bus. Sees mutation events for the
    /// currently-active scope plus `ScopeLoaded` / `ScopeCleared` signals.
    pub fn subscribe(&self) -> Result<broadcast::Receiver<StoreEvent>> {
        Ok(self.inner()?.memory.subscribe())
    }

    /// Broadcast receiver for the persistence bus. Sees every persisted write
    /// across all scopes — useful for cross-scope observation (e.g. a
    /// dashboard of all root entities). Returns `None` when persistence is not
    /// configured.
    pub fn subscribe_persistence(&self) -> Result<Option<broadcast::Receiver<StoreEvent>>> {
        Ok(self.inner()?.persistence.as_ref().map(|p| p.subscribe()))
    }

    /// Stream of mutation events filtered to a single entity. When
    /// persistence is configured, the memory-bus filter additionally requires
    /// `Origin::Load` or `Origin::Clear` (mirroring TS's "snapshot refresh"
    /// semantics) and the persistence bus is forwarded as-is.
    ///
    /// The returned receiver is an unbounded mpsc and supports only a single
    /// consumer (no `resubscribe`). Drop the receiver to stop the forwarder
    /// task. Use [`Store::subscribe`] if you need broadcast semantics.
    pub fn subscribe_entity(
        &self,
        entity: &str,
    ) -> Result<tokio::sync::mpsc::UnboundedReceiver<MutationEvent>> {
        let inner = self.inner()?;
        let mem_rx = inner.memory.subscribe();
        let persistence_rx = inner.persistence.as_ref().map(|p| p.subscribe());
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let entity_owned = entity.to_string();
        let has_persistence = persistence_rx.is_some();

        spawn_filtered_forwarder(
            mem_rx,
            tx.clone(),
            entity_owned.clone(),
            None,
            move |event| {
                if has_persistence {
                    matches!(event.origin, Origin::Load | Origin::Clear)
                } else {
                    true
                }
            },
            &inner.tasks,
        );

        if let Some(rx_persistence) = persistence_rx {
            spawn_filtered_forwarder(
                rx_persistence,
                tx,
                entity_owned,
                None,
                |_| true,
                &inner.tasks,
            );
        }

        Ok(rx)
    }

    /// Stream of mutation events filtered to a specific `(scope_id, entity)`
    /// pair on the memory bus. Single-consumer unbounded mpsc; see
    /// [`Store::subscribe_entity`] for caveats.
    pub fn subscribe_scope_entity(
        &self,
        scope_id: &str,
        entity: &str,
    ) -> Result<tokio::sync::mpsc::UnboundedReceiver<MutationEvent>> {
        let inner = self.inner()?;
        let mem_rx = inner.memory.subscribe();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        spawn_filtered_forwarder(
            mem_rx,
            tx,
            entity.to_string(),
            Some(scope_id.to_string()),
            |_| true,
            &inner.tasks,
        );
        Ok(rx)
    }

    /// Defer memory-bus notifications. While the batch counter is non-zero,
    /// mutations replace entries in a `HashMap<(scope, entity), MutationEvent>`,
    /// so rapid bursts collapse to one event per unique `(scope, entity)`.
    /// Calls nest; the outermost [`Store::end_batch`] drains and emits.
    pub fn begin_batch(&self) -> Result<()> {
        self.inner()?.memory.begin_batch();
        Ok(())
    }

    /// Pair to [`Store::begin_batch`]; the outermost call flushes the
    /// deduplicated buffer onto the memory bus.
    pub fn end_batch(&self) -> Result<()> {
        self.inner()?.memory.end_batch();
        Ok(())
    }

    /// Current remote connection status, or `Offline` if no remote is
    /// configured.
    pub fn connection_status(&self) -> Result<ConnectionStatus> {
        let inner = self.inner()?;
        Ok(inner.state.lock().unwrap().last_connection_status)
    }

    /// Subscribe to remote connection-status changes. `None` when no remote is
    /// configured.
    pub fn subscribe_connection_status(
        &self,
    ) -> Result<Option<broadcast::Receiver<ConnectionStatus>>> {
        Ok(self
            .inner()?
            .remote
            .as_ref()
            .map(|r| r.subscribe_connection_status()))
    }

    /// Set or clear the authenticated user. Passes through to the offline
    /// queue so persisted rows are scoped to the user that wrote them.
    pub fn set_authenticated_user(&self, user_id: Option<String>) -> Result<()> {
        let inner = self.inner()?;
        inner.state.lock().unwrap().authenticated_user = user_id.clone();
        if let Some(q) = &inner.queue {
            q.set_authenticated_user(user_id);
        }
        Ok(())
    }

    /// The currently-loaded scope id, or `None` if none is active.
    pub fn current_scope(&self) -> Result<Option<String>> {
        Ok(self.inner()?.state.lock().unwrap().current_scope.clone())
    }

    /// `true` once [`Store::initialize`] has completed successfully.
    #[must_use]
    pub fn ready(&self) -> bool {
        self.inner.get().is_some()
    }

    /// `true` once the first post-connect sync has finished. When no remote is
    /// configured this is `true` immediately after `initialize`.
    pub fn initial_sync_done(&self) -> Result<bool> {
        Ok(self.inner()?.state.lock().unwrap().initial_sync_done)
    }

    /// `true` if the client has been connected at least once and the current
    /// status indicates the underlying mqtt5 client is between connections
    /// (`Connecting` during the active retry, or `Disconnected` during the
    /// backoff window before the next retry fires). `false` after explicit
    /// disconnect or a terminal error.
    pub fn is_reconnecting(&self) -> Result<bool> {
        let state = self.inner()?.state.lock().unwrap();
        Ok(state.has_been_connected
            && matches!(
                state.last_connection_status,
                ConnectionStatus::Connecting | ConnectionStatus::Disconnected
            ))
    }

    /// Insert a row. For child entities the `scope_id` argument identifies the
    /// owning scope; for the root entity it's ignored and the row's own id
    /// becomes the scope. Returns the new row's id (either taken from the
    /// `data.id` field or freshly generated).
    pub async fn create(
        &self,
        entity: &str,
        scope_id: &str,
        mut data: Record,
        origin: Origin,
    ) -> Result<String> {
        let inner = self.inner()?;
        let root_entity = &inner.config.scope.root_entity;
        let is_top_level = inner
            .config
            .top_level_entities
            .iter()
            .any(|t| t.entity == entity);
        let id = data
            .get("id")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| Uuid::now_v7().to_string());
        data.insert("id".to_string(), Value::String(id.clone()));
        let effective_scope = if entity == root_entity {
            id.clone()
        } else {
            scope_id.to_string()
        };
        let has_sync_target = !effective_scope.is_empty() || is_top_level;

        inner
            .memory
            .create(entity, &effective_scope, data.clone(), origin)
            .await?;

        if let Some(persistence) = &inner.persistence
            && !origin.skips_persistence()
        {
            let _ = persistence.create(entity, data.clone(), origin).await;
        }

        if let Some(queue) = &inner.queue
            && has_sync_target
            && !origin.skips_remote()
        {
            queue
                .queue(crate::types::PendingMutation {
                    op: Operation::Insert,
                    entity: entity.to_string(),
                    id: id.clone(),
                    scope_id: effective_scope.clone(),
                    data: Some(data.clone()),
                    created_at: 0,
                })
                .await?;
        }

        if let Some(remote) = &inner.remote
            && !origin.skips_remote()
            && inner.state.lock().unwrap().last_connection_status == ConnectionStatus::Connected
            && has_sync_target
        {
            match remote.sync_create(entity, &effective_scope, data).await {
                Ok(_) => {
                    if let Some(queue) = &inner.queue {
                        let _ = queue
                            .remove(entity, &id, &effective_scope, Operation::Insert)
                            .await;
                    }
                }
                Err(err) if err.is_ownership() => {
                    if let Some(queue) = &inner.queue {
                        let _ = queue
                            .remove(entity, &id, &effective_scope, Operation::Insert)
                            .await;
                    }
                }
                Err(err) if err.is_transient() => {}
                Err(err) => {
                    tracing::warn!(
                        entity = %entity,
                        id = %id,
                        error = %err,
                        "remote create failed"
                    );
                }
            }
        }

        Ok(id)
    }

    /// Partial-update an existing row. `fields` is merged into the row;
    /// null-valued entries are stripped before write (matching TS behavior).
    pub async fn update(
        &self,
        entity: &str,
        id: &str,
        fields: Record,
        origin: Origin,
    ) -> Result<()> {
        let inner = self.inner()?;
        let scope_field = &inner.config.scope.scope_field;
        let root_entity = &inner.config.scope.root_entity;
        let is_top_level = inner
            .config
            .top_level_entities
            .iter()
            .any(|t| t.entity == entity);

        let existing = inner.memory.read(entity, id).await?;
        let mut scope_id = existing.as_ref().and_then(|e| {
            if entity == root_entity {
                e.get("id").and_then(Value::as_str).map(str::to_string)
            } else if is_top_level {
                Some(String::new())
            } else {
                e.get(scope_field)
                    .and_then(Value::as_str)
                    .map(str::to_string)
            }
        });

        if existing.is_some() && scope_id.is_some() {
            inner
                .memory
                .update(entity, id, fields.clone(), origin)
                .await?;
        }

        if let Some(persistence) = &inner.persistence
            && !origin.skips_persistence()
        {
            if scope_id.is_none()
                && let Some(p_existing) = persistence.read(entity, id).await?
            {
                scope_id = if entity == root_entity {
                    p_existing
                        .get("id")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                } else if is_top_level {
                    Some(String::new())
                } else {
                    p_existing
                        .get(scope_field)
                        .and_then(Value::as_str)
                        .map(str::to_string)
                };
                if scope_id.is_some() && existing.is_none() {
                    inner
                        .memory
                        .update(entity, id, fields.clone(), origin)
                        .await
                        .ok();
                }
            }
            if scope_id.is_some() {
                let _ = persistence.update(entity, id, fields.clone(), origin).await;
            }
        } else if existing.is_none() {
            return Ok(());
        }

        let Some(scope_id) = scope_id else {
            return Ok(());
        };

        if let Some(queue) = &inner.queue
            && !origin.skips_remote()
        {
            queue
                .queue(crate::types::PendingMutation {
                    op: Operation::Update,
                    entity: entity.to_string(),
                    id: id.to_string(),
                    scope_id: scope_id.clone(),
                    data: Some(fields.clone()),
                    created_at: 0,
                })
                .await?;
        }

        if let Some(remote) = &inner.remote
            && !origin.skips_remote()
            && inner.state.lock().unwrap().last_connection_status == ConnectionStatus::Connected
        {
            match remote.sync_update(entity, &scope_id, id, fields).await {
                Ok(()) => {
                    if let Some(queue) = &inner.queue {
                        let _ = queue.remove(entity, id, &scope_id, Operation::Update).await;
                    }
                }
                Err(err) if err.is_ownership() => {
                    if let Some(queue) = &inner.queue {
                        let _ = queue.remove(entity, id, &scope_id, Operation::Update).await;
                    }
                }
                Err(err) if err.is_transient() => {}
                Err(err) => {
                    tracing::warn!(entity = %entity, id = %id, error = %err, "remote update failed");
                }
            }
        }

        Ok(())
    }

    /// Delete a row. No-op if the row doesn't exist.
    pub async fn delete(&self, entity: &str, id: &str, origin: Origin) -> Result<()> {
        let inner = self.inner()?;
        let scope_field = &inner.config.scope.scope_field;
        let root_entity = &inner.config.scope.root_entity;
        let is_top_level = inner
            .config
            .top_level_entities
            .iter()
            .any(|t| t.entity == entity);

        let existing = inner.memory.read(entity, id).await?;
        let mut scope_id = existing.as_ref().and_then(|e| {
            if entity == root_entity {
                e.get("id").and_then(Value::as_str).map(str::to_string)
            } else if is_top_level {
                Some(String::new())
            } else {
                e.get(scope_field)
                    .and_then(Value::as_str)
                    .map(str::to_string)
            }
        });

        if existing.is_some() && scope_id.is_some() {
            inner.memory.delete(entity, id, origin).await.ok();
        }

        if let Some(persistence) = &inner.persistence
            && !origin.skips_persistence()
        {
            if scope_id.is_none()
                && let Some(p_existing) = persistence.read(entity, id).await?
            {
                scope_id = if entity == root_entity {
                    p_existing
                        .get("id")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                } else if is_top_level {
                    Some(String::new())
                } else {
                    p_existing
                        .get(scope_field)
                        .and_then(Value::as_str)
                        .map(str::to_string)
                };
                if scope_id.is_some() && existing.is_none() {
                    inner.memory.delete(entity, id, origin).await.ok();
                }
            }
            if scope_id.is_some() {
                let _ = persistence.delete(entity, id, origin).await;
            }
        } else if existing.is_none() {
            return Ok(());
        }

        let Some(scope_id) = scope_id else {
            return Ok(());
        };

        if let Some(queue) = &inner.queue
            && !origin.skips_remote()
        {
            queue
                .queue(crate::types::PendingMutation {
                    op: Operation::Delete,
                    entity: entity.to_string(),
                    id: id.to_string(),
                    scope_id: scope_id.clone(),
                    data: None,
                    created_at: 0,
                })
                .await?;
        }

        if let Some(remote) = &inner.remote
            && !origin.skips_remote()
            && inner.state.lock().unwrap().last_connection_status == ConnectionStatus::Connected
        {
            match remote.sync_delete(entity, &scope_id, id).await {
                Ok(()) => {
                    if let Some(queue) = &inner.queue {
                        let _ = queue.remove(entity, id, &scope_id, Operation::Delete).await;
                    }
                }
                Err(err) if err.is_ownership() => {
                    if let Some(queue) = &inner.queue {
                        let _ = queue.remove(entity, id, &scope_id, Operation::Delete).await;
                    }
                }
                Err(err) if err.is_not_found() || err.is_transient() => {}
                Err(err) => {
                    tracing::warn!(entity = %entity, id = %id, error = %err, "remote delete failed");
                }
            }
        }

        Ok(())
    }

    /// Read a single row from the in-memory cache. Returns `None` if the row
    /// isn't in the currently-loaded scope (call [`Store::read_local_state`]
    /// if you need to hit persistence directly).
    pub async fn read(&self, entity: &str, id: &str) -> Result<Option<Record>> {
        self.inner()?.memory.read(entity, id).await
    }

    /// List rows of an entity. When persistence is configured, the filter's
    /// `scope_id` / `sort` / `projection` are applied at the fjall level.
    /// Without persistence, only `scope_id` is honored — `sort` and
    /// `projection` are silently ignored on the memory cache.
    pub async fn list(&self, entity: &str, filter: Option<ListFilter>) -> Result<Vec<Record>> {
        let inner = self.inner()?;
        if let Some(persistence) = &inner.persistence {
            let mut filters = Vec::new();
            if let Some(filter) = filter.as_ref()
                && let Some(scope_id) = filter.scope_id.as_deref()
            {
                filters.push(Filter::new(
                    inner.config.scope.scope_field.clone(),
                    FilterOp::Eq,
                    Value::String(scope_id.to_string()),
                ));
            }
            let sort = filter
                .as_ref()
                .map(|f| sort_fields_to_orders(&f.sort))
                .unwrap_or_default();
            let projection = filter
                .as_ref()
                .map(|f| f.projection.clone())
                .unwrap_or_default();
            persistence
                .list_with_projection(entity, filters, sort, None, projection)
                .await
        } else {
            let scope_id = filter
                .as_ref()
                .and_then(|f| f.scope_id.as_deref())
                .unwrap_or("");
            inner.memory.list(entity, scope_id).await
        }
    }

    /// List all root entities across all scopes, optionally sorted. Returns
    /// an empty `Vec` until [`Store::initial_sync_done`] is `true` when a
    /// remote is configured (so callers don't see stale local state during
    /// reconnect).
    pub async fn list_root_entities(&self, sort: Vec<SortField>) -> Result<Vec<Record>> {
        let inner = self.inner()?;
        if inner.remote.is_some() && !inner.state.lock().unwrap().initial_sync_done {
            return Ok(Vec::new());
        }
        if let Some(persistence) = &inner.persistence {
            persistence
                .list(
                    &inner.config.scope.root_entity,
                    Vec::new(),
                    sort_fields_to_orders(&sort),
                    None,
                )
                .await
        } else {
            inner.memory.list(&inner.config.scope.root_entity, "").await
        }
    }

    /// Snapshot of all rows for an entity within a scope, taken from the
    /// in-memory cache. Equivalent to TS `getSnapshot`.
    pub async fn snapshot(&self, entity: &str, scope_id: &str) -> Result<Vec<Record>> {
        self.inner()?.memory.list(entity, scope_id).await
    }

    /// Count of rows of an entity within a scope. Uses persistence when
    /// configured, otherwise counts the memory snapshot.
    pub async fn child_count(&self, entity: &str, scope_id: &str) -> Result<usize> {
        let inner = self.inner()?;
        if let Some(persistence) = &inner.persistence {
            let filters = vec![Filter::new(
                inner.config.scope.scope_field.clone(),
                FilterOp::Eq,
                Value::String(scope_id.to_string()),
            )];
            let rows = persistence.list(entity, filters, Vec::new(), None).await?;
            Ok(rows.len())
        } else {
            Ok(inner.memory.list(entity, scope_id).await?.len())
        }
    }

    /// Switch the active scope. If a remote is connected, fetches the new
    /// scope's root + children, reconciles them with local state, replays any
    /// buffered live mutations, and atomically swaps the memory cache. When
    /// offline, just loads from persistence. Emits `ScopeCleared` for the
    /// prior scope (if any) and `ScopeLoaded` for the new one on the memory
    /// bus.
    pub async fn replace_scope(&self, scope_id: &str) -> Result<()> {
        let inner = self.inner()?;
        let _serializer = inner.replace_scope_lock.lock().await;
        if inner.state.lock().unwrap().current_scope.as_deref() == Some(scope_id) {
            return Ok(());
        }

        let previous = inner
            .state
            .lock()
            .unwrap()
            .current_scope
            .replace(scope_id.to_string());
        if let Some(prev) = previous
            && prev != scope_id
            && let Some(remote) = &inner.remote
        {
            let _ = remote.close_scope(&prev).await;
        }

        let connected =
            inner.state.lock().unwrap().last_connection_status == ConnectionStatus::Connected;
        if let (Some(remote), true) = (&inner.remote, connected) {
            if let Some(persistence) = &inner.persistence {
                persistence.set_suppress_notifications(true);
            }
            let outcome = inner
                .open_scope_with_server(scope_id, remote.as_ref())
                .await;
            if let Some(persistence) = &inner.persistence {
                persistence.set_suppress_notifications(false);
            }
            outcome?;
        } else if inner.persistence.is_some() {
            let bundle = inner.load_bundle_from_persistence(scope_id).await?;
            inner.memory.load_scope(scope_id, bundle).await?;
        }
        Ok(())
    }

    /// Unsubscribe from a scope's MQTT topics and clear it from
    /// `current_scope` if it was active.
    pub async fn close_scope(&self, scope_id: &str) -> Result<()> {
        let inner = self.inner()?;
        if let Some(remote) = &inner.remote {
            let _ = remote.close_scope(scope_id).await;
        }
        let mut state = inner.state.lock().unwrap();
        if state.current_scope.as_deref() == Some(scope_id) {
            state.current_scope = None;
        }
        Ok(())
    }

    /// Disconnect the remote MQTT client. Pending requests are drained with
    /// [`Error::ConnectionClosed`]. Memory and persistence are unaffected.
    pub async fn disconnect(&self) -> Result<()> {
        if let Some(remote) = &self.inner()?.remote {
            let _ = remote.disconnect().await;
        }
        Ok(())
    }

    /// Hot-swap the underlying fjall database after corruption. Returns
    /// `Error::Config` if persistence isn't configured. Callers must drop any
    /// outstanding `Arc<Database>` clones reached through the internal layer
    /// modules before calling this.
    pub async fn recover_persistence(&self) -> Result<()> {
        let inner = self.inner()?;
        let Some(persistence) = &inner.persistence else {
            return Err(Error::Config(
                "recover_persistence requires persistence configuration".into(),
            ));
        };
        persistence.recover().await
    }

    /// Reconnect the remote client, optionally with a fresh auth ticket.
    pub async fn reconnect(&self, server_url: &str, ticket: Option<String>) -> Result<()> {
        let inner = self.inner()?;
        let Some(remote) = &inner.remote else {
            return Err(Error::Config(
                "reconnect requires remote configuration".into(),
            ));
        };
        remote.reconnect(server_url, ticket).await
    }

    /// Register a callback fired when the broker returns
    /// `MQTT_DISCONNECT_AUTH_FAILURE`. Use it to clear cached auth state and
    /// route the user to re-login.
    pub fn set_session_invalid_handler<F>(&self, handler: F) -> Result<()>
    where
        F: Fn() + Send + Sync + 'static,
    {
        let inner = self.inner()?;
        if let Some(remote) = &inner.remote {
            remote.set_session_invalid_handler(handler);
        }
        Ok(())
    }

    /// Register an async validator fired at the start of every successful
    /// (re)connect. See [`ReconnectValidator`].
    pub fn set_reconnect_validator(&self, validator: ReconnectValidator) -> Result<()> {
        let inner = self.inner()?;
        *inner.reconnect_validator.lock().unwrap() = Some(validator);
        Ok(())
    }

    /// Read a row from persistence directly, bypassing memory and remote.
    /// Useful for `local_only_entities` (user prefs, draft state). Falls back
    /// to memory when no persistence is configured.
    pub async fn read_local_state(&self, entity: &str, id: &str) -> Result<Option<Record>> {
        let inner = self.inner()?;
        if let Some(persistence) = &inner.persistence {
            persistence.read(entity, id).await
        } else {
            inner.memory.read(entity, id).await
        }
    }

    /// Upsert into persistence directly. Tries `update` first and falls back
    /// to `create` if the row doesn't exist. Bypasses memory and remote.
    pub async fn update_local_state(&self, entity: &str, id: &str, fields: Record) -> Result<()> {
        let inner = self.inner()?;
        if let Some(persistence) = &inner.persistence {
            match persistence
                .update(entity, id, fields.clone(), Origin::Load)
                .await
            {
                Ok(_) => Ok(()),
                Err(_) => {
                    let mut record = fields;
                    record.insert("id".to_string(), Value::String(id.to_string()));
                    persistence
                        .create(entity, record, Origin::Load)
                        .await
                        .map(|_| ())
                }
            }
        } else if inner.memory.read(entity, id).await?.is_some() {
            inner
                .memory
                .update(entity, id, fields, Origin::Load)
                .await?;
            Ok(())
        } else {
            let mut record = fields;
            record.insert("id".to_string(), Value::String(id.to_string()));
            inner.memory.create(entity, "", record, Origin::Load).await?;
            Ok(())
        }
    }

    /// Last scope-version (ms timestamp) this client has observed —
    /// seeded from the root record at `replace_scope` time and overwritten
    /// by this client's own `bump_scope_version`. Cleared on `close_scope`
    /// and on explicit `disconnect()`. `None` if the scope hasn't been opened
    /// or no remote is configured. Mirrors TS `getAppliedVersion`.
    pub fn applied_version(&self, scope_id: &str) -> Result<Option<i64>> {
        let inner = self.inner()?;
        Ok(inner
            .remote
            .as_ref()
            .and_then(|r| r.applied_version(scope_id)))
    }

    /// Send an arbitrary MQTT request and await the broker's response.
    /// Returns `Error::Config` if no remote is configured. For built-in CRUD,
    /// prefer the typed methods on `Store`.
    pub async fn request(&self, topic: &str, payload: Value) -> Result<Record> {
        let inner = self.inner()?;
        let Some(remote) = &inner.remote else {
            return Err(Error::Config("request requires remote configuration".into()));
        };
        remote.request(topic, payload).await
    }

    /// Disconnect the remote client, clear auth + sync state, and drop the
    /// `reconnect_validator` and `session_invalid_handler` callbacks.
    /// Persistence and the offline queue stay alive (Rust's ownership model
    /// prevents the TS-style mid-flight teardown safely); to fully reset,
    /// drop the `Store` and construct a new one.
    pub async fn reset_for_logout(&self) -> Result<()> {
        let inner = self.inner()?;
        if let Some(remote) = &inner.remote {
            remote.clear_session_invalid_handler();
            let _ = remote.disconnect().await;
        }
        *inner.reconnect_validator.lock().unwrap() = None;
        {
            let mut state = inner.state.lock().unwrap();
            state.authenticated_user = None;
            state.current_scope = None;
            state.initial_sync_done = false;
            state.last_connection_status = ConnectionStatus::Offline;
            state.has_been_connected = false;
        }
        if let Some(queue) = &inner.queue {
            queue.set_authenticated_user(None);
        }
        Ok(())
    }

    /// Disconnect the remote, close persistence, and abort background tasks.
    /// Idempotent.
    pub async fn shutdown(&self) -> Result<()> {
        let Some(inner) = self.inner.get() else {
            return Ok(());
        };
        if let Some(remote) = &inner.remote {
            let _ = remote.disconnect().await;
        }
        if let Some(persistence) = &inner.persistence {
            persistence.close();
        }
        let handles: Vec<JoinHandle<()>> = inner.tasks.lock().unwrap().drain(..).collect();
        for handle in handles {
            handle.abort();
        }
        Ok(())
    }
}

impl StoreInner {
    async fn open(
        config: Arc<StoreConfig>,
        options: &StoreOptions,
        client_id: &str,
    ) -> Result<Arc<Self>> {
        let memory = Arc::new(MemoryStore::new(Arc::clone(&config)).await?);

        let persistence = if let Some(pcfg) = &options.persistence {
            Some(Arc::new(
                PersistenceLayer::open(pcfg, Arc::clone(&config)).await?,
            ))
        } else {
            None
        };

        let remote = if options.remote.is_some() {
            Some(Arc::new(
                RemoteSyncLayer::new(client_id.to_string(), Arc::clone(&config)).await?,
            ))
        } else {
            None
        };

        let queue: Option<Arc<dyn OfflineQueue>> = match (&persistence, remote.is_some()) {
            (Some(p), true) => Some(Arc::new(
                PersistentOfflineQueue::new(Arc::clone(p), config.scope.root_entity.clone())
                    .await?,
            )),
            (None, true) => Some(Arc::new(InMemoryOfflineQueue::new(
                config.scope.root_entity.clone(),
            ))),
            _ => None,
        };

        let inner = Arc::new(Self {
            config: Arc::clone(&config),
            memory,
            persistence,
            remote,
            queue,
            state: Mutex::new(StoreState {
                current_scope: None,
                initial_sync_done: options.remote.is_none(),
                authenticated_user: None,
                last_connection_status: ConnectionStatus::Offline,
                has_been_connected: false,
            }),
            reconnect_validator: Mutex::new(None),
            replace_scope_lock: tokio::sync::Mutex::new(()),
            tasks: Mutex::new(Vec::new()),
        });

        if let Some(remote) = &inner.remote {
            let mutation_rx = remote.subscribe_mutations();
            let status_rx = remote.subscribe_connection_status();

            let mutation_task = tokio::spawn(mutation_loop(Arc::clone(&inner), mutation_rx));
            let status_task = tokio::spawn(status_loop(Arc::clone(&inner), status_rx));

            inner.tasks.lock().unwrap().push(mutation_task);
            inner.tasks.lock().unwrap().push(status_task);

            if let Some(remote_cfg) = &options.remote {
                let ticket = match &remote_cfg.get_ticket {
                    Some(provider) => Some(provider().await?),
                    None => None,
                };
                let _ = remote.connect(&remote_cfg.server_url, ticket).await;
            }
        }

        Ok(inner)
    }

    async fn open_scope_with_server(
        self: &Arc<Self>,
        scope_id: &str,
        remote: &RemoteSyncLayer,
    ) -> Result<()> {
        let state = remote.open_scope(scope_id).await?;
        let accessor = self.local_accessor();
        let queue_ref = self.queue.as_deref();

        if !state.root.is_empty() {
            if accessor
                .read(&self.config.scope.root_entity, scope_id)
                .await?
                .is_some()
            {
                let _ = accessor
                    .update(&self.config.scope.root_entity, scope_id, state.root.clone())
                    .await;
            } else {
                let _ = accessor
                    .create(&self.config.scope.root_entity, state.root.clone())
                    .await;
            }
        }

        for (child_entity, records) in &state.children {
            remote
                .reconcile_children(
                    scope_id,
                    child_entity,
                    records.clone(),
                    &accessor,
                    queue_ref,
                )
                .await?;
        }

        for mutation in state.buffered_mutations {
            let _ = remote.apply_mutation_to_db(mutation, &accessor).await;
        }

        let bundle = self.load_bundle_from_persistence(scope_id).await?;
        self.memory.load_scope(scope_id, bundle).await?;
        Ok(())
    }

    async fn load_bundle_from_persistence(&self, scope_id: &str) -> Result<ScopeBundle> {
        let Some(persistence) = &self.persistence else {
            return Ok(ScopeBundle::default());
        };
        let root = persistence
            .read(&self.config.scope.root_entity, scope_id)
            .await?;
        let mut children: BTreeMap<String, Vec<Record>> = BTreeMap::new();
        for child in &self.config.scope.child_entities {
            let list = persistence.list_by_scope(child, scope_id).await?;
            children.insert(child.clone(), list);
        }
        Ok(ScopeBundle { root, children })
    }

    fn local_accessor(self: &Arc<Self>) -> InnerLocalAccessor {
        InnerLocalAccessor {
            inner: Arc::clone(self),
        }
    }
}

struct InnerLocalAccessor {
    inner: Arc<StoreInner>,
}

#[async_trait]
impl LocalAccessor for InnerLocalAccessor {
    async fn read(&self, entity: &str, id: &str) -> Result<Option<Record>> {
        if let Some(p) = &self.inner.persistence {
            p.read(entity, id).await
        } else {
            self.inner.memory.read(entity, id).await
        }
    }

    async fn list(&self, entity: &str, scope_id: Option<&str>) -> Result<Vec<Record>> {
        if let Some(p) = &self.inner.persistence {
            if let Some(sid) = scope_id {
                p.list_by_scope(entity, sid).await
            } else {
                p.list(entity, Vec::new(), Vec::new(), None).await
            }
        } else {
            let sid = scope_id.unwrap_or("");
            self.inner.memory.list(entity, sid).await
        }
    }

    async fn create(&self, entity: &str, data: Record) -> Result<()> {
        if let Some(p) = &self.inner.persistence {
            p.create(entity, data, Origin::Remote).await?;
        } else {
            let scope_field = &self.inner.config.scope.scope_field;
            let scope_id = data
                .get(scope_field)
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            self.inner
                .memory
                .create(entity, &scope_id, data, Origin::Remote)
                .await?;
        }
        Ok(())
    }

    async fn update(&self, entity: &str, id: &str, fields: Record) -> Result<()> {
        if let Some(p) = &self.inner.persistence {
            p.update(entity, id, fields, Origin::Remote).await?;
        } else {
            self.inner
                .memory
                .update(entity, id, fields, Origin::Remote)
                .await?;
        }
        Ok(())
    }

    async fn delete(&self, entity: &str, id: &str) -> Result<()> {
        if let Some(p) = &self.inner.persistence {
            match p.delete(entity, id, Origin::Remote).await {
                Ok(()) | Err(_) => Ok(()),
            }
        } else {
            self.inner
                .memory
                .delete(entity, id, Origin::Remote)
                .await
                .ok();
            Ok(())
        }
    }
}

async fn mutation_loop(inner: Arc<StoreInner>, mut rx: broadcast::Receiver<MutationDelivery>) {
    loop {
        match rx.recv().await {
            Ok(delivery) => handle_remote_mutation(&inner, delivery.mutation).await,
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                tracing::warn!(skipped, "mutation receiver lagged");
                continue;
            }
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
}

async fn status_loop(inner: Arc<StoreInner>, mut rx: broadcast::Receiver<ConnectionStatus>) {
    loop {
        match rx.recv().await {
            Ok(status) => {
                {
                    let mut state = inner.state.lock().unwrap();
                    state.last_connection_status = status;
                    if status == ConnectionStatus::Connected {
                        state.has_been_connected = true;
                    }
                }
                if status == ConnectionStatus::Connected {
                    on_connected(Arc::clone(&inner)).await;
                }
            }
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
}

async fn handle_remote_mutation(inner: &Arc<StoreInner>, mutation: SyncMutation) {
    let accessor = inner.local_accessor();
    if let Some(remote) = &inner.remote {
        let _ = remote
            .apply_mutation_to_db(mutation.clone(), &accessor)
            .await;
    }

    let current_scope = inner.state.lock().unwrap().current_scope.clone();
    let root_entity = &inner.config.scope.root_entity;
    let scope_field = &inner.config.scope.scope_field;
    let is_top_level = inner
        .config
        .top_level_entities
        .iter()
        .any(|t| t.entity == mutation.entity);

    let should_apply_to_memory = if is_top_level {
        true
    } else {
        match (&mutation.data, &mutation.op) {
            (Some(data), _) => {
                if mutation.entity == *root_entity {
                    Some(mutation.id.clone()) == current_scope
                } else {
                    data.get(scope_field)
                        .and_then(Value::as_str)
                        .map(str::to_string)
                        == current_scope
                }
            }
            (None, Operation::Delete) => {
                if let Some(existing) = inner
                    .memory
                    .read(&mutation.entity, &mutation.id)
                    .await
                    .ok()
                    .flatten()
                {
                    if mutation.entity == *root_entity {
                        Some(mutation.id.clone()) == current_scope
                    } else {
                        existing
                            .get(scope_field)
                            .and_then(Value::as_str)
                            .map(str::to_string)
                            == current_scope
                    }
                } else {
                    false
                }
            }
            _ => false,
        }
    };
    if !should_apply_to_memory {
        return;
    }

    match mutation.op {
        Operation::Insert => {
            if let Some(mut data) = mutation.data {
                let sid = if is_top_level {
                    Some(String::new())
                } else {
                    data.get(scope_field)
                        .and_then(Value::as_str)
                        .map(str::to_string)
                        .or_else(|| {
                            if mutation.entity == *root_entity {
                                data.get("id").and_then(Value::as_str).map(str::to_string)
                            } else {
                                None
                            }
                        })
                };
                if let Some(sid) = sid {
                    data.insert("id".into(), Value::String(mutation.id));
                    let _ = inner
                        .memory
                        .create(&mutation.entity, &sid, data, Origin::Remote)
                        .await;
                }
            }
        }
        Operation::Update => {
            if let Some(data) = mutation.data {
                let _ = inner
                    .memory
                    .update(&mutation.entity, &mutation.id, data, Origin::Remote)
                    .await;
            }
        }
        Operation::Delete => {
            let _ = inner
                .memory
                .delete(&mutation.entity, &mutation.id, Origin::Remote)
                .await;
        }
    }
}

async fn on_connected(inner: Arc<StoreInner>) {
    let user = inner.state.lock().unwrap().authenticated_user.clone();
    let queue_ref = inner.queue.as_deref();
    let remote = inner.remote.clone();
    let accessor = inner.local_accessor();

    let validator = inner.reconnect_validator.lock().unwrap().clone();
    if let Some(validator) = validator
        && let Err(err) = validator().await
    {
        tracing::warn!(error = %err, "reconnect validator failed");
    }

    if let (Some(queue), Some(remote)) = (queue_ref, remote.as_ref()) {
        let sender: &dyn crate::offline_queue::MutationSender = remote.as_ref();
        let _ = queue.flush(sender).await;
        let _ = queue.flush(sender).await;
    }
    if let Some(remote) = &remote {
        {
            let mut state = inner.state.lock().unwrap();
            state.initial_sync_done = false;
        }
        let _ = remote
            .sync_root_entity_list(&accessor, queue_ref, user.as_deref())
            .await;
        let mut state = inner.state.lock().unwrap();
        state.initial_sync_done = true;
    }
}

fn spawn_filtered_forwarder<F>(
    mut rx: broadcast::Receiver<StoreEvent>,
    tx: tokio::sync::mpsc::UnboundedSender<MutationEvent>,
    entity: String,
    scope_id: Option<String>,
    accept: F,
    tasks: &Mutex<Vec<JoinHandle<()>>>,
) where
    F: Fn(&MutationEvent) -> bool + Send + 'static,
{
    let handle = tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(StoreEvent::Mutation(event)) => {
                    if event.entity != entity {
                        continue;
                    }
                    if let Some(scope) = scope_id.as_deref()
                        && event.scope_id != scope
                    {
                        continue;
                    }
                    if !accept(&event) {
                        continue;
                    }
                    if tx.send(event).is_err() {
                        break;
                    }
                }
                Ok(_) => {}
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });
    let mut guard = tasks.lock().unwrap();
    guard.retain(|h| !h.is_finished());
    guard.push(handle);
}

fn sort_fields_to_orders(sort: &[SortField]) -> Vec<MqdbSortOrder> {
    sort.iter()
        .map(|s| MqdbSortOrder {
            field: s.field.clone(),
            direction: match s.direction {
                SortDirection::Asc => MqdbSortDirection::Asc,
                SortDirection::Desc => MqdbSortDirection::Desc,
            },
        })
        .collect()
}

impl Drop for StoreInner {
    fn drop(&mut self) {
        let handles: Vec<JoinHandle<()>> = self.tasks.lock().unwrap().drain(..).collect();
        for handle in handles {
            handle.abort();
        }
    }
}
