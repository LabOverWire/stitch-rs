use crate::config::{StoreConfig, StoreOptions};
use crate::error::{Error, Result};
use crate::memory_store::MemoryStore;
use crate::offline_queue::{InMemoryOfflineQueue, OfflineQueue, PersistentOfflineQueue};
use crate::origin::Origin;
use crate::persistence::PersistenceLayer;
use crate::remote_sync::{LocalAccessor, RemoteSyncLayer};
use crate::sync_engine::MutationDelivery;
use crate::types::{
    ConnectionStatus, Operation, Record, ScopeBundle, StoreEvent, SyncMutation,
};
use async_trait::async_trait;
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tokio::sync::{OnceCell, broadcast};
use tokio::task::JoinHandle;
use uuid::Uuid;

pub struct Store {
    config: Arc<StoreConfig>,
    options: StoreOptions,
    client_id: String,
    inner: OnceCell<Arc<StoreInner>>,
}

struct StoreInner {
    config: Arc<StoreConfig>,
    memory: Arc<MemoryStore>,
    persistence: Option<Arc<PersistenceLayer>>,
    remote: Option<Arc<RemoteSyncLayer>>,
    queue: Option<Arc<dyn OfflineQueue>>,
    state: Mutex<StoreState>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

struct StoreState {
    current_scope: Option<String>,
    initial_sync_done: bool,
    authenticated_user: Option<String>,
    last_connection_status: ConnectionStatus,
}

impl Store {
    #[must_use]
    pub fn new(config: StoreConfig, options: StoreOptions) -> Self {
        Self::with_client_id(config, options, Uuid::now_v7().to_string())
    }

    #[must_use]
    pub fn with_client_id(
        config: StoreConfig,
        options: StoreOptions,
        client_id: String,
    ) -> Self {
        Self {
            config: Arc::new(config),
            options,
            client_id,
            inner: OnceCell::new(),
        }
    }

    pub async fn initialize(&self) -> Result<()> {
        self.inner
            .get_or_try_init(|| async {
                let inner = StoreInner::open(
                    Arc::clone(&self.config),
                    &self.options,
                    &self.client_id,
                )
                .await?;
                Ok::<Arc<StoreInner>, Error>(inner)
            })
            .await?;
        Ok(())
    }

    fn inner(&self) -> Result<&Arc<StoreInner>> {
        self.inner.get().ok_or(Error::NotInitialized)
    }

    pub fn subscribe(&self) -> Result<broadcast::Receiver<StoreEvent>> {
        Ok(self.inner()?.memory.subscribe())
    }

    pub fn subscribe_persistence(&self) -> Result<Option<broadcast::Receiver<StoreEvent>>> {
        Ok(self.inner()?.persistence.as_ref().map(|p| p.subscribe()))
    }

    pub fn begin_batch(&self) -> Result<()> {
        self.inner()?.memory.begin_batch();
        Ok(())
    }

    pub fn end_batch(&self) -> Result<()> {
        self.inner()?.memory.end_batch();
        Ok(())
    }

    pub fn connection_status(&self) -> Result<ConnectionStatus> {
        let inner = self.inner()?;
        Ok(inner.state.lock().unwrap().last_connection_status)
    }

    pub fn subscribe_connection_status(
        &self,
    ) -> Result<Option<broadcast::Receiver<ConnectionStatus>>> {
        Ok(self
            .inner()?
            .remote
            .as_ref()
            .map(|r| r.subscribe_connection_status()))
    }

    pub fn set_authenticated_user(&self, user_id: Option<String>) -> Result<()> {
        let inner = self.inner()?;
        inner.state.lock().unwrap().authenticated_user = user_id.clone();
        if let Some(q) = &inner.queue {
            q.set_authenticated_user(user_id);
        }
        Ok(())
    }

    pub fn current_scope(&self) -> Result<Option<String>> {
        Ok(self.inner()?.state.lock().unwrap().current_scope.clone())
    }

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
                e.get(scope_field).and_then(Value::as_str).map(str::to_string)
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
                        let _ = queue
                            .remove(entity, id, &scope_id, Operation::Update)
                            .await;
                    }
                }
                Err(err) if err.is_ownership() => {
                    if let Some(queue) = &inner.queue {
                        let _ = queue
                            .remove(entity, id, &scope_id, Operation::Update)
                            .await;
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
                e.get(scope_field).and_then(Value::as_str).map(str::to_string)
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
                        let _ = queue
                            .remove(entity, id, &scope_id, Operation::Delete)
                            .await;
                    }
                }
                Err(err) if err.is_ownership() => {
                    if let Some(queue) = &inner.queue {
                        let _ = queue
                            .remove(entity, id, &scope_id, Operation::Delete)
                            .await;
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

    pub async fn read(&self, entity: &str, id: &str) -> Result<Option<Record>> {
        self.inner()?.memory.read(entity, id).await
    }

    pub async fn list(&self, entity: &str, scope_id: &str) -> Result<Vec<Record>> {
        let inner = self.inner()?;
        if let Some(persistence) = &inner.persistence {
            persistence.list_by_scope(entity, scope_id).await
        } else {
            inner.memory.list(entity, scope_id).await
        }
    }

    pub async fn list_root_entities(&self) -> Result<Vec<Record>> {
        let inner = self.inner()?;
        if inner.remote.is_some() && !inner.state.lock().unwrap().initial_sync_done {
            return Ok(Vec::new());
        }
        if let Some(persistence) = &inner.persistence {
            persistence.list_root().await
        } else {
            inner.memory.list(&inner.config.scope.root_entity, "").await
        }
    }

    pub async fn replace_scope(&self, scope_id: &str) -> Result<()> {
        let inner = self.inner()?;
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

        let connected = inner.state.lock().unwrap().last_connection_status
            == ConnectionStatus::Connected;
        if let (Some(remote), true) = (&inner.remote, connected) {
            if let Some(persistence) = &inner.persistence {
                persistence.set_suppress_notifications(true);
            }
            let outcome = inner.open_scope_with_server(scope_id, remote.as_ref()).await;
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

    pub async fn disconnect(&self) -> Result<()> {
        if let Some(remote) = &self.inner()?.remote {
            let _ = remote.disconnect().await;
        }
        Ok(())
    }

    pub async fn recover_persistence(&self) -> Result<()> {
        let inner = self.inner()?;
        let Some(persistence) = &inner.persistence else {
            return Err(Error::Config(
                "recover_persistence requires persistence configuration".into(),
            ));
        };
        persistence.recover().await
    }

    pub async fn reconnect(&self, server_url: &str, ticket: Option<String>) -> Result<()> {
        let inner = self.inner()?;
        let Some(remote) = &inner.remote else {
            return Err(Error::Config(
                "reconnect requires remote configuration".into(),
            ));
        };
        remote.reconnect(server_url, ticket).await
    }

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
            }),
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
            if accessor.read(&self.config.scope.root_entity, scope_id).await?.is_some() {
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
                .reconcile_children(scope_id, child_entity, records.clone(), &accessor, queue_ref)
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
            self.inner.memory.delete(entity, id, Origin::Remote).await.ok();
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
                if let Some(existing) =
                    inner.memory.read(&mutation.entity, &mutation.id).await.ok().flatten()
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

impl Drop for StoreInner {
    fn drop(&mut self) {
        let handles: Vec<JoinHandle<()>> = self.tasks.lock().unwrap().drain(..).collect();
        for handle in handles {
            handle.abort();
        }
    }
}
