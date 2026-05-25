use crate::config::StoreConfig;
use crate::error::{Error, Result};
use crate::offline_queue::{MutationSender, OfflineQueue};
use crate::sync_engine::{MutationDelivery, SyncEngine};
use crate::types::{ConnectionStatus, Operation, Record, ScopeState, SyncMutation};
use async_trait::async_trait;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::broadcast;

#[async_trait]
pub trait LocalAccessor: Send + Sync {
    async fn read(&self, entity: &str, id: &str) -> Result<Option<Record>>;
    async fn list(&self, entity: &str, scope_id: Option<&str>) -> Result<Vec<Record>>;
    async fn create(&self, entity: &str, data: Record) -> Result<()>;
    async fn update(&self, entity: &str, id: &str, fields: Record) -> Result<()>;
    async fn delete(&self, entity: &str, id: &str) -> Result<()>;
}

pub struct RemoteSyncLayer {
    config: Arc<StoreConfig>,
    sync: Arc<SyncEngine>,
    top_level: HashSet<String>,
}

impl RemoteSyncLayer {
    pub async fn new(client_id: String, config: Arc<StoreConfig>) -> Result<Self> {
        let sync = Arc::new(SyncEngine::new(client_id, Arc::clone(&config)).await?);
        let top_level: HashSet<String> = config
            .top_level_entities
            .iter()
            .map(|t| t.entity.clone())
            .collect();
        Ok(Self {
            config,
            sync,
            top_level,
        })
    }

    pub fn engine(&self) -> Arc<SyncEngine> {
        Arc::clone(&self.sync)
    }

    pub fn subscribe_mutations(&self) -> broadcast::Receiver<MutationDelivery> {
        self.sync.mutations()
    }

    pub fn subscribe_connection_status(&self) -> broadcast::Receiver<ConnectionStatus> {
        self.sync.connection_status()
    }

    pub async fn connect(&self, server_url: &str, ticket: Option<String>) -> Result<()> {
        self.sync.connect(server_url, ticket).await
    }

    pub async fn disconnect(&self) -> Result<()> {
        self.sync.disconnect().await
    }

    pub async fn reconnect(&self, server_url: &str, ticket: Option<String>) -> Result<()> {
        self.sync.reconnect(server_url, ticket).await
    }

    pub fn set_session_invalid_handler<F>(&self, handler: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.sync.set_session_invalid_handler(handler);
    }

    pub async fn is_connected(&self) -> bool {
        self.sync.is_connected().await
    }

    pub async fn request(&self, topic: &str, payload: Value) -> Result<Record> {
        self.sync.request(topic, payload).await
    }

    pub async fn open_scope(&self, scope_id: &str) -> Result<ScopeState> {
        self.sync.open_scope(scope_id).await
    }

    pub async fn close_scope(&self, scope_id: &str) -> Result<()> {
        self.sync.close_scope(scope_id).await
    }

    pub async fn fetch_list(&self, entity: &str, scope_id: Option<&str>) -> Result<Vec<Record>> {
        self.sync.fetch_list(entity, scope_id).await
    }

    pub async fn fetch_one(&self, entity: &str, id: &str) -> Result<Option<Record>> {
        self.sync.fetch_one(entity, id).await
    }

    pub async fn sync_create(&self, entity: &str, scope_id: &str, data: Record) -> Result<String> {
        let role = self.entity_role(entity);
        let outbound_scope = match role {
            EntityRole::Child => scope_id,
            EntityRole::Root | EntityRole::TopLevel | EntityRole::Unknown => "",
        };
        self.sync.sync_create(entity, outbound_scope, data).await
    }

    pub async fn sync_update(
        &self,
        entity: &str,
        scope_id: &str,
        id: &str,
        data: Record,
    ) -> Result<()> {
        let role = self.entity_role(entity);
        let outbound_scope = match role {
            EntityRole::Child => scope_id,
            EntityRole::Root | EntityRole::TopLevel | EntityRole::Unknown => "",
        };
        self.sync
            .sync_update(entity, outbound_scope, id, data)
            .await
    }

    pub async fn sync_delete(&self, entity: &str, scope_id: &str, id: &str) -> Result<()> {
        let role = self.entity_role(entity);
        let outbound_scope = match role {
            EntityRole::Child => scope_id,
            EntityRole::Root | EntityRole::TopLevel | EntityRole::Unknown => "",
        };
        self.sync.sync_delete(entity, outbound_scope, id).await
    }

    pub async fn apply_mutation_to_db(
        &self,
        mutation: SyncMutation,
        accessor: &dyn LocalAccessor,
    ) -> Result<()> {
        match mutation.op {
            Operation::Insert => {
                let Some(mut data) = mutation.data else {
                    return Ok(());
                };
                if let Some(ref_scope) = data
                    .get(&self.config.scope.scope_field)
                    .and_then(Value::as_str)
                {
                    let ref_scope = ref_scope.to_string();
                    if accessor
                        .read(&self.config.scope.root_entity, &ref_scope)
                        .await?
                        .is_none()
                    {
                        return Ok(());
                    }
                }
                data.insert("id".to_string(), Value::String(mutation.id.clone()));
                accessor.create(&mutation.entity, data).await?;
            }
            Operation::Update => {
                let Some(data) = mutation.data else {
                    return Ok(());
                };
                let existing = accessor.read(&mutation.entity, &mutation.id).await?;
                let Some(existing) = existing else {
                    return Ok(());
                };
                if let (Some(remote), Some(local)) = (
                    data.get(&self.config.version_field).and_then(Value::as_u64),
                    existing
                        .get(&self.config.version_field)
                        .and_then(Value::as_u64),
                ) && remote < local
                {
                    return Ok(());
                }
                if let (Some(remote_ts), Some(local_ts)) = (
                    data.get(&self.config.updated_at_field)
                        .and_then(Value::as_u64),
                    existing
                        .get(&self.config.updated_at_field)
                        .and_then(Value::as_u64),
                ) && remote_ts < local_ts
                {
                    return Ok(());
                }
                accessor
                    .update(&mutation.entity, &mutation.id, data)
                    .await?;
            }
            Operation::Delete => match accessor.delete(&mutation.entity, &mutation.id).await {
                Ok(()) => {}
                Err(err) if err.is_not_found() => {}
                Err(err) => return Err(err),
            },
        }
        Ok(())
    }

    pub async fn reconcile_children(
        &self,
        scope_id: &str,
        entity: &str,
        server_records: Vec<Record>,
        accessor: &dyn LocalAccessor,
        queue: Option<&dyn OfflineQueue>,
    ) -> Result<()> {
        let scope_field = self.config.scope.scope_field.clone();
        let local_records = accessor.list(entity, Some(scope_id)).await?;

        let pending: Vec<_> = if let Some(q) = queue {
            q.pending_for_scope(scope_id)
                .await?
                .into_iter()
                .filter(|p| p.entity == entity)
                .collect()
        } else {
            Vec::new()
        };

        let pending_by_id: HashMap<String, Vec<Operation>> = {
            let mut map: HashMap<String, Vec<Operation>> = HashMap::new();
            for p in &pending {
                map.entry(p.id.clone()).or_default().push(p.op);
            }
            map
        };
        let server_ids: HashSet<String> = server_records
            .iter()
            .filter_map(|r| r.get("id").and_then(Value::as_str).map(str::to_string))
            .collect();
        let local_ids: HashSet<String> = local_records
            .iter()
            .filter_map(|r| r.get("id").and_then(Value::as_str).map(str::to_string))
            .collect();

        for record in server_records {
            let Some(id) = record.get("id").and_then(Value::as_str).map(str::to_string) else {
                continue;
            };
            let pending_ops = pending_by_id.get(&id);
            let has_pending_delete = pending_ops
                .map(|ops| ops.contains(&Operation::Delete))
                .unwrap_or(false);
            let has_pending_update = pending_ops
                .map(|ops| ops.contains(&Operation::Update))
                .unwrap_or(false);
            if has_pending_delete || has_pending_update {
                continue;
            }
            let mut cleaned = strip_nulls(record);
            if !cleaned.contains_key(&scope_field) {
                cleaned.insert(scope_field.clone(), Value::String(scope_id.to_string()));
            }
            if local_ids.contains(&id) {
                accessor.update(entity, &id, cleaned).await?;
            } else {
                accessor.create(entity, cleaned).await?;
            }
        }

        for p in &pending {
            if p.op == Operation::Delete {
                continue;
            }
            if server_ids.contains(&p.id) || local_ids.contains(&p.id) {
                continue;
            }
            let Some(data) = p.data.clone() else {
                continue;
            };
            let _ = accessor.create(entity, data).await;
        }

        for id in local_ids {
            if server_ids.contains(&id) {
                continue;
            }
            if pending_by_id.contains_key(&id) {
                continue;
            }
            let _ = accessor.delete(entity, &id).await;
        }
        Ok(())
    }

    pub async fn sync_root_entity_list(
        &self,
        accessor: &dyn LocalAccessor,
        queue: Option<&dyn OfflineQueue>,
        authenticated_user: Option<&str>,
    ) -> Result<()> {
        let root_entity = self.config.scope.root_entity.clone();
        let server_entities = self.sync.fetch_list(&root_entity, None).await?;
        let scoped = if let (Some(user), Some(field)) =
            (authenticated_user, &self.config.user_scope_field)
        {
            server_entities
                .into_iter()
                .filter(|r| r.get(field).and_then(Value::as_str) == Some(user))
                .collect()
        } else {
            server_entities
        };

        let local_entities = accessor.list(&root_entity, None).await?;
        for local in &local_entities {
            let Some(id) = local.get("id").and_then(Value::as_str) else {
                continue;
            };
            if authenticated_user.is_none() {
                continue;
            }
            let in_server = scoped
                .iter()
                .any(|r| r.get("id").and_then(Value::as_str) == Some(id));
            if in_server {
                continue;
            }
            if let Some(q) = queue
                && q.has_pending_insert(&root_entity, id).await?
            {
                continue;
            }
            let _ = accessor.delete(&root_entity, id).await;
        }

        for entity in scoped {
            let Some(id) = entity.get("id").and_then(Value::as_str).map(str::to_string) else {
                continue;
            };
            if let Some(q) = queue {
                let pending = q.pending_for_scope(&id).await?;
                let has_pending_root_delete = pending
                    .iter()
                    .any(|p| p.op == Operation::Delete && p.entity == root_entity);
                if has_pending_root_delete {
                    continue;
                }
            }
            let exists = accessor.read(&root_entity, &id).await?.is_some();
            if exists {
                accessor.update(&root_entity, &id, entity.clone()).await?;
            } else {
                accessor.create(&root_entity, entity.clone()).await?;
            }

            for child in &self.config.scope.child_entities {
                let records = self.sync.fetch_list(child, Some(&id)).await?;
                self.reconcile_children(&id, child, records, accessor, queue)
                    .await?;
            }
        }
        Ok(())
    }

    fn entity_role(&self, entity: &str) -> EntityRole {
        if entity == self.config.scope.root_entity {
            EntityRole::Root
        } else if self.top_level.contains(entity) {
            EntityRole::TopLevel
        } else if self.config.scope.child_entities.iter().any(|c| c == entity) {
            EntityRole::Child
        } else {
            EntityRole::Unknown
        }
    }
}

#[async_trait]
impl MutationSender for RemoteSyncLayer {
    async fn sync_create(&self, entity: &str, scope_id: &str, data: Record) -> Result<()> {
        self.sync_create(entity, scope_id, data).await.map(|_| ())
    }

    async fn sync_update(
        &self,
        entity: &str,
        scope_id: &str,
        id: &str,
        data: Record,
    ) -> Result<()> {
        self.sync_update(entity, scope_id, id, data).await
    }

    async fn sync_delete(&self, entity: &str, scope_id: &str, id: &str) -> Result<()> {
        self.sync_delete(entity, scope_id, id).await
    }

    async fn read_entity(&self, entity: &str, id: &str) -> Result<Record> {
        match self.sync.fetch_one(entity, id).await? {
            Some(r) => Ok(r),
            None => Err(Error::NotFound {
                entity: entity.to_string(),
                id: id.to_string(),
            }),
        }
    }

    async fn delete_entity(&self, entity: &str, id: &str) -> Result<()> {
        match self.sync.sync_delete(entity, "", id).await {
            Ok(()) => Ok(()),
            Err(err) if err.is_not_found() => Ok(()),
            Err(err) => Err(err),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EntityRole {
    Root,
    Child,
    TopLevel,
    Unknown,
}

fn strip_nulls(record: Record) -> Record {
    record.into_iter().filter(|(_, v)| !v.is_null()).collect()
}
