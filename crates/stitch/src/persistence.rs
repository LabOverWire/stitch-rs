use crate::backend::{DynDb, open_persistent_db, value_to_record};
use crate::config::{EntityDefinition, PersistenceConfig, StoreConfig};
use crate::error::Result;
use crate::origin::Origin;
use crate::types::{MutationEvent, Operation, Record, StoreEvent};
use mqdb_core::types::{Filter, FilterOp, Pagination, SortOrder};
use serde_json::Value;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Durable local store: fjall natively, IndexedDB on wasm. Routes all record
/// access through the backend [`Db`] trait; owns the mutation bus and
/// scope-resolution logic shared by both platforms.
pub struct PersistenceLayer {
    #[cfg(not(target_arch = "wasm32"))]
    db: std::sync::RwLock<DynDb>,
    #[cfg(target_arch = "wasm32")]
    db: DynDb,
    bus: tokio::sync::broadcast::Sender<StoreEvent>,
    config: Arc<StoreConfig>,
    #[cfg(not(target_arch = "wasm32"))]
    persistence_config: PersistenceConfig,
    top_level: HashSet<String>,
    suppress: AtomicBool,
}

impl PersistenceLayer {
    pub async fn open(persistence: &PersistenceConfig, config: Arc<StoreConfig>) -> Result<Self> {
        let db = open_persistent_db(&config, persistence).await?;
        let (bus, _) = tokio::sync::broadcast::channel(config.event_channel_capacity);
        let top_level: HashSet<String> = config
            .top_level_entities
            .iter()
            .map(|t| t.entity.clone())
            .collect();
        Ok(Self {
            #[cfg(not(target_arch = "wasm32"))]
            db: std::sync::RwLock::new(db),
            #[cfg(target_arch = "wasm32")]
            db,
            bus,
            config,
            #[cfg(not(target_arch = "wasm32"))]
            persistence_config: persistence.clone(),
            top_level,
            suppress: AtomicBool::new(false),
        })
    }

    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<StoreEvent> {
        self.bus.subscribe()
    }

    pub fn set_suppress_notifications(&self, suppress: bool) {
        self.suppress.store(suppress, Ordering::SeqCst);
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn db(&self) -> DynDb {
        self.db.read().expect("persistence db lock").clone()
    }

    #[cfg(target_arch = "wasm32")]
    fn db(&self) -> DynDb {
        crate::rt::Shared::clone(&self.db)
    }

    /// Register an extra entity schema on the durable store (e.g. the offline
    /// queue's pending-mutation table).
    pub async fn register_schema(&self, name: &str, def: &EntityDefinition) -> Result<()> {
        self.db().register_schema(name, def).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn recover(&self) -> Result<()> {
        let placeholder = crate::backend::open_memory_db(&self.config).await?;
        let old = {
            let mut guard = self.db.write().expect("persistence db lock");
            std::mem::replace(&mut *guard, placeholder)
        };
        old.close();
        drop(old);

        for attempt in 0..10 {
            match open_persistent_db(&self.config, &self.persistence_config).await {
                Ok(fresh) => {
                    *self.db.write().expect("persistence db lock") = fresh;
                    return Ok(());
                }
                Err(err) if attempt < 9 => {
                    tracing::debug!(attempt, error = %err, "recover: backend still locked, retrying");
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                Err(err) => return Err(err),
            }
        }
        unreachable!("recover loop exits via return")
    }

    pub async fn create(&self, entity: &str, data: Record, origin: Origin) -> Result<Record> {
        let mut data = data;
        strip_nulls(&mut data);
        let value = self.db().create(entity, Value::Object(data)).await?;
        let record = value_to_record(value)?;
        let id = record
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let scope_id = self.resolve_scope(entity, &record).unwrap_or_default();
        self.emit_mutation(MutationEvent {
            operation: Operation::Insert,
            entity: entity.to_string(),
            id,
            scope_id,
            data: Some(record.clone()),
            origin,
        });
        Ok(record)
    }

    pub async fn read(&self, entity: &str, id: &str) -> Result<Option<Record>> {
        match self.db().read(entity, id).await? {
            Some(value) => Ok(Some(value_to_record(value)?)),
            None => Ok(None),
        }
    }

    pub async fn update(
        &self,
        entity: &str,
        id: &str,
        fields: Record,
        origin: Origin,
    ) -> Result<Record> {
        let mut fields = fields;
        strip_nulls(&mut fields);
        let updated = self.db().update(entity, id, Value::Object(fields)).await?;
        let record = value_to_record(updated)?;
        let Some(scope_id) = self.resolve_scope(entity, &record) else {
            return Ok(record);
        };
        let id_owned = record
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or(id)
            .to_string();
        self.emit_mutation(MutationEvent {
            operation: Operation::Update,
            entity: entity.to_string(),
            id: id_owned,
            scope_id,
            data: Some(record.clone()),
            origin,
        });
        Ok(record)
    }

    pub async fn delete(&self, entity: &str, id: &str, origin: Origin) -> Result<()> {
        let existing = self.read(entity, id).await?;
        let scope_id = existing
            .as_ref()
            .and_then(|r| self.resolve_scope(entity, r))
            .unwrap_or_default();

        self.db().delete(entity, id).await?;

        self.emit_mutation(MutationEvent {
            operation: Operation::Delete,
            entity: entity.to_string(),
            id: id.to_string(),
            scope_id,
            data: None,
            origin,
        });
        Ok(())
    }

    pub async fn list(
        &self,
        entity: &str,
        filters: Vec<Filter>,
        sort: Vec<SortOrder>,
        pagination: Option<Pagination>,
    ) -> Result<Vec<Record>> {
        self.list_with_projection(entity, filters, sort, pagination, Vec::new())
            .await
    }

    pub async fn list_with_projection(
        &self,
        entity: &str,
        filters: Vec<Filter>,
        sort: Vec<SortOrder>,
        pagination: Option<Pagination>,
        projection: Vec<String>,
    ) -> Result<Vec<Record>> {
        let values = self
            .db()
            .list(entity, filters, sort, pagination, projection)
            .await?;
        values.into_iter().map(value_to_record).collect()
    }

    pub async fn list_by_scope(&self, entity: &str, scope_id: &str) -> Result<Vec<Record>> {
        let filters = if entity == self.config.scope.root_entity || self.top_level.contains(entity)
        {
            Vec::new()
        } else {
            vec![Filter::new(
                self.config.scope.scope_field.clone(),
                FilterOp::Eq,
                Value::String(scope_id.to_string()),
            )]
        };
        self.list(entity, filters, Vec::new(), None).await
    }

    pub async fn list_root(&self) -> Result<Vec<Record>> {
        let root = self.config.scope.root_entity.clone();
        self.list(&root, Vec::new(), Vec::new(), None).await
    }

    pub fn close(&self) {
        self.db().close();
    }

    fn emit_mutation(&self, event: MutationEvent) {
        if self.suppress.load(Ordering::SeqCst) {
            return;
        }
        let _ = self.bus.send(StoreEvent::Mutation(event));
    }

    fn resolve_scope(&self, entity: &str, data: &Record) -> Option<String> {
        if entity == self.config.scope.root_entity {
            return data.get("id").and_then(Value::as_str).map(str::to_string);
        }
        if self.top_level.contains(entity) {
            return Some(String::new());
        }
        data.get(&self.config.scope.scope_field)
            .and_then(Value::as_str)
            .map(str::to_string)
    }
}

/// Drop null fields from a record before passing to mqdb. Without this,
/// the schema validator rejects e.g. `read_at: null` for an optional
/// `Number` field with `expected type Number, got null`. Mirrors the
/// same helper in `memory_store` so both layers accept the same
/// records (otherwise memory accepts but persistence silently drops,
/// leaving `Store::list` empty until restart).
fn strip_nulls(record: &mut Record) {
    record.retain(|_, v| !v.is_null());
}
