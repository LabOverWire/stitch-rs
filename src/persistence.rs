use crate::config::{PersistenceConfig, StoreConfig};
use crate::db_helpers::{open_persistent_db, register_schemas, value_to_record};
use crate::error::{Error, Result};
use crate::origin::Origin;
use crate::types::{MutationEvent, Operation, Record, StoreEvent};
use arc_swap::ArcSwap;
use mqdb_agent::Database;
use mqdb_core::types::{Filter, FilterOp, Pagination, ScopeConfig as MqdbScopeConfig, SortOrder};
use serde_json::Value;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct PersistenceLayer {
    db: ArcSwap<Database>,
    bus: tokio::sync::broadcast::Sender<StoreEvent>,
    config: Arc<StoreConfig>,
    persistence_config: PersistenceConfig,
    mqdb_scope: MqdbScopeConfig,
    top_level: HashSet<String>,
    suppress: AtomicBool,
}

impl PersistenceLayer {
    pub async fn open(
        persistence: &PersistenceConfig,
        config: Arc<StoreConfig>,
    ) -> Result<Self> {
        let db = open_persistent_db(&persistence.db_path, persistence.passphrase.as_deref()).await?;
        register_schemas(&db, &config).await?;
        let (bus, _) = tokio::sync::broadcast::channel(config.event_channel_capacity);
        let mqdb_scope = MqdbScopeConfig::new(
            config.scope.root_entity.clone(),
            config.scope.scope_field.clone(),
        );
        let top_level: HashSet<String> = config
            .top_level_entities
            .iter()
            .map(|t| t.entity.clone())
            .collect();
        Ok(Self {
            db: ArcSwap::new(Arc::new(db)),
            bus,
            config,
            persistence_config: persistence.clone(),
            mqdb_scope,
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

    pub fn database(&self) -> Arc<Database> {
        self.db.load_full()
    }

    pub async fn recover(&self) -> Result<()> {
        let placeholder =
            crate::db_helpers::open_memory_db().await?;
        let old = self.db.swap(Arc::new(placeholder));
        old.shutdown();
        drop(old);

        for attempt in 0..10 {
            match open_persistent_db(
                &self.persistence_config.db_path,
                self.persistence_config.passphrase.as_deref(),
            )
            .await
            {
                Ok(fresh) => {
                    register_schemas(&fresh, &self.config).await?;
                    self.db.store(Arc::new(fresh));
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
        let db = self.database();
        let value = db
            .create(
                entity.to_string(),
                Value::Object(data),
                None,
                None,
                None,
                &self.mqdb_scope,
            )
            .await
            .map_err(|e| Error::mqdb(format!("persistence.create:{entity}"), e))?;

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
        let db = self.database();
        match db
            .read(entity.to_string(), id.to_string(), Vec::new(), None)
            .await
        {
            Ok(value) => Ok(Some(value_to_record(value)?)),
            Err(mqdb_core::error::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(Error::mqdb(format!("persistence.read:{entity}"), e)),
        }
    }

    pub async fn update(
        &self,
        entity: &str,
        id: &str,
        fields: Record,
        origin: Origin,
    ) -> Result<Record> {
        let caller = mqdb_agent::CallerContext {
            sender: None,
            client_id: None,
            scope_config: &self.mqdb_scope,
        };
        let db = self.database();
        let updated = db
            .update(
                entity.to_string(),
                id.to_string(),
                Value::Object(fields),
                None,
                &caller,
            )
            .await
            .map_err(|e| Error::mqdb(format!("persistence.update:{entity}"), e))?;
        let record = value_to_record(updated)?;
        let scope_id = self.resolve_scope(entity, &record).ok_or_else(|| {
            Error::Config(format!(
                "persistence.update:{entity}/{id}: record has no scope (missing {})",
                self.config.scope.scope_field
            ))
        })?;
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

        let db = self.database();
        db.delete(
                entity.to_string(),
                id.to_string(),
                None,
                None,
                &self.mqdb_scope,
                &mqdb_core::types::OwnershipConfig::default(),
            )
            .await
            .map_err(|e| Error::mqdb(format!("persistence.delete:{entity}"), e))?;

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
        let db = self.database();
        let values = db
            .list(
                entity.to_string(),
                filters,
                sort,
                pagination,
                Vec::new(),
                None,
            )
            .await
            .map_err(|e| Error::mqdb(format!("persistence.list:{entity}"), e))?;
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
        self.database().shutdown();
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
