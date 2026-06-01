use super::{Db, DynDb};
use crate::config::{EntityDefinition, PersistenceConfig, StoreConfig};
use crate::db_helpers::{open_persistent_db, register_entity_schema, register_schemas};
use crate::error::{Error, Result};
use crate::rt::Shared;
use mqdb_agent::{CallerContext, Database};
use mqdb_core::config::DatabaseConfig;
use mqdb_core::storage::MemoryBackend;
use mqdb_core::types::{Filter, OwnershipConfig, Pagination, ScopeConfig as MqdbScopeConfig, SortOrder};
use serde_json::Value;
use std::sync::Arc;

pub(crate) struct NativeDb {
    db: Database,
    scope: MqdbScopeConfig,
}

fn scope_of(config: &StoreConfig) -> MqdbScopeConfig {
    MqdbScopeConfig::new(
        config.scope.root_entity.clone(),
        config.scope.scope_field.clone(),
    )
}

pub(crate) async fn open_memory(config: &StoreConfig) -> Result<DynDb> {
    let backend: Arc<dyn mqdb_core::storage::StorageBackend> = Arc::new(MemoryBackend::new());
    let db_config = DatabaseConfig::new("stitch-memory").without_background_tasks();
    let db = Database::open_with_backend(backend, db_config)
        .await
        .map_err(|e| Error::mqdb("open_memory_db", e))?;
    register_schemas(&db, config).await?;
    Ok(Shared::new(NativeDb {
        db,
        scope: scope_of(config),
    }))
}

pub(crate) async fn open_persistent(
    config: &StoreConfig,
    persistence: &PersistenceConfig,
) -> Result<DynDb> {
    let db = open_persistent_db(&persistence.db_path, persistence.passphrase.as_deref()).await?;
    register_schemas(&db, config).await?;
    Ok(Shared::new(NativeDb {
        db,
        scope: scope_of(config),
    }))
}

#[async_trait::async_trait]
impl Db for NativeDb {
    async fn create(&self, entity: &str, data: Value) -> Result<Value> {
        self.db
            .create(entity.to_string(), data, None, None, None, &self.scope)
            .await
            .map_err(|e| Error::mqdb(format!("create:{entity}"), e))
    }

    async fn read(&self, entity: &str, id: &str) -> Result<Option<Value>> {
        match self
            .db
            .read(entity.to_string(), id.to_string(), Vec::new(), None)
            .await
        {
            Ok(value) => Ok(Some(value)),
            Err(mqdb_core::error::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(Error::mqdb(format!("read:{entity}"), e)),
        }
    }

    async fn update(&self, entity: &str, id: &str, fields: Value) -> Result<Value> {
        let caller = CallerContext {
            sender: None,
            client_id: None,
            scope_config: &self.scope,
        };
        self.db
            .update(entity.to_string(), id.to_string(), fields, None, &caller)
            .await
            .map_err(|e| match e {
                mqdb_core::error::Error::Conflict(_) => Error::Conflict {
                    entity: entity.to_string(),
                    id: id.to_string(),
                },
                other => Error::mqdb(format!("update:{entity}"), other),
            })
    }

    async fn delete(&self, entity: &str, id: &str) -> Result<()> {
        self.db
            .delete(
                entity.to_string(),
                id.to_string(),
                None,
                None,
                &self.scope,
                &OwnershipConfig::default(),
            )
            .await
            .map_err(|e| match e {
                mqdb_core::error::Error::Conflict(_) => Error::Conflict {
                    entity: entity.to_string(),
                    id: id.to_string(),
                },
                other => Error::mqdb(format!("delete:{entity}"), other),
            })
    }

    async fn list(
        &self,
        entity: &str,
        filters: Vec<Filter>,
        sort: Vec<SortOrder>,
        pagination: Option<Pagination>,
        projection: Vec<String>,
    ) -> Result<Vec<Value>> {
        self.db
            .list(entity.to_string(), filters, sort, pagination, projection, None)
            .await
            .map_err(|e| Error::mqdb(format!("list:{entity}"), e))
    }

    async fn register_schema(&self, name: &str, def: &EntityDefinition) -> Result<()> {
        register_entity_schema(&self.db, name, def).await
    }

    fn close(&self) {
        self.db.shutdown();
    }
}
