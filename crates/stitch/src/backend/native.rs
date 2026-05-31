use super::{Db, DynDb};
use crate::config::StoreConfig;
use crate::db_helpers::register_schemas;
use crate::error::{Error, Result};
use crate::rt::Shared;
use mqdb_agent::{CallerContext, Database};
use mqdb_core::config::DatabaseConfig;
use mqdb_core::storage::MemoryBackend;
use mqdb_core::types::{Filter, FilterOp, OwnershipConfig, ScopeConfig as MqdbScopeConfig};
use serde_json::Value;
use std::sync::Arc;

pub(crate) struct NativeDb {
    db: Database,
    scope: MqdbScopeConfig,
}

pub(crate) async fn open_memory(config: &StoreConfig) -> Result<DynDb> {
    let backend: Arc<dyn mqdb_core::storage::StorageBackend> = Arc::new(MemoryBackend::new());
    let db_config = DatabaseConfig::new("stitch-memory").without_background_tasks();
    let db = Database::open_with_backend(backend, db_config)
        .await
        .map_err(|e| Error::mqdb("open_memory_db", e))?;
    register_schemas(&db, config).await?;
    let scope = MqdbScopeConfig::new(
        config.scope.root_entity.clone(),
        config.scope.scope_field.clone(),
    );
    Ok(Shared::new(NativeDb { db, scope }))
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

    async fn list_eq(&self, entity: &str, filters: &[(String, Value)]) -> Result<Vec<Value>> {
        let filters: Vec<Filter> = filters
            .iter()
            .map(|(field, value)| Filter::new(field.clone(), FilterOp::Eq, value.clone()))
            .collect();
        self.db
            .list(entity.to_string(), filters, Vec::new(), None, Vec::new(), None)
            .await
            .map_err(|e| Error::mqdb(format!("list:{entity}"), e))
    }
}
