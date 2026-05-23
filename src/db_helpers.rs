use crate::config::{FieldType, StoreConfig};
use crate::error::{Error, Result};
use crate::types::Record;
use mqdb_agent::Database;
use mqdb_core::config::DatabaseConfig;
use mqdb_core::schema::{FieldDefinition, FieldType as MqdbFieldType, Schema};
use mqdb_core::storage::MemoryBackend;
use serde_json::Value;
use std::path::Path;
use std::sync::Arc;

pub(crate) async fn open_memory_db() -> Result<Database> {
    let backend: Arc<dyn mqdb_core::storage::StorageBackend> = Arc::new(MemoryBackend::new());
    let config = DatabaseConfig::new("stitch-memory").without_background_tasks();
    Database::open_with_backend(backend, config)
        .await
        .map_err(|e| Error::mqdb("open_memory_db", e))
}

pub(crate) async fn open_persistent_db(
    path: &Path,
    passphrase: Option<&str>,
) -> Result<Database> {
    let mut config = DatabaseConfig::new(path.to_path_buf()).without_background_tasks();
    if let Some(secret) = passphrase {
        config = config.with_passphrase(secret.to_string());
    }
    Database::open_with_config(config)
        .await
        .map_err(|e| Error::mqdb(format!("open_persistent_db:{}", path.display()), e))
}

pub(crate) async fn register_schemas(db: &Database, config: &StoreConfig) -> Result<()> {
    for (name, def) in config.entities.iter().chain(config.local_only_entities.iter()) {
        register_entity_schema(db, name, def).await?;
    }
    Ok(())
}

pub(crate) async fn register_entity_schema(
    db: &Database,
    name: &str,
    def: &crate::config::EntityDefinition,
) -> Result<()> {
    let mut schema = Schema::new(name.to_string());
    for field in &def.fields {
        let mut fd = FieldDefinition::new(field.name.clone(), map_field_type(field.r#type));
        if field.required {
            fd = fd.required();
        }
        if let Some(default) = &field.default {
            fd = fd.with_default(default.clone());
        }
        schema = schema.add_field(fd);
    }
    db.add_schema(schema)
        .await
        .map_err(|e| Error::mqdb(format!("add_schema:{name}"), e))?;
    for index_field in &def.indexes {
        db.add_index(name.to_string(), vec![index_field.clone()])
            .await
            .map_err(|e| Error::mqdb(format!("add_index:{name}:{index_field}"), e))?;
    }
    Ok(())
}

pub(crate) fn map_field_type(ft: FieldType) -> MqdbFieldType {
    match ft {
        FieldType::String => MqdbFieldType::String,
        FieldType::Number => MqdbFieldType::Number,
        FieldType::Boolean => MqdbFieldType::Boolean,
        FieldType::Object => MqdbFieldType::Object,
        FieldType::Array => MqdbFieldType::Array,
    }
}

pub(crate) fn value_to_record(value: Value) -> Result<Record> {
    match value {
        Value::Object(map) => Ok(map),
        other => Err(Error::Config(format!(
            "expected object record, got {other:?}"
        ))),
    }
}
