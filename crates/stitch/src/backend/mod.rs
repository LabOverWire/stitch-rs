use crate::config::{EntityDefinition, PersistenceConfig, StoreConfig};
use crate::error::{Error, Result};
use crate::rt::Shared;
use crate::types::Record;
use mqdb_core::types::{Filter, Pagination, SortOrder};
use serde_json::Value;

#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(target_arch = "wasm32")]
mod wasm;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) trait MaybeSendSync: Send + Sync {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send + Sync + ?Sized> MaybeSendSync for T {}

#[cfg(target_arch = "wasm32")]
pub(crate) trait MaybeSendSync {}
#[cfg(target_arch = "wasm32")]
impl<T: ?Sized> MaybeSendSync for T {}

/// Backend-agnostic record store. The native implementation wraps
/// `mqdb_agent::Database` (`MemoryBackend` on the memory store, fjall on
/// persistence); the wasm implementation wraps `mqdb_wasm::WasmDatabase`
/// (in-memory or IndexedDB-backed). All scope/caller context is owned by the
/// implementation, so callers work purely in terms of JSON records and the
/// platform-neutral `mqdb_core` query types.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub(crate) trait Db: MaybeSendSync {
    async fn create(&self, entity: &str, data: Value) -> Result<Value>;
    async fn read(&self, entity: &str, id: &str) -> Result<Option<Value>>;
    async fn update(&self, entity: &str, id: &str, fields: Value) -> Result<Value>;
    async fn delete(&self, entity: &str, id: &str) -> Result<()>;
    async fn list(
        &self,
        entity: &str,
        filters: Vec<Filter>,
        sort: Vec<SortOrder>,
        pagination: Option<Pagination>,
        projection: Vec<String>,
    ) -> Result<Vec<Value>>;
    async fn register_schema(&self, name: &str, def: &EntityDefinition) -> Result<()>;
    fn close(&self);

    #[cfg(target_arch = "wasm32")]
    fn read_sync(&self, entity: &str, id: &str) -> Result<Option<Value>>;
    #[cfg(target_arch = "wasm32")]
    fn list_sync(
        &self,
        entity: &str,
        filters: Vec<Filter>,
        sort: Vec<SortOrder>,
        pagination: Option<Pagination>,
        projection: Vec<String>,
    ) -> Result<Vec<Value>>;
    #[cfg(target_arch = "wasm32")]
    fn count_sync(&self, entity: &str, filters: Vec<Filter>) -> Result<usize>;
}

pub(crate) type DynDb = Shared<dyn Db>;

/// Open a fresh in-memory record store with the config's schemas registered.
pub(crate) async fn open_memory_db(config: &StoreConfig) -> Result<DynDb> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        native::open_memory(config).await
    }
    #[cfg(target_arch = "wasm32")]
    {
        wasm::open_memory(config).await
    }
}

/// Open a durable record store (fjall natively, IndexedDB on wasm) with the
/// config's schemas registered.
pub(crate) async fn open_persistent_db(
    config: &StoreConfig,
    persistence: &PersistenceConfig,
) -> Result<DynDb> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        native::open_persistent(config, persistence).await
    }
    #[cfg(target_arch = "wasm32")]
    {
        wasm::open_persistent(config, persistence).await
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
