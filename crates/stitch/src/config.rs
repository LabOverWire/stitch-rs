use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Primitive types a schema field can hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldType {
    String,
    Number,
    Boolean,
    Object,
    Array,
}

/// A single field on an entity's schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaField {
    pub name: String,
    pub r#type: FieldType,
    #[serde(default)]
    pub required: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<serde_json::Value>,
}

/// Cascade behavior when a parent row referenced by a foreign key is deleted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnDeleteAction {
    Cascade,
    SetNull,
    Restrict,
}

/// Foreign-key relationship between two entities.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKeyDefinition {
    pub field: String,
    pub references: String,
    pub on_delete: OnDeleteAction,
}

/// Schema for a single entity, including fields, foreign keys, and indexes.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EntityDefinition {
    pub fields: Vec<SchemaField>,
    #[serde(default)]
    pub foreign_keys: Vec<ForeignKeyDefinition>,
    #[serde(default)]
    pub unique_constraints: Vec<Vec<String>>,
    #[serde(default)]
    pub indexes: Vec<String>,
}

/// How a single scope is organized: which entity is the root, which are
/// children that belong to that root, and which field on children carries the
/// owning scope id.
#[derive(Debug, Clone)]
pub struct ScopeConfig {
    pub root_entity: String,
    pub child_entities: Vec<String>,
    pub scope_field: String,
}

/// An entity that lives outside any scope (e.g. user profiles, app config) and
/// is synced through its own MQTT subscription pattern.
#[derive(Debug, Clone)]
pub struct TopLevelEntity {
    pub entity: String,
    pub subscription_pattern: String,
}

/// Top-level configuration passed to [`Store::new`](crate::Store::new).
///
/// Most fields have sensible defaults; only `entities` and `scope` must be
/// supplied. See [`StoreConfig::new`] for the canonical constructor.
#[derive(Debug, Clone)]
pub struct StoreConfig {
    pub entities: HashMap<String, EntityDefinition>,
    pub scope: ScopeConfig,
    pub top_level_entities: Vec<TopLevelEntity>,
    pub local_only_entities: HashMap<String, EntityDefinition>,
    pub sync_topic_prefix: String,
    pub response_topic_prefix: String,
    pub version_field: String,
    pub updated_at_field: String,
    pub user_scope_field: Option<String>,
    pub event_channel_capacity: usize,
    pub session_expiry_secs: u32,
    pub clean_start: bool,
}

impl StoreConfig {
    #[must_use]
    pub fn new(entities: HashMap<String, EntityDefinition>, scope: ScopeConfig) -> Self {
        Self {
            entities,
            scope,
            top_level_entities: Vec::new(),
            local_only_entities: HashMap::new(),
            sync_topic_prefix: "$DB".to_string(),
            response_topic_prefix: "$DB/clients".to_string(),
            version_field: "version".to_string(),
            updated_at_field: "updatedAt".to_string(),
            user_scope_field: None,
            event_channel_capacity: 4096,
            session_expiry_secs: 3600,
            clean_start: false,
        }
    }

    #[must_use]
    pub fn all_entity_names(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self.entities.keys().map(String::as_str).collect();
        names.extend(self.local_only_entities.keys().map(String::as_str));
        names.sort_unstable();
        names.dedup();
        names
    }
}

/// Local persistence settings. When provided to [`StoreOptions::persistence`],
/// the store opens an fjall-backed database at `db_path` (encrypted with
/// `passphrase` if supplied) for durable local state.
#[derive(Debug, Clone)]
pub struct PersistenceConfig {
    pub db_path: std::path::PathBuf,
    pub passphrase: Option<String>,
}

pub type TicketFuture =
    std::pin::Pin<Box<dyn std::future::Future<Output = crate::error::Result<String>> + Send>>;

pub type TicketProvider = Arc<dyn Fn() -> TicketFuture + Send + Sync>;

/// MQTT broker connection settings for remote sync. When provided to
/// [`StoreOptions::remote`], the store connects to `server_url` and authenticates
/// with a JWT for enhanced auth: `get_ticket` (a per-connect provider, native) is
/// used if set, otherwise the static `ticket`. The static `ticket` is the path
/// browser builds use (no `Send` async provider).
#[derive(Clone)]
pub struct RemoteConfig {
    pub server_url: String,
    pub client_id: Option<String>,
    pub get_ticket: Option<TicketProvider>,
    pub ticket: Option<String>,
    pub request_timeout: Duration,
}

impl std::fmt::Debug for RemoteConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteConfig")
            .field("server_url", &self.server_url)
            .field("client_id", &self.client_id)
            .field("get_ticket", &self.get_ticket.as_ref().map(|_| "<fn>"))
            .field("ticket", &self.ticket.as_ref().map(|_| "<redacted>"))
            .field("request_timeout", &self.request_timeout)
            .finish()
    }
}

impl RemoteConfig {
    #[must_use]
    pub fn new(server_url: impl Into<String>) -> Self {
        Self {
            server_url: server_url.into(),
            client_id: None,
            get_ticket: None,
            ticket: None,
            request_timeout: Duration::from_secs(30),
        }
    }
}

/// Optional layers attached to a [`Store`](crate::Store): durable persistence
/// and remote MQTT sync. Both default to `None`, yielding an in-memory-only
/// store.
#[derive(Debug, Clone, Default)]
pub struct StoreOptions {
    pub persistence: Option<PersistenceConfig>,
    pub remote: Option<RemoteConfig>,
}
