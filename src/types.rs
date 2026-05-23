use crate::origin::Origin;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

pub type Record = serde_json::Map<String, Value>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Operation {
    Insert,
    Update,
    Delete,
}

impl Operation {
    #[must_use]
    pub fn priority(self) -> u8 {
        match self {
            Self::Insert => 0,
            Self::Update => 1,
            Self::Delete => 2,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MutationEvent {
    pub operation: Operation,
    pub entity: String,
    pub id: String,
    pub scope_id: String,
    pub data: Option<Record>,
    pub origin: Origin,
}

#[derive(Debug, Clone)]
pub enum StoreEvent {
    Mutation(MutationEvent),
    ScopeLoaded {
        scope_id: String,
        entities: Vec<String>,
    },
    ScopeCleared {
        scope_id: String,
        entities: Vec<String>,
    },
}

#[derive(Debug, Clone, Default)]
pub struct ScopeBundle {
    pub root: Option<Record>,
    pub children: BTreeMap<String, Vec<Record>>,
}

#[derive(Debug, Clone)]
pub struct ScopeState {
    pub root: Record,
    pub children: BTreeMap<String, Vec<Record>>,
    pub version: u64,
    pub buffered_mutations: Vec<SyncMutation>,
}

#[derive(Debug, Clone)]
pub struct SyncMutation {
    pub op: Operation,
    pub entity: String,
    pub id: String,
    pub data: Option<Record>,
    pub operation_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PendingMutation {
    pub op: Operation,
    pub entity: String,
    pub id: String,
    pub scope_id: String,
    pub data: Option<Record>,
    pub created_at: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConnectionStatus {
    Offline,
    Connecting,
    Connected,
    Error,
    Disconnected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortField {
    pub field: String,
    pub direction: SortDirection,
}

#[derive(Debug, Clone, Default)]
pub struct ListFilter {
    pub scope_id: Option<String>,
    pub sort: Vec<SortField>,
    pub projection: Vec<String>,
}
