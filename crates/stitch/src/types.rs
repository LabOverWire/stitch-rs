use crate::origin::Origin;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// A single entity row, represented as a JSON object.
pub type Record = serde_json::Map<String, Value>;

pub(crate) fn strip_nulls(record: &mut Record) {
    record.retain(|_, v| !v.is_null());
}

/// The kind of mutation an event represents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Operation {
    Insert,
    Update,
    Delete,
}

impl Operation {
    /// Replay-safe ordering when consolidating queued mutations:
    /// `Insert (0) < Update (1) < Delete (2)`.
    #[must_use]
    pub fn priority(self) -> u8 {
        match self {
            Self::Insert => 0,
            Self::Update => 1,
            Self::Delete => 2,
        }
    }
}

/// Emitted on the memory and persistence subscription buses for every write.
#[derive(Debug, Clone)]
pub struct MutationEvent {
    pub operation: Operation,
    pub entity: String,
    pub id: String,
    pub scope_id: String,
    pub data: Option<Record>,
    pub origin: Origin,
}

/// Top-level event emitted by [`Store::subscribe`](crate::Store::subscribe).
/// Includes both per-row mutations and whole-scope load/clear signals.
#[derive(Debug, Clone)]
pub enum StoreEvent {
    /// A single row insert/update/delete.
    Mutation(MutationEvent),
    /// A fresh scope was loaded — fires after `replace_scope` swaps the
    /// memory cache and after explicit `MemoryStore::load_scope` calls.
    ScopeLoaded {
        scope_id: String,
        entities: Vec<String>,
    },
    /// A scope was torn down. Fires from explicit `MemoryStore::clear_scope`
    /// **and** from `replace_scope` for the prior scope when switching
    /// scopes. Subscribers should treat it as "this scope id is no longer
    /// active in memory."
    ScopeCleared {
        scope_id: String,
        entities: Vec<String>,
    },
}

/// Snapshot of one scope: the root record plus all children, grouped by
/// entity. Returned when reading a scope's local state from persistence.
#[derive(Debug, Clone, Default)]
pub struct ScopeBundle {
    pub root: Option<Record>,
    pub children: BTreeMap<String, Vec<Record>>,
}

/// Server-side snapshot of a scope as returned by `open_scope`. Includes the
/// version stamp and any mutations the engine buffered while the fetch was in
/// flight.
#[derive(Debug, Clone)]
pub struct ScopeState {
    pub root: Record,
    pub children: BTreeMap<String, Vec<Record>>,
    pub version: u64,
    pub buffered_mutations: Vec<SyncMutation>,
}

/// Mutation arriving from the broker or queued for outbound send.
#[derive(Debug, Clone)]
pub struct SyncMutation {
    pub op: Operation,
    pub entity: String,
    pub id: String,
    pub data: Option<Record>,
    pub operation_id: Option<String>,
}

/// Row in the persistent offline queue. Replayed in order on reconnect.
#[derive(Debug, Clone)]
pub struct PendingMutation {
    pub op: Operation,
    pub entity: String,
    pub id: String,
    pub scope_id: String,
    pub data: Option<Record>,
    pub created_at: u64,
}

/// Connection state of the remote MQTT client.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConnectionStatus {
    Offline,
    Connecting,
    Connected,
    Error,
    Disconnected,
}

/// Sort direction for [`SortField`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SortDirection {
    Asc,
    Desc,
}

/// A single sort key. Multiple `SortField`s in a [`ListFilter`] are applied in
/// order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortField {
    pub field: String,
    pub direction: SortDirection,
}

/// Optional filters for [`Store::list`](crate::Store::list). All fields are
/// optional; the empty `ListFilter` returns every row of the entity.
#[derive(Debug, Clone, Default)]
pub struct ListFilter {
    pub scope_id: Option<String>,
    pub sort: Vec<SortField>,
    pub projection: Vec<String>,
}
