use crate::backend::{DynDb, open_memory_db, value_to_record};
use crate::config::StoreConfig;
use crate::error::Result;
use crate::origin::Origin;
use crate::rt::Shared;
use crate::types::{MutationEvent, Operation, Record, ScopeBundle, StoreEvent, strip_nulls};
use mqdb_core::types::{Filter, FilterOp};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};
use tokio::sync::broadcast;

pub struct MemoryStore {
    state: RwLock<State>,
    bus: broadcast::Sender<StoreEvent>,
    config: Arc<StoreConfig>,
    top_level: HashSet<String>,
    batch: Mutex<BatchState>,
    versions: Mutex<HashMap<(String, String), u64>>,
}

struct State {
    db: DynDb,
    current_scope: Option<String>,
}

#[derive(Default)]
struct BatchState {
    depth: u32,
    buffered: HashMap<(String, String), MutationEvent>,
}

impl MemoryStore {
    pub async fn new(config: Arc<StoreConfig>) -> Result<Self> {
        let db = open_memory_db(&config).await?;
        let (bus, _) = broadcast::channel(config.event_channel_capacity);
        let top_level: HashSet<String> = config
            .top_level_entities
            .iter()
            .map(|t| t.entity.clone())
            .collect();
        Ok(Self {
            state: RwLock::new(State {
                db,
                current_scope: None,
            }),
            bus,
            config,
            top_level,
            batch: Mutex::new(BatchState::default()),
            versions: Mutex::new(HashMap::new()),
        })
    }

    pub fn begin_batch(&self) {
        let mut batch = self.batch.lock().unwrap();
        batch.depth = batch.depth.saturating_add(1);
    }

    pub fn end_batch(&self) {
        let drained: Vec<MutationEvent> = {
            let mut batch = self.batch.lock().unwrap();
            if batch.depth == 0 {
                return;
            }
            batch.depth -= 1;
            if batch.depth > 0 {
                return;
            }
            batch.buffered.drain().map(|(_, ev)| ev).collect()
        };
        for event in drained {
            self.bump_version(
                &self.version_scope(&event.entity, &event.scope_id),
                &event.entity,
            );
            let _ = self.bus.send(StoreEvent::Mutation(event));
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<StoreEvent> {
        self.bus.subscribe()
    }

    pub fn current_scope(&self) -> Option<String> {
        self.state.read().unwrap().current_scope.clone()
    }

    fn db(&self) -> DynDb {
        Shared::clone(&self.state.read().unwrap().db)
    }

    pub async fn create(
        &self,
        entity: &str,
        scope_id: &str,
        mut data: Record,
        origin: Origin,
    ) -> Result<Record> {
        let scope_field = &self.config.scope.scope_field;
        let is_root = entity == self.config.scope.root_entity;
        let is_top_level = self.top_level.contains(entity);
        strip_nulls(&mut data);
        if !is_root && !is_top_level && !data.contains_key(scope_field) {
            data.insert(scope_field.clone(), Value::String(scope_id.to_string()));
        }

        let db = self.db();
        let value = db.create(entity, Value::Object(data)).await?;

        let record = value_to_record(value)?;
        let id = record
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        self.emit_mutation(MutationEvent {
            operation: Operation::Insert,
            entity: entity.to_string(),
            id,
            scope_id: scope_id.to_string(),
            data: Some(record.clone()),
            origin,
        });
        Ok(record)
    }

    pub async fn read(&self, entity: &str, id: &str) -> Result<Option<Record>> {
        let db = self.db();
        match db.read(entity, id).await? {
            Some(value) => Ok(Some(value_to_record(value)?)),
            None => Ok(None),
        }
    }

    pub async fn list(&self, entity: &str, scope_id: &str) -> Result<Vec<Record>> {
        let db = self.db();
        let filters = self.list_filters(entity, scope_id);
        let values = db
            .list(entity, filters, Vec::new(), None, Vec::new())
            .await?;
        values.into_iter().map(value_to_record).collect()
    }

    #[cfg(target_arch = "wasm32")]
    pub fn read_sync(&self, entity: &str, id: &str) -> Result<Option<Record>> {
        match self.db().read_sync(entity, id)? {
            Some(value) => Ok(Some(value_to_record(value)?)),
            None => Ok(None),
        }
    }

    #[cfg(target_arch = "wasm32")]
    pub fn list_sync(&self, entity: &str, scope_id: &str) -> Result<Vec<Record>> {
        let filters = self.list_filters(entity, scope_id);
        let values = self
            .db()
            .list_sync(entity, filters, Vec::new(), None, Vec::new())?;
        values.into_iter().map(value_to_record).collect()
    }

    #[cfg(target_arch = "wasm32")]
    pub fn count_sync(&self, entity: &str, scope_id: &str) -> Result<usize> {
        let filters = self.list_filters(entity, scope_id);
        self.db().count_sync(entity, filters)
    }

    pub async fn update(
        &self,
        entity: &str,
        id: &str,
        mut fields: Record,
        origin: Origin,
    ) -> Result<Record> {
        strip_nulls(&mut fields);
        let db = self.db();
        let updated = db.update(entity, id, Value::Object(fields)).await?;
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
        let db = self.db();
        let existing = self.read(entity, id).await?;
        let scope_id = existing
            .as_ref()
            .and_then(|r| self.resolve_scope(entity, r))
            .unwrap_or_default();

        db.delete(entity, id).await?;

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

    pub async fn load_scope(&self, scope_id: &str, bundle: ScopeBundle) -> Result<()> {
        let fresh = open_memory_db(&self.config).await?;

        if let Some(root) = bundle.root {
            fresh
                .create(&self.config.scope.root_entity, Value::Object(root))
                .await?;
        }

        for (entity, records) in &bundle.children {
            for record in records {
                fresh.create(entity, Value::Object(record.clone())).await?;
            }
        }

        let previous_scope = {
            let mut state = self.state.write().unwrap();
            let prev = state.current_scope.take();
            state.db = fresh;
            state.current_scope = Some(scope_id.to_string());
            prev
        };

        if let Some(prev) = previous_scope
            && prev != scope_id
        {
            self.remove_scope_versions(&prev);
            let _ = self.bus.send(StoreEvent::ScopeCleared {
                scope_id: prev,
                entities: self.all_scoped_entities(),
            });
        }

        self.bump_scope_versions(scope_id);
        let _ = self.bus.send(StoreEvent::ScopeLoaded {
            scope_id: scope_id.to_string(),
            entities: self.all_scoped_entities(),
        });
        Ok(())
    }

    pub async fn clear_scope(&self, scope_id: &str) -> Result<()> {
        let fresh = open_memory_db(&self.config).await?;
        {
            let mut state = self.state.write().unwrap();
            state.db = fresh;
            if state.current_scope.as_deref() == Some(scope_id) {
                state.current_scope = None;
            }
        }
        self.remove_scope_versions(scope_id);
        let _ = self.bus.send(StoreEvent::ScopeCleared {
            scope_id: scope_id.to_string(),
            entities: self.all_scoped_entities(),
        });
        Ok(())
    }

    pub fn get_version(&self, scope_id: &str, entity: &str) -> u64 {
        self.versions
            .lock()
            .unwrap()
            .get(&(scope_id.to_string(), entity.to_string()))
            .copied()
            .unwrap_or(0)
    }

    fn bump_version(&self, scope_id: &str, entity: &str) {
        let mut versions = self.versions.lock().unwrap();
        let counter = versions
            .entry((scope_id.to_string(), entity.to_string()))
            .or_insert(0);
        *counter += 1;
    }

    fn bump_scope_versions(&self, scope_id: &str) {
        for entity in self.all_scoped_entities() {
            self.bump_version(&self.version_scope(&entity, scope_id), &entity);
        }
    }

    fn remove_scope_versions(&self, scope_id: &str) {
        self.versions
            .lock()
            .unwrap()
            .retain(|(scope, _), _| scope != scope_id);
    }

    fn version_scope(&self, entity: &str, scope_id: &str) -> String {
        if self.top_level.contains(entity) {
            String::new()
        } else {
            scope_id.to_string()
        }
    }

    fn emit_mutation(&self, event: MutationEvent) {
        {
            let mut batch = self.batch.lock().unwrap();
            if batch.depth > 0 {
                let key = (event.scope_id.clone(), event.entity.clone());
                batch.buffered.insert(key, event);
                return;
            }
        }
        self.bump_version(
            &self.version_scope(&event.entity, &event.scope_id),
            &event.entity,
        );
        let _ = self.bus.send(StoreEvent::Mutation(event));
    }

    fn list_filters(&self, entity: &str, scope_id: &str) -> Vec<Filter> {
        if entity == self.config.scope.root_entity || self.top_level.contains(entity) {
            Vec::new()
        } else {
            vec![Filter::new(
                self.config.scope.scope_field.clone(),
                FilterOp::Eq,
                Value::String(scope_id.to_string()),
            )]
        }
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

    fn all_scoped_entities(&self) -> Vec<String> {
        std::iter::once(self.config.scope.root_entity.clone())
            .chain(self.config.scope.child_entities.iter().cloned())
            .chain(self.top_level.iter().cloned())
            .collect()
    }
}
