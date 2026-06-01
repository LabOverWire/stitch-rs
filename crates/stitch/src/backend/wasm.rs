use super::{Db, DynDb};
use crate::config::{EntityDefinition, FieldType, PersistenceConfig, SchemaField, StoreConfig};
use crate::error::{Error, Result};
use crate::rt::Shared;
use mqdb_core::types::{Filter, FilterOp, Pagination, SortDirection, SortOrder};
use mqdb_wasm::WasmDatabase;
use serde_json::{Value, json};
use wasm_bindgen::JsValue;

pub(crate) struct WasmDb {
    db: WasmDatabase,
}

fn js_err(ctx: &str, js: &JsValue) -> Error {
    Error::Config(format!("wasm db {ctx}: {js:?}"))
}

fn to_js(value: &Value) -> Result<JsValue> {
    let text = serde_json::to_string(value)?;
    js_sys::JSON::parse(&text).map_err(|e| js_err("json parse", &e))
}

fn from_js(js: &JsValue) -> Result<Value> {
    let text = js_sys::JSON::stringify(js).map_err(|e| js_err("json stringify", &e))?;
    Ok(serde_json::from_str(&String::from(text))?)
}

fn field_type_str(ft: FieldType) -> &'static str {
    match ft {
        FieldType::String => "string",
        FieldType::Number => "number",
        FieldType::Boolean => "boolean",
        FieldType::Object => "object",
        FieldType::Array => "array",
    }
}

fn filter_op_str(op: &FilterOp) -> &'static str {
    match op {
        FilterOp::Eq => "eq",
        FilterOp::Neq => "neq",
        FilterOp::Lt => "lt",
        FilterOp::Lte => "lte",
        FilterOp::Gt => "gt",
        FilterOp::Gte => "gte",
        FilterOp::In => "in",
        FilterOp::Like => "like",
        FilterOp::IsNull => "is_null",
        FilterOp::IsNotNull => "is_not_null",
    }
}

fn sort_dir_str(dir: &SortDirection) -> &'static str {
    match dir {
        SortDirection::Asc => "asc",
        SortDirection::Desc => "desc",
    }
}

fn schema_js(fields: &[SchemaField]) -> Value {
    let fields: Vec<Value> = fields
        .iter()
        .map(|f| {
            let mut obj = serde_json::Map::new();
            obj.insert("name".into(), Value::String(f.name.clone()));
            obj.insert("type".into(), Value::String(field_type_str(f.r#type).into()));
            obj.insert("required".into(), Value::Bool(f.required));
            if let Some(default) = &f.default {
                obj.insert("default".into(), default.clone());
            }
            Value::Object(obj)
        })
        .collect();
    json!({ "fields": fields })
}

async fn register_all(db: &WasmDb, config: &StoreConfig) -> Result<()> {
    for (name, def) in config
        .entities
        .iter()
        .chain(config.local_only_entities.iter())
    {
        db.register_schema(name, def).await?;
    }
    Ok(())
}

pub(crate) async fn open_memory(config: &StoreConfig) -> Result<DynDb> {
    let db = WasmDb {
        db: WasmDatabase::new(),
    };
    register_all(&db, config).await?;
    Ok(Shared::new(db))
}

pub(crate) async fn open_persistent(
    config: &StoreConfig,
    persistence: &PersistenceConfig,
) -> Result<DynDb> {
    let name = persistence.db_path.to_string_lossy();
    let inner = match persistence.passphrase.as_deref() {
        Some(pass) => WasmDatabase::open_encrypted(&name, pass).await,
        None => WasmDatabase::open_persistent(&name).await,
    }
    .map_err(|e| js_err("open_persistent", &e))?;
    let db = WasmDb { db: inner };
    register_all(&db, config).await?;
    Ok(Shared::new(db))
}

#[async_trait::async_trait(?Send)]
impl Db for WasmDb {
    async fn create(&self, entity: &str, data: Value) -> Result<Value> {
        let payload = to_js(&data)?;
        let out = self
            .db
            .create(entity.to_string(), payload)
            .await
            .map_err(|e| js_err("create", &e))?;
        from_js(&out)
    }

    async fn read(&self, entity: &str, id: &str) -> Result<Option<Value>> {
        match self.db.read(entity.to_string(), id.to_string()).await {
            Ok(js) => Ok(Some(from_js(&js)?)),
            Err(e) => {
                if e.as_string().is_some_and(|s| s.contains("not found")) {
                    Ok(None)
                } else {
                    Err(js_err("read", &e))
                }
            }
        }
    }

    async fn update(&self, entity: &str, id: &str, fields: Value) -> Result<Value> {
        let payload = to_js(&fields)?;
        let out = self
            .db
            .update(entity.to_string(), id.to_string(), payload)
            .await
            .map_err(|e| js_err("update", &e))?;
        from_js(&out)
    }

    async fn delete(&self, entity: &str, id: &str) -> Result<()> {
        self.db
            .delete(entity.to_string(), id.to_string())
            .await
            .map_err(|e| js_err("delete", &e))
    }

    async fn list(
        &self,
        entity: &str,
        filters: Vec<Filter>,
        sort: Vec<SortOrder>,
        pagination: Option<Pagination>,
        projection: Vec<String>,
    ) -> Result<Vec<Value>> {
        let filter_js: Vec<Value> = filters
            .iter()
            .map(|f| json!({ "field": f.field, "op": filter_op_str(&f.op), "value": f.value }))
            .collect();
        let mut options = serde_json::Map::new();
        options.insert("filters".into(), Value::Array(filter_js));
        if !sort.is_empty() {
            let sort_js: Vec<Value> = sort
                .iter()
                .map(|s| json!({ "field": s.field, "direction": sort_dir_str(&s.direction) }))
                .collect();
            options.insert("sort".into(), Value::Array(sort_js));
        }
        if let Some(p) = pagination {
            options.insert("pagination".into(), json!({ "offset": p.offset, "limit": p.limit }));
        }
        if !projection.is_empty() {
            options.insert(
                "projection".into(),
                Value::Array(projection.into_iter().map(Value::String).collect()),
            );
        }
        let options = to_js(&Value::Object(options))?;
        let out = self
            .db
            .list(entity.to_string(), options)
            .await
            .map_err(|e| js_err("list", &e))?;
        match from_js(&out)? {
            Value::Array(items) => Ok(items),
            other => Err(Error::Config(format!("list returned non-array: {other:?}"))),
        }
    }

    async fn register_schema(&self, name: &str, def: &EntityDefinition) -> Result<()> {
        let schema = to_js(&schema_js(&def.fields))?;
        if self.db.is_memory_backend() {
            self.db
                .add_schema(name.to_string(), schema)
                .map_err(|e| js_err("add_schema", &e))?;
            for index_field in &def.indexes {
                self.db
                    .add_index(name.to_string(), vec![index_field.clone()])
                    .map_err(|e| js_err("add_index", &e))?;
            }
        } else {
            self.db
                .add_schema_async(name.to_string(), schema)
                .await
                .map_err(|e| js_err("add_schema_async", &e))?;
            for index_field in &def.indexes {
                self.db
                    .add_index_async(name.to_string(), vec![index_field.clone()])
                    .await
                    .map_err(|e| js_err("add_index_async", &e))?;
            }
        }
        Ok(())
    }

    fn close(&self) {}
}
