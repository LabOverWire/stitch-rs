use super::{Db, DynDb};
use crate::config::{FieldType, SchemaField, StoreConfig};
use crate::error::{Error, Result};
use crate::rt::Shared;
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

pub(crate) async fn open_memory(config: &StoreConfig) -> Result<DynDb> {
    let db = WasmDatabase::new();
    for (name, def) in config
        .entities
        .iter()
        .chain(config.local_only_entities.iter())
    {
        let schema = to_js(&schema_js(&def.fields))?;
        db.add_schema(name.clone(), schema)
            .map_err(|e| js_err("add_schema", &e))?;
    }
    Ok(Shared::new(WasmDb { db }))
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

    async fn list_eq(&self, entity: &str, filters: &[(String, Value)]) -> Result<Vec<Value>> {
        let filter_js: Vec<Value> = filters
            .iter()
            .map(|(field, value)| json!({ "field": field, "op": "eq", "value": value }))
            .collect();
        let options = to_js(&json!({ "filters": filter_js }))?;
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
}
