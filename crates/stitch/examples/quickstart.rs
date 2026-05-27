use serde_json::json;
use std::collections::HashMap;
use stitch::config::{EntityDefinition, FieldType, PersistenceConfig, SchemaField, ScopeConfig};
use stitch::types::StoreEvent;
use stitch::{Origin, Store, StoreConfig, StoreOptions};

#[tokio::main]
async fn main() -> stitch::Result<()> {
    let mut entities = HashMap::new();
    entities.insert(
        "project".into(),
        EntityDefinition {
            fields: vec![
                SchemaField {
                    name: "id".into(),
                    r#type: FieldType::String,
                    required: true,
                    default: None,
                },
                SchemaField {
                    name: "name".into(),
                    r#type: FieldType::String,
                    required: false,
                    default: None,
                },
            ],
            ..EntityDefinition::default()
        },
    );

    let config = StoreConfig::new(
        entities,
        ScopeConfig {
            root_entity: "project".into(),
            child_entities: vec!["task".into()],
            scope_field: "projectId".into(),
        },
    );

    let options = StoreOptions {
        persistence: Some(PersistenceConfig {
            db_path: "./stitch-data".into(),
            passphrase: None,
        }),
        ..StoreOptions::default()
    };

    let store = Store::new(config, options);
    store.initialize().await?;

    let mut events = store.subscribe()?;
    tokio::spawn(async move {
        while let Ok(StoreEvent::Mutation(m)) = events.recv().await {
            println!("{:?} {}/{}", m.operation, m.entity, m.id);
        }
    });

    let mut data = serde_json::Map::new();
    data.insert("id".into(), json!("p1"));
    data.insert("name".into(), json!("Alpha"));
    store.create("project", "p1", data, Origin::Local).await?;
    store.replace_scope("p1").await?;

    let project = store.read("project", "p1").await?.expect("just created");
    println!("loaded: {:?}", project);

    store.shutdown().await?;
    Ok(())
}
