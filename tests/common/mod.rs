use mqdb_agent::Database;
use mqdb_agent::MqdbAgent;
use mqdb_core::types::ScopeConfig as MqdbScopeConfig;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use stitch::config::{EntityDefinition, FieldType, SchemaField, ScopeConfig};
use stitch::StoreConfig;
use tempfile::TempDir;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

static NEXT_PORT: AtomicU16 = AtomicU16::new(28800);

pub fn alloc_port() -> u16 {
    use std::net::TcpListener;
    for _ in 0..256 {
        let candidate = NEXT_PORT.fetch_add(1, Ordering::SeqCst);
        if candidate < 1024 {
            continue;
        }
        if let Ok(listener) = TcpListener::bind(("127.0.0.1", candidate)) {
            drop(listener);
            return candidate;
        }
    }
    panic!("could not allocate broker port")
}

pub fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "stitch=debug,mqtt5=info,mqdb_agent=info".into()),
        )
        .with_test_writer()
        .try_init();
}

pub struct BrokerFixture {
    pub addr: SocketAddr,
    _broker_dir: TempDir,
    shutdown: broadcast::Sender<()>,
    handle: Option<JoinHandle<()>>,
}

impl BrokerFixture {
    pub async fn start() -> Self {
        let port = alloc_port();
        let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        let broker_dir = TempDir::new().unwrap();
        let db = Database::open_without_background_tasks(broker_dir.path().join("agent"))
            .await
            .expect("open broker db");
        let agent = MqdbAgent::new(db)
            .with_bind_address(addr)
            .with_anonymous(true)
            .with_scope_config(MqdbScopeConfig::new(
                "project".to_string(),
                "projectId".to_string(),
            ));
        let (handle, mut ready_rx, shutdown_tx) = agent.start().await.expect("start broker");
        loop {
            if *ready_rx.borrow() {
                break;
            }
            ready_rx.changed().await.expect("ready watcher");
        }
        Self {
            addr,
            _broker_dir: broker_dir,
            shutdown: shutdown_tx,
            handle: Some(handle),
        }
    }

    pub fn url(&self) -> String {
        format!("mqtt://{}", self.addr)
    }

    pub async fn shutdown(mut self) {
        let _ = self.shutdown.send(());
        if let Some(handle) = self.handle.take() {
            let _ =
                tokio::time::timeout(std::time::Duration::from_millis(500), handle).await;
        }
    }
}

impl Drop for BrokerFixture {
    fn drop(&mut self) {
        let _ = self.shutdown.send(());
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

pub fn fixture_config() -> StoreConfig {
    let mut entities = HashMap::new();
    entities.insert(
        "project".to_string(),
        EntityDefinition {
            fields: vec![
                SchemaField {
                    name: "id".to_string(),
                    r#type: FieldType::String,
                    required: true,
                    default: None,
                },
                SchemaField {
                    name: "name".to_string(),
                    r#type: FieldType::String,
                    required: false,
                    default: None,
                },
            ],
            ..EntityDefinition::default()
        },
    );
    entities.insert(
        "task".to_string(),
        EntityDefinition {
            fields: vec![
                SchemaField {
                    name: "id".to_string(),
                    r#type: FieldType::String,
                    required: true,
                    default: None,
                },
                SchemaField {
                    name: "title".to_string(),
                    r#type: FieldType::String,
                    required: false,
                    default: None,
                },
                SchemaField {
                    name: "projectId".to_string(),
                    r#type: FieldType::String,
                    required: true,
                    default: None,
                },
            ],
            ..EntityDefinition::default()
        },
    );

    StoreConfig::new(
        entities,
        ScopeConfig {
            root_entity: "project".to_string(),
            child_entities: vec!["task".to_string()],
            scope_field: "projectId".to_string(),
        },
    )
}

pub fn make_record(pairs: &[(&str, Value)]) -> Map<String, Value> {
    let mut map = Map::new();
    for (k, v) in pairs {
        map.insert((*k).to_string(), v.clone());
    }
    map
}

#[allow(dead_code)]
pub fn _suppress_dead<T>(_: Arc<T>) {}
