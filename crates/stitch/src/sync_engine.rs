use crate::config::StoreConfig;
use crate::error::{Error, Result};
use crate::types::{ConnectionStatus, Operation, Record, ScopeState, SyncMutation};
use mqtt5::client::{ConnectionEvent, DisconnectReason, JwtAuthHandler};
use mqtt5::types::{ConnectOptions, Message, PublishOptions, PublishProperties, SubscribeOptions};
use mqtt5::{MqttClient, QoS};
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{broadcast, oneshot};
use uuid::Uuid;

const ORIGIN_USER_PROPERTY: &str = "x-origin-client-id";
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
pub struct MutationDelivery {
    pub scope_id: String,
    pub mutation: SyncMutation,
}

pub struct SyncEngine {
    client_id: String,
    config: Arc<StoreConfig>,
    prefix: String,
    response_prefix: String,
    client: MqttClient,
    state: Arc<EngineState>,
    request_timeout: Duration,
}

type SessionInvalidHandler = Arc<dyn Fn() + Send + Sync>;

struct EngineState {
    pending_requests: Mutex<HashMap<String, oneshot::Sender<Result<Value>>>>,
    subscribed_scopes: Mutex<HashSet<String>>,
    awaiting_state: Mutex<HashSet<String>>,
    buffered: Mutex<HashMap<String, Vec<SyncMutation>>>,
    applied_version: Mutex<HashMap<String, i64>>,
    mutation_bus: broadcast::Sender<MutationDelivery>,
    connection_status: broadcast::Sender<ConnectionStatus>,
    session_invalid: Mutex<Option<SessionInvalidHandler>>,
    client_id: String,
    prefix: String,
    response_prefix: String,
    root_entity: String,
    top_level_patterns: Vec<TopLevelPattern>,
}

#[derive(Clone)]
struct TopLevelPattern {
    entity: String,
    pattern: String,
}

impl SyncEngine {
    pub async fn new(client_id: String, config: Arc<StoreConfig>) -> Result<Self> {
        if client_id.contains(['+', '#', '/']) {
            return Err(Error::Config(format!(
                "clientId must not contain MQTT special characters (+, #, /): {client_id}"
            )));
        }
        let prefix = config.sync_topic_prefix.clone();
        let response_prefix = config.response_topic_prefix.clone();
        let (mutation_bus, _) = broadcast::channel(config.event_channel_capacity);
        let (connection_status, _) = broadcast::channel(16);

        let top_level_patterns: Vec<TopLevelPattern> = config
            .top_level_entities
            .iter()
            .map(|t| TopLevelPattern {
                entity: t.entity.clone(),
                pattern: t.subscription_pattern.clone(),
            })
            .collect();

        let state = Arc::new(EngineState {
            pending_requests: Mutex::new(HashMap::new()),
            subscribed_scopes: Mutex::new(HashSet::new()),
            awaiting_state: Mutex::new(HashSet::new()),
            buffered: Mutex::new(HashMap::new()),
            applied_version: Mutex::new(HashMap::new()),
            mutation_bus,
            connection_status,
            session_invalid: Mutex::new(None),
            client_id: client_id.clone(),
            prefix: prefix.clone(),
            response_prefix: response_prefix.clone(),
            root_entity: config.scope.root_entity.clone(),
            top_level_patterns,
        });

        let client = MqttClient::new(&client_id);
        Self::wire_connection_events(&client, Arc::clone(&state)).await?;

        Ok(Self {
            client_id,
            config,
            prefix,
            response_prefix,
            client,
            state,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
        })
    }

    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    pub fn mutations(&self) -> broadcast::Receiver<MutationDelivery> {
        self.state.mutation_bus.subscribe()
    }

    pub fn connection_status(&self) -> broadcast::Receiver<ConnectionStatus> {
        self.state.connection_status.subscribe()
    }

    pub async fn is_connected(&self) -> bool {
        self.client.is_connected().await
    }

    pub async fn connect(&self, server_url: &str, ticket: Option<String>) -> Result<()> {
        let _ = self
            .state
            .connection_status
            .send(ConnectionStatus::Connecting);

        let mut options = ConnectOptions::new(&self.client_id)
            .with_clean_start(self.config.clean_start)
            .with_keep_alive(Duration::from_secs(60))
            .with_session_expiry_interval(self.config.session_expiry_secs);

        if let Some(ref t) = ticket {
            options = options.with_authentication_method("JWT");
            self.client
                .set_auth_handler(JwtAuthHandler::new(t.clone()))
                .await;
        }

        let result = self
            .client
            .connect_with_options(server_url, options)
            .await
            .map_err(|e| Error::Mqtt(format!("connect:{server_url}: {e}")))?;

        if !result.session_present {
            self.subscribe_to_response_topic().await?;
            self.subscribe_to_top_level().await?;
            for scope_id in self.snapshot_subscribed_scopes() {
                if let Err(e) = self.subscribe_to_scope(&scope_id).await {
                    tracing::warn!(scope = %scope_id, error = %e, "resubscribe failed after session loss");
                }
            }
        }
        Ok(())
    }

    pub async fn disconnect(&self) -> Result<()> {
        self.client
            .disconnect()
            .await
            .map_err(|e| Error::Mqtt(format!("disconnect: {e}")))?;
        self.cleanup_on_disconnect();
        Ok(())
    }

    pub async fn open_scope(&self, scope_id: &str) -> Result<ScopeState> {
        {
            let mut awaiting = self.state.awaiting_state.lock().unwrap();
            awaiting.insert(scope_id.to_string());
        }
        {
            let mut buffered = self.state.buffered.lock().unwrap();
            buffered.insert(scope_id.to_string(), Vec::new());
        }

        match self.do_open_scope(scope_id).await {
            Ok(state) => Ok(state),
            Err(err) => {
                self.state.awaiting_state.lock().unwrap().remove(scope_id);
                self.state.buffered.lock().unwrap().remove(scope_id);
                self.state
                    .subscribed_scopes
                    .lock()
                    .unwrap()
                    .remove(scope_id);
                Err(err)
            }
        }
    }

    async fn do_open_scope(&self, scope_id: &str) -> Result<ScopeState> {
        self.subscribe_to_scope(scope_id).await?;
        self.state
            .subscribed_scopes
            .lock()
            .unwrap()
            .insert(scope_id.to_string());

        let root_future = self.fetch_one(&self.config.scope.root_entity, scope_id);
        let child_futures = self.config.scope.child_entities.iter().map(|entity| {
            let entity = entity.clone();
            async move {
                let list = self.fetch_list(&entity, Some(scope_id)).await?;
                Ok::<(String, Vec<Record>), Error>((entity, list))
            }
        });

        let (root_record, child_results) =
            tokio::join!(root_future, futures::future::try_join_all(child_futures),);
        let root_record = root_record?;
        let mut children: std::collections::BTreeMap<String, Vec<Record>> =
            std::collections::BTreeMap::new();
        for (entity, list) in child_results? {
            children.insert(entity, list);
        }

        let root = root_record.unwrap_or_default();
        let version = root
            .get(&self.config.version_field)
            .and_then(Value::as_u64)
            .unwrap_or(0);

        if version > 0
            && let Ok(version_i64) = i64::try_from(version)
        {
            self.state
                .applied_version
                .lock()
                .unwrap()
                .insert(scope_id.to_string(), version_i64);
        }

        let buffered = self
            .state
            .buffered
            .lock()
            .unwrap()
            .remove(scope_id)
            .unwrap_or_default();
        self.state.awaiting_state.lock().unwrap().remove(scope_id);

        Ok(ScopeState {
            root,
            children,
            version,
            buffered_mutations: buffered,
        })
    }

    pub async fn close_scope(&self, scope_id: &str) -> Result<()> {
        self.state
            .subscribed_scopes
            .lock()
            .unwrap()
            .remove(scope_id);
        self.state.buffered.lock().unwrap().remove(scope_id);
        self.state.awaiting_state.lock().unwrap().remove(scope_id);
        self.state.applied_version.lock().unwrap().remove(scope_id);
        let topic = format!(
            "{}/{}/{}/#",
            self.prefix, self.config.scope.root_entity, scope_id
        );
        let _ = self.client.unsubscribe(topic).await;
        Ok(())
    }

    pub async fn sync_create(
        &self,
        entity: &str,
        scope_id: &str,
        mut data: Record,
    ) -> Result<String> {
        let is_child = self.config.scope.child_entities.iter().any(|e| e == entity);
        if is_child {
            data.entry(self.config.scope.scope_field.clone())
                .or_insert_with(|| Value::String(scope_id.to_string()));
        }
        let topic = format!("{}/{}/create", self.prefix, entity);
        let response = self.request(&topic, Value::Object(data)).await?;
        check_response(&response)?;
        let id = extract_id(&response)?;
        if is_child && !scope_id.is_empty() {
            self.bump_scope_version(scope_id).await?;
        }
        Ok(id)
    }

    pub async fn sync_update(
        &self,
        entity: &str,
        scope_id: &str,
        id: &str,
        data: Record,
    ) -> Result<()> {
        let topic = format!("{}/{}/{}/update", self.prefix, entity, id);
        let response = self.request(&topic, Value::Object(data)).await?;
        check_response(&response)?;
        let is_child = self.config.scope.child_entities.iter().any(|e| e == entity);
        if is_child && !scope_id.is_empty() {
            self.bump_scope_version(scope_id).await?;
        }
        Ok(())
    }

    pub async fn sync_delete(&self, entity: &str, scope_id: &str, id: &str) -> Result<()> {
        let topic = format!("{}/{}/{}/delete", self.prefix, entity, id);
        let response = self.request(&topic, Value::Object(Map::new())).await?;
        check_response(&response)?;
        let is_child = self.config.scope.child_entities.iter().any(|e| e == entity);
        if is_child && !scope_id.is_empty() {
            self.bump_scope_version(scope_id).await?;
        }
        Ok(())
    }

    pub async fn bump_scope_version(&self, scope_id: &str) -> Result<()> {
        let now = now_millis();
        let mut payload = Map::new();
        payload.insert(self.config.version_field.clone(), Value::from(now));
        payload.insert(self.config.updated_at_field.clone(), Value::from(now));
        let topic = format!(
            "{}/{}/{}/update",
            self.prefix, self.config.scope.root_entity, scope_id
        );
        let response = self.request(&topic, Value::Object(payload)).await?;
        check_response(&response)?;
        self.state
            .applied_version
            .lock()
            .unwrap()
            .insert(scope_id.to_string(), now);
        Ok(())
    }

    pub fn applied_version(&self, scope_id: &str) -> Option<i64> {
        self.state
            .applied_version
            .lock()
            .unwrap()
            .get(scope_id)
            .copied()
    }

    pub async fn fetch_one(&self, entity: &str, id: &str) -> Result<Option<Record>> {
        let topic = format!("{}/{}/{}", self.prefix, entity, id);
        match self.request(&topic, Value::Object(Map::new())).await {
            Ok(response) => {
                if let Err(err) = check_response(&response) {
                    if err.is_not_found() {
                        return Ok(None);
                    }
                    return Err(err);
                }
                match response.get("data") {
                    Some(Value::Object(map)) => Ok(Some(map.clone())),
                    _ => Ok(None),
                }
            }
            Err(err) if err.is_not_found() => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub async fn fetch_list(&self, entity: &str, scope_id: Option<&str>) -> Result<Vec<Record>> {
        let mut payload = Map::new();
        if let Some(sid) = scope_id {
            let scope_field = self.config.scope.scope_field.clone();
            payload.insert(
                "filters".into(),
                Value::Array(vec![Value::Object({
                    let mut f = Map::new();
                    f.insert("field".into(), Value::String(scope_field));
                    f.insert("op".into(), Value::String("eq".into()));
                    f.insert("value".into(), Value::String(sid.to_string()));
                    f
                })]),
            );
        }
        let topic = format!("{}/{}/list", self.prefix, entity);
        let response = self.request(&topic, Value::Object(payload)).await?;
        check_response(&response)?;
        let data = response.get("data").cloned().unwrap_or(Value::Null);
        let Value::Array(arr) = data else {
            return Ok(Vec::new());
        };
        let mut out = Vec::with_capacity(arr.len());
        for v in arr {
            if let Value::Object(m) = v {
                out.push(m);
            }
        }
        Ok(out)
    }

    pub async fn request(&self, topic: &str, payload: Value) -> Result<Map<String, Value>> {
        let request_id = Uuid::now_v7().to_string();
        let response_topic = format!("{}/{}/{}", self.response_prefix, self.client_id, request_id);
        let (tx, rx) = oneshot::channel();
        self.state
            .pending_requests
            .lock()
            .unwrap()
            .insert(request_id.clone(), tx);

        let props = PublishProperties {
            response_topic: Some(response_topic),
            correlation_data: Some(request_id.as_bytes().to_vec()),
            user_properties: vec![(ORIGIN_USER_PROPERTY.to_string(), self.client_id.clone())],
            ..PublishProperties::default()
        };
        let opts = PublishOptions {
            qos: QoS::AtLeastOnce,
            retain: false,
            properties: props,
            skip_codec: false,
        };

        let body = serde_json::to_vec(&payload)?;
        let publish = self
            .client
            .publish_with_options(topic.to_string(), body, opts)
            .await;
        if let Err(e) = publish {
            self.state
                .pending_requests
                .lock()
                .unwrap()
                .remove(&request_id);
            return Err(Error::Mqtt(format!("publish:{topic}: {e}")));
        }

        let timeout = tokio::time::timeout(self.request_timeout, rx).await;
        match timeout {
            Ok(Ok(result)) => match result? {
                Value::Object(m) => Ok(m),
                _ => Err(Error::Mqtt("response was not an object".into())),
            },
            Ok(Err(_)) => Err(Error::ConnectionClosed),
            Err(_) => {
                self.state
                    .pending_requests
                    .lock()
                    .unwrap()
                    .remove(&request_id);
                Err(Error::Timeout(
                    u64::try_from(self.request_timeout.as_millis()).unwrap_or(u64::MAX),
                ))
            }
        }
    }

    async fn subscribe_to_response_topic(&self) -> Result<()> {
        let pattern = format!("{}/{}/#", self.response_prefix, self.client_id);
        let state = Arc::clone(&self.state);
        let options = SubscribeOptions {
            qos: QoS::AtLeastOnce,
            ..SubscribeOptions::default()
        };
        self.client
            .subscribe_with_options(pattern, options, move |msg: Message| {
                handle_response(&state, msg);
            })
            .await
            .map_err(|e| Error::Mqtt(format!("subscribe_response: {e}")))?;
        Ok(())
    }

    fn snapshot_subscribed_scopes(&self) -> Vec<String> {
        self.state
            .subscribed_scopes
            .lock()
            .unwrap()
            .iter()
            .cloned()
            .collect()
    }

    async fn subscribe_to_top_level(&self) -> Result<()> {
        let root_wildcard = format!(
            "{}/{}/+/events/#",
            self.prefix, self.config.scope.root_entity
        );
        let state = Arc::clone(&self.state);
        let options = SubscribeOptions {
            qos: QoS::AtLeastOnce,
            ..SubscribeOptions::default()
        };
        self.client
            .subscribe_with_options(root_wildcard, options.clone(), move |msg: Message| {
                handle_root_wildcard_message(&state, msg);
            })
            .await
            .map_err(|e| Error::Mqtt(format!("subscribe_root_wildcard: {e}")))?;

        for tl in &self.state.top_level_patterns {
            let state = Arc::clone(&self.state);
            let entity = tl.entity.clone();
            self.client
                .subscribe_with_options(tl.pattern.clone(), options.clone(), move |msg: Message| {
                    handle_top_level_message(&state, &entity, msg);
                })
                .await
                .map_err(|e| Error::Mqtt(format!("subscribe_top_level:{}: {e}", tl.entity)))?;
        }
        Ok(())
    }

    async fn subscribe_to_scope(&self, scope_id: &str) -> Result<()> {
        let pattern = format!(
            "{}/{}/{}/#",
            self.prefix, self.config.scope.root_entity, scope_id
        );
        let state = Arc::clone(&self.state);
        let options = SubscribeOptions {
            qos: QoS::AtLeastOnce,
            ..SubscribeOptions::default()
        };
        self.client
            .subscribe_with_options(pattern, options, move |msg: Message| {
                handle_scope_message(&state, msg);
            })
            .await
            .map_err(|e| Error::Mqtt(format!("subscribe_scope:{scope_id}: {e}")))?;
        Ok(())
    }

    async fn wire_connection_events(client: &MqttClient, state: Arc<EngineState>) -> Result<()> {
        client
            .on_connection_event(move |event| {
                let (status, auth_failure) = match event {
                    ConnectionEvent::Connecting => (ConnectionStatus::Connecting, false),
                    ConnectionEvent::Connected { .. } => (ConnectionStatus::Connected, false),
                    ConnectionEvent::Disconnected {
                        reason: DisconnectReason::AuthFailure,
                    } => (ConnectionStatus::Error, true),
                    ConnectionEvent::Disconnected { .. } => (ConnectionStatus::Disconnected, false),
                    ConnectionEvent::Reconnecting { .. } => (ConnectionStatus::Connecting, false),
                    ConnectionEvent::ReconnectFailed { .. } => (ConnectionStatus::Error, false),
                };
                let _ = state.connection_status.send(status);
                if auth_failure && let Some(handler) = state.session_invalid.lock().unwrap().clone()
                {
                    handler();
                }
            })
            .await
            .map_err(|e| Error::Mqtt(format!("on_connection_event: {e}")))?;
        Ok(())
    }

    pub fn set_session_invalid_handler<F>(&self, handler: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        *self.state.session_invalid.lock().unwrap() = Some(Arc::new(handler));
    }

    pub fn clear_session_invalid_handler(&self) {
        *self.state.session_invalid.lock().unwrap() = None;
    }

    pub async fn reconnect(&self, server_url: &str, ticket: Option<String>) -> Result<()> {
        let _ = self.client.disconnect().await;
        self.connect(server_url, ticket).await
    }

    fn cleanup_on_disconnect(&self) {
        self.state.subscribed_scopes.lock().unwrap().clear();
        self.state.awaiting_state.lock().unwrap().clear();
        self.state.buffered.lock().unwrap().clear();
        self.state.applied_version.lock().unwrap().clear();
        let pending: Vec<oneshot::Sender<Result<Value>>> = self
            .state
            .pending_requests
            .lock()
            .unwrap()
            .drain()
            .map(|(_, v)| v)
            .collect();
        for tx in pending {
            let _ = tx.send(Err(Error::ConnectionClosed));
        }
    }
}

fn handle_response(state: &EngineState, msg: Message) {
    let expected = format!("{}/{}/", state.response_prefix, state.client_id);
    let Some(request_id) = msg.topic.strip_prefix(&expected) else {
        return;
    };
    let Some(sender) = state.pending_requests.lock().unwrap().remove(request_id) else {
        return;
    };
    let parsed = serde_json::from_slice::<Value>(&msg.payload);
    let result: Result<Value> = match parsed {
        Ok(value) => Ok(value),
        Err(err) => Err(Error::Serde(err)),
    };
    let _ = sender.send(result);
}

fn handle_scope_message(state: &EngineState, msg: Message) {
    if own_user_property(state, &msg) {
        return;
    }
    let Some(parsed) = parse_scoped_topic(&state.prefix, &state.root_entity, &msg.topic) else {
        return;
    };
    let event_op = match parsed.event_type.as_str() {
        "created" => Operation::Insert,
        "updated" => Operation::Update,
        "deleted" => Operation::Delete,
        _ => return,
    };
    let payload = match serde_json::from_slice::<Value>(&msg.payload) {
        Ok(v) => v,
        Err(_) => return,
    };
    if sender_is_self(&state.client_id, &payload) {
        return;
    }
    let Some(mutation) = build_mutation(parsed.entity.clone(), event_op, payload) else {
        return;
    };

    let scope_id = parsed.scope_id.clone();
    let buffered = state.awaiting_state.lock().unwrap().contains(&scope_id);
    if buffered && let Some(list) = state.buffered.lock().unwrap().get_mut(&scope_id) {
        list.push(mutation);
        return;
    }
    let _ = state
        .mutation_bus
        .send(MutationDelivery { scope_id, mutation });
}

fn handle_root_wildcard_message(state: &EngineState, msg: Message) {
    if own_user_property(state, &msg) {
        return;
    }
    let Some(parsed) = parse_scoped_topic(&state.prefix, &state.root_entity, &msg.topic) else {
        return;
    };
    if parsed.entity != state.root_entity {
        return;
    }
    if state
        .subscribed_scopes
        .lock()
        .unwrap()
        .contains(&parsed.scope_id)
    {
        return;
    }
    let event_op = match parsed.event_type.as_str() {
        "created" => Operation::Insert,
        "updated" => Operation::Update,
        "deleted" => Operation::Delete,
        _ => return,
    };
    let payload = match serde_json::from_slice::<Value>(&msg.payload) {
        Ok(v) => v,
        Err(_) => return,
    };
    if sender_is_self(&state.client_id, &payload) {
        return;
    }
    let Some(mutation) = build_mutation(parsed.entity.clone(), event_op, payload) else {
        return;
    };
    let _ = state.mutation_bus.send(MutationDelivery {
        scope_id: parsed.scope_id,
        mutation,
    });
}

fn handle_top_level_message(state: &EngineState, entity: &str, msg: Message) {
    if own_user_property(state, &msg) {
        return;
    }
    let payload = match serde_json::from_slice::<Value>(&msg.payload) {
        Ok(v) => v,
        Err(_) => return,
    };
    if sender_is_self(&state.client_id, &payload) {
        return;
    }
    let Some(event_op) = payload
        .get("operation")
        .and_then(Value::as_str)
        .and_then(|s| match s {
            "Create" => Some(Operation::Insert),
            "Update" => Some(Operation::Update),
            "Delete" => Some(Operation::Delete),
            _ => None,
        })
    else {
        return;
    };
    let Some(mutation) = build_mutation(entity.to_string(), event_op, payload) else {
        return;
    };
    let _ = state.mutation_bus.send(MutationDelivery {
        scope_id: String::new(),
        mutation,
    });
}

fn own_user_property(state: &EngineState, msg: &Message) -> bool {
    msg.properties
        .user_properties
        .iter()
        .any(|(k, v)| k == ORIGIN_USER_PROPERTY && v == &state.client_id)
}

fn sender_is_self(client_id: &str, payload: &Value) -> bool {
    payload
        .get("sender")
        .and_then(Value::as_str)
        .map(|s| s == client_id)
        .unwrap_or(false)
        || payload
            .get("client_id")
            .and_then(Value::as_str)
            .map(|s| s == client_id)
            .unwrap_or(false)
}

struct ScopedTopic {
    scope_id: String,
    entity: String,
    event_type: String,
}

fn parse_scoped_topic(prefix: &str, root_entity: &str, topic: &str) -> Option<ScopedTopic> {
    let head = format!("{prefix}/{root_entity}/");
    let rest = topic.strip_prefix(&head)?;
    let parts: Vec<&str> = rest.split('/').collect();
    match parts.as_slice() {
        [scope_id, "events", event_type] => Some(ScopedTopic {
            scope_id: (*scope_id).to_string(),
            entity: root_entity.to_string(),
            event_type: (*event_type).to_string(),
        }),
        [scope_id, entity, "events", event_type] => Some(ScopedTopic {
            scope_id: (*scope_id).to_string(),
            entity: (*entity).to_string(),
            event_type: (*event_type).to_string(),
        }),
        _ => None,
    }
}

fn build_mutation(entity: String, op: Operation, payload: Value) -> Option<SyncMutation> {
    let Value::Object(obj) = payload else {
        return None;
    };
    let id = obj.get("id").and_then(Value::as_str)?.to_string();
    let data = obj.get("data").and_then(|v| match v {
        Value::Object(m) => Some(m.clone()),
        Value::Null => None,
        _ => None,
    });
    let operation_id = obj
        .get("operation_id")
        .and_then(Value::as_str)
        .map(str::to_string);
    Some(SyncMutation {
        op,
        entity,
        id,
        data,
        operation_id,
    })
}

fn check_response(response: &Map<String, Value>) -> Result<()> {
    let status = response.get("status").and_then(Value::as_str);
    if status != Some("error") {
        return Ok(());
    }
    let code = response.get("code").and_then(Value::as_i64).unwrap_or(0);
    let message = response
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("operation failed")
        .to_string();
    let entity = response
        .get("entity")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let id = response
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    Err(match code {
        401 => Error::SessionInvalid,
        403 => Error::Ownership { entity, id },
        404 => Error::NotFound { entity, id },
        409 => Error::Conflict { entity, id },
        _ => Error::Mqtt(message),
    })
}

fn extract_id(response: &Map<String, Value>) -> Result<String> {
    let data = response.get("data");
    let Some(Value::Object(obj)) = data else {
        return Err(Error::Mqtt("response missing data object".into()));
    };
    obj.get("id")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| Error::Mqtt("response missing data.id".into()))
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}
