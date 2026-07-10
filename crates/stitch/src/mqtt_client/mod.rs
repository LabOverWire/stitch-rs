use crate::backend::MaybeSendSync;
use crate::error::Result;
use crate::rt::Shared;
use crate::types::ConnectionStatus;

#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(target_arch = "wasm32")]
mod wasm;

/// Connect parameters, platform-neutral. `jwt_ticket` drives MQTT5 enhanced
/// auth; `username` + `password` drive classic password auth (JWT wins when
/// both are set).
pub(crate) struct ConnectArgs {
    pub clean_start: bool,
    pub keep_alive_secs: u64,
    pub session_expiry_secs: u32,
    pub jwt_ticket: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub will: Option<crate::config::WillConfig>,
}

pub(crate) struct ConnectOutcome {
    pub session_present: bool,
}

/// Publish parameters. All sync publishes are request/response at QoS 1, so the
/// adapter hardcodes `AtLeastOnce` and carries the response topic, correlation
/// data, and user properties.
pub(crate) struct PublishArgs {
    pub response_topic: String,
    pub correlation_data: Vec<u8>,
    pub user_properties: Vec<(String, String)>,
}

/// Inbound message, reduced to what the sync engine reads.
pub(crate) struct IncomingMessage {
    pub topic: String,
    pub payload: Vec<u8>,
    pub user_properties: Vec<(String, String)>,
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) type MessageHandler = Box<dyn Fn(IncomingMessage) + Send + Sync + 'static>;
#[cfg(target_arch = "wasm32")]
pub(crate) type MessageHandler = Box<dyn Fn(IncomingMessage) + 'static>;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) type ConnectionHandler = Box<dyn Fn(ConnectionStatus, bool) + Send + Sync + 'static>;
#[cfg(target_arch = "wasm32")]
pub(crate) type ConnectionHandler = Box<dyn Fn(ConnectionStatus, bool) + 'static>;

/// The minimal MQTT client surface the sync engine drives. The native adapter
/// wraps `mqtt5::MqttClient` (TCP/TLS); the wasm adapter wraps
/// `mqtt5_wasm::WasmMqttClient` (WebSocket). The connection-event handler
/// receives a neutral `(status, auth_failure)` pair.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub(crate) trait MqttClientApi: MaybeSendSync {
    async fn connect(&self, url: &str, args: ConnectArgs) -> Result<ConnectOutcome>;
    async fn disconnect(&self) -> Result<()>;
    async fn disconnect_abnormally(&self) -> Result<()>;
    async fn is_connected(&self) -> bool;
    async fn publish(&self, topic: String, payload: Vec<u8>, args: PublishArgs) -> Result<()>;
    async fn subscribe(&self, topic: String, on_message: MessageHandler) -> Result<()>;
    async fn unsubscribe(&self, topic: String) -> Result<()>;
    async fn on_connection_event(&self, handler: ConnectionHandler) -> Result<()>;
}

pub(crate) type DynMqttClient = Shared<dyn MqttClientApi>;

/// Construct a platform-appropriate MQTT client for `client_id`.
pub(crate) fn new_client(client_id: &str) -> DynMqttClient {
    #[cfg(not(target_arch = "wasm32"))]
    {
        Shared::new(native::NativeMqttClient::new(client_id))
    }
    #[cfg(target_arch = "wasm32")]
    {
        Shared::new(wasm::WasmMqttClientAdapter::new(client_id))
    }
}
