use super::{
    ConnectArgs, ConnectOutcome, ConnectionHandler, IncomingMessage, MessageHandler, MqttClientApi,
    PublishArgs,
};
use crate::error::{Error, Result};
use crate::types::ConnectionStatus;
use mqtt5::client::{ConnectionEvent, DisconnectReason, JwtAuthHandler};
use mqtt5::types::{ConnectOptions, Message, PublishOptions, PublishProperties, SubscribeOptions};
use mqtt5::{MqttClient, QoS};
use std::time::Duration;

pub(crate) struct NativeMqttClient {
    client: MqttClient,
    client_id: String,
}

impl NativeMqttClient {
    pub(crate) fn new(client_id: &str) -> Self {
        Self {
            client: MqttClient::new(client_id),
            client_id: client_id.to_string(),
        }
    }
}

#[async_trait::async_trait]
impl MqttClientApi for NativeMqttClient {
    async fn connect(&self, url: &str, args: ConnectArgs) -> Result<ConnectOutcome> {
        let mut options = ConnectOptions::new(&self.client_id)
            .with_clean_start(args.clean_start)
            .with_keep_alive(Duration::from_secs(args.keep_alive_secs))
            .with_session_expiry_interval(args.session_expiry_secs);

        if let Some(ticket) = &args.jwt_ticket {
            options = options.with_authentication_method("JWT");
            self.client
                .set_auth_handler(JwtAuthHandler::new(ticket.clone()))
                .await;
        } else if let (Some(user), Some(pass)) = (&args.username, &args.password) {
            options = options.with_credentials(user.clone(), pass.clone());
        }

        let result = self
            .client
            .connect_with_options(url, options)
            .await
            .map_err(|e| Error::Mqtt(format!("connect:{url}: {e}")))?;
        Ok(ConnectOutcome {
            session_present: result.session_present,
        })
    }

    async fn disconnect(&self) -> Result<()> {
        self.client
            .disconnect()
            .await
            .map_err(|e| Error::Mqtt(format!("disconnect: {e}")))
    }

    async fn is_connected(&self) -> bool {
        self.client.is_connected().await
    }

    async fn publish(&self, topic: String, payload: Vec<u8>, args: PublishArgs) -> Result<()> {
        let props = PublishProperties {
            response_topic: Some(args.response_topic),
            correlation_data: Some(args.correlation_data),
            user_properties: args.user_properties,
            ..PublishProperties::default()
        };
        let opts = PublishOptions {
            qos: QoS::AtLeastOnce,
            retain: false,
            properties: props,
            skip_codec: false,
        };
        self.client
            .publish_with_options(topic.clone(), payload, opts)
            .await
            .map(|_| ())
            .map_err(|e| Error::Mqtt(format!("publish:{topic}: {e}")))
    }

    async fn subscribe(&self, topic: String, on_message: MessageHandler) -> Result<()> {
        let options = SubscribeOptions {
            qos: QoS::AtLeastOnce,
            ..SubscribeOptions::default()
        };
        self.client
            .subscribe_with_options(topic.clone(), options, move |msg: Message| {
                on_message(IncomingMessage {
                    topic: msg.topic,
                    payload: msg.payload,
                    user_properties: msg.properties.user_properties,
                });
            })
            .await
            .map(|_| ())
            .map_err(|e| Error::Mqtt(format!("subscribe:{topic}: {e}")))
    }

    async fn unsubscribe(&self, topic: String) -> Result<()> {
        self.client
            .unsubscribe(topic.clone())
            .await
            .map(|_| ())
            .map_err(|e| Error::Mqtt(format!("unsubscribe:{topic}: {e}")))
    }

    async fn on_connection_event(&self, handler: ConnectionHandler) -> Result<()> {
        self.client
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
                handler(status, auth_failure);
            })
            .await
            .map_err(|e| Error::Mqtt(format!("on_connection_event: {e}")))
    }
}
