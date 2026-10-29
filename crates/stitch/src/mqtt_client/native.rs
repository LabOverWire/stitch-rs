use super::{
    ConnectArgs, ConnectOutcome, ConnectionHandler, IncomingMessage, MessageHandler, MqttClientApi,
    PublishArgs,
};
use crate::error::{Error, Result};
use crate::types::ConnectionStatus;
use mqtt5::client::{ConnectionEvent, DisconnectReason, JwtAuthHandler};
use mqtt5::types::{
    ConnectOptions, Message, PublishOptions, PublishProperties, ReasonCode, SubscribeOptions,
    WillMessage,
};
use mqtt5::{MqttClient, MqttError, QoS};
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
            .with_resume_existing_session(!args.clean_start)
            .with_keep_alive(Duration::from_secs(args.keep_alive_secs))
            .with_session_expiry_interval(args.session_expiry_secs);

        if let Some(will) = &args.will {
            let qos = match will.qos {
                1 => QoS::AtLeastOnce,
                2 => QoS::ExactlyOnce,
                _ => QoS::AtMostOnce,
            };
            let mut message = WillMessage::new(will.topic.clone(), will.payload.clone())
                .with_qos(qos)
                .with_retain(will.retain);
            message.properties.will_delay_interval = will.will_delay_interval_secs;
            message
                .properties
                .content_type
                .clone_from(&will.content_type);
            options = options.with_will(message);
        }

        if let Some(ticket) = &args.jwt_ticket {
            options = options.with_authentication_method("JWT");
            self.client
                .set_auth_handler(JwtAuthHandler::new(ticket.clone()))
                .await;
        } else if let (Some(user), Some(pass)) = (&args.username, &args.password) {
            options = options.with_credentials(user.clone(), pass.clone());
        }

        self.client.set_queue_on_disconnect(false).await;
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

    async fn disconnect_abnormally(&self) -> Result<()> {
        self.client
            .disconnect_abnormally()
            .await
            .map_err(|e| Error::Mqtt(format!("disconnect_abnormally: {e}")))
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
            .map_err(|e| match e {
                MqttError::PublishFailed(reason_code) => Error::PublishRejected {
                    reason_code: u8::from(reason_code),
                },
                other => Error::Mqtt(format!("publish:{topic}: {other}")),
            })
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
                        reason:
                            DisconnectReason::AuthFailure
                            | DisconnectReason::ServerDisconnect(
                                ReasonCode::NotAuthorized
                                | ReasonCode::BadUsernameOrPassword
                                | ReasonCode::BadAuthenticationMethod,
                            ),
                    } => (ConnectionStatus::Error, true),
                    ConnectionEvent::Reconnecting { .. } => (ConnectionStatus::Connecting, false),
                    ConnectionEvent::ReconnectFailed { .. } => (ConnectionStatus::Error, false),
                    _ => (ConnectionStatus::Disconnected, false),
                };
                handler(status, auth_failure);
            })
            .await
            .map_err(|e| Error::Mqtt(format!("on_connection_event: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stitch_harness::BrokerHarness;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn publish_while_disconnected_fails_instead_of_queueing_in_mqtt5() {
        let broker = BrokerHarness::new().start().await.expect("start broker");
        let client = NativeMqttClient::new("queue-check");
        client
            .connect(
                &broker.tcp_url(),
                ConnectArgs {
                    clean_start: false,
                    keep_alive_secs: 60,
                    session_expiry_secs: 3600,
                    jwt_ticket: None,
                    username: None,
                    password: None,
                    will: None,
                },
            )
            .await
            .expect("connect");
        broker.shutdown().await;
        for _ in 0..100 {
            if !client.is_connected().await {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            !client.is_connected().await,
            "client must notice the broker is gone"
        );

        let published = client
            .publish(
                "$DB/notes/create".into(),
                b"{}".to_vec(),
                PublishArgs {
                    response_topic: "$DB/clients/queue-check/r1".into(),
                    correlation_data: b"r1".to_vec(),
                    user_properties: Vec::new(),
                },
            )
            .await;
        assert!(
            published.is_err(),
            "a publish while disconnected must fail so only stitch's offline queue replays it"
        );
    }
}
