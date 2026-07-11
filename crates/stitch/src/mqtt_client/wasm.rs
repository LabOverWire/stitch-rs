use super::{
    ConnectArgs, ConnectOutcome, ConnectionHandler, IncomingMessage, MessageHandler, MqttClientApi,
    PublishArgs,
};
use crate::error::{Error, Result};
use crate::types::ConnectionStatus;
use mqtt5_protocol::types::QoS;
use mqtt5_wasm::WasmMqttClient;
use mqtt5_wasm::client::RustMessage;
use mqtt5_wasm::config::{WasmConnectOptions, WasmPublishOptions, WasmWillMessage};
use std::cell::RefCell;
use std::rc::Rc;
use wasm_bindgen::prelude::*;

pub(crate) struct WasmMqttClientAdapter {
    client: WasmMqttClient,
    callbacks: RefCell<Vec<Closure<dyn FnMut()>>>,
}

impl WasmMqttClientAdapter {
    pub(crate) fn new(client_id: &str) -> Self {
        Self {
            client: WasmMqttClient::new(client_id.to_string()),
            callbacks: RefCell::new(Vec::new()),
        }
    }
}

fn js_err(ctx: &str, e: &JsValue) -> Error {
    Error::Mqtt(format!("{ctx}: {e:?}"))
}

#[async_trait::async_trait(?Send)]
impl MqttClientApi for WasmMqttClientAdapter {
    async fn connect(&self, url: &str, args: ConnectArgs) -> Result<ConnectOutcome> {
        let mut opts = WasmConnectOptions::new();
        opts.set_cleanStart(args.clean_start);
        opts.set_keepAlive(u16::try_from(args.keep_alive_secs).unwrap_or(u16::MAX));
        opts.set_sessionExpiryInterval(Some(args.session_expiry_secs));
        if let Some(will) = &args.will {
            let mut message = WasmWillMessage::new(will.topic.clone(), will.payload.clone());
            message.set_qos(will.qos);
            message.set_retain(will.retain);
            message.set_willDelayInterval(will.will_delay_interval_secs);
            message.set_contentType(will.content_type.clone());
            opts.set_will(message);
        }
        if let Some(ticket) = &args.jwt_ticket {
            opts.set_authenticationMethod(Some("JWT".into()));
            opts.set_authenticationData(ticket.as_bytes());
            let on_challenge = Closure::<dyn FnMut()>::new(|| {});
            self.client.on_auth_challenge(
                on_challenge
                    .as_ref()
                    .unchecked_ref::<js_sys::Function>()
                    .clone(),
            );
            self.callbacks.borrow_mut().push(on_challenge);
        } else if let (Some(user), Some(pass)) = (&args.username, &args.password) {
            opts.set_username(Some(user.clone()));
            opts.set_password(pass.as_bytes());
        }
        self.client
            .connect_with_options(url, &opts)
            .await
            .map_err(|e| js_err("connect", &e))?;
        Ok(ConnectOutcome {
            session_present: false,
        })
    }

    async fn disconnect(&self) -> Result<()> {
        self.client
            .disconnect()
            .await
            .map_err(|e| js_err("disconnect", &e))
    }

    async fn disconnect_abnormally(&self) -> Result<()> {
        Err(Error::Mqtt(
            "disconnect_abnormally is only supported on native targets".into(),
        ))
    }

    async fn is_connected(&self) -> bool {
        self.client.is_connected()
    }

    async fn publish(&self, topic: String, payload: Vec<u8>, args: PublishArgs) -> Result<()> {
        let mut opts = WasmPublishOptions::new();
        opts.set_qos(QoS::AtLeastOnce as u8);
        opts.set_responseTopic(Some(args.response_topic));
        opts.set_correlationData(&args.correlation_data);
        for (key, value) in args.user_properties {
            opts.addUserProperty(key, value);
        }
        self.client
            .publish_with_options(&topic, &payload, &opts)
            .await
            .map_err(|e| js_err("publish", &e))
    }

    async fn subscribe(&self, topic: String, on_message: MessageHandler) -> Result<()> {
        let callback: Box<dyn Fn(RustMessage)> = Box::new(move |msg: RustMessage| {
            on_message(IncomingMessage {
                topic: msg.topic,
                payload: msg.payload,
                user_properties: msg.properties.user_properties,
            });
        });
        self.client
            .subscribe_with_callback_internal_opts(&topic, QoS::AtLeastOnce, false, callback)
            .await
            .map(|_| ())
            .map_err(|e| js_err("subscribe", &e))
    }

    async fn unsubscribe(&self, topic: String) -> Result<()> {
        self.client
            .unsubscribe(&topic)
            .await
            .map(|_| ())
            .map_err(|e| js_err("unsubscribe", &e))
    }

    async fn on_connection_event(&self, handler: ConnectionHandler) -> Result<()> {
        let handler = Rc::new(handler);
        let on_connect_handler = Rc::clone(&handler);
        let on_connect = Closure::<dyn FnMut()>::new(move || {
            (*on_connect_handler)(ConnectionStatus::Connected, false);
        });
        let on_disconnect_handler = Rc::clone(&handler);
        let on_disconnect = Closure::<dyn FnMut()>::new(move || {
            (*on_disconnect_handler)(ConnectionStatus::Disconnected, false);
        });
        self.client.on_connect(
            on_connect
                .as_ref()
                .unchecked_ref::<js_sys::Function>()
                .clone(),
        );
        self.client.on_disconnect(
            on_disconnect
                .as_ref()
                .unchecked_ref::<js_sys::Function>()
                .clone(),
        );
        self.callbacks.borrow_mut().push(on_connect);
        self.callbacks.borrow_mut().push(on_disconnect);
        Ok(())
    }
}
