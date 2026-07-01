use crate::error::{Error, Result};
use mqtt5::client::MqttClient;
use mqtt5::types::{PublishOptions, PublishProperties};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::net::SocketAddr;
use std::time::Duration;
use tracing::{debug, error, trace};

const DEFAULT_TIMEOUT_MS: u64 = 5000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    pub id: String,
    pub name: String,
    pub status: String,
    pub quic_port: u16,
    pub cert_fingerprint: String,
    #[serde(default)]
    pub public_addr: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionOffer {
    pub from: String,
    pub session_id: String,
    pub cert_fingerprint: String,
    pub candidates: Vec<Candidate>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionSync {
    pub from: String,
    pub session_id: String,
    pub rtt_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Candidate {
    pub addr: SocketAddr,
    #[serde(default)]
    pub kind: CandidateKind,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub enum CandidateKind {
    Host,
    #[default]
    Srflx,
}

pub struct SignalingClient {
    client: MqttClient,
}

impl SignalingClient {
    pub fn new(client: MqttClient) -> Self {
        Self { client }
    }

    pub fn mqtt_client(&self) -> &MqttClient {
        &self.client
    }

    pub async fn connect(&self, broker_addr: &str) -> Result<()> {
        self.client
            .connect(broker_addr)
            .await
            .map_err(|e| Error::Signaling(format!("MQTT connect failed: {e}")))?;
        debug!(broker = broker_addr, "signaling client connected");
        Ok(())
    }

    pub async fn register_peer(
        &self,
        name: &str,
        quic_port: u16,
        cert_fingerprint: &str,
    ) -> Result<String> {
        let payload = serde_json::json!({
            "name": name,
            "status": "online",
            "quic_port": quic_port,
            "cert_fingerprint": cert_fingerprint,
        });

        let response = self
            .publish_and_wait("$DB/peers/create", &serde_json::to_vec(&payload)?)
            .await?;

        check_response(&response)?;

        let id = response
            .pointer("/data/id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::Signaling("no peer ID in create response".into()))?
            .to_string();

        debug!(peer_id = id, name, "peer registered");
        Ok(id)
    }

    pub async fn update_peer_addr(&self, peer_id: &str, public_addr: SocketAddr) -> Result<()> {
        let topic = format!("$DB/peers/{peer_id}/update");
        let payload = serde_json::json!({
            "public_addr": public_addr.to_string(),
        });

        let response = self
            .publish_and_wait(&topic, &serde_json::to_vec(&payload)?)
            .await?;

        check_response(&response)?;
        debug!(peer_id, addr = %public_addr, "peer address updated");
        Ok(())
    }

    pub async fn list_peers(&self) -> Result<Vec<PeerInfo>> {
        let response = self.publish_and_wait("$DB/peers/list", b"{}").await?;
        check_response(&response)?;

        let data = response.get("data").cloned().unwrap_or(Value::Null);
        let items = match data {
            Value::Array(items) => items,
            Value::Object(ref obj) => obj
                .get("items")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_else(|| vec![data.clone()]),
            Value::Null => vec![],
            other => vec![other],
        };

        let peers: Vec<PeerInfo> = items
            .into_iter()
            .filter_map(|v| serde_json::from_value(v).ok())
            .collect();

        debug!(count = peers.len(), "peers discovered");
        Ok(peers)
    }

    pub async fn deregister_peer(&self, peer_id: &str) -> Result<()> {
        let topic = format!("$DB/peers/{peer_id}/delete");
        let response = self.publish_and_wait(&topic, b"").await?;
        check_response(&response)?;
        debug!(peer_id, "peer deregistered");
        Ok(())
    }

    pub async fn send_offer(&self, target_peer_id: &str, offer: &ConnectionOffer) -> Result<()> {
        let topic = format!("p2p/{target_peer_id}/offer");
        let payload = serde_json::to_vec(offer)?;
        self.client
            .publish(&topic, payload)
            .await
            .map_err(|e| Error::Signaling(format!("send offer failed: {e}")))?;
        debug!(target = target_peer_id, "connection offer sent");
        Ok(())
    }

    pub async fn send_answer(
        &self,
        initiator_peer_id: &str,
        answer: &ConnectionOffer,
    ) -> Result<()> {
        let topic = format!("p2p/{initiator_peer_id}/answer");
        let payload = serde_json::to_vec(answer)?;
        self.client
            .publish(&topic, payload)
            .await
            .map_err(|e| Error::Signaling(format!("send answer failed: {e}")))?;
        debug!(initiator = initiator_peer_id, "connection answer sent");
        Ok(())
    }

    pub async fn subscribe_offers(
        &self,
        my_peer_id: &str,
    ) -> Result<flume::Receiver<ConnectionOffer>> {
        let topic = format!("p2p/{my_peer_id}/offer");
        let (tx, rx) = flume::bounded::<ConnectionOffer>(16);

        self.client
            .subscribe(&topic, move |msg| {
                if let Ok(offer) = serde_json::from_slice::<ConnectionOffer>(&msg.payload) {
                    let _ = tx.try_send(offer);
                }
            })
            .await
            .map_err(|e| Error::Signaling(format!("subscribe offers failed: {e}")))?;

        debug!(peer_id = my_peer_id, "subscribed to offers");
        Ok(rx)
    }

    pub async fn subscribe_answers(
        &self,
        my_peer_id: &str,
    ) -> Result<flume::Receiver<ConnectionOffer>> {
        let topic = format!("p2p/{my_peer_id}/answer");
        let (tx, rx) = flume::bounded::<ConnectionOffer>(16);

        self.client
            .subscribe(&topic, move |msg| {
                if let Ok(answer) = serde_json::from_slice::<ConnectionOffer>(&msg.payload) {
                    let _ = tx.try_send(answer);
                }
            })
            .await
            .map_err(|e| Error::Signaling(format!("subscribe answers failed: {e}")))?;

        debug!(peer_id = my_peer_id, "subscribed to answers");
        Ok(rx)
    }

    pub async fn send_sync(&self, target_peer_id: &str, sync: &ConnectionSync) -> Result<()> {
        let topic = format!("p2p/{target_peer_id}/sync");
        let payload = serde_json::to_vec(sync)?;
        self.client
            .publish(&topic, payload)
            .await
            .map_err(|e| Error::Signaling(format!("send sync failed: {e}")))?;
        debug!(target = target_peer_id, rtt_ms = sync.rtt_ms, "sync sent");
        Ok(())
    }

    pub async fn subscribe_sync(
        &self,
        my_peer_id: &str,
    ) -> Result<flume::Receiver<ConnectionSync>> {
        let topic = format!("p2p/{my_peer_id}/sync");
        let (tx, rx) = flume::bounded::<ConnectionSync>(16);

        self.client
            .subscribe(&topic, move |msg| {
                if let Ok(sync) = serde_json::from_slice::<ConnectionSync>(&msg.payload) {
                    let _ = tx.try_send(sync);
                }
            })
            .await
            .map_err(|e| Error::Signaling(format!("subscribe sync failed: {e}")))?;

        debug!(peer_id = my_peer_id, "subscribed to sync");
        Ok(rx)
    }

    async fn publish_and_wait(&self, topic: &str, payload: &[u8]) -> Result<Value> {
        let response_topic = format!("mqp2p/responses/{}", uuid::Uuid::new_v4());
        let (tx, rx) = flume::bounded::<Vec<u8>>(1);

        self.client
            .subscribe(&response_topic, move |msg| {
                let _ = tx.try_send(msg.payload.clone());
            })
            .await
            .map_err(|e| Error::Signaling(format!("subscribe failed: {e}")))?;

        tokio::time::sleep(Duration::from_millis(50)).await;

        let opts = PublishOptions {
            properties: PublishProperties {
                response_topic: Some(response_topic.clone()),
                ..Default::default()
            },
            ..Default::default()
        };

        trace!(topic, "publishing signaling request");

        self.client
            .publish_with_options(topic, payload.to_vec(), opts)
            .await
            .map_err(|e| Error::Signaling(format!("publish failed: {e}")))?;

        let result =
            match tokio::time::timeout(Duration::from_millis(DEFAULT_TIMEOUT_MS), rx.recv_async())
                .await
            {
                Ok(Ok(data)) => serde_json::from_slice(&data).map_err(Error::Json),
                Ok(Err(e)) => Err(Error::Signaling(format!("channel recv error: {e}"))),
                Err(_) => Err(Error::Signaling(format!(
                    "timeout waiting for response on {topic}"
                ))),
            };

        let _ = self.client.unsubscribe(&response_topic).await;
        result
    }
}

fn check_response(response: &Value) -> Result<()> {
    let status = response
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    if status == "ok" {
        Ok(())
    } else {
        let msg = response
            .get("message")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown error");
        error!(status, msg, "signaling DB operation failed");
        Err(Error::Signaling(format!("DB error: {msg}")))
    }
}
