use crate::error::{Error, Result};
use mqtt5::client::MqttClient;
use mqtt5::types::{PublishOptions, PublishProperties};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::{debug, error, trace};

const DEFAULT_TIMEOUT_MS: u64 = 5000;

pub const PEER_LEASE_SECS: u64 = 15;

pub const MIN_LEASE_SECS: u64 = 2;

pub const CLIENT_ID_PREFIX: &str = "mqp2p-";

const PRESENCE_TOPIC: &str = "$DB/_presence/#";

const MAX_TRACKED_CLIENTS: usize = 4096;

pub fn heartbeat_interval_secs(lease_secs: u64) -> u64 {
    (lease_secs / 3).max(1)
}

pub fn client_id_for(name: &str) -> String {
    format!("{CLIENT_ID_PREFIX}{name}")
}

#[derive(Debug, Deserialize)]
struct PresenceEvent {
    client_id: String,
    event: String,
    ts: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PresenceMark {
    ts: u64,
    online: bool,
}

type PresenceTable = HashMap<String, PresenceMark>;

fn apply_presence(table: &mut PresenceTable, event: PresenceEvent) {
    if !event.client_id.starts_with(CLIENT_ID_PREFIX) {
        return;
    }
    let online = match event.event.as_str() {
        "connect" => true,
        "disconnect" => false,
        _ => return,
    };
    match table.get(&event.client_id) {
        Some(mark) if mark.ts > event.ts => {}
        None if table.len() >= MAX_TRACKED_CLIENTS => {}
        _ => {
            table.insert(
                event.client_id,
                PresenceMark {
                    ts: event.ts,
                    online,
                },
            );
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct LeaseSighting {
    expires_at: u64,
    seen_at: Instant,
}

type LeaseTable = HashMap<String, LeaseSighting>;

fn lease_is_live(table: &mut LeaseTable, peer: &PeerInfo, now: Instant, unix_now: u64) -> bool {
    let (Some(expires_at), Some(lease_secs)) = (peer.expires_at, peer.lease_secs) else {
        return peer_is_live(peer.expires_at, unix_now);
    };
    let seen_at = match table.get(&peer.id) {
        Some(sighting) if sighting.expires_at == expires_at => sighting.seen_at,
        None if table.len() >= MAX_TRACKED_CLIENTS => {
            return peer_is_live(peer.expires_at, unix_now);
        }
        _ => now,
    };
    table.insert(
        peer.id.clone(),
        LeaseSighting {
            expires_at,
            seen_at,
        },
    );
    now.duration_since(seen_at) < Duration::from_secs(lease_secs)
}

fn departed(table: &PresenceTable, name: &str) -> bool {
    table
        .get(&client_id_for(name))
        .is_some_and(|mark| !mark.online)
}

fn unix_now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn peer_is_live(expires_at: Option<u64>, now: u64) -> bool {
    expires_at.map(|e| e > now).unwrap_or(true)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    pub id: String,
    pub name: String,
    pub status: String,
    pub quic_port: u16,
    pub cert_fingerprint: String,
    #[serde(default)]
    pub public_addr: Option<String>,
    #[serde(rename = "_expires_at", default)]
    pub expires_at: Option<u64>,
    #[serde(default)]
    pub lease_secs: Option<u64>,
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
    Relay,
    #[serde(other)]
    Unknown,
}

impl CandidateKind {
    pub fn is_connectable(&self) -> bool {
        matches!(self, Self::Host | Self::Srflx | Self::Relay)
    }

    pub fn is_direct(&self) -> bool {
        matches!(self, Self::Host | Self::Srflx)
    }
}

#[derive(Clone)]
pub struct SignalingClient {
    client: MqttClient,
    presence: Arc<Mutex<PresenceTable>>,
    leases: Arc<Mutex<LeaseTable>>,
}

impl SignalingClient {
    pub fn new(client: MqttClient) -> Self {
        Self {
            client,
            presence: Arc::new(Mutex::new(PresenceTable::new())),
            leases: Arc::new(Mutex::new(LeaseTable::new())),
        }
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

    pub async fn disconnect(&self) -> Result<()> {
        self.client
            .disconnect()
            .await
            .map_err(|e| Error::Signaling(format!("MQTT disconnect failed: {e}")))?;
        debug!("signaling client disconnected");
        Ok(())
    }

    pub async fn track_presence(&self) -> Result<()> {
        let presence = Arc::clone(&self.presence);
        self.client
            .subscribe(PRESENCE_TOPIC, move |msg| {
                if let Ok(event) = serde_json::from_slice::<PresenceEvent>(&msg.payload)
                    && let Ok(mut table) = presence.lock()
                {
                    apply_presence(&mut table, event);
                }
            })
            .await
            .map_err(|e| Error::Signaling(format!("subscribe presence failed: {e}")))?;
        debug!("tracking broker presence");
        Ok(())
    }

    pub async fn register_peer(
        &self,
        id: &str,
        name: &str,
        quic_port: u16,
        cert_fingerprint: &str,
        public_addr: Option<&str>,
        lease_secs: u64,
    ) -> Result<()> {
        let mut payload = serde_json::json!({
            "id": id,
            "name": name,
            "status": "online",
            "quic_port": quic_port,
            "cert_fingerprint": cert_fingerprint,
            "_expires_at": unix_now_secs() + lease_secs,
            "lease_secs": lease_secs,
        });
        if let Some(addr) = public_addr {
            payload["public_addr"] = Value::String(addr.to_string());
        }

        let response = self
            .publish_and_wait("$DB/peers/create", &serde_json::to_vec(&payload)?)
            .await?;

        check_response(&response)?;
        debug!(peer_id = id, name, "peer registered");
        Ok(())
    }

    pub async fn renew_lease(&self, peer_id: &str, lease_secs: u64) -> Result<()> {
        let topic = format!("$DB/peers/{peer_id}/update");
        let payload = serde_json::json!({
            "_expires_at": unix_now_secs() + lease_secs,
            "lease_secs": lease_secs,
        });

        let response = self
            .publish_and_wait(&topic, &serde_json::to_vec(&payload)?)
            .await?;

        check_response(&response)?;
        trace!(peer_id, "peer lease renewed");
        Ok(())
    }

    pub async fn ensure_registered(
        &self,
        id: &str,
        name: &str,
        quic_port: u16,
        cert_fingerprint: &str,
        public_addr: Option<&str>,
        lease_secs: u64,
    ) -> Result<()> {
        match self.renew_lease(id, lease_secs).await {
            Ok(()) => Ok(()),
            Err(e) => {
                debug!(peer_id = id, error = %e, "lease renewal failed, re-creating record");
                self.register_peer(
                    id,
                    name,
                    quic_port,
                    cert_fingerprint,
                    public_addr,
                    lease_secs,
                )
                .await
            }
        }
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

        let unix_now = unix_now_secs();
        let now = Instant::now();
        let records: Vec<PeerInfo> = items
            .into_iter()
            .filter_map(|v| serde_json::from_value::<PeerInfo>(v).ok())
            .collect();
        let presence = self
            .presence
            .lock()
            .map_err(|_| Error::Signaling("presence table poisoned".into()))?;
        let mut leases = self
            .leases
            .lock()
            .map_err(|_| Error::Signaling("lease table poisoned".into()))?;
        let listed: HashSet<&str> = records.iter().map(|p| p.id.as_str()).collect();
        leases.retain(|id, _| listed.contains(id.as_str()));
        let peers: Vec<PeerInfo> = records
            .iter()
            .filter(|p| {
                lease_is_live(&mut leases, p, now, unix_now) && !departed(&presence, &p.name)
            })
            .cloned()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_relay_candidate_round_trips() {
        let relay = Candidate {
            addr: "203.0.113.7:5000".parse().unwrap(),
            kind: CandidateKind::Relay,
        };
        let json = serde_json::to_string(&relay).unwrap();
        assert!(json.contains("Relay"));
        let decoded: Candidate = serde_json::from_str(&json).unwrap();
        assert!(matches!(decoded.kind, CandidateKind::Relay));
    }

    #[test]
    fn relay_is_connectable_but_not_direct() {
        assert!(CandidateKind::Host.is_connectable());
        assert!(CandidateKind::Srflx.is_connectable());
        assert!(CandidateKind::Relay.is_connectable());
        assert!(!CandidateKind::Unknown.is_connectable());

        assert!(CandidateKind::Host.is_direct());
        assert!(CandidateKind::Srflx.is_direct());
        assert!(!CandidateKind::Relay.is_direct());
        assert!(!CandidateKind::Unknown.is_direct());
    }

    fn event(client_id: &str, event: &str, ts: u64) -> PresenceEvent {
        PresenceEvent {
            client_id: client_id.into(),
            event: event.into(),
            ts,
        }
    }

    #[test]
    fn disconnect_marks_a_peer_departed_until_it_reconnects() {
        let mut table = PresenceTable::new();
        apply_presence(&mut table, event("mqp2p-edge", "connect", 1));
        assert!(!departed(&table, "edge"));
        apply_presence(&mut table, event("mqp2p-edge", "disconnect", 2));
        assert!(departed(&table, "edge"));
        apply_presence(&mut table, event("mqp2p-edge", "connect", 3));
        assert!(!departed(&table, "edge"));
    }

    #[test]
    fn an_older_presence_event_does_not_override_a_newer_one() {
        let mut table = PresenceTable::new();
        apply_presence(&mut table, event("mqp2p-edge", "connect", 5));
        apply_presence(&mut table, event("mqp2p-edge", "disconnect", 4));
        assert!(!departed(&table, "edge"));
    }

    #[test]
    fn unknown_peers_and_foreign_clients_are_not_departed() {
        let mut table = PresenceTable::new();
        apply_presence(&mut table, event("stitch-app", "disconnect", 1));
        apply_presence(&mut table, event("mqp2p-edge", "expired", 1));
        assert!(table.is_empty());
        assert!(!departed(&table, "edge"));
    }

    #[test]
    fn an_unknown_candidate_kind_parses_instead_of_failing() {
        let candidate: Candidate = serde_json::from_value(serde_json::json!({
            "addr": "127.0.0.1:9000",
            "kind": "Prflx"
        }))
        .expect("an unknown candidate kind must still deserialize");
        assert!(matches!(candidate.kind, CandidateKind::Unknown));
    }

    #[test]
    fn an_offer_with_an_unknown_candidate_kind_still_deserializes() {
        let offer: ConnectionOffer = serde_json::from_value(serde_json::json!({
            "from": "peer-a",
            "session_id": "s1",
            "cert_fingerprint": "sha256:aa",
            "candidates": [
                { "addr": "127.0.0.1:1", "kind": "Host" },
                { "addr": "127.0.0.1:2", "kind": "FutureKind" }
            ]
        }))
        .expect("an offer carrying an unknown candidate kind must still parse");
        assert_eq!(offer.candidates.len(), 2);
        assert!(matches!(offer.candidates[0].kind, CandidateKind::Host));
        assert!(matches!(offer.candidates[1].kind, CandidateKind::Unknown));
    }

    #[test]
    fn presence_table_stops_growing_at_its_limit() {
        let mut table = PresenceTable::new();
        for i in 0..MAX_TRACKED_CLIENTS {
            apply_presence(&mut table, event(&format!("mqp2p-p{i}"), "connect", 1));
        }
        apply_presence(&mut table, event("mqp2p-late", "disconnect", 2));
        assert_eq!(table.len(), MAX_TRACKED_CLIENTS);
        assert!(!departed(&table, "late"));
        apply_presence(&mut table, event("mqp2p-p0", "disconnect", 2));
        assert!(departed(&table, "p0"));
    }

    fn leased(id: &str, expires_at: u64, lease_secs: Option<u64>) -> PeerInfo {
        PeerInfo {
            id: id.into(),
            name: id.into(),
            status: "online".into(),
            quic_port: 4433,
            cert_fingerprint: "fp".into(),
            public_addr: None,
            expires_at: Some(expires_at),
            lease_secs,
        }
    }

    #[test]
    fn lease_runs_from_when_the_observer_saw_the_renewal() {
        let mut table = LeaseTable::new();
        let start = Instant::now();
        let peer = leased("p1", 100, Some(15));
        assert!(lease_is_live(&mut table, &peer, start, 1_000_000));
        assert!(lease_is_live(
            &mut table,
            &peer,
            start + Duration::from_secs(14),
            0
        ));
        assert!(!lease_is_live(
            &mut table,
            &peer,
            start + Duration::from_secs(15),
            0
        ));
        let renewed = leased("p1", 105, Some(15));
        assert!(lease_is_live(
            &mut table,
            &renewed,
            start + Duration::from_secs(20),
            0
        ));
        assert!(lease_is_live(
            &mut table,
            &renewed,
            start + Duration::from_secs(34),
            0
        ));
    }

    #[test]
    fn a_record_without_lease_secs_falls_back_to_its_timestamp() {
        let mut table = LeaseTable::new();
        let now = Instant::now();
        assert!(lease_is_live(
            &mut table,
            &leased("old", 1001, None),
            now,
            1000
        ));
        assert!(!lease_is_live(
            &mut table,
            &leased("old", 999, None),
            now,
            1000
        ));
        assert!(table.is_empty());
    }

    #[test]
    fn lease_table_stops_growing_at_its_limit() {
        let mut table = LeaseTable::new();
        let now = Instant::now();
        for i in 0..MAX_TRACKED_CLIENTS {
            lease_is_live(&mut table, &leased(&format!("p{i}"), 1, Some(15)), now, 0);
        }
        assert!(!lease_is_live(
            &mut table,
            &leased("late", 999, Some(15)),
            now,
            1000
        ));
        assert!(lease_is_live(
            &mut table,
            &leased("late", 1001, Some(15)),
            now,
            1000
        ));
        assert_eq!(table.len(), MAX_TRACKED_CLIENTS);
    }

    #[test]
    fn leaseless_peer_is_live() {
        assert!(peer_is_live(None, 1000));
    }

    #[test]
    fn future_lease_is_live() {
        assert!(peer_is_live(Some(1001), 1000));
    }

    #[test]
    fn lease_at_current_second_is_stale() {
        assert!(!peer_is_live(Some(1000), 1000));
    }

    #[test]
    fn past_lease_is_stale() {
        assert!(!peer_is_live(Some(999), 1000));
    }

    #[test]
    fn heartbeat_interval_is_a_third_of_lease_min_one() {
        assert_eq!(heartbeat_interval_secs(15), 5);
        assert_eq!(heartbeat_interval_secs(3), 1);
        assert_eq!(heartbeat_interval_secs(2), 1);
        assert_eq!(heartbeat_interval_secs(1), 1);
    }

    #[test]
    fn expires_at_round_trips_as_mqdb_field() {
        let info = PeerInfo {
            id: "p1".into(),
            name: "edge".into(),
            status: "online".into(),
            quic_port: 4433,
            cert_fingerprint: "ab".into(),
            public_addr: None,
            expires_at: Some(1234),
            lease_secs: None,
        };
        let json = serde_json::to_value(&info).expect("serialize");
        assert_eq!(json.get("_expires_at").and_then(Value::as_u64), Some(1234));
        let back: PeerInfo = serde_json::from_value(json).expect("deserialize");
        assert_eq!(back.expires_at, Some(1234));
    }

    #[test]
    fn missing_expires_at_deserializes_to_none() {
        let json = serde_json::json!({
            "id": "p1",
            "name": "edge",
            "status": "online",
            "quic_port": 4433,
            "cert_fingerprint": "ab",
        });
        let info: PeerInfo = serde_json::from_value(json).expect("deserialize");
        assert_eq!(info.expires_at, None);
    }
}
