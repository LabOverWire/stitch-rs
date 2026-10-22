use crate::error::{Error, Result};
use crate::quic::{self, QuicEndpoint, SegmentationOffload};
use crate::signaling::{
    Candidate, CandidateKind, ConnectionOffer, ConnectionSync, MIN_LEASE_SECS, PEER_LEASE_SECS,
    PeerInfo, SignalingClient, client_id_for, heartbeat_interval_secs,
};
use crate::stun;
use crate::transfer::{self, FileOffer, TransferProgress, TransferResult};
use bytes::Bytes;
use mqtt5::ConnectOptions;
use mqtt5::client::MqttClient;
use quinn::Connection;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};
use zeroize::Zeroizing;

pub struct PeerConfig {
    pub name: String,
    pub broker_addr: String,
    pub bind_addr: SocketAddr,
    pub stun_server: Option<String>,
    pub credentials: Option<(String, String)>,
    pub lease_secs: u64,
    pub keepalive_secs: Option<u64>,
    pub presence: bool,
    pub segmentation_offload: Option<SegmentationOffload>,
    pub transport_config: Option<quinn::TransportConfig>,
    pub ed25519_seed: Option<[u8; 32]>,
}

impl PeerConfig {
    pub fn new(name: impl Into<String>, broker_addr: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            broker_addr: broker_addr.into(),
            bind_addr: "0.0.0.0:0".parse().unwrap_or_else(|_| unreachable!()),
            stun_server: Some(stun::DEFAULT_STUN_SERVER.into()),
            credentials: None,
            lease_secs: PEER_LEASE_SECS,
            keepalive_secs: None,
            presence: false,
            segmentation_offload: None,
            transport_config: None,
            ed25519_seed: None,
        }
    }

    pub fn with_lease_secs(mut self, lease_secs: u64) -> Self {
        self.lease_secs = lease_secs;
        self
    }

    pub fn with_keepalive_secs(mut self, keepalive_secs: u64) -> Self {
        self.keepalive_secs = Some(keepalive_secs);
        self
    }

    pub fn with_presence(mut self) -> Self {
        self.presence = true;
        self
    }

    fn effective_lease_secs(&self) -> u64 {
        self.lease_secs.max(MIN_LEASE_SECS)
    }

    fn effective_keepalive_secs(&self) -> u64 {
        self.keepalive_secs
            .unwrap_or_else(|| heartbeat_interval_secs(self.effective_lease_secs()))
    }

    pub fn with_bind_addr(mut self, addr: SocketAddr) -> Self {
        self.bind_addr = addr;
        self
    }

    pub fn with_stun_server(mut self, server: impl Into<String>) -> Self {
        self.stun_server = Some(server.into());
        self
    }

    pub fn without_stun(mut self) -> Self {
        self.stun_server = None;
        self
    }

    pub fn with_segmentation_offload(mut self) -> Self {
        self.segmentation_offload = Some(SegmentationOffload::Enabled);
        self
    }

    pub fn without_segmentation_offload(mut self) -> Self {
        self.segmentation_offload = Some(SegmentationOffload::Disabled);
        self
    }

    /// Apply a custom QUIC transport config (keep-alive, idle timeout,
    /// concurrent-stream limits, stream and datagram windows, congestion
    /// controller) to both the client and server endpoints. The config's own
    /// segmentation-offload setting is left untouched unless
    /// [`PeerConfig::with_segmentation_offload`] or
    /// [`PeerConfig::without_segmentation_offload`] is also called, in which
    /// case that choice overrides it.
    pub fn with_transport_config(mut self, transport: quinn::TransportConfig) -> Self {
        self.transport_config = Some(transport);
        self
    }

    pub fn with_credentials(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.credentials = Some((username.into(), password.into()));
        self
    }

    /// Derive the peer's self-signed certificate from a persisted 32-byte
    /// Ed25519 seed, so its certificate, fingerprint, and verified public key
    /// stay the same across restarts. Without a seed an ephemeral identity is
    /// generated on each start. The same seed is the one behind a
    /// `stitch_p2p::membership::Identity`, so a device presents one key for
    /// both the transport and its signed writes.
    pub fn with_ed25519_identity(mut self, seed: [u8; 32]) -> Self {
        self.ed25519_seed = Some(seed);
        self
    }
}

fn offload_override(offload: Option<SegmentationOffload>) -> Option<bool> {
    offload.map(|o| o == SegmentationOffload::Enabled)
}

async fn establish_signaling(
    signaling: &SignalingClient,
    broker_addr: &str,
    presence: bool,
) -> Result<()> {
    signaling.connect(broker_addr).await?;
    if presence {
        signaling.track_presence().await?;
    }
    Ok(())
}

pub type PeerId = String;

pub struct Peer {
    name: String,
    fingerprint: String,
    signaling: SignalingClient,
    quic: QuicEndpoint,
    record_id: String,
    peer_id: Option<String>,
    std_socket: std::net::UdpSocket,
    host_addr: SocketAddr,
    srflx_addr: Option<SocketAddr>,
    lease_secs: u64,
    heartbeat: Option<tokio::task::JoinHandle<()>>,
}

impl Peer {
    pub async fn new(mut config: PeerConfig) -> Result<Self> {
        let identity = match config.ed25519_seed.take() {
            Some(seed) => quic::cert_from_ed25519_seed(&Zeroizing::new(seed))?,
            None => quic::generate_self_signed_cert()?,
        };

        let std_socket = std::net::UdpSocket::bind(config.bind_addr)?;
        std_socket.set_nonblocking(true)?;
        let local_addr = std_socket.local_addr()?;
        info!(addr = %local_addr, "UDP socket bound");

        let host_addr = if local_addr.ip().is_unspecified() {
            let probe = std::net::UdpSocket::bind("0.0.0.0:0")?;
            probe.connect(config.broker_addr.as_str())?;
            let real_ip = probe.local_addr()?.ip();
            SocketAddr::new(real_ip, local_addr.port())
        } else {
            local_addr
        };

        let srflx_addr = if let Some(ref server) = config.stun_server {
            let tokio_socket = tokio::net::UdpSocket::from_std(std_socket.try_clone()?)?;
            let result = match stun::discover_external_addr(&tokio_socket, server).await {
                Ok(addr) => {
                    info!(srflx = %addr, "STUN discovery succeeded");
                    Some(addr)
                }
                Err(e) => {
                    warn!(error = %e, "STUN discovery failed, peer will only be reachable on LAN");
                    None
                }
            };
            drop(tokio_socket);
            result
        } else {
            debug!("STUN disabled, peer will only be reachable on LAN");
            None
        };

        let quic_socket = std_socket.try_clone()?;
        let fingerprint = identity.fingerprint.clone();
        let offload = config.segmentation_offload;
        let mut transport = config.transport_config.take().unwrap_or_default();
        if let Some(enabled) = offload_override(offload) {
            transport.enable_segmentation_offload(enabled);
        }
        debug!(?offload, "QUIC transport configured");
        let quic = QuicEndpoint::bind_with_transport(quic_socket, identity, Arc::new(transport))?;

        let lease_secs = config.effective_lease_secs();
        let keepalive_secs = config.effective_keepalive_secs();
        let options = ConnectOptions::new(client_id_for(&config.name))
            .with_keep_alive(Duration::from_secs(keepalive_secs));
        let options = match config.credentials {
            Some((ref user, ref pass)) => options.with_credentials(user, pass.as_bytes()),
            None => options,
        };
        let signaling = SignalingClient::new(MqttClient::with_options(options));
        if let Err(e) = establish_signaling(&signaling, &config.broker_addr, config.presence).await
        {
            if let Err(disc) = signaling.disconnect().await {
                debug!(error = %disc, "signaling disconnect failed after a failed connect");
            }
            return Err(e);
        }
        debug!(
            keepalive_secs,
            presence = config.presence,
            "signaling configured"
        );

        Ok(Self {
            name: config.name,
            fingerprint,
            signaling,
            quic,
            record_id: uuid::Uuid::new_v4().to_string(),
            peer_id: None,
            std_socket,
            host_addr,
            srflx_addr,
            lease_secs,
            heartbeat: None,
        })
    }

    pub async fn register(&mut self) -> Result<PeerId> {
        let quic_port = self.quic.local_addr()?.port();
        let peer_id = self.record_id.clone();
        let public_addr = self.srflx_addr.map(|a| a.to_string());

        self.signaling
            .register_peer(
                &peer_id,
                &self.name,
                quic_port,
                &self.fingerprint,
                public_addr.as_deref(),
                self.lease_secs,
            )
            .await?;

        self.peer_id = Some(peer_id.clone());

        if let Some(handle) = self.heartbeat.take() {
            handle.abort();
        }

        let signaling = self.signaling.clone();
        let renew_id = peer_id.clone();
        let renew_name = self.name.clone();
        let renew_fingerprint = self.fingerprint.clone();
        let renew_public_addr = public_addr;
        let lease_secs = self.lease_secs;
        self.heartbeat = Some(tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(
                heartbeat_interval_secs(lease_secs),
            ));
            ticker.tick().await;
            loop {
                ticker.tick().await;
                if let Err(e) = signaling
                    .ensure_registered(
                        &renew_id,
                        &renew_name,
                        quic_port,
                        &renew_fingerprint,
                        renew_public_addr.as_deref(),
                        lease_secs,
                    )
                    .await
                {
                    warn!(peer_id = renew_id, error = %e, "peer presence renewal failed");
                }
            }
        }));

        info!(peer_id, name = self.name, "peer registered");
        Ok(peer_id)
    }

    pub async fn discover_peers(&self) -> Result<Vec<PeerInfo>> {
        self.signaling.list_peers().await
    }

    pub fn peer_id(&self) -> Option<&str> {
        self.peer_id.as_deref()
    }

    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    pub async fn connect_to(&self, target: &PeerInfo) -> Result<P2pConnection> {
        let my_peer_id = self
            .peer_id
            .as_deref()
            .ok_or(Error::Signaling("not registered".into()))?;

        let session_id = uuid::Uuid::new_v4().to_string();

        let answer_rx = self.signaling.subscribe_answers(my_peer_id).await?;

        let mut candidates = vec![Candidate {
            addr: self.host_addr,
            kind: CandidateKind::Host,
        }];
        if let Some(srflx) = self.srflx_addr {
            candidates.push(Candidate {
                addr: srflx,
                kind: CandidateKind::Srflx,
            });
        }

        let offer = ConnectionOffer {
            from: my_peer_id.to_string(),
            session_id: session_id.clone(),
            cert_fingerprint: self.fingerprint.clone(),
            candidates,
        };

        let offer_sent = tokio::time::Instant::now();
        self.signaling.send_offer(&target.id, &offer).await?;

        let answer =
            tokio::time::timeout(std::time::Duration::from_secs(10), answer_rx.recv_async())
                .await
                .map_err(|_| Error::Signaling("timeout waiting for connection answer".into()))?
                .map_err(|e| Error::Signaling(format!("answer channel closed: {e}")))?;

        let signaling_rtt = offer_sent.elapsed();

        if answer.session_id != session_id {
            return Err(Error::Signaling(format!(
                "session ID mismatch: expected {session_id}, got {}",
                answer.session_id
            )));
        }

        info!(
            session = answer.session_id,
            from = answer.from,
            rtt_ms = signaling_rtt.as_millis() as u64,
            "received answer, measured signaling RTT"
        );

        let ordered_candidates: Vec<SocketAddr> = {
            let srflx: Vec<_> = answer
                .candidates
                .iter()
                .filter(|c| matches!(c.kind, CandidateKind::Srflx))
                .map(|c| c.addr)
                .collect();
            let host: Vec<_> = answer
                .candidates
                .iter()
                .filter(|c| matches!(c.kind, CandidateKind::Host))
                .map(|c| c.addr)
                .collect();
            let mut all = srflx;
            all.extend(host);
            all
        };

        if ordered_candidates.is_empty() {
            return Err(Error::Signaling("no candidates in answer".into()));
        }

        let sync = ConnectionSync {
            from: my_peer_id.to_string(),
            session_id: session_id.clone(),
            rtt_ms: signaling_rtt.as_millis() as u64,
        };
        self.signaling.send_sync(&answer.from, &sync).await?;

        let mut connection = None;
        for (i, remote_addr) in ordered_candidates.iter().enumerate() {
            info!(remote = %remote_addr, attempt = i + 1, "trying candidate");

            {
                let tokio_socket = tokio::net::UdpSocket::from_std(self.std_socket.try_clone()?)?;
                let _ = tokio_socket
                    .send_to(&[0x50, 0x32, 0x50, 0x50], *remote_addr)
                    .await;
            }

            match tokio::time::timeout(
                std::time::Duration::from_secs(5),
                self.quic.connect(*remote_addr, &answer.cert_fingerprint),
            )
            .await
            {
                Ok(Ok(conn)) => {
                    info!(
                        remote = %remote_addr,
                        peer = target.name,
                        "P2P connection established (initiator)"
                    );
                    connection = Some(conn);
                    break;
                }
                Ok(Err(e)) => {
                    warn!(remote = %remote_addr, error = %e, "candidate failed");
                }
                Err(_) => {
                    warn!(remote = %remote_addr, "candidate timed out");
                }
            }
        }

        let connection =
            connection.ok_or_else(|| Error::Quic("all candidates exhausted".into()))?;

        let remote_public_key = quic::peer_public_key(&connection);

        Ok(P2pConnection {
            connection,
            remote_peer: target.clone(),
            remote_public_key,
        })
    }

    pub async fn accept_connection(&self) -> Result<P2pConnection> {
        let my_peer_id = self
            .peer_id
            .as_deref()
            .ok_or(Error::Signaling("not registered".into()))?;

        let offer_rx = self.signaling.subscribe_offers(my_peer_id).await?;
        let sync_rx = self.signaling.subscribe_sync(my_peer_id).await?;

        let offer = offer_rx
            .recv_async()
            .await
            .map_err(|e| Error::Signaling(format!("offer channel closed: {e}")))?;

        debug!(
            session = offer.session_id,
            from = offer.from,
            "received connection offer"
        );

        let mut candidates = vec![Candidate {
            addr: self.host_addr,
            kind: CandidateKind::Host,
        }];
        if let Some(srflx) = self.srflx_addr {
            candidates.push(Candidate {
                addr: srflx,
                kind: CandidateKind::Srflx,
            });
        }

        let answer = ConnectionOffer {
            from: my_peer_id.to_string(),
            session_id: offer.session_id.clone(),
            cert_fingerprint: self.fingerprint.clone(),
            candidates,
        };

        self.signaling.send_answer(&offer.from, &answer).await?;

        let skipped = offer
            .candidates
            .iter()
            .filter(|c| !c.kind.is_connectable())
            .count();
        if skipped > 0 {
            debug!(skipped, "ignoring unrecognized candidate kinds in offer");
        }

        if !offer.candidates.iter().any(|c| c.kind.is_connectable()) {
            return Err(Error::Signaling(
                "no connectable candidates in offer".into(),
            ));
        }

        let sync = tokio::time::timeout(std::time::Duration::from_secs(10), sync_rx.recv_async())
            .await
            .map_err(|_| Error::Signaling("timeout waiting for sync".into()))?
            .map_err(|e| Error::Signaling(format!("sync channel closed: {e}")))?;

        let wait_ms = sync.rtt_ms / 2;
        info!(
            rtt_ms = sync.rtt_ms,
            wait_ms, "received sync, waiting RTT/2 before probing"
        );

        tokio::time::sleep(std::time::Duration::from_millis(wait_ms)).await;

        for candidate in offer.candidates.iter().filter(|c| c.kind.is_connectable()) {
            let tokio_socket = tokio::net::UdpSocket::from_std(self.std_socket.try_clone()?)?;
            let _ = tokio_socket
                .send_to(&[0x50, 0x32, 0x50, 0x50], candidate.addr)
                .await;
        }

        let connection = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            self.quic.accept_with_fingerprint(&offer.cert_fingerprint),
        )
        .await
        .map_err(|_| Error::Signaling("timeout waiting for inbound connection".into()))??;

        let remote_addr = connection.remote_address();

        let remote_peer = PeerInfo {
            id: offer.from,
            name: String::new(),
            status: "connected".into(),
            quic_port: remote_addr.port(),
            cert_fingerprint: offer.cert_fingerprint,
            public_addr: Some(remote_addr.to_string()),
            expires_at: None,
            lease_secs: None,
        };

        let remote_public_key = quic::peer_public_key(&connection);

        info!(
            remote = %remote_addr,
            peer_id = remote_peer.id,
            "P2P connection established (responder)"
        );

        Ok(P2pConnection {
            connection,
            remote_peer,
            remote_public_key,
        })
    }

    pub async fn disconnect(&self) {
        if let Some(handle) = &self.heartbeat {
            handle.abort();
        }
        if let Some(peer_id) = &self.peer_id
            && let Err(e) = self.signaling.deregister_peer(peer_id).await
        {
            warn!(peer_id, error = %e, "peer deregistration failed at disconnect");
        }
        if let Err(e) = self.signaling.disconnect().await {
            warn!(error = %e, "signaling disconnect failed at disconnect");
        }
        self.quic.close();
        info!("peer disconnected");
    }

    pub async fn shutdown(self) -> Result<()> {
        self.disconnect().await;
        Ok(())
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        if let Some(handle) = self.heartbeat.take() {
            handle.abort();
        }
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let signaling = self.signaling.clone();
            runtime.spawn(async move {
                if signaling.mqtt_client().is_connected().await
                    && let Err(e) = signaling.disconnect().await
                {
                    debug!(error = %e, "signaling disconnect failed on drop");
                }
            });
        }
    }
}

pub struct P2pConnection {
    connection: Connection,
    remote_peer: PeerInfo,
    remote_public_key: Option<[u8; 32]>,
}

impl P2pConnection {
    pub fn remote_peer(&self) -> &PeerInfo {
        &self.remote_peer
    }

    /// The remote peer's verified Ed25519 public key, taken from the
    /// certificate it presented during the QUIC handshake. When the remote used
    /// a persisted identity ([`PeerConfig::with_ed25519_identity`]) this is its
    /// `stitch_p2p::membership` peer id, so a host can decide whether a
    /// connecting device is paired. `None` if no key could be read.
    pub fn remote_public_key(&self) -> Option<[u8; 32]> {
        self.remote_public_key
    }

    /// Open a fresh bidirectional stream as the initiator. Use for carrying an
    /// application protocol over the established connection (e.g. state sync).
    pub async fn open_stream(&self) -> Result<(quinn::SendStream, quinn::RecvStream)> {
        self.connection
            .open_bi()
            .await
            .map_err(|e| Error::Quic(format!("open bidirectional stream: {e}")))
    }

    /// Accept the next bidirectional stream opened by the peer.
    pub async fn accept_stream(&self) -> Result<(quinn::SendStream, quinn::RecvStream)> {
        self.connection
            .accept_bi()
            .await
            .map_err(|e| Error::Quic(format!("accept bidirectional stream: {e}")))
    }

    /// Open a fresh unidirectional stream as the sender. Fits one message per
    /// stream (e.g. an encoded video frame that can be dropped with
    /// [`quinn::SendStream::reset`] once a newer frame supersedes it).
    pub async fn open_uni(&self) -> Result<quinn::SendStream> {
        self.connection
            .open_uni()
            .await
            .map_err(|e| Error::Quic(format!("open unidirectional stream: {e}")))
    }

    /// Accept the next unidirectional stream opened by the peer.
    pub async fn accept_uni(&self) -> Result<quinn::RecvStream> {
        self.connection
            .accept_uni()
            .await
            .map_err(|e| Error::Quic(format!("accept unidirectional stream: {e}")))
    }

    /// Send an unreliable datagram. Fits small latest-value-wins state (e.g.
    /// cursor position); it is unordered, unacknowledged, and never
    /// retransmitted, with no flow-control backpressure.
    pub fn send_datagram(&self, data: Bytes) -> Result<()> {
        self.connection
            .send_datagram(data)
            .map_err(|e| Error::Quic(format!("send datagram: {e}")))
    }

    /// Receive the next datagram sent by the peer.
    pub async fn read_datagram(&self) -> Result<Bytes> {
        self.connection
            .read_datagram()
            .await
            .map_err(|e| Error::Quic(format!("read datagram: {e}")))
    }

    /// Largest datagram that can currently be sent, or `None` when the peer
    /// has not enabled datagram support.
    pub fn max_datagram_size(&self) -> Option<usize> {
        self.connection.max_datagram_size()
    }

    pub async fn send_file(
        &self,
        path: &Path,
        progress: impl FnMut(TransferProgress),
    ) -> Result<TransferResult> {
        let (mut send, mut recv) = self
            .connection
            .open_bi()
            .await
            .map_err(|e| Error::Quic(format!("open bidirectional stream: {e}")))?;

        transfer::send_file(&mut send, &mut recv, path, progress).await
    }

    pub async fn receive_file(
        &self,
        output_dir: &Path,
        accept: impl FnOnce(&FileOffer) -> bool,
        progress: impl FnMut(TransferProgress),
    ) -> Result<TransferResult> {
        let (mut send, mut recv) = self
            .connection
            .accept_bi()
            .await
            .map_err(|e| Error::Quic(format!("accept bidirectional stream: {e}")))?;

        transfer::receive_file(&mut send, &mut recv, output_dir, accept, progress).await
    }

    pub fn close(self) -> Result<()> {
        self.connection.close(0u32.into(), b"done");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keepalive_defaults_to_a_third_of_the_lease() {
        let config = PeerConfig::new("edge", "127.0.0.1:1883");
        assert_eq!(config.effective_keepalive_secs(), PEER_LEASE_SECS / 3);
        let config = config.with_lease_secs(30);
        assert_eq!(config.effective_keepalive_secs(), 10);
    }

    #[test]
    fn keepalive_follows_the_lease_floor() {
        let config = PeerConfig::new("edge", "127.0.0.1:1883").with_lease_secs(0);
        assert_eq!(config.effective_lease_secs(), MIN_LEASE_SECS);
        assert_eq!(config.effective_keepalive_secs(), 1);
    }

    #[test]
    fn explicit_keepalive_overrides_the_default() {
        let config = PeerConfig::new("edge", "127.0.0.1:1883")
            .with_lease_secs(15)
            .with_keepalive_secs(60);
        assert_eq!(config.effective_keepalive_secs(), 60);
    }

    #[test]
    fn offload_override_only_sets_when_explicit() {
        assert_eq!(offload_override(None), None);
        assert_eq!(
            offload_override(Some(SegmentationOffload::Enabled)),
            Some(true)
        );
        assert_eq!(
            offload_override(Some(SegmentationOffload::Disabled)),
            Some(false)
        );
    }

    #[test]
    fn offload_defaults_to_unset_and_builders_override_it() {
        let config = PeerConfig::new("edge", "127.0.0.1:1883");
        assert_eq!(config.segmentation_offload, None);
        assert_eq!(
            config.with_segmentation_offload().segmentation_offload,
            Some(SegmentationOffload::Enabled)
        );
        let config = PeerConfig::new("edge", "127.0.0.1:1883");
        assert_eq!(
            config.without_segmentation_offload().segmentation_offload,
            Some(SegmentationOffload::Disabled)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn p2p_connection_exposes_uni_and_datagrams() {
        use crate::quic::{QuicEndpoint, generate_self_signed_cert};

        let peer_info = |id: &str, fingerprint: &str| PeerInfo {
            id: id.to_string(),
            name: id.to_string(),
            status: "online".to_string(),
            quic_port: 0,
            cert_fingerprint: fingerprint.to_string(),
            public_addr: None,
            expires_at: None,
            lease_secs: None,
        };

        let seed_b = [23u8; 32];
        let id_a = generate_self_signed_cert().expect("cert a");
        let id_b = quic::cert_from_ed25519_seed(&seed_b).expect("cert b");
        let expected_b = quic::public_key_from_cert(&id_b.cert_der).expect("b public key");
        let fp_a = id_a.fingerprint.clone();
        let fp_b = id_b.fingerprint.clone();
        let socket = || std::net::UdpSocket::bind("127.0.0.1:0").expect("socket");
        let ep_a = QuicEndpoint::bind(socket(), id_a).expect("bind a");
        let ep_b = QuicEndpoint::bind(socket(), id_b).expect("bind b");
        let addr_b = ep_b.local_addr().expect("addr b");

        let info_a = peer_info("a", &fp_a);
        let info_b = peer_info("b", &fp_b);

        let accept = tokio::spawn(async move {
            let raw = ep_b.accept_with_fingerprint(&fp_a).await.expect("accept");
            let remote_public_key = quic::peer_public_key(&raw);
            P2pConnection {
                connection: raw,
                remote_peer: info_a,
                remote_public_key,
            }
        });

        let raw_a = ep_a.connect(addr_b, &fp_b).await.expect("connect");
        let remote_public_key = quic::peer_public_key(&raw_a);
        let conn_a = P2pConnection {
            connection: raw_a,
            remote_peer: info_b,
            remote_public_key,
        };
        let conn_b = accept.await.expect("accept task");

        assert_eq!(conn_a.remote_public_key(), Some(expected_b));

        let mut send = conn_a.open_uni().await.expect("open_uni");
        send.write_all(b"frame-1").await.expect("write");
        send.finish().expect("finish");
        let mut recv = conn_b.accept_uni().await.expect("accept_uni");
        let frame = recv.read_to_end(1024).await.expect("read uni");
        assert_eq!(frame, b"frame-1");

        assert!(conn_a.max_datagram_size().is_some());
        conn_a
            .send_datagram(Bytes::from_static(b"cursor"))
            .expect("send_datagram");
        let datagram = conn_b.read_datagram().await.expect("read_datagram");
        assert_eq!(&datagram[..], b"cursor");

        conn_a.close().expect("close a");
        conn_b.close().expect("close b");
    }
}
