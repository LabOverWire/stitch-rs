use crate::error::{Error, Result};
use crate::quic::{self, CertIdentity, QuicEndpoint};
use crate::signaling::{
    Candidate, CandidateKind, ConnectionOffer, ConnectionSync, PeerInfo, SignalingClient,
};
use crate::stun;
use crate::transfer::{self, FileOffer, TransferProgress, TransferResult};
use mqtt5::ConnectOptions;
use mqtt5::client::MqttClient;
use quinn::Connection;
use std::net::SocketAddr;
use std::path::Path;
use tracing::{debug, info, warn};

pub struct PeerConfig {
    pub name: String,
    pub broker_addr: String,
    pub bind_addr: SocketAddr,
    pub stun_server: Option<String>,
    pub credentials: Option<(String, String)>,
}

impl PeerConfig {
    pub fn new(name: impl Into<String>, broker_addr: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            broker_addr: broker_addr.into(),
            bind_addr: "0.0.0.0:0".parse().unwrap_or_else(|_| unreachable!()),
            stun_server: Some(stun::DEFAULT_STUN_SERVER.into()),
            credentials: None,
        }
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

    pub fn with_credentials(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.credentials = Some((username.into(), password.into()));
        self
    }
}

pub type PeerId = String;

pub struct Peer {
    name: String,
    identity: CertIdentity,
    signaling: SignalingClient,
    quic: QuicEndpoint,
    peer_id: Option<String>,
    std_socket: std::net::UdpSocket,
    host_addr: SocketAddr,
    srflx_addr: Option<SocketAddr>,
}

impl Peer {
    pub async fn new(config: PeerConfig) -> Result<Self> {
        let identity = quic::generate_self_signed_cert()?;

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
        let quic = QuicEndpoint::bind(
            quic_socket,
            CertIdentity {
                cert_der: identity.cert_der.clone(),
                key_der: identity.key_der.clone(),
                fingerprint: identity.fingerprint.clone(),
            },
        )?;

        let client_id = format!("mqp2p-{}", config.name);
        let mqtt = if let Some((ref user, ref pass)) = config.credentials {
            let opts = ConnectOptions::new(client_id).with_credentials(user, pass.as_bytes());
            MqttClient::with_options(opts)
        } else {
            MqttClient::new(client_id)
        };
        let signaling = SignalingClient::new(mqtt);
        signaling.connect(&config.broker_addr).await?;

        Ok(Self {
            name: config.name,
            identity,
            signaling,
            quic,
            peer_id: None,
            std_socket,
            host_addr,
            srflx_addr,
        })
    }

    pub async fn register(&mut self) -> Result<PeerId> {
        let quic_port = self.quic.local_addr()?.port();

        let peer_id = self
            .signaling
            .register_peer(&self.name, quic_port, &self.identity.fingerprint)
            .await?;

        if let Some(addr) = self.srflx_addr {
            self.signaling.update_peer_addr(&peer_id, addr).await?;
        }

        self.peer_id = Some(peer_id.clone());
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
        &self.identity.fingerprint
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
            cert_fingerprint: self.identity.fingerprint.clone(),
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

        Ok(P2pConnection {
            connection,
            remote_peer: target.clone(),
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
            cert_fingerprint: self.identity.fingerprint.clone(),
            candidates,
        };

        self.signaling.send_answer(&offer.from, &answer).await?;

        if offer.candidates.is_empty() {
            return Err(Error::Signaling("no candidates in offer".into()));
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

        for candidate in &offer.candidates {
            let tokio_socket = tokio::net::UdpSocket::from_std(self.std_socket.try_clone()?)?;
            let _ = tokio_socket
                .send_to(&[0x50, 0x32, 0x50, 0x50], candidate.addr)
                .await;
        }

        let connection = self
            .quic
            .accept_with_fingerprint(&offer.cert_fingerprint)
            .await?;

        let remote_addr = connection.remote_address();

        let remote_peer = PeerInfo {
            id: offer.from,
            name: String::new(),
            status: "connected".into(),
            quic_port: remote_addr.port(),
            cert_fingerprint: offer.cert_fingerprint,
            public_addr: Some(remote_addr.to_string()),
        };

        info!(
            remote = %remote_addr,
            peer_id = remote_peer.id,
            "P2P connection established (responder)"
        );

        Ok(P2pConnection {
            connection,
            remote_peer,
        })
    }

    pub async fn shutdown(self) -> Result<()> {
        if let Some(peer_id) = &self.peer_id {
            let _ = self.signaling.deregister_peer(peer_id).await;
        }
        self.quic.close();
        info!("peer shut down");
        Ok(())
    }
}

pub struct P2pConnection {
    connection: Connection,
    remote_peer: PeerInfo,
}

impl P2pConnection {
    pub fn remote_peer(&self) -> &PeerInfo {
        &self.remote_peer
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
