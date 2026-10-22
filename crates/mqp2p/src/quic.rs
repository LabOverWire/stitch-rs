use crate::error::{Error, Result};
use quinn::{Connection, Endpoint, ServerConfig};
use rcgen::{CertificateParams, KeyPair};
use ring::digest;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{debug, info};

pub struct CertIdentity {
    pub cert_der: Vec<u8>,
    pub key_der: Vec<u8>,
    pub fingerprint: String,
}

const ED25519_PKCS8_PREFIX: [u8; 16] = [
    0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04, 0x20,
];

pub fn generate_self_signed_cert() -> Result<CertIdentity> {
    let key_pair = KeyPair::generate_for(&rcgen::PKCS_ED25519)
        .map_err(|e| Error::Quic(format!("key generation failed: {e}")))?;
    self_signed_identity(&key_pair)
}

pub fn cert_from_ed25519_seed(seed: &[u8; 32]) -> Result<CertIdentity> {
    let mut pkcs8 = Vec::with_capacity(ED25519_PKCS8_PREFIX.len() + seed.len());
    pkcs8.extend_from_slice(&ED25519_PKCS8_PREFIX);
    pkcs8.extend_from_slice(seed);
    let key_pair = KeyPair::from_pkcs8_der_and_sign_algo(
        &PrivatePkcs8KeyDer::from(pkcs8),
        &rcgen::PKCS_ED25519,
    )
    .map_err(|e| Error::Quic(format!("key load failed: {e}")))?;
    self_signed_identity(&key_pair)
}

fn self_signed_identity(key_pair: &KeyPair) -> Result<CertIdentity> {
    let mut params = CertificateParams::new(vec!["mqp2p-peer".into()])
        .map_err(|e| Error::Quic(format!("cert params failed: {e}")))?;
    params.distinguished_name = rcgen::DistinguishedName::new();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "mqp2p-peer");

    let cert = params
        .self_signed(key_pair)
        .map_err(|e| Error::Quic(format!("self-sign failed: {e}")))?;

    let cert_der = cert.der().to_vec();
    let key_der = key_pair.serialize_der();
    let fingerprint = compute_fingerprint(&cert_der);

    debug!(fingerprint, "built self-signed certificate");

    Ok(CertIdentity {
        cert_der,
        key_der,
        fingerprint,
    })
}

pub fn public_key_from_cert(cert_der: &[u8]) -> Option<[u8; 32]> {
    let (_, cert) = x509_parser::parse_x509_certificate(cert_der).ok()?;
    <[u8; 32]>::try_from(cert.public_key().subject_public_key.data.as_ref()).ok()
}

pub fn peer_public_key(connection: &Connection) -> Option<[u8; 32]> {
    let key = connection
        .peer_identity()
        .and_then(|id| id.downcast::<Vec<CertificateDer<'static>>>().ok())
        .and_then(|certs| certs.first().and_then(|c| public_key_from_cert(c.as_ref())));
    if key.is_none() {
        debug!("could not read a peer ed25519 public key from the connection");
    }
    key
}

pub fn compute_fingerprint(cert_der: &[u8]) -> String {
    let hash = digest::digest(&digest::SHA256, cert_der);
    let hex: String = hash
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<Vec<_>>()
        .join(":");
    format!("sha256:{hex}")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentationOffload {
    Enabled,
    Disabled,
}

fn transport_config(offload: SegmentationOffload) -> Arc<quinn::TransportConfig> {
    let mut transport = quinn::TransportConfig::default();
    transport.enable_segmentation_offload(offload == SegmentationOffload::Enabled);
    Arc::new(transport)
}

fn build_server_config(
    identity: &CertIdentity,
    transport: Arc<quinn::TransportConfig>,
) -> Result<ServerConfig> {
    let cert = CertificateDer::from(identity.cert_der.clone());
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(identity.key_der.clone()));

    let client_verifier = Arc::new(CollectClientCert);

    let server_crypto = rustls::ServerConfig::builder()
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(vec![cert], key)
        .map_err(|e| Error::Quic(format!("server TLS config failed: {e}")))?;

    let quic_server_config = quinn::crypto::rustls::QuicServerConfig::try_from(server_crypto)
        .map_err(|e| Error::Quic(format!("QUIC server config failed: {e}")))?;

    let mut server_config = ServerConfig::with_crypto(Arc::new(quic_server_config));
    server_config.transport_config(transport);
    Ok(server_config)
}

fn build_client_config(
    identity: &CertIdentity,
    expected_fingerprint: &str,
    transport: Arc<quinn::TransportConfig>,
) -> Result<quinn::ClientConfig> {
    let cert = CertificateDer::from(identity.cert_der.clone());
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(identity.key_der.clone()));

    let verifier = Arc::new(FingerprintVerifier {
        expected: expected_fingerprint.to_string(),
    });

    let client_crypto = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_client_auth_cert(vec![cert], key)
        .map_err(|e| Error::Quic(format!("client TLS config failed: {e}")))?;

    let quic_client_config = quinn::crypto::rustls::QuicClientConfig::try_from(client_crypto)
        .map_err(|e| Error::Quic(format!("QUIC client config failed: {e}")))?;

    let mut client_config = quinn::ClientConfig::new(Arc::new(quic_client_config));
    client_config.transport_config(transport);
    Ok(client_config)
}

#[derive(Debug)]
struct FingerprintVerifier {
    expected: String,
}

impl rustls::client::danger::ServerCertVerifier for FingerprintVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        let actual = compute_fingerprint(end_entity.as_ref());
        if actual == self.expected {
            Ok(rustls::client::danger::ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::General(format!(
                "fingerprint mismatch: expected {}, got {actual}",
                self.expected
            )))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[derive(Debug)]
struct CollectClientCert;

impl rustls::server::danger::ClientCertVerifier for CollectClientCert {
    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self) -> bool {
        true
    }

    fn root_hint_subjects(&self) -> &[rustls::DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::server::danger::ClientCertVerified, rustls::Error> {
        Ok(rustls::server::danger::ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

const MAX_BUFFERED_CONNECTIONS: usize = 256;
const BUFFERED_CONNECTION_TTL: Duration = Duration::from_secs(30);

struct Buffered {
    conn: Connection,
    at: Instant,
}

#[derive(Default)]
struct AcceptState {
    waiters: HashMap<String, VecDeque<(u64, oneshot::Sender<Connection>)>>,
    arrived: HashMap<String, VecDeque<Buffered>>,
    next_waiter_id: u64,
    closed: bool,
}

impl AcceptState {
    fn buffered_total(&self) -> usize {
        self.arrived.values().map(VecDeque::len).sum()
    }

    fn remove_waiter(&mut self, fingerprint: &str, id: u64) {
        if let Some(queue) = self.waiters.get_mut(fingerprint) {
            queue.retain(|(waiter_id, _)| *waiter_id != id);
            if queue.is_empty() {
                self.waiters.remove(fingerprint);
            }
        }
    }
}

/// Removes a waiter on cancellation. If `accept_with_fingerprint` is dropped
/// before a matching connection arrives, its sender would otherwise linger in
/// `waiters` until a connection with that fingerprint happened to appear.
struct WaiterGuard<'a> {
    state: &'a Mutex<AcceptState>,
    fingerprint: &'a str,
    id: u64,
}

impl Drop for WaiterGuard<'_> {
    fn drop(&mut self) {
        self.state
            .lock()
            .expect("accept state")
            .remove_waiter(self.fingerprint, self.id);
    }
}

fn connection_fingerprint(connection: &Connection) -> Option<String> {
    let peer_certs = connection
        .peer_identity()?
        .downcast::<Vec<CertificateDer<'static>>>()
        .ok()?;
    let cert = peer_certs.first()?;
    Some(compute_fingerprint(cert.as_ref()))
}

fn evict_overflow(state: &mut AcceptState) {
    while state.buffered_total() >= MAX_BUFFERED_CONNECTIONS {
        let Some(oldest) = state
            .arrived
            .iter()
            .filter_map(|(fp, queue)| queue.front().map(|b| (fp.clone(), b.at)))
            .min_by_key(|(_, at)| *at)
            .map(|(fp, _)| fp)
        else {
            break;
        };
        let queue = state.arrived.get_mut(&oldest).expect("oldest present");
        let dropped = queue.pop_front().expect("front present");
        dropped.conn.close(0u32.into(), b"accept buffer full");
        if queue.is_empty() {
            state.arrived.remove(&oldest);
        }
    }
}

fn expire_buffered(state: &mut AcceptState) {
    let now = Instant::now();
    for queue in state.arrived.values_mut() {
        while queue
            .front()
            .is_some_and(|b| now.duration_since(b.at) >= BUFFERED_CONNECTION_TTL)
        {
            let expired = queue.pop_front().expect("front present");
            expired.conn.close(0u32.into(), b"unclaimed");
        }
    }
    state.arrived.retain(|_, queue| !queue.is_empty());
}

fn spawn_sweep_loop(state: Arc<Mutex<AcceptState>>) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(BUFFERED_CONNECTION_TTL).await;
            let mut guard = state.lock().expect("accept state");
            if guard.closed {
                break;
            }
            expire_buffered(&mut guard);
        }
    })
}

fn deliver(state: &Mutex<AcceptState>, fingerprint: String, mut conn: Connection) {
    let mut guard = state.lock().expect("accept state");
    if guard.closed {
        conn.close(0u32.into(), b"endpoint closed");
        return;
    }
    if let Some(queue) = guard.waiters.get_mut(&fingerprint) {
        while let Some((_, waiter)) = queue.pop_front() {
            match waiter.send(conn) {
                Ok(()) => {
                    if queue.is_empty() {
                        guard.waiters.remove(&fingerprint);
                    }
                    return;
                }
                Err(returned) => conn = returned,
            }
        }
        guard.waiters.remove(&fingerprint);
    }
    evict_overflow(&mut guard);
    guard
        .arrived
        .entry(fingerprint)
        .or_default()
        .push_back(Buffered {
            conn,
            at: Instant::now(),
        });
}

fn spawn_accept_loop(endpoint: Endpoint, state: Arc<Mutex<AcceptState>>) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(incoming) = endpoint.accept().await {
            let state = Arc::clone(&state);
            tokio::spawn(async move {
                let connection = match incoming.await {
                    Ok(connection) => connection,
                    Err(e) => {
                        debug!(error = %e, "inbound QUIC handshake failed");
                        return;
                    }
                };
                let Some(fingerprint) = connection_fingerprint(&connection) else {
                    connection.close(1u32.into(), b"no verifiable certificate");
                    return;
                };
                debug!(
                    remote = %connection.remote_address(),
                    "routing inbound connection by fingerprint"
                );
                deliver(&state, fingerprint, connection);
            });
        }
        let mut state = state.lock().expect("accept state");
        state.closed = true;
        state.waiters.clear();
        for (_, queue) in state.arrived.drain() {
            for buffered in queue {
                buffered.conn.close(0u32.into(), b"endpoint closed");
            }
        }
    })
}

pub struct QuicEndpoint {
    endpoint: Endpoint,
    identity: CertIdentity,
    transport: Arc<quinn::TransportConfig>,
    accept_state: Arc<Mutex<AcceptState>>,
    accept_task: JoinHandle<()>,
    sweep_task: JoinHandle<()>,
}

impl QuicEndpoint {
    pub fn bind(socket: std::net::UdpSocket, identity: CertIdentity) -> Result<Self> {
        Self::bind_with_offload(socket, identity, SegmentationOffload::Enabled)
    }

    pub fn bind_with_offload(
        socket: std::net::UdpSocket,
        identity: CertIdentity,
        offload: SegmentationOffload,
    ) -> Result<Self> {
        debug!(?offload, "QUIC transport: segmentation offload");
        Self::bind_with_transport(socket, identity, transport_config(offload))
    }

    pub fn bind_with_transport(
        socket: std::net::UdpSocket,
        identity: CertIdentity,
        transport: Arc<quinn::TransportConfig>,
    ) -> Result<Self> {
        let server_config = build_server_config(&identity, Arc::clone(&transport))?;
        let runtime = quinn::default_runtime()
            .ok_or_else(|| Error::Quic("no async runtime available".into()))?;

        let endpoint = Endpoint::new(
            quinn::EndpointConfig::default(),
            Some(server_config),
            socket,
            runtime,
        )
        .map_err(|e| Error::Quic(format!("endpoint bind failed: {e}")))?;

        let local = endpoint
            .local_addr()
            .map_err(|e| Error::Quic(format!("{e}")))?;
        info!(addr = %local, "QUIC endpoint bound");

        let accept_state = Arc::new(Mutex::new(AcceptState::default()));
        let accept_task = spawn_accept_loop(endpoint.clone(), Arc::clone(&accept_state));
        let sweep_task = spawn_sweep_loop(Arc::clone(&accept_state));

        Ok(Self {
            endpoint,
            identity,
            transport,
            accept_state,
            accept_task,
            sweep_task,
        })
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.endpoint
            .local_addr()
            .map_err(|e| Error::Quic(format!("{e}")))
    }

    pub fn fingerprint(&self) -> &str {
        &self.identity.fingerprint
    }

    pub async fn connect(&self, addr: SocketAddr, remote_fingerprint: &str) -> Result<Connection> {
        let client_config = build_client_config(
            &self.identity,
            remote_fingerprint,
            Arc::clone(&self.transport),
        )?;

        let connection = self
            .endpoint
            .connect_with(client_config, addr, "mqp2p-peer")
            .map_err(|e| Error::Quic(format!("connect failed: {e}")))?
            .await
            .map_err(|e| Error::QuicConnect { addr, source: e })?;

        debug!(addr = %addr, "QUIC connection established (initiator)");
        Ok(connection)
    }

    pub async fn accept_with_fingerprint(&self, expected_fingerprint: &str) -> Result<Connection> {
        let (id, receiver) = {
            let mut state = self.accept_state.lock().expect("accept state");
            if state.closed {
                return Err(Error::Quic("endpoint closed".into()));
            }
            loop {
                let buffered = match state.arrived.get_mut(expected_fingerprint) {
                    Some(queue) => {
                        let front = queue.pop_front();
                        if queue.is_empty() {
                            state.arrived.remove(expected_fingerprint);
                        }
                        front
                    }
                    None => None,
                };
                match buffered {
                    Some(buffered) if buffered.conn.close_reason().is_none() => {
                        debug!(
                            remote = %buffered.conn.remote_address(),
                            "QUIC connection accepted with verified fingerprint"
                        );
                        return Ok(buffered.conn);
                    }
                    Some(_) => continue,
                    None => break,
                }
            }
            let id = state.next_waiter_id;
            state.next_waiter_id += 1;
            let (sender, receiver) = oneshot::channel();
            state
                .waiters
                .entry(expected_fingerprint.to_string())
                .or_default()
                .push_back((id, sender));
            (id, receiver)
        };

        let _guard = WaiterGuard {
            state: &self.accept_state,
            fingerprint: expected_fingerprint,
            id,
        };
        let connection = receiver
            .await
            .map_err(|_| Error::Quic("endpoint closed".into()))?;
        debug!(
            remote = %connection.remote_address(),
            "QUIC connection accepted with verified fingerprint"
        );
        Ok(connection)
    }

    pub fn close(&self) {
        self.endpoint.close(0u32.into(), b"shutdown");
    }
}

impl Drop for QuicEndpoint {
    fn drop(&mut self) {
        self.accept_task.abort();
        self.sweep_task.abort();
        let mut state = self.accept_state.lock().expect("accept state");
        state.closed = true;
        state.waiters.clear();
        for (_, queue) in state.arrived.drain() {
            for buffered in queue {
                buffered.conn.close(0u32.into(), b"endpoint closed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_cert_produces_valid_fingerprint() {
        let id = generate_self_signed_cert().unwrap();
        assert!(id.fingerprint.starts_with("sha256:"));
        assert!(!id.cert_der.is_empty());
        assert!(!id.key_der.is_empty());
    }

    #[test]
    fn fingerprint_is_deterministic() {
        let data = b"test certificate data";
        let fp1 = compute_fingerprint(data);
        let fp2 = compute_fingerprint(data);
        assert_eq!(fp1, fp2);
        assert!(fp1.starts_with("sha256:"));
    }

    #[test]
    fn different_certs_have_different_fingerprints() {
        let id1 = generate_self_signed_cert().unwrap();
        let id2 = generate_self_signed_cert().unwrap();
        assert_ne!(id1.fingerprint, id2.fingerprint);
    }

    #[test]
    fn a_persisted_seed_yields_a_stable_certificate() {
        let seed = [9u8; 32];
        let a = cert_from_ed25519_seed(&seed).unwrap();
        let b = cert_from_ed25519_seed(&seed).unwrap();
        assert_eq!(a.cert_der, b.cert_der);
        assert_eq!(a.key_der, b.key_der);
        assert_eq!(a.fingerprint, b.fingerprint);
    }

    #[test]
    fn different_seeds_yield_different_identities() {
        let a = cert_from_ed25519_seed(&[1u8; 32]).unwrap();
        let b = cert_from_ed25519_seed(&[2u8; 32]).unwrap();
        assert_ne!(a.fingerprint, b.fingerprint);
    }

    #[test]
    fn a_certificate_carries_a_stable_ed25519_public_key() {
        let seed = [3u8; 32];
        let first = cert_from_ed25519_seed(&seed).unwrap();
        let second = cert_from_ed25519_seed(&seed).unwrap();
        let key = public_key_from_cert(&first.cert_der).expect("ed25519 public key");
        assert_eq!(public_key_from_cert(&second.cert_der), Some(key));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bind_with_transport_applies_the_supplied_config_to_both_endpoints() {
        let no_datagrams = || {
            let mut transport = quinn::TransportConfig::default();
            transport.datagram_receive_buffer_size(None);
            Arc::new(transport)
        };

        let id_a = generate_self_signed_cert().expect("cert a");
        let id_b = generate_self_signed_cert().expect("cert b");
        let fp_a = id_a.fingerprint.clone();
        let fp_b = id_b.fingerprint.clone();
        let socket = || std::net::UdpSocket::bind("127.0.0.1:0").expect("socket");
        let ep_a =
            QuicEndpoint::bind_with_transport(socket(), id_a, no_datagrams()).expect("bind a");
        let ep_b =
            QuicEndpoint::bind_with_transport(socket(), id_b, no_datagrams()).expect("bind b");
        let addr_b = ep_b.local_addr().expect("addr b");

        let accept =
            tokio::spawn(async move { ep_b.accept_with_fingerprint(&fp_a).await.expect("accept") });

        let conn_a = ep_a.connect(addr_b, &fp_b).await.expect("connect");
        let conn_b = accept.await.expect("accept task");

        assert!(
            conn_a.max_datagram_size().is_none(),
            "accept-side (server) transport applied: B disabled datagram receive",
        );
        assert!(
            conn_b.max_datagram_size().is_none(),
            "connect-side (client) transport applied: A disabled datagram receive",
        );
    }

    async fn remote_key_seen_by_initiator(seed: [u8; 32], fp_b: &str) -> [u8; 32] {
        let id_a = generate_self_signed_cert().expect("cert a");
        let fp_a = id_a.fingerprint.clone();
        let id_b = cert_from_ed25519_seed(&seed).expect("cert b");
        let socket = || std::net::UdpSocket::bind("127.0.0.1:0").expect("socket");
        let ep_a = QuicEndpoint::bind(socket(), id_a).expect("bind a");
        let ep_b = QuicEndpoint::bind(socket(), id_b).expect("bind b");
        let addr_b = ep_b.local_addr().expect("addr b");
        let accept =
            tokio::spawn(async move { ep_b.accept_with_fingerprint(&fp_a).await.expect("accept") });
        let conn_a = ep_a.connect(addr_b, fp_b).await.expect("connect");
        let _conn_b = accept.await.expect("accept task");
        peer_public_key(&conn_a).expect("remote public key")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_persisted_identity_presents_the_same_key_across_restarts() {
        let seed = [11u8; 32];
        let identity = cert_from_ed25519_seed(&seed).unwrap();
        let expected = public_key_from_cert(&identity.cert_der).unwrap();

        let first = remote_key_seen_by_initiator(seed, &identity.fingerprint).await;
        let second = remote_key_seen_by_initiator(seed, &identity.fingerprint).await;
        assert_eq!(first, expected);
        assert_eq!(second, expected);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_dials_route_to_their_matching_offers() {
        let id_r = generate_self_signed_cert().expect("cert r");
        let id_a = generate_self_signed_cert().expect("cert a");
        let id_b = generate_self_signed_cert().expect("cert b");
        let fp_r = id_r.fingerprint.clone();
        let fp_a = id_a.fingerprint.clone();
        let fp_b = id_b.fingerprint.clone();
        let socket = || std::net::UdpSocket::bind("127.0.0.1:0").expect("socket");
        let ep_r = Arc::new(QuicEndpoint::bind(socket(), id_r).expect("bind r"));
        let ep_a = QuicEndpoint::bind(socket(), id_a).expect("bind a");
        let ep_b = QuicEndpoint::bind(socket(), id_b).expect("bind b");
        let addr_r = ep_r.local_addr().expect("addr r");

        let accept_a = {
            let ep = Arc::clone(&ep_r);
            let fp = fp_a.clone();
            tokio::spawn(async move { ep.accept_with_fingerprint(&fp).await })
        };
        let accept_b = {
            let ep = Arc::clone(&ep_r);
            let fp = fp_b.clone();
            tokio::spawn(async move { ep.accept_with_fingerprint(&fp).await })
        };

        let (conn_a, conn_b) =
            tokio::join!(ep_a.connect(addr_r, &fp_r), ep_b.connect(addr_r, &fp_r));
        conn_a.expect("peer a dials the responder");
        conn_b.expect("peer b dials the responder");

        let accepted_a = accept_a.await.expect("accept task a").expect("accept a");
        let accepted_b = accept_b.await.expect("accept task b").expect("accept b");

        assert_eq!(
            connection_fingerprint(&accepted_a).as_deref(),
            Some(fp_a.as_str()),
            "the waiter for peer a received peer a's connection"
        );
        assert_eq!(
            connection_fingerprint(&accepted_b).as_deref(),
            Some(fp_b.as_str()),
            "the waiter for peer b received peer b's connection"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_cancelled_accept_does_not_leak_a_waiter() {
        let id = generate_self_signed_cert().expect("cert");
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").expect("socket");
        let ep = QuicEndpoint::bind(socket, id).expect("bind");

        let outcome = tokio::time::timeout(
            Duration::from_millis(50),
            ep.accept_with_fingerprint("sha256:never-arrives"),
        )
        .await;
        assert!(outcome.is_err(), "no connection should arrive");

        let state = ep.accept_state.lock().expect("accept state");
        assert!(
            state.waiters.is_empty(),
            "a cancelled accept must remove its waiter"
        );
    }
}
