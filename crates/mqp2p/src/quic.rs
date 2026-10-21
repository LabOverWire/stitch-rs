use crate::error::{Error, Result};
use quinn::{Connection, Endpoint, ServerConfig};
use rcgen::{CertificateParams, KeyPair};
use ring::digest;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use std::net::SocketAddr;
use std::sync::Arc;
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
    let certs = connection
        .peer_identity()
        .and_then(|id| id.downcast::<Vec<CertificateDer<'static>>>().ok())?;
    public_key_from_cert(certs.first()?.as_ref())
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

pub struct QuicEndpoint {
    endpoint: Endpoint,
    identity: CertIdentity,
    transport: Arc<quinn::TransportConfig>,
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

        Ok(Self {
            endpoint,
            identity,
            transport,
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
        let incoming = self
            .endpoint
            .accept()
            .await
            .ok_or_else(|| Error::Quic("endpoint closed".into()))?;

        let connection = incoming
            .await
            .map_err(|e| Error::Quic(format!("accept failed: {e}")))?;

        let peer_certs = connection
            .peer_identity()
            .and_then(|id| id.downcast::<Vec<CertificateDer<'static>>>().ok())
            .ok_or_else(|| Error::Quic("no peer certificate presented".into()))?;

        let cert = peer_certs
            .first()
            .ok_or_else(|| Error::Quic("empty peer certificate chain".into()))?;

        let actual = compute_fingerprint(cert.as_ref());
        if actual != expected_fingerprint {
            connection.close(1u32.into(), b"fingerprint mismatch");
            return Err(Error::FingerprintMismatch {
                expected: expected_fingerprint.to_string(),
                actual,
            });
        }

        let remote = connection.remote_address();
        debug!(remote = %remote, "QUIC connection accepted with verified fingerprint");
        Ok(connection)
    }

    pub fn close(&self) {
        self.endpoint.close(0u32.into(), b"shutdown");
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
}
