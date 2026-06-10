use std::net::SocketAddr;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("STUN request to {server} failed: {reason}")]
    Stun { server: SocketAddr, reason: String },

    #[error("STUN response parse error: {0}")]
    StunParse(String),

    #[error("QUIC connection error: {0}")]
    Quic(String),

    #[error("QUIC connection to {addr} failed: {source}")]
    QuicConnect {
        addr: SocketAddr,
        source: quinn::ConnectionError,
    },

    #[error("certificate fingerprint mismatch: expected {expected}, got {actual}")]
    FingerprintMismatch { expected: String, actual: String },

    #[error("signaling error: {0}")]
    Signaling(String),

    #[error("transfer error: {0}")]
    Transfer(String),

    #[error("file hash mismatch: expected {expected}, got {actual}")]
    HashMismatch { expected: String, actual: String },

    #[error("transfer rejected: {0}")]
    TransferRejected(String),

    #[error("unexpected frame type: {0:#04x}")]
    UnexpectedFrame(u8),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
