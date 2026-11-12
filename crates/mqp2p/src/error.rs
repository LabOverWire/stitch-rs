use std::net::SocketAddr;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("STUN request to {server} failed: {reason}")]
    Stun { server: SocketAddr, reason: String },

    #[error("STUN response parse error: {0}")]
    StunParse(String),

    #[error("TURN message parse error: {0}")]
    TurnParse(String),

    #[error("TURN error: {0}")]
    Turn(String),

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

    #[error("{stage} stage failed for session {session} with peer {peer}: {reason}")]
    Connection {
        stage: ConnectionStage,
        session: String,
        peer: String,
        reason: String,
    },

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

/// The stage of connection establishment that failed, named in
/// [`Error::Connection`] so a field report can be tied to a session and peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionStage {
    /// Waiting for the peer's answer to an offer (initiator).
    Answer,
    /// Waiting for the initiator's sync before probing (responder).
    Sync,
    /// Accepting the inbound QUIC connection (responder).
    InboundAccept,
    /// Selecting or reaching a candidate address: the initiator exhausting the
    /// answer's candidates, or the responder finding no connectable candidate in
    /// the offer.
    Candidate,
}

impl Error {
    pub(crate) fn connection(
        stage: ConnectionStage,
        session: &str,
        peer: &str,
        reason: impl Into<String>,
    ) -> Self {
        Error::Connection {
            stage,
            session: session.to_string(),
            peer: peer.to_string(),
            reason: reason.into(),
        }
    }
}

impl std::fmt::Display for ConnectionStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            ConnectionStage::Answer => "answer",
            ConnectionStage::Sync => "sync",
            ConnectionStage::InboundAccept => "inbound accept",
            ConnectionStage::Candidate => "candidate",
        })
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_stage_names_are_stable() {
        assert_eq!(ConnectionStage::Answer.to_string(), "answer");
        assert_eq!(ConnectionStage::Sync.to_string(), "sync");
        assert_eq!(ConnectionStage::InboundAccept.to_string(), "inbound accept");
        assert_eq!(ConnectionStage::Candidate.to_string(), "candidate");
    }

    #[test]
    fn connection_error_names_stage_session_and_peer() {
        let err = Error::Connection {
            stage: ConnectionStage::Sync,
            session: "sess-1".into(),
            peer: "peer-9".into(),
            reason: "timed out waiting for sync".into(),
        };
        assert_eq!(
            err.to_string(),
            "sync stage failed for session sess-1 with peer peer-9: timed out waiting for sync"
        );
    }
}
