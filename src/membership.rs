use crate::hlc::PeerId;
use crate::sync_state::FrameAuth;
use crate::wire::{SIGNATURE_LEN, WriteFrame};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};

/// A node's signing identity. The peer id is the Ed25519 public key, so a
/// frame's signature is verifiable directly against `stamp.peer` — no key
/// distribution needed. Construct from a persisted 32-byte seed.
pub struct Identity {
    key: SigningKey,
    peer_id: PeerId,
}

impl Identity {
    #[must_use]
    pub fn from_seed(seed: [u8; 32]) -> Self {
        let key = SigningKey::from_bytes(&seed);
        let peer_id = key.verifying_key().to_bytes();
        Self { key, peer_id }
    }

    #[must_use]
    pub fn peer_id(&self) -> PeerId {
        self.peer_id
    }
}

/// Verify a signed frame against its author's public key (`stamp.peer`).
/// Unsigned frames fail. Pure function — usable without holding an [`Identity`].
#[must_use]
pub fn verify_frame(frame: &WriteFrame) -> bool {
    let Some(sig_bytes) = frame.signature else {
        return false;
    };
    let Ok(verifying) = VerifyingKey::from_bytes(&frame.stamp.peer) else {
        return false;
    };
    let Ok(bytes) = frame.signing_bytes() else {
        return false;
    };
    let signature = Signature::from_bytes(&sig_bytes);
    verifying.verify(&bytes, &signature).is_ok()
}

impl FrameAuth for Identity {
    fn sign(&self, signing_bytes: &[u8]) -> [u8; SIGNATURE_LEN] {
        self.key.sign(signing_bytes).to_bytes()
    }

    fn verify(&self, frame: &WriteFrame) -> bool {
        verify_frame(frame)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hlc::{Hlc, Stamp};
    use crate::lww::Op;
    use crate::sync_state::SyncState;

    fn signed_frame(id: &Identity, seq: u64) -> WriteFrame {
        let mut frame = WriteFrame {
            stamp: Stamp::new(Hlc::new(seq, 0), id.peer_id()),
            seq,
            op: Op::Insert,
            entity: "task".into(),
            id: "t1".into(),
            data: vec![1, 2, 3],
            signature: None,
        };
        frame.signature = Some(id.sign(&frame.signing_bytes().unwrap()));
        frame
    }

    #[test]
    fn valid_signature_verifies() {
        let id = Identity::from_seed([7u8; 32]);
        let frame = signed_frame(&id, 1);
        assert!(verify_frame(&frame));
    }

    #[test]
    fn unsigned_frame_fails() {
        let id = Identity::from_seed([7u8; 32]);
        let mut frame = signed_frame(&id, 1);
        frame.signature = None;
        assert!(!verify_frame(&frame));
    }

    #[test]
    fn tampered_data_fails() {
        let id = Identity::from_seed([7u8; 32]);
        let mut frame = signed_frame(&id, 1);
        frame.data.push(99);
        assert!(!verify_frame(&frame));
    }

    #[test]
    fn forged_author_fails() {
        let author = Identity::from_seed([1u8; 32]);
        let imposter = Identity::from_seed([2u8; 32]);
        let mut frame = signed_frame(&author, 1);
        // Claim the write came from the imposter while keeping author's sig.
        frame.stamp = Stamp::new(frame.stamp.hlc, imposter.peer_id());
        assert!(!verify_frame(&frame));
    }

    #[tokio::test]
    async fn sync_state_rejects_unsigned_when_auth_set() {
        use std::sync::Arc;
        let id = Arc::new(Identity::from_seed([5u8; 32]));
        let mut state = SyncState::new(id.peer_id());
        state.set_auth(id.clone());

        let unsigned = WriteFrame {
            stamp: Stamp::new(Hlc::new(1, 0), id.peer_id()),
            seq: 1,
            op: Op::Insert,
            entity: "task".into(),
            id: "t1".into(),
            data: vec![],
            signature: None,
        };
        assert_eq!(
            state.receive(unsigned, 1),
            crate::replog::RecordOutcome::Rejected
        );
        assert_eq!(state.visible("task", "t1"), None);
    }

    #[tokio::test]
    async fn sync_state_signs_local_writes() {
        use std::sync::Arc;
        let id = Arc::new(Identity::from_seed([5u8; 32]));
        let mut state = SyncState::new(id.peer_id());
        state.set_auth(id.clone());
        let frame = state.local_write(1, Op::Insert, "task", "t1", vec![9]);
        assert!(frame.signature.is_some());
        assert!(verify_frame(&frame));
    }
}
