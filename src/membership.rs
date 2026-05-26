use crate::hlc::{PEER_ID_LEN, PeerId};
use crate::sync_state::FrameAuth;
use crate::wire::{SIGNATURE_LEN, WriteFrame};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use std::collections::HashMap;

/// Reserved entity holding membership records (one per target peer, id = the
/// target's hex peer id, data = a single role byte). Authorization reads this.
pub const MEMBERS_ENTITY: &str = "_members";

/// A member's role. Owner and Admin may change membership; all roles may write
/// data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Owner,
    Admin,
    Member,
}

impl Role {
    #[must_use]
    pub fn as_byte(self) -> u8 {
        match self {
            Role::Owner => 0,
            Role::Admin => 1,
            Role::Member => 2,
        }
    }

    #[must_use]
    pub fn from_byte(b: u8) -> Option<Role> {
        match b {
            0 => Some(Role::Owner),
            1 => Some(Role::Admin),
            2 => Some(Role::Member),
            _ => None,
        }
    }

    /// May this role grant/revoke membership?
    #[must_use]
    pub fn can_admin(self) -> bool {
        matches!(self, Role::Owner | Role::Admin)
    }
}

/// Render a peer id as the `_members` record id (lowercase hex).
#[must_use]
pub fn member_record_id(peer: &PeerId) -> String {
    let mut s = String::with_capacity(peer.len() * 2);
    for b in peer {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// Parse a `_members` record id (hex) back to a peer id.
#[must_use]
pub fn peer_from_record_id(id: &str) -> Option<PeerId> {
    if id.len() != PEER_ID_LEN * 2 {
        return None;
    }
    let mut peer = [0u8; PEER_ID_LEN];
    for (i, slot) in peer.iter_mut().enumerate() {
        *slot = u8::from_str_radix(&id[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(peer)
}

/// Derive the authorized member set from the genesis owner and the current
/// membership records. Each record is `(target, granter, role)` — the granter
/// is the peer that authored the record. A record takes effect only if its
/// granter is itself an authorized owner/admin; this is evaluated to a fixpoint
/// from the genesis owner, so chained delegation resolves and entries rooted in
/// no authority are ignored. The genesis owner is always Owner and cannot be
/// demoted by any record.
///
/// This is a pure function of converged state, which is why authorization can
/// be applied as a read-time filter without breaking convergence (see
/// `spec/StitchP2PAuth.tla`).
#[must_use]
pub fn authorized_members(
    owner: PeerId,
    records: &[(PeerId, PeerId, Role)],
) -> HashMap<PeerId, Role> {
    let mut authorized: HashMap<PeerId, Role> = HashMap::new();
    authorized.insert(owner, Role::Owner);
    loop {
        let mut changed = false;
        for (target, granter, role) in records {
            if *target == owner {
                continue;
            }
            let granter_can_admin = authorized.get(granter).is_some_and(|r| r.can_admin());
            if granter_can_admin && authorized.get(target) != Some(role) {
                authorized.insert(*target, *role);
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
    authorized
}

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

    fn pid(n: u8) -> PeerId {
        let mut id = [0u8; 32];
        id[0] = n;
        id
    }

    #[test]
    fn owner_alone_is_authorized() {
        let set = authorized_members(pid(1), &[]);
        assert_eq!(set.get(&pid(1)), Some(&Role::Owner));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn owner_invites_member() {
        let set = authorized_members(pid(1), &[(pid(2), pid(1), Role::Member)]);
        assert_eq!(set.get(&pid(2)), Some(&Role::Member));
    }

    #[test]
    fn non_admin_grant_is_ignored() {
        // member 2 (invited by owner) tries to invite 3 — 2 isn't admin.
        let records = vec![
            (pid(2), pid(1), Role::Member),
            (pid(3), pid(2), Role::Member),
        ];
        let set = authorized_members(pid(1), &records);
        assert_eq!(set.get(&pid(2)), Some(&Role::Member));
        assert_eq!(set.get(&pid(3)), None, "member cannot invite");
    }

    #[test]
    fn admin_delegation_chains() {
        // owner makes 2 an admin; 2 invites 3.
        let records = vec![
            (pid(2), pid(1), Role::Admin),
            (pid(3), pid(2), Role::Member),
        ];
        let set = authorized_members(pid(1), &records);
        assert_eq!(set.get(&pid(2)), Some(&Role::Admin));
        assert_eq!(set.get(&pid(3)), Some(&Role::Member), "admin can invite");
    }

    #[test]
    fn unrooted_records_are_ignored() {
        // 2 and 3 grant each other but neither is rooted in the owner.
        let records = vec![
            (pid(2), pid(3), Role::Admin),
            (pid(3), pid(2), Role::Admin),
        ];
        let set = authorized_members(pid(1), &records);
        assert_eq!(set.len(), 1);
        assert_eq!(set.get(&pid(1)), Some(&Role::Owner));
    }

    #[test]
    fn owner_cannot_be_demoted() {
        let set = authorized_members(pid(1), &[(pid(1), pid(1), Role::Member)]);
        assert_eq!(set.get(&pid(1)), Some(&Role::Owner));
    }

    #[test]
    fn record_id_round_trips() {
        let p = pid(42);
        assert_eq!(peer_from_record_id(&member_record_id(&p)), Some(p));
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
