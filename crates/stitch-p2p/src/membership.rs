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

    /// The 32-byte Ed25519 seed behind this identity. Feed it to
    /// `mqp2p::PeerConfig::with_ed25519_identity` so the device's transport
    /// certificate carries the same key as its membership peer id.
    #[must_use]
    pub fn seed(&self) -> [u8; 32] {
        self.key.to_bytes()
    }
}

/// A read-only view of a scope's authorized membership over converged state,
/// for gating peers by their transport key (which equals their membership peer
/// id once both are built from one [`Identity`]). Clone-cheap; every clone
/// shares the node's state.
#[derive(Clone)]
pub struct Membership {
    node: crate::node::SyncNode,
    owner: PeerId,
}

impl Membership {
    #[must_use]
    pub fn new(node: crate::node::SyncNode, owner: PeerId) -> Self {
        Self { node, owner }
    }

    /// The authorized member set derived from converged `_members` records.
    pub async fn authorized(&self) -> HashMap<PeerId, Role> {
        let records: Vec<(PeerId, PeerId, Role)> = self
            .node
            .entries_with_authors(MEMBERS_ENTITY)
            .await
            .into_iter()
            .filter_map(|(id, data, granter)| {
                let target = peer_from_record_id(&id)?;
                let role = Role::from_byte(*data.first()?)?;
                Some((target, granter, role))
            })
            .collect();
        authorized_members(self.owner, &records)
    }

    /// Whether `peer` is an authorized member of the scope right now.
    pub async fn is_member(&self, peer: &PeerId) -> bool {
        self.authorized().await.contains_key(peer)
    }

    /// This node's membership proof: the signed `_members` frames it holds. A
    /// peer presents these on connect so a receiver that has not yet synced the
    /// peer's membership record can still authorize it against the genesis
    /// owner, breaking the member-to-member bootstrap deadlock.
    pub async fn proof(&self) -> Vec<WriteFrame> {
        self.node.frames_for_entity(MEMBERS_ENTITY).await
    }

    /// Verify a peer's membership proof and decide whether to admit `key`. Each
    /// presented frame must be a `_members` record with a valid signature;
    /// verified frames are absorbed through the LWW engine (so a stale invite
    /// loses to a newer revoke this node already holds), then authorization is
    /// read from the converged state. Admits iff `key` resolves to an authorized
    /// member rooted at the genesis owner.
    pub async fn admits_with_proof(&self, key: PeerId, presented: Vec<WriteFrame>) -> bool {
        let verified: Vec<WriteFrame> = presented
            .into_iter()
            .filter(|frame| frame.entity == MEMBERS_ENTITY && verify_frame(frame))
            .collect();
        if !verified.is_empty() {
            self.node.ingest(verified).await;
        }
        self.is_member(&key).await
    }

    /// A stream that fires whenever the `_members` set changes (an invite or a
    /// revoke, local or synced in), so a host can re-check live peers and drop
    /// those a revoke removed. Subscribe before the first [`Membership::is_member`]
    /// check so a change landing between the two is not missed.
    pub async fn subscribe(&self) -> MembershipChanges {
        MembershipChanges {
            inner: self.node.subscribe().await,
        }
    }
}

/// Receiver of membership-change notifications from [`Membership::subscribe`].
pub struct MembershipChanges {
    inner: tokio::sync::broadcast::Receiver<crate::sync_state::MutationEvent>,
}

impl MembershipChanges {
    /// Wait for the next change to the `_members` set. Mutations to other
    /// entities are skipped. A receiver that lagged past the buffered backlog
    /// returns `Ok(())` so the caller re-checks membership rather than losing
    /// the watch, which may have dropped the revoke it exists to catch.
    ///
    /// # Errors
    ///
    /// When the underlying broadcast channel is closed.
    pub async fn changed(&mut self) -> Result<(), tokio::sync::broadcast::error::RecvError> {
        use tokio::sync::broadcast::error::RecvError;
        loop {
            match self.inner.recv().await {
                Ok(event) if event.entity == MEMBERS_ENTITY => return Ok(()),
                Ok(_) => continue,
                Err(RecvError::Lagged(_)) => return Ok(()),
                Err(RecvError::Closed) => return Err(RecvError::Closed),
            }
        }
    }
}

#[cfg(feature = "discovery")]
impl crate::discovery::MemberGate for Membership {
    fn admits(
        &self,
        key: [u8; 32],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + '_>> {
        Box::pin(async move { self.is_member(&key).await })
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
    use crate::node::SyncNode;
    use crate::sync_state::SyncState;
    use std::sync::Arc;

    fn signing_node(id: Arc<Identity>) -> SyncNode {
        let mut state = SyncState::new(id.peer_id());
        state.set_auth(id);
        SyncNode::from_state(state)
    }

    async fn invite(node: &SyncNode, target: PeerId, role: Role) -> WriteFrame {
        node.local_write(
            Op::Insert,
            MEMBERS_ENTITY,
            member_record_id(&target),
            vec![role.as_byte()],
        )
        .await
        .expect("invite write")
    }

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
        let records = vec![(pid(2), pid(3), Role::Admin), (pid(3), pid(2), Role::Admin)];
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
        let frame = state
            .local_write(1, Op::Insert, "task", "t1", vec![9])
            .expect("write");
        assert!(frame.signature.is_some());
        assert!(verify_frame(&frame));
    }

    #[tokio::test]
    async fn proof_admits_a_member_the_receiver_has_not_synced() {
        let owner = Arc::new(Identity::from_seed([1u8; 32]));
        let member = Arc::new(Identity::from_seed([2u8; 32]));
        let owner_node = signing_node(owner.clone());
        invite(&owner_node, member.peer_id(), Role::Member).await;
        let proof = Membership::new(owner_node, owner.peer_id()).proof().await;
        assert!(!proof.is_empty());

        let receiver_id = Arc::new(Identity::from_seed([3u8; 32]));
        let receiver = Membership::new(signing_node(receiver_id.clone()), owner.peer_id());
        assert!(!receiver.is_member(&member.peer_id()).await);
        assert!(receiver.admits_with_proof(member.peer_id(), proof).await);
        assert!(
            receiver.is_member(&member.peer_id()).await,
            "a verified proof converges the receiver's membership view"
        );
    }

    #[tokio::test]
    async fn an_outsiders_self_granted_proof_is_rejected() {
        let owner = Arc::new(Identity::from_seed([1u8; 32]));
        let outsider = Arc::new(Identity::from_seed([9u8; 32]));
        let outsider_node = signing_node(outsider.clone());
        invite(&outsider_node, outsider.peer_id(), Role::Owner).await;
        let forged = Membership::new(outsider_node, outsider.peer_id())
            .proof()
            .await;

        let receiver_id = Arc::new(Identity::from_seed([3u8; 32]));
        let receiver = Membership::new(signing_node(receiver_id), owner.peer_id());
        assert!(!receiver.admits_with_proof(outsider.peer_id(), vec![]).await);
        assert!(
            !receiver.admits_with_proof(outsider.peer_id(), forged).await,
            "a chain not rooted at the genesis owner must not admit"
        );
    }

    #[tokio::test]
    async fn a_replayed_chain_does_not_admit_a_different_key() {
        let owner = Arc::new(Identity::from_seed([1u8; 32]));
        let member = Arc::new(Identity::from_seed([2u8; 32]));
        let outsider = Arc::new(Identity::from_seed([9u8; 32]));
        let owner_node = signing_node(owner.clone());
        invite(&owner_node, member.peer_id(), Role::Member).await;
        let proof = Membership::new(owner_node, owner.peer_id()).proof().await;

        let receiver_id = Arc::new(Identity::from_seed([3u8; 32]));
        let receiver = Membership::new(signing_node(receiver_id), owner.peer_id());
        assert!(
            !receiver.admits_with_proof(outsider.peer_id(), proof).await,
            "a member's real chain cannot admit an outsider's transport key"
        );
    }

    #[tokio::test]
    async fn a_replayed_invite_cannot_resurrect_a_revoked_member() {
        let owner = Arc::new(Identity::from_seed([1u8; 32]));
        let member = Arc::new(Identity::from_seed([2u8; 32]));
        let owner_node = signing_node(owner.clone());
        let stale_invite = invite(&owner_node, member.peer_id(), Role::Member).await;
        let revoke = owner_node
            .local_write(
                Op::Delete,
                MEMBERS_ENTITY,
                member_record_id(&member.peer_id()),
                vec![],
            )
            .await
            .expect("revoke write");

        let receiver_id = Arc::new(Identity::from_seed([3u8; 32]));
        let receiver = Membership::new(signing_node(receiver_id), owner.peer_id());
        receiver
            .node
            .ingest(vec![stale_invite.clone(), revoke])
            .await;
        assert!(!receiver.is_member(&member.peer_id()).await);
        assert!(
            !receiver
                .admits_with_proof(member.peer_id(), vec![stale_invite])
                .await,
            "replaying an already-delivered invite must not resurrect a revoked member"
        );
    }
}
