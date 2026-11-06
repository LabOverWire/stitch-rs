//! Short-code device pairing over SPAKE2, bound to both peers' transport keys.
//!
//! The owner mints a [`PairingOffer`] with a one-time [`PairingCode`] and reads
//! it to the user; the new device takes the code and runs the exchange. Both
//! sides run SPAKE2 with the code as the password and confirm with an HMAC over
//! a transcript that pins both peers' transport-authenticated Ed25519 keys, so a
//! relay that terminates each leg sees a different key and cannot complete the
//! exchange. On success the owner learns the device's verified [`PeerId`] (ready
//! for [`crate::membership::Membership`] invite) and the device learns the
//! owner's.
//!
//! The core does no networking: each step returns a [`PairingMessage`] the
//! application carries on its own channel. The transport-authenticated peer key
//! (`mqp2p::P2pConnection::remote_public_key`) is supplied as a non-optional
//! value; the caller aborts when it is absent, so no path skips the binding.
//!
//! Flights (device confirms first, so the owner never emits a verifier to an
//! unproven peer):
//! 1. device → owner: SPAKE2 message B
//! 2. owner → device: SPAKE2 message A
//! 3. device → owner: device confirmation MAC
//! 4. owner → device: owner confirmation MAC

use crate::hlc::PeerId;
use crate::membership::Identity;
use hkdf::Hkdf;
use hmac::{Hmac, Mac};
use rand_core::{OsRng, RngCore};
use sha2::Sha256;
use spake2::{Ed25519Group, Identity as SpakeIdentity, Password, Spake2};
use zeroize::Zeroizing;

const DOMAIN: &[u8] = b"stitch-p2p/pairing/v1";
const CONFIRM_OWNER: &[u8] = b"confirm:owner";
const CONFIRM_DEVICE: &[u8] = b"confirm:device";
const CODE_DIGITS: usize = 6;
const CODE_MODULUS: u32 = 1_000_000;
const CODE_REJECT_AT: u32 = 4_294_000_000;
const OFFER_TTL_MS: u64 = 300_000;
const CONFIRM_KEY_LEN: usize = 32;

type ConfirmKey = Zeroizing<[u8; CONFIRM_KEY_LEN]>;

/// A pairing failure. All code/confirmation mismatches collapse to
/// [`PairingError::ConfirmationFailed`] so an online attacker learns only that
/// its single guess was wrong.
#[derive(Debug, thiserror::Error)]
pub enum PairingError {
    /// The offer's time-to-live elapsed before the device responded.
    #[error("pairing code expired")]
    Expired,
    /// The peer's confirmation MAC did not verify (wrong code, relayed
    /// connection, or key mismatch).
    #[error("pairing confirmation failed")]
    ConfirmationFailed,
    /// A SPAKE2 message could not be parsed.
    #[error("malformed pairing message")]
    Malformed,
    /// A supplied code was not exactly six decimal digits.
    #[error("invalid pairing code")]
    InvalidCode,
    /// A key-derivation or MAC primitive failed unexpectedly.
    #[error("internal pairing error")]
    Internal,
}

/// An opaque message one side hands the other over its own channel.
#[derive(Clone, Debug)]
pub struct PairingMessage(Vec<u8>);

impl PairingMessage {
    /// The message bytes to transmit.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Reconstruct a message from received bytes.
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
}

/// A one-time six-digit pairing code. Its `Debug` is redacted and its contents
/// are zeroized on drop.
pub struct PairingCode(Zeroizing<String>);

impl PairingCode {
    fn generate() -> Self {
        let value = loop {
            let candidate = OsRng.next_u32();
            if candidate < CODE_REJECT_AT {
                break candidate % CODE_MODULUS;
            }
        };
        Self(Zeroizing::new(format!("{value:06}")))
    }

    /// Parse a code a user entered on the device. Requires exactly six decimal
    /// digits.
    pub fn parse(input: &str) -> Result<Self, PairingError> {
        if input.len() == CODE_DIGITS && input.bytes().all(|b| b.is_ascii_digit()) {
            Ok(Self(Zeroizing::new(input.to_string())))
        } else {
            Err(PairingError::InvalidCode)
        }
    }

    /// The code as a string, for the owner to display.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl Clone for PairingCode {
    fn clone(&self) -> Self {
        Self(Zeroizing::new(self.0.to_string()))
    }
}

impl core::fmt::Debug for PairingCode {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("PairingCode(******)")
    }
}

/// The owner's starting state: a minted code bound to an expiry.
pub struct PairingOffer {
    code: PairingCode,
    expires_at_ms: u64,
}

impl PairingOffer {
    /// Mint a new offer valid for five minutes from `now_ms`, returning the
    /// offer and the code to show the user.
    pub fn new(now_ms: u64) -> (PairingOffer, PairingCode) {
        let code = PairingCode::generate();
        let offer = PairingOffer {
            code: code.clone(),
            expires_at_ms: now_ms.saturating_add(OFFER_TTL_MS),
        };
        (offer, code)
    }

    /// Consume the offer with the device's first message, the owner's own
    /// identity, and the device's transport-authenticated key. Runs SPAKE2 as
    /// party A and returns the owner's message to send plus the confirming
    /// state. The offer is consumed on every outcome, so a code is spent by its
    /// first attempt.
    pub fn accept(
        self,
        owner: &Identity,
        device_key: PeerId,
        device_hello: &PairingMessage,
        now_ms: u64,
    ) -> Result<(OwnerConfirm, PairingMessage), PairingError> {
        if now_ms > self.expires_at_ms {
            return Err(PairingError::Expired);
        }
        let owner_key = owner.peer_id();
        let (state, message_a) = Spake2::<Ed25519Group>::start_a(
            &Password::new(self.code.as_bytes()),
            &SpakeIdentity::new(owner_key.as_slice()),
            &SpakeIdentity::new(device_key.as_slice()),
        );
        let secret = Zeroizing::new(
            state
                .finish(device_hello.as_bytes())
                .map_err(|_| PairingError::Malformed)?,
        );
        let (key_owner, key_device) = derive_confirm_keys(&secret)?;
        let transcript = transcript(&owner_key, &device_key, &message_a, device_hello.as_bytes());
        Ok((
            OwnerConfirm {
                key_owner,
                key_device,
                transcript,
                device_key,
            },
            PairingMessage(message_a),
        ))
    }
}

/// The owner awaiting the device's confirmation.
pub struct OwnerConfirm {
    key_owner: ConfirmKey,
    key_device: ConfirmKey,
    transcript: Vec<u8>,
    device_key: PeerId,
}

impl OwnerConfirm {
    /// Verify the device's confirmation, then return the device's verified
    /// `PeerId` and the owner's confirmation to send. The returned id is the
    /// transport key passed to [`PairingOffer::accept`], never a value read from
    /// the wire.
    pub fn confirm(
        self,
        device_confirm: &PairingMessage,
    ) -> Result<(PeerId, PairingMessage), PairingError> {
        verify_mac(
            &self.key_device,
            &self.transcript,
            device_confirm.as_bytes(),
        )?;
        let tag = compute_mac(&self.key_owner, &self.transcript)?;
        Ok((self.device_key, PairingMessage(tag.to_vec())))
    }
}

/// The device awaiting the owner's SPAKE2 message.
pub struct PairingDevice {
    state: Spake2<Ed25519Group>,
    owner_key: PeerId,
    device_key: PeerId,
    message_b: Vec<u8>,
}

impl PairingDevice {
    /// Begin pairing with the device's own identity, the owner's
    /// transport-authenticated key, and the code the user entered. Runs SPAKE2
    /// as party B and returns the device's first message to send.
    pub fn begin(
        device: &Identity,
        owner_key: PeerId,
        code: &PairingCode,
    ) -> (PairingDevice, PairingMessage) {
        let device_key = device.peer_id();
        let (state, message_b) = Spake2::<Ed25519Group>::start_b(
            &Password::new(code.as_bytes()),
            &SpakeIdentity::new(owner_key.as_slice()),
            &SpakeIdentity::new(device_key.as_slice()),
        );
        (
            PairingDevice {
                state,
                owner_key,
                device_key,
                message_b: message_b.clone(),
            },
            PairingMessage(message_b),
        )
    }

    /// Consume the owner's message, finish SPAKE2, and return the device's
    /// confirmation to send plus the state awaiting the owner's confirmation.
    pub fn receive(
        self,
        owner_reply: &PairingMessage,
    ) -> Result<(DeviceConfirm, PairingMessage), PairingError> {
        let message_a = owner_reply.as_bytes();
        let secret = Zeroizing::new(
            self.state
                .finish(message_a)
                .map_err(|_| PairingError::Malformed)?,
        );
        let (key_owner, key_device) = derive_confirm_keys(&secret)?;
        let transcript = transcript(
            &self.owner_key,
            &self.device_key,
            message_a,
            &self.message_b,
        );
        let tag = compute_mac(&key_device, &transcript)?;
        Ok((
            DeviceConfirm {
                key_owner,
                transcript,
                owner_key: self.owner_key,
            },
            PairingMessage(tag.to_vec()),
        ))
    }
}

/// The device awaiting the owner's confirmation.
pub struct DeviceConfirm {
    key_owner: ConfirmKey,
    transcript: Vec<u8>,
    owner_key: PeerId,
}

impl DeviceConfirm {
    /// Verify the owner's confirmation and return the owner's verified `PeerId`
    /// to pin. The returned id is the transport key passed to
    /// [`PairingDevice::begin`], never a value read from the wire.
    pub fn finish(self, owner_confirm: &PairingMessage) -> Result<PeerId, PairingError> {
        verify_mac(&self.key_owner, &self.transcript, owner_confirm.as_bytes())?;
        Ok(self.owner_key)
    }
}

fn transcript(
    owner_key: &PeerId,
    device_key: &PeerId,
    message_a: &[u8],
    message_b: &[u8],
) -> Vec<u8> {
    let mut out = Vec::with_capacity(
        DOMAIN.len() + owner_key.len() + device_key.len() + message_a.len() + message_b.len() + 16,
    );
    out.extend_from_slice(DOMAIN);
    for part in [
        owner_key.as_slice(),
        device_key.as_slice(),
        message_a,
        message_b,
    ] {
        out.extend_from_slice(&(part.len() as u32).to_be_bytes());
        out.extend_from_slice(part);
    }
    out
}

fn derive_confirm_keys(secret: &[u8]) -> Result<(ConfirmKey, ConfirmKey), PairingError> {
    let hkdf = Hkdf::<Sha256>::new(Some(DOMAIN), secret);
    let mut key_owner: ConfirmKey = Zeroizing::new([0u8; CONFIRM_KEY_LEN]);
    let mut key_device: ConfirmKey = Zeroizing::new([0u8; CONFIRM_KEY_LEN]);
    hkdf.expand(CONFIRM_OWNER, key_owner.as_mut())
        .map_err(|_| PairingError::Internal)?;
    hkdf.expand(CONFIRM_DEVICE, key_device.as_mut())
        .map_err(|_| PairingError::Internal)?;
    Ok((key_owner, key_device))
}

fn compute_mac(
    key: &[u8; CONFIRM_KEY_LEN],
    transcript: &[u8],
) -> Result<[u8; CONFIRM_KEY_LEN], PairingError> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).map_err(|_| PairingError::Internal)?;
    mac.update(transcript);
    Ok(mac.finalize().into_bytes().into())
}

fn verify_mac(
    key: &[u8; CONFIRM_KEY_LEN],
    transcript: &[u8],
    tag: &[u8],
) -> Result<(), PairingError> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).map_err(|_| PairingError::Internal)?;
    mac.update(transcript);
    mac.verify_slice(tag)
        .map_err(|_| PairingError::ConfirmationFailed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(
        owner: &Identity,
        device: &Identity,
        owner_view_of_device: PeerId,
        device_view_of_owner: PeerId,
        device_code: &PairingCode,
        offer_code: PairingCode,
    ) -> Result<(PeerId, PeerId), PairingError> {
        let offer = PairingOffer {
            code: offer_code,
            expires_at_ms: OFFER_TTL_MS,
        };
        let (device_state, hello) = PairingDevice::begin(device, device_view_of_owner, device_code);
        let (owner_state, reply) = offer.accept(owner, owner_view_of_device, &hello, 0)?;
        let (device_confirm, device_tag) = device_state.receive(&reply)?;
        let (device_peer_id, owner_tag) = owner_state.confirm(&device_tag)?;
        let owner_peer_id = device_confirm.finish(&owner_tag)?;
        Ok((device_peer_id, owner_peer_id))
    }

    #[test]
    fn matching_code_pairs_and_yields_transport_keys() {
        let owner = Identity::from_seed([1u8; 32]);
        let device = Identity::from_seed([2u8; 32]);
        let (offer, code) = PairingOffer::new(0);
        let (device_peer_id, owner_peer_id) = run(
            &owner,
            &device,
            device.peer_id(),
            owner.peer_id(),
            &code,
            offer.code,
        )
        .expect("pairing succeeds");
        assert_eq!(device_peer_id, device.peer_id());
        assert_eq!(owner_peer_id, owner.peer_id());
    }

    #[test]
    fn wrong_code_fails_confirmation() {
        let owner = Identity::from_seed([1u8; 32]);
        let device = Identity::from_seed([2u8; 32]);
        let (offer, _code) = PairingOffer::new(0);
        let wrong = PairingCode::parse("000000").expect("valid digits");
        let result = run(
            &owner,
            &device,
            device.peer_id(),
            owner.peer_id(),
            &wrong,
            offer.code,
        );
        assert!(matches!(result, Err(PairingError::ConfirmationFailed)));
    }

    #[test]
    fn mismatched_transport_key_fails_confirmation() {
        let owner = Identity::from_seed([1u8; 32]);
        let device = Identity::from_seed([2u8; 32]);
        let relay = Identity::from_seed([9u8; 32]);
        let (offer, code) = PairingOffer::new(0);
        let result = run(
            &owner,
            &device,
            relay.peer_id(),
            owner.peer_id(),
            &code,
            offer.code,
        );
        assert!(matches!(result, Err(PairingError::ConfirmationFailed)));
    }

    #[test]
    fn expired_offer_is_rejected() {
        let owner = Identity::from_seed([1u8; 32]);
        let device = Identity::from_seed([2u8; 32]);
        let (offer, code) = PairingOffer::new(0);
        let (_device_state, hello) = PairingDevice::begin(&device, owner.peer_id(), &code);
        let result = offer.accept(&owner, device.peer_id(), &hello, OFFER_TTL_MS + 1);
        assert!(matches!(result, Err(PairingError::Expired)));
    }

    #[test]
    fn reflected_device_mac_is_rejected() {
        let owner = Identity::from_seed([1u8; 32]);
        let device = Identity::from_seed([2u8; 32]);
        let (offer, code) = PairingOffer::new(0);
        let (device_state, hello) = PairingDevice::begin(&device, owner.peer_id(), &code);
        let (owner_state, reply) = offer
            .accept(&owner, device.peer_id(), &hello, 0)
            .expect("accept");
        let (device_confirm, device_tag) = device_state.receive(&reply).expect("receive");
        owner_state.confirm(&device_tag).expect("confirm");
        assert!(matches!(
            device_confirm.finish(&device_tag),
            Err(PairingError::ConfirmationFailed)
        ));
    }

    #[test]
    fn malformed_message_is_rejected() {
        let owner = Identity::from_seed([1u8; 32]);
        let device = Identity::from_seed([2u8; 32]);
        let (offer, _code) = PairingOffer::new(0);
        let garbage = PairingMessage::from_bytes(vec![0u8; 8]);
        let result = offer.accept(&owner, device.peer_id(), &garbage, 0);
        assert!(matches!(result, Err(PairingError::Malformed)));
    }

    #[test]
    fn code_parse_rejects_non_digits() {
        assert!(matches!(
            PairingCode::parse("12345"),
            Err(PairingError::InvalidCode)
        ));
        assert!(matches!(
            PairingCode::parse("1234567"),
            Err(PairingError::InvalidCode)
        ));
        assert!(matches!(
            PairingCode::parse("12a456"),
            Err(PairingError::InvalidCode)
        ));
        assert!(PairingCode::parse("012345").is_ok());
    }

    #[test]
    fn generated_code_is_six_digits_and_redacted() {
        let (_offer, code) = PairingOffer::new(0);
        assert_eq!(code.as_str().len(), CODE_DIGITS);
        assert!(code.as_str().bytes().all(|b| b.is_ascii_digit()));
        assert_eq!(format!("{code:?}"), "PairingCode(******)");
    }
}
