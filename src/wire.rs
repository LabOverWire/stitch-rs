use crate::hlc::{Hlc, PEER_ID_LEN, PeerId, Stamp};
use crate::lww::{Op, StampedWrite};

pub const WIRE_VERSION: u8 = 2;
pub const HEADER_LEN: usize = 60;
pub const SIGNATURE_LEN: usize = 64;

const OP_INSERT: u8 = 0;
const OP_UPDATE: u8 = 1;
const OP_DELETE: u8 = 2;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum WireError {
    #[error("frame truncated: need {need} bytes, have {have}")]
    Truncated { need: usize, have: usize },
    #[error("unsupported wire version {0}")]
    BadVersion(u8),
    #[error("unknown op discriminant {0}")]
    BadOp(u8),
    #[error("entity name exceeds 255 bytes")]
    EntityTooLong,
    #[error("id exceeds 255 bytes")]
    IdTooLong,
    #[error("entity or id is not valid UTF-8")]
    Utf8,
    #[error("invalid signature presence flag {0}")]
    BadSignatureFlag(u8),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteFrame {
    pub stamp: Stamp,
    pub seq: u64,
    pub op: Op,
    pub entity: String,
    pub id: String,
    pub data: Vec<u8>,
    /// Ed25519 signature over [`WriteFrame::signing_bytes`] by the author
    /// (`stamp.peer` is the author's public key). `None` for unsigned frames.
    pub signature: Option<[u8; SIGNATURE_LEN]>,
}

impl WriteFrame {
    #[must_use]
    pub fn into_stamped(self) -> StampedWrite {
        StampedWrite {
            stamp: self.stamp,
            op: self.op,
            entity: self.entity,
            id: self.id,
            data: self.data,
        }
    }

    /// The bytes a signature covers: everything but the signature trailer.
    /// Stable across signed/unsigned framing so a signature verifies regardless
    /// of how the frame is later re-encoded.
    pub fn signing_bytes(&self) -> Result<Vec<u8>, WireError> {
        let entity_bytes = self.entity.as_bytes();
        let id_bytes = self.id.as_bytes();
        let entity_len = u8::try_from(entity_bytes.len()).map_err(|_| WireError::EntityTooLong)?;
        let id_len = u8::try_from(id_bytes.len()).map_err(|_| WireError::IdTooLong)?;
        let data_len = u32::try_from(self.data.len()).expect("payload exceeds u32");

        let mut out =
            Vec::with_capacity(HEADER_LEN + entity_bytes.len() + id_bytes.len() + self.data.len());
        out.push(WIRE_VERSION);
        out.push(op_to_u8(self.op));
        out.extend_from_slice(&self.stamp.hlc.physical.to_be_bytes());
        out.extend_from_slice(&self.stamp.hlc.logical.to_be_bytes());
        out.extend_from_slice(&self.stamp.peer);
        out.extend_from_slice(&self.seq.to_be_bytes());
        out.push(entity_len);
        out.push(id_len);
        out.extend_from_slice(&data_len.to_be_bytes());
        out.extend_from_slice(entity_bytes);
        out.extend_from_slice(id_bytes);
        out.extend_from_slice(&self.data);
        Ok(out)
    }

    pub fn encode(&self) -> Result<Vec<u8>, WireError> {
        let mut out = self.signing_bytes()?;
        match &self.signature {
            Some(sig) => {
                out.push(1);
                out.extend_from_slice(sig);
            }
            None => out.push(0),
        }
        Ok(out)
    }

    pub fn decode(buf: &[u8]) -> Result<Self, WireError> {
        if buf.len() < HEADER_LEN {
            return Err(WireError::Truncated {
                need: HEADER_LEN,
                have: buf.len(),
            });
        }
        let version = buf[0];
        if version != WIRE_VERSION {
            return Err(WireError::BadVersion(version));
        }
        let op = u8_to_op(buf[1])?;
        let physical = u64::from_be_bytes(buf[2..10].try_into().expect("8 bytes"));
        let logical = u32::from_be_bytes(buf[10..14].try_into().expect("4 bytes"));
        let mut peer: PeerId = [0u8; PEER_ID_LEN];
        peer.copy_from_slice(&buf[14..46]);
        let seq = u64::from_be_bytes(buf[46..54].try_into().expect("8 bytes"));
        let entity_len = buf[54] as usize;
        let id_len = buf[55] as usize;
        let data_len = u32::from_be_bytes(buf[56..60].try_into().expect("4 bytes")) as usize;

        let entity_end = HEADER_LEN + entity_len;
        let id_end = entity_end + id_len;
        let data_end = id_end + data_len;
        let flag_end = data_end + 1;
        if buf.len() < flag_end {
            return Err(WireError::Truncated {
                need: flag_end,
                have: buf.len(),
            });
        }

        let entity = std::str::from_utf8(&buf[HEADER_LEN..entity_end])
            .map_err(|_| WireError::Utf8)?
            .to_string();
        let id = std::str::from_utf8(&buf[entity_end..id_end])
            .map_err(|_| WireError::Utf8)?
            .to_string();
        let data = buf[id_end..data_end].to_vec();

        let signature = match buf[data_end] {
            0 => None,
            1 => {
                let sig_end = flag_end + SIGNATURE_LEN;
                if buf.len() < sig_end {
                    return Err(WireError::Truncated {
                        need: sig_end,
                        have: buf.len(),
                    });
                }
                let mut sig = [0u8; SIGNATURE_LEN];
                sig.copy_from_slice(&buf[flag_end..sig_end]);
                Some(sig)
            }
            other => return Err(WireError::BadSignatureFlag(other)),
        };

        Ok(Self {
            stamp: Stamp::new(Hlc::new(physical, logical), peer),
            seq,
            op,
            entity,
            id,
            data,
            signature,
        })
    }

    #[must_use]
    pub fn encoded_len(&self) -> usize {
        HEADER_LEN
            + self.entity.len()
            + self.id.len()
            + self.data.len()
            + 1
            + if self.signature.is_some() {
                SIGNATURE_LEN
            } else {
                0
            }
    }
}

fn op_to_u8(op: Op) -> u8 {
    match op {
        Op::Insert => OP_INSERT,
        Op::Update => OP_UPDATE,
        Op::Delete => OP_DELETE,
    }
}

fn u8_to_op(byte: u8) -> Result<Op, WireError> {
    match byte {
        OP_INSERT => Ok(Op::Insert),
        OP_UPDATE => Ok(Op::Update),
        OP_DELETE => Ok(Op::Delete),
        other => Err(WireError::BadOp(other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(n: u8) -> PeerId {
        let mut id = [0u8; PEER_ID_LEN];
        id[0] = n;
        id[31] = n;
        id
    }

    fn frame() -> WriteFrame {
        WriteFrame {
            stamp: Stamp::new(Hlc::new(1_700_000_000_123, 7), peer(42)),
            seq: 99,
            op: Op::Update,
            entity: "task".into(),
            id: "t-uuid-1".into(),
            data: br#"{"title":"hello"}"#.to_vec(),
            signature: None,
        }
    }

    #[test]
    fn signed_frame_round_trips() {
        let mut f = frame();
        f.signature = Some([7u8; SIGNATURE_LEN]);
        let bytes = f.encode().unwrap();
        assert_eq!(bytes.len(), f.encoded_len());
        let back = WriteFrame::decode(&bytes).unwrap();
        assert_eq!(back, f);
        assert_eq!(back.signature, Some([7u8; SIGNATURE_LEN]));
    }

    #[test]
    fn signing_bytes_independent_of_signature() {
        let unsigned = frame();
        let mut signed = frame();
        signed.signature = Some([9u8; SIGNATURE_LEN]);
        assert_eq!(
            unsigned.signing_bytes().unwrap(),
            signed.signing_bytes().unwrap()
        );
    }

    #[test]
    fn round_trip_preserves_all_fields() {
        let f = frame();
        let bytes = f.encode().unwrap();
        assert_eq!(bytes.len(), f.encoded_len());
        let back = WriteFrame::decode(&bytes).unwrap();
        assert_eq!(back, f);
    }

    #[test]
    fn delete_frame_round_trips_with_empty_data() {
        let mut f = frame();
        f.op = Op::Delete;
        f.data.clear();
        let bytes = f.encode().unwrap();
        let back = WriteFrame::decode(&bytes).unwrap();
        assert_eq!(back, f);
        assert!(back.data.is_empty());
    }

    #[test]
    fn truncated_header_is_rejected() {
        let bytes = frame().encode().unwrap();
        let err = WriteFrame::decode(&bytes[..HEADER_LEN - 1]).unwrap_err();
        assert!(matches!(err, WireError::Truncated { .. }));
    }

    #[test]
    fn truncated_payload_is_rejected() {
        let bytes = frame().encode().unwrap();
        let err = WriteFrame::decode(&bytes[..bytes.len() - 3]).unwrap_err();
        assert!(matches!(err, WireError::Truncated { .. }));
    }

    #[test]
    fn bad_version_is_rejected() {
        let mut bytes = frame().encode().unwrap();
        bytes[0] = 99;
        assert_eq!(WriteFrame::decode(&bytes), Err(WireError::BadVersion(99)));
    }

    #[test]
    fn bad_op_is_rejected() {
        let mut bytes = frame().encode().unwrap();
        bytes[1] = 7;
        assert_eq!(WriteFrame::decode(&bytes), Err(WireError::BadOp(7)));
    }

    #[test]
    fn frame_converts_to_stamped_write() {
        let f = frame();
        let stamp = f.stamp;
        let stamped = f.into_stamped();
        assert_eq!(stamped.stamp, stamp);
        assert_eq!(stamped.entity, "task");
    }
}
