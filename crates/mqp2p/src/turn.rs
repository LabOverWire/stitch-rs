use crate::error::{Error, Result};
use md5::{Digest, Md5};
use ring::hmac;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

const MAGIC_COOKIE: u32 = 0x2112_A442;
const HEADER_LEN: usize = 20;
const FINGERPRINT_XOR: u32 = 0x5354_554E;

pub const METHOD_BINDING: u16 = 0x001;
pub const METHOD_ALLOCATE: u16 = 0x003;
pub const METHOD_REFRESH: u16 = 0x004;
pub const METHOD_SEND: u16 = 0x006;
pub const METHOD_DATA: u16 = 0x007;
pub const METHOD_CREATE_PERMISSION: u16 = 0x008;
pub const METHOD_CHANNEL_BIND: u16 = 0x009;

pub const TRANSPORT_UDP: u8 = 17;
pub const CHANNEL_MIN: u16 = 0x4000;
pub const CHANNEL_MAX: u16 = 0x4FFF;

const ATTR_MAPPED_ADDRESS: u16 = 0x0001;
const ATTR_USERNAME: u16 = 0x0006;
const ATTR_MESSAGE_INTEGRITY: u16 = 0x0008;
const ATTR_ERROR_CODE: u16 = 0x0009;
const ATTR_CHANNEL_NUMBER: u16 = 0x000C;
const ATTR_LIFETIME: u16 = 0x000D;
const ATTR_XOR_PEER_ADDRESS: u16 = 0x0012;
const ATTR_DATA: u16 = 0x0013;
const ATTR_REALM: u16 = 0x0014;
const ATTR_NONCE: u16 = 0x0015;
const ATTR_XOR_RELAYED_ADDRESS: u16 = 0x0016;
const ATTR_REQUESTED_TRANSPORT: u16 = 0x0019;
const ATTR_DONT_FRAGMENT: u16 = 0x001A;
const ATTR_XOR_MAPPED_ADDRESS: u16 = 0x0020;
const ATTR_SOFTWARE: u16 = 0x8022;
const ATTR_FINGERPRINT: u16 = 0x8028;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    Request,
    Indication,
    Success,
    Error,
}

impl Class {
    fn bits(self) -> u16 {
        match self {
            Class::Request => 0b00,
            Class::Indication => 0b01,
            Class::Success => 0b10,
            Class::Error => 0b11,
        }
    }

    fn from_bits(bits: u16) -> Self {
        match bits & 0b11 {
            0b00 => Class::Request,
            0b01 => Class::Indication,
            0b10 => Class::Success,
            _ => Class::Error,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Attribute {
    MappedAddress(SocketAddr),
    XorMappedAddress(SocketAddr),
    XorPeerAddress(SocketAddr),
    XorRelayedAddress(SocketAddr),
    Username(String),
    Realm(String),
    Nonce(String),
    Software(String),
    MessageIntegrity([u8; 20]),
    Fingerprint(u32),
    ErrorCode { code: u16, reason: String },
    Lifetime(u32),
    RequestedTransport(u8),
    ChannelNumber(u16),
    Data(Vec<u8>),
    DontFragment,
    Unknown { typ: u16, value: Vec<u8> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub method: u16,
    pub class: Class,
    pub transaction_id: [u8; 12],
    pub attributes: Vec<Attribute>,
}

impl Message {
    pub fn new(method: u16, class: Class, transaction_id: [u8; 12]) -> Self {
        Self {
            method,
            class,
            transaction_id,
            attributes: Vec::new(),
        }
    }

    pub fn push(&mut self, attr: Attribute) -> &mut Self {
        self.attributes.push(attr);
        self
    }

    pub fn error_code(&self) -> Option<u16> {
        self.attributes.iter().find_map(|a| match a {
            Attribute::ErrorCode { code, .. } => Some(*code),
            _ => None,
        })
    }

    pub fn realm(&self) -> Option<&str> {
        self.attributes.iter().find_map(|a| match a {
            Attribute::Realm(r) => Some(r.as_str()),
            _ => None,
        })
    }

    pub fn nonce(&self) -> Option<&str> {
        self.attributes.iter().find_map(|a| match a {
            Attribute::Nonce(n) => Some(n.as_str()),
            _ => None,
        })
    }

    pub fn lifetime(&self) -> Option<u32> {
        self.attributes.iter().find_map(|a| match a {
            Attribute::Lifetime(l) => Some(*l),
            _ => None,
        })
    }

    pub fn xor_relayed_address(&self) -> Option<SocketAddr> {
        self.attributes.iter().find_map(|a| match a {
            Attribute::XorRelayedAddress(addr) => Some(*addr),
            _ => None,
        })
    }

    pub fn xor_mapped_address(&self) -> Option<SocketAddr> {
        self.attributes.iter().find_map(|a| match a {
            Attribute::XorMappedAddress(addr) => Some(*addr),
            _ => None,
        })
    }

    pub fn encode(&self, integrity_key: Option<&[u8]>, fingerprint: bool) -> Vec<u8> {
        let mut buf = Vec::with_capacity(HEADER_LEN + 64);
        buf.extend_from_slice(&message_type(self.method, self.class).to_be_bytes());
        buf.extend_from_slice(&[0, 0]);
        buf.extend_from_slice(&MAGIC_COOKIE.to_be_bytes());
        buf.extend_from_slice(&self.transaction_id);

        for attr in &self.attributes {
            encode_attribute(&mut buf, attr, &self.transaction_id);
        }

        if let Some(key) = integrity_key {
            let through_mi = buf.len() - HEADER_LEN + 24;
            set_length(&mut buf, through_mi);
            let mac = hmac_sha1(key, &buf);
            push_raw_attribute(&mut buf, ATTR_MESSAGE_INTEGRITY, &mac);
        }

        if fingerprint {
            let through_fp = buf.len() - HEADER_LEN + 8;
            set_length(&mut buf, through_fp);
            let crc = crc32fast::hash(&buf) ^ FINGERPRINT_XOR;
            push_raw_attribute(&mut buf, ATTR_FINGERPRINT, &crc.to_be_bytes());
        }

        let total = buf.len() - HEADER_LEN;
        set_length(&mut buf, total);
        buf
    }

    pub fn decode(buf: &[u8]) -> Result<Self> {
        if buf.len() < HEADER_LEN {
            return Err(Error::TurnParse("message shorter than header".into()));
        }

        let raw_type = u16::from_be_bytes([buf[0], buf[1]]);
        if raw_type & 0xC000 != 0 {
            return Err(Error::TurnParse("not a STUN message".into()));
        }

        let cookie = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
        if cookie != MAGIC_COOKIE {
            return Err(Error::TurnParse("bad magic cookie".into()));
        }

        let msg_len = u16::from_be_bytes([buf[2], buf[3]]) as usize;
        if HEADER_LEN + msg_len > buf.len() {
            return Err(Error::TurnParse("declared length exceeds buffer".into()));
        }

        let mut transaction_id = [0u8; 12];
        transaction_id.copy_from_slice(&buf[8..20]);
        let (method, class) = decode_type(raw_type);

        let body = &buf[HEADER_LEN..HEADER_LEN + msg_len];
        let mut attributes = Vec::new();
        let mut offset = 0;
        while offset + 4 <= body.len() {
            let atyp = u16::from_be_bytes([body[offset], body[offset + 1]]);
            let alen = u16::from_be_bytes([body[offset + 2], body[offset + 3]]) as usize;
            offset += 4;
            if offset + alen > body.len() {
                return Err(Error::TurnParse("attribute length exceeds message".into()));
            }
            let value = &body[offset..offset + alen];
            attributes.push(decode_attribute(atyp, value, &transaction_id)?);
            offset += alen + (4 - (alen % 4)) % 4;
        }

        Ok(Message {
            method,
            class,
            transaction_id,
            attributes,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelData {
    pub channel: u16,
    pub data: Vec<u8>,
}

impl ChannelData {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(4 + self.data.len());
        out.extend_from_slice(&self.channel.to_be_bytes());
        out.extend_from_slice(&(self.data.len() as u16).to_be_bytes());
        out.extend_from_slice(&self.data);
        out
    }

    pub fn decode(buf: &[u8]) -> Result<Self> {
        if buf.len() < 4 {
            return Err(Error::TurnParse("channel data shorter than header".into()));
        }
        let channel = u16::from_be_bytes([buf[0], buf[1]]);
        if !(CHANNEL_MIN..=CHANNEL_MAX).contains(&channel) {
            return Err(Error::TurnParse(format!(
                "channel number {channel:#06x} out of range"
            )));
        }
        let len = u16::from_be_bytes([buf[2], buf[3]]) as usize;
        if 4 + len > buf.len() {
            return Err(Error::TurnParse(
                "channel data length exceeds buffer".into(),
            ));
        }
        Ok(ChannelData {
            channel,
            data: buf[4..4 + len].to_vec(),
        })
    }
}

pub fn is_channel_data(buf: &[u8]) -> bool {
    buf.first().is_some_and(|b| b & 0xC0 == 0x40)
}

pub fn long_term_key(username: &str, realm: &str, password: &str) -> [u8; 16] {
    let mut hasher = Md5::new();
    hasher.update(username.as_bytes());
    hasher.update(b":");
    hasher.update(realm.as_bytes());
    hasher.update(b":");
    hasher.update(password.as_bytes());
    hasher.finalize().into()
}

fn message_type(method: u16, class: Class) -> u16 {
    let c = class.bits();
    ((method & 0x0F80) << 2)
        | ((c & 0x02) << 7)
        | ((method & 0x0070) << 1)
        | ((c & 0x01) << 4)
        | (method & 0x000F)
}

fn decode_type(raw: u16) -> (u16, Class) {
    let method = ((raw & 0x3E00) >> 2) | ((raw & 0x00E0) >> 1) | (raw & 0x000F);
    let class = Class::from_bits(((raw & 0x0100) >> 7) | ((raw & 0x0010) >> 4));
    (method, class)
}

fn set_length(buf: &mut [u8], len: usize) {
    let bytes = (len as u16).to_be_bytes();
    buf[2] = bytes[0];
    buf[3] = bytes[1];
}

fn push_raw_attribute(buf: &mut Vec<u8>, typ: u16, value: &[u8]) {
    buf.extend_from_slice(&typ.to_be_bytes());
    buf.extend_from_slice(&(value.len() as u16).to_be_bytes());
    buf.extend_from_slice(value);
    let pad = (4 - (value.len() % 4)) % 4;
    buf.resize(buf.len() + pad, 0);
}

fn encode_attribute(buf: &mut Vec<u8>, attr: &Attribute, txn: &[u8; 12]) {
    match attr {
        Attribute::MappedAddress(addr) => {
            push_raw_attribute(buf, ATTR_MAPPED_ADDRESS, &encode_plain_address(*addr))
        }
        Attribute::XorMappedAddress(addr) => push_raw_attribute(
            buf,
            ATTR_XOR_MAPPED_ADDRESS,
            &encode_xor_address(*addr, txn),
        ),
        Attribute::XorPeerAddress(addr) => {
            push_raw_attribute(buf, ATTR_XOR_PEER_ADDRESS, &encode_xor_address(*addr, txn))
        }
        Attribute::XorRelayedAddress(addr) => push_raw_attribute(
            buf,
            ATTR_XOR_RELAYED_ADDRESS,
            &encode_xor_address(*addr, txn),
        ),
        Attribute::Username(s) => push_raw_attribute(buf, ATTR_USERNAME, s.as_bytes()),
        Attribute::Realm(s) => push_raw_attribute(buf, ATTR_REALM, s.as_bytes()),
        Attribute::Nonce(s) => push_raw_attribute(buf, ATTR_NONCE, s.as_bytes()),
        Attribute::Software(s) => push_raw_attribute(buf, ATTR_SOFTWARE, s.as_bytes()),
        Attribute::MessageIntegrity(mac) => push_raw_attribute(buf, ATTR_MESSAGE_INTEGRITY, mac),
        Attribute::Fingerprint(crc) => {
            push_raw_attribute(buf, ATTR_FINGERPRINT, &crc.to_be_bytes())
        }
        Attribute::ErrorCode { code, reason } => {
            let mut value = Vec::with_capacity(4 + reason.len());
            value.extend_from_slice(&[0, 0, (code / 100) as u8, (code % 100) as u8]);
            value.extend_from_slice(reason.as_bytes());
            push_raw_attribute(buf, ATTR_ERROR_CODE, &value);
        }
        Attribute::Lifetime(seconds) => {
            push_raw_attribute(buf, ATTR_LIFETIME, &seconds.to_be_bytes())
        }
        Attribute::RequestedTransport(proto) => {
            push_raw_attribute(buf, ATTR_REQUESTED_TRANSPORT, &[*proto, 0, 0, 0])
        }
        Attribute::ChannelNumber(number) => {
            let n = number.to_be_bytes();
            push_raw_attribute(buf, ATTR_CHANNEL_NUMBER, &[n[0], n[1], 0, 0]);
        }
        Attribute::Data(data) => push_raw_attribute(buf, ATTR_DATA, data),
        Attribute::DontFragment => push_raw_attribute(buf, ATTR_DONT_FRAGMENT, &[]),
        Attribute::Unknown { typ, value } => push_raw_attribute(buf, *typ, value),
    }
}

fn decode_attribute(typ: u16, value: &[u8], txn: &[u8; 12]) -> Result<Attribute> {
    Ok(match typ {
        ATTR_MAPPED_ADDRESS => Attribute::MappedAddress(decode_plain_address(value)?),
        ATTR_XOR_MAPPED_ADDRESS => Attribute::XorMappedAddress(decode_xor_address(value, txn)?),
        ATTR_XOR_PEER_ADDRESS => Attribute::XorPeerAddress(decode_xor_address(value, txn)?),
        ATTR_XOR_RELAYED_ADDRESS => Attribute::XorRelayedAddress(decode_xor_address(value, txn)?),
        ATTR_USERNAME => Attribute::Username(decode_string(value)?),
        ATTR_REALM => Attribute::Realm(decode_string(value)?),
        ATTR_NONCE => Attribute::Nonce(decode_string(value)?),
        ATTR_SOFTWARE => Attribute::Software(decode_string(value)?),
        ATTR_MESSAGE_INTEGRITY => {
            if value.len() != 20 {
                return Err(Error::TurnParse(
                    "message-integrity must be 20 bytes".into(),
                ));
            }
            let mut mac = [0u8; 20];
            mac.copy_from_slice(value);
            Attribute::MessageIntegrity(mac)
        }
        ATTR_FINGERPRINT => {
            if value.len() != 4 {
                return Err(Error::TurnParse("fingerprint must be 4 bytes".into()));
            }
            Attribute::Fingerprint(u32::from_be_bytes([value[0], value[1], value[2], value[3]]))
        }
        ATTR_ERROR_CODE => {
            if value.len() < 4 {
                return Err(Error::TurnParse("error-code shorter than 4 bytes".into()));
            }
            let code = u16::from(value[2]) * 100 + u16::from(value[3]);
            let reason = String::from_utf8_lossy(&value[4..]).into_owned();
            Attribute::ErrorCode { code, reason }
        }
        ATTR_LIFETIME => {
            if value.len() < 4 {
                return Err(Error::TurnParse("lifetime shorter than 4 bytes".into()));
            }
            Attribute::Lifetime(u32::from_be_bytes([value[0], value[1], value[2], value[3]]))
        }
        ATTR_REQUESTED_TRANSPORT => {
            if value.is_empty() {
                return Err(Error::TurnParse("requested-transport empty".into()));
            }
            Attribute::RequestedTransport(value[0])
        }
        ATTR_CHANNEL_NUMBER => {
            if value.len() < 2 {
                return Err(Error::TurnParse(
                    "channel-number shorter than 2 bytes".into(),
                ));
            }
            Attribute::ChannelNumber(u16::from_be_bytes([value[0], value[1]]))
        }
        ATTR_DATA => Attribute::Data(value.to_vec()),
        ATTR_DONT_FRAGMENT => Attribute::DontFragment,
        other => Attribute::Unknown {
            typ: other,
            value: value.to_vec(),
        },
    })
}

fn decode_string(value: &[u8]) -> Result<String> {
    String::from_utf8(value.to_vec())
        .map_err(|_| Error::TurnParse("attribute is not valid utf-8".into()))
}

fn encode_plain_address(addr: SocketAddr) -> Vec<u8> {
    let mut out = Vec::with_capacity(20);
    out.push(0);
    match addr.ip() {
        IpAddr::V4(v4) => {
            out.push(0x01);
            out.extend_from_slice(&addr.port().to_be_bytes());
            out.extend_from_slice(&v4.octets());
        }
        IpAddr::V6(v6) => {
            out.push(0x02);
            out.extend_from_slice(&addr.port().to_be_bytes());
            out.extend_from_slice(&v6.octets());
        }
    }
    out
}

fn decode_plain_address(value: &[u8]) -> Result<SocketAddr> {
    if value.len() < 8 {
        return Err(Error::TurnParse("address shorter than 8 bytes".into()));
    }
    let port = u16::from_be_bytes([value[2], value[3]]);
    match value[1] {
        0x01 => {
            let ip = Ipv4Addr::new(value[4], value[5], value[6], value[7]);
            Ok(SocketAddr::new(ip.into(), port))
        }
        0x02 => {
            if value.len() < 20 {
                return Err(Error::TurnParse(
                    "ipv6 address shorter than 20 bytes".into(),
                ));
            }
            let mut octets = [0u8; 16];
            octets.copy_from_slice(&value[4..20]);
            Ok(SocketAddr::new(Ipv6Addr::from(octets).into(), port))
        }
        other => Err(Error::TurnParse(format!(
            "unknown address family {other:#04x}"
        ))),
    }
}

fn encode_xor_address(addr: SocketAddr, txn: &[u8; 12]) -> Vec<u8> {
    let mut out = Vec::with_capacity(20);
    out.push(0);
    let xor_port = addr.port() ^ (MAGIC_COOKIE >> 16) as u16;
    match addr.ip() {
        IpAddr::V4(v4) => {
            out.push(0x01);
            out.extend_from_slice(&xor_port.to_be_bytes());
            let xored = u32::from(v4) ^ MAGIC_COOKIE;
            out.extend_from_slice(&xored.to_be_bytes());
        }
        IpAddr::V6(v6) => {
            out.push(0x02);
            out.extend_from_slice(&xor_port.to_be_bytes());
            let mut octets = v6.octets();
            xor_with_key(&mut octets, txn);
            out.extend_from_slice(&octets);
        }
    }
    out
}

fn decode_xor_address(value: &[u8], txn: &[u8; 12]) -> Result<SocketAddr> {
    if value.len() < 8 {
        return Err(Error::TurnParse("xor address shorter than 8 bytes".into()));
    }
    let port = u16::from_be_bytes([value[2], value[3]]) ^ (MAGIC_COOKIE >> 16) as u16;
    match value[1] {
        0x01 => {
            let xored = u32::from_be_bytes([value[4], value[5], value[6], value[7]]);
            let ip = Ipv4Addr::from(xored ^ MAGIC_COOKIE);
            Ok(SocketAddr::new(ip.into(), port))
        }
        0x02 => {
            if value.len() < 20 {
                return Err(Error::TurnParse(
                    "xor ipv6 address shorter than 20 bytes".into(),
                ));
            }
            let mut octets = [0u8; 16];
            octets.copy_from_slice(&value[4..20]);
            xor_with_key(&mut octets, txn);
            Ok(SocketAddr::new(Ipv6Addr::from(octets).into(), port))
        }
        other => Err(Error::TurnParse(format!(
            "unknown address family {other:#04x}"
        ))),
    }
}

fn xor_with_key(octets: &mut [u8; 16], txn: &[u8; 12]) {
    let mut key = [0u8; 16];
    key[0..4].copy_from_slice(&MAGIC_COOKIE.to_be_bytes());
    key[4..16].copy_from_slice(txn);
    octets.iter_mut().zip(key.iter()).for_each(|(o, k)| *o ^= k);
}

fn hmac_sha1(key: &[u8], msg: &[u8]) -> [u8; 20] {
    let signing_key = hmac::Key::new(hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY, key);
    let tag = hmac::sign(&signing_key, msg);
    let mut out = [0u8; 20];
    out.copy_from_slice(tag.as_ref());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const RFC5769_PASSWORD: &str = "VOkJxbRl1RmTxUk/WvJxBt";

    fn rfc5769_sample_request() -> Vec<u8> {
        vec![
            0x00, 0x01, 0x00, 0x58, 0x21, 0x12, 0xa4, 0x42, 0xb7, 0xe7, 0xa7, 0x01, 0xbc, 0x34,
            0xd6, 0x86, 0xfa, 0x87, 0xdf, 0xae, 0x80, 0x22, 0x00, 0x10, 0x53, 0x54, 0x55, 0x4e,
            0x20, 0x74, 0x65, 0x73, 0x74, 0x20, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x00, 0x24,
            0x00, 0x04, 0x6e, 0x00, 0x01, 0xff, 0x80, 0x29, 0x00, 0x08, 0x93, 0x2f, 0xf9, 0xb1,
            0x51, 0x26, 0x3b, 0x36, 0x00, 0x06, 0x00, 0x09, 0x65, 0x76, 0x74, 0x6a, 0x3a, 0x68,
            0x36, 0x76, 0x59, 0x20, 0x20, 0x20, 0x00, 0x08, 0x00, 0x14, 0x9a, 0xea, 0xa7, 0x0c,
            0xbf, 0xd8, 0xcb, 0x56, 0x78, 0x1e, 0xf2, 0xb5, 0xb2, 0xd3, 0xf2, 0x49, 0xc1, 0xb5,
            0x71, 0xa2, 0x80, 0x28, 0x00, 0x04, 0xe5, 0x7a, 0x3b, 0xcf,
        ]
    }

    #[test]
    fn message_type_round_trips_for_each_class() {
        for method in [METHOD_BINDING, METHOD_ALLOCATE, METHOD_CHANNEL_BIND] {
            for class in [
                Class::Request,
                Class::Indication,
                Class::Success,
                Class::Error,
            ] {
                let raw = message_type(method, class);
                assert_eq!(decode_type(raw), (method, class));
            }
        }
    }

    #[test]
    fn allocate_request_is_0x0003() {
        assert_eq!(message_type(METHOD_ALLOCATE, Class::Request), 0x0003);
        assert_eq!(message_type(METHOD_BINDING, Class::Success), 0x0101);
    }

    #[test]
    fn the_rfc5769_sample_request_decodes_into_its_attributes() {
        let msg = Message::decode(&rfc5769_sample_request()).unwrap();
        assert_eq!(msg.method, METHOD_BINDING);
        assert_eq!(msg.class, Class::Request);
        assert_eq!(
            msg.transaction_id,
            [
                0xb7, 0xe7, 0xa7, 0x01, 0xbc, 0x34, 0xd6, 0x86, 0xfa, 0x87, 0xdf, 0xae
            ]
        );
        assert!(matches!(
            &msg.attributes[0],
            Attribute::Software(s) if s == "STUN test client"
        ));
        assert!(matches!(
            &msg.attributes[3],
            Attribute::Username(u) if u == "evtj:h6vY"
        ));
        assert!(matches!(&msg.attributes[4], Attribute::MessageIntegrity(_)));
        assert!(matches!(
            msg.attributes[5],
            Attribute::Fingerprint(0xe57a3bcf)
        ));
    }

    #[test]
    fn message_integrity_matches_the_rfc5769_vector() {
        let packet = rfc5769_sample_request();
        let mut prefix = packet[..76].to_vec();
        set_length(&mut prefix, 76 - HEADER_LEN + 24);
        let mac = hmac_sha1(RFC5769_PASSWORD.as_bytes(), &prefix);
        assert_eq!(&mac, &packet[80..100]);
    }

    #[test]
    fn fingerprint_matches_the_rfc5769_vector() {
        let packet = rfc5769_sample_request();
        let crc = crc32fast::hash(&packet[..100]) ^ FINGERPRINT_XOR;
        assert_eq!(crc.to_be_bytes(), [0xe5, 0x7a, 0x3b, 0xcf]);
    }

    #[test]
    fn encoding_then_decoding_preserves_attributes() {
        let mut msg = Message::new(METHOD_ALLOCATE, Class::Request, [7u8; 12]);
        msg.push(Attribute::RequestedTransport(TRANSPORT_UDP))
            .push(Attribute::Username("device-a".into()))
            .push(Attribute::Realm("laboverwire".into()))
            .push(Attribute::Nonce("abc123".into()))
            .push(Attribute::Lifetime(600));

        let key = long_term_key("device-a", "laboverwire", "secret");
        let bytes = msg.encode(Some(&key), true);
        let decoded = Message::decode(&bytes).unwrap();

        assert_eq!(decoded.method, METHOD_ALLOCATE);
        assert_eq!(decoded.class, Class::Request);
        assert!(decoded.attributes.contains(&Attribute::Lifetime(600)));
        assert!(
            decoded
                .attributes
                .contains(&Attribute::RequestedTransport(TRANSPORT_UDP))
        );
        assert!(matches!(
            decoded.attributes[5],
            Attribute::MessageIntegrity(_)
        ));
        assert!(matches!(decoded.attributes[6], Attribute::Fingerprint(_)));
    }

    #[test]
    fn our_own_message_integrity_verifies_over_the_encoded_bytes() {
        let mut msg = Message::new(METHOD_REFRESH, Class::Request, [3u8; 12]);
        msg.push(Attribute::Lifetime(0));
        let key = long_term_key("u", "r", "p");
        let bytes = msg.encode(Some(&key), false);

        let mi_start = bytes.len() - 24;
        let mut prefix = bytes[..mi_start].to_vec();
        set_length(&mut prefix, mi_start - HEADER_LEN + 24);
        let expected = hmac_sha1(&key, &prefix);
        assert_eq!(&expected, &bytes[mi_start + 4..]);
    }

    #[test]
    fn xor_mapped_address_round_trips_ipv4_and_ipv6() {
        let txn = [0x11u8; 12];
        for addr in [
            "203.0.113.5:8080".parse::<SocketAddr>().unwrap(),
            "[2001:db8::1]:9000".parse::<SocketAddr>().unwrap(),
        ] {
            let encoded = encode_xor_address(addr, &txn);
            assert_eq!(decode_xor_address(&encoded, &txn).unwrap(), addr);
        }
    }

    #[test]
    fn channel_data_round_trips() {
        let frame = ChannelData {
            channel: 0x4001,
            data: vec![1, 2, 3, 4, 5],
        };
        let encoded = frame.encode();
        assert!(is_channel_data(&encoded));
        assert_eq!(ChannelData::decode(&encoded).unwrap(), frame);
    }

    #[test]
    fn channel_data_rejects_out_of_range_channels() {
        let mut encoded = ChannelData {
            channel: 0x4001,
            data: vec![9],
        }
        .encode();
        encoded[0] = 0x30;
        assert!(ChannelData::decode(&encoded).is_err());
    }

    #[test]
    fn a_stun_message_is_not_mistaken_for_channel_data() {
        let bytes = Message::new(METHOD_ALLOCATE, Class::Request, [0u8; 12]).encode(None, false);
        assert!(!is_channel_data(&bytes));
    }

    #[test]
    fn decode_rejects_a_bad_magic_cookie() {
        let mut bytes = Message::new(METHOD_BINDING, Class::Request, [0u8; 12]).encode(None, false);
        bytes[4] = 0;
        assert!(Message::decode(&bytes).is_err());
    }
}
