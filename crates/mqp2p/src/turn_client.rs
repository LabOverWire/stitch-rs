use crate::error::{Error, Result};
use crate::turn::{
    self, Attribute, CHANNEL_MIN, ChannelData, Class, METHOD_ALLOCATE, METHOD_CHANNEL_BIND,
    METHOD_REFRESH, Message, TRANSPORT_UDP,
};
use md5::{Digest, Md5};
use quinn::udp::{RecvMeta, Transmit};
use quinn::{AsyncUdpSocket, UdpPoller};
use ring::rand::SecureRandom;
use std::io::{self, IoSliceMut};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::{Mutex as AsyncMutex, oneshot};
use tokio::task::JoinHandle;
use tracing::{debug, trace, warn};

const INBOUND_CAPACITY: usize = 1024;
const RECV_BUFFER_LEN: usize = 65535;
const INITIAL_RTO: Duration = Duration::from_millis(500);
const MAX_RTO: Duration = Duration::from_secs(2);
const MAX_ATTEMPTS: u32 = 7;
const STALE_NONCE: u16 = 438;

pub fn long_term_key(username: &str, realm: &str, password: &str) -> [u8; 16] {
    let mut hasher = Md5::new();
    hasher.update(username.as_bytes());
    hasher.update(b":");
    hasher.update(realm.as_bytes());
    hasher.update(b":");
    hasher.update(password.as_bytes());
    hasher.finalize().into()
}

#[derive(Debug, Clone)]
pub struct TurnConfig {
    pub server: SocketAddr,
    pub username: String,
    pub password: String,
    pub lifetime_secs: u32,
}

struct RelayDatagram {
    data: Vec<u8>,
}

struct Pending {
    txn: [u8; 12],
    tx: oneshot::Sender<Message>,
}

struct TurnInner {
    socket: Arc<UdpSocket>,
    username: String,
    realm: Mutex<String>,
    nonce: Mutex<String>,
    key: Mutex<[u8; 16]>,
    pending: Mutex<Option<Pending>>,
    request_lock: AsyncMutex<()>,
}

impl TurnInner {
    async fn transact(&self, bytes: Vec<u8>, txn: [u8; 12]) -> Result<Message> {
        let _guard = self.request_lock.lock().await;
        let (tx, mut rx) = oneshot::channel();
        *self.pending.lock().unwrap() = Some(Pending { txn, tx });

        let mut rto = INITIAL_RTO;
        for _ in 0..MAX_ATTEMPTS {
            self.socket
                .send(&bytes)
                .await
                .map_err(|e| Error::Turn(format!("send failed: {e}")))?;
            match tokio::time::timeout(rto, &mut rx).await {
                Ok(Ok(msg)) => return Ok(msg),
                Ok(Err(_)) => return Err(Error::Turn("transaction cancelled".into())),
                Err(_) => rto = (rto * 2).min(MAX_RTO),
            }
        }

        *self.pending.lock().unwrap() = None;
        Err(Error::Turn("request timed out".into()))
    }

    async fn request_authed(&self, method: u16, extra: &[Attribute]) -> Result<Message> {
        for attempt in 0..2 {
            let txn = new_transaction_id();
            let mut msg = Message::new(method, Class::Request, txn);
            for attr in extra {
                msg.push(attr.clone());
            }
            msg.push(Attribute::Username(self.username.clone()));
            msg.push(Attribute::Realm(self.realm.lock().unwrap().clone()));
            msg.push(Attribute::Nonce(self.nonce.lock().unwrap().clone()));
            let key = *self.key.lock().unwrap();
            let bytes = msg.encode(Some(&key), false);

            let resp = self.transact(bytes, txn).await?;
            if resp.class == Class::Error {
                let code = resp.error_code().unwrap_or_default();
                if code == STALE_NONCE && attempt == 0 {
                    if let Some(nonce) = resp.nonce() {
                        *self.nonce.lock().unwrap() = nonce.to_string();
                    }
                    continue;
                }
                return Err(Error::Turn(format!(
                    "{method:#05x} rejected with code {code}"
                )));
            }
            return Ok(resp);
        }
        Err(Error::Turn("stale-nonce retry exhausted".into()))
    }
}

pub struct TurnClient {
    inner: Arc<TurnInner>,
    relayed_addr: SocketAddr,
    channel: u16,
    inbound_rx: Mutex<Option<tokio::sync::mpsc::Receiver<RelayDatagram>>>,
    demux: JoinHandle<()>,
    refresh: JoinHandle<()>,
}

impl TurnClient {
    pub async fn connect(config: TurnConfig) -> Result<Self> {
        let bind_addr: SocketAddr = if config.server.is_ipv6() {
            "[::]:0".parse().unwrap_or_else(|_| unreachable!())
        } else {
            "0.0.0.0:0".parse().unwrap_or_else(|_| unreachable!())
        };
        let socket = UdpSocket::bind(bind_addr)
            .await
            .map_err(|e| Error::Turn(format!("bind failed: {e}")))?;
        socket
            .connect(config.server)
            .await
            .map_err(|e| Error::Turn(format!("connect failed: {e}")))?;
        let socket = Arc::new(socket);

        let (inbound_tx, inbound_rx) = tokio::sync::mpsc::channel(INBOUND_CAPACITY);
        let inner = Arc::new(TurnInner {
            socket: Arc::clone(&socket),
            username: config.username.clone(),
            realm: Mutex::new(String::new()),
            nonce: Mutex::new(String::new()),
            key: Mutex::new([0u8; 16]),
            pending: Mutex::new(None),
            request_lock: AsyncMutex::new(()),
        });

        let demux = tokio::spawn(run_demux(Arc::clone(&inner), inbound_tx));

        let (relayed_addr, lifetime) = allocate(&inner, &config).await?;
        debug!(%relayed_addr, lifetime, "turn allocation established");

        let refresh = tokio::spawn(run_refresh(Arc::clone(&inner), lifetime));

        Ok(Self {
            inner,
            relayed_addr,
            channel: CHANNEL_MIN,
            inbound_rx: Mutex::new(Some(inbound_rx)),
            demux,
            refresh,
        })
    }

    pub fn relayed_addr(&self) -> SocketAddr {
        self.relayed_addr
    }

    pub async fn bind_channel(&self, peer: SocketAddr) -> Result<()> {
        let resp = self
            .inner
            .request_authed(
                METHOD_CHANNEL_BIND,
                &[
                    Attribute::ChannelNumber(self.channel),
                    Attribute::XorPeerAddress(peer),
                ],
            )
            .await?;
        if resp.class != Class::Success {
            return Err(Error::Turn(format!(
                "channel bind rejected: {:?}",
                resp.error_code()
            )));
        }
        debug!(channel = self.channel, %peer, "turn channel bound");
        Ok(())
    }

    pub async fn relay_socket(&self, peer: SocketAddr) -> Result<Arc<TurnRelaySocket>> {
        self.bind_channel(peer).await?;
        let rx = self
            .inbound_rx
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| Error::Turn("relay socket already taken".into()))?;
        Ok(Arc::new(TurnRelaySocket {
            socket: Arc::clone(&self.inner.socket),
            channel: self.channel,
            peer,
            local: self.relayed_addr,
            inbound: Mutex::new(rx),
        }))
    }
}

impl Drop for TurnClient {
    fn drop(&mut self) {
        self.demux.abort();
        self.refresh.abort();
    }
}

async fn allocate(inner: &TurnInner, config: &TurnConfig) -> Result<(SocketAddr, u32)> {
    let txn = new_transaction_id();
    let mut initial = Message::new(METHOD_ALLOCATE, Class::Request, txn);
    initial.push(Attribute::RequestedTransport(TRANSPORT_UDP));
    let resp = inner.transact(initial.encode(None, false), txn).await?;

    if resp.class != Class::Success {
        let realm = resp
            .realm()
            .ok_or_else(|| Error::Turn("allocate challenge missing realm".into()))?
            .to_string();
        let nonce = resp
            .nonce()
            .ok_or_else(|| Error::Turn("allocate challenge missing nonce".into()))?
            .to_string();
        let key = long_term_key(&config.username, &realm, &config.password);
        *inner.realm.lock().unwrap() = realm;
        *inner.nonce.lock().unwrap() = nonce;
        *inner.key.lock().unwrap() = key;

        let authed = inner
            .request_authed(
                METHOD_ALLOCATE,
                &[
                    Attribute::RequestedTransport(TRANSPORT_UDP),
                    Attribute::Lifetime(config.lifetime_secs),
                ],
            )
            .await?;
        return parse_allocation(&authed, config.lifetime_secs);
    }

    parse_allocation(&resp, config.lifetime_secs)
}

fn parse_allocation(resp: &Message, requested: u32) -> Result<(SocketAddr, u32)> {
    if resp.class != Class::Success {
        return Err(Error::Turn(format!(
            "allocate rejected: {:?}",
            resp.error_code()
        )));
    }
    let relayed = resp
        .xor_relayed_address()
        .ok_or_else(|| Error::Turn("allocate success missing relayed address".into()))?;
    let lifetime = resp.lifetime().unwrap_or(requested);
    Ok((relayed, lifetime))
}

async fn run_demux(inner: Arc<TurnInner>, inbound_tx: tokio::sync::mpsc::Sender<RelayDatagram>) {
    let mut buf = vec![0u8; RECV_BUFFER_LEN];
    loop {
        let n = match inner.socket.recv(&mut buf).await {
            Ok(n) => n,
            Err(e) => {
                warn!(error = %e, "turn demux recv failed");
                break;
            }
        };
        let data = &buf[..n];

        if turn::is_channel_data(data) {
            if let Ok(frame) = ChannelData::decode(data)
                && inbound_tx
                    .try_send(RelayDatagram { data: frame.data })
                    .is_err()
            {
                trace!("turn relay inbound queue full or closed, dropping datagram");
            }
            continue;
        }

        if let Ok(msg) = Message::decode(data) {
            let mut pending = inner.pending.lock().unwrap();
            let matches = pending
                .as_ref()
                .is_some_and(|p| p.txn == msg.transaction_id);
            if matches && let Some(p) = pending.take() {
                let _ = p.tx.send(msg);
            }
        }
    }
}

async fn run_refresh(inner: Arc<TurnInner>, lifetime: u32) {
    let period = Duration::from_secs(u64::from((lifetime / 2).max(60)));
    loop {
        tokio::time::sleep(period).await;
        match inner
            .request_authed(METHOD_REFRESH, &[Attribute::Lifetime(lifetime)])
            .await
        {
            Ok(_) => debug!("turn allocation refreshed"),
            Err(e) => {
                warn!(error = %e, "turn refresh failed");
                break;
            }
        }
    }
}

fn new_transaction_id() -> [u8; 12] {
    let mut id = [0u8; 12];
    if ring::rand::SystemRandom::new().fill(&mut id).is_err() {
        let fallback = uuid::Uuid::new_v4();
        id.copy_from_slice(&fallback.as_bytes()[..12]);
    }
    id
}

#[derive(Debug)]
pub struct TurnRelaySocket {
    socket: Arc<UdpSocket>,
    channel: u16,
    peer: SocketAddr,
    local: SocketAddr,
    inbound: Mutex<tokio::sync::mpsc::Receiver<RelayDatagram>>,
}

#[derive(Debug)]
struct RelayPoller {
    socket: Arc<UdpSocket>,
}

impl UdpPoller for RelayPoller {
    fn poll_writable(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        self.socket.poll_send_ready(cx)
    }
}

impl AsyncUdpSocket for TurnRelaySocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        Box::pin(RelayPoller {
            socket: Arc::clone(&self.socket),
        })
    }

    fn try_send(&self, transmit: &Transmit) -> io::Result<()> {
        if transmit.destination != self.peer {
            trace!(dest = %transmit.destination, "turn relay dropping datagram for unbound destination");
            return Ok(());
        }
        let frame = ChannelData {
            channel: self.channel,
            data: transmit.contents.to_vec(),
        }
        .encode();
        self.socket.try_send(&frame).map(|_| ())
    }

    fn poll_recv(
        &self,
        cx: &mut Context,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        let mut rx = self.inbound.lock().unwrap();
        let mut count = 0;
        while count < bufs.len() {
            match rx.poll_recv(cx) {
                Poll::Ready(Some(datagram)) => {
                    let len = datagram.data.len().min(bufs[count].len());
                    bufs[count][..len].copy_from_slice(&datagram.data[..len]);
                    meta[count] = RecvMeta {
                        addr: self.peer,
                        len,
                        stride: len,
                        ecn: None,
                        dst_ip: None,
                    };
                    count += 1;
                }
                Poll::Ready(None) | Poll::Pending => break,
            }
        }
        if count == 0 {
            Poll::Pending
        } else {
            Poll::Ready(Ok(count))
        }
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.local)
    }

    fn max_transmit_segments(&self) -> usize {
        1
    }

    fn max_receive_segments(&self) -> usize {
        1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::turn::ChannelData;
    use std::future::poll_fn;

    async fn fake_turn_server(socket: UdpSocket, relayed: SocketAddr) {
        let mut buf = vec![0u8; RECV_BUFFER_LEN];
        loop {
            let (n, from) = match socket.recv_from(&mut buf).await {
                Ok(v) => v,
                Err(_) => return,
            };
            let data = &buf[..n];

            if turn::is_channel_data(data) {
                if let Ok(frame) = ChannelData::decode(data) {
                    let _ = socket.send_to(&frame.encode(), from).await;
                }
                continue;
            }

            let Ok(msg) = Message::decode(data) else {
                continue;
            };
            let reply = match msg.method {
                METHOD_ALLOCATE => {
                    let authed = msg
                        .attributes
                        .iter()
                        .any(|a| matches!(a, Attribute::MessageIntegrity(_)));
                    if authed {
                        let mut r =
                            Message::new(METHOD_ALLOCATE, Class::Success, msg.transaction_id);
                        r.push(Attribute::XorRelayedAddress(relayed));
                        r.push(Attribute::Lifetime(600));
                        r.encode(None, false)
                    } else {
                        let mut r = Message::new(METHOD_ALLOCATE, Class::Error, msg.transaction_id);
                        r.push(Attribute::ErrorCode {
                            code: 401,
                            reason: "Unauthorized".into(),
                        });
                        r.push(Attribute::Realm("example.org".into()));
                        r.push(Attribute::Nonce("nonce-value".into()));
                        r.encode(None, false)
                    }
                }
                METHOD_CHANNEL_BIND => {
                    Message::new(METHOD_CHANNEL_BIND, Class::Success, msg.transaction_id)
                        .encode(None, false)
                }
                METHOD_REFRESH => {
                    let mut r = Message::new(METHOD_REFRESH, Class::Success, msg.transaction_id);
                    r.push(Attribute::Lifetime(600));
                    r.encode(None, false)
                }
                _ => continue,
            };
            let _ = socket.send_to(&reply, from).await;
        }
    }

    #[tokio::test]
    async fn allocates_and_relays_through_a_fake_turn_server() {
        let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let server_addr = server.local_addr().unwrap();
        let relayed: SocketAddr = "127.0.0.1:55555".parse().unwrap();
        tokio::spawn(fake_turn_server(server, relayed));

        let password = format!("pw-{}", std::process::id());
        let client = TurnClient::connect(TurnConfig {
            server: server_addr,
            username: "device-a".into(),
            password,
            lifetime_secs: 600,
        })
        .await
        .unwrap();

        assert_eq!(client.relayed_addr(), relayed);

        let peer: SocketAddr = "127.0.0.1:40000".parse().unwrap();
        let relay = client.relay_socket(peer).await.unwrap();

        relay
            .try_send(&Transmit {
                destination: peer,
                ecn: None,
                contents: b"hello-quic",
                segment_size: None,
                src_ip: None,
            })
            .unwrap();

        let mut storage = [0u8; 1500];
        let (n, addr) = tokio::time::timeout(
            Duration::from_secs(2),
            poll_fn(|cx| {
                let mut bufs = [IoSliceMut::new(&mut storage)];
                let mut meta = [RecvMeta::default()];
                relay.poll_recv(cx, &mut bufs, &mut meta).map_ok(|count| {
                    assert_eq!(count, 1);
                    (meta[0].len, meta[0].addr)
                })
            }),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(&storage[..n], b"hello-quic");
        assert_eq!(addr, peer);
    }
}
