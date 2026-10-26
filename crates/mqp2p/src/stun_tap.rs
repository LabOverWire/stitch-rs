use crate::error::{Error, Result};
use crate::stun;
use quinn::udp::{RecvMeta, Transmit};
use quinn::{AsyncUdpSocket, UdpPoller};
use std::io::IoSliceMut;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tracing::trace;

#[derive(Debug)]
pub(crate) struct StunTap {
    inner: Arc<dyn AsyncUdpSocket>,
    responses: UnboundedSender<Vec<u8>>,
}

impl StunTap {
    pub(crate) fn wrap(inner: Arc<dyn AsyncUdpSocket>) -> (Arc<Self>, UnboundedReceiver<Vec<u8>>) {
        let (responses, rx) = unbounded_channel();
        (Arc::new(Self { inner, responses }), rx)
    }

    pub(crate) fn send_datagram(&self, destination: SocketAddr, packet: &[u8]) -> std::io::Result<()> {
        self.inner.try_send(&Transmit {
            destination,
            ecn: None,
            contents: packet,
            segment_size: None,
            src_ip: None,
        })
    }
}

impl AsyncUdpSocket for StunTap {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        Arc::clone(&self.inner).create_io_poller()
    }

    fn try_send(&self, transmit: &Transmit) -> std::io::Result<()> {
        self.inner.try_send(transmit)
    }

    fn poll_recv(
        &self,
        cx: &mut Context,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<std::io::Result<usize>> {
        let poll = self.inner.poll_recv(cx, bufs, meta);
        if let Poll::Ready(Ok(count)) = &poll {
            for i in 0..*count {
                let len = meta[i].len;
                if len == 0 || (meta[i].stride != 0 && meta[i].stride < len) {
                    continue;
                }
                if stun::looks_like_binding_response(&bufs[i][..len]) {
                    let _ = self.responses.send(bufs[i][..len].to_vec());
                    meta[i].len = 0;
                    trace!("tapped a STUN binding response off the QUIC socket");
                }
            }
        }
        poll
    }

    fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    fn max_transmit_segments(&self) -> usize {
        self.inner.max_transmit_segments()
    }

    fn max_receive_segments(&self) -> usize {
        self.inner.max_receive_segments()
    }

    fn may_fragment(&self) -> bool {
        self.inner.may_fragment()
    }
}

pub(crate) async fn rediscover_srflx(
    tap: &StunTap,
    responses: &mut UnboundedReceiver<Vec<u8>>,
    stun_server: &str,
    timeout: Duration,
) -> Result<SocketAddr> {
    let server_addr = stun::resolve_server(stun_server).await?;
    let txn_id = stun::random_txn_id()?;
    let request = stun::build_binding_request(&txn_id);
    tap.send_datagram(server_addr, &request)
        .map_err(|e| Error::Stun {
            server: server_addr,
            reason: format!("tap send failed: {e}"),
        })?;

    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(Error::Stun {
                server: server_addr,
                reason: "timeout waiting for STUN response".into(),
            });
        }
        match tokio::time::timeout(remaining, responses.recv()).await {
            Ok(Some(payload)) => {
                if let Ok(addr) = stun::parse_binding_response(&payload, &txn_id) {
                    return Ok(addr);
                }
            }
            Ok(None) => {
                return Err(Error::Stun {
                    server: server_addr,
                    reason: "STUN response channel closed".into(),
                });
            }
            Err(_) => {
                return Err(Error::Stun {
                    server: server_addr,
                    reason: "timeout waiting for STUN response".into(),
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::Mutex;

    #[derive(Debug)]
    struct FakeInner {
        local: SocketAddr,
        incoming: Mutex<VecDeque<(SocketAddr, Vec<u8>)>>,
        sent: Mutex<Vec<(SocketAddr, Vec<u8>)>>,
    }

    impl FakeInner {
        fn new() -> Self {
            Self {
                local: SocketAddr::from(([127, 0, 0, 1], 55555)),
                incoming: Mutex::new(VecDeque::new()),
                sent: Mutex::new(Vec::new()),
            }
        }

        fn push_incoming(&self, from: SocketAddr, data: Vec<u8>) {
            self.incoming.lock().expect("lock").push_back((from, data));
        }

        fn sent_count(&self) -> usize {
            self.sent.lock().expect("lock").len()
        }
    }

    #[derive(Debug)]
    struct AlwaysWritable;

    impl UdpPoller for AlwaysWritable {
        fn poll_writable(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncUdpSocket for FakeInner {
        fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
            Box::pin(AlwaysWritable)
        }

        fn try_send(&self, transmit: &Transmit) -> std::io::Result<()> {
            self.sent
                .lock()
                .expect("lock")
                .push((transmit.destination, transmit.contents.to_vec()));
            Ok(())
        }

        fn poll_recv(
            &self,
            _cx: &mut Context,
            bufs: &mut [IoSliceMut<'_>],
            meta: &mut [RecvMeta],
        ) -> Poll<std::io::Result<usize>> {
            let mut queue = self.incoming.lock().expect("lock");
            let mut n = 0;
            while n < bufs.len() && n < meta.len() {
                let Some((from, data)) = queue.pop_front() else {
                    break;
                };
                let len = data.len();
                bufs[n][..len].copy_from_slice(&data);
                meta[n] = RecvMeta {
                    addr: from,
                    len,
                    stride: len,
                    ecn: None,
                    dst_ip: None,
                };
                n += 1;
            }
            if n == 0 {
                Poll::Pending
            } else {
                Poll::Ready(Ok(n))
            }
        }

        fn local_addr(&self) -> std::io::Result<SocketAddr> {
            Ok(self.local)
        }
    }

    fn fake_binding_response(txn_id: &[u8; 12]) -> Vec<u8> {
        let mut buf = vec![0u8; 20];
        buf[0] = 0x01;
        buf[1] = 0x01;
        buf[4..8].copy_from_slice(&0x2112_A442u32.to_be_bytes());
        buf[8..20].copy_from_slice(txn_id);
        buf
    }

    fn poll_once(tap: &StunTap, bufs: &mut [IoSliceMut<'_>], meta: &mut [RecvMeta]) -> usize {
        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);
        match tap.poll_recv(&mut cx, bufs, meta) {
            Poll::Ready(Ok(n)) => n,
            other => panic!("expected ready recv, got {other:?}"),
        }
    }

    #[test]
    fn poll_recv_taps_stun_and_passes_quic_through() {
        let inner = Arc::new(FakeInner::new());
        let stun = fake_binding_response(&[7u8; 12]);
        let quic = vec![0x40u8; 32];
        inner.push_incoming(SocketAddr::from(([9, 9, 9, 9], 3478)), stun.clone());
        inner.push_incoming(SocketAddr::from(([1, 2, 3, 4], 4433)), quic.clone());

        let (tap, mut responses) = StunTap::wrap(inner);

        let mut b0 = [0u8; 1500];
        let mut b1 = [0u8; 1500];
        let mut bufs = [IoSliceMut::new(&mut b0), IoSliceMut::new(&mut b1)];
        let mut meta = [RecvMeta::default(), RecvMeta::default()];

        let n = poll_once(&tap, &mut bufs, &mut meta);
        assert_eq!(n, 2);
        assert_eq!(meta[0].len, 0, "the STUN datagram must be hidden from quinn");
        assert_eq!(meta[1].len, quic.len(), "the QUIC datagram must pass through");
        assert_eq!(&bufs[1][..meta[1].len], &quic[..]);

        assert_eq!(responses.try_recv().expect("stun tapped"), stun);
        assert!(responses.try_recv().is_err());
    }

    #[tokio::test]
    async fn rediscover_sends_a_request_and_times_out_without_a_response() {
        let inner = Arc::new(FakeInner::new());
        let (tap, mut responses) = StunTap::wrap(inner.clone());

        let result = rediscover_srflx(
            &tap,
            &mut responses,
            "127.0.0.1:3478",
            Duration::from_millis(50),
        )
        .await;

        assert!(matches!(result, Err(Error::Stun { .. })));
        assert_eq!(inner.sent_count(), 1, "a binding request must have been sent");
    }
}
