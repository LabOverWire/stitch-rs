#![cfg(all(feature = "store", feature = "membership"))]
//! Owner-controlled membership end to end: an owner, an invited member, and an
//! uninvited outsider all sync. Only the owner's and invited member's records
//! are visible; the outsider's are filtered out. Authorization is a read-time
//! filter over converged state (per `spec/StitchP2PAuth.tla`), so every peer
//! reaches the same view, and revocation hides the revoked peer's records.

use serde_json::json;
use std::time::Duration;
use stitch_p2p::membership::Role;
use stitch_p2p::{Identity, Store, session};
use tokio::io::{DuplexStream, split};
use tokio::task::JoinHandle;

const PULL: Duration = Duration::from_millis(20);

fn connect(a: &Store, a_io: DuplexStream, b: &Store, b_io: DuplexStream) -> Vec<JoinHandle<()>> {
    let rx_a = a.node().register_session();
    let (ar, aw) = split(a_io);
    let sa = a.node().state();
    let rx_b = b.node().register_session();
    let (br, bw) = split(b_io);
    let sb = b.node().state();
    vec![
        tokio::spawn(async move {
            let _ = session::run(sa, ar, aw, rx_a, PULL).await;
        }),
        tokio::spawn(async move {
            let _ = session::run(sb, br, bw, rx_b, PULL).await;
        }),
    ]
}

async fn wait_until<F, Fut>(timeout: Duration, mut cond: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if cond().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn member_writes_visible_outsider_filtered() {
    let owner_id = Identity::from_seed([1u8; 32]);
    let owner_pid = owner_id.peer_id();
    let member_id = Identity::from_seed([2u8; 32]);
    let member_pid = member_id.peer_id();
    let outsider_id = Identity::from_seed([3u8; 32]);

    let owner = Store::with_owner(owner_id);
    let member = Store::join(member_id, owner_pid);
    let outsider = Store::join(outsider_id, owner_pid);

    // Owner invites the member (not the outsider).
    owner.invite(member_pid, Role::Member).await;

    // Each writes a record under its own identity.
    member
        .create("task", "m", json!({"by": "member"}))
        .await
        .unwrap();
    outsider
        .create("task", "o", json!({"by": "outsider"}))
        .await
        .unwrap();

    // Wire a hub through the owner: owner<->member, owner<->outsider.
    let (om_o, om_m) = tokio::io::duplex(64 * 1024);
    let (oo_o, oo_o2) = tokio::io::duplex(64 * 1024);
    let mut handles = connect(&owner, om_o, &member, om_m);
    handles.extend(connect(&owner, oo_o, &outsider, oo_o2));

    // Owner sees the member's record (authorized) but not the outsider's.
    let ok = wait_until(Duration::from_secs(5), || async {
        owner.read("task", "m").await.is_some()
    })
    .await;
    for h in &handles {
        h.abort();
    }
    assert!(ok, "owner never saw the member's authorized write");

    assert_eq!(
        owner.read("task", "m").await,
        Some(json!({"by": "member"})),
        "invited member's record is visible"
    );
    assert_eq!(
        owner.read("task", "o").await,
        None,
        "uninvited outsider's record is filtered out"
    );
    // list reflects the same filter.
    let tasks = owner.list("task").await;
    assert_eq!(tasks, vec![json!({"by": "member"})]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn revocation_hides_records() {
    let owner_id = Identity::from_seed([1u8; 32]);
    let owner_pid = owner_id.peer_id();
    let member_id = Identity::from_seed([2u8; 32]);
    let member_pid = member_id.peer_id();

    let owner = Store::with_owner(owner_id);
    let member = Store::join(member_id, owner_pid);

    owner.invite(member_pid, Role::Member).await;
    member.create("task", "m", json!({"x": 1})).await.unwrap();

    let (a_io, b_io) = tokio::io::duplex(64 * 1024);
    let handles = connect(&owner, a_io, &member, b_io);

    assert!(
        wait_until(Duration::from_secs(5), || async {
            owner.read("task", "m").await.is_some()
        })
        .await,
        "member write should be visible before revocation"
    );

    owner.revoke(member_pid).await;

    assert!(
        wait_until(Duration::from_secs(5), || async {
            owner.read("task", "m").await.is_none()
        })
        .await,
        "after revocation the member's record should be hidden"
    );
    for h in &handles {
        h.abort();
    }
}
