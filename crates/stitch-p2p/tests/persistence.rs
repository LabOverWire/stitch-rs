#![cfg(all(feature = "store", feature = "persistence"))]
//! Durability: state written to a `Store::open` path survives dropping and
//! reopening the store — including that deletes stay deleted and the HLC keeps
//! advancing so post-reopen writes still win.

use serde_json::json;
use stitch_p2p::{PEER_ID_LEN, PeerId, Store};
use tempfile::TempDir;

fn peer(n: u8) -> PeerId {
    let mut id = [0u8; PEER_ID_LEN];
    id[0] = n;
    id
}

#[tokio::test]
async fn state_survives_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("log");

    {
        let store = Store::open(peer(1), &path).unwrap();
        store.create("task", "t1", json!({"title": "alpha"})).await.unwrap();
        store.create("task", "t2", json!({"title": "beta"})).await.unwrap();
        store.delete("task", "t2").await;
        assert_eq!(store.read("task", "t1").await, Some(json!({"title": "alpha"})));
        assert_eq!(store.read("task", "t2").await, None);
    }

    let store = Store::open(peer(1), &path).unwrap();
    assert_eq!(
        store.read("task", "t1").await,
        Some(json!({"title": "alpha"})),
        "record should survive reopen"
    );
    assert_eq!(
        store.read("task", "t2").await,
        None,
        "tombstone should survive reopen"
    );
    assert_eq!(store.list("task").await.len(), 1);
}

#[tokio::test]
async fn post_reopen_write_wins_over_prior_state() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("log");

    {
        let store = Store::open(peer(1), &path).unwrap();
        store.create("doc", "d1", json!({"v": 1})).await.unwrap();
    }

    let store = Store::open(peer(1), &path).unwrap();
    store.create("doc", "d1", json!({"v": 2})).await.unwrap();
    assert_eq!(
        store.read("doc", "d1").await,
        Some(json!({"v": 2})),
        "a write after reopen must out-rank the persisted one (clock recovered)"
    );

    drop(store);
    let reopened = Store::open(peer(1), &path).unwrap();
    assert_eq!(reopened.read("doc", "d1").await, Some(json!({"v": 2})));
}
