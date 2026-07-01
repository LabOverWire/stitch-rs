//! In-browser smoke test for the wasm `Store`. Drives the in-memory store
//! through create/read/snapshot, registers a subscription, and exercises
//! IndexedDB persistence (plaintext + encrypted) across a store reopen,
//! proving the `mqdb-wasm`-backed core runs under real WebAssembly. Run with
//! `wasm-pack test --headless --chrome crates/stitch-wasm`.

#![cfg(target_arch = "wasm32")]

use wasm_bindgen::JsValue;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

fn config() -> JsValue {
    js_sys::JSON::parse(
        r#"{
            "entities": {
                "project": {"fields":[{"name":"id","type":"string","required":true},{"name":"name","type":"string"}]},
                "task": {"fields":[{"name":"id","type":"string","required":true},{"name":"projectId","type":"string"},{"name":"title","type":"string"}]}
            },
            "scope": {"rootEntity":"project","childEntities":["task"],"scopeField":"projectId"}
        }"#,
    )
    .unwrap()
}

#[wasm_bindgen_test]
async fn create_read_snapshot_in_browser() {
    let store = stitch_wasm::create_store(config(), JsValue::UNDEFINED).expect("create_store");
    store.initialize().await.expect("initialize");

    let first = js_sys::JSON::parse(r#"{"title":"first"}"#).unwrap();
    let id = store
        .create("task".into(), "p1".into(), first, None)
        .await
        .expect("create first");

    let second = js_sys::JSON::parse(r#"{"title":"second"}"#).unwrap();
    store
        .create("task".into(), "p1".into(), second, None)
        .await
        .expect("create second");

    let got = store.read("task".into(), id).expect("read");
    assert!(!got.is_null(), "a created row must be readable back");

    let snapshot = store
        .snapshot("task".into(), "p1".into())
        .expect("snapshot");
    let rows = js_sys::Array::from(&snapshot);
    assert_eq!(rows.length(), 2, "snapshot must contain both created tasks");
}

#[wasm_bindgen_test]
async fn scope_isolation_in_browser() {
    let store = stitch_wasm::create_store(config(), JsValue::UNDEFINED).expect("create_store");
    store.initialize().await.expect("initialize");

    let a = js_sys::JSON::parse(r#"{"title":"a"}"#).unwrap();
    store
        .create("task".into(), "p1".into(), a, None)
        .await
        .unwrap();
    let b = js_sys::JSON::parse(r#"{"title":"b"}"#).unwrap();
    store
        .create("task".into(), "p2".into(), b, None)
        .await
        .unwrap();

    let p1 = js_sys::Array::from(&store.snapshot("task".into(), "p1".into()).unwrap());
    let p2 = js_sys::Array::from(&store.snapshot("task".into(), "p2".into()).unwrap());
    assert_eq!(p1.length(), 1, "scope p1 sees only its own task");
    assert_eq!(p2.length(), 1, "scope p2 sees only its own task");
}

#[wasm_bindgen_test]
async fn subscribe_registers_in_browser() {
    let store = stitch_wasm::create_store(config(), JsValue::UNDEFINED).expect("create_store");
    store.initialize().await.expect("initialize");
    let callback = js_sys::Function::new_no_args("");
    store
        .subscribe_to_entity("task".into(), callback)
        .expect("subscribe_to_entity");
}

#[wasm_bindgen_test]
async fn subscribe_fires_on_matching_mutation_in_browser() {
    use std::cell::Cell;
    use std::rc::Rc;
    use wasm_bindgen::JsCast;
    use wasm_bindgen::closure::Closure;

    let store = stitch_wasm::create_store(config(), JsValue::UNDEFINED).expect("create_store");
    store.initialize().await.expect("initialize");

    let hits = Rc::new(Cell::new(0u32));
    let hits_cb = Rc::clone(&hits);
    let closure = Closure::<dyn FnMut()>::new(move || hits_cb.set(hits_cb.get() + 1));
    store
        .subscribe_to_entity(
            "task".into(),
            closure.as_ref().unchecked_ref::<js_sys::Function>().clone(),
        )
        .expect("subscribe_to_entity");

    let row = js_sys::JSON::parse(r#"{"title":"hello"}"#).unwrap();
    store
        .create("task".into(), "p1".into(), row, None)
        .await
        .expect("create");

    for _ in 0..50 {
        if hits.get() > 0 {
            break;
        }
        wasm_bindgen_futures::JsFuture::from(js_sys::Promise::resolve(&JsValue::NULL))
            .await
            .unwrap();
    }

    assert_eq!(
        hits.get(),
        1,
        "subscription callback must fire exactly once for a matching mutation"
    );
    closure.forget();
}

fn persist_options(db_name: &str) -> JsValue {
    js_sys::JSON::parse(&format!(r#"{{"persistence":{{"dbName":"{db_name}"}}}}"#)).unwrap()
}

fn encrypted_options(db_name: &str, passphrase: &str) -> JsValue {
    js_sys::JSON::parse(&format!(
        r#"{{"persistence":{{"dbName":"{db_name}","passphrase":"{passphrase}"}}}}"#
    ))
    .unwrap()
}

fn remote_options(url: &str) -> JsValue {
    js_sys::JSON::parse(&format!(r#"{{"remote":{{"url":"{url}"}}}}"#)).unwrap()
}

fn persist_remote_options(db_name: &str, url: &str) -> JsValue {
    js_sys::JSON::parse(&format!(
        r#"{{"persistence":{{"dbName":"{db_name}"}},"remote":{{"url":"{url}"}}}}"#
    ))
    .unwrap()
}

#[wasm_bindgen_test]
async fn offline_write_queues_and_survives_reopen_in_browser() {
    let opts = || persist_remote_options("stitch-m31-queue", "ws://127.0.0.1:1");

    {
        let store = stitch_wasm::create_store(config(), opts()).expect("create_store");
        store.initialize().await.expect("initialize");
        store
            .set_authenticated_user(Some("u1".into()))
            .expect("set user");
        let row = js_sys::JSON::parse(r#"{"title":"queued"}"#).unwrap();
        store
            .create("task".into(), "p1".into(), row, None)
            .await
            .expect("create while offline");
        let pending = store
            .pending_mutation_count("p1".into())
            .await
            .expect("pending count");
        assert!(
            pending >= 1,
            "a write made while disconnected must be queued"
        );
    }

    let store = stitch_wasm::create_store(config(), opts()).expect("create_store");
    store.initialize().await.expect("initialize");
    store
        .set_authenticated_user(Some("u1".into()))
        .expect("set user");
    let pending = store
        .pending_mutation_count("p1".into())
        .await
        .expect("pending count");
    assert!(
        pending >= 1,
        "the queued offline write must survive a reopen via IndexedDB"
    );
}

#[wasm_bindgen_test]
async fn remote_connect_failure_is_graceful_in_browser() {
    let store = stitch_wasm::create_store(config(), remote_options("ws://127.0.0.1:1"))
        .expect("create_store");
    store
        .initialize()
        .await
        .expect("initialize succeeds even when the broker is unreachable");

    let status = store.connection_status().expect("connection_status");
    assert_ne!(
        status, "Connected",
        "must not report Connected against a dead broker"
    );

    let row = js_sys::JSON::parse(r#"{"title":"offline"}"#).unwrap();
    let id = store
        .create("task".into(), "p1".into(), row, None)
        .await
        .expect("local create works without a broker");
    let got = store.read("task".into(), id).expect("read");
    assert!(
        !got.is_null(),
        "local create/read works with a configured-but-unreachable remote"
    );
}

#[wasm_bindgen_test]
async fn set_session_invalid_handler_accepts_js_callback_in_browser() {
    use wasm_bindgen::JsCast;
    use wasm_bindgen::closure::Closure;

    let store = stitch_wasm::create_store(config(), remote_options("ws://127.0.0.1:1"))
        .expect("create_store");
    store.initialize().await.expect("initialize");

    let closure = Closure::<dyn FnMut()>::new(move || {});
    store
        .set_session_invalid_handler(closure.as_ref().unchecked_ref::<js_sys::Function>().clone())
        .expect("setSessionInvalidHandler accepts a non-Send JS callback");
    closure.forget();
}

#[wasm_bindgen_test]
async fn persistence_survives_reopen_in_browser() {
    let id = {
        let store = stitch_wasm::create_store(config(), persist_options("stitch-m2-reopen"))
            .expect("create_store");
        store.initialize().await.expect("initialize");
        let row = js_sys::JSON::parse(r#"{"title":"durable"}"#).unwrap();
        store
            .create("task".into(), "p1".into(), row, None)
            .await
            .expect("create")
    };

    let store = stitch_wasm::create_store(config(), persist_options("stitch-m2-reopen"))
        .expect("create_store");
    store.initialize().await.expect("initialize");

    let got = store
        .read_local_state("task".into(), id)
        .await
        .expect("read_local_state");
    assert!(!got.is_null(), "persisted row must survive a store reopen");

    store
        .replace_scope("p1".into())
        .await
        .expect("replace_scope");
    let rows = js_sys::Array::from(&store.snapshot("task".into(), "p1".into()).unwrap());
    assert_eq!(
        rows.length(),
        1,
        "reopened store rehydrates the task from IndexedDB"
    );
}

#[wasm_bindgen_test]
async fn encrypted_persistence_round_trip_in_browser() {
    let id = {
        let store =
            stitch_wasm::create_store(config(), encrypted_options("stitch-m2-enc", "s3cret"))
                .expect("create_store");
        store.initialize().await.expect("initialize");
        let row = js_sys::JSON::parse(r#"{"title":"secret"}"#).unwrap();
        store
            .create("task".into(), "p1".into(), row, None)
            .await
            .expect("create")
    };

    let store = stitch_wasm::create_store(config(), encrypted_options("stitch-m2-enc", "s3cret"))
        .expect("create_store");
    store.initialize().await.expect("initialize");

    let got = store
        .read_local_state("task".into(), id)
        .await
        .expect("read_local_state");
    assert!(
        !got.is_null(),
        "encrypted persisted row must round-trip with the correct passphrase"
    );
}

#[wasm_bindgen_test]
async fn remote_with_jwt_connects_without_panicking() {
    let opts = js_sys::JSON::parse(
        r#"{"remote":{"url":"ws://127.0.0.1:1","ticket":"header.payload.sig"}}"#,
    )
    .unwrap();
    let store = stitch_wasm::create_store(config(), opts).expect("create_store");
    store
        .initialize()
        .await
        .expect("initialize with a JWT ticket sets enhanced auth and attempts connect");
    let status = store.connection_status().expect("connection_status");
    assert_ne!(
        status, "Connected",
        "must not report Connected against a dead broker"
    );
}

#[wasm_bindgen_test]
async fn remote_with_password_connects_without_panicking() {
    let opts = js_sys::JSON::parse(
        r#"{"remote":{"url":"ws://127.0.0.1:1","username":"alice","password":"s3cret"}}"#,
    )
    .unwrap();
    let store = stitch_wasm::create_store(config(), opts)
        .expect("create_store must parse username/password remote options");
    store
        .initialize()
        .await
        .expect("initialize with username+password sets classic auth and attempts connect");
    let status = store.connection_status().expect("connection_status");
    assert_ne!(
        status, "Connected",
        "must not report Connected against a dead broker"
    );
}

#[wasm_bindgen_test]
async fn reconnect_with_password_args_is_callable_in_browser() {
    let store = stitch_wasm::create_store(config(), remote_options("ws://127.0.0.1:1"))
        .expect("create_store");
    store.initialize().await.expect("initialize");
    let result = store
        .reconnect(
            "ws://127.0.0.1:1".to_string(),
            None,
            Some("alice".to_string()),
            Some("s3cret".to_string()),
        )
        .await;
    assert!(
        result.is_err(),
        "reconnect to a dead broker drives the classic-auth path and fails gracefully with an error, not a panic"
    );
    let status = store.connection_status().expect("connection_status");
    assert_ne!(
        status, "Connected",
        "must not report Connected against a dead broker"
    );
}

#[wasm_bindgen_test]
async fn list_child_count_snapshot_map_in_browser() {
    use wasm_bindgen::JsCast;

    let store = stitch_wasm::create_store(config(), JsValue::UNDEFINED).expect("create_store");
    store.initialize().await.expect("initialize");
    for (scope, title) in [("p1", "a"), ("p1", "b"), ("p2", "c")] {
        let row = js_sys::JSON::parse(&format!(r#"{{"title":"{title}"}}"#)).unwrap();
        store
            .create("task".into(), scope.into(), row, None)
            .await
            .expect("create");
    }

    let filter = js_sys::JSON::parse(r#"{"scopeId":"p1"}"#).unwrap();
    let listed = js_sys::Array::from(&store.list("task".into(), filter).await.expect("list"));
    assert_eq!(listed.length(), 2, "list filters by scope");

    let count = store
        .get_child_count("task".into(), "p1".into())
        .expect("child count");
    assert_eq!(count, 2, "getChildCount matches the scope's rows");

    let map = store
        .get_snapshot_as_map("task".into(), "p1".into())
        .expect("snapshot map");
    let keys = js_sys::Object::keys(&map.dyn_into::<js_sys::Object>().unwrap());
    assert_eq!(keys.length(), 2, "getSnapshotAsMap is keyed by row id");
}

#[wasm_bindgen_test]
async fn subscribe_passes_data_op_and_unsubscribe_stops_in_browser() {
    use std::cell::RefCell;
    use std::rc::Rc;
    use wasm_bindgen::JsCast;
    use wasm_bindgen::closure::Closure;

    async fn yield_microtasks(n: usize) {
        for _ in 0..n {
            wasm_bindgen_futures::JsFuture::from(js_sys::Promise::resolve(&JsValue::NULL))
                .await
                .unwrap();
        }
    }

    let store = stitch_wasm::create_store(config(), JsValue::UNDEFINED).expect("create_store");
    store.initialize().await.expect("initialize");

    let events: Rc<RefCell<Vec<String>>> = Rc::new(RefCell::new(Vec::new()));
    let ev = Rc::clone(&events);
    let cb = Closure::<dyn FnMut(JsValue, JsValue)>::new(move |data: JsValue, op: JsValue| {
        let op = op.as_string().unwrap_or_default();
        ev.borrow_mut().push(format!("{op}:{}", !data.is_null()));
    });
    let unsub = store
        .subscribe_to_entity(
            "task".into(),
            cb.as_ref().unchecked_ref::<js_sys::Function>().clone(),
        )
        .expect("subscribe");
    cb.forget();

    store
        .create(
            "task".into(),
            "p1".into(),
            js_sys::JSON::parse(r#"{"title":"a"}"#).unwrap(),
            None,
        )
        .await
        .expect("create");
    yield_microtasks(50).await;
    assert_eq!(
        events.borrow().as_slice(),
        &["insert:true".to_string()],
        "subscribeToEntity delivers (data, op)"
    );

    let unsub: js_sys::Function = unsub.unchecked_into();
    unsub.call0(&JsValue::NULL).expect("unsubscribe");
    yield_microtasks(20).await;
    store
        .create(
            "task".into(),
            "p1".into(),
            js_sys::JSON::parse(r#"{"title":"b"}"#).unwrap(),
            None,
        )
        .await
        .expect("create2");
    yield_microtasks(30).await;
    assert_eq!(
        events.borrow().len(),
        1,
        "no further callbacks after unsubscribe"
    );
}

#[wasm_bindgen_test]
async fn version_bumps_on_mutation_in_browser() {
    let store = stitch_wasm::create_store(config(), JsValue::UNDEFINED).expect("create_store");
    store.initialize().await.expect("initialize");

    let v0 = store
        .get_version("p1".into(), "task".into())
        .expect("version");
    store
        .create(
            "task".into(),
            "p1".into(),
            js_sys::JSON::parse(r#"{"title":"a"}"#).unwrap(),
            None,
        )
        .await
        .expect("create");
    let v1 = store
        .get_version("p1".into(), "task".into())
        .expect("version");
    assert!(
        v1 > v0,
        "version must bump on a mutation (v0={v0}, v1={v1})"
    );

    let other = store
        .get_version("p2".into(), "task".into())
        .expect("version");
    assert_eq!(other, 0.0, "an untouched (scope,entity) stays at 0");
}

#[wasm_bindgen_test]
async fn scope_signal_observes_committed_data_in_browser() {
    use std::cell::Cell;
    use std::rc::Rc;
    use wasm_bindgen::JsCast;
    use wasm_bindgen::closure::Closure;

    let store = Rc::new(stitch_wasm::create_store(config(), JsValue::UNDEFINED).expect("create"));
    store.initialize().await.expect("initialize");

    let seen = Rc::new(Cell::new(usize::MAX));
    let store_cb = Rc::clone(&store);
    let seen_cb = Rc::clone(&seen);
    let closure = Closure::<dyn FnMut()>::new(move || {
        let rows = js_sys::Array::from(
            &store_cb
                .snapshot("task".into(), "p1".into())
                .expect("sync snapshot in callback"),
        );
        seen_cb.set(rows.length() as usize);
    });
    let _unsub = store
        .subscribe_to_scope(
            "p1".into(),
            "task".into(),
            closure.as_ref().unchecked_ref::<js_sys::Function>().clone(),
        )
        .expect("subscribe_to_scope");

    store
        .create(
            "task".into(),
            "p1".into(),
            js_sys::JSON::parse(r#"{"title":"x"}"#).unwrap(),
            None,
        )
        .await
        .expect("create");

    for _ in 0..50 {
        if seen.get() != usize::MAX {
            break;
        }
        wasm_bindgen_futures::JsFuture::from(js_sys::Promise::resolve(&JsValue::NULL))
            .await
            .unwrap();
    }
    assert_eq!(
        seen.get(),
        1,
        "scope callback's synchronous snapshot must see the committed row"
    );
    closure.forget();
}

#[wasm_bindgen_test]
async fn load_and_clear_scope_in_browser() {
    let store = stitch_wasm::create_store(config(), JsValue::UNDEFINED).expect("create_store");
    store.initialize().await.expect("initialize");

    let data = js_sys::JSON::parse(r#"{"task":[{"id":"t1","title":"a"},{"id":"t2","title":"b"}]}"#)
        .unwrap();
    store
        .load_scope("p1".into(), data)
        .await
        .expect("load_scope");

    let rows = js_sys::Array::from(&store.snapshot("task".into(), "p1".into()).unwrap());
    assert_eq!(rows.length(), 2, "loadScope populates the in-memory scope");

    store.clear_scope("p1".into()).await.expect("clear_scope");
    let after = js_sys::Array::from(&store.snapshot("task".into(), "p1".into()).unwrap());
    assert_eq!(after.length(), 0, "clearScope empties the scope");
}

#[wasm_bindgen_test]
async fn origin_tag_remote_skips_persistence_in_browser() {
    let store = stitch_wasm::create_store(config(), persist_options("stitch-tag-remote"))
        .expect("create_store");
    store.initialize().await.expect("initialize");

    let row = js_sys::JSON::parse(r#"{"title":"r"}"#).unwrap();
    let id = store
        .create("task".into(), "p1".into(), row, Some("remote".into()))
        .await
        .expect("create");

    let got = store.read("task".into(), id.clone()).expect("read");
    assert!(!got.is_null(), "remote-tagged create still lands in memory");

    let durable = store
        .read_local_state("task".into(), id)
        .await
        .expect("read_local_state");
    assert!(
        durable.is_null(),
        "a 'remote'-tagged create must skip persistence (Origin::Remote)"
    );
}

#[wasm_bindgen_test]
async fn update_local_state_round_trip_in_browser() {
    let store = stitch_wasm::create_store(config(), JsValue::UNDEFINED).expect("create_store");
    store.initialize().await.expect("initialize");
    let fields = js_sys::JSON::parse(r#"{"projectId":"p1","title":"draft"}"#).unwrap();
    store
        .update_local_state("task".into(), "t1".into(), fields)
        .await
        .expect("update_local_state");
    let got = store
        .read_local_state("task".into(), "t1".into())
        .await
        .expect("read_local_state");
    assert!(!got.is_null(), "updateLocalState upserts and reads back");
}
