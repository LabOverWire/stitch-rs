//! In-browser smoke test for the wasm `Store`. Drives the in-memory store
//! through create/read/snapshot and registers a subscription, proving the
//! `mqdb-wasm`-backed core runs under real WebAssembly. Run with
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
    let store = stitch_wasm::create_store(config()).expect("create_store");
    store.initialize().await.expect("initialize");

    let first = js_sys::JSON::parse(r#"{"title":"first"}"#).unwrap();
    let id = store
        .create("task".into(), "p1".into(), first)
        .await
        .expect("create first");

    let second = js_sys::JSON::parse(r#"{"title":"second"}"#).unwrap();
    store
        .create("task".into(), "p1".into(), second)
        .await
        .expect("create second");

    let got = store.read("task".into(), id).await.expect("read");
    assert!(!got.is_null(), "a created row must be readable back");

    let snapshot = store
        .snapshot("task".into(), "p1".into())
        .await
        .expect("snapshot");
    let rows = js_sys::Array::from(&snapshot);
    assert_eq!(rows.length(), 2, "snapshot must contain both created tasks");
}

#[wasm_bindgen_test]
async fn scope_isolation_in_browser() {
    let store = stitch_wasm::create_store(config()).expect("create_store");
    store.initialize().await.expect("initialize");

    let a = js_sys::JSON::parse(r#"{"title":"a"}"#).unwrap();
    store.create("task".into(), "p1".into(), a).await.unwrap();
    let b = js_sys::JSON::parse(r#"{"title":"b"}"#).unwrap();
    store.create("task".into(), "p2".into(), b).await.unwrap();

    let p1 = js_sys::Array::from(&store.snapshot("task".into(), "p1".into()).await.unwrap());
    let p2 = js_sys::Array::from(&store.snapshot("task".into(), "p2".into()).await.unwrap());
    assert_eq!(p1.length(), 1, "scope p1 sees only its own task");
    assert_eq!(p2.length(), 1, "scope p2 sees only its own task");
}

#[wasm_bindgen_test]
async fn subscribe_registers_in_browser() {
    let store = stitch_wasm::create_store(config()).expect("create_store");
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

    let store = stitch_wasm::create_store(config()).expect("create_store");
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
        .create("task".into(), "p1".into(), row)
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
