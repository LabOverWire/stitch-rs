//! Platform task-spawning shim. Native uses the multi-thread tokio runtime;
//! wasm uses `wasm_bindgen_futures::spawn_local` (single-threaded, no `Send`
//! requirement). The returned handle abstracts abort-on-drop — a real
//! `tokio::task::JoinHandle` natively, a no-op on wasm where spawned futures end
//! when their input channels close.

use std::future::Future;

/// Reference-counted handle: `Arc` natively (threads cross task boundaries),
/// `Rc` on wasm (single-threaded — the browser handles it wraps are `!Send`, so
/// `Arc` would be misused). Used for the handful of types that hold those
/// `!Send` browser resources; `Send + Sync` data keeps using `Arc` directly.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) type Shared<T> = std::sync::Arc<T>;
#[cfg(target_arch = "wasm32")]
pub(crate) type Shared<T> = std::rc::Rc<T>;

/// A fresh unique id string. Natively a time-ordered UUIDv7; on wasm a random
/// UUIDv4, since `now_v7` reads `SystemTime`, which panics on
/// `wasm32-unknown-unknown`.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn new_id() -> String {
    uuid::Uuid::now_v7().to_string()
}
#[cfg(target_arch = "wasm32")]
pub(crate) fn new_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) type JoinHandle = tokio::task::JoinHandle<()>;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn spawn<F>(future: F) -> JoinHandle
where
    F: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(future)
}

#[cfg(target_arch = "wasm32")]
#[derive(Debug)]
pub(crate) struct JoinHandle;

#[cfg(target_arch = "wasm32")]
impl JoinHandle {
    pub(crate) fn abort(&self) {}

    pub(crate) fn is_finished(&self) -> bool {
        false
    }
}

#[cfg(target_arch = "wasm32")]
pub(crate) fn spawn<F>(future: F) -> JoinHandle
where
    F: Future<Output = ()> + 'static,
{
    wasm_bindgen_futures::spawn_local(future);
    JoinHandle
}

/// Await `future` with a timeout. Native uses `tokio::time`; wasm races it
/// against a `gloo_timers` browser timer (tokio's timer is unavailable on
/// `wasm32-unknown-unknown`). `Err(())` on elapse.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn timeout<F: Future>(
    duration: std::time::Duration,
    future: F,
) -> Result<F::Output, ()> {
    tokio::time::timeout(duration, future).await.map_err(|_| ())
}

#[cfg(target_arch = "wasm32")]
pub(crate) async fn timeout<F: Future>(
    duration: std::time::Duration,
    future: F,
) -> Result<F::Output, ()> {
    use futures::future::{Either, select};
    let millis = u32::try_from(duration.as_millis()).unwrap_or(u32::MAX);
    let timer = std::pin::pin!(gloo_timers::future::TimeoutFuture::new(millis));
    let future = std::pin::pin!(future);
    match select(future, timer).await {
        Either::Left((output, _)) => Ok(output),
        Either::Right(((), _)) => Err(()),
    }
}
