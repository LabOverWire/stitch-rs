# stitch-wasm

Browser (`wasm-bindgen`) bindings over [`stitch-sync`](https://crates.io/crates/stitch-sync):
a `createStore` factory and a `Store` class for JavaScript. A drop-in for the
framework-agnostic core of the TypeScript
[`@laboverwire/stitch`](https://github.com/LabOverWire/stitch).

The browser build runs the full stack: an in-memory cache, durable IndexedDB
persistence (plaintext or AES-GCM encrypted) via `mqdb-wasm`, remote MQTT sync
over WebSocket via `mqtt5-wasm` with MQTT v5 JWT enhanced-auth, and a durable
offline queue (writes made while disconnected persist and replay on reconnect).

## Usage (JavaScript)

```js
import init, { createStore } from "./pkg/stitch_wasm.js";

await init();

const store = createStore(
  {
    entities: { project: { fields: [/* ... */] } },
    scope: { rootEntity: "project", childEntities: ["task"], scopeField: "projectId" },
  },
  {
    persistence: { dbName: "app", passphrase: "optional-aes-gcm-key" },
    remote: { url: "wss://broker.example/mqtt", clientId: "tab-1", ticket: "<JWT>" },
  },
);

await store.initialize();

const unsub = store.subscribeToScope("p1", (data, op) => render(data, op));
await store.create("project", "p1", { id: "p1", name: "Alpha" });
await store.replaceScope("p1");
```

`remote.url` is a `ws://`/`wss://` MQTT endpoint; `remote.ticket` is a JWT used
for MQTT v5 enhanced-auth.

## API surface

The `Store` mirrors the TS core:

- **CRUD**: `create`, `read`, `update`, `delete`
- **Reads**: `list`, `listRootEntities`, `getChildCount`, `getSnapshot`,
  `getSnapshotAsMap`, `readLocalState`
- **Scope**: `replaceScope`, `closeScope`
- **Subscriptions** (each returns an unsubscribe fn): `subscribeToEntity`
  (`(data, op)`), `subscribeToScope`, `subscribeToConnectionStatus`
- **Connection**: `initialize`, `connectionStatus`, `disconnect`, `reconnect`,
  `isReconnecting`, `ready`
- **Batch**: `beginBatch`, `endBatch`
- **Misc**: `request`, `updateLocalState`, `setAuthenticatedUser`,
  `pendingMutationCount`, `resetForLogout`, `destroy`
- **Capabilities**: `hasPersistence`, `hasRemote`

## Build & test

```sh
wasm-pack build crates/stitch-wasm                       # produces pkg/
wasm-pack test --headless --chrome crates/stitch-wasm    # in-browser smoke test
```

The headless suite cannot host a broker, so it asserts the wasm client builds
and fails gracefully when the broker is unreachable; the native broker-backed
`stitch-sync` tests cover the sync engine itself.

## License

Apache-2.0 — see [LICENSE](./LICENSE).
