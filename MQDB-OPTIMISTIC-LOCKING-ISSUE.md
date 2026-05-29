# mqdb optimistic-locking (CAS) on `Database::update` — questions + requests from the stitch side

From the stitch owner, following the chorale AWS scale test (2026-05-28,
`clawcode-next/infrastructure/MQDB-STITCH-FINDINGS.md`). stitch embeds
`mqdb-agent` as its in-memory/persistent backend (single process, single
writer). At 2000-task / 10-worker scale, `Database::update` started returning
`concurrent modification conflict: optimistic lock failed: value was modified`,
stalling the application. We are fixing the stitch side (retry + LWW-aware
apply); this issue captures what we need from mqdb and the design questions it
raises.

## What we observe

`mqdb_agent::database::crud::Database::update` (`crates/mqdb-agent/src/database/crud.rs`):

- L168 reads `existing_data`.
- L194 `schema_registry.read().await`, L219 `constraint_manager.read().await`,
  L250 `dispatcher.dispatch(event).await` — multiple `.await` points between the
  read and the commit.
- L228 `batch.expect_value(key, existing_data)` then L248 `batch.commit()` — an
  unconditional compare-and-swap on the previously-read serialized bytes.

Because there are await points between the read and the CAS commit, two
concurrent `update` futures targeting the **same key** both read version N, both
compute the merge, the first commits, and the second's `expect_value` no longer
matches → `Error::Conflict("optimistic lock failed: value was modified")`
(produced in `crates/mqdb-core/src/storage/{memory,fjall,encrypted}_backend.rs`,
typed as `mqdb_core::error::Error::Conflict` at `crates/mqdb-core/src/error.rs:42`).

In stitch's embedded use the record is **single-owner** — there is no
cross-process writer. The conflict is stitch's own sync-apply path racing its
local update API inside one process, surfaced only now that the embedded backend
does CAS. `delete` (crud.rs:260) uses `batch.remove` with no `expect_value`, and
`create` allocates a fresh key, so this is specific to `update`.

## What already works (no change needed)

`mqdb_core::error::Error::Conflict(String)` is already a distinct, typed variant.
stitch will match on it directly (mapping it to stitch's own `Error::Conflict`)
to drive retry/LWW logic. **Please keep this variant stable and reserved for the
CAS-conflict case** — do not fold it into a generic storage/validation error.
That single guarantee is the only hard dependency the stitch fix has on mqdb.

## Requests / questions (in priority order)

### 1. Is the CAS intended for the embedded single-writer backend, or only the broker/multi-writer path?

stitch already performs its own last-write-wins reconciliation at the
application layer (it compares an `updatedAt` timestamp and a `version` field
before applying a remote mutation). For stitch's embedded use, mqdb's CAS is
therefore redundant with — and actively fights — the layer above it: it converts
a benign same-process race into a hard error.

If the CAS is meant for the multi-writer broker case, we'd like the embedded
`Database` to **opt out**. Two acceptable shapes:

- **(preferred) an explicit last-write-wins / upsert write mode** on `update`
  (skip `expect_value`, just read-merge-commit), selectable per call or per
  `Database` instance; or
- a documented guarantee that callers may retry on `Error::Conflict` and that a
  retried `update` re-reads the latest committed value before merging (see #2).

### 2. Consider a bounded internal retry-on-conflict inside `update`

Since `update` is a partial-field merge (crud.rs:176-180) and it re-reads
`existing_data` at the top of each call, a bounded retry loop *inside* `update`
(on `expect_value` failure: re-read, re-merge, re-commit, up to N attempts)
would make same-process self-conflicts resolve transparently and converge to a
field-level last-writer-wins. This would fix the stall for every embedded caller
without each one reimplementing the loop. If you'd rather keep mqdb's `update`
single-shot, that's fine — we'll do the retry on the stitch side — but please
confirm the re-read-on-retry semantics in #1.

### 3. (Finding 3, minor) Index points to non-existent entity under concurrent load

Under the same concurrent write load the broker logs:

```
WARN mqdb_agent::database::query: index pointed to non-existent entity: pending_sync/<id>
```

frequently. It coincides with the CAS churn. Worth checking whether index
entries are written/cleared in a step that isn't covered by the same
`expect_value`-guarded batch, so that a losing-CAS `update` or an interleaved
`delete` can leave a stale index pointer. Possibly benign, but it should not
persist once the CAS churn from #1/#2 is resolved.

## Versions

mqdb moved 0.8.4 → 0.8.5 → 0.8.6 → 0.8.7 during this work; the optimistic-locking
behavior appeared in this window and the stitch/chorale design predates it.
stitch consumes `mqdb-agent` and `mqdb-core` by path from `../../../../MQDB`.

## Minimal reproduction (no AWS)

One embedded `mqdb_agent::Database`: create a row, then fire two `update` calls
on that same key concurrently (e.g. `tokio::join!`). One returns
`Error::Conflict("optimistic lock failed: value was modified")`. In stitch this
maps to a create-echo sync-apply racing a local `Store::update` on the same row.
