# Response from the mqdb side — optimistic-locking (CAS) on `Database::update`/`delete`

Thanks for the precise write-up. We investigated and confirmed your reading of
the race, fixed it, and found one additional backend bug that matters to you.
All `cargo make dev` (format + clippy-pedantic + ~1047 tests) is green, including
new concurrency regression tests.

## TL;DR

- **No API change.** We implemented your request #2 (bounded internal retry) instead
  of an opt-out write-mode. `update` now resolves same-process self-conflicts
  transparently and converges to field-level last-writer-wins. Your stall should
  disappear with no stitch change required.
- **`Error::Conflict` stays a dedicated variant** reserved for the CAS case — your
  one hard dependency is honored.
- **Finding 3 is real and fixed**, but the mechanism was `delete`, not a losing-CAS
  `update` (details below).
- **New: the persistent (fjall) backend had a non-atomic CAS** (silent lost updates).
  Fixed. Your reported `Error::Conflict` means you are on the in-memory backend,
  which was already correct — but read the fjall section before you ever switch.

## The `Error::Conflict` guarantee (no change needed)

`mqdb_core::error::Error::Conflict(String)` remains a distinct variant. Every
construction site across all backends (memory, fjall, encrypted, wasm/indexeddb)
is the CAS "optimistic lock failed" case — it is never folded into validation or
generic storage errors, and `transport.rs` maps it to a dedicated
`ErrorCode::Conflict` on the wire. Match on it freely.

## #1 / #2 — what we shipped

We chose the **bounded internal retry** (your #2) over an opt-out LWW write-mode,
because the retry is strictly better: it keeps CAS protection (no lost updates
across the read→commit gap) *and* converges to field-level last-writer-wins,
whereas a "skip `expect_value`" write-mode would clobber whole records (the merge
is computed from a stale read).

- `Database::update` is now a thin wrapper that calls `try_update_once` and retries
  on `Error::Conflict` up to `MAX_WRITE_ATTEMPTS` (32).
- **Re-read-on-retry confirmed:** each attempt re-reads the latest committed value
  at the top of `try_update_once` and re-applies your partial-field merge on top.
  Concurrent updates to *different* fields all survive; concurrent updates to the
  *same* field resolve to the last committer.
- **Caveat — vault path:** retry is skipped when `update_constraint_data` is `Some`
  (vault-encrypted entities), because that plaintext merge is precomputed upstream
  against a now-stale read and cannot be re-derived inside `update`. Those still
  surface a typed `Conflict`. This does not affect stitch unless you enable vault
  encryption.
- **Backstop:** after 32 failed attempts under pathological same-key contention,
  `Error::Conflict` is returned. Keep your app-layer match as the backstop; your
  `updatedAt`/`version` LWW reconciliation remains valid and complementary.

## #3 (Finding 3) — confirmed, but the cause was `delete`

Your hypothesis that *"a losing-CAS `update` can leave a stale index pointer"* is
not what happens. `update` writes the data row, the index changes, and the CAS
precondition into **one batch**; a precondition failure aborts the *entire* batch
atomically, so a losing update writes nothing — it cannot strand an index entry.

The real leak was `delete`, which had **no `expect_value`**. A delete that read an
older version committed unconditionally; if a concurrent `update` had already
changed an *indexed* field and committed first, the delete removed only the *old*
index key (plus the data row) and left the *new* index key dangling → the
`index pointed to non-existent entity` warning, persistently.

Fix: `delete` now CAS-guards the primary entity and retries on conflict. On a
conflict it re-reads the fresh entity and removes the *current* index keys, so no
dangling pointer remains.

Two scope notes:
- Cascade / set-null **child** rows are not individually CAS-guarded — the reported
  `pending_sync/<id>` is the primary entity; per-child CAS is a larger, separate
  change we did not take on here.
- There is also a **benign transient** source of the same warning: a `list` snapshots
  index ids, then a concurrent atomic delete removes the row before the per-id read.
  The read path logs the warning and skips the entry; it self-heals and leaves no
  persistent stale index. You may still see this occasionally under churn — it is
  harmless and is *not* a leak.

## New finding — the persistent (fjall) backend had a non-atomic CAS

This is the most important thing for you to know.

Your reported `Error::Conflict` tells us you are on the **in-memory** backend,
whose CAS is correct: the data write lock spans the precondition check *and* the
apply, so it is atomic.

The **persistent fjall backend was not.** Its `commit` checked preconditions
against a snapshot, then applied in a *separate* write batch — a classic
check-then-act. Under concurrency, two commits could both pass their CAS check and
both apply: a **silent lost update with no `Conflict` returned**, and the same race
could strand index entries. We confirmed this empirically (a concurrent-update test
lost a write with no error) and fixed it by serializing the check+apply with a
commit lock (the fsync stays outside the lock, so durability cost is unchanged).

Implication for stitch: if you ever move the embedded backend from in-memory to
persistent/fjall, the CAS now behaves identically to in-memory. **Before this fix,
fjall would not have surfaced your conflict at all — it would have dropped a write
silently**, which is worse than the stall you reported.

## Version note (correction)

The optimistic-locking behavior did **not** appear in the 0.8.4 → 0.8.7 window.
Backend CAS enforcement has existed since 2025-11-30; the `update` path has called
`expect_value` since before the workspace split (≤ 2026-03-19) — both well before
v0.8.4 (2026-05-25). Your 2000-task / 10-worker scale simply started hitting a
pre-existing race; this is not a recent regression.

## Files changed (mqdb)

- `crates/mqdb-agent/src/database/crud.rs` — `update`/`delete` retry wrappers,
  `delete` primary-entity CAS, concurrency regression tests.
- `crates/mqdb-core/src/storage/fjall_backend.rs` — atomic CAS via commit lock.

## What we suggest on the stitch side

Nothing is required for the stall — the internal retry handles same-process
self-conflicts. Keep your `Error::Conflict` match and LWW reconciliation as the
backstop (cap-exhaustion + the vault path). If you are weighing a move to a
persistent embedded backend, you are now safe to do so with respect to CAS.
