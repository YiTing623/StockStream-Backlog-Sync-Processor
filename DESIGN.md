# Design

## Overview

The processor treats each store as an ordered stream. A worker does not claim
`queue_messages` rows as independent jobs. Instead, it selects a store stream,
locks the store-local state, recomputes the next required sequence from
`store_stock.last_seq`, and processes only `store_stock.last_seq + 1`.

This model keeps ordering local to each `(chain_id, store_id)` stream. One
broken, delayed, or blocked store does not prevent unrelated stores from
advancing. Different stores can progress independently as long as each store's
own sequence is applied in order.

## Risks Identified

- Queue arrival order is not store event order.
  - Found from the spec. The queue has arrival rows, but correctness depends on
    store-local `seq`, not queue `id`.
- Duplicate retransmitted queue rows.
  - Found from the spec and seeded data inspection. Retransmits can create
    multiple queue rows for the same logical message.
- Multiple distinct `message_id` values for the same store seq.
  - Found from failure reasoning. This makes the logical event ambiguous, so the
    affected store must be blocked instead of guessed.
- Receipt `ready_at` in the future.
  - Found from the spec and seeded data inspection. The message exists but is
    not yet ready to apply.
- Receipt `ready_at IS NULL`.
  - Found from the spec and poison seeded data. This is treated as a terminal
    missing receipt for that store-local seq.
- Worker death between audit insert and stock update.
  - Found from failure reasoning. `apply_log` and `store_stock` must not become
    inconsistent across crashes.
- Processor-owned metadata surviving `seed.js`.
  - Found from inspecting seeded-data behavior. Startup must rebuild processor
    metadata because `seed.js` does not clear every processor-owned table.
- One broken store blocking unrelated stores.
  - Found from failure reasoning. Store-local blocking must not become global
    processor blocking.
- Future receipts causing premature completion.
  - Found from failure reasoning. A future receipt means work is pending, not
    complete.
- Gateway restart causing later snapshots in the same stream.
  - Found from the spec and failure reasoning. Snapshots are normal stream
    events and must be applied in sequence, replacing current stock at that seq.

## Database Additions

`processor_store_state` stores processor metadata for each store stream,
including `max_seq` and block/completion metadata. It is used for worker
coordination and for deciding when a store is completed or terminally blocked.
It is not the progress source of truth.

`store_stock.last_seq` is the progress source of truth. Workers recompute the
next sequence from `store_stock.last_seq` inside the transaction before applying
work. Startup rebuilds `processor_store_state` from the queue, but it does not
truncate or reset `store_stock`.

`processor_store_blocks` records terminal store-local blocks, including the
blocked sequence and reason. It is processor-owned diagnostic metadata and is
cleared on startup. If a restarted run reaches the same terminal condition
again, the worker records the block again. Durable progress remains in
store_stock and apply_log; processor_store_state is rebuilt from the queue and
current store_stock state.

`apply_log` has a unique constraint on `message_id` so one logical message is
audited once even if duplicate queue rows are retransmitted. It also has a
unique constraint on `(chain_id, store_id, seq)` so only one logical event can
be applied for a store-local sequence. Multiple different `message_id` values
for the same `(chain_id, store_id, seq)` are treated as
`corrupt_seq_conflict`.

Queue indexes support the worker access pattern: finding per-store `max_seq`,
loading rows for a specific `(chain_id, store_id, seq)`, and joining queue rows
to receipts. The processor avoids scanning or claiming queue rows by arrival
order.

On startup, the processor rebuilds `processor_store_state` from `queue_messages` and the current `store_stock` state. It clears `processor_store_blocks` because that table is diagnostic metadata; if a terminal condition is encountered again after restart, the worker records the block again. Startup does not truncate `store_stock` or `apply_log`, because those tables are durable processing output and are needed for restart correctness and idempotence.

## Worker Algorithm

Each worker repeatedly selects one eligible store stream by locking a
`processor_store_state` row with `FOR UPDATE SKIP LOCKED`. That gives workers
database-backed coordination while allowing other workers to process other
stores.

Inside the same transaction, the worker locks and reads the matching
`store_stock` row, then recomputes `next_seq` as `store_stock.last_seq + 1`.
The worker loads all queue rows for that store and `next_seq`.

If all rows for the seq have the same `message_id`, the rows are treated as
retransmits of the same message and one canonical message is applied. If the
same `(chain_id, store_id, seq)` has multiple distinct `message_id` values, the
store is terminally blocked with `corrupt_seq_conflict`.

Receipt handling is part of the store-local decision:

- `ready_at > now` means the store should wait. The worker leaves the seq
  unapplied and releases the lock so other stores can run.
- `ready_at IS NULL` means the store is terminally blocked.
- A ready receipt allows the worker to apply the message.

Message semantics are deterministic. A snapshot replaces the store's stock at
that sequence. A delta applies `payload.set` first and `payload.unset` second,
so an unset wins if the same key appears in both collections.

## Transaction Boundary and Crash Behavior

One database transaction handles one store-local seq. For ready work, the
`apply_log` insert and the `store_stock` update are committed atomically.

If a worker dies before commit, Postgres rolls back both the audit insert and
the stock update. If a worker dies after commit, both records are visible. This
prevents an applied stock update without an audit row, and prevents an audit row
for stock that was not advanced.

Postgres releases row locks when a worker process dies or its connection is
closed. Other workers can then continue from durable state. The supervisor may
replace signaled workers, but it fails fast on application errors instead of
silently retrying faulty code forever.

## Completion Criteria

The processor exits with status 0 when every store stream is either completed
through its `max_seq` or terminally blocked. Completion is store-local and based
on durable progress from `store_stock.last_seq` plus terminal block metadata.

Future receipts are not completion. A store waiting on `ready_at > now` still
has pending work and must be revisited later. A poisoned store does not prevent
other stores from completing; it is recorded as blocked while unrelated stores
continue to advance.

## Alternatives Rejected

- Processing by queue `id`.
  - Rejected because queue arrival order is not store event order.
- Claiming queue rows directly.
  - Rejected because claiming independent rows could apply seq `N+1` before
    seq `N` for the same store.
- In-memory or IPC locks.
  - Rejected because worker coordination must survive process death and work
    across OS worker processes through Postgres.
- Using `processor_store_state` as progress truth.
  - Rejected because restart correctness depends on durable applied output in
    `store_stock.last_seq`.
- Guessing among conflicting messages.
  - Rejected because distinct messages for the same store seq make the audit
    record untrustworthy.
- Treating `ready_at NULL` as retryable forever.
  - Rejected because it would leave a poisoned store in an infinite pending
    state.
- Treating future `ready_at` as done.
  - Rejected because future receipts represent delayed work, not completion.
- One global lock.
  - Rejected because one delayed or broken store would block all unrelated
    stores.

## Verification

The independent verifier replays `queue_messages` joined to `receipt_files`. It
groups messages by store, sorts each stream by `seq`, and applies the same
stock semantics as the processor: snapshots replace stock, and deltas apply
`set` before `unset`.

The verifier compares final `store_stock` contents, `version`, and `last_seq`.
It compares exact `apply_log` rows by `message_id`, `chain_id`, `store_id`,
`seq`, and `kind`. It checks duplicate handling and validates
`processor_store_state` and `processor_store_blocks` for blocked stores.

Observed local results:

- Normal seed: verified 24 stores, 1887 applied updates, 0 blocked stores.
- Poison seed: verified 24 stores, 1822 applied updates, 1 blocked store.

Useful verification commands:

```bash
npm run seed
npm start
npm run verify

npm run seed -- --poison
npm start
npm run verify
```

## Known Limitations

- Polling is used instead of `LISTEN/NOTIFY`.
- Conflict handling blocks the affected store instead of attempting automatic
  repair.
- The verifier is assignment-focused, not production monitoring.
