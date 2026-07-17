# StockStream Backlog Sync Processor

## Architecture Review and Senior Architect Discussion Guide

**Review scope:** repository `main` at commit `be67552`  
**System type:** finite-backlog processor  
**Primary technologies:** Node.js 20+, TypeScript, PostgreSQL 16, and the `pg` driver

## 1. Executive Overview

StockStream reconstructs current store stock from snapshots and deltas in a
shared backlog. Because offline buffering, interleaving, and retransmission make
arrival order unreliable, correctness follows each store's contiguous `seq`.

PostgreSQL provides durability and the only coordination channel for four worker
OS processes. Workers claim store streams and advance one sequence per
transaction, recording the audit row and stock change atomically.

This is a strong assignment-scale batch processor for a fixed backlog with
delayed or failed receipts. It is not a production-ready continuous service:
the high-water mark is captured at startup, telemetry is minimal, some corrupt
data stops the fleet, and automated verification is incomplete.

The labels **Implemented behavior**, **Assessment**, and **Recommendation**
distinguish current code from analysis and future work. Source code is the
behavioral source of truth; existing documents provide intent and historical
evidence.

### Scope Boundary

The application drains a finite backlog. At startup it calculates `max(seq)`
for every known store and exits when each store reaches that high-water mark or
is terminally blocked. New stores and sequences above the captured maximum are
not scheduled. A row inserted later inside an existing captured range can still
be read if it arrives before the worker reaches or blocks that sequence.

Receipt readiness can change during the run: a future `ready_at` remains pending
and is polled. That does not imply continuous queue discovery.

## 2. Requirements Traceability

| Requirement | Implemented approach | Evidence | Assessment |
| --- | --- | --- | --- |
| Rebuild accurate stock | Process only `store_stock.last_seq + 1`; snapshots replace stock and deltas mutate it | `src/worker.ts:247-338`, `src/stock.ts:8-51` | Sound for the captured backlog and accepted payloads |
| Ignore queue arrival order | Schedule by `(chain_id, store_id)` and load the exact next `seq` | `src/worker.ts:67-97`, `src/worker.ts:148-173` | Queue `id` is used only to choose a canonical retransmit row |
| Trustworthy audit trail | Insert `apply_log` and update `store_stock` in one transaction | `src/worker.ts:175-245`, `src/worker.ts:247-343` | Strong atomicity; corrupt identity handling needs refinement |
| Exactly four OS workers | Supervisor forks slots 1 through 4 | `src/config.ts:4`, `src/index.ts:36-45`, `src/index.ts:128-137` | Meets the explicit process requirement |
| PostgreSQL-only coordination | Workers use row locks with `FOR UPDATE SKIP LOCKED` | `src/worker.ts:67-97` | No shared memory, IPC scheduling, or local coordination files |
| Receipt compliance | Apply only when `ready_at <= now()`; defer future receipts; block `NULL` receipts | `src/worker.ts:148-173`, `src/worker.ts:306-334` | Correct for the receipt states modeled by the assignment |
| Worker-death resilience | PostgreSQL rolls back uncommitted work; supervisor replaces signaled workers when work remains | `src/index.ts:47-97`, `src/worker.ts:247-343` | Protects stock/audit consistency across process death |
| Store-level isolation | Lock and advance one store stream independently | `src/worker.ts:67-97`, `src/worker.ts:287-338` | Holds for recognized block cases, but not malformed payloads |
| Safe completion | Exit successfully when no unblocked store remains below its startup `max_seq` | `src/worker.ts:51-65`, `src/index.ts:13-34` | Terminally blocked stores count as finished work |
| Duplicate retransmission | Group rows for a store/sequence by `message_id` and apply the first row when the ID is shared | `src/worker.ts:287-306` | Prevents repeat application but does not prove row equivalence |
| No ORM or job framework | Use parameterized SQL through direct `pg` clients; each worker keeps one long-lived client | `src/db.ts:1-7`, `src/worker.ts:345-364`, `package.json:16-18` | Meets the implementation constraint |

## 3. System Architecture

### 3.1 Context and Components

```mermaid
flowchart LR
    upstream["POS gateways and receipt uploader<br/>(outside this repository)"]

    subgraph postgres["PostgreSQL 16 — data and worker coordination"]
        inputs["queue_messages<br/>receipt_files"]
        durable["store_stock<br/>apply_log"]
        metadata["processor_store_state<br/>processor_store_blocks"]
    end

    supervisor["Supervisor<br/>src/index.ts"]
    migration["Migration and state rebuild<br/>src/migrate.ts"]
    workers["Exactly 4 worker processes<br/>src/worker.ts"]
    transform["Stock transformation<br/>src/stock.ts"]
    verifier["Database replay and reconciliation<br/>scripts/verify.ts"]

    upstream --> inputs
    supervisor --> migration
    migration --> inputs
    migration --> metadata
    migration --> durable
    supervisor --> workers
    workers --> inputs
    workers --> metadata
    workers --> durable
    workers --> transform
    verifier --> inputs
    verifier --> metadata
    verifier --> durable
```

The application has no HTTP server. `index.ts` owns process lifecycle,
`migrate.ts` owns processor schema and scheduling bootstrap, `worker.ts` owns
transactional orchestration, and `stock.ts` owns transformations. `verify.ts`
replays and reconciles database results. PostgreSQL is the integration boundary;
command-line programs are the operational boundary.

### 3.2 Database Ownership and Contracts

The database is both the source of work and the concurrency control plane.

| Table | Ownership | Contract |
| --- | --- | --- |
| `queue_messages` | Provided input | Backlog rows assumed stable during a run; immutability is not enforced. `id` is arrival order, while correctness uses store-local `seq`. |
| `receipt_files` | Provided input | Receipt readiness. A future timestamp is pending; `NULL` means permanent upload failure. |
| `store_stock` | Durable processor output | Current materialized stock, applied-update count in `version`, and authoritative progress in `last_seq`. |
| `apply_log` | Durable processor output | Billing/compliance audit row for each committed update, including the worker identity. |
| `processor_store_state` | Processor-owned metadata | Startup high-water mark, current block state, completion timestamp, and scheduling timestamp for each store. |
| `processor_store_blocks` | Processor-owned diagnostics | Terminal block reason and optional message, receipt, and detail fields for the current run. |

`store_stock.last_seq` is the durable progress source of truth.
`processor_store_state` is disposable coordination metadata rebuilt from the
queue and current stock at application startup. This lets a sequential restart
continue after committed work instead of replaying from zero.

Two unique indexes backstop the audit invariants:

- `apply_log(message_id)` permits one audit row for a logical message ID.
- `apply_log(chain_id, store_id, seq)` permits one audit row for a store-local
  sequence.

Queue indexes support store/sequence/message lookup and receipt joins. These
objects are created by `migrateAndRebuildState` in `src/migrate.ts:5-109`.

## 4. Runtime Processing Model

### 4.1 Startup

`npm start` compiles and launches the supervisor, which rebuilds state in one
transaction before forking workers:

1. Ensure processor tables, columns, audit constraints, and queue indexes exist.
2. Recreate block diagnostics and clear scheduling state.
3. Initialize missing stock rows, capture each store's `max(seq)`, and mark
   already-drained stores complete.
4. Commit, then fork four workers.

Durable `store_stock` and `apply_log` rows are preserved, so a sequential restart
continues committed work. Blocks are rediscovered because diagnostic and
scheduling rows are recreated.

### 4.2 One Store-Sequence Transaction

```mermaid
sequenceDiagram
    participant S as Supervisor
    participant W as Worker
    participant DB as PostgreSQL

    S->>DB: Rebuild scheduling state in one transaction
    S->>W: Fork worker slots 1 through 4

    loop One store-local sequence per transaction
        W->>DB: BEGIN
        W->>DB: Lock one eligible store using SKIP LOCKED
        W->>DB: Lock store_stock and recompute next_seq
        W->>DB: Load all rows and receipts for next_seq

        alt Missing sequence, conflicting IDs, or NULL receipt
            W->>DB: Record a store-local terminal block
            W->>DB: COMMIT
        else Receipt is not ready yet
            W->>DB: Update scheduling timestamp only
            W->>DB: COMMIT
        else Receipt is ready
            W->>W: Validate and transform stock
            alt Payload is valid
                W->>DB: Insert apply_log
                W->>DB: Update store_stock and processor state
                W->>DB: COMMIT
            else Payload is malformed
                W->>DB: ROLLBACK
                W-->>S: Exit with application error
                S-->>W: Terminate the remaining worker fleet
            end
        end
    end
```

A worker locks one unfinished store with `FOR UPDATE ... SKIP LOCKED`; ready
stores are preferred and `updated_at` supplies secondary fairness. It then locks
`store_stock` and recomputes `next_seq = last_seq + 1`, preventing stale
scheduling reads while other stores advance independently.

The worker then loads all queue rows for that exact store and sequence:

- No rows produce `missing_seq`.
- Distinct message IDs produce `corrupt_seq_conflict`.
- One shared ID is treated as retransmission; the earliest row is canonical.
- `ready_at IS NULL` produces `receipt_permanent_failure`.
- `ready_at > now()` leaves the sequence pending.
- A ready message is transformed and committed.

### 4.3 Store Lifecycle

```mermaid
stateDiagram-v2
    [*] --> Active: Startup state rebuild
    Active --> Active: Commit next ready sequence
    Active --> Waiting: Next receipt is in the future
    Waiting --> Active: Poll after receipt becomes ready
    Active --> Blocked: Missing seq, conflicting IDs, or failed receipt
    Active --> Completed: last_seq reaches max_seq
    Blocked --> [*]: No processable work in this run
    Completed --> [*]: No remaining work

    note right of Waiting
        Waiting is inferred from receipt time;
        it is not a persisted status enum.
    end note
```

A restart rebuilds the lifecycle. A block is rediscovered or clears after data
repair; a completed store stays complete when durable `last_seq` still reaches
the rebuilt high-water mark.

### 4.4 Stock Semantics

`applyMessage` implements two deterministic operations:

- A `snapshot` requires an object at `payload.stock` and replaces the complete
  current stock.
- A `delta` begins with current stock, applies every key in `payload.set`, and
  then removes every key in `payload.unset`. Therefore, an unset wins when the
  same key appears in both collections.

The implementation validates that the snapshot stock and delta `set` containers
are objects and that `unset` is an array. Individual set values remain
unrestricted, and unset elements are not validated as strings.

## 5. Correctness and Failure Guarantees

### 5.1 Principal Invariants

1. **Store order:** a store advances only from `last_seq` to `last_seq + 1`.
   Queue arrival order never determines business order.
2. **Store serialization:** one locked processor state row represents ownership
   of a store stream for the duration of a transaction.
3. **Durable progress:** `store_stock.last_seq`, not ephemeral scheduling
   metadata, determines the next sequence after restart.
4. **Atomic stock and audit:** the audit insert and stock update commit or roll
   back together.
5. **Uniqueness:** database indexes reject a second audit row for the same
   message ID or store sequence.
6. **Receipt gating:** the selected canonical row is not applied until its
   receipt has a non-null `ready_at` no later than database time.
7. **Isolation of recognized blocks:** missing sequence, conflicting message IDs
   at one store sequence, and permanent receipt failure stop only that store.
8. **Finite completion:** a run ends when every captured store is complete or
   terminally blocked.

The phrase “exactly once” should be used carefully. The code provides one
committed stock/audit outcome for each accepted store sequence across worker
retries and process death. It does not establish that all rows sharing a
message ID are semantically identical, and invalid identity reuse can turn a
uniqueness violation into a fleet-wide application failure.

### 5.2 Crash Matrix

| Failure point | Database outcome | Recovery behavior |
| --- | --- | --- |
| Before a worker acquires a store | No state change | Another worker can select the store |
| After locks but before writes | Transaction rolls back and locks are released | Store is immediately eligible again |
| After `apply_log` insert but before stock update | Audit insert rolls back | No orphan billing/audit record remains |
| After stock update but before commit | Stock and audit both roll back | Durable `last_seq` remains unchanged |
| Immediately after commit | Stock and audit are both durable | Restart begins at the following sequence |
| Worker killed by a signal | PostgreSQL closes the session and releases locks | Supervisor starts a replacement if active work remains |
| Worker exits with application error | Its transaction rolls back | Supervisor terminates the other workers and exits nonzero |
| Whole application restarts | Scheduling metadata is rebuilt | Durable stock/audit progress is preserved |

## 6. Interfaces and Operational Contract

### 6.1 Command-Line Interface

| Command | Behavior |
| --- | --- |
| `npm run seed` | Reset and seed the provided normal database fixture |
| `npm run seed -- --poison` | Seed one permanently failed receipt |
| `npm run build` | Compile TypeScript into ignored `dist/` output |
| `npm run migrate` | Build and run migration/state rebuilding directly |
| `npm start` | Build, migrate, supervise, and run exactly four workers |
| `npm run worker` | Build and run one worker directly; primarily diagnostic |
| `npm run verify` | Build and reconcile database results against a replay |

### 6.2 Configuration and Process Behavior

- `DATABASE_URL` is the only environment-level application configuration. Its
  development default is
  `postgres://postgres:postgres@localhost:5439/stockstream`.
- `WORKER_COUNT` is hard-coded to `4`.
- `IDLE_SLEEP_MS` is hard-coded to `250`.
- Worker audit identity is `pid:<process-id>/slot:<slot>`.
- Workers exit normally when no active stores remain.
- A worker application error exits with code 1 and causes supervisor fail-fast.
- Supervisor `SIGINT` and `SIGTERM` handling propagates the signal and uses
  conventional exit codes 130 and 143.
- Exit code 0 includes runs with terminally blocked stores. The completion log
  does not report their count, so automation must query block state to
  distinguish full coverage from partial coverage.

### 6.3 Explicitly Absent Interfaces

There is no HTTP API, message-broker client, object-storage client, health or
readiness endpoint, structured metrics endpoint, or application Docker image.
`docker-compose.yml` starts only the development PostgreSQL database. Receipt
upload behavior is represented by database timestamps rather than by direct
object-storage access.

## 7. Candid Architecture Assessment

### 7.1 Strengths

- Store-scoped serialization matches the business ordering boundary.
- PostgreSQL locks coordinate workers and recover naturally after process death.
- Stock and audit atomicity is enforced in one transaction.
- Durable progress is separated from rebuildable scheduling state.
- Delayed receipts remain pending; recognized permanent failures are isolated.
- Later snapshots replace stock as ordinary ordered events.

### 7.2 Prioritized Risk Register

`P0` affects the stated correctness contract, `P1` affects assurance or robust
operation, and `P2` affects production maturity or scale. Identity-related P0
items can be discharged only by an explicit, enforced upstream invariant.

| Priority | Risk and implemented behavior | Impact | Evidence and recommended direction |
| --- | --- | --- | --- |
| P0 | Malformed payloads throw from `applyMessage`; the worker exits nonzero and the supervisor kills all workers | One bad store can stop unrelated stores, contradicting the isolation goal | `src/stock.ts:8-51`, `src/worker.ts:336-368`, `src/index.ts:93-114`; convert deterministic input errors to a store-local quarantine/block |
| P0 | Retransmits are considered equivalent solely because their `message_id` matches | Differing payload, kind, or receipt data can be silently resolved by earliest queue arrival | `src/worker.ts:287-306`; compare all logical fields before choosing a canonical row |
| P0 | Reusing a message ID at another store or sequence reaches the global unique index during apply | The insert fails and becomes a fleet-wide application error instead of classified corruption | `src/migrate.ts:45-53`, `src/worker.ts:184-197`; define identity semantics and quarantine inconsistent reuse |
| P1 | Store set and `max_seq` are captured only during startup | New stores and higher sequences are ignored; repairing an already blocked stream requires restart | `src/migrate.ts:94-109`; retain this boundary for batch mode or add deliberate live discovery |
| P1 | Startup drops block diagnostics and rebuilds state without an advisory or leadership lock | Concurrent or rolling supervisors can contend with or disrupt an active fleet | `src/migrate.ts:29-65`, `src/index.ts:128-157`; serialize startup and use versioned migrations |
| P1 | Block diagnostics are recreated on every start | Operational history and evidence of repeated poison data are lost | `src/migrate.ts:29-43`; separate durable block history from current scheduling state |
| P1 | Database connection and query errors are not classified or retried | A transient database or network interruption can stop the run | `src/db.ts:5-6`, `src/index.ts:93-114`; add bounded retry/backoff and explicit fatal/transient classification |
| P1 | Signal exits respawn immediately; shutdown forces parent exit after 500 ms; no child `error` handler is registered | Repeated OOM/signals can cause a crash loop, and shutdown may not observe child cleanup | `src/index.ts:47-125`; add bounded restart policy and awaited shutdown |
| P1 | Exit 0 includes blocked stores but reports only generic completion | A scheduler can mistake partial stock coverage for a fully synchronized estate | `src/index.ts:66-89`; emit a completion summary and machine-readable blocked count |
| P1 | The verifier imports `applyMessage` and mirrors worker grouping, receipt, and block decisions | Shared defects can make processor and oracle agree incorrectly | `scripts/verify.ts:2-3`, `scripts/verify.ts:90-127`; use an independent oracle and hostile expectations |
| P1 | No unit, integration, deterministic chaos, or CI suite exists | Important hostile paths are implemented but not continuously proven | `package.json:8-15`, `README.md:53-65`; automate domain, database, restart, and collision fixtures |
| P2 | Future receipts are polled and write `updated_at` every deferred attempt | Database churn and store-selection sorting may become expensive at large scale | `src/worker.ts:67-94`, `src/worker.ts:322-334`; benchmark and schedule the next eligible receipt time |
| P2 | Console logging exists, but structured progress logging, metrics, tracing, packaging, and health reporting do not | Operators cannot readily measure backlog age, block causes, or fleet health | `src/index.ts`, `src/worker.ts`, `docker-compose.yml`; introduce telemetry and deployment contracts |
| P2 | One URL/role performs runtime DML and startup drop/alter DDL; local Compose publishes development credentials and a host port; TLS and timeouts are not explicit | The runtime requires excessive privilege and development defaults are unsuitable for production | `src/db.ts:5-6`, `src/migrate.ts:19-70`, `docker-compose.yml:3-8`; split roles and require production-safe connection settings |

### 7.3 Documentation-to-Code Discrepancy

`README.md` summarizes the processor as blocking only the affected store for a
missing receipt or corrupt input. The implementation does that for
`ready_at IS NULL`, missing sequences, and multiple distinct message IDs at one
sequence. It does **not** do so for a malformed snapshot or delta payload.
Those errors propagate out of the worker and cause supervisor-wide fail-fast.

This should be corrected in code or narrowed in the README before claiming
general corrupt-input isolation.

## 8. Recommended Evolution

### P0 — Correctness and Contract Alignment

1. Convert deterministic validation failures to a diagnostic store-local block.
2. Compare identity, store/sequence, kind, canonical payload, and receipt
   semantics before accepting retransmits; block disagreement.
3. Define and enforce whether `message_id` is global or scoped before audit
   insertion.
4. Align README and design claims with the implemented corruption policy.

### P1 — Assurance, Recovery, and Safe Operation

1. Add an independent, table-driven domain oracle and hostile database fixtures
   for malformed payloads, gaps, identity conflicts, partial progress, future
   receipts, repairs, and alternate seeds.
2. Automate process death before and after transaction commit.
3. Replace ad hoc startup DDL with versioned migrations and protect the
   migration/state-rebuild phase with a PostgreSQL advisory or equivalent
   singleton lock.
4. Define durable block history and an explicit repair/resume workflow.
5. Add bounded retry with jitter for transient connection failures while
   retaining fail-fast behavior for deterministic application defects.

### P2 — Production Readiness and Scale

1. Add structured logs and metrics for backlog age, apply rate, waits, blocks,
   latency, restarts, and completion.
2. Add CI, an application image, batch health semantics, secrets/TLS/timeouts,
   and least-privilege roles.
3. Benchmark selection, transaction, and polling cost; schedule the next
   eligible receipt time if polling writes become material.
4. Add continuous discovery only for an explicit live-service requirement,
   because it changes completion, topology, retention, and repair semantics.

## 9. Decisions for the Senior Architect Discussion

| Decision | Why it matters | Recommended starting position |
| --- | --- | --- |
| Fixed backlog or continuous ingestion? | Determines high-water marks, completion, deployment lifetime, and late-arrival handling | Keep the current finite-run contract for this assignment; design continuous intake as a separate product requirement |
| What is the corruption policy? | Store order prevents safely skipping an untrusted sequence without a business rule | Quarantine/block the store at the first corrupt sequence and preserve diagnostics for operator repair |
| How are blocked streams repaired? | Current blocks are rediscovered only after restart | Persist block history and require an explicit resume after the underlying message or receipt is corrected |
| What exactly does `message_id` identify? | Global and store-scoped interpretations produce different constraints and collision behavior | Treat it as globally unique if upstream can guarantee it; always retain the independent store/sequence uniqueness invariant |
| May more than one supervisor run? | Current startup rebuild is not safe as a rolling multi-replica deployment | Enforce a singleton startup/runtime contract until multi-replica lifecycle behavior is deliberately designed |
| What scale and SLO are expected? | The current query and polling strategy is proven only at assignment scale | Define stores, events, receipt-delay distribution, backlog completion time, and recovery objectives before optimizing |
| What evidence is required for compliance? | Ephemeral blocks and limited audit verification may not meet retention expectations | Preserve immutable audit and block history with explicit retention and reconciliation requirements |

## 10. Verification Evidence and Gaps

### Current Review Evidence

- `npm run build` passed during this architecture review.
- The repository was clean before authoring this document.
- Static review traced the implementation through the supervisor, migration,
  worker, transformation, verifier, schema, seed generator, and run
  documentation.
- Docker was not running in the review environment, so the database integration
  flow and manual worker-death experiment could not be rerun.

### Historical Repository Evidence

The following values are reported by `DESIGN.md` and `AI-USAGE.md`; they are
historical project results, not newly reproduced results from this review:

- Normal seed: 24 stores, 1,887 applied updates, and 0 blocked stores.
- Poison seed: 24 stores, 1,822 applied updates, and 1 blocked store.

The verifier currently checks:

- Final stock JSON, `version`, and `last_seq` for every store.
- Exact audit identity fields: message ID, chain, store, sequence, and kind.
- Duplicate audit rows by both message ID and store sequence.
- Block-row presence plus `blocked_seq` and `reason`.

It is not a fully independent oracle: it shares `applyMessage` and mirrors the
worker's grouping, receipt, and block decisions. It also omits audit
`worker_id`/`applied_at`, detailed block fields, `completed_at`, receipt timing,
per-store `apply_pos` ordering, hostile fixtures, and deterministic
inside-transaction process death.

## Appendix A — Repository Map

| Path | Role |
| --- | --- |
| `ASSIGNMENT.md` | Business requirements, operating constraints, data reference, and evaluation criteria |
| `README.md` | Local runbook and concise design summary |
| `DESIGN.md` | Original design rationale, rejected alternatives, and historical verification results |
| `AI-USAGE.md` | AI assistance disclosure and documented manual corrections |
| `schema.sql` | Provided base database schema |
| `seed.js` | Deterministic fixture generator |
| `src/index.ts` | Supervisor and process lifecycle |
| `src/migrate.ts` | Processor schema additions and scheduling-state rebuild |
| `src/worker.ts` | Store selection and transactional processing |
| `src/stock.ts` | Snapshot/delta domain transformation |
| `scripts/verify.ts` | Database result reconciliation |
| `docker-compose.yml` | Local PostgreSQL 16 service |

## Appendix B — Local Runbook

```bash
docker compose up -d
npm install
npm run seed
npm start
npm run verify
```

Poison-receipt scenario:

```bash
npm run seed -- --poison
npm start
npm run verify
```

## Appendix C — Terminology

| Term | Meaning |
| --- | --- |
| Chain | Company grouping multiple stores |
| Store stream | Ordered events for one `(chain_id, store_id)` |
| Snapshot | Complete replacement of a store's stock at one sequence |
| Delta | Partial stock mutation with `set` and `unset` |
| Receipt | Compliance evidence whose readiness gates application |
| Retransmit | Duplicate queue delivery of the same logical message |
| High-water mark | Maximum store sequence captured during startup |
| Terminal block | Store-local condition that cannot progress during the current run |
| Durable progress | Committed `store_stock.last_seq` and its corresponding audit history |

## Appendix D — Key Evidence Index

- Worker count and polling: `src/config.ts:4-5`
- Supervisor lifecycle: `src/index.ts:36-137`
- Transactional migration startup: `src/index.ts:139-157`
- Processor tables and indexes: `src/migrate.ts:5-109`
- Store selection and `SKIP LOCKED`: `src/worker.ts:67-97`
- Duplicate and receipt classification: `src/worker.ts:287-334`
- Atomic audit and stock write: `src/worker.ts:175-245`
- Worker transaction boundary: `src/worker.ts:247-343`
- Snapshot and delta semantics: `src/stock.ts:8-55`
- Verification behavior: `scripts/verify.ts:53-344`
- Seeded workload characteristics: `seed.js:17-25`, `seed.js:78-117`
