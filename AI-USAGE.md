# AI Usage

I used AI tools during this assignment for planning, implementation assistance, review, and documentation support.

## Where AI was used

I used AI to help reason through the system design before implementation, especially around the failure modes in the assignment:

1. Queue arrival order is not the same as store event order.
2. Duplicate queue rows may represent gateway retransmits.
3. Receipt files may be delayed or permanently failed.
4. Multiple worker processes need to coordinate only through Postgres.
5. Worker death must not leave `store_stock` and `apply_log` inconsistent.
6. A broken store must not block unrelated stores.
7. Startup must be restart safe because processor owned metadata can survive reseeding.

I also used AI coding assistance to generate parts of the initial implementation, including the migration, worker loop, supervisor, verifier, README, and DESIGN draft. I reviewed and revised the generated code manually rather than accepting it as final.

## What AI generated

AI helped generate the first versions of:

1. The database migration for processor owned tables and indexes.
2. The stock transformation helper.
3. The store serialized worker loop.
4. The four worker supervisor.
5. The independent verification script.
6. README and DESIGN documentation drafts.

The implementation was then reviewed section by section. I checked the database schema, transaction boundaries, store selection logic, duplicate handling, receipt handling, worker restart behavior, and verifier correctness before committing.

## What I reviewed or reworked manually

I manually reviewed the design and code against the assignment requirements and made targeted corrections.

Important areas I reviewed included:

1. Whether workers claim store streams rather than individual queue rows.
2. Whether each worker recomputes `next_seq` from `store_stock.last_seq` inside the transaction.
3. Whether `apply_log` insert and `store_stock` update happen atomically.
4. Whether delayed receipts are treated as pending work instead of completion.
5. Whether `ready_at IS NULL` blocks only the affected store.
6. Whether duplicate queue rows are applied only once.
7. Whether the verifier independently replays the queue instead of trusting the worker result.
8. Whether documentation accurately describes the implemented behavior.

## Places where AI was wrong and I caught it

One early AI generated design suggested applying delta `unset` before `set`, which would make `set` win if the same key appeared in both collections. I changed the implementation and documentation to apply `payload.set` first and `payload.unset` second, so `unset` wins consistently in both the worker and verifier.

Another AI generated version of the worker still inserted into a legacy `processor_store_blocks.seq` column after the migration had been changed to use `blocked_seq`. That would have crashed in poison or block cases. I caught the mismatch by inspecting the database schema and updated the worker to insert into the current `processor_store_blocks` columns only.

The verifier initially checked `apply_log` uniqueness and total count but did not verify that the exact expected audit rows were present. I strengthened it to compare each expected applied update by `message_id`, `chain_id`, `store_id`, `seq`, and `kind`, and to fail on missing, unexpected, or mismatched audit rows.

## Other corrections made during review

Beyond the specific AI mistakes above, I made several smaller corrections during review:

1. I updated the migration so missing `store_stock` rows are initialized with `ON CONFLICT DO NOTHING`, while preserving existing `store_stock` and `apply_log` rows for restart safety.
2. I cleaned up the processor-owned `processor_store_blocks` table after an earlier version left legacy columns such as `seq` and `blocked_at`. Since this table is diagnostic metadata, the migration now recreates it with the intended schema.
3. I wrapped startup migration in a transaction in both the direct migration command and the supervisor start path, so a partial migration failure does not leave processor metadata half rebuilt.
4. I adjusted supervisor signal handling so a worker killed by signal does not cause the parent process to fail if all processable work is already complete.
5. I updated `.gitignore` to exclude `dist/`, because TypeScript build output should not be committed.
6. I revised README and DESIGN wording where the initial drafts did not precisely match the implementation.


## Validation performed

After reviewing and revising the AI assisted implementation, I ran the processor and verifier locally.

Normal seed result:

verified 24 stores, 1887 applied updates, 0 blocked stores

Poison seed result:

verified 24 stores, 1822 applied updates, 1 blocked store

These checks were used to confirm that the implementation handles normal processing, duplicate retransmits, delayed receipts, terminal receipt failure, and exact audit log reconciliation.