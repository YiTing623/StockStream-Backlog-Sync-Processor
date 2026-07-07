# StockStream Backlog Sync Processor

Node.js backlog processor using TypeScript, Postgres, and `pg`.

## Requirements

- Node >= 20
- Docker / Docker Compose
- Postgres is started through Docker Compose

Default database URL:

```text
postgres://postgres:postgres@localhost:5439/stockstream
```

Set `DATABASE_URL` to override it.

## Install and Run

```bash
docker compose up -d
npm install
npm run seed
npm start
npm run verify
```

`npm start` builds TypeScript, runs the startup migration, starts one supervisor,
and launches exactly 4 worker OS processes. Workers coordinate only through
Postgres row locks.

## Scripts

- `npm run seed` - reset and seed normal input data.
- `npm run seed -- --poison` - reset and seed input data with one permanently failed receipt upload.
- `npm run build` - compile TypeScript into `dist/`.
- `npm start` - build, migrate, supervise, and run 4 workers.
- `npm run verify` - check the final database state.
- `npm run worker` - run a single worker process directly.

## Poison Run

```bash
npm run seed -- --poison
npm start
npm run verify
```

One store is terminally blocked at the failed receipt. Other stores continue to
completion.

## Worker Death / Restart Test

This is a manual chaos check. Exact process matching can vary by shell and OS.

```bash
npm run seed
npm start &
sleep 2
pkill -f "dist/src/worker.js"
wait
npm start
npm run verify
```

## Design Summary

The processor treats each store as an ordered stream. It only applies
`store_stock.last_seq + 1`, waits for `receipt_files.ready_at`, and blocks only
the affected store for `ready_at IS NULL` or corrupt input. It inserts
`apply_log` and updates `store_stock` in one transaction.

## Notes

- `seed.js` and `schema.sql` are intentionally not modified.
- `dist/` is build output and is not committed.
