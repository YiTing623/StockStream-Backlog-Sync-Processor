import { fileURLToPath } from 'node:url'
import type { Client } from 'pg'
import { createClient } from './db.js'

export async function migrateAndRebuildState(client: Client): Promise<void> {
  await client.query(`
    CREATE TABLE IF NOT EXISTS processor_store_state (
      chain_id UUID NOT NULL,
      store_id UUID NOT NULL,
      max_seq INTEGER NOT NULL,
      blocked_seq INTEGER,
      blocked_reason TEXT,
      completed_at TIMESTAMPTZ,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (chain_id, store_id)
    )
  `)

  await client.query('ALTER TABLE processor_store_state ADD COLUMN IF NOT EXISTS chain_id UUID')
  await client.query('ALTER TABLE processor_store_state ADD COLUMN IF NOT EXISTS store_id UUID')
  await client.query('ALTER TABLE processor_store_state ADD COLUMN IF NOT EXISTS max_seq INTEGER')
  await client.query('ALTER TABLE processor_store_state ADD COLUMN IF NOT EXISTS blocked_seq INTEGER')
  await client.query('ALTER TABLE processor_store_state ADD COLUMN IF NOT EXISTS blocked_reason TEXT')
  await client.query('ALTER TABLE processor_store_state ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ')
  await client.query(
    'ALTER TABLE processor_store_state ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now()',
  )

  await client.query('DROP TABLE IF EXISTS processor_store_blocks')

  await client.query(`
    CREATE TABLE processor_store_blocks (
      chain_id UUID NOT NULL,
      store_id UUID NOT NULL,
      blocked_seq INTEGER NOT NULL,
      reason TEXT NOT NULL,
      message_id UUID,
      receipt_id UUID,
      details JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (chain_id, store_id)
    )
  `)

  await client.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS apply_log_message_id_unique
      ON apply_log (message_id)
  `)

  await client.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS apply_log_store_seq_unique
      ON apply_log (chain_id, store_id, seq)
  `)

  await client.query(`
    CREATE INDEX IF NOT EXISTS queue_messages_store_seq_message_idx
      ON queue_messages (chain_id, store_id, seq, message_id)
  `)

  await client.query(`
    CREATE INDEX IF NOT EXISTS queue_messages_receipt_id_idx
      ON queue_messages (receipt_id)
  `)

  await client.query('DELETE FROM processor_store_state')

  await client.query('ALTER TABLE processor_store_state ALTER COLUMN chain_id SET NOT NULL')
  await client.query('ALTER TABLE processor_store_state ALTER COLUMN store_id SET NOT NULL')
  await client.query('ALTER TABLE processor_store_state ALTER COLUMN max_seq SET NOT NULL')
  await client.query('ALTER TABLE processor_store_state ALTER COLUMN updated_at SET NOT NULL')

  await client.query(`
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'processor_store_state'::regclass
          AND contype = 'p'
      ) THEN
        ALTER TABLE processor_store_state ADD PRIMARY KEY (chain_id, store_id);
      END IF;
    END
    $$
  `)

  await client.query(`
    INSERT INTO store_stock (chain_id, store_id, stock, version, last_seq)
    SELECT DISTINCT chain_id, store_id, '{}'::jsonb, 0, 0
    FROM queue_messages
    ON CONFLICT (chain_id, store_id) DO NOTHING
  `)

  await client.query(`
    INSERT INTO processor_store_state (chain_id, store_id, max_seq)
    SELECT chain_id, store_id, max(seq) AS max_seq
    FROM queue_messages
    GROUP BY chain_id, store_id
  `)

  await client.query(`
    UPDATE processor_store_state s
    SET completed_at = now(),
        updated_at = now()
    FROM store_stock ss
    WHERE ss.chain_id = s.chain_id
      AND ss.store_id = s.store_id
      AND ss.last_seq >= s.max_seq
  `)
}

async function main(): Promise<void> {
  const client = createClient()
  await client.connect()

  try {
    await client.query('BEGIN')
    await migrateAndRebuildState(client)
    await client.query('COMMIT')
  } catch (error) {
    await client.query('ROLLBACK')
    throw error
  } finally {
    await client.end()
  }
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  main().catch((error) => {
    console.error(error)
    process.exit(1)
  })
}
