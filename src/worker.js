import { createClient } from './db.js'
import { IDLE_SLEEP_MS } from './config.js'
import { applyMessage } from './stock.js'

const workerSlot = process.env.STOCKSTREAM_WORKER_SLOT ?? 'unknown'
const workerId = `pid:${process.pid}/slot:${workerSlot}`

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

async function hasActiveStores(client) {
  const result = await client.query(`
    SELECT EXISTS (
      SELECT 1
      FROM processor_store_state s
      LEFT JOIN store_stock ss
        ON ss.chain_id = s.chain_id
       AND ss.store_id = s.store_id
      WHERE s.blocked_seq IS NULL
        AND coalesce(ss.last_seq, 0) < s.max_seq
    ) AS has_active
  `)

  return result.rows[0].has_active
}

async function selectStore(client) {
  const result = await client.query(`
    SELECT
      s.chain_id,
      s.store_id,
      s.max_seq,
      coalesce(ss.last_seq, 0) AS last_seq,
      coalesce(ss.last_seq, 0) + 1 AS next_seq
    FROM processor_store_state s
    LEFT JOIN store_stock ss
      ON ss.chain_id = s.chain_id
     AND ss.store_id = s.store_id
    WHERE s.blocked_seq IS NULL
      AND coalesce(ss.last_seq, 0) < s.max_seq
    ORDER BY
      EXISTS (
        SELECT 1
        FROM queue_messages q
        JOIN receipt_files r ON r.id = q.receipt_id
        WHERE q.chain_id = s.chain_id
          AND q.store_id = s.store_id
          AND q.seq = coalesce(ss.last_seq, 0) + 1
          AND r.ready_at <= now()
      ) DESC,
      s.updated_at ASC
    LIMIT 1
    FOR UPDATE OF s SKIP LOCKED
  `)

  return result.rows[0] ?? null
}

async function blockStore(
  client,
  store,
  reason,
  { details = {}, messageId = null, receiptId = null } = {},
) {
  await client.query(
    `
      UPDATE processor_store_state
      SET blocked_seq = $3,
          blocked_reason = $4,
          updated_at = now()
      WHERE chain_id = $1
        AND store_id = $2
    `,
    [store.chain_id, store.store_id, store.next_seq, reason],
  )

  await client.query(
    `
      INSERT INTO processor_store_blocks (
        chain_id,
        store_id,
        blocked_seq,
        reason,
        message_id,
        receipt_id,
        details
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
      ON CONFLICT (chain_id, store_id) DO UPDATE
      SET blocked_seq = EXCLUDED.blocked_seq,
          reason = EXCLUDED.reason,
          message_id = EXCLUDED.message_id,
          receipt_id = EXCLUDED.receipt_id,
          details = EXCLUDED.details
    `,
    [
      store.chain_id,
      store.store_id,
      store.next_seq,
      reason,
      messageId,
      receiptId,
      JSON.stringify(details),
    ],
  )
}

async function loadMessagesForSeq(client, store) {
  const result = await client.query(
    `
      SELECT
        q.id,
        q.message_id,
        q.chain_id,
        q.store_id,
        q.seq,
        q.kind,
        q.payload,
        q.receipt_id,
        r.ready_at,
        r.ready_at <= now() AS receipt_ready
      FROM queue_messages q
      JOIN receipt_files r ON r.id = q.receipt_id
      WHERE q.chain_id = $1
        AND q.store_id = $2
        AND q.seq = $3
      ORDER BY q.id ASC
    `,
    [store.chain_id, store.store_id, store.next_seq],
  )

  return result.rows
}

async function applyReadyMessage(client, store, stockRow, message) {
  const currentStock = stockRow?.stock ?? {}
  const nextStock = applyMessage(currentStock, message)

  await client.query(
    `
      INSERT INTO apply_log (message_id, chain_id, store_id, seq, kind, worker_id)
      VALUES ($1, $2, $3, $4, $5, $6)
    `,
    [
      message.message_id,
      message.chain_id,
      message.store_id,
      message.seq,
      message.kind,
      workerId,
    ],
  )

  if (stockRow) {
    const updateResult = await client.query(
      `
        UPDATE store_stock
        SET stock = $3::jsonb,
            version = version + 1,
            last_seq = $4,
            updated_at = now()
        WHERE chain_id = $1
          AND store_id = $2
          AND last_seq = $5
      `,
      [
        store.chain_id,
        store.store_id,
        JSON.stringify(nextStock),
        store.next_seq,
        stockRow.last_seq,
      ],
    )

    if (updateResult.rowCount !== 1) {
      throw new Error(
        `Expected to update one store_stock row for ${store.chain_id}/${store.store_id} at seq ${store.next_seq}, updated ${updateResult.rowCount}`,
      )
    }
  } else {
    await client.query(
      `
        INSERT INTO store_stock (chain_id, store_id, stock, version, last_seq)
        VALUES ($1, $2, $3::jsonb, 1, $4)
      `,
      [store.chain_id, store.store_id, JSON.stringify(nextStock), store.next_seq],
    )
  }

  await client.query(
    `
      UPDATE processor_store_state
      SET completed_at = CASE WHEN $3 >= max_seq THEN now() ELSE completed_at END,
          updated_at = now()
      WHERE chain_id = $1
        AND store_id = $2
    `,
    [store.chain_id, store.store_id, store.next_seq],
  )
}

async function processOne(client) {
  await client.query('BEGIN')

  try {
    const store = await selectStore(client)

    if (!store) {
      await client.query('COMMIT')
      return { processed: false, active: await hasActiveStores(client) }
    }

    const stockResult = await client.query(
      `
        SELECT stock, version, last_seq
        FROM store_stock
        WHERE chain_id = $1
          AND store_id = $2
        FOR UPDATE
      `,
      [store.chain_id, store.store_id],
    )
    const stockRow = stockResult.rows[0] ?? null
    const lastSeq = stockRow?.last_seq ?? 0
    store.next_seq = lastSeq + 1

    if (lastSeq >= store.max_seq) {
      await client.query(
        `
          UPDATE processor_store_state
          SET completed_at = coalesce(completed_at, now()),
              updated_at = now()
          WHERE chain_id = $1
            AND store_id = $2
        `,
        [store.chain_id, store.store_id],
      )
      await client.query('COMMIT')
      return { processed: true, active: true }
    }

    const messages = await loadMessagesForSeq(client, store)

    if (messages.length === 0) {
      await blockStore(client, store, 'missing_seq')
      await client.query('COMMIT')
      return { processed: true, active: true }
    }

    const messageIds = new Set(messages.map((row) => row.message_id))
    if (messageIds.size > 1) {
      await blockStore(client, store, 'corrupt_seq_conflict', {
        details: {
          message_ids: [...messageIds].sort(),
        },
      })
      await client.query('COMMIT')
      return { processed: true, active: true }
    }

    const message = messages[0]

    if (message.ready_at === null) {
      await blockStore(
        client,
        store,
        'receipt_permanent_failure',
        {
          messageId: message.message_id,
          receiptId: message.receipt_id,
        },
      )
      await client.query('COMMIT')
      return { processed: true, active: true }
    }

    if (!message.receipt_ready) {
      await client.query(
        `
          UPDATE processor_store_state
          SET updated_at = now()
          WHERE chain_id = $1
            AND store_id = $2
        `,
        [store.chain_id, store.store_id],
      )
      await client.query('COMMIT')
      return { processed: false, active: true }
    }

    await applyReadyMessage(client, store, stockRow, message)
    await client.query('COMMIT')
    return { processed: true, active: true }
  } catch (error) {
    await client.query('ROLLBACK')
    throw error
  }
}

async function main() {
  const client = createClient()
  await client.connect()

  try {
    for (;;) {
      const result = await processOne(client)

      if (!result.active) {
        return
      }

      if (!result.processed) {
        await sleep(IDLE_SLEEP_MS)
      }
    }
  } finally {
    await client.end()
  }
}

main().catch((error) => {
  console.error(`[worker ${workerId}]`, error)
  process.exit(1)
})
