import type { Client } from 'pg'
import { createClient } from '../src/db.js'
import { applyMessage, sameJson, type Stock, type StockMessage } from '../src/stock.js'

type StoreRow = {
  chain_id: string
  store_id: string
  max_seq: number
}

type MessageRow = StockMessage & {
  message_id: string
  chain_id: string
  store_id: string
  seq: number
  kind: string
  payload: unknown
  receipt_id: string | null
  ready_at: Date | null
  receipt_ready: boolean
}

type ExpectedBlock = {
  seq: number
  reason: string
}

type ExpectedStore = {
  stock: Stock
  lastSeq: number
  applied: MessageRow[]
  block: ExpectedBlock | null
}

type BlockedStore = StoreRow & ExpectedBlock

type AuditRow = {
  message_id: string
  chain_id: string
  store_id: string
  seq: number
  kind: string
}

function fail(message: string): never {
  throw new Error(message)
}

function storeKey(store: Pick<StoreRow, 'chain_id' | 'store_id'>): string {
  return `${store.chain_id}/${store.store_id}`
}

async function loadStores(client: Client): Promise<StoreRow[]> {
  const result = await client.query<StoreRow>(`
    SELECT chain_id, store_id, max(seq) AS max_seq
    FROM queue_messages
    GROUP BY chain_id, store_id
    ORDER BY chain_id, store_id
  `)

  return result.rows
}

async function loadMessages(client: Client, store: StoreRow, seq: number): Promise<MessageRow[]> {
  const result = await client.query<MessageRow>(
    `
      SELECT
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
    [store.chain_id, store.store_id, seq],
  )

  return result.rows
}

async function expectedForStore(client: Client, store: StoreRow): Promise<ExpectedStore> {
  let stock: Stock = {}
  let lastSeq = 0
  const applied: MessageRow[] = []
  let block = null

  for (let seq = 1; seq <= store.max_seq; seq += 1) {
    const rows = await loadMessages(client, store, seq)

    if (rows.length === 0) {
      block = { seq, reason: 'missing_seq' }
      break
    }

    const messageIds = new Set(rows.map((row) => row.message_id))
    if (messageIds.size > 1) {
      block = { seq, reason: 'corrupt_seq_conflict' }
      break
    }

    const message = rows[0]

    if (message.ready_at === null) {
      block = { seq, reason: 'receipt_permanent_failure' }
      break
    }

    if (!message.receipt_ready) {
      fail(`store ${store.chain_id}/${store.store_id} still waits for future receipt at seq ${seq}`)
    }

    stock = applyMessage(stock, message)
    lastSeq = seq
    applied.push(message)
  }

  return { stock, lastSeq, applied, block }
}

async function verifyStore(client: Client, store: StoreRow): Promise<ExpectedStore> {
  const expected = await expectedForStore(client, store)

  const stockResult = await client.query<{ stock: Stock; version: number; last_seq: number }>(
    `
      SELECT stock, version, last_seq
      FROM store_stock
      WHERE chain_id = $1
        AND store_id = $2
    `,
    [store.chain_id, store.store_id],
  )
  const actual = stockResult.rows[0]

  if (!actual && expected.applied.length === 0) {
    return expected
  }

  if (!actual) {
    fail(`missing store_stock row for ${store.chain_id}/${store.store_id}`)
  }

  if (actual.last_seq !== expected.lastSeq) {
    fail(
      `last_seq mismatch for ${store.chain_id}/${store.store_id}: got ${actual.last_seq}, expected ${expected.lastSeq}`,
    )
  }

  if (actual.version !== expected.applied.length) {
    fail(
      `version mismatch for ${store.chain_id}/${store.store_id}: got ${actual.version}, expected ${expected.applied.length}`,
    )
  }

  if (!sameJson(actual.stock, expected.stock)) {
    fail(`stock mismatch for ${store.chain_id}/${store.store_id}`)
  }

  return expected
}

async function verifyAudit(client: Client, expectedApplied: MessageRow[]): Promise<void> {
  const duplicateMessage = await client.query<{ message_id: string; count: number }>(`
    SELECT message_id, count(*)::int AS count
    FROM apply_log
    GROUP BY message_id
    HAVING count(*) > 1
    LIMIT 1
  `)

  if (duplicateMessage.rows.length > 0) {
    fail(`duplicate apply_log message_id ${duplicateMessage.rows[0].message_id}`)
  }

  const duplicateStoreSeq = await client.query<{
    chain_id: string
    store_id: string
    seq: number
    count: number
  }>(`
    SELECT chain_id, store_id, seq, count(*)::int AS count
    FROM apply_log
    GROUP BY chain_id, store_id, seq
    HAVING count(*) > 1
    LIMIT 1
  `)

  if (duplicateStoreSeq.rows.length > 0) {
    const row = duplicateStoreSeq.rows[0]
    fail(`duplicate apply_log store seq ${row.chain_id}/${row.store_id}/${row.seq}`)
  }

  const countResult = await client.query<{ count: number }>('SELECT count(*)::int AS count FROM apply_log')
  const actualCount = countResult.rows[0].count
  const expectedAppliedCount = expectedApplied.length

  if (actualCount !== expectedAppliedCount) {
    fail(`apply_log count mismatch: got ${actualCount}, expected ${expectedAppliedCount}`)
  }

  const expectedRows = new Map<string, AuditRow>()

  for (const message of expectedApplied) {
    expectedRows.set(message.message_id, {
      message_id: message.message_id,
      chain_id: message.chain_id,
      store_id: message.store_id,
      seq: message.seq,
      kind: message.kind,
    })
  }

  const actualResult = await client.query<AuditRow>(`
    SELECT message_id, chain_id, store_id, seq, kind
    FROM apply_log
    ORDER BY apply_pos ASC
  `)
  const actualRows = new Map<string, AuditRow>()

  for (const row of actualResult.rows) {
    const expected = expectedRows.get(row.message_id)

    if (!expected) {
      fail(`unexpected apply_log row ${row.message_id}`)
    }

    if (
      row.chain_id !== expected.chain_id ||
      row.store_id !== expected.store_id ||
      row.seq !== expected.seq ||
      row.kind !== expected.kind
    ) {
      fail(
        `apply_log mismatch for ${row.message_id}: got ${row.chain_id}/${row.store_id}/${row.seq}/${row.kind}, expected ${expected.chain_id}/${expected.store_id}/${expected.seq}/${expected.kind}`,
      )
    }

    actualRows.set(row.message_id, row)
  }

  for (const expected of expectedRows.values()) {
    if (!actualRows.has(expected.message_id)) {
      fail(`missing apply_log row ${expected.message_id}`)
    }
  }
}

async function verifyBlocks(client: Client, stores: StoreRow[], expectedBlocks: BlockedStore[]): Promise<void> {
  const expectedByStore = new Map(expectedBlocks.map((block) => [storeKey(block), block]))

  for (const store of stores) {
    const expectedBlock = expectedByStore.get(storeKey(store)) ?? null
    const result = await client.query<{ blocked_seq: number | null; blocked_reason: string | null }>(
      `
        SELECT blocked_seq, blocked_reason
        FROM processor_store_state
        WHERE chain_id = $1
          AND store_id = $2
      `,
      [store.chain_id, store.store_id],
    )
    const actual = result.rows[0]

    if (!actual) {
      fail(`missing processor state for ${store.chain_id}/${store.store_id}`)
    }

    const blockResult = await client.query<{ blocked_seq: number; reason: string }>(
      `
        SELECT blocked_seq, reason
        FROM processor_store_blocks
        WHERE chain_id = $1
          AND store_id = $2
      `,
      [store.chain_id, store.store_id],
    )
    const actualBlock = blockResult.rows[0]

    if (expectedBlock) {
      if (actual.blocked_seq !== expectedBlock.seq || actual.blocked_reason !== expectedBlock.reason) {
        fail(
          `block mismatch for ${store.chain_id}/${store.store_id}: got ${actual.blocked_seq}/${actual.blocked_reason}, expected ${expectedBlock.seq}/${expectedBlock.reason}`,
        )
      }

      if (!actualBlock) {
        fail(`missing processor block row for ${store.chain_id}/${store.store_id}`)
      }

      if (actualBlock.blocked_seq !== expectedBlock.seq || actualBlock.reason !== expectedBlock.reason) {
        fail(
          `processor block mismatch for ${store.chain_id}/${store.store_id}: got ${actualBlock.blocked_seq}/${actualBlock.reason}, expected ${expectedBlock.seq}/${expectedBlock.reason}`,
        )
      }

      continue
    }

    if (actual.blocked_seq !== null || actual.blocked_reason !== null) {
      fail(
        `unexpected processor state block for ${store.chain_id}/${store.store_id}: got ${actual.blocked_seq}/${actual.blocked_reason}`,
      )
    }

    if (actualBlock) {
      fail(
        `unexpected processor block row for ${store.chain_id}/${store.store_id}: got ${actualBlock.blocked_seq}/${actualBlock.reason}`,
      )
    }
  }
}

async function main(): Promise<void> {
  const client = createClient()
  await client.connect()

  try {
    const stores = await loadStores(client)
    let appliedCount = 0
    const applied: MessageRow[] = []
    const blocks: BlockedStore[] = []

    for (const store of stores) {
      const expected = await verifyStore(client, store)
      appliedCount += expected.applied.length
      applied.push(...expected.applied)

      if (expected.block) {
        blocks.push({ ...store, ...expected.block })
      }
    }

    await verifyAudit(client, applied)
    await verifyBlocks(client, stores, blocks)

    console.log(`verified ${stores.length} stores, ${appliedCount} applied updates, ${blocks.length} blocked stores`)
  } finally {
    await client.end()
  }
}

main().catch((error) => {
  console.error(error.message)
  process.exit(1)
})
