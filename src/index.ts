import { fork } from 'node:child_process'
import type { ChildProcess } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { createClient } from './db.js'
import { migrateAndRebuildState } from './migrate.js'
import { WORKER_COUNT } from './config.js'

const workerPath = fileURLToPath(new URL('./worker.js', import.meta.url))
const children = new Map<number | undefined, { child: ChildProcess; slot: number }>()
let shuttingDown = false
let failureCode = 0

async function hasActiveStores(): Promise<boolean> {
  const client = createClient()
  await client.connect()

  try {
    const result = await client.query<{ has_active: boolean }>(`
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
  } finally {
    await client.end()
  }
}

function startWorker(slot: number): void {
  const child = fork(workerPath, [], {
    env: {
      ...process.env,
      STOCKSTREAM_WORKER_SLOT: `${slot}`,
    },
  })

  children.set(child.pid, { child, slot })
  console.log(`[supervisor] started worker slot=${slot} pid=${child.pid}`)

  child.on('exit', async (code: number | null, signal: NodeJS.Signals | null) => {
    children.delete(child.pid)

    if (shuttingDown) {
      return
    }

    if (code === 0) {
      try {
        if (await hasActiveStores()) {
          startWorker(slot)
          return
        }
      } catch (error) {
        console.error('[supervisor] failed checking active stores after worker exit', error)
        failFast(1)
        return
      }

      if (children.size === 0) {
        console.log('[supervisor] all processable work is complete')
        process.exit(0)
      }
      return
    }

    if (signal) {
      console.warn(`[supervisor] worker slot=${slot} pid=${child.pid} exited by ${signal}`)
      try {
        if (await hasActiveStores()) {
          startWorker(slot)
          return
        }
      } catch (error) {
        console.error('[supervisor] failed checking active stores after signaled worker exit', error)
        failFast(1)
        return
      }

      if (children.size === 0) {
        console.log('[supervisor] all processable work is complete')
        process.exit(0)
      }
      return
    }

    console.error(
      `[supervisor] worker slot=${slot} pid=${child.pid} failed with code=${code} signal=${signal}`,
    )
    failFast(code ?? 1)
  })
}

function failFast(code: number): void {
  if (shuttingDown) {
    return
  }

  shuttingDown = true
  failureCode = code

  for (const { child } of children.values()) {
    child.kill('SIGTERM')
  }

  process.exitCode = failureCode
  setTimeout(() => process.exit(failureCode), 500).unref()
}

function stopAll(signal: NodeJS.Signals): void {
  shuttingDown = true
  const code = signal === 'SIGINT' ? 130 : 143
  process.exitCode = code

  for (const { child } of children.values()) {
    child.kill(signal)
  }

  setTimeout(() => process.exit(code), 500).unref()
}

async function main(): Promise<void> {
  await runStartupMigration()

  process.on('SIGINT', () => stopAll('SIGINT'))
  process.on('SIGTERM', () => stopAll('SIGTERM'))

  for (let slot = 1; slot <= WORKER_COUNT; slot += 1) {
    startWorker(slot)
  }
}

async function runStartupMigration(): Promise<void> {
  const client = createClient()
  await client.connect()

  try {
    await client.query('BEGIN')

    try {
      await migrateAndRebuildState(client)
    } catch (error) {
      await client.query('ROLLBACK')
      throw error
    }

    await client.query('COMMIT')
  } finally {
    await client.end()
  }
}

main().catch((error) => {
  console.error('[supervisor]', error)
  process.exit(1)
})
