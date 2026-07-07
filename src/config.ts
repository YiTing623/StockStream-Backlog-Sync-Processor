export const DATABASE_URL =
  process.env.DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5439/stockstream'

export const WORKER_COUNT = 4
export const IDLE_SLEEP_MS = 250
