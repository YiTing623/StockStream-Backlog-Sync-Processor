import pg from 'pg'
import type { Client } from 'pg'
import { DATABASE_URL } from './config.js'

export function createClient(): Client {
  return new pg.Client({ connectionString: DATABASE_URL })
}
