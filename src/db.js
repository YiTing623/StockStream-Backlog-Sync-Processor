import pg from 'pg'
import { DATABASE_URL } from './config.js'

export function createClient() {
  return new pg.Client({ connectionString: DATABASE_URL })
}
