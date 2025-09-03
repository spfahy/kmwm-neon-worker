// /api/dbinfo.js
import { Pool } from 'pg';

export default async function handler(req, res) {
  const pool = new Pool({ connectionString: process.env.DATABASE_URL });
  const q = `
    select
      current_user    as db_user,
      current_database() as db_name,
      current_schema  as schema,
      inet_server_addr()::text as server_ip,
      inet_server_port()      as server_port,
      current_setting('server_version') as server_version,
      current_setting('search_path')    as search_path
  `;
  let info;
  try {
    const { rows } = await pool.query(q);
    info = rows?.[0] ?? {};
  } catch (e) {
    return res.status(500).json({ error: e.message });
  } finally {
    await pool.end();
  }

  // Mask secrets but echo the high-signal bits of DATABASE_URL
  const url = process.env.DATABASE_URL || '';
  const masked = url.replace(/:\/\/([^:]+):([^@]+)@/, '://$1:***@');
  res.status(200).json({ dbinfo: info, database_url_hint: masked });
}
