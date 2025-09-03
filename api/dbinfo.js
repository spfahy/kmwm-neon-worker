import { Pool } from 'pg';

export default async function handler(req, res) {
  const pool = new Pool({ connectionString: process.env.DATABASE_URL });

  try {
    const { rows } = await pool.query(`
      select
        current_user    as db_user,
        current_database() as db_name,
        current_schema  as schema,
        current_setting('search_path') as search_path
    `);

    // Mask password but show host/db part
    const url = process.env.DATABASE_URL || '';
    const masked = url.replace(/:\/\/([^:]+):([^@]+)@/, '://$1:***@');

    res.status(200).json({
      dbinfo: rows?.[0] ?? {},
      database_url_hint: masked,
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  } finally {
    await pool.end();
  }
}
