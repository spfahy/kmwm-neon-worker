// /api/dbinfo.js
import { Pool } from "pg";

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  application_name: "KMWM-DBInfo",
});

export default async function handler(req, res) {
  try {
    const client = await pool.connect();
    try {
      const id  = await client.query(
        `select current_database() as db,
                current_user     as role,
                current_schema() as schema`
      );
      const cnt = await client.query(
        `select count(*)::int as rows from public.positions`
      );
      res.status(200).json({
        ok: true,
        ...id.rows[0],
        positions_rows: cnt.rows[0].rows,
      });
    } finally {
      client.release();
    }
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
}
