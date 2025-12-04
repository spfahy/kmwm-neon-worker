// api/metals_status.js
import pg from "pg";

const { Pool } = pg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

export default async function handler(req, res) {
  try {
    const auth = req.headers.authorization || "";
    const token = auth.replace("Bearer ", "").trim();

    if (!process.env.METALS_INGEST_TOKEN) {
      return res
        .status(500)
        .json({ error: "METALS_INGEST_TOKEN not configured in environment" });
    }

    if (token !== process.env.METALS_INGEST_TOKEN) {
      return res.status(401).json({ error: "unauthorized" });
    }

    const client = await pool.connect();

    try {
      // Latest date
      const latest = await client.query(
        `
        SELECT as_of_date, COUNT(*) AS total_rows
        FROM metals_curve_latest
        GROUP BY as_of_date
        ORDER BY as_of_date DESC
        LIMIT 1
        `
      );

      if (latest.rows.length === 0) {
        return res.json({
          ok: true,
          as_of_date: null,
          total_rows: 0,
          per_metal: {},
        });
      }

      const row = latest.rows[0];
      const asOf = row.as_of_date;
      const totalRows = Number(row.total_rows) || 0;

      // Per-metal rows
      const perMetalQuery = await client.query(
        `
        SELECT metal, COUNT(*) AS rows_per_metal
        FROM metals_curve_latest
        WHERE as_of_date = $1
        GROUP BY metal
        ORDER BY metal
        `,
        [asOf]
      );

      const perMetal = {};
      for (const m of perMetalQuery.rows) {
        perMetal[m.metal] = Number(m.rows_per_metal) || 0;
      }

      return res.json({
        ok: true,
        as_of_date: asOf,
        total_rows: totalRows,
        per_metal: perMetal,
      });
    } finally {
      client.release();
    }
  } catch (err) {
    console.error("metals_status error:", err);
    return res.status(500).json({ error: "unhandled_exception" });
  }
}
