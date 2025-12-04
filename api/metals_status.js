// api/metals_status.js
import pg from "pg";

const { Pool } = pg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

export default async function handler(req, res) {
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
    // Latest date in metals_curve_latest
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
        ok: false,
        message: "No rows in metals_curve_latest",
      });
    }

    const { as_of_date, total_rows } = latest.rows[0];

    // Breakdown by metal for that date
    const perMetal = await client.query(
      `
        SELECT metal, COUNT(*) AS rows_per_metal
        FROM metals_curve_latest
        WHERE as_of_date = $1
        GROUP BY metal
        ORDER BY metal
      `,
      [as_of_date]
    );

    const perMetalMap = {};
    for (const row of perMetal.rows) {
      perMetalMap[row.metal] = Number(row.rows_per_metal);
    }

    return res.json({
      ok: true,
      as_of_date,
      total_rows: Number(total_rows),
      per_metal: perMetalMap,
    });
  } catch (err) {
    console.error("metals_status error:", err);
    return res.status(500).json({ error: "unhandled_exception" });
  } finally {
    client.release();
  }
}
