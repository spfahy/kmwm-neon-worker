// api/metals_status.js
// Read-only status endpoint so Sheets can validate that Neon has today's metals curve.

import pg from "pg";

const { Pool } = pg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

// Get today's date in America/Chicago as YYYY-MM-DD
function getTodayCT() {
  const now = new Date();
  const ctString = now.toLocaleString("en-US", { timeZone: "America/Chicago" });
  const ctDate = new Date(ctString);
  const y = ctDate.getFullYear();
  const m = String(ctDate.getMonth() + 1).padStart(2, "0");
  const d = String(ctDate.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

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

  // Optional ?date=YYYY-MM-DD override; default to "today" in CT
  const qDate =
    (req.query.date && String(req.query.date)) || getTodayCT();

  const client = await pool.connect();

  try {
    // 1) Today's curve in metals_curve_latest
    const curveResult = await client.query(
      `
      SELECT
        as_of_date,
        metal,
        tenor_months,
        price,
        real_10yr_yld,
        dollar_index,
        deficit_gdp_flag
      FROM metals_curve_latest
      WHERE as_of_date = $1
      ORDER BY metal, tenor_months
      `,
      [qDate]
    );

    // 2) Most recent ingest log record for that date (if any)
    const logResult = await client.query(
      `
      SELECT
        run_timestamp,
        run_date,
        trigger_source,
        status,
        error_reason,
        row_count
      FROM metals_ingest_log
      WHERE run_date = $1
      ORDER BY run_timestamp DESC
      LIMIT 1
      `,
      [qDate]
    );

    const rows = curveResult.rows || [];
    const lastIngest = logResult.rows[0] || null;

    return res.json({
      ok: true,
      as_of_date: qDate,
      rowCount: rows.length,
      rows,
      lastIngest,
    });
  } catch (err) {
    console.error("metals_status error:", err);
    return res.status(500).json({ error: "unhandled_exception" });
  } finally {
    client.release();
  }
}
