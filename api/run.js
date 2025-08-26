// trigger deploy 
// redeploy
import pg from "pg";
import Papa from "papaparse";

const { Client } = pg;

// AC = column 28 (0-based). A = column 0.
const COL_TICKER = 0;
const COL_PRICE_AC = 28;

// 15-minute bucket helper (UTC)
function currentMinuteBucketUTC(date = new Date()) {
  const m = date.getUTCMinutes();
  const bucketMin = m - (m % 15);
  return new Date(Date.UTC(
    date.getUTCFullYear(),
    date.getUTCMonth(),
    date.getUTCDate(),
    date.getUTCHours(),
    bucketMin, 0, 0
  ));
}

export default async function handler(req, res) {
  const started = Date.now();
  const fromCron = req.query.tag || null;

  // --- fetch CSV from Google Sheets ---
  const csvUrl = process.env.PRICE_CSV_URL;
  if (!csvUrl) {
    return res.status(500).json({ ok: false, error: "Missing PRICE_CSV_URL env var" });
  }

  let csvText;
  try {
    const r = await fetch(csvUrl, { cache: "no-store" });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    csvText = await r.text();
  } catch (err) {
    return res.status(500).json({ ok: false, error: `Fetch failed: ${String(err)}` });
  }

  // --- parse CSV (no headers) ---
  const parsed = Papa.parse(csvText.trim(), { header: false, dynamicTyping: false });
  if (parsed.errors?.length) {
    return res.status(500).json({ ok: false, error: `CSV parse errors: ${parsed.errors[0].message}` });
  }
  const rows = parsed.data || [];
  if (rows.length < 2) {
    return res.status(400).json({ ok: false, error: "No data rows found in CSV" });
  }

  // --- connect to Neon ---
  const client = new Client({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
  });

  // SQL DDL (idempotent)
  const ddl = `
  CREATE TABLE IF NOT EXISTS prices_latest (
    ticker TEXT PRIMARY KEY,
    price NUMERIC NOT NULL,
    asof_ts TIMESTAMPTZ NOT NULL DEFAULT now(),
    source TEXT DEFAULT 'gsheet'
  );

  CREATE TABLE IF NOT EXISTS prices_history (
    ticker TEXT NOT NULL,
    price NUMERIC NOT NULL,
    asof_ts TIMESTAMPTZ NOT NULL,
    minute_bucket TIMESTAMPTZ NOT NULL,
    source TEXT DEFAULT 'gsheet',
    PRIMARY KEY (ticker, minute_bucket)
  );
  `;

  const upsertLatestSQL = `
    INSERT INTO prices_latest (ticker, price, asof_ts, source)
    VALUES ($1, $2, now(), 'gsheet')
    ON CONFLICT (ticker) DO UPDATE
    SET price = EXCLUDED.price,
        asof_ts = EXCLUDED.asof_ts,
        source = EXCLUDED.source;
  `;

  const insertHistorySQL = `
    INSERT INTO prices_history (ticker, price, asof_ts, minute_bucket, source)
    VALUES ($1, $2, now(), $3, 'gsheet')
    ON CONFLICT DO NOTHING;
  `;

  let processed = 0, skipped = 0;

  try {
    await client.connect();
    await client.query(ddl);

    const minuteBucket = currentMinuteBucketUTC();

    // Iterate data rows (skip header row 0)
    for (let i = 1; i < rows.length; i++) {
      const row = rows[i];
      if (!row || !row.length) { skipped++; continue; }

      // ticker in col A
      const rawTicker = (row[COL_TICKER] ?? "").toString().trim();
      const ticker = rawTicker.toUpperCase();

      // price in col AC
      const rawPrice = (row[COL_PRICE_AC] ?? "").toString().replace(/[$,\s]/g, "");
      const price = parseFloat(rawPrice);

      if (!ticker || !isFinite(price)) { skipped++; continue; }

      // Upsert latest
      await client.query(upsertLatestSQL, [ticker, price]);
      // Append history (15-min bucket)
      await client.query(insertHistorySQL, [ticker, price, minuteBucket]);

      processed++;
    }

    const ms = Date.now() - started;
    return res.status(200).json({
      ok: true,
      message: "Prices ingested",
      processed,
      skipped,
      minute_bucket_utc: minuteBucket.toISOString(),
      runtime_ms: ms,
      tag: fromCron || null
    });

  } catch (err) {
    return res.status(500).json({ ok: false, error: String(err) });
  } finally {
    await client.end().catch(() => {});
  }
}



