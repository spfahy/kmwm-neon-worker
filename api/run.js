// api/run.js
// Main worker: loads equity prices from Google Sheets into Neon
// and ingests gold/silver metals curve from a separate CSV.

// trigger deploy
import pg from "pg";
import Papa from "papaparse";

const { Client } = pg;

// ----- Price CSV layout (Stocks sheet) -----
// AC = column 28 (0-based). A = column 0.
const COL_TICKER = 0;
const COL_PRICE_AC = 28;

// 15-minute bucket helper (UTC)
function currentMinuteBucketUTC(date = new Date()) {
  const m = date.getUTCMinutes();
  const bucketMin = m - (m % 15);
  return new Date(
    Date.UTC(
      date.getUTCFullYear(),
      date.getUTCMonth(),
      date.getUTCDate(),
      date.getUTCHours(),
      bucketMin,
      0,
      0
    )
  );
}

// ----- Prices loader (existing behaviour, wrapped in a function) -----
async function loadPricesFromSheet(client, logger = console) {
  const csvUrl = process.env.PRICE_CSV_URL;
  if (!csvUrl) {
    throw new Error("Missing PRICE_CSV_URL env var");
  }

  logger.log("Fetching prices CSV from:", csvUrl);

  let csvText;
  try {
    const r = await fetch(csvUrl, { cache: "no-store" });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    csvText = await r.text();
  } catch (err) {
    throw new Error(`Price fetch failed: ${String(err)}`);
  }

  const parsed = Papa.parse(csvText.trim(), {
    header: false,
    dynamicTyping: false,
  });
  if (parsed.errors?.length) {
    throw new Error(`CSV parse errors (prices): ${parsed.errors[0].message}`);
  }

  const rows = parsed.data || [];
  if (rows.length < 2) {
    throw new Error("No data rows found in prices CSV");
  }

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

  let processed = 0,
    skipped = 0;

  await client.query(ddl);

  const minuteBucket = currentMinuteBucketUTC();

  // Iterate data rows (skip header row 0)
  for (let i = 1; i < rows.length; i++) {
    const row = rows[i];
    if (!row || !row.length) {
      skipped++;
      continue;
    }

    // ticker in col A
    const rawTicker = (row[COL_TICKER] ?? "").toString().trim();
    const ticker = rawTicker.toUpperCase();

    // price in col AC
    const rawPrice = (row[COL_PRICE_AC] ?? "")
      .toString()
      .replace(/[$,\s]/g, "");
    const price = parseFloat(rawPrice);

    if (!ticker || !isFinite(price)) {
      skipped++;
      continue;
    }

    await client.query(upsertLatestSQL, [ticker, price]);
    await client.query(insertHistorySQL, [ticker, price, minuteBucket]);

    processed++;
  }

  logger.log(`Prices load complete: processed=${processed}, skipped=${skipped}`);

  return {
    processed,
    skipped,
    minute_bucket_utc: minuteBucket.toISOString(),
  };
}

// ----- Metals loader (Metallink) -----
const METALS_REQUIRED_COLS = [
  "as of date",
  "metal",
  "tenor months",
  "price",
];

async function loadMetalsCurveFromSheet(client, logger = console) {
  const url = process.env.METALS_CSV_URL;
  if (!url) {
    logger.warn("METALS_CSV_URL not set, skipping metals load");
    return {
      processed: 0,
      skipped: 0,
      skipped_reason: "METALS_CSV_URL missing",
    };
  }

  logger.log("Fetching metals CSV from:", url);

  let csvText;
  try {
    const r = await fetch(url, { cache: "no-store" });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    csvText = await r.text();
  } catch (err) {
    throw new Error(`Metals fetch failed: ${String(err)}`);
  }

  const parsed = Papa.parse(csvText.trim(), {
    header: false,
    dynamicTyping: false,
  });
  if (parsed.errors?.length) {
    throw new Error(`CSV parse errors (metals): ${parsed.errors[0].message}`);
  }

  const rows = parsed.data || [];
  if (rows.length <
