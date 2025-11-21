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
    const rawPrice = (row[COL_PRICE_AC] ?? "").toString().replace(/[$,\s]/g, "");
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
    return { processed: 0, skipped: 0, skipped_reason: "METALS_CSV_URL missing" };
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
  if (rows.length < 2) {
    logger.warn("Metals CSV has no data rows");
    return { processed: 0, skipped: 0 };
  }

  const header = rows[0].map((h) => h.toString().trim().toLowerCase());
  const idx = (name) => header.indexOf(name);

  const iAsOf = idx("as of date");
  const iMetal = idx("metal");
  const iTenor = idx("tenor months");
  const iPrice = idx("price");
  const iReal = idx("10 yr real yld");
  const iDxy = idx("dollar index");
  const iDef = idx("deficit gdp flag");

  // Ensure required columns exist
  for (const col of METALS_REQUIRED_COLS) {
    if (header.indexOf(col) === -1) {
      throw new Error(`Metals CSV missing required column: "${col}"`);
    }
  }

  const upsertLatestMetalsSQL = `
    INSERT INTO metals_curve_latest (
      as_of_date, metal, tenor_months, price,
      real_10yr_yld, dollar_index, deficit_gdp_flag
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    ON CONFLICT (metal, tenor_months)
    DO UPDATE SET
      as_of_date       = EXCLUDED.as_of_date,
      price            = EXCLUDED.price,
      real_10yr_yld    = EXCLUDED.real_10yr_yld,
      dollar_index     = EXCLUDED.dollar_index,
      deficit_gdp_flag = EXCLUDED.deficit_gdp_flag,
      updated_at       = now();
  `;

  const insertHistoryMetalsSQL = `
    INSERT INTO metals_curve_history (
      as_of_date, metal, tenor_months, price,
      real_10yr_yld, dollar_index, deficit_gdp_flag
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7);
  `;

  let processed = 0;
  let skipped = 0;

  for (let i = 1; i < rows.length; i++) {
    const row = rows[i];
    if (!row || !row.length) {
      skipped++;
      continue;
    }

    const asOfRaw = row[iAsOf] ?? "";
    const metal = (row[iMetal] ?? "").toString().trim();
    const tenorMonths = parseInt(row[iTenor] ?? "", 10);
    const price = parseFloat((row[iPrice] ?? "").toString());

    const real10yr =
      iReal >= 0 && row[iReal] !== "" ? parseFloat(row[iReal]) : null;
    const dxy =
      iDxy >= 0 && row[iDxy] !== "" ? parseFloat(row[iDxy]) : null;
    const deficitFlag =
      iDef >= 0 && row[iDef] !== ""
        ? String(row[iDef]).trim().toLowerCase() === "true"
        : null;

    if (!metal || Number.isNaN(tenorMonths) || Number.isNaN(price)) {
      skipped++;
      continue;
    }

    const asOfDate = asOfRaw || null; // let Postgres cast

    await client.query(upsertLatestMetalsSQL, [
      asOfDate,
      metal,
      tenorMonths,
      price,
      real10yr,
      dxy,
      deficitFlag,
    ]);

    await client.query(insertHistoryMetalsSQL, [
      asOfDate,
      metal,
      tenorMonths,
      price,
      real10yr,
      dxy,
      deficitFlag,
    ]);

    processed++;
  }

  logger.log(
    `Metals load complete: processed=${processed}, skipped=${skipped}`
  );

  return { processed, skipped };
}

// ----- Main handler -----
export default async function handler(req, res) {
  const started = Date.now();
  const fromCron = req.query.tag || null;

  const client = new Client({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false },
  });

  try {
    await client.connect();

    const pricesResult = await loadPricesFromSheet(client);
    const metalsResult = await loadMetalsCurveFromSheet(client);

    const runtimeMs = Date.now() - started;

    return res.status(200).json({
      ok: true,
      message: "Prices and metals ingested",
      pricesResult,
      metalsResult,
      runtime_ms: runtimeMs,
      tag: fromCron || null,
    });
  } catch (err) {
    console.error("run.js error:", err);
    return res.status(500).json({ ok: false, error: String(err) });
  } finally {
    await client.end().catch(() => {});
  }
}
