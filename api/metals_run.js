// api/metals_run.js

// Use ES module import so it works in "type": "module" / ESM environments
import pg from "pg";

const { Pool } = pg;

// --------------------------------------------------
// Database pool
// --------------------------------------------------
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

// --------------------------------------------------
// Helpers
// --------------------------------------------------

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

// Log status in metals_ingest_log
async function logIngest(client, runDate, source, status, reason, rowCount) {
  try {
    await client.query(
      `
      INSERT INTO metals_ingest_log
        (run_timestamp, run_date, trigger_source, status, error_reason, row_count)
      VALUES (NOW(), $1, $2, $3, $4, $5)
      `,
      [runDate, source, status, reason, rowCount]
    );
  } catch (err) {
    console.error("Failed to log metals ingest:", err);
  }
}

// Parse a single CSV line, handling quotes and commas in quotes
function parseCsvLine(line) {
  const out = [];
  let cur = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];

    if (ch === '"') {
      if (inQuotes && i + 1 < line.length && line[i + 1] === '"') {
        cur += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (ch === "," && !inQuotes) {
      out.push(cur);
      cur = "";
    } else {
      cur += ch;
    }
  }
  out.push(cur);

  return out;
}

// Trim and strip wrapping quotes
function cleanField(value) {
  // Turn a numeric-looking string into a number, stripping commas
function toNumber(value) {
  if (value == null) return null;
  const cleaned = String(value).replace(/,/g, "").trim();
  const n = Number(cleaned);
  return Number.isFinite(n) ? n : null;
}

  if (value == null) return "";
  let v = String(value).trim();
  if (v.startsWith('"') && v.endsWith('"')) {
    v = v.slice(1, -1);
  }
  return v.trim();
}

// --------------------------------------------------
// Parse Metals CSV from the Google Sheet
// --------------------------------------------------
function parseMetalsCsv(text) {
  const lines = text.trim().split(/\r?\n/);
  if (lines.length < 2) return [];

  // Header
  const headerRaw = parseCsvLine(lines[0]);
  const header = headerRaw.map(cleanField);

  const idx = {
    as_of_date: header.indexOf("As Of Date"),
    metal: header.indexOf("Metal"),
    tenor_months: header.indexOf("Tenor Months"),
    price: header.indexOf("Price"),
    real_10yr_yld: header.indexOf("10 Yr Real Yld"),
    dollar_index: header.indexOf("Dollar Index"),
    deficit_gdp_flag: header.indexOf("Deficit GDP Flag"),
  };

  // All required columns must exist
  if (
    idx.as_of_date < 0 ||
    idx.metal < 0 ||
    idx.tenor_months < 0 ||
    idx.price < 0 ||
    idx.real_10yr_yld < 0 ||
    idx.dollar_index < 0 ||
    idx.deficit_gdp_flag < 0
  ) {
    return [];
  }

  const rows = [];

  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;

    const colsRaw = parseCsvLine(line);
    const cols = colsRaw.map(cleanField);

    const asOf = cols[idx.as_of_date];
    const metal = cols[idx.metal]?.toLowerCase();
    const tenorStr = cols[idx.tenor_months];
    const priceStr = cols[idx.price];
    const realStr = cols[idx.real_10yr_yld];
    const dxStr = cols[idx.dollar_index];
    const deficitStr = cols[idx.deficit_gdp_flag];

    if (!asOf || !metal) continue;

    const tenor = Number(tenorStr);
    const price = Number(priceStr);
    const real = Number(realStr);
    const dx = Number(dxStr);
    const deficit = Number(deficitStr);

    if (!Number.isFinite(tenor)) continue;

    const safePrice = Number.isFinite(price) ? price : 0.0;
    const safeReal = Number.isFinite(real) ? real : null;
    const safeDx = Number.isFinite(dx) ? dx : null;
    const safeDeficit = Number.isFinite(deficit) ? deficit : 0;

    rows.push({
      as_of_date: asOf,
      metal,
      tenor_months: tenor,
      price: safePrice,
      real_10yr_yld: safeReal,
      dollar_index: safeDx,
      deficit_gdp_flag: safeDeficit,
    });
  }

  return rows;
}

// --------------------------------------------------
// Main handler – ES module default export
// --------------------------------------------------
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

  const source = String(req.query.source || "unknown"); // 'manual' or 'cron_1600'
  const force = String(req.query.force || "0") === "1";

  const todayStr = getTodayCT();
  const client = await pool.connect();

  try {
    // 1) Check if a successful ingest already happened today
    const existing = await client.query(
      `
      SELECT id
      FROM metals_ingest_log
      WHERE run_date = $1
        AND status = 'success'
      LIMIT 1
      `,
      [todayStr]
    );

    const hasSuccessToday = existing.rows.length > 0;

    if (source.startsWith("cron") && hasSuccessToday && !force) {
      await logIngest(
        client,
        todayStr,
        source,
        "skipped",
        "already_ingested_today",
        0
      );
      return res.json({
        ok: true,
        skipped: true,
        reason: "already_ingested_today",
      });
    }

    // 2) Fetch CSV
    const csvUrl = process.env.METALS_CSV_URL;
    if (!csvUrl) {
      await logIngest(
        client,
        todayStr,
        source,
        "error",
        "METALS_CSV_URL_not_configured",
        0
      );
      return res
        .status(500)
        .json({ error: "METALS_CSV_URL not configured in environment" });
    }

    const resp = await fetch(csvUrl);
    if (!resp.ok) {
      await logIngest(client, todayStr, source, "error", "fetch_failed", 0);
      return res.status(500).json({ error: "fetch_failed" });
    }

    const csvText = await resp.text();
    const rows = parseMetalsCsv(csvText);

    if (!rows.length) {
      await logIngest(
        client,
        todayStr,
        source,
        "error",
        "no_rows_in_sheet_or_bad_headers",
        0
      );
      return res
        .status(400)
        .json({ error: "no_rows_in_sheet_or_bad_headers" });
    }

    // 3) Date validation
    const uniqueDates = [...new Set(rows.map((r) => r.as_of_date))];
    let sheetDate = uniqueDates.length === 1 ? uniqueDates[0] : null;

    if (!force) {
      if (!sheetDate) {
        await logIngest(
          client,
          todayStr,
          source,
          "error",
          "multiple_as_of_dates_in_sheet",
          rows.length
        );
        return res.status(400).json({
          error: "multiple_as_of_dates_in_sheet",
          uniqueDates,
        });
      }

      if (sheetDate !== todayStr) {
        const reason = `sheet_date_mismatch: sheet=${sheetDate}, expected=${todayStr}`;
        await logIngest(
          client,
          todayStr,
          source,
          "error",
          reason,
          rows.length
        );

        return res.status(400).json({
          error: "sheet_date_mismatch",
          sheetDate,
          expectedDate: todayStr,
        });
      }
    } else {
      // Force mode: override all dates
      sheetDate = todayStr;
      for (const r of rows) {
        r.as_of_date = todayStr;
      }
    }

    // 4) Write to Neon
    await client.query("BEGIN");

    for (const r of rows) {
      await client.query(
        `
        INSERT INTO metals_curve_latest
          (as_of_date, metal, tenor_months, price,
           real_10yr_yld, dollar_index, deficit_gdp_flag, updated_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7, NOW())
        ON CONFLICT (metal, tenor_months)
        DO UPDATE SET
          as_of_date = EXCLUDED.as_of_date,
          price = EXCLUDED.price,
          real_10yr_yld = EXCLUDED.real_10yr_yld,
          dollar_index = EXCLUDED.dollar_index,
          deficit_gdp_flag = EXCLUDED.deficit_gdp_flag,
          updated_at = NOW()
        `,
        [
          r.as_of_date,
          r.metal,
          r.tenor_months,
          r.price,
          r.real_10yr_yld,
          r.dollar_index,
          r.deficit_gdp_flag,
        ]
      );
    }

    for (const r of rows) {
      await client.query(
        `
        INSERT INTO metals_curve_history
          (as_of_date, metal, tenor_months, price,
           real_10yr_yld, dollar_index, deficit_gdp_flag, inserted_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7, NOW())
        `,
        [
          r.as_of_date,
          r.metal,
          r.tenor_months,
          r.price,
          r.real_10yr_yld,
          r.dollar_index,
          r.deficit_gdp_flag,
        ]
      );
    }

    await client.query("COMMIT");
    await logIngest(client, todayStr, source, "success", null, rows.length);

    return res.json({
      ok: true,
      sheetDate,
      rowCount: rows.length,
      trigger_source: source,
    });
  } catch (err) {
    console.error("metals_run error:", err);
    try {
      await client.query("ROLLBACK");
    } catch {
      // ignore
    }
    await logIngest(
      client,
      todayStr,
      source,
      "error",
      "unhandled_exception",
      0
    );
    return res.status(500).json({ error: "unhandled_exception" });
  } finally {
    client.release();
  }
}
