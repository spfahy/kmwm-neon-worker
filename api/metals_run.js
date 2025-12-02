import { Pool } from "pg";

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

// Strip surrounding quotes and whitespace
function cleanField(value) {
  if (value == null) return "";
  return value.replace(/^"|"$/g, "").trim();
}

// Log status in metals_ingest_log
async function logIngest(client, runDate, source, status, reason, rowCount) {
  if (!client) {
    // Best-effort logging if we don't have a client from a transaction
    try {
      await pool.query(
        `
        INSERT INTO metals_ingest_log
          (run_timestamp, run_date, trigger_source, status, error_reason, row_count)
        VALUES (NOW(), $1, $2, $3, $4, $5)
        `,
        [runDate, source, status, reason, rowCount]
      );
    } catch (e) {
      console.error("Failed to log ingest without client:", e);
    }
    return;
  }

  await client.query(
    `
    INSERT INTO metals_ingest_log
      (run_timestamp, run_date, trigger_source, status, error_reason, row_count)
    VALUES (NOW(), $1, $2, $3, $4, $5)
    `,
    [runDate, source, status, reason, rowCount]
  );
}

// Parse Metals CSV from the Google Sheet
function parseMetalsCsv(text) {
  const lines = text.trim().split(/\r?\n/);
  if (lines.length < 2) return [];

  // Clean the header fields (strip quotes + spaces)
  const headerRaw = lines[0].split(",");
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

  // If any required column is missing, bail
  if (
    idx.as_of_date < 0 ||
    idx.metal < 0 ||
    idx.tenor_months < 0 ||
    idx.price < 0 ||
    idx.real_10yr_yld < 0 ||
    idx.dollar_index < 0 ||
    idx.deficit_gdp_flag < 0
  ) {
    console.error("Header mismatch in Metals CSV:", header);
    return [];
  }

  const rows = [];

  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;

    const rawCols = line.split(",");
    const cols = rawCols.map(cleanField);

    const asOf = cols[idx.as_of_date];
    if (!asOf) continue;

    const tenorStr = cols[idx.tenor_months];
    const priceStr = cols[idx.price];
    const realStr = cols[idx.real_10yr_yld];
    const dxStr = cols[idx.dollar_index];
    const deficitStr = cols[idx.deficit_gdp_flag];

    const tenor = parseInt(tenorStr, 10);
    const price = parseFloat(priceStr);
    const real = parseFloat(realStr);
    const dx = parseFloat(dxStr);
    const deficit = parseInt(deficitStr, 10);

    if (!Number.isFinite(tenor) || !Number.isFinite(price)) {
      // Skip clearly bad rows
      continue;
    }

    rows.push({
      as_of_date: asOf, // must be YYYY-MM-DD in the sheet
      metal: cols[idx.metal]?.toLowerCase(),
      tenor_months: tenor,
      price,
      real_10yr_yld: real,
      dollar_index: dx,
      deficit_gdp_flag: Number.isFinite(deficit) ? deficit : 0,
    });
  }

  return rows;
}

export default async function metalsRun(req, res) {
  const auth = req.headers.authorization || "";
  const token = auth.replace("Bearer ", "");
  if (token !== process.env.METALS_INGEST_TOKEN) {
    return res.status(401).json({ error: "unauthorized" });
  }

  const source = String(req.query.source || "unknown"); // 'manual' or 'cron_1600'
  const force = String(req.query.force || "0") === "1";

  const todayStr = getTodayCT();
  let client = null;

  try {
    // Connect to the database inside try so any connection error is caught
    client = await pool.connect();

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
      await logIngest(client, todayStr, source, "skipped", "already_ingested_today", 0);
      return res.json({ ok: true, skipped: true, reason: "already_ingested_today" });
    }

    // 2) Fetch CSV from Metals Sheet
    const csvUrl = process.env.METALS_CSV_URL;
    if (!csvUrl) {
      await logIngest(client, todayStr, source, "error", "missing_METALS_CSV_URL", 0);
      return res.status(500).json({ error: "missing_METALS_CSV_URL" });
    }

    const resp = await fetch(csvUrl);
    if (!resp.ok) {
      await logIngest(client, todayStr, source, "error", "fetch_failed", 0);
      return res.status(500).json({ error: "fetch_failed" });
    }

    const csvText = await resp.text();
    const rows = parseMetalsCsv(csvText);

    if (!rows.length) {
      await logIngest(client, todayStr, source, "error", "no_rows_in_sheet_or_bad_headers", 0);
      return res
        .status(400)
        .json({ error: "no_rows_in_sheet_or_bad_headers" });
    }

    // 3) Date validation
    const uniqueDates = [...new Set(rows.map((r) => r.as_of_date))];
    if (uniqueDates.length !== 1) {
      await logIngest(
        client,
        todayStr,
        source,
        "error",
        "multiple_as_of_dates_in_sheet",
        rows.length
      );
      return res
        .status(400)
        .json({ error: "multiple_as_of_dates_in_sheet", uniqueDates });
    }

    const sheetDate = uniqueDates[0];

    if (sheetDate !== todayStr && !force) {
      const reason = `sheet_date_mismatch: sheet=${sheetDate}, expected=${todayStr}`;
      await logIngest(client, todayStr, source, "error", reason, rows.length);

      return res.status(400).json({
        error: "sheet_date_mismatch",
        sheetDate,
        expectedDate: todayStr,
      });
    }

    // 4) Ingest into Neon
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

    await logIngest(client, todayStr, source, "success", null, rows.length);
    await client.query("COMMIT");

    return res.json({
      ok: true,
      sheetDate,
      rowCount: rows.length,
      trigger_source: source,
    });
  } catch (e) {
    console.error("metals_run error:", e);

    if (client) {
      try {
        await client.query("ROLLBACK");
      } catch (rollbackErr) {
        console.error("Rollback failed:", rollbackErr);
      }
      try {
        await logIngest(client, todayStr, source, "error", "unhandled_exception", 0);
      } catch (logErr) {
        console.error("Failed to log error:", logErr);
      }
    } else {
      await logIngest(null, todayStr, source, "error", "unhandled_exception_no_client", 0);
    }

    return res.status(500).json({ error: "unhandled_exception" });
  } finally {
    if (client) {
      client.release();
    }
  }
}
