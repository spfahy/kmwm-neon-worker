const { Pool } = require("pg");

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

// Log status in metals_ingest_log
async function logIngest(client, runDate, source, status, reason, rowCount) {
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

  const header = lines[0].split(",");
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
    return [];
  }

  const rows = [];

  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;

    const cols = line.split(",");
    const asOf = cols[idx.as_of_date]?.trim();
    if (!asOf) continue;

    rows.push({
      as_of_date: asOf, // must be YYYY-MM-DD in the sheet
      metal: cols[idx.metal]?.trim().toLowerCase(),
      tenor_months: parseInt(cols[idx.tenor_months], 10),
      price: parseFloat(cols[idx.price]),
      real_10yr_yld: parseFloat(cols[idx.real_10yr_yld]),
      dollar_index: parseFloat(cols[idx.dollar_index]),
      deficit_gdp_flag: parseInt(cols[idx.deficit_gdp_flag], 10),
    });
  }

  return rows;
}

module.exports = async (req, res) => {
  const auth = req.headers.authorization || "";
  const token = auth.replace("Bearer ", "");
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
      await logIngest(client, todayStr, source, "skipped", "already_ingested_today", 0);
      return res.json({ ok: true, skipped: true, reason: "already_ingested_today" });
    }

    // 2) Fetch CSV from Metals Sheet
    const csvUrl = process.env.METALS_CSV_URL;
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
    await client.query("ROLLBACK");
    console.error(e);
    await logIngest(client, todayStr, source, "error", "unhandled_exception", 0);
    return res.status(500).json({ error: "unhandled_exception" });
  } finally {
    client.release();
  }
};
