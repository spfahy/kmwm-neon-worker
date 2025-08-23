// api/run.js
// Required ENV:
//   DATABASE_URL     -> postgres://agent_writer:<pwd>@ep-...-pooler....neon.tech/neondb?sslmode=require
//   PRICE_CSV_URL    -> https://docs.google.com/.../export?format=csv&gid=...
// Optional ENV (defaults shown):
//   LOCAL_TZ         -> America/Chicago
//   LOCAL_HOUR       -> 9
//   LOCAL_MINUTE     -> 0

import pg from "pg";
import { parse } from "csv-parse/sync";
const { Client } = pg;

// ----- local time helpers (for logging + "run once at 09:00 local" on cron) -----
const TZ = process.env.LOCAL_TZ || "America/Chicago";
const LOCAL_HOUR = parseInt(process.env.LOCAL_HOUR || "9", 10);
const LOCAL_MINUTE = parseInt(process.env.LOCAL_MINUTE || "0", 10);

function localParts(d = new Date(), tz = TZ) {
  const fmt = new Intl.DateTimeFormat("en-US", {
    timeZone: tz,
    year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", second: "2-digit",
    hour12: false
  });
  const parts = Object.fromEntries(fmt.formatToParts(d).map(p => [p.type, p.value]));
  return {
    yyyy: parts.year, mm: parts.month, dd: parts.day,
    hh: Number(parts.hour), min: Number(parts.minute), ss: Number(parts.second),
    display: new Intl.DateTimeFormat("en-US", { timeZone: tz, dateStyle: "medium", timeStyle: "short" }).format(d)
  };
}

function localDateISO(d = new Date(), tz = TZ) {
  const p = localParts(d, tz);
  return `${p.yyyy}-${p.mm}-${p.dd}`;
}

// ----- csv helpers -----
function pick(row, ...names) {
  const wanted = names.map(n => n.toLowerCase().trim());
  const key = Object.keys(row).find(h => wanted.includes(h.toLowerCase().trim()));
  return key ? row[key] : null;
}

function toPct(v) {
  if (v == null) return null;
  const n = parseFloat(String(v).replace(/[% ,]/g, ""));
  if (Number.isNaN(n)) return null;
  return n > 1 ? n / 100 : n; // 8.2 -> 0.082, 0.082 stays 0.082
}

export default async function handler(req, res) {
  try {
    // 0) Validate env
    const csvUrl = process.env.PRICE_CSV_URL;
    const dbUrl  = process.env.DATABASE_URL;
    if (!dbUrl)  return res.status(500).json({ ok: false, error: "DATABASE_URL is not set" });
    if (!csvUrl) return res.status(500).json({ ok: false, error: "PRICE_CSV_URL is not set" });

    const now = new Date();
    const lp = localParts(now);

    // 1) If invoked by Vercel cron, only do the job at 09:00 local (manual calls always run)
    const isCron = req.headers["x-vercel-cron"] === "1" || req.headers["x-vercel-cron"] === "true";
    if (isCron && !(lp.hh === LOCAL_HOUR && lp.min === LOCAL_MINUTE)) {
      return res.status(200).json({
        ok: true,
        skipped: true,
        reason: `outside ${String(LOCAL_HOUR).padStart(2,"0")}:${String(LOCAL_MINUTE).padStart(2,"0")} local window`,
        local_time: lp.display,
        tz: TZ
      });
    }

    // 2) Fetch CSV
    const resp = await fetch(csvUrl);
    if (!resp.ok) {
      const body = await resp.text().catch(() => "");
      return res.status(500).json({ ok: false, error: `CSV fetch failed: ${resp.status} ${resp.statusText}`, detail: body.slice(0, 300) });
    }
    const csvText = await resp.text();

    // 3) Parse CSV (expects a header row)
    let rowsRaw;
    try {
      rowsRaw = parse(csvText, { columns: true, skip_empty_lines: true, trim: true });
    } catch (e) {
      return res.status(500).json({ ok: false, error: `CSV parse failed: ${String(e)}` });
    }

    // 4) Normalize rows to our schema
    const asof = localDateISO(now, TZ); // YYYY-MM-DD in local tz
    const records = rowsRaw.map(r => ({
      symbol: (pick(r, "Symbol") || "").toString().trim(),
      d_yld:  toPct(pick(r, "D Yld", "Dividend Yield")),
      r1y:    toPct(pick(r, "1 Yr", "1 Year Total Returns (Daily)", "1 Year Price Returns (Daily)")),
      r90d:   toPct(pick(r, "90 Days", "3 Month Price Returns (Daily)", "3 Month Total Returns (Daily)")),
      r30d:   toPct(pick(r, "30 Days", "1 Month Price Returns (Daily)", "1 Month Total Returns (Daily)")),
      sector: (pick(r, "Sector") || null)?.toString().trim() || null
    })).filter(r => r.symbol);

    // 5) Upsert into Postgres
    const client = new Client({ connectionString: dbUrl, ssl: { rejectUnauthorized: false } });
    let inserted = 0, updated = 0;

    try {
      await client.connect();

      // Optional: small sanity query (logs DB host without secrets)
      try {
        const host = new URL(dbUrl).host;
        console.log("DB host:", host, "Rows to upsert:", records.length, "Asof:", asof);
      } catch {}

      // Use a single transaction for consistency
      await client.query("BEGIN");

      // Assumes a table `prices` with primary key (symbol, asof_date)
      // CREATE TABLE if not already present (will no-op if it exists and you have privileges)
      // If agent_writer lacks CREATE privileges, this will be ignored by catch and we proceed.
      try {
        await client.query(`
          CREATE TABLE IF NOT EXISTS prices (
            symbol     text NOT NULL,
            asof_date  date NOT NULL,
            d_yld      numeric,
            r1y        numeric,
            r90d       numeric,
            r30d       numeric,
            sector     text,
            PRIMARY KEY (symbol, asof_date)
          );
        `);
      } catch (e) {
        console.warn("CREATE TABLE skipped/failed (likely insufficient privileges):", String(e));
      }

      const text = `
        INSERT INTO prices (symbol, asof_date, d_yld, r1y, r90d, r30d, sector)
        VALUES ($1,$2,$3,$4,$5,$6,$7)
        ON CONFLICT (symbol, asof_date)
        DO UPDATE SET
          d_yld = EXCLUDED.d_yld,
          r1y   = EXCLUDED.r1y,
          r90d  = EXCLUDED.r90d,
          r30d  = EXCLUDED.r30d,
          sector= COALESCE(EXCLUDED.sector, prices.sector)
        RETURNING (xmax = 0) AS inserted;
      `;

      for (const r of records) {
        const { rows } = await client.query(text, [r.symbol, asof, r.d_yld, r.r1y, r.r90d, r.r30d, r.sector]);
        if (rows[0]?.inserted) inserted++; else updated++;
      }

      await client.query("COMMIT");
    } catch (e) {
      try { await client.query("ROLLBACK"); } catch {}
      return res.status(500).json({ ok: false, error: String(e) });
    } finally {
      try { await client.end(); } catch {}
    }

    // 6) Done
    return res.status(200).json({
      ok: true,
      inserted,
      updated,
      total: inserted + updated,
      generated_at_local: lp.display,
      generated_at_utc: now.toISOString(),
      tz: TZ
    });

  } catch (err) {
    console.error("Unhandled error in /api/run:", err);
    return res.status(500).json({ ok: false, error: String(err) });
  }
}
