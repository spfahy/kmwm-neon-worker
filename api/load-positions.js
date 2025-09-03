console.log("DB WhoAmI:", whoami.rows[0]);
// api/load-positions.js
import { Client } from "pg";
import Papa from "papaparse";

export const config = { runtime: "nodejs" };

function parseCsvStrict(csvText) {
  const { data, errors, meta } = Papa.parse(csvText, {
    header: true,
    dynamicTyping: true,
    skipEmptyLines: true
  });
  if (errors?.length) throw new Error("CSV parse errors: " + errors.map(e => e.message).join("; "));
  const need = ["Symbol","Ave Share Price","Jan 1 Price","Pur Dt","Shares"];
  const got = meta.fields || [];
  for (const r of need) if (!got.includes(r)) throw new Error(`Missing required column: ${r}. Found: [${got.join(", ")}]`);
  return data;
}

export default async function handler(req, res) {
  const url = process.env.POSITIONS_CSV_URL;
  const db  = process.env.DATABASE_URL;
  if (!url || !db) return res.status(500).json({ ok:false, error:"Missing POSITIONS_CSV_URL or DATABASE_URL" });

  let csvText;
  try {
    const r = await fetch(url, { cache: "no-store" });
    if (!r.ok) throw new Error(`Fetch failed: ${r.status} ${r.statusText}`);
    csvText = await r.text();
  } catch (e) { return res.status(500).json({ ok:false, step:"fetch", error:String(e) }); }

  let rows;
  try { rows = parseCsvStrict(csvText); }
  catch (e) { return res.status(500).json({ ok:false, step:"parse", error:String(e) }); }

  const client = new Client({ connectionString: db, ssl: { rejectUnauthorized: false } });
  await client.connect();

  // Diagnostic query
  const whoami = await client.query(
    "select current_user, current_database(), current_schema"
  );
  console.log("DB WhoAmI:", whoami.rows[0]);

  const upsert = `
    insert into positions_raw(symbol, name, "Ave Share Price", "Jan 1 Price", "Pur Dt", shares)
    values ($1, $2, $3::numeric, $4::numeric, $5::date, $6::numeric)
    on conflict (symbol) do update
      set name = excluded.name,
          "Ave Share Price" = excluded."Ave Share Price",
          "Jan 1 Price"    = excluded."Jan 1 Price",
          "Pur Dt"         = excluded."Pur Dt",
          shares           = excluded.shares
  `;

  let processed = 0, skipped = 0;
  try {
    for (const r of rows) {
      const sym = String(r.Symbol ?? r.symbol ?? "").trim().toUpperCase();
      if (!sym) { skipped++; continue; }
      const name   = (r.Name ?? "").toString().trim();
      const ave    = r["Ave Share Price"] ?? null;
      const jan1   = r["Jan 1 Price"] ?? null;
      const purdt  = r["Pur Dt"] ? new Date(r["Pur Dt"]) : null;
      const shares = r.Shares ?? null;

      await client.query(upsert, [
        sym, name, ave, jan1,
        purdt instanceof Date && !isNaN(purdt) ? purdt.toISOString().slice(0,10) : null,
        shares
      ]);
      processed++;
    }
  } catch (e) {
    await client.end().catch(()=>{});
    return res.status(500).json({ ok:false, step:"upsert", processed, skipped, error:String(e) });
  }

  await client.end();
  return res.status(200).json({ ok:true, message:"Positions loaded", processed, skipped });
}
