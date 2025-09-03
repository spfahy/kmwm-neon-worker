// api/load-positions.js
import { Client } from "pg";

export const config = { runtime: "nodejs" };

export default async function handler(req, res) {
  const client = new Client({ connectionString: process.env.DATABASE_URL });

  try {
    await client.connect();

    // ✅ Log exactly which role/DB/schema the loader is using
    const who = await client.query(
      "select current_user, current_database(), current_schema"
    );
    console.log("DB WhoAmI:", who.rows[0]);

    // (Optional) quick ping to prove we can query
    const ping = await client.query("select now()");
    console.log("DB Ping:", ping.rows[0]);

    // Temporary success response so we can re-run this endpoint freely
    res.status(200).json({ ok: true, whoami: who.rows[0] });
  } catch (err) {
    console.error("load-positions error:", err);
    res.status(500).json({ ok: false, error: String(err?.message || err) });
  } finally {
    try { await client.end(); } catch {}
  }
}
