import { Client } from "pg";

export default async function handler(req, res) {
  const url = process.env.DATABASE_URL;
  if (!url) {
    return res.status(500).json({ ok: false, error: "DATABASE_URL is missing" });
  }

  const host = (() => { try { return new URL(url).host } catch { return "invalid" } })();

  const client = new Client({
    connectionString: url,
    ssl: { rejectUnauthorized: false }
  });

  try {
    await client.connect();
    const { rows } = await client.query("SELECT now() AS ts");
    return res.status(200).json({
      ok: true,
      message: "Connected to Neon",
      ts: rows[0].ts,
      db_host: host
    });
  } catch (err) {
    return res.status(500).json({ ok: false, error: String(err), db_host: host });
  } finally {
    try { await client.end(); } catch {}
  }
}
