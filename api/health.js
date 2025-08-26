import pg from "pg";
const { Client } = pg;

export default async function handler(req, res) {
  const client = new Client({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
  });

  try {
    await client.connect();
    const ping = await client.query("SELECT now() as ts");
    const latest = await client.query("SELECT count(*)::int AS n FROM prices_latest");
    const history = await client.query("SELECT count(*)::int AS n FROM prices_history");
    return res.status(200).json({
      ok: true,
      ts: ping.rows[0].ts,
      rows_latest: latest.rows[0].n,
      rows_history: history.rows[0].n
    });
  } catch (err) {
    return res.status(500).json({ ok: false, error: String(err) });
  } finally {
    await client.end().catch(() => {});
  }
}
