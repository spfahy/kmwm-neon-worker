import pg from "pg";
const { Client } = pg;

export default async function handler(req, res) {
  const client = new Client({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
  });

  try {
    await client.connect();
    const { rows } = await client.query("SELECT now() AS ts");
    return res.status(200).json({ ok: true, message: "Connected to Neon", ts: rows[0].ts });
  } catch (err) {
    return res.status(500).json({ ok: false, error: String(err) });
  } finally {
    await client.end();
  }
}

