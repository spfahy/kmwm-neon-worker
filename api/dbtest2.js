export default async function handler(req, res) {
  try {
    // dynamic import so any import error is caught and returned
    const pg = await import("pg");
    const { Client } = pg.default || pg;

    const client = new Client({
      connectionString: process.env.DATABASE_URL,
      ssl: { rejectUnauthorized: false }
    });

    await client.connect();
    const { rows } = await client.query("SELECT now() AS ts");
    await client.end();

    res.status(200).json({ ok: true, message: "Connected to Neon", ts: rows[0].ts });
  } catch (e) {
    res.status(200).json({ ok: false, error: String(e), hint: "dbtest2 caught the error" });
  }
}
