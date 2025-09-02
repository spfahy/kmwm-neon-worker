// /api/dbinfo.js (Edge runtime)
export const config = { runtime: "edge" };

export default async function handler(req) {
  // use the web-standard client for Edge; if your project already set up a pooled fetch,
  // keep it consistent with your existing edge functions
  const { Pool } = await import("pg"); // Vercel supports dynamic import on Edge with pg >= 8.11
  const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    application_name: "KMWM-DBInfo",
  });

  try {
    const client = await pool.connect();
    try {
      const id  = await client.query(
        `select current_database() as db,
                current_user     as role,
                current_schema() as schema`
      );
      const cnt = await client.query(
        `select count(*)::int as rows from public.positions`
      );
      const body = JSON.stringify({ ok: true, ...id.rows[0], positions_rows: cnt.rows[0].rows });
      return new Response(body, { headers: { "content-type": "application/json" } });
    } finally {
      client.release();
    }
  } catch (e) {
    return new Response(JSON.stringify({ ok: false, error: String(e) }), {
      status: 500,
      headers: { "content-type": "application/json" },
    });
  }
}
