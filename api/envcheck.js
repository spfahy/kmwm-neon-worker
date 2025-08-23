// api/envcheck.js
export default async function handler(req, res) {
  const v = process.env.DATABASE_URL || "";
  let host = "invalid";
  try { host = new URL(v).host; } catch {}

  return res.status(200).json({
    ok: true,
    len: v.length,
    startsWith: v.slice(0, 10), // should be "postgres://"
    host,                       // should be your ep-....neon.tech host
    hasQuotes: /^['"].*['"]$/.test(v.trim()),
    hasPsqlPrefix: v.trim().startsWith("psql ")
  });
}
