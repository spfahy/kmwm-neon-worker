// api/envcheck.js — shows DB + PRICE_CSV_URL and tests the CSV link
export default async function handler(req, res) {
  const db = process.env.DATABASE_URL || "";
  const priceUrl = process.env.PRICE_CSV_URL || "";

  // Test the CSV link from the server (200 = good; 404/403 = bad link or sharing)
  let priceStatus = null, priceErr = null;
  if (priceUrl) {
    try {
      const r = await fetch(priceUrl, { cache: "no-store" });
      priceStatus = r.status;
    } catch (e) {
      priceErr = String(e);
    }
  }

  // Show masked DB host only (no credentials)
  const host = db.includes("@") ? db.split("@")[1]?.split("/")[0] : null;

  return res.status(200).json({
    ok: true,
    database_url: {
      len: db.length,
      startsWith: db.slice(0, 9),   // "postgres:"
      host
    },
    price_csv_url: {
      present: !!priceUrl,
      len: priceUrl.length,
      beginsWith: priceUrl.slice(0, 60),
      status: priceStatus,          // expect 200
      error: priceErr
    }
  });
}
