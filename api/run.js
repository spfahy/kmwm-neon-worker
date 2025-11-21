// ----- Main handler -----
export default async function handler(req, res) {
  const started = Date.now();
  const fromCron = req.query.tag || null;

  const client = new Client({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false },
  });

  try {
    await client.connect();

    const pricesResult = await loadPricesFromSheet(client);
    const metalsResult = await loadMetalsCurveFromSheet(client);
    // Remove / comment out the live overlay until we have a proper API:
    // const liveMetalsResult = await loadLiveMetalsSpot(client);

    const runtimeMs = Date.now() - started;

    return res.status(200).json({
      ok: true,
      message: "Prices and metals ingested",
      pricesResult,
      metalsResult,
      // liveMetalsResult, // not returned for now
      runtime_ms: runtimeMs,
      tag: fromCron || null,
    });
  } catch (err) {
    console.error("run.js error:", err);
    return res.status(500).json({ ok: false, error: String(err) });
  } finally {
    await client.end().catch(() => {});
  }
}
