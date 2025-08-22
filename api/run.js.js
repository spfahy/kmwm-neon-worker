import { Client } from "pg";

export default async function handler(req, res) {
  const client = new Client({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
  });

  try {
    await client.connect();
    console.log("Connected to Neon!");

    // Example insert
    await client.query(
      "INSERT INTO funding_flows (flow_type, amount) VALUES ($1, $2)",
      ["test", 123]
    );

    res.status(200).json({ message: "Worker ran successfully!" });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  } finally {
    await client.end();
  }
}
