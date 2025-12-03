function parseMetalsCsv(text) {
  const lines = text.trim().split(/\r?\n/);
  if (lines.length < 2) return [];

  // --- Parse CSV line safely (handles quotes, commas inside quotes, etc.)
  function splitCsvLine(line) {
    const result = [];
    let current = "";
    let insideQuotes = false;

    for (let i = 0; i < line.length; i++) {
      const c = line[i];

      if (c === '"') {
        insideQuotes = !insideQuotes;
      } else if (c === "," && !insideQuotes) {
        result.push(current.trim());
        current = "";
      } else {
        current += c;
      }
    }
    result.push(current.trim());
  
    return result;
  }

  // --- Clean field (remove wrapping quotes)
  function cleanField(v) {
    if (!v) return "";
    return v.replace(/^"(.*)"$/, "$1").trim();
  }

  // --- Header
  const header = splitCsvLine(lines[0]).map(cleanField);

  const idx = {
    as_of_date: header.indexOf("As Of Date"),
    metal: header.indexOf("Metal"),
    tenor_months: header.indexOf("Tenor Months"),
    price: header.indexOf("Price"),
    real_10yr_yld: header.indexOf("10 Yr Real Yld"),
    dollar_index: header.indexOf("Dollar Index"),
    deficit_gdp_flag: header.indexOf("Deficit GDP Flag"),
  };

  // If any column is missing — fail early
  for (const key of Object.keys(idx)) {
    if (idx[key] < 0) return [];
  }

  const rows = [];

  // --- Parse all data rows
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;

    const cols = splitCsvLine(line).map(cleanField);

    const tenorStr = cols[idx.tenor_months];
    const priceStr = cols[idx.price];
    const realStr = cols[idx.real_10yr_yld];
    const dxStr = cols[idx.dollar_index];
    const deficitStr = cols[idx.deficit_gdp_flag];

    const tenor = Number(tenorStr);
    const price = Number(priceStr);
    const real = Number(realStr);
    const dx = Number(dxStr);
    const deficit = Number(deficitStr);

    if (!Number.isFinite(tenor)) continue;
    if (!Number.isFinite(price)) continue;
    if (!Number.isFinite(real)) continue;
    if (!Number.isFinite(dx)) continue;
    if (!Number.isFinite(deficit)) continue;

    rows.push({
      as_of_date: cols[idx.as_of_date],
      metal: cols[idx.metal]?.toLowerCase(),
      tenor_months: tenor,
      price,
      real_10yr_yld: real,
      dollar_index: dx,
      deficit_gdp_flag: deficit,
    });
  }

  return rows;
}
