const fs = require("fs");
const Database = require("better-sqlite3");
const { parse } = require("csv-parse/sync");

function inferColumnTypes(headers, sampleRow) {
  return headers.map((header, i) => {
    const value = sampleRow[header];
    if (value === "NA" || value === "" || value === null)
      return `"${header}" TEXT`;
    if (!isNaN(Number(value)) && value !== "") return `"${header}" REAL`;
    if (String(value).match(/^\d{4}-\d{2}-\d{2}/)) return `"${header}" TEXT`;
    return `"${header}" TEXT`;
  });
}

function importCsvToSqlite(csvFilePath, dbFilePath) {
  const csvData = fs.readFileSync(csvFilePath, "utf8");
  const records = parse(csvData, {
    columns: true,
    skip_empty_lines: true,
    delimiter: ",",
    quote: '"',
  });
  if (records.length === 0) {
    console.log("No records found in CSV.");
    return;
  }
  const headers = Object.keys(records[0]);
  const columnDefs = inferColumnTypes(headers, records[0]).join(", ");
  const db = new Database(dbFilePath);
  // PRAGMA for size reduction
  db.pragma("journal_mode = OFF");
  db.pragma("synchronous = OFF");
  db.pragma("temp_store = MEMORY");
  db.pragma("page_size = 4096");
  db.pragma("cache_size = 10000");
  db.pragma("auto_vacuum = FULL");
  db.exec("BEGIN TRANSACTION");
  db.exec(`CREATE TABLE IF NOT EXISTS bird_detections (${columnDefs})`);
  const placeholders = headers.map(() => "?").join(", ");
  const insert = db.prepare(
    `INSERT INTO bird_detections VALUES (${placeholders})`
  );
  const insertMany = db.transaction((rows) => {
    for (const row of rows) {
      insert.run(headers.map((h) => (row[h] === "NA" ? null : row[h])));
    }
  });
  insertMany(records);
  db.exec("COMMIT");
  db.close();
  console.log(`Imported ${records.length} records to ${dbFilePath}`);
}

const csvFilePath = process.argv[2] || "input.csv";
const dbFilePath = process.argv[3] || "bird_detections.db";

importCsvToSqlite(csvFilePath, dbFilePath);
