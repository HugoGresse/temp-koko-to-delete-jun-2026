const fs = require("fs");
const Database = require("better-sqlite3");
const { createReadStream } = require("fs");
const { parse } = require("csv-parse");

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
  // Try to delete the DB file if it exists
  try {
    fs.unlinkSync(dbFilePath);
  } catch (err) {
    // File might not exist, that's fine
  }

  const db = new Database(dbFilePath);
  // PRAGMA for performance
  db.pragma("journal_mode = OFF");
  db.pragma("synchronous = OFF");
  db.pragma("temp_store = MEMORY");
  db.pragma("page_size = 4096");
  db.pragma("cache_size = 10000");
  db.pragma("auto_vacuum = FULL");

  db.exec("BEGIN TRANSACTION");

  // Variables to track progress
  let headers = null;
  let insert = null;
  let rowCount = 0;
  let batchSize = 1000;
  let batch = [];
  let firstRowProcessed = false;

  // Create a read stream for the CSV file
  const parser = parse({
    columns: true,
    skip_empty_lines: true,
    delimiter: ",",
    quote: '"',
  });

  // Process data in chunks
  parser.on("data", function (record) {
    // Skip if this is the first row (already processed in readable)
    if (!firstRowProcessed) {
      headers = Object.keys(record);
      const columnDefs = inferColumnTypes(headers, record).join(", ");
      db.exec(`CREATE TABLE IF NOT EXISTS bird_detections (${columnDefs})`);

      // Prepare the insert statement
      const placeholders = headers.map(() => "?").join(", ");
      insert = db.prepare(
        `INSERT INTO bird_detections VALUES (${placeholders})`
      );

      // Process the first row
      batch.push(record);
      rowCount++;
      firstRowProcessed = true;
      return;
    }

    batch.push(record);
    rowCount++;

    // When batch is full, insert and clear
    if (batch.length >= batchSize) {
      insertBatch();
      batch = [];
    }

    // Log progress every 10,000 rows
    if (rowCount % 10000 === 0) {
      console.log(`Processed ${rowCount} rows...`);
    }
  });

  // Finalize when done
  parser.on("end", function () {
    // Insert any remaining records
    console.log(`Inserting ${batch.length} records...`);
    if (batch.length > 0) {
      insertBatch();
    }

    // Commit the transaction and optimize the database
    db.exec("COMMIT");
    db.exec("VACUUM");
    db.close();
    console.log(`Imported ${rowCount} records to ${dbFilePath}`);
  });

  // Handle errors
  parser.on("error", function (err) {
    console.error("Error parsing CSV:", err.message);
    db.exec("ROLLBACK");
    db.close();
  });

  // Function to insert a batch of records
  function insertBatch() {
    db.transaction((rows) => {
      for (const row of rows) {
        insert.run(headers.map((h) => (row[h] === "NA" ? null : row[h])));
      }
    })(batch);
  }

  // Start the stream pipeline
  createReadStream(csvFilePath).pipe(parser);
}

const csvFilePath = process.argv[2] || "data.csv";
const dbFilePath = process.argv[3] || "bird_detections.db";

fs.unlinkSync(dbFilePath);
importCsvToSqlite(csvFilePath, dbFilePath);
