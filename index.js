const fs = require('fs');
const sqlite3 = require('sqlite3').verbose();
const { parse } = require('csv-parse/sync');

// Function to import CSV to SQLite
async function importCsvToSqlite(csvFilePath, dbFilePath) {
  // Read CSV file
  const csvData = fs.readFileSync(csvFilePath, 'utf8');
  
  // Parse CSV
  const records = parse(csvData, {
    columns: true,
    skip_empty_lines: true,
    delimiter: ',',
    quote: '"'
  });
  
  // Create or open SQLite database
  const db = new sqlite3.Database(dbFilePath);
  
  // Begin transaction
  db.serialize(() => {
    db.run('BEGIN TRANSACTION');
    
    // Create table based on CSV headers
    if (records.length > 0) {
      const columns = Object.keys(records[0])
        .map(col => `"${col}" TEXT`)
        .join(', ');
      
      db.run(`CREATE TABLE IF NOT EXISTS bird_detections (${columns})`);
      
      // Prepare insert statement
      const placeholders = Object.keys(records[0])
        .map(() => '?')
        .join(', ');
      
      const stmt = db.prepare(`INSERT INTO bird_detections VALUES (${placeholders})`);
      
      // Insert records
      records.forEach(record => {
        stmt.run(Object.values(record));
      });
      
      stmt.finalize();
    }
    
    // Commit transaction
    db.run('COMMIT');
  });
  
  // Close database
  db.close();
  
  console.log(`Imported ${records.length} records to ${dbFilePath}`);
}

// Usage
const csvFilePath = process.argv[2] || 'input.csv';
const dbFilePath = process.argv[3] || 'bird_detections.db';

importCsvToSqlite(csvFilePath, dbFilePath);