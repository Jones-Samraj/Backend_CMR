const mysql = require("mysql2");

const pool = mysql.createPool({
  host: process.env.DB_HOST || "localhost",
  port: process.env.DB_PORT || 3306,
  user: process.env.DB_USER || "root",
  password: process.env.DB_PASSWORD || "",
  database: process.env.DB_NAME || "pothole_detection_system",
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
});

// Promisify for async/await usage
const promisePool = pool.promise();

// Test connection once at startup for clearer logs
pool.getConnection((err, connection) => {
  if (err) {
    console.error("[DB] Database connection failed:", err.message);
  } else {
    console.log("[DB] Database connected successfully!");
    connection.release();
  }
});

// Export pool with promise method that returns promisePool
module.exports = pool;
module.exports.promise = () => promisePool;
