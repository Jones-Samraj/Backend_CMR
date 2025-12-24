const express = require("express");
const cors = require("cors");
const app = express();

// Middleware
app.use(cors());
app.use(express.json({ limit: "10mb" }));
app.use(express.urlencoded({ extended: true, limit: "10mb" }));

// Request logging middleware (clean, single-line per request)
app.use((req, res, next) => {
  try {
    const timestamp = new Date().toISOString();
    const method = req.method;
    const url = req.originalUrl || req.url || req.path;
    console.log(`[REQ ${timestamp}] ${method} ${url}`);
  } catch {
    // Fallback in case logging itself throws
    console.log("[REQ] Incoming request");
  }
  next();
});

// Routes
app.use("/api/admin", require("./routes/admin_routes"));
app.use("/api/user", require("./routes/user_routes"));
app.use("/api/contractor", require("./routes/contractor_routes"));
app.use("/api/reports", require("./routes/report_routes"));
app.use("/api/auth", require("./routes/auth_routes"));

// Health check endpoint
app.get("/api/health", (req, res) => {
  res.json({ status: "ok", timestamp: new Date().toISOString() });
});

// 404 handler
app.use((req, res) => {
  res.status(404).json({ message: "Route not found" });
});

// Error handler
app.use((err, req, res, next) => {
  console.error("Server Error:", err);
  res.status(500).json({ message: "Internal server error", error: err.message });
});

module.exports = app;
