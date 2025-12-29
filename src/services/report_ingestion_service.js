const db = require("../config/db");

// Helper function to convert ISO datetime to MySQL format
const toMySQLDatetime = (isoString) => {
  if (!isoString) return new Date().toISOString().slice(0, 19).replace("T", " ");
  const date = new Date(isoString);
  return date.toISOString().slice(0, 19).replace("T", " ");
};

const severityOrder = { Low: 1, Medium: 2, High: 3 };

function validateReportPayload(payload) {
  if (!payload || typeof payload !== "object") {
    return { ok: false, message: "Payload must be an object" };
  }

  const { report_id, device_id, anomalies } = payload;

  if (!report_id || typeof report_id !== "string") {
    return { ok: false, message: "Missing/invalid report_id" };
  }

  if (!device_id || typeof device_id !== "string") {
    return { ok: false, message: "Missing/invalid device_id" };
  }

  if (!Array.isArray(anomalies) || anomalies.length === 0) {
    return { ok: false, message: "Missing/invalid anomalies (must be a non-empty array)" };
  }

  for (const a of anomalies) {
    if (!a || typeof a !== "object") {
      return { ok: false, message: "Invalid anomaly item" };
    }

    if (a.type !== "pothole" && a.type !== "road_anomaly") {
      return { ok: false, message: "Invalid anomaly type" };
    }

    if (a.type === "pothole") {
      if (typeof a.latitude !== "number" || typeof a.longitude !== "number") {
        return { ok: false, message: "Pothole anomaly must include latitude/longitude" };
      }
    }

    if (a.type === "road_anomaly") {
      const startLat = a.start_latitude ?? a.latitude;
      const startLng = a.start_longitude ?? a.longitude;

      if (typeof startLat !== "number" || typeof startLng !== "number") {
        return { ok: false, message: "Road anomaly must include start coordinates" };
      }

      if (a.end_latitude !== undefined && typeof a.end_latitude !== "number") {
        return { ok: false, message: "Invalid end_latitude" };
      }
      if (a.end_longitude !== undefined && typeof a.end_longitude !== "number") {
        return { ok: false, message: "Invalid end_longitude" };
      }
    }

    if (a.severity && !severityOrder[a.severity]) {
      return { ok: false, message: "Invalid severity" };
    }

    // Optional cross-check similar to pothole_user logic: if speed exists, require > 10
    if (a.speed !== undefined && (typeof a.speed !== "number" || a.speed <= 10)) {
      return { ok: false, message: "Invalid speed (must be > 10)" };
    }
  }

  return { ok: true };
}

async function updateAggregatedLocation(connection, data, type) {
  const lat = parseFloat(data.latitude).toFixed(4);
  const lng = parseFloat(data.longitude).toFixed(4);
  const gridId = `${lat}_${lng}`;

  const severity = data.severity || "Medium";

  // Check if grid exists
  const [existing] = await connection.query(
    "SELECT * FROM aggregated_locations WHERE grid_id = ?",
    [gridId]
  );

  if (existing.length > 0) {
    const current = existing[0];
    const newHighest =
      severityOrder[severity] > severityOrder[current.highest_severity]
        ? severity
        : current.highest_severity;

    await connection.query(
      `UPDATE aggregated_locations 
       SET total_potholes = total_potholes + ?,
           total_patchy = total_patchy + ?,
           highest_severity = ?,
           report_count = report_count + 1,
           last_reported_at = NOW()
       WHERE grid_id = ?`,
      [
        type === "pothole" ? 1 : 0,
        type === "patchy" ? 1 : 0,
        newHighest,
        gridId,
      ]
    );
  } else {
    await connection.query(
      `INSERT INTO aggregated_locations 
       (grid_id, latitude, longitude, total_potholes, total_patchy, highest_severity, first_reported_at, last_reported_at)
       VALUES (?, ?, ?, ?, ?, ?, NOW(), NOW())`,
      [
        gridId,
        lat,
        lng,
        type === "pothole" ? 1 : 0,
        type === "patchy" ? 1 : 0,
        severity,
      ]
    );
  }
}

async function ingestReportWithinTransaction(connection, payload) {
  const validation = validateReportPayload(payload);
  if (!validation.ok) {
    const err = new Error(validation.message);
    err.code = "INVALID_REPORT_PAYLOAD";
    throw err;
  }

  const { report_id, device_id, reported_at, anomalies, userId = null } = payload;

  // Calculate totals
  const potholes = anomalies.filter((a) => a.type === "pothole");
  const patchyRoads = anomalies.filter((a) => a.type === "road_anomaly");

  // Calculate health score (100 - penalties)
  const penalty = potholes.length * 15 + patchyRoads.length * 5;
  const healthScore = Math.max(0, 100 - penalty);

  // Insert main report
  const [reportResult] = await connection.query(
    `INSERT INTO reports (report_id, user_id, device_id, reported_at, total_potholes, total_patchy_roads, health_score, status)
     VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')`,
    [
      report_id,
      userId,
      device_id,
      toMySQLDatetime(reported_at),
      potholes.length,
      patchyRoads.length,
      healthScore,
    ]
  );

  const dbReportId = reportResult.insertId;

  // Insert pothole detections
  for (const pothole of potholes) {
    await connection.query(
      `INSERT INTO pothole_detections 
       (report_id, location_id, latitude, longitude, severity, timestamp, synced)
       VALUES (?, ?, ?, ?, ?, ?, TRUE)`,
      [
        dbReportId,
        pothole.location_id,
        pothole.latitude,
        pothole.longitude,
        pothole.severity || "Medium",
        toMySQLDatetime(pothole.timestamp),
      ]
    );

    await updateAggregatedLocation(connection, pothole, "pothole");
  }

  // Insert road anomalies (patchy roads)
  for (const patchy of patchyRoads) {
    await connection.query(
      `INSERT INTO road_anomalies 
       (report_id, location_id, start_latitude, start_longitude, end_latitude, end_longitude, severity, start_timestamp, end_timestamp, duration_seconds)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        dbReportId,
        patchy.location_id,
        patchy.start_latitude || patchy.latitude,
        patchy.start_longitude || patchy.longitude,
        patchy.end_latitude,
        patchy.end_longitude,
        patchy.severity || "Medium",
        toMySQLDatetime(patchy.start_timestamp),
        toMySQLDatetime(patchy.end_timestamp),
        patchy.duration_seconds,
      ]
    );

    await updateAggregatedLocation(
      connection,
      {
        latitude: patchy.start_latitude || patchy.latitude,
        longitude: patchy.start_longitude || patchy.longitude,
        severity: patchy.severity,
      },
      "patchy"
    );
  }

  return {
    reportId: report_id,
    dbId: dbReportId,
    totalPotholes: potholes.length,
    totalPatchy: patchyRoads.length,
    healthScore,
    status: "pending",
  };
}

async function ingestReport(payload) {
  const connection = await db.promise().getConnection();

  try {
    await connection.beginTransaction();
    const result = await ingestReportWithinTransaction(connection, payload);
    await connection.commit();
    return result;
  } catch (error) {
    await connection.rollback();
    throw error;
  } finally {
    connection.release();
  }
}

module.exports = {
  validateReportPayload,
  ingestReportWithinTransaction,
  ingestReport,
};
