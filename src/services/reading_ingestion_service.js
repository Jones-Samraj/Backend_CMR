const db = require("../config/db");

const severityOrder = { Low: 1, Medium: 2, High: 3 };

function normalizeUnixMs(ts) {
  const n = Number(ts);
  if (!Number.isFinite(n) || n <= 0) return null;
  // If value looks like unix seconds, convert to ms
  if (n < 1e12) return Math.round(n * 1000);
  return Math.round(n);
}

function mapVibrationToSeverity(vibrationValue) {
  // Matches pothole_user logic: zCorrected >= 9.0 => High else Medium
  const v = Math.abs(Number(vibrationValue));
  if (!Number.isFinite(v)) return "Low";
  if (v >= 9.0) return "High";
  if (v >= 7.0) return "Medium";
  return "Low";
}

function normalizeReading(payload) {
  const latitude = Number(payload.Latitude ?? payload.latitude ?? payload.lat);
  const longitude = Number(payload.Longitude ?? payload.longitude ?? payload.lon);
  const vibration = Number(payload.Vibration ?? payload.vibration ?? payload.z ?? payload.zCorrected);
  const speed = Number(payload.Speed ?? payload.speed ?? 0);
  const timestampMs = normalizeUnixMs(payload.timestamp ?? payload.Timestamp);

  return { latitude, longitude, vibration, speed, timestampMs };
}

function validateReadingPayload(payload) {
  if (!payload || typeof payload !== "object") {
    return { ok: false, message: "Reading must be an object" };
  }

  const { latitude: lat, longitude: lng, vibration, timestampMs } = normalizeReading(payload);

  if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
    return { ok: false, message: "Missing/invalid Latitude/Longitude" };
  }

  // Basic GPS lock check (your screenshot uses 0 when not locked)
  if (lat === 0 || lng === 0) {
    return { ok: false, message: "Denied: GPS not locked (Latitude/Longitude is 0)" };
  }

  if (lat < -90 || lat > 90 || lng < -180 || lng > 180) {
    return { ok: false, message: "Denied: Latitude/Longitude out of range" };
  }

  // We keep vibration optional here; event detection decides if it's pothole/patchy.
  if (payload.severity && !severityOrder[payload.severity]) {
    return { ok: false, message: "Invalid severity" };
  }

  if ((payload.timestamp !== undefined || payload.Timestamp !== undefined) && !timestampMs) {
    return { ok: false, message: "Invalid timestamp" };
  }

  return { ok: true };
}

async function upsertAggregatedLocationEvent(connection, event) {
  const type = event.type; // 'pothole' | 'patchy'
  const latRaw = Number(event.latitude);
  const lngRaw = Number(event.longitude);
  const timestampMs = normalizeUnixMs(event.timestampMs ?? event.timestamp ?? event.time);

  // Use rounded coordinates only for grid bucketing (grid_id),
  // but store exact lat/lng values in the table columns.
  const gridLat = Number.isFinite(latRaw) ? latRaw.toFixed(4) : null;
  const gridLng = Number.isFinite(lngRaw) ? lngRaw.toFixed(4) : null;

  if (gridLat === null || gridLng === null) {
    throw new Error("Invalid latitude/longitude for aggregated event");
  }

  const gridId = `${gridLat}_${gridLng}`;

  const severity = event.severity || "Low";

  const [existing] = await connection.query(
    "SELECT id, highest_severity FROM aggregated_locations WHERE grid_id = ?",
    [gridId]
  );

  const tsSqlSeconds = timestampMs ? Math.floor(timestampMs / 1000) : null;

  const potholeInc = type === "pothole" ? 1 : 0;
  const patchyInc = type === "patchy" ? 1 : 0;

  if (existing.length > 0) {
    const current = existing[0];
    const newHighest =
      severityOrder[severity] > severityOrder[current.highest_severity]
        ? severity
        : current.highest_severity;

    if (tsSqlSeconds) {
      await connection.query(
        `UPDATE aggregated_locations 
         SET total_potholes = total_potholes + ?,
             total_patchy = total_patchy + ?,
             highest_severity = ?,
             report_count = report_count + 1,
             last_reported_at = FROM_UNIXTIME(?)
         WHERE grid_id = ?`,
        [potholeInc, patchyInc, newHighest, tsSqlSeconds, gridId]
      );
    } else {
      await connection.query(
        `UPDATE aggregated_locations 
         SET total_potholes = total_potholes + ?,
             total_patchy = total_patchy + ?,
             highest_severity = ?,
             report_count = report_count + 1,
             last_reported_at = NOW()
         WHERE grid_id = ?`,
        [potholeInc, patchyInc, newHighest, gridId]
      );
    }

    return { gridId, created: false, aggregatedLocationId: current.id, highestSeverity: newHighest };
  }

  if (tsSqlSeconds) {
    const [result] = await connection.query(
      `INSERT INTO aggregated_locations 
       (grid_id, latitude, longitude, total_potholes, total_patchy, highest_severity, report_count, first_reported_at, last_reported_at)
       VALUES (?, ?, ?, ?, ?, ?, 1, FROM_UNIXTIME(?), FROM_UNIXTIME(?))`,
      [gridId, latRaw, lngRaw, potholeInc, patchyInc, severity, tsSqlSeconds, tsSqlSeconds]
    );

    return { gridId, created: true, aggregatedLocationId: result.insertId, highestSeverity: severity };
  }

  const [result] = await connection.query(
    `INSERT INTO aggregated_locations 
     (grid_id, latitude, longitude, total_potholes, total_patchy, highest_severity, report_count, first_reported_at, last_reported_at)
     VALUES (?, ?, ?, ?, ?, ?, 1, NOW(), NOW())`,
    [gridId, latRaw, lngRaw, potholeInc, patchyInc, severity]
  );

  return { gridId, created: true, aggregatedLocationId: result.insertId, highestSeverity: severity };
}

async function ingestReadingIntoAggregatedLocations(reading) {
  const validation = validateReadingPayload(reading);
  if (!validation.ok) {
    const err = new Error(validation.message);
    err.code = "INVALID_READING";
    throw err;
  }

  const connection = await db.promise().getConnection();
  try {
    await connection.beginTransaction();
    // Backward-compatible: treat a single reading as a pothole event
    const normalized = normalizeReading(reading);
    const result = await upsertAggregatedLocationEvent(connection, {
      type: "pothole",
      latitude: normalized.latitude,
      longitude: normalized.longitude,
      severity: reading.severity || mapVibrationToSeverity(normalized.vibration),
      timestampMs: normalized.timestampMs,
    });
    await connection.commit();
    return result;
  } catch (error) {
    await connection.rollback();
    throw error;
  } finally {
    connection.release();
  }
}

async function ingestAggregatedEvent(event) {
  const connection = await db.promise().getConnection();
  try {
    await connection.beginTransaction();
    const result = await upsertAggregatedLocationEvent(connection, event);
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
  normalizeUnixMs,
  normalizeReading,
  mapVibrationToSeverity,
  validateReadingPayload,
  upsertAggregatedLocationEvent,
  ingestReadingIntoAggregatedLocations,
  ingestAggregatedEvent,
};
