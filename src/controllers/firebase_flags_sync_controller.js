const { getFirebaseAdmin } = require("../config/firebase");
const {
  ingestAggregatedEvent,
  normalizeUnixMs,
  mapVibrationToSeverity,
} = require("../services/reading_ingestion_service");

function nowIso() {
  return new Date().toISOString();
}

function getLat(reading) {
  const v = Number(reading.Latitude ?? reading.latitude ?? reading.lat);
  return Number.isFinite(v) ? v : NaN;
}

function getLon(reading) {
  const v = Number(reading.Longitude ?? reading.longitude ?? reading.lon);
  return Number.isFinite(v) ? v : NaN;
}

function getVibration(reading) {
  const v = Number(reading.zCorrected ?? reading.Vibration ?? reading.vibration ?? reading.z);
  return Number.isFinite(v) ? v : 0;
}

function eventTimestampMs(key, reading) {
  const fromPayload = normalizeUnixMs(reading.timestamp ?? reading.Timestamp);
  if (fromPayload) return fromPayload;
  return normalizeUnixMs(key) || Date.now();
}

function looksLikeReading(obj) {
  if (!obj || typeof obj !== "object") return false;
  // Heuristics: either flags exist, or coordinates exist.
  if (obj.potholeFlag !== undefined || obj.patchyFlag !== undefined) return true;
  if (
    obj.Latitude !== undefined ||
    obj.Longitude !== undefined ||
    obj.latitude !== undefined ||
    obj.longitude !== undefined ||
    obj.lat !== undefined ||
    obj.lon !== undefined
  ) {
    return true;
  }
  return false;
}

function flattenReadingsTree(root) {
  const out = [];

  function walk(node, pathParts) {
    if (!node || typeof node !== "object") return;

    // If this node itself looks like a reading payload, treat it as a leaf.
    if (looksLikeReading(node)) {
      out.push({ pathParts, key: pathParts[pathParts.length - 1] || "", reading: node });
      return;
    }

    // Otherwise traverse children.
    for (const [k, v] of Object.entries(node)) {
      if (v && typeof v === "object") {
        walk(v, [...pathParts, k]);
      }
    }
  }

  walk(root, []);
  return out;
}

function isAlreadyProcessed(reading) {
  const status = reading?._migration?.status;
  const processed = reading?._migration?.processed;
  if (status === "migrated" || status === "denied") return true;
  if (processed === true) return true;
  return false;
}

exports.syncFlaggedReadingsToAggregatedLocations = async (req, res) => {
  try {
    const admin = getFirebaseAdmin();
    const db = admin.database();

    const limit = Math.min(parseInt(req.body?.limit ?? req.query?.limit ?? "500", 10) || 500, 5000);
    const dryRun = Boolean(req.body?.dryRun ?? req.query?.dryRun);
    const reprocess = Boolean(req.body?.reprocess ?? req.query?.reprocess);
    const firebasePath =
      process.env.FIREBASE_READINGS_PATH ||
      process.env.FIREBASE_REPORTS_PATH ||
      "UsersData"; // Expecting a path that points directly at a readings collection

    const ref = db.ref(firebasePath);
    // Read once and flatten so we can handle nested structures like UsersData/<uid>/readings/<readingId>
    const snap = await ref.once("value");
    const all = snap.val() || {};

    const flattened = flattenReadingsTree(all)
      .filter((x) => x.reading && typeof x.reading === "object")
      .filter((x) => (reprocess ? true : !isAlreadyProcessed(x.reading)));

    // Roughly match "latest N" behavior by ordering by timestamp-ish key/payload.
    const candidates = flattened
      .sort((a, b) => {
        const aTs = eventTimestampMs(a.key, a.reading) || 0;
        const bTs = eventTimestampMs(b.key, b.reading) || 0;
        return aTs - bTs;
      })
      .slice(-limit);

    const results = {
      firebasePath,
      scanned: flattened.length,
      candidates: candidates.length,
      flagged: 0,
      migrated: 0,
      denied: 0,
      potholeEvents: 0,
      patchyEvents: 0,
      dryRun,
      reprocess,
      items: [],
    };

    console.log(
      `[FirebaseFlagsSync] start path=${firebasePath} scanned=${results.scanned} candidates=${results.candidates} dryRun=${dryRun} reprocess=${reprocess}`
    );

    for (const entry of candidates) {
      const key = entry.key;
      const reading = entry.reading;
      const itemRef = entry.pathParts.length ? ref.child(entry.pathParts.join("/")) : ref.child(key);

      const potholeFlag = Boolean(reading.potholeFlag);
      const patchyFlag = Boolean(reading.patchyFlag);

      // If both flags are false => deny and do not store in MySQL
      if (!potholeFlag && !patchyFlag) {
        results.denied += 1;
        results.items.push({ key, status: "denied", reason: "Flags false (potholeFlag & patchyFlag)" });
        if (!dryRun) {
          await itemRef.update({
            _migration: {
              status: "denied",
              processed: true,
              at: nowIso(),
              error: "Flags false (potholeFlag & patchyFlag)",
            },
          });
        }
        continue;
      }
      results.flagged += 1;

      const lat = getLat(reading);
      const lon = getLon(reading);

      // Accept zero coordinates per requirement; only deny non-finite values
      if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
        results.denied += 1;
        results.items.push({ key, status: "denied", reason: "invalid_coordinates" });
        // Mark as processed with denial if not dry-run
        if (!dryRun) {
          await itemRef.update({
            _migration: {
              status: "denied",
              processed: true,
              at: nowIso(),
              error: "invalid_coordinates",
            },
          });
        }
        continue;
      }

      const vibration = getVibration(reading);
      const tsMs = eventTimestampMs(key, reading);

      const eventsToInsert = [];
      if (potholeFlag) {
        eventsToInsert.push({
          type: "pothole",
          latitude: lat,
          longitude: lon,
          severity: mapVibrationToSeverity(vibration),
          timestampMs: tsMs,
        });
      }
      if (patchyFlag) {
        eventsToInsert.push({
          type: "patchy",
          latitude: lat,
          longitude: lon,
          severity: "Low",
          timestampMs: tsMs,
        });
      }

      if (dryRun) {
        results.migrated += eventsToInsert.length;
        results.potholeEvents += potholeFlag ? 1 : 0;
        results.patchyEvents += patchyFlag ? 1 : 0;
        results.items.push({ key, status: "would_migrate", types: eventsToInsert.map((e) => e.type) });
        continue;
      }

      try {
        for (const ev of eventsToInsert) {
          await ingestAggregatedEvent(ev);
        }

        results.migrated += eventsToInsert.length;
        results.potholeEvents += potholeFlag ? 1 : 0;
        results.patchyEvents += patchyFlag ? 1 : 0;

        await itemRef.update({
          _migration: {
            status: "migrated",
            processed: true,
            at: nowIso(),
            type: eventsToInsert.map((e) => e.type).join(","),
            severity: eventsToInsert.map((e) => e.severity).join(","),
          },
        });

        results.items.push({ key, status: "migrated", types: eventsToInsert.map((e) => e.type) });
      } catch (error) {
        results.denied += 1;
        results.items.push({ key, status: "error", error: error.message });
        await itemRef.update({
          _migration: {
            status: "denied",
            processed: true,
            at: nowIso(),
            error: error.message,
          },
        });
      }
    }

    console.log(
      `[FirebaseFlagsSync] done migrated=${results.migrated} denied=${results.denied} flagged=${results.flagged}`
    );

    res.json({ success: true, ...results });
  } catch (error) {
    console.error("Firebase flags sync error:", error);
    res.status(500).json({ success: false, message: "Failed to sync flagged readings to aggregated_locations", error: error.message });
  }
};
