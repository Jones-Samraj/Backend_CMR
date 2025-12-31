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
  if (
    obj.potholeFlag !== undefined ||
    obj.patchyFlag !== undefined ||
    obj.pothole !== undefined ||
    obj.patchy !== undefined
  ) {
    return true;
  }
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

function getFlag(reading, primaryKey, fallbackKey) {
  if (!reading || typeof reading !== "object") return false;
  if (reading[primaryKey] !== undefined) return Boolean(reading[primaryKey]);
  if (fallbackKey && reading[fallbackKey] !== undefined) return Boolean(reading[fallbackKey]);
  return false;
}

function isGpsLocked(reading) {
  if (!reading || typeof reading !== "object") return true;
  // If gpsFix is provided, respect it.
  if (reading.gpsFix !== undefined) return Boolean(reading.gpsFix);
  if (reading.GpsFix !== undefined) return Boolean(reading.GpsFix);
  return true;
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
  // For flags-based sync we only treat "migrated" as final.
  // Denied/skipped items may become eligible later when flags/coords change.
  return status === "migrated";
}

function envNumber(name, fallback) {
  const raw = process.env[name];
  if (raw === undefined || raw === null || raw === "") return fallback;
  const n = Number(raw);
  return Number.isFinite(n) ? n : fallback;
}

async function runFlaggedReadingsSync(options = {}) {
  const admin = getFirebaseAdmin();
  const db = admin.database();

  const limit = Math.min(
    parseInt(options.limit ?? envNumber("FIREBASE_FLAGS_SYNC_LIMIT", 500), 10) || 500,
    5000
  );
  const dryRun = Boolean(options.dryRun ?? false);
  const reprocess = Boolean(options.reprocess ?? false);
  const firebasePath =
    options.firebasePath ||
    process.env.FIREBASE_READINGS_PATH ||
    process.env.FIREBASE_REPORTS_PATH ||
    "UsersData";

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

    const potholeFlag = getFlag(reading, "potholeFlag", "pothole");
    const patchyFlag = getFlag(reading, "patchyFlag", "patchy");

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

    // Deny when GPS isn't locked (or coords are clearly missing)
    if (!isGpsLocked(reading) || lat === 0 || lon === 0) {
      results.denied += 1;
      results.items.push({ key, status: "denied", reason: "gps_not_locked_or_zero_coords" });
      if (!dryRun) {
        await itemRef.update({
          _migration: {
            status: "denied",
            processed: true,
            at: nowIso(),
            error: "gps_not_locked_or_zero_coords",
          },
        });
      }
      continue;
    }

    // Only deny non-finite values after the zero check
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

  return results;
}

async function processFlaggedReadingSnapshot(snapshot, options = {}) {
  const reading = snapshot.val();
  if (!reading || typeof reading !== "object") return { processed: false, reason: "not_object" };

  const key = snapshot.key || "";
  const itemRef = snapshot.ref;

  // Avoid reprocessing already migrated items unless explicitly asked.
  const reprocess = Boolean(options.reprocess ?? false);
  if (!reprocess && isAlreadyProcessed(reading)) {
    return { processed: false, reason: "already_migrated" };
  }

  const potholeFlag = getFlag(reading, "potholeFlag", "pothole");
  const patchyFlag = getFlag(reading, "patchyFlag", "patchy");
  if (!potholeFlag && !patchyFlag) {
    // Not eligible yet; do nothing so future flag flips can trigger processing.
    return { processed: false, reason: "flags_false" };
  }

  const lat = getLat(reading);
  const lon = getLon(reading);
  if (!isGpsLocked(reading) || lat === 0 || lon === 0) {
    return { processed: false, reason: "gps_not_locked_or_zero_coords" };
  }
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
    return { processed: false, reason: "invalid_coordinates" };
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

  // Write to MySQL
  for (const ev of eventsToInsert) {
    await ingestAggregatedEvent(ev);
  }

  // Mark migrated in Firebase (idempotent: if already migrated, skip update)
  if (reading?._migration?.status !== "migrated") {
    await itemRef.update({
      _migration: {
        status: "migrated",
        processed: true,
        at: nowIso(),
        type: eventsToInsert.map((e) => e.type).join(","),
        severity: eventsToInsert.map((e) => e.severity).join(","),
      },
    });
  }

  return { processed: true, key, types: eventsToInsert.map((e) => e.type) };
}

function startFlaggedReadingsWatcher(options = {}) {
  const firebasePath =
    options.firebasePath ||
    process.env.FIREBASE_READINGS_PATH ||
    process.env.FIREBASE_REPORTS_PATH ||
    "UsersData";

  const reprocess = Boolean(options.reprocess ?? false);

  const admin = getFirebaseAdmin();
  const rootRef = admin.database().ref(firebasePath);

  const watchedRefs = new Set();
  const inFlight = new Set();

  const attachCollection = (collectionRef, label) => {
    const refKey = collectionRef.toString();
    if (watchedRefs.has(refKey)) return;
    watchedRefs.add(refKey);

    const handle = async (snap, kind) => {
      const snapKey = snap.key || "";
      const flightKey = `${collectionRef.toString()}::${snapKey}`;
      if (inFlight.has(flightKey)) return;
      inFlight.add(flightKey);
      try {
        const val = snap.val();

        // If this is a container node (e.g. date bucket, uid node, etc.),
        // attach a watcher to it so we can reach leaf reading objects.
        if (val && typeof val === "object" && !looksLikeReading(val)) {
          attachCollection(snap.ref, `${label}/${snapKey}`);
          return;
        }

        const result = await processFlaggedReadingSnapshot(snap, { reprocess });
        if (result.processed) {
          console.log(
            `[FirebaseFlagsWatch] ${kind} ${label} key=${snapKey} migrated types=${(result.types || []).join(",")}`
          );
        }
      } catch (e) {
        console.error(
          `[FirebaseFlagsWatch] ${kind} ${label} key=${snapKey} error=${e.message}`
        );
      } finally {
        inFlight.delete(flightKey);
      }
    };

    // These fire for existing children on initial attach and for future updates.
    collectionRef.on("child_added", (snap) => handle(snap, "child_added"));
    collectionRef.on("child_changed", (snap) => handle(snap, "child_changed"));
  };

  // Attach to the provided path; if it's already a readings collection this is enough.
  attachCollection(rootRef, firebasePath);

  // Also support common nesting: UsersData/<uid>/readings/<readingId>
  const attachIfUserHasReadings = (userSnap, kind) => {
    const userVal = userSnap.val();
    if (!userVal || typeof userVal !== "object") return;
    if (looksLikeReading(userVal)) return;

    const readingsNode = userVal.readings;
    if (readingsNode && typeof readingsNode === "object") {
      attachCollection(userSnap.ref.child("readings"), `${firebasePath}/${userSnap.key}/readings`);
    }

    // If there are other nested buckets, attach recursively from the user node as well.
    // This helps when the structure is UsersData/<uid>/<someBucket>/<readingId>.
    attachCollection(userSnap.ref, `${firebasePath}/${userSnap.key}`);

    if (kind === "child_changed") {
      // No-op, but keeps parity with log patterns.
    }
  };

  rootRef.on("child_added", (userSnap) => attachIfUserHasReadings(userSnap, "child_added"));
  rootRef.on("child_changed", (userSnap) => attachIfUserHasReadings(userSnap, "child_changed"));

  console.log(`[FirebaseFlagsWatch] listening path=${firebasePath} reprocess=${reprocess}`);
  return { firebasePath };
}

exports.syncFlaggedReadingsToAggregatedLocations = async (req, res) => {
  try {
    const results = await runFlaggedReadingsSync({
      limit: req.body?.limit ?? req.query?.limit,
      dryRun: req.body?.dryRun ?? req.query?.dryRun,
      reprocess: req.body?.reprocess ?? req.query?.reprocess,
    });
    res.json({ success: true, ...results });
  } catch (error) {
    console.error("Firebase flags sync error:", error);
    res.status(500).json({ success: false, message: "Failed to sync flagged readings to aggregated_locations", error: error.message });
  }
};

module.exports.runFlaggedReadingsSync = runFlaggedReadingsSync;
module.exports.startFlaggedReadingsWatcher = startFlaggedReadingsWatcher;
