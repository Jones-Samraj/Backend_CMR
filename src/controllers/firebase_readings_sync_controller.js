const { getFirebaseAdmin } = require("../config/firebase");
const {
  validateReadingPayload,
  ingestAggregatedEvent,
  normalizeReading,
  normalizeUnixMs,
} = require("../services/reading_ingestion_service");

function nowIso() {
  return new Date().toISOString();
}

function pickPendingFirebaseItems(all, reprocess) {
  const entries = Object.entries(all || {}).filter(
    ([, value]) => value && typeof value === "object"
  );

  if (reprocess) return entries;

  return entries.filter(([, value]) => {
    const status = value?._migration?.status;
    const processed = value?._migration?.processed;

    if (status === "migrated" || status === "denied") return false;
    if (processed === true) return false;
    return true;
  });
}

function envNumber(name, fallback) {
  const raw = process.env[name];
  if (raw === undefined || raw === null || raw === "") return fallback;
  const n = Number(raw);
  return Number.isFinite(n) ? n : fallback;
}

// Backend-tuned defaults (can be overridden via env vars)
// Use these if your Firebase "Vibration" values aren't exactly the same scale as the app.
//
// Env overrides:
// READINGS_MIN_SPEED_KMH
// READINGS_SPEED_NOISE_KMH
// READINGS_POTHOLE_PEAK_DELTA
// READINGS_POTHOLE_Z_MIN
// READINGS_POTHOLE_COOLDOWN_MS
// READINGS_POTHOLE_HIGH_Z
// READINGS_PATCHY_MIN
// READINGS_PATCHY_MAX
// READINGS_PATCHY_DURATION_MS
// READINGS_PATCHY_RESET_MS
const MIN_SPEED = envNumber("READINGS_MIN_SPEED_KMH", 0);
const SPEED_NOISE = envNumber("READINGS_SPEED_NOISE_KMH", 3);

const PEAK_DELTA = envNumber("READINGS_POTHOLE_PEAK_DELTA", 3.5);
const Z_MIN_THRESHOLD = envNumber("READINGS_POTHOLE_Z_MIN", 8.0);
const COOLDOWN_MS = envNumber("READINGS_POTHOLE_COOLDOWN_MS", 4000);
const POTHOLE_HIGH_Z = envNumber("READINGS_POTHOLE_HIGH_Z", 12.0);

const PATCHY_MIN = envNumber("READINGS_PATCHY_MIN", 2.0);
const PATCHY_MAX = envNumber("READINGS_PATCHY_MAX", 6.0);
const PATCHY_DURATION = envNumber("READINGS_PATCHY_DURATION_MS", 3000);
const PATCHY_RESET_MS = envNumber("READINGS_PATCHY_RESET_MS", 800);

function coerceEventTimestampMs(readingKey, reading) {
  // Prefer payload timestamp; fallback to Firebase key (often unix seconds)
  const fromPayload = normalizeUnixMs(reading.timestamp ?? reading.Timestamp);
  if (fromPayload) return fromPayload;
  return normalizeUnixMs(readingKey);
}

function computeSpeedKmh(rawSpeed) {
  let sp = Number(rawSpeed);
  if (!Number.isFinite(sp)) sp = 0;
  if (sp < SPEED_NOISE) sp = 0;
  return sp;
}

function mapPotholeSeverityFromZ(zCorrected) {
  return zCorrected >= POTHOLE_HIGH_Z ? "High" : "Medium";
}

exports.syncReadingsToAggregatedLocations = async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.body?.limit ?? "200", 10) || 200, 2000);
    const dryRun = Boolean(req.body?.dryRun);
    const reprocess = Boolean(req.body?.reprocess);
    const verbose = req.body?.verbose === undefined ? true : Boolean(req.body?.verbose);

    // Your screenshot path looks like: UsersData/<uid>/readings
    const firebasePath =
      process.env.FIREBASE_READINGS_PATH ||
      process.env.FIREBASE_REPORTS_PATH ||
      "UsersData";

    const admin = getFirebaseAdmin();
    const ref = admin.database().ref(firebasePath);

    const snapshot = await ref.once("value");
    const all = snapshot.val() || {};

    // Sort by time to replicate delta/cooldown logic correctly
    const candidates = pickPendingFirebaseItems(all, reprocess)
      .sort(([aKey, aVal], [bKey, bVal]) => {
        const aTs = coerceEventTimestampMs(aKey, aVal) || 0;
        const bTs = coerceEventTimestampMs(bKey, bVal) || 0;
        return aTs - bTs;
      })
      .slice(0, limit);

    // Detection state (mirrors HomeScreen refs)
    let prevZ = 0;
    let lastDetectionTime = 0;
    let patchyStart = null;
    let patchyAlert = false;
    let lastPatchyVibrationTime = 0;

    const results = {
      firebasePath,
      dryRun,
      reprocess,
      thresholds: {
        MIN_SPEED,
        SPEED_NOISE,
        PEAK_DELTA,
        Z_MIN_THRESHOLD,
        COOLDOWN_MS,
        POTHOLE_HIGH_Z,
        PATCHY_MIN,
        PATCHY_MAX,
        PATCHY_DURATION,
        PATCHY_RESET_MS,
      },
      scanned: Object.keys(all).length,
      candidates: candidates.length,
      migrated: 0,
      denied: 0,
      errors: 0,
      items: [],
    };

    console.log(
      `[FirebaseReadingsSync] start path=${firebasePath} scanned=${results.scanned} candidates=${results.candidates} dryRun=${dryRun} reprocess=${reprocess}`
    );

    if (verbose) {
      console.log(
        `[FirebaseReadingsSync] thresholds minSpeed=${MIN_SPEED} speedNoise=${SPEED_NOISE} pothole(delta>${PEAK_DELTA}, z>=${Z_MIN_THRESHOLD}, cooldownMs=${COOLDOWN_MS}, highZ>=${POTHOLE_HIGH_Z}) patchy(z>=${PATCHY_MIN} && z<${PATCHY_MAX} for ${PATCHY_DURATION}ms)`
      );
    }

    for (const [key, reading] of candidates) {
      const validation = validateReadingPayload(reading);

      if (!validation.ok) {
        results.denied += 1;
        results.items.push({ key, status: "denied", reason: validation.message });

        if (verbose) {
          console.log(`[FirebaseReadingsSync] DENIED key=${key} reason=${validation.message}`);
        }

        if (!dryRun) {
          await ref.child(key).update({
            _migration: {
              status: "denied",
              processed: true,
              at: nowIso(),
              error: validation.message,
            },
          });
        }

        continue;
      }

      const normalized = normalizeReading(reading);
      const timestampMs = coerceEventTimestampMs(key, reading) || Date.now();

      const speed = computeSpeedKmh(normalized.speed);
      const zCorrected = Math.abs(Number(normalized.vibration));
      const delta = Math.abs(zCorrected - prevZ);

      // --- 1) PATCHY LOGIC (same as app) ---
      let detectedEvent = null;
      if (
        speed >= MIN_SPEED &&
        Number.isFinite(zCorrected) &&
        zCorrected >= PATCHY_MIN &&
        zCorrected < PATCHY_MAX
      ) {
        lastPatchyVibrationTime = timestampMs;
        if (!patchyStart) patchyStart = timestampMs;

        if (
          timestampMs - patchyStart >= PATCHY_DURATION &&
          !patchyAlert
        ) {
          detectedEvent = {
            type: "patchy",
            latitude: normalized.latitude,
            longitude: normalized.longitude,
            severity: "Low",
            timestampMs,
          };
          patchyAlert = true;
        }
      } else if (timestampMs - lastPatchyVibrationTime > PATCHY_RESET_MS) {
        patchyStart = null;
        patchyAlert = false;
      }

      // --- 2) POTHOLE LOGIC (same as app) ---
      if (!detectedEvent) {
        if (
          speed >= MIN_SPEED &&
          Number.isFinite(zCorrected) &&
          delta > PEAK_DELTA &&
          zCorrected >= Z_MIN_THRESHOLD &&
          timestampMs - lastDetectionTime > COOLDOWN_MS
        ) {
          detectedEvent = {
            type: "pothole",
            latitude: normalized.latitude,
            longitude: normalized.longitude,
            severity: mapPotholeSeverityFromZ(zCorrected),
            timestampMs,
          };
          lastDetectionTime = timestampMs;
        }
      }

      prevZ = Number.isFinite(zCorrected) ? zCorrected : prevZ;

      // If conditions not satisfied => deny
      if (!detectedEvent) {
        results.denied += 1;
        results.items.push({ key, status: "denied", reason: "No pothole/patchy conditions met" });

        if (verbose) {
          console.log(
            `[FirebaseReadingsSync] DENIED key=${key} reason=no_conditions speed=${speed} z=${Number.isFinite(zCorrected) ? zCorrected.toFixed(3) : "NaN"} delta=${Number.isFinite(delta) ? delta.toFixed(3) : "NaN"}`
          );
        }

        if (!dryRun) {
          await ref.child(key).update({
            _migration: {
              status: "denied",
              processed: true,
              at: nowIso(),
              error: "No pothole/patchy conditions met",
            },
          });
        }

        continue;
      }

      if (dryRun) {
        results.migrated += 1;
        results.items.push({ key, status: "would_migrate", type: detectedEvent.type });

        if (verbose) {
          console.log(`[FirebaseReadingsSync] WOULD_MIGRATE key=${key} type=${detectedEvent.type} severity=${detectedEvent.severity}`);
        }
        continue;
      }

      try {
        const insertResult = await ingestAggregatedEvent(detectedEvent);

        results.migrated += 1;
        results.items.push({
          key,
          status: "migrated",
          type: detectedEvent.type,
          severity: detectedEvent.severity,
          gridId: insertResult.gridId,
          aggregatedLocationId: insertResult.aggregatedLocationId,
          created: insertResult.created,
        });

        await ref.child(key).update({
          _migration: {
            status: "migrated",
            processed: true,
            at: nowIso(),
            type: detectedEvent.type,
            severity: detectedEvent.severity,
            gridId: insertResult.gridId,
            aggregatedLocationId: insertResult.aggregatedLocationId,
          },
        });

        if (verbose) {
          console.log(
            `[FirebaseReadingsSync] MIGRATED key=${key} type=${detectedEvent.type} severity=${detectedEvent.severity} gridId=${insertResult.gridId} created=${insertResult.created}`
          );
        }
      } catch (error) {
        results.errors += 1;
        results.items.push({ key, status: "error", error: error.message });

        console.log(`[FirebaseReadingsSync] ERROR key=${key} error=${error.message}`);

        await ref.child(key).update({
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
      `[FirebaseReadingsSync] done migrated=${results.migrated} denied=${results.denied} errors=${results.errors}`
    );

    res.json({ success: true, ...results });
  } catch (error) {
    console.error("Firebase readings sync error:", error);
    res.status(500).json({
      success: false,
      message: "Failed to sync readings from Firebase to aggregated_locations",
      error: error.message,
    });
  }
};
