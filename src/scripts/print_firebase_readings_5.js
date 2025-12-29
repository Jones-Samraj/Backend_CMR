const path = require("path");

// Load Backend_S3/.env (same behavior as src/server.js)
require("dotenv").config({
  path: path.resolve(__dirname, "..", "..", ".env"),
});

const { getFirebaseAdmin } = require("../config/firebase");

function parseArgs(argv) {
  const args = {
    path: undefined,
    uid: undefined,
    limit: 5,
  };

  for (const raw of argv) {
    if (!raw.startsWith("--")) continue;

    const [key, ...rest] = raw.slice(2).split("=");
    const value = rest.length ? rest.join("=") : "true";

    if (key === "path") args.path = value;
    if (key === "uid") args.uid = value;
    if (key === "limit") {
      const n = Number(value);
      if (Number.isFinite(n) && n > 0) args.limit = Math.floor(n);
    }
  }

  return args;
}

function looksLikeReading(value) {
  if (!value || typeof value !== "object") return false;
  const hasLat = value.Latitude !== undefined || value.latitude !== undefined || value.lat !== undefined;
  const hasLng = value.Longitude !== undefined || value.longitude !== undefined || value.lon !== undefined;
  const hasVib = value.Vibration !== undefined || value.vibration !== undefined || value.z !== undefined;
  return (hasLat && hasLng) || (hasVib && (hasLat || hasLng));
}

function looksLikeReadingsCollection(value) {
  if (!value || typeof value !== "object") return false;
  const entries = Object.entries(value);
  if (entries.length === 0) return false;
  let hits = 0;
  for (const [, v] of entries.slice(0, 10)) {
    if (looksLikeReading(v)) hits += 1;
  }
  return hits > 0;
}

function formatMaybeTimestamp(key) {
  const n = Number(key);
  if (!Number.isFinite(n) || n <= 0) return null;
  // seconds vs ms heuristic
  const ms = n < 1e12 ? n * 1000 : n;
  const d = new Date(ms);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString();
}

async function main() {
  const args = parseArgs(process.argv.slice(2));

  const basePath =
    args.path || process.env.FIREBASE_READINGS_PATH || process.env.FIREBASE_REPORTS_PATH || "UsersData";

  const admin = getFirebaseAdmin();
  const db = admin.database();

  let readingsRef = db.ref(basePath);
  let inferredPath = basePath;

  // Try to infer the actual readings location for common layouts.
  const snap = await readingsRef.once("value");
  const val = snap.val();

  if (!val) {
    console.log(`[Firebase] No data found at path: ${basePath}`);
    process.exit(0);
  }

  // Case A: basePath points at a user object that contains { readings: {...} }
  if (val && typeof val === "object" && val.readings && looksLikeReadingsCollection(val.readings)) {
    readingsRef = db.ref(`${basePath}/readings`);
    inferredPath = `${basePath}/readings`;
  }
  // Case B: basePath points at UsersData (uids underneath)
  else if (
    val &&
    typeof val === "object" &&
    !looksLikeReadingsCollection(val) &&
    Object.values(val).some((child) => child && typeof child === "object" && child.readings)
  ) {
    const uids = Object.keys(val);
    const uid = args.uid || (uids.length === 1 ? uids[0] : uids[0]);

    if (!args.uid && uids.length > 1) {
      console.log(
        `[Firebase] Multiple users under ${basePath}. Using uid=${uid}. (Pass --uid=<uid> to choose.)`
      );
    }

    readingsRef = db.ref(`${basePath}/${uid}/readings`);
    inferredPath = `${basePath}/${uid}/readings`;
  }
  // Case C: basePath already looks like a readings collection
  else if (looksLikeReadingsCollection(val)) {
    // keep readingsRef as-is
  } else {
    console.log(
      `[Firebase] Path ${basePath} does not look like a readings collection.\n` +
        `Try passing --path=UsersData/<uid>/readings (or set FIREBASE_READINGS_PATH).`
    );
    process.exit(1);
  }

  const limit = Math.min(args.limit, 50);

  const lastSnap = await readingsRef.orderByKey().limitToLast(limit).once("value");
  const readings = lastSnap.val() || {};
  const items = Object.entries(readings).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));

  console.log(`[Firebase] Readings path: ${inferredPath}`);
  console.log(`[Firebase] Printing last ${items.length} reading(s) (limit=${limit})`);

  for (const [key, reading] of items) {
    const iso = formatMaybeTimestamp(key);
    const ts = iso ? ` (${iso})` : "";
    console.log(`\n--- ${key}${ts} ---`);
    console.log(JSON.stringify(reading, null, 2));
  }

  process.exit(0);
}

main().catch((err) => {
  console.error("[Firebase] Failed to fetch readings:", err);
  process.exit(1);
});
