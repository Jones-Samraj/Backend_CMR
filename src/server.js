const path = require("path");
require("dotenv").config({
  path: path.resolve(__dirname, "..", ".env"),
});
const app = require("./app");
const { runFlaggedReadingsSync, startFlaggedReadingsWatcher } = require("./controllers/firebase_flags_sync_controller");

const PORT = process.env.PORT || 5000;

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);

  // Startup sync: fetch from Firebase and upsert into MySQL on every backend start.
  // Controlled via env vars:
  // STARTUP_FLAGS_SYNC (default: true)
  // STARTUP_FLAGS_SYNC_LIMIT (default: 500)
  // STARTUP_FLAGS_SYNC_REPROCESS (default: false)
  const startupEnabledRaw = process.env.STARTUP_FLAGS_SYNC;
  const startupEnabled = startupEnabledRaw === undefined ? true : String(startupEnabledRaw).toLowerCase() !== "false";
  if (!startupEnabled) {
    console.log("[StartupFlagsSync] disabled via STARTUP_FLAGS_SYNC=false");
    return;
  }

  const limit = Number(process.env.STARTUP_FLAGS_SYNC_LIMIT || 500);
  const reprocess = String(process.env.STARTUP_FLAGS_SYNC_REPROCESS || "false").toLowerCase() === "true";

  setImmediate(() => {
    runFlaggedReadingsSync({ limit, dryRun: false, reprocess })
      .then((r) => {
        console.log(
          `[StartupFlagsSync] done migrated=${r.migrated} denied=${r.denied} flagged=${r.flagged} candidates=${r.candidates}`
        );
      })
      .catch((err) => {
        // Do not crash the server on startup sync failure.
        console.error(`[StartupFlagsSync] failed: ${err.message}`);
      });
  });

  // Realtime watch: keep MySQL updated as Firebase values change.
  // REALTIME_FLAGS_WATCH (default: true)
  // REALTIME_FLAGS_WATCH_REPROCESS (default: false)
  const watchEnabledRaw = process.env.REALTIME_FLAGS_WATCH;
  const watchEnabled = watchEnabledRaw === undefined ? true : String(watchEnabledRaw).toLowerCase() !== "false";
  if (watchEnabled) {
    const watchReprocess = String(process.env.REALTIME_FLAGS_WATCH_REPROCESS || "false").toLowerCase() === "true";
    try {
      startFlaggedReadingsWatcher({ reprocess: watchReprocess });
    } catch (err) {
      console.error(`[RealtimeFlagsWatch] failed to start: ${err.message}`);
    }
  } else {
    console.log("[RealtimeFlagsWatch] disabled via REALTIME_FLAGS_WATCH=false");
  }
});
