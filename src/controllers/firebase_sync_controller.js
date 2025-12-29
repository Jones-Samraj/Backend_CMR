const db = require("../config/db");
const { getFirebaseAdmin } = require("../config/firebase");
const { ingestReport, validateReportPayload } = require("../services/report_ingestion_service");

function nowIso() {
  return new Date().toISOString();
}

function pickPendingFirebaseReports(all) {
  return Object.entries(all || {})
    .filter(([, value]) => value && typeof value === "object")
    .filter(([, value]) => {
      const status = value.migrationStatus;
      const processed = value.processed;

      // Treat as pending if not marked migrated/denied
      if (status === "migrated" || status === "denied") return false;
      if (processed === true) return false;

      return true;
    });
}

exports.syncReportsFromFirebase = async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.body?.limit ?? "50", 10) || 50, 500);
    const dryRun = Boolean(req.body?.dryRun);

    const firebasePath = process.env.FIREBASE_REPORTS_PATH || "reports_queue";

    const admin = getFirebaseAdmin();
    const ref = admin.database().ref(firebasePath);

    const snapshot = await ref.once("value");
    const all = snapshot.val() || {};

    const candidates = pickPendingFirebaseReports(all).slice(0, limit);

    const results = {
      firebasePath,
      dryRun,
      scanned: Object.keys(all).length,
      candidates: candidates.length,
      migrated: 0,
      denied: 0,
      alreadyExists: 0,
      errors: 0,
      items: [],
    };

    for (const [key, payload] of candidates) {
      const validation = validateReportPayload(payload);

      if (!validation.ok) {
        results.denied += 1;
        results.items.push({ key, status: "denied", reason: validation.message });

        if (!dryRun) {
          await ref.child(key).update({
            migrationStatus: "denied",
            migrationError: validation.message,
            migratedAt: nowIso(),
          });
        }

        continue;
      }

      if (dryRun) {
        results.migrated += 1;
        results.items.push({ key, status: "would_migrate" });
        continue;
      }

      try {
        const insertResult = await ingestReport(payload);

        results.migrated += 1;
        results.items.push({
          key,
          status: "migrated",
          reportId: insertResult.reportId,
          sqlReportDbId: insertResult.dbId,
        });

        await ref.child(key).update({
          migrationStatus: "migrated",
          migratedAt: nowIso(),
          sqlReportDbId: insertResult.dbId,
          processed: true,
        });
      } catch (error) {
        if (error.code === "ER_DUP_ENTRY") {
          results.alreadyExists += 1;
          results.items.push({ key, status: "already_exists" });

          await ref.child(key).update({
            migrationStatus: "migrated",
            migratedAt: nowIso(),
            migrationNote: "Already exists in SQL (duplicate report_id)",
            processed: true,
          });
        } else {
          results.errors += 1;
          results.items.push({ key, status: "error", error: error.message });

          await ref.child(key).update({
            migrationStatus: "denied",
            migratedAt: nowIso(),
            migrationError: error.message,
          });
        }
      }
    }

    res.json({ success: true, ...results });
  } catch (error) {
    console.error("Firebase sync error:", error);
    res.status(500).json({
      success: false,
      message: "Failed to sync reports from Firebase",
      error: error.message,
    });
  }
};
