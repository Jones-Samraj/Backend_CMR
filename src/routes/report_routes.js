const router = require("express").Router();
const reportController = require("../controllers/report_controller");
const auth = require("../middlewares/authMiddleware");
const optionalAuth = require("../middlewares/optionalAuthMiddleware");

// Submit a new report (from mobile app) - optional auth for anonymous reports
router.post("/submit", optionalAuth, reportController.submitReport);

// Get all reports (admin)
router.get("/", auth, reportController.getAllReports);

// Get report by ID
router.get("/:reportId", auth, reportController.getReportById);

// Get reports by location (within radius)
router.get("/location/nearby", reportController.getReportsByLocation);

// Get aggregated locations (for map display)
router.get("/aggregated/locations", reportController.getAggregatedLocations);

// Get contractors list (public - for dropdown)
router.get("/contractors/list", reportController.getContractorsList);

// Work assignments (public - for dashboard)
router.get("/assignments", reportController.getWorkAssignments);
router.post("/assignments", reportController.createWorkAssignment);
router.patch("/assignments/:assignmentId", reportController.updateWorkAssignment);

// Verified history (public - for history page)
router.get("/history/verified", reportController.getVerifiedHistory);

// Verify location (public - for dashboard without auth)
// IMPORTANT: batch route must come BEFORE :locationId route
router.post("/verify/batch", reportController.batchVerifyLocations);
router.post("/verify/:locationId", reportController.verifyLocation);

// Update report status (admin)
router.patch("/:reportId/status", auth, reportController.updateReportStatus);

// Get statistics
router.get("/stats/overview", reportController.getStatistics);

module.exports = router;
