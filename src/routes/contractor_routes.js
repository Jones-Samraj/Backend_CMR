const router = require("express").Router();
const auth = require("../middlewares/authMiddleware");
const role = require("../middlewares/roleMiddleware");
const controller = require("../controllers/contractor_controller");

// Get assigned jobs
router.get("/jobs", auth, role("contractor"), controller.jobs);

// Update job status
router.patch("/jobs/:jobId/status", auth, role("contractor"), controller.updateJobStatus);

// Reject/delete a job assignment
router.delete("/jobs/:jobId", auth, role("contractor"), controller.rejectJob);

// Contractor profile
router.get("/profile", auth, role("contractor"), controller.getProfile);

// Job statistics
router.get("/stats", auth, role("contractor"), controller.getStats);

module.exports = router;
