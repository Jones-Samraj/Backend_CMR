const router = require("express").Router();
const auth = require("../middlewares/authMiddleware");
const role = require("../middlewares/roleMiddleware");
const controller = require("../controllers/contractor_controller");
const multer = require("multer");
const path = require("path");
const fs = require("fs");

const uploadDir = path.join(__dirname, "..", "..", "uploads");
try {
	if (!fs.existsSync(uploadDir)) fs.mkdirSync(uploadDir, { recursive: true });
} catch (_) {}

const storage = multer.diskStorage({
	destination: (req, file, cb) => cb(null, uploadDir),
	filename: (req, file, cb) => {
		const photoType = String(req.body?.photoType || "").toLowerCase();
		const safeType = photoType === "post" ? "post" : "pre";
		const jobId = req.params?.jobId || "job";
		const ext = path.extname(file.originalname || "") || ".jpg";
		cb(null, `${safeType}_work_${jobId}_${Date.now()}${ext}`);
	},
});

const upload = multer({ storage });

// Get assigned jobs
router.get("/jobs", auth, role("contractor"), controller.jobs);

// Update job status
router.patch("/jobs/:jobId/status", auth, role("contractor"), controller.updateJobStatus);

// Upload pre/post work photo
router.post(
	"/jobs/:jobId/photo",
	auth,
	role("contractor"),
	upload.single("photo"),
	controller.uploadJobPhoto
);

// Reject/delete a job assignment
router.delete("/jobs/:jobId", auth, role("contractor"), controller.rejectJob);

// Contractor profile
router.get("/profile", auth, role("contractor"), controller.getProfile);

// Job statistics
router.get("/stats", auth, role("contractor"), controller.getStats);

module.exports = router;
