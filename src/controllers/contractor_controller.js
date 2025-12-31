const db = require("../config/db");

// Get contractor's assigned jobs
exports.jobs = async (req, res) => {
  try {
    // Get contractor ID from user
    const [contractors] = await db.promise().query(
      "SELECT id FROM contractors WHERE user_id = ?",
      [req.user.id]
    );

    if (contractors.length === 0) {
      return res.status(404).json({ message: "Contractor profile not found" });
    }

    const contractorId = contractors[0].id;

    // Get assignments
    const [jobs] = await db.promise().query(
      `SELECT wa.*, al.grid_id, al.latitude, al.longitude, al.total_potholes, al.total_patchy, al.highest_severity
       FROM work_assignments wa
       JOIN aggregated_locations al ON wa.aggregated_location_id = al.id
       WHERE wa.contractor_id = ?
       ORDER BY wa.assigned_at DESC`,
      [contractorId]
    );

    res.json({ jobs });
  } catch (error) {
    console.error("Get jobs error:", error);
    res.status(500).json({ message: "Failed to get jobs", error: error.message });
  }
};

// Update job status
exports.updateJobStatus = async (req, res) => {
  try {
    const { jobId } = req.params;
    const { status, notes } = req.body;

    const validStatuses = ["assigned", "in_progress", "completed", "verified"];
    if (!validStatuses.includes(status)) {
      return res.status(400).json({ message: "Invalid status" });
    }

    // Verify contractor owns this job
    const [contractors] = await db.promise().query(
      "SELECT id FROM contractors WHERE user_id = ?",
      [req.user.id]
    );

    if (contractors.length === 0) {
      return res.status(404).json({ message: "Contractor profile not found" });
    }

    const [jobs] = await db.promise().query(
      "SELECT * FROM work_assignments WHERE id = ? AND contractor_id = ?",
      [jobId, contractors[0].id]
    );

    if (jobs.length === 0) {
      return res.status(404).json({ message: "Job not found" });
    }

    // Update job
    const completionDate = status === "completed" ? new Date() : null;

    await db.promise().query(
      `UPDATE work_assignments 
       SET status = ?, notes = CONCAT(COALESCE(notes, ''), '\n', ?), completion_date = ?
       WHERE id = ?`,
      [status, notes || "", completionDate, jobId]
    );

    // If completed, update aggregated location
    if (status === "completed") {
      await db.promise().query(
        "UPDATE aggregated_locations SET status = 'fixed' WHERE id = ?",
        [jobs[0].aggregated_location_id]
      );
    }

    res.json({ success: true, message: "Job status updated successfully" });
  } catch (error) {
    console.error("Update job error:", error);
    res.status(500).json({ message: "Failed to update job", error: error.message });
  }
};

// Upload and persist pre/post work photo URL
exports.uploadJobPhoto = async (req, res) => {
  try {
    const { jobId } = req.params;
    const photoType = String(req.body?.photoType || '').toLowerCase();

    if (!req.file) {
      return res.status(400).json({ message: 'No photo uploaded' });
    }

    const normalizedType = photoType === 'post' ? 'post' : 'pre';
    const column = normalizedType === 'post' ? 'post_work_photo_url' : 'pre_work_photo_url';

    // Verify contractor owns this job
    const [contractors] = await db.promise().query(
      'SELECT id FROM contractors WHERE user_id = ?',
      [req.user.id]
    );

    if (contractors.length === 0) {
      return res.status(404).json({ message: 'Contractor profile not found' });
    }

    const [jobs] = await db.promise().query(
      'SELECT * FROM work_assignments WHERE id = ? AND contractor_id = ?',
      [jobId, contractors[0].id]
    );

    if (jobs.length === 0) {
      return res.status(404).json({ message: 'Job not found' });
    }

    // Build an absolute URL for the uploaded file
    const host = req.get('host');
    const protocol = req.protocol;
    const fileUrl = `${protocol}://${host}/uploads/${req.file.filename}`;

    await db.promise().query(
      `UPDATE work_assignments SET ${column} = ? WHERE id = ?`,
      [fileUrl, jobId]
    );

    res.json({ success: true, photoType: normalizedType, photoUrl: fileUrl });
  } catch (error) {
    console.error('Upload job photo error:', error);
    res.status(500).json({ message: 'Failed to upload photo', error: error.message });
  }
};

// Reject/delete a job assignment
exports.rejectJob = async (req, res) => {
  try {
    const { jobId } = req.params;
    const { reason } = req.body;

    // Verify contractor owns this job
    const [contractors] = await db.promise().query(
      "SELECT id FROM contractors WHERE user_id = ?",
      [req.user.id]
    );

    if (contractors.length === 0) {
      return res.status(404).json({ message: "Contractor profile not found" });
    }

    const [jobs] = await db.promise().query(
      "SELECT * FROM work_assignments WHERE id = ? AND contractor_id = ?",
      [jobId, contractors[0].id]
    );

    if (jobs.length === 0) {
      return res.status(404).json({ message: "Job not found" });
    }

    // Update aggregated_location status back to pending
    await db.promise().query(
      "UPDATE aggregated_locations SET status = 'pending' WHERE id = ?",
      [jobs[0].aggregated_location_id]
    );

    // Delete the assignment
    await db.promise().query(
      "DELETE FROM work_assignments WHERE id = ?",
      [jobId]
    );

    console.log(`Job ${jobId} rejected by contractor. Reason: ${reason || 'No reason provided'}`);

    res.json({ message: "Job rejected successfully" });
  } catch (error) {
    console.error("Reject job error:", error);
    res.status(500).json({ message: "Failed to reject job", error: error.message });
  }
};

// Get contractor profile
exports.getProfile = async (req, res) => {
  try {
    const [contractors] = await db.promise().query(
      `SELECT c.*, u.email 
       FROM contractors c 
       JOIN users u ON c.user_id = u.id
       WHERE c.user_id = ?`,
      [req.user.id]
    );

    if (contractors.length === 0) {
      return res.status(404).json({ message: "Contractor profile not found" });
    }

    res.json({ contractor: contractors[0] });
  } catch (error) {
    console.error("Get profile error:", error);
    res.status(500).json({ message: "Failed to get profile", error: error.message });
  }
};

// Get job statistics
exports.getStats = async (req, res) => {
  try {
    const [contractors] = await db.promise().query(
      "SELECT id FROM contractors WHERE user_id = ?",
      [req.user.id]
    );

    if (contractors.length === 0) {
      return res.status(404).json({ message: "Contractor profile not found" });
    }

    const contractorId = contractors[0].id;

    const [stats] = await db.promise().query(
      `SELECT 
        COUNT(*) as total_jobs,
        SUM(CASE WHEN status = 'assigned' THEN 1 ELSE 0 END) as assigned,
        SUM(CASE WHEN status = 'in_progress' THEN 1 ELSE 0 END) as in_progress,
        SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
        SUM(CASE WHEN status = 'verified' THEN 1 ELSE 0 END) as verified
       FROM work_assignments 
       WHERE contractor_id = ?`,
      [contractorId]
    );

    res.json({ stats: stats[0] });
  } catch (error) {
    console.error("Get stats error:", error);
    res.status(500).json({ message: "Failed to get stats", error: error.message });
  }
};
