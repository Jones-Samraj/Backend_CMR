const db = require("../config/db");
const reportIngestionService = require("../services/report_ingestion_service");

const DUE_DAYS_BY_SEVERITY = {
  High: 3,
  Medium: 5,
  Low: 7,
};

function normalizeSeverity(severity) {
  const s = String(severity || "").trim();
  if (s === "High" || s === "Medium" || s === "Low") return s;
  return "Low";
}

function isWeekendUtc(date) {
  const day = date.getUTCDay();
  return day === 0 || day === 6;
}

function addBusinessDaysUtc(startDate, businessDays) {
  const d = new Date(startDate);
  d.setUTCHours(0, 0, 0, 0);

  const daysToAdd = Number(businessDays);
  if (!Number.isFinite(daysToAdd) || daysToAdd <= 0) return d;

  let added = 0;
  while (added < daysToAdd) {
    d.setUTCDate(d.getUTCDate() + 1);
    if (!isWeekendUtc(d)) added += 1;
  }
  return d;
}

function toMySQLDate(date) {
  if (!date) return null;
  const d = new Date(date);
  if (Number.isNaN(d.getTime())) return null;
  // YYYY-MM-DD
  return d.toISOString().slice(0, 10);
}

// Helper function to convert ISO datetime to MySQL format
const toMySQLDatetime = (isoString) => {
  if (!isoString) return new Date().toISOString().slice(0, 19).replace('T', ' ');
  const date = new Date(isoString);
  return date.toISOString().slice(0, 19).replace('T', ' ');
};

/**
 * Submit a new road audit report from mobile app
 * Handles the JSON structure from pothole_user app
 */
exports.submitReport = async (req, res) => {
  const connection = await db.promise().getConnection();
  
  try {
    await connection.beginTransaction();

    const { report_id, device_id, reported_at, anomalies } = req.body;

    // Validate required fields
    if (!report_id || !device_id || !anomalies) {
      return res.status(400).json({ 
        message: "Missing required fields: report_id, device_id, anomalies" 
      });
    }

    // Get user_id from device_id if available
    let userId = null;
    if (req.user?.id) {
      userId = req.user.id;
    } else {
      const [users] = await connection.query(
        "SELECT id FROM users WHERE device_id = ?",
        [device_id]
      );
      if (users.length > 0) userId = users[0].id;
    }

    const ingestResult = await reportIngestionService.ingestReportWithinTransaction(
      connection,
      { report_id, device_id, reported_at, anomalies, userId }
    );

    await connection.commit();

    res.status(201).json({
      success: true,
      message: "Report submitted successfully",
      data: {
        ...ingestResult
      }
    });

  } catch (error) {
    await connection.rollback();
    console.error("Submit report error:", error);
    
    // Handle duplicate report
    if (error.code === "ER_DUP_ENTRY") {
      return res.status(409).json({ 
        message: "Report already submitted",
        error: "Duplicate report_id"
      });
    }
    
    res.status(500).json({ 
      message: "Failed to submit report", 
      error: error.message 
    });
  } finally {
    connection.release();
  }
};

/**
 * Get all reports with pagination
 */
exports.getAllReports = async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 20;
    const offset = (page - 1) * limit;
    const status = req.query.status;

    let query = `
      SELECT r.*, u.email as user_email 
      FROM reports r 
      LEFT JOIN users u ON r.user_id = u.id
    `;
    const params = [];

    if (status) {
      query += " WHERE r.status = ?";
      params.push(status);
    }

    query += " ORDER BY r.reported_at DESC LIMIT ? OFFSET ?";
    params.push(limit, offset);

    const [reports] = await db.promise().query(query, params);

    // Get total count
    let countQuery = "SELECT COUNT(*) as total FROM reports";
    if (status) {
      countQuery += " WHERE status = ?";
    }
    const [countResult] = await db.promise().query(countQuery, status ? [status] : []);

    res.json({
      reports,
      pagination: {
        page,
        limit,
        total: countResult[0].total,
        totalPages: Math.ceil(countResult[0].total / limit)
      }
    });
  } catch (error) {
    console.error("Get reports error:", error);
    res.status(500).json({ message: "Failed to get reports", error: error.message });
  }
};

/**
 * Get report by ID with all detections
 */
exports.getReportById = async (req, res) => {
  try {
    const { reportId } = req.params;

    // Get report
    const [reports] = await db.promise().query(
      `SELECT r.*, u.email as user_email 
       FROM reports r 
       LEFT JOIN users u ON r.user_id = u.id 
       WHERE r.report_id = ?`,
      [reportId]
    );

    if (reports.length === 0) {
      return res.status(404).json({ message: "Report not found" });
    }

    const report = reports[0];

    // Get pothole detections
    const [potholes] = await db.promise().query(
      "SELECT * FROM pothole_detections WHERE report_id = ?",
      [report.id]
    );

    // Get road anomalies
    const [anomalies] = await db.promise().query(
      "SELECT * FROM road_anomalies WHERE report_id = ?",
      [report.id]
    );

    res.json({
      ...report,
      potholes,
      roadAnomalies: anomalies
    });
  } catch (error) {
    console.error("Get report error:", error);
    res.status(500).json({ message: "Failed to get report", error: error.message });
  }
};

/**
 * Get reports by location (within radius in km)
 */
exports.getReportsByLocation = async (req, res) => {
  try {
    const { lat, lng, radius = 5 } = req.query;

    if (!lat || !lng) {
      return res.status(400).json({ message: "Latitude and longitude are required" });
    }

    // Haversine formula to find reports within radius
    const [potholes] = await db.promise().query(
      `SELECT pd.*, r.report_id, r.device_id, r.reported_at,
       (6371 * acos(cos(radians(?)) * cos(radians(latitude)) * cos(radians(longitude) - radians(?)) + sin(radians(?)) * sin(radians(latitude)))) AS distance
       FROM pothole_detections pd
       JOIN reports r ON pd.report_id = r.id
       HAVING distance < ?
       ORDER BY distance`,
      [lat, lng, lat, radius]
    );

    res.json({ potholes, count: potholes.length });
  } catch (error) {
    console.error("Get by location error:", error);
    res.status(500).json({ message: "Failed to get reports by location", error: error.message });
  }
};

/**
 * Get aggregated locations for map display
 */
exports.getAggregatedLocations = async (req, res) => {
  try {
    const { status, severity, minLat, maxLat, minLng, maxLng } = req.query;

    let query = `
      SELECT 
        al.*,
        wa.id as assignment_id,
        wa.contractor_id,
        wa.status as assignment_status,
        wa.due_date,
        wa.assigned_at,
        c.company_name as contractor_name,
        c.contact_email as contractor_email
      FROM aggregated_locations al
      LEFT JOIN work_assignments wa ON wa.aggregated_location_id = al.id 
        AND wa.status NOT IN ('completed', 'verified')
      LEFT JOIN contractors c ON wa.contractor_id = c.id
      WHERE 1=1
    `;
    const params = [];

    if (status) {
      query += " AND al.status = ?";
      params.push(status);
    }

    if (severity) {
      query += " AND al.highest_severity = ?";
      params.push(severity);
    }

    if (minLat && maxLat && minLng && maxLng) {
      query += " AND al.latitude BETWEEN ? AND ? AND al.longitude BETWEEN ? AND ?";
      params.push(minLat, maxLat, minLng, maxLng);
    }

    query += " ORDER BY al.report_count DESC LIMIT 500";

    const [locations] = await db.promise().query(query, params);

    res.json({ locations, count: locations.length });
  } catch (error) {
    console.error("Get aggregated locations error:", error);
    res.status(500).json({ message: "Failed to get locations", error: error.message });
  }
};

/**
 * Update report status
 */
exports.updateReportStatus = async (req, res) => {
  try {
    const { reportId } = req.params;
    const { status } = req.body;

    const validStatuses = ["pending", "reviewed", "assigned", "in_progress", "resolved"];
    if (!validStatuses.includes(status)) {
      return res.status(400).json({ message: "Invalid status" });
    }

    const [result] = await db.promise().query(
      "UPDATE reports SET status = ? WHERE report_id = ?",
      [status, reportId]
    );

    if (result.affectedRows === 0) {
      return res.status(404).json({ message: "Report not found" });
    }

    res.json({ message: "Status updated successfully", status });
  } catch (error) {
    console.error("Update status error:", error);
    res.status(500).json({ message: "Failed to update status", error: error.message });
  }
};

/**
 * Get dashboard statistics
 */
exports.getStatistics = async (req, res) => {
  try {
    const [totalReports] = await db.promise().query(
      "SELECT COUNT(*) as count FROM reports"
    );

    const [totalPotholes] = await db.promise().query(
      "SELECT COUNT(*) as count FROM pothole_detections"
    );

    const [totalAnomalies] = await db.promise().query(
      "SELECT COUNT(*) as count FROM road_anomalies"
    );

    const [byStatus] = await db.promise().query(
      "SELECT status, COUNT(*) as count FROM reports GROUP BY status"
    );

    const [bySeverity] = await db.promise().query(
      "SELECT severity, COUNT(*) as count FROM pothole_detections GROUP BY severity"
    );

    const [recentReports] = await db.promise().query(
      `SELECT report_id, device_id, total_potholes, total_patchy_roads, health_score, status, reported_at 
       FROM reports 
       ORDER BY reported_at DESC 
       LIMIT 10`
    );

    const [hotspots] = await db.promise().query(
      `SELECT * FROM aggregated_locations 
       WHERE highest_severity = 'High' 
       ORDER BY report_count DESC 
       LIMIT 10`
    );

    res.json({
      overview: {
        totalReports: totalReports[0].count,
        totalPotholes: totalPotholes[0].count,
        totalAnomalies: totalAnomalies[0].count
      },
      byStatus: byStatus.reduce((acc, curr) => ({ ...acc, [curr.status]: curr.count }), {}),
      bySeverity: bySeverity.reduce((acc, curr) => ({ ...acc, [curr.severity]: curr.count }), {}),
      recentReports,
      hotspots
    });
  } catch (error) {
    console.error("Get statistics error:", error);
    res.status(500).json({ message: "Failed to get statistics", error: error.message });
  }
};

/**
 * Get all active contractors (public endpoint for dropdown)
 */
exports.getContractorsList = async (req, res) => {
  try {
    const [contractors] = await db.promise().query(
      `SELECT id, company_name, contact_email 
       FROM contractors 
       WHERE is_active = TRUE 
       ORDER BY company_name`
    );

    res.json({ 
      contractors: contractors.map(c => ({
        id: String(c.id),
        name: c.company_name || c.contact_email,
        company: c.company_name || 'Contractor'
      }))
    });
  } catch (error) {
    console.error("Get contractors list error:", error);
    res.status(500).json({ message: "Failed to get contractors", error: error.message });
  }
};

/**
 * Get all work assignments (public endpoint for dashboard)
 */
exports.getWorkAssignments = async (req, res) => {
  try {
    const { status, contractorId, locationId } = req.query;

    let query = `
      SELECT 
        wa.*,
        al.grid_id,
        al.latitude,
        al.longitude,
        al.total_potholes,
        al.highest_severity,
        al.status as location_status,
        c.company_name as contractor_name,
        c.contact_email as contractor_email
      FROM work_assignments wa
      LEFT JOIN aggregated_locations al ON wa.aggregated_location_id = al.id
      LEFT JOIN contractors c ON wa.contractor_id = c.id
      WHERE 1=1
    `;
    const params = [];

    if (status) {
      query += " AND wa.status = ?";
      params.push(status);
    }

    if (contractorId) {
      query += " AND wa.contractor_id = ?";
      params.push(contractorId);
    }

    if (locationId) {
      query += " AND wa.aggregated_location_id = ?";
      params.push(locationId);
    }

    query += " ORDER BY wa.assigned_at DESC";

    const [assignments] = await db.promise().query(query, params);

    res.json({ assignments, count: assignments.length });
  } catch (error) {
    console.error("Get work assignments error:", error);
    res.status(500).json({ message: "Failed to get work assignments", error: error.message });
  }
};

/**
 * Create work assignment (public endpoint for demo - without strict auth)
 */
exports.createWorkAssignment = async (req, res) => {
  try {
    const { locationId, contractorId, notes } = req.body;

    if (!locationId || !contractorId) {
      return res.status(400).json({ message: "locationId and contractorId are required" });
    }

    // Validate location exists + fetch severity
    const [locations] = await db.promise().query(
      "SELECT id, highest_severity FROM aggregated_locations WHERE id = ?",
      [locationId]
    );

    if (locations.length === 0) {
      return res.status(404).json({ message: "Location not found" });
    }

    // Validate contractor exists
    const [contractors] = await db.promise().query(
      "SELECT id FROM contractors WHERE id = ? AND is_active = TRUE",
      [contractorId]
    );

    if (contractors.length === 0) {
      return res.status(404).json({ message: "Contractor not found or inactive" });
    }

    const severity = normalizeSeverity(locations[0]?.highest_severity);
    const dueDays = DUE_DAYS_BY_SEVERITY[severity] ?? DUE_DAYS_BY_SEVERITY.Low;
    const dueDate = toMySQLDate(addBusinessDaysUtc(new Date(), dueDays));

    // Check if assignment already exists
    const [existing] = await db.promise().query(
      "SELECT id FROM work_assignments WHERE aggregated_location_id = ? AND status NOT IN ('completed', 'verified')",
      [locationId]
    );

    if (existing.length > 0) {
      // Update existing assignment
      await db.promise().query(
        `UPDATE work_assignments 
         SET contractor_id = ?, assigned_at = NOW(), due_date = ?, notes = ?, status = 'assigned', completed_at = NULL
         WHERE id = ?`,
        [contractorId, dueDate || null, notes || null, existing[0].id]
      );

      // Update location status
      await db.promise().query(
        "UPDATE aggregated_locations SET status = 'assigned' WHERE id = ?",
        [locationId]
      );

      return res.json({
        message: "Assignment updated successfully",
        assignmentId: existing[0].id,
        dueDate,
        severity
      });
    }

    // Create new assignment
    const [result] = await db.promise().query(
      `INSERT INTO work_assignments (aggregated_location_id, contractor_id, due_date, notes)
       VALUES (?, ?, ?, ?)`,
      [locationId, contractorId, dueDate || null, notes || null]
    );

    // Update location status
    await db.promise().query(
      "UPDATE aggregated_locations SET status = 'assigned' WHERE id = ?",
      [locationId]
    );

    res.status(201).json({
      message: "Assignment created successfully",
      assignmentId: result.insertId,
      dueDate,
      severity
    });
  } catch (error) {
    console.error("Create work assignment error:", error);
    res.status(500).json({ message: "Failed to create assignment", error: error.message });
  }
};

/**
 * Update work assignment status (public endpoint for demo)
 */
exports.updateWorkAssignment = async (req, res) => {
  try {
    const { assignmentId } = req.params;
    const { status, notes } = req.body;

    const validStatuses = ['assigned', 'in_progress', 'pending_verification', 'completed', 'verified'];
    if (status && !validStatuses.includes(status)) {
      return res.status(400).json({ message: "Invalid status" });
    }

    const updates = [];
    const params = [];

    if (status) {
      updates.push("status = ?");
      params.push(status);
      
      if (status === 'completed' || status === 'verified') {
        updates.push("completed_at = NOW()");
      }
    }

    if (notes !== undefined) {
      updates.push("notes = ?");
      params.push(notes);
    }

    if (updates.length === 0) {
      return res.status(400).json({ message: "No updates provided" });
    }

    params.push(assignmentId);

    const [result] = await db.promise().query(
      `UPDATE work_assignments SET ${updates.join(", ")} WHERE id = ?`,
      params
    );

    if (result.affectedRows === 0) {
      return res.status(404).json({ message: "Assignment not found" });
    }

    // If status is being updated, also update the location status
    if (status) {
      const [assignment] = await db.promise().query(
        "SELECT aggregated_location_id FROM work_assignments WHERE id = ?",
        [assignmentId]
      );

      if (assignment.length > 0) {
        let locationStatus = status;
        if (status === 'completed') locationStatus = 'pending_verification';
        if (status === 'verified') locationStatus = 'verified';
        
        await db.promise().query(
          "UPDATE aggregated_locations SET status = ? WHERE id = ?",
          [locationStatus, assignment[0].aggregated_location_id]
        );
      }
    }

    res.json({ message: "Assignment updated successfully" });
  } catch (error) {
    console.error("Update work assignment error:", error);
    res.status(500).json({ message: "Failed to update assignment", error: error.message });
  }
};

/**
 * Get verified/repair history (public endpoint)
 */
exports.getVerifiedHistory = async (req, res) => {
  try {
    const { startDate, endDate } = req.query;

    let whereClause = "WHERE al.status = 'verified'";
    const params = [];

    if (startDate) {
      whereClause += " AND al.verified_at >= ?";
      const d = new Date(startDate);
      params.push(d.toISOString().slice(0, 19).replace('T', ' '));
    }

    if (endDate) {
      whereClause += " AND al.verified_at <= ?";
      const d = new Date(endDate);
      params.push(d.toISOString().slice(0, 19).replace('T', ' '));
    }

    const [history] = await db.promise().query(
      `SELECT 
        al.id,
        al.latitude,
        al.longitude,
        al.road_name,
        al.report_count,
        al.highest_severity,
        al.status,
        al.verified_at,
        wa.contractor_id,
        wa.completed_at,
        wa.assigned_at,
        c.company_name as contractor_name,
        c.contact_email as contractor_email
       FROM aggregated_locations al
       LEFT JOIN work_assignments wa ON wa.aggregated_location_id = al.id
       LEFT JOIN contractors c ON wa.contractor_id = c.id
       ${whereClause}
       ORDER BY al.verified_at DESC`,
      params
    );

    res.json({ history });
  } catch (error) {
    console.error("Get verified history error:", error);
    res.status(500).json({ message: "Failed to get history", error: error.message });
  }
};

/**
 * Verify work completion (public endpoint for dashboard)
 * Updates both aggregated_locations and work_assignments to 'verified'
 */
exports.verifyLocation = async (req, res) => {
  try {
    const { locationId } = req.params;
    const { notes } = req.body;

    // Update location status
    const [locationResult] = await db.promise().query(
      "UPDATE aggregated_locations SET status = 'verified', verified_at = NOW() WHERE id = ?",
      [locationId]
    );

    if (locationResult.affectedRows === 0) {
      return res.status(404).json({ message: "Location not found" });
    }

    // Update assignment status (match pending_verification, in_progress, or assigned)
    const [assignmentResult] = await db.promise().query(
      `UPDATE work_assignments 
       SET status = 'verified', completed_at = NOW(), admin_notes = ?
       WHERE aggregated_location_id = ? AND status IN ('pending_verification', 'in_progress', 'assigned')`,
      [notes || 'Verified from dashboard', locationId]
    );

    console.log(`Verified location ${locationId}: location updated=${locationResult.affectedRows}, assignment updated=${assignmentResult.affectedRows}`);

    res.json({ 
      message: "Work verified successfully",
      locationUpdated: locationResult.affectedRows > 0,
      assignmentUpdated: assignmentResult.affectedRows > 0
    });
  } catch (error) {
    console.error("Verify location error:", error);
    res.status(500).json({ message: "Failed to verify work", error: error.message });
  }
};

/**
 * Batch verify multiple locations (public endpoint)
 */
exports.batchVerifyLocations = async (req, res) => {
  const connection = await db.promise().getConnection();
  
  try {
    await connection.beginTransaction();
    
    const { locationIds, notes } = req.body;

    if (!locationIds || !locationIds.length) {
      return res.status(400).json({ message: "locationIds are required" });
    }

    let locationsUpdated = 0;
    let assignmentsUpdated = 0;

    for (const locationId of locationIds) {
      const [locResult] = await connection.query(
        "UPDATE aggregated_locations SET status = 'verified', verified_at = NOW() WHERE id = ?",
        [locationId]
      );
      locationsUpdated += locResult.affectedRows;

      const [assignResult] = await connection.query(
        `UPDATE work_assignments 
         SET status = 'verified', completed_at = NOW(), admin_notes = ?
         WHERE aggregated_location_id = ? AND status IN ('pending_verification', 'in_progress', 'assigned')`,
        [notes || 'Batch verified from dashboard', locationId]
      );
      assignmentsUpdated += assignResult.affectedRows;
    }

    await connection.commit();

    console.log(`Batch verified ${locationIds.length} locations: locations updated=${locationsUpdated}, assignments updated=${assignmentsUpdated}`);

    res.json({ 
      message: `${locationIds.length} locations verified successfully`,
      locationsUpdated,
      assignmentsUpdated
    });
  } catch (error) {
    await connection.rollback();
    console.error("Batch verify error:", error);
    res.status(500).json({ message: "Failed to batch verify", error: error.message });
  } finally {
    connection.release();
  }
};
