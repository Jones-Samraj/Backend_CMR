const db = require("../config/db");
const { toPublicPhotoUrl } = require('../utils/photoUrl');

// Helper function to convert ISO datetime to MySQL format
const toMySQLDatetime = (isoString) => {
  if (!isoString) return new Date().toISOString().slice(0, 19).replace('T', ' ');
  const date = new Date(isoString);
  return date.toISOString().slice(0, 19).replace('T', ' ');
};

// Admin dashboard data
exports.dashboard = async (req, res) => {
  try {
    // Get overview statistics
    const [totalReports] = await db.promise().query(
      "SELECT COUNT(*) as count FROM reports"
    );

    const [pendingReports] = await db.promise().query(
      "SELECT COUNT(*) as count FROM reports WHERE status = 'pending'"
    );

    const [totalPotholes] = await db.promise().query(
      "SELECT COUNT(*) as count FROM pothole_detections"
    );

    const [totalUsers] = await db.promise().query(
      "SELECT COUNT(*) as count FROM users WHERE role = 'user'"
    );

    const [highSeverity] = await db.promise().query(
      "SELECT COUNT(*) as count FROM aggregated_locations WHERE highest_severity = 'High' AND status = 'pending'"
    );

    // Recent reports
    const [recentReports] = await db.promise().query(
      `SELECT r.*, u.email 
       FROM reports r 
       LEFT JOIN users u ON r.user_id = u.id 
       ORDER BY r.reported_at DESC 
       LIMIT 10`
    );

    // Hotspots (high severity areas with most reports)
    const [hotspots] = await db.promise().query(
      `SELECT * FROM aggregated_locations 
       WHERE status = 'pending' 
       ORDER BY report_count DESC, 
                FIELD(highest_severity, 'High', 'Medium', 'Low')
       LIMIT 20`
    );

    res.json({
      overview: {
        totalReports: totalReports[0].count,
        pendingReports: pendingReports[0].count,
        totalPotholes: totalPotholes[0].count,
        totalUsers: totalUsers[0].count,
        highSeverityAreas: highSeverity[0].count
      },
      recentReports,
      hotspots
    });
  } catch (error) {
    console.error("Dashboard error:", error);
    res.status(500).json({ message: "Failed to get dashboard data", error: error.message });
  }
};

// Get all users
exports.getAllUsers = async (req, res) => {
  try {
    const [users] = await db.promise().query(
      `SELECT id, email, role, device_id, device_model, created_at 
       FROM users ORDER BY created_at DESC`
    );

    res.json({ users });
  } catch (error) {
    console.error("Get users error:", error);
    res.status(500).json({ message: "Failed to get users", error: error.message });
  }
};

// Assign location to contractor
exports.assignToContractor = async (req, res) => {
  try {
    const { locationId, contractorId, dueDate, notes } = req.body;

    // Validate location exists
    const [locations] = await db.promise().query(
      "SELECT id FROM aggregated_locations WHERE id = ?",
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

    // Create assignment
    const [result] = await db.promise().query(
      `INSERT INTO work_assignments (aggregated_location_id, contractor_id, assigned_by, due_date, notes)
       VALUES (?, ?, ?, ?, ?)`,
      [locationId, contractorId, req.user.id, dueDate, notes]
    );

    // Update location status
    await db.promise().query(
      "UPDATE aggregated_locations SET status = 'assigned' WHERE id = ?",
      [locationId]
    );

    res.status(201).json({
      message: "Assignment created successfully",
      assignmentId: result.insertId
    });
  } catch (error) {
    console.error("Assign error:", error);
    res.status(500).json({ message: "Failed to create assignment", error: error.message });
  }
};

// Get all contractors
exports.getContractors = async (req, res) => {
  try {
    const [contractors] = await db.promise().query(
      `SELECT c.*, u.email 
       FROM contractors c 
       LEFT JOIN users u ON c.user_id = u.id
       ORDER BY c.company_name`
    );

    res.json({ contractors });
  } catch (error) {
    console.error("Get contractors error:", error);
    res.status(500).json({ message: "Failed to get contractors", error: error.message });
  }
};

// Create contractor
exports.createContractor = async (req, res) => {
  try {
    const { companyName, contactEmail, contactPhone, serviceAreaLat, serviceAreaLng, serviceRadiusKm } = req.body;

    const [result] = await db.promise().query(
      `INSERT INTO contractors (company_name, contact_email, contact_phone, service_area_lat, service_area_lng, service_radius_km)
       VALUES (?, ?, ?, ?, ?, ?)`,
      [companyName, contactEmail, contactPhone, serviceAreaLat, serviceAreaLng, serviceRadiusKm]
    );

    res.status(201).json({
      message: "Contractor created successfully",
      contractorId: result.insertId
    });
  } catch (error) {
    console.error("Create contractor error:", error);
    res.status(500).json({ message: "Failed to create contractor", error: error.message });
  }
};

// Get all reports with filters (for admin dashboard)
exports.getAllReports = async (req, res) => {
  try {
    const { status, severity, page = 1, limit = 50 } = req.query;
    const offset = (page - 1) * limit;

    let whereClause = "WHERE 1=1";
    const params = [];

    if (status) {
      whereClause += " AND r.status = ?";
      params.push(status);
    }

    const [reports] = await db.promise().query(
      `SELECT r.*, 
              u.email as user_email,
              (SELECT COUNT(*) FROM pothole_detections WHERE report_id = r.id) as pothole_count,
              (SELECT COUNT(*) FROM road_anomalies WHERE report_id = r.id) as anomaly_count
       FROM reports r 
       LEFT JOIN users u ON r.user_id = u.id 
       ${whereClause}
       ORDER BY r.reported_at DESC
       LIMIT ? OFFSET ?`,
      [...params, parseInt(limit), parseInt(offset)]
    );

    const [total] = await db.promise().query(
      `SELECT COUNT(*) as count FROM reports r ${whereClause}`,
      params
    );

    res.json({
      reports,
      pagination: {
        page: parseInt(page),
        limit: parseInt(limit),
        total: total[0].count,
        totalPages: Math.ceil(total[0].count / limit)
      }
    });
  } catch (error) {
    console.error("Get all reports error:", error);
    res.status(500).json({ message: "Failed to get reports", error: error.message });
  }
};

// Get potholes grouped by road/grid
exports.getPotholesGrouped = async (req, res) => {
  try {
    const [locations] = await db.promise().query(
      `SELECT 
        al.*,
        (SELECT GROUP_CONCAT(pd.id) FROM pothole_detections pd 
         WHERE CONCAT(ROUND(pd.latitude, 4), '_', ROUND(pd.longitude, 4)) = al.grid_id) as pothole_ids,
        wa.contractor_id,
        c.company_name as contractor_name,
        c.contact_email as contractor_email
       FROM aggregated_locations al
       LEFT JOIN work_assignments wa ON wa.aggregated_location_id = al.id AND wa.status != 'completed'
       LEFT JOIN contractors c ON wa.contractor_id = c.id
       ORDER BY al.last_reported_at DESC`
    );

    res.json({ locations });
  } catch (error) {
    console.error("Get grouped potholes error:", error);
    res.status(500).json({ message: "Failed to get grouped potholes", error: error.message });
  }
};

// Get all pothole detections
exports.getPotholeDetections = async (req, res) => {
  try {
    const { status, severity } = req.query;

    let whereClause = "WHERE 1=1";
    const params = [];

    if (severity) {
      whereClause += " AND pd.severity = ?";
      params.push(severity);
    }

    const [potholes] = await db.promise().query(
      `SELECT pd.*, r.status as report_status, r.report_id as report_code
       FROM pothole_detections pd
       LEFT JOIN reports r ON pd.report_id = r.id
       ${whereClause}
       ORDER BY pd.timestamp DESC`,
      params
    );

    res.json({ potholes });
  } catch (error) {
    console.error("Get potholes error:", error);
    res.status(500).json({ message: "Failed to get potholes", error: error.message });
  }
};

// Batch assign multiple locations to contractor
exports.batchAssign = async (req, res) => {
  const connection = await db.promise().getConnection();
  
  try {
    await connection.beginTransaction();
    
    const { locationIds, contractorId, dueDate, notes } = req.body;

    if (!locationIds || !locationIds.length || !contractorId) {
      return res.status(400).json({ message: "locationIds and contractorId are required" });
    }

    // Validate contractor exists
    const [contractors] = await connection.query(
      "SELECT id FROM contractors WHERE id = ? AND is_active = TRUE",
      [contractorId]
    );

    if (contractors.length === 0) {
      return res.status(404).json({ message: "Contractor not found or inactive" });
    }

    const assignmentIds = [];
    
    for (const locationId of locationIds) {
      const [result] = await connection.query(
        `INSERT INTO work_assignments (aggregated_location_id, contractor_id, assigned_by, due_date, notes)
         VALUES (?, ?, ?, ?, ?)`,
        [locationId, contractorId, req.user.id, dueDate ? toMySQLDatetime(dueDate) : null, notes]
      );
      assignmentIds.push(result.insertId);

      // Update location status
      await connection.query(
        "UPDATE aggregated_locations SET status = 'assigned' WHERE id = ?",
        [locationId]
      );
    }

    await connection.commit();

    res.status(201).json({
      message: `${locationIds.length} locations assigned successfully`,
      assignmentIds
    });
  } catch (error) {
    await connection.rollback();
    console.error("Batch assign error:", error);
    res.status(500).json({ message: "Failed to batch assign", error: error.message });
  } finally {
    connection.release();
  }
};

// Verify completed work
exports.verifyWork = async (req, res) => {
  try {
    const { locationId } = req.params;
    const { notes, rating } = req.body;

    // Update location status
    await db.promise().query(
      "UPDATE aggregated_locations SET status = 'verified', verified_at = NOW() WHERE id = ?",
      [locationId]
    );

    // Update assignment status (match pending_verification, in_progress, or assigned)
    await db.promise().query(
      `UPDATE work_assignments 
       SET status = 'verified', completed_at = NOW(), admin_notes = ?
       WHERE aggregated_location_id = ? AND status IN ('pending_verification', 'in_progress', 'assigned')`,
      [notes, locationId]
    );

    res.json({ message: "Work verified successfully" });
  } catch (error) {
    console.error("Verify work error:", error);
    res.status(500).json({ message: "Failed to verify work", error: error.message });
  }
};

// Batch verify multiple locations
exports.batchVerify = async (req, res) => {
  const connection = await db.promise().getConnection();
  
  try {
    await connection.beginTransaction();
    
    const { locationIds, notes } = req.body;

    if (!locationIds || !locationIds.length) {
      return res.status(400).json({ message: "locationIds are required" });
    }

    for (const locationId of locationIds) {
      await connection.query(
        "UPDATE aggregated_locations SET status = 'verified', verified_at = NOW() WHERE id = ?",
        [locationId]
      );

      await connection.query(
        `UPDATE work_assignments 
         SET status = 'verified', completed_at = NOW(), admin_notes = ?
         WHERE aggregated_location_id = ? AND status IN ('pending_verification', 'in_progress', 'assigned')`,
        [notes, locationId]
      );
    }

    await connection.commit();

    res.json({ message: `${locationIds.length} locations verified successfully` });
  } catch (error) {
    await connection.rollback();
    console.error("Batch verify error:", error);
    res.status(500).json({ message: "Failed to batch verify", error: error.message });
  } finally {
    connection.release();
  }
};

// Reject verification
exports.rejectVerification = async (req, res) => {
  try {
    const { locationId } = req.params;
    const { reason, remarks } = req.body;

    const remarkText = (remarks ?? reason ?? '').toString();

    // Update assignment status back to in_progress
    const [assignmentResult] = await db.promise().query(
      `UPDATE work_assignments 
       SET status = 'in_progress', completed_at = NULL, remarks = ?
       WHERE aggregated_location_id = ? AND status = 'pending_verification'`,
      [remarkText || null, locationId]
    );

    if (assignmentResult.affectedRows === 0) {
      return res.status(404).json({ message: 'No pending verification assignment found for this location' });
    }

    // Update location status
    await db.promise().query(
      "UPDATE aggregated_locations SET status = 'in_progress' WHERE id = ?",
      [locationId]
    );

    res.json({ message: "Verification rejected, sent back to contractor" });
  } catch (error) {
    console.error("Reject verification error:", error);
    res.status(500).json({ message: "Failed to reject verification", error: error.message });
  }
};

// Get all assignments
exports.getAssignments = async (req, res) => {
  try {
    const { status, contractorId } = req.query;

    let whereClause = "WHERE 1=1";
    const params = [];

    if (status) {
      whereClause += " AND wa.status = ?";
      params.push(status);
    }

    if (contractorId) {
      whereClause += " AND wa.contractor_id = ?";
      params.push(contractorId);
    }

    const [assignments] = await db.promise().query(
      `SELECT wa.*, 
              al.grid_id, al.latitude, al.longitude, al.total_potholes, al.highest_severity,
              c.company_name as contractor_name,
              u.email as assigned_by_email
       FROM work_assignments wa
       LEFT JOIN aggregated_locations al ON wa.aggregated_location_id = al.id
       LEFT JOIN contractors c ON wa.contractor_id = c.id
       LEFT JOIN users u ON wa.assigned_by = u.id
       ${whereClause}
       ORDER BY wa.created_at DESC`,
      params
    );

    const mappedAssignments = assignments.map((a) => ({
      ...a,
      pre_work_photo_url: toPublicPhotoUrl(req, a.pre_work_photo_url),
      post_work_photo_url: toPublicPhotoUrl(req, a.post_work_photo_url),
    }));

    res.json({ assignments: mappedAssignments });
  } catch (error) {
    console.error("Get assignments error:", error);
    res.status(500).json({ message: "Failed to get assignments", error: error.message });
  }
};

// Get map points for visualization
exports.getMapPoints = async (req, res) => {
  try {
    const [points] = await db.promise().query(
      `SELECT 
        al.id,
        al.latitude as lat,
        al.longitude as lng,
        al.highest_severity as severity,
        al.status,
        al.total_potholes,
        al.total_patchy,
        al.last_reported_at as date,
        COALESCE(al.ward, 'Unknown') as ward
       FROM aggregated_locations al
       ORDER BY al.last_reported_at DESC`
    );

    // Map status to frontend format
    const mappedPoints = points.map(p => ({
      ...p,
      severity: p.severity?.toLowerCase() || 'medium',
      status: mapStatusForFrontend(p.status)
    }));

    res.json({ points: mappedPoints });
  } catch (error) {
    console.error("Get map points error:", error);
    res.status(500).json({ message: "Failed to get map points", error: error.message });
  }
};

// Helper to map backend status to frontend status
function mapStatusForFrontend(status) {
  const statusMap = {
    'pending': 'open',
    'assigned': 'in-progress',
    'in_progress': 'in-progress',
    'pending_verification': 'in-progress',
    'verified': 'resolved',
    'completed': 'resolved'
  };
  return statusMap[status] || 'open';
}

// Get repair history (verified locations)
exports.getHistory = async (req, res) => {
  try {
    const { startDate, endDate, roadName } = req.query;

    let whereClause = "WHERE al.status = 'verified'";
    const params = [];

    if (startDate) {
      whereClause += " AND al.verified_at >= ?";
      params.push(toMySQLDatetime(startDate));
    }

    if (endDate) {
      whereClause += " AND al.verified_at <= ?";
      params.push(toMySQLDatetime(endDate));
    }

    const [history] = await db.promise().query(
      `SELECT 
        al.*,
        wa.contractor_id,
        wa.completed_at,
        c.company_name as contractor_name,
        c.contact_email as contractor_email
       FROM aggregated_locations al
       LEFT JOIN work_assignments wa ON wa.aggregated_location_id = al.id AND wa.status = 'completed'
       LEFT JOIN contractors c ON wa.contractor_id = c.id
       ${whereClause}
       ORDER BY al.verified_at DESC`,
      params
    );

    res.json({ history });
  } catch (error) {
    console.error("Get history error:", error);
    res.status(500).json({ message: "Failed to get history", error: error.message });
  }
};
