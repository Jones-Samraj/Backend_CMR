/**
 * Database Initialization Script
 * Run: npm run db:init
 */

require("dotenv").config();
const mysql = require("mysql2/promise");

const initDatabase = async () => {
  // Connect without database first
  const connection = await mysql.createConnection({
    host: process.env.DB_HOST,
    port: process.env.DB_PORT || 3306,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    multipleStatements: true
  });

  console.log("Connected to MySQL server...");

  const dbName = process.env.DB_NAME || "pothole_detection_system";

  // Create database
  await connection.query(`CREATE DATABASE IF NOT EXISTS ${dbName}`);
  await connection.query(`USE ${dbName}`);
  console.log(`Database '${dbName}' selected.`);

  // Create tables
  const createTables = `
    -- Users table
    CREATE TABLE IF NOT EXISTS users (
      id INT AUTO_INCREMENT PRIMARY KEY,
      email VARCHAR(255) UNIQUE NOT NULL,
      password VARCHAR(255) NOT NULL,
      role ENUM('user', 'admin', 'contractor') DEFAULT 'user',
      device_id VARCHAR(100),
      device_model VARCHAR(100),
      device_platform VARCHAR(50),
      app_version VARCHAR(20),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      INDEX idx_email (email),
      INDEX idx_device_id (device_id)
    );

    -- Reports table
    CREATE TABLE IF NOT EXISTS reports (
      id INT AUTO_INCREMENT PRIMARY KEY,
      report_id VARCHAR(50) UNIQUE NOT NULL,
      user_id INT,
      device_id VARCHAR(100) NOT NULL,
      reported_at TIMESTAMP NOT NULL,
      total_potholes INT DEFAULT 0,
      total_patchy_roads INT DEFAULT 0,
      health_score DECIMAL(5,2),
      status ENUM('pending', 'reviewed', 'assigned', 'in_progress', 'resolved') DEFAULT 'pending',
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL,
      INDEX idx_report_id (report_id),
      INDEX idx_status (status),
      INDEX idx_reported_at (reported_at)
    );

    -- Pothole detections table
    CREATE TABLE IF NOT EXISTS pothole_detections (
      id INT AUTO_INCREMENT PRIMARY KEY,
      report_id INT NOT NULL,
      location_id VARCHAR(50),
      latitude DECIMAL(10, 6) NOT NULL,
      longitude DECIMAL(10, 6) NOT NULL,
      severity ENUM('Low', 'Medium', 'High') NOT NULL,
      z_axis_acceleration DECIMAL(6, 3),
      speed_kmh DECIMAL(5, 2),
      timestamp TIMESTAMP NOT NULL,
      synced BOOLEAN DEFAULT FALSE,
      confirmation_count INT DEFAULT 0,
      road_type VARCHAR(50),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (report_id) REFERENCES reports(id) ON DELETE CASCADE,
      INDEX idx_location (latitude, longitude),
      INDEX idx_severity (severity),
      INDEX idx_timestamp (timestamp)
    );

    -- Road anomalies (patchy roads) table
    CREATE TABLE IF NOT EXISTS road_anomalies (
      id INT AUTO_INCREMENT PRIMARY KEY,
      report_id INT NOT NULL,
      location_id VARCHAR(50),
      start_latitude DECIMAL(10, 6) NOT NULL,
      start_longitude DECIMAL(10, 6) NOT NULL,
      end_latitude DECIMAL(10, 6),
      end_longitude DECIMAL(10, 6),
      severity ENUM('Low', 'Medium', 'High') DEFAULT 'Medium',
      start_timestamp TIMESTAMP NOT NULL,
      end_timestamp TIMESTAMP,
      duration_seconds INT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (report_id) REFERENCES reports(id) ON DELETE CASCADE,
      INDEX idx_location (start_latitude, start_longitude),
      INDEX idx_duration (duration_seconds)
    );

    -- Aggregated locations table
    CREATE TABLE IF NOT EXISTS aggregated_locations (
      id INT AUTO_INCREMENT PRIMARY KEY,
      grid_id VARCHAR(50) UNIQUE NOT NULL,
      latitude DECIMAL(10, 4) NOT NULL,
      longitude DECIMAL(10, 4) NOT NULL,
      road_name VARCHAR(255),
      total_potholes INT DEFAULT 0,
      total_patchy INT DEFAULT 0,
      highest_severity ENUM('Low', 'Medium', 'High') DEFAULT 'Low',
      report_count INT DEFAULT 1,
      ward VARCHAR(100),
      first_reported_at TIMESTAMP,
      last_reported_at TIMESTAMP,
      status ENUM('pending', 'assigned', 'in_progress', 'pending_verification', 'verified', 'fixed') DEFAULT 'pending',
      verified_at TIMESTAMP,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      INDEX idx_grid (grid_id),
      INDEX idx_severity (highest_severity),
      INDEX idx_location (latitude, longitude),
      INDEX idx_status (status)
    );

    -- App settings table
    CREATE TABLE IF NOT EXISTS app_settings (
      id INT AUTO_INCREMENT PRIMARY KEY,
      user_id INT UNIQUE,
      sensitivity ENUM('low', 'medium', 'high') DEFAULT 'medium',
      alerts_enabled BOOLEAN DEFAULT TRUE,
      alert_sound BOOLEAN DEFAULT TRUE,
      alert_vibration BOOLEAN DEFAULT TRUE,
      data_usage ENUM('wifi-only', 'wifi-and-mobile') DEFAULT 'wifi-and-mobile',
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    );

    -- Contractors table
    CREATE TABLE IF NOT EXISTS contractors (
      id INT AUTO_INCREMENT PRIMARY KEY,
      user_id INT,
      company_name VARCHAR(255),
      contact_email VARCHAR(255),
      contact_phone VARCHAR(20),
      service_area_lat DECIMAL(10, 6),
      service_area_lng DECIMAL(10, 6),
      service_radius_km DECIMAL(5, 2),
      is_active BOOLEAN DEFAULT TRUE,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL
    );

    -- Work assignments table
    CREATE TABLE IF NOT EXISTS work_assignments (
      id INT AUTO_INCREMENT PRIMARY KEY,
      aggregated_location_id INT NOT NULL,
      contractor_id INT NOT NULL,
      assigned_by INT,
      assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      due_date DATE,
      status ENUM('assigned', 'in_progress', 'pending_verification', 'completed', 'verified') DEFAULT 'assigned',
      pre_work_photo_url VARCHAR(1024),
      post_work_photo_url VARCHAR(1024),
      completed_at TIMESTAMP,
      admin_notes TEXT,
      notes TEXT,
      FOREIGN KEY (aggregated_location_id) REFERENCES aggregated_locations(id) ON DELETE CASCADE,
      FOREIGN KEY (contractor_id) REFERENCES contractors(id) ON DELETE CASCADE,
      FOREIGN KEY (assigned_by) REFERENCES users(id) ON DELETE SET NULL,
      INDEX idx_status (status),
      INDEX idx_contractor (contractor_id)
    );
  `;

  await connection.query(createTables);
  console.log("All tables created successfully!");

  // Create default admin user
  const bcrypt = require("bcryptjs");
  const hashedPassword = await bcrypt.hash("admin123", 10);
  
  try {
    await connection.query(
      `INSERT IGNORE INTO users (email, password, role) VALUES (?, ?, ?)`,
      ["admin@pothole.com", hashedPassword, "admin"]
    );
    console.log("Default admin user created (admin@pothole.com / admin123)");
  } catch (err) {
    console.log("Admin user may already exist.");
  }

  // Create sample contractors if none exist
  try {
    const [existingContractors] = await connection.query("SELECT COUNT(*) as count FROM contractors");
    if (existingContractors[0].count === 0) {
      await connection.query(`
        INSERT INTO contractors (company_name, contact_email, contact_phone, is_active) VALUES
        ('Metro Road Works Pvt Ltd', 'metro@roadworks.com', '9876543210', TRUE),
        ('Highway Repairs Co', 'contact@highwayrepairs.com', '9876543211', TRUE),
        ('City Infrastructure Ltd', 'info@cityinfra.com', '9876543212', TRUE),
        ('Urban Road Solutions', 'support@urbanroads.com', '9876543213', TRUE),
        ('Express Roadways', 'admin@expressroads.com', '9876543214', TRUE)
      `);
      console.log("Sample contractors created");
    }
  } catch (err) {
    console.log("Contractors may already exist:", err.message);
  }

  // Create sample aggregated locations if none exist
  try {
    const [existingLocations] = await connection.query("SELECT COUNT(*) as count FROM aggregated_locations");
    if (existingLocations[0].count === 0) {
      await connection.query(`
        INSERT INTO aggregated_locations (grid_id, latitude, longitude, road_name, total_potholes, total_patchy, highest_severity, report_count, ward, first_reported_at, last_reported_at, status, verified_at) VALUES
        ('13.0827_80.2707', 13.0827, 80.2707, 'Anna Salai', 3, 2, 'High', 5, 'Anna Nagar', NOW() - INTERVAL 5 DAY, NOW() - INTERVAL 1 DAY, 'pending', NULL),
        ('13.0569_80.2425', 13.0569, 80.2425, 'Mount Road', 4, 1, 'Medium', 4, 'T. Nagar', NOW() - INTERVAL 7 DAY, NOW() - INTERVAL 2 DAY, 'assigned', NULL),
        ('13.0674_80.2376', 13.0674, 80.2376, 'Cathedral Road', 2, 4, 'High', 6, 'Mylapore', NOW() - INTERVAL 10 DAY, NOW() - INTERVAL 3 DAY, 'pending_verification', NULL),
        ('13.0600_80.2800', 13.0600, 80.2800, 'Gandhi Road', 5, 0, 'Low', 3, 'Adyar', NOW() - INTERVAL 3 DAY, NOW() - INTERVAL 1 DAY, 'pending', NULL),
        ('8.4283_78.0254', 8.4283, 78.0254, 'Beach Road', 2, 1, 'Medium', 2, 'Beach Road', NOW() - INTERVAL 2 DAY, NOW(), 'pending', NULL),
        ('13.0900_80.2560', 13.0900, 80.2560, 'ECR Road', 3, 1, 'High', 4, 'Thiruvanmiyur', NOW() - INTERVAL 30 DAY, NOW() - INTERVAL 15 DAY, 'verified', NOW() - INTERVAL 10 DAY),
        ('13.0350_80.2650', 13.0350, 80.2650, 'OMR Road', 2, 2, 'Medium', 3, 'Sholinganallur', NOW() - INTERVAL 25 DAY, NOW() - INTERVAL 12 DAY, 'verified', NOW() - INTERVAL 8 DAY)
      `);
      console.log("Sample aggregated locations created");
    }
  } catch (err) {
    console.log("Aggregated locations may already exist:", err.message);
  }

  // Create sample work assignments if none exist
  try {
    const [existingAssignments] = await connection.query("SELECT COUNT(*) as count FROM work_assignments");
    if (existingAssignments[0].count === 0) {
      // Get admin user id
      const [adminUsers] = await connection.query("SELECT id FROM users WHERE role = 'admin' LIMIT 1");
      const adminId = adminUsers.length > 0 ? adminUsers[0].id : null;
      
      // Get contractor ids
      const [contractorRows] = await connection.query("SELECT id FROM contractors ORDER BY id LIMIT 5");
      
      // Get location ids
      const [locationRows] = await connection.query("SELECT id, status FROM aggregated_locations ORDER BY id");
      
      if (contractorRows.length > 0 && locationRows.length > 0) {
        // Create assignment for "assigned" location
        const assignedLocation = locationRows.find(l => l.status === 'assigned') || locationRows[1];
        if (assignedLocation) {
          await connection.query(`
            INSERT INTO work_assignments (aggregated_location_id, contractor_id, assigned_by, due_date, status, notes)
            VALUES (?, ?, ?, DATE_ADD(NOW(), INTERVAL 7 DAY), 'assigned', 'Sample assignment - road requires immediate attention')
          `, [assignedLocation.id, contractorRows[0].id, adminId]);
        }
        
        // Create assignment for "pending_verification" location
        const pendingLocation = locationRows.find(l => l.status === 'pending_verification') || locationRows[2];
        if (pendingLocation && contractorRows.length > 2) {
          await connection.query(`
            INSERT INTO work_assignments (aggregated_location_id, contractor_id, assigned_by, due_date, status, completed_at, notes)
            VALUES (?, ?, ?, DATE_ADD(NOW(), INTERVAL 3 DAY), 'pending_verification', NOW(), 'Work completed - awaiting admin verification')
          `, [pendingLocation.id, contractorRows[2].id, adminId]);
        }
        
        // Create assignments for "verified" locations (for History page)
        const verifiedLocations = locationRows.filter(l => l.status === 'verified');
        for (let i = 0; i < verifiedLocations.length && i < contractorRows.length; i++) {
          await connection.query(`
            INSERT INTO work_assignments (aggregated_location_id, contractor_id, assigned_by, due_date, status, completed_at, notes)
            VALUES (?, ?, ?, DATE_ADD(NOW(), INTERVAL -5 DAY), 'verified', NOW() - INTERVAL ? DAY, 'Work verified and closed')
          `, [verifiedLocations[i].id, contractorRows[i % contractorRows.length].id, adminId, 10 + i * 2]);
        }
        
        console.log("Sample work assignments created");
      }
    }
  } catch (err) {
    console.log("Work assignments may already exist:", err.message);
  }

  await connection.end();
  console.log("\nDatabase initialization complete!");
};

initDatabase().catch((err) => {
  console.error("Database initialization failed:", err);
  process.exit(1);
});
