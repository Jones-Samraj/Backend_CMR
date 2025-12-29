/**
 * Utility script to create users (admin, contractor, user)
 * 
 * Usage:
 *   node src/utils/createUser.js
 * 
 * Or import and use the functions in your code
 */

require('dotenv').config();
const db = require('../config/db');
const bcrypt = require('bcryptjs');

/**
 * Create a new user
 */
async function createUser(email, password, role = 'user') {
  try {
    // Check if user exists
    const [existing] = await db.promise().query(
      'SELECT id FROM users WHERE email = ?',
      [email]
    );

    if (existing.length > 0) {
      console.log(`User ${email} already exists with ID: ${existing[0].id}`);
      return existing[0].id;
    }

    // Hash password
    const hashedPassword = await bcrypt.hash(password, 10);

    // Insert user
    const [result] = await db.promise().query(
      'INSERT INTO users (email, password, role) VALUES (?, ?, ?)',
      [email, hashedPassword, role]
    );

    console.log(`✅ User created: ${email} (${role}) - ID: ${result.insertId}`);
    return result.insertId;
  } catch (error) {
    console.error('Error creating user:', error.message);
    throw error;
  }
}

/**
 * Create a contractor with profile
 */
async function createContractor(email, password, companyName, contactPhone = null) {
  try {
    // Create user with contractor role
    const userId = await createUser(email, password, 'contractor');

    // Check if contractor profile exists
    const [existing] = await db.promise().query(
      'SELECT id FROM contractors WHERE user_id = ?',
      [userId]
    );

    if (existing.length > 0) {
      console.log(`Contractor profile already exists for ${email}`);
      return existing[0].id;
    }

    // Create contractor profile
    const [result] = await db.promise().query(
      `INSERT INTO contractors (user_id, company_name, contact_email, contact_phone, is_active) 
       VALUES (?, ?, ?, ?, 1)`,
      [userId, companyName, email, contactPhone]
    );

    console.log(`✅ Contractor profile created: ${companyName} - ID: ${result.insertId}`);
    return result.insertId;
  } catch (error) {
    console.error('Error creating contractor:', error.message);
    throw error;
  }
}

/**
 * Create an admin user
 */
async function createAdmin(email, password) {
  return await createUser(email, password, 'admin');
}

/**
 * List all users
 */
async function listUsers() {
  try {
    const [users] = await db.promise().query(
      'SELECT id, email, role, created_at FROM users ORDER BY id'
    );
    console.log('\n📋 All Users:');
    console.table(users);
    return users;
  } catch (error) {
    console.error('Error listing users:', error.message);
  }
}

/**
 * List all contractors
 */
async function listContractors() {
  try {
    const [contractors] = await db.promise().query(
      `SELECT c.id, u.email, c.company_name, c.contact_phone, c.is_active, c.created_at 
       FROM contractors c 
       JOIN users u ON c.user_id = u.id 
       ORDER BY c.id`
    );
    console.log('\n🔧 All Contractors:');
    console.table(contractors);
    return contractors;
  } catch (error) {
    console.error('Error listing contractors:', error.message);
  }
}

/**
 * Update user password
 */
async function updatePassword(email, newPassword) {
  try {
    const hashedPassword = await bcrypt.hash(newPassword, 10);
    const [result] = await db.promise().query(
      'UPDATE users SET password = ? WHERE email = ?',
      [hashedPassword, email]
    );

    if (result.affectedRows > 0) {
      console.log(`✅ Password updated for ${email}`);
    } else {
      console.log(`❌ User ${email} not found`);
    }
  } catch (error) {
    console.error('Error updating password:', error.message);
  }
}

/**
 * Delete user
 */
async function deleteUser(email) {
  try {
    // Get user ID first
    const [users] = await db.promise().query(
      'SELECT id FROM users WHERE email = ?',
      [email]
    );

    if (users.length === 0) {
      console.log(`❌ User ${email} not found`);
      return;
    }

    const userId = users[0].id;

    // Delete contractor profile if exists
    await db.promise().query('DELETE FROM contractors WHERE user_id = ?', [userId]);

    // Delete user
    await db.promise().query('DELETE FROM users WHERE id = ?', [userId]);

    console.log(`✅ User ${email} deleted`);
  } catch (error) {
    console.error('Error deleting user:', error.message);
  }
}

// ============================================
// MAIN - Run when executed directly
// ============================================
async function main() {
  console.log('\n🚀 User Management Utility\n');

  // Example: Create a contractor
  await createContractor(
    'arun@gmail.com',     // email
    '123',                 // password (will be hashed)
    'arun Construction',  // company name
    '9876543210'           // phone
  );

  // Create an admin
  await createAdmin('admin@example.com', 'adminpass123');

  // Update existing password
  await updatePassword('jones@gmail.com', '123');

  // List all users and contractors
  await listUsers();
  await listContractors();

  // Close database connection
  process.exit(0);
}

// Run if executed directly
if (require.main === module) {
  main().catch(console.error);
}

// Export functions for use in other files
module.exports = {
  createUser,
  createContractor,
  createAdmin,
  listUsers,
  listContractors,
  updatePassword,
  deleteUser,
};
