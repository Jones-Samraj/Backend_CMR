/**
 * Quick script to add contractor profile for jones@gmail.com
 * Run: node src/utils/addJonesContractor.js
 */

require('dotenv').config();
const db = require('../config/db');

async function main() {
  try {
    // Check if contractor profile already exists
    const [existing] = await db.promise().query(
      'SELECT * FROM contractors WHERE user_id = 3'
    );

    if (existing.length > 0) {
      console.log('Contractor profile already exists:', existing[0]);
      process.exit(0);
    }

    // Create contractor profile for user_id = 3 (jones@gmail.com)
    await db.promise().query(
      `INSERT INTO contractors (user_id, company_name, contact_email, contact_phone, is_active) 
       VALUES (3, 'Jones Construction', 'jones@gmail.com', '1234567890', 1)`
    );

    console.log('✅ Contractor profile created for jones@gmail.com');
    process.exit(0);
  } catch (error) {
    console.error('Error:', error.message);
    process.exit(1);
  }
}

main();
