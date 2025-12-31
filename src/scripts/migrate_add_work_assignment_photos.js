const db = require("../config/db");

const columnExists = async (connection, table, column) => {
  const [rows] = await connection.query(
    `SELECT COUNT(*) AS cnt
     FROM INFORMATION_SCHEMA.COLUMNS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?`,
    [table, column]
  );
  return (rows?.[0]?.cnt || 0) > 0;
};

(async () => {
  const connection = await db.promise().getConnection();
  try {
    console.log("[MIGRATE] Checking work_assignments photo columns...");

    const table = "work_assignments";
    const preCol = "pre_work_photo_url";
    const postCol = "post_work_photo_url";

    const hasPre = await columnExists(connection, table, preCol);
    const hasPost = await columnExists(connection, table, postCol);

    const alters = [];
    if (!hasPre) alters.push(`ADD COLUMN ${preCol} VARCHAR(1024) NULL`);
    if (!hasPost) alters.push(`ADD COLUMN ${postCol} VARCHAR(1024) NULL`);

    if (alters.length === 0) {
      console.log("[MIGRATE] Nothing to do. Columns already exist.");
      process.exit(0);
    }

    const sql = `ALTER TABLE ${table} ${alters.join(", ")};`;
    console.log("[MIGRATE] Applying:", sql);
    await connection.query(sql);

    console.log("[MIGRATE] Done.");
    process.exit(0);
  } catch (err) {
    console.error("[MIGRATE] Failed:", err);
    process.exit(1);
  } finally {
    connection.release();
  }
})();
