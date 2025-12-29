const admin = require("firebase-admin");
const fs = require("fs");

let initialized = false;

function initFirebase() {
  if (initialized) return;

  const databaseURL = process.env.FIREBASE_DATABASE_URL;
  if (!databaseURL) {
    throw new Error(
      "FIREBASE_DATABASE_URL is not set. Needed to read from Firebase Realtime Database."
    );
  }

  let credential;

  if (process.env.FIREBASE_SERVICE_ACCOUNT_JSON) {
    try {
      const parsed = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT_JSON);
      credential = admin.credential.cert(parsed);
    } catch (e) {
      throw new Error(
        `Invalid FIREBASE_SERVICE_ACCOUNT_JSON (must be valid JSON): ${e.message}`
      );
    }
  } else if (process.env.FIREBASE_SERVICE_ACCOUNT_PATH) {
    const path = process.env.FIREBASE_SERVICE_ACCOUNT_PATH;
    if (!fs.existsSync(path)) {
      throw new Error(`FIREBASE_SERVICE_ACCOUNT_PATH not found: ${path}`);
    }
    const parsed = JSON.parse(fs.readFileSync(path, "utf8"));
    credential = admin.credential.cert(parsed);
  } else if (process.env.GOOGLE_APPLICATION_CREDENTIALS) {
    const path = process.env.GOOGLE_APPLICATION_CREDENTIALS;
    if (!fs.existsSync(path)) {
      throw new Error(`GOOGLE_APPLICATION_CREDENTIALS not found: ${path}`);
    }
    const parsed = JSON.parse(fs.readFileSync(path, "utf8"));
    credential = admin.credential.cert(parsed);
  } else {
    throw new Error(
      "Firebase not configured. Set FIREBASE_SERVICE_ACCOUNT_JSON or FIREBASE_SERVICE_ACCOUNT_PATH (or GOOGLE_APPLICATION_CREDENTIALS)."
    );
  }

  admin.initializeApp({
    credential,
    databaseURL,
  });

  initialized = true;
}

function getFirebaseAdmin() {
  initFirebase();
  return admin;
}

module.exports = {
  getFirebaseAdmin,
};
