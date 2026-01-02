const path = require('path');

function looksLikeAbsoluteUrl(value) {
  return typeof value === 'string' && /^https?:\/\//i.test(value);
}

function stripQueryAndHash(value) {
  if (typeof value !== 'string') return value;
  return value.split('#')[0].split('?')[0];
}

/**
 * Normalize whatever is stored (legacy full URL, /uploads/..., or filename)
 * into a filename suitable for DB storage.
 */
function toStoredPhotoFilename(value) {
  if (!value) return null;

  const raw = String(value);

  try {
    if (looksLikeAbsoluteUrl(raw)) {
      const url = new URL(raw);
      return path.basename(url.pathname);
    }
  } catch {
    // fall through
  }

  const cleaned = stripQueryAndHash(raw);

  // Handles: "/uploads/foo.jpg", "uploads/foo.jpg", "foo.jpg"
  return path.basename(cleaned);
}

/**
 * Convert stored value (filename, /uploads/..., legacy full URL) into
 * an absolute URL based on the current request.
 */
function toPublicPhotoUrl(req, storedValue) {
  if (!storedValue) return null;

  const raw = String(storedValue);
  if (looksLikeAbsoluteUrl(raw)) return raw;

  const host = req.get('host');
  const protocol = req.protocol;
  const filename = toStoredPhotoFilename(raw);
  return `${protocol}://${host}/uploads/${filename}`;
}

module.exports = {
  toStoredPhotoFilename,
  toPublicPhotoUrl,
};
