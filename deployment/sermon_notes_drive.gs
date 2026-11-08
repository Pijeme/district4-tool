/** Personal sermon-note backups. Use a NEW Apps Script project, not CCM/Event.
 * Run setupSermonNotesDrive as the supplied folder's owner, then deploy a Web
 * app executing as Me, access Anyone. Only the website server holds the token.
 * Nothing here grants Drive sharing permissions or accepts browser credentials.
 */
const SERMON_NOTES_FOLDER_ID = '1E-bNrscGBsddTljXDqGlOwk0YaAoIZEG';
const SN_TOKEN_PROPERTY = 'SERMON_NOTES_DRIVE_API_TOKEN';
const SN_MAX_REQUEST_BYTES = 1024 * 1024;
const SN_MAX_NOTE_BYTES = 512 * 1024;
const SN_MAX_INDEX_BYTES = 4 * 1024 * 1024;
const SN_ID = /^[a-f0-9]{32}$/;

function setupSermonNotesDrive() {
  const lock = LockService.getScriptLock();
  lock.waitLock(25000);
  try {
    const root = DriveApp.getFolderById(SERMON_NOTES_FOLDER_ID);
    snFolder(root, 'users', true);
    const properties = PropertiesService.getScriptProperties();
    const token = properties.getProperty(SN_TOKEN_PROPERTY) ||
      (Utilities.getUuid() + Utilities.getUuid()).replace(/-/g, '');
    properties.setProperty(SN_TOKEN_PROPERTY, token);
    console.log('Sermon notes folder: ' + root.getName());
    console.log(SN_TOKEN_PROPERTY + '=' + token);
  } finally {
    lock.releaseLock();
  }
}

/** Run from the editor after setup. Verifies actual writes, not just access. */
function verifySermonNotesDrive() {
  let scratch;
  try {
    const root = DriveApp.getFolderById(SERMON_NOTES_FOLDER_ID);
    scratch = root.createFolder('setup-check-' + Utilities.getUuid());
    const file = scratch.createFile(Utilities.newBlob('{"check":1}', 'application/json', 'check.json'));
    if (file.getBlob().getDataAsString('UTF-8') !== '{"check":1}') throw Error('Read failed.');
    file.setContent('{"check":2}');
    if (file.getBlob().getDataAsString('UTF-8') !== '{"check":2}') throw Error('Update failed.');
    console.log('PASS: sermon notes Drive create, read and update.');
  } finally {
    if (scratch) scratch.setTrashed(true);
  }
}

function doGet() {
  // Opening the URL never reveals account names, note text or configuration.
  return snReply({ok: false, code: 'POST_REQUIRED'});
}

function doPost(event) {
  let lock;
  try {
    const raw = event && event.postData && event.postData.contents;
    if (typeof raw !== 'string' || raw.length > SN_MAX_REQUEST_BYTES || snBytes(raw) > SN_MAX_REQUEST_BYTES)
      return snReply({ok: false, code: 'INPUT'});
    let data;
    try { data = JSON.parse(raw); } catch (_) { return snReply({ok: false, code: 'INPUT'}); }
    if (!snObject(data)) return snReply({ok: false, code: 'INPUT'});
    const expected = PropertiesService.getScriptProperties().getProperty(SN_TOKEN_PROPERTY);
    if (!expected || expected.length < 32 || typeof data.token !== 'string' ||
        data.token.length > 256 || !snEqual(snHash(expected), snHash(data.token)))
      return snReply({ok: false, code: 'AUTH'});
    if (data.purpose !== 'sermon_notes') return snReply({ok: false, code: 'SERMON_NOTES_ONLY'});
    const actions = ['status', 'list', 'get', 'put', 'delete'];
    if (!actions.includes(data.action)) return snReply({ok: false, code: 'INPUT'});
    const allowed = ['token', 'purpose', 'action', 'user_key', 'note_id', 'base_version',
      'request_id', 'note', 'after', 'limit', 'index_version', 'version'];
    if (Object.keys(data).some(key => !allowed.includes(key))) return snReply({ok: false, code: 'INPUT'});
    if (data.action === 'status') {
      const root = DriveApp.getFolderById(SERMON_NOTES_FOLDER_ID);
      return snReply({ok: true, schema_version: 1, folder_id: root.getId(),
        max_note_bytes: SN_MAX_NOTE_BYTES});
    }
    if (typeof data.user_key !== 'string' || data.user_key.length > 200 ||
        !/^[^\s:\x00-\x1f\x7f]+:[^\x00-\x1f\x7f]+$/.test(data.user_key) ||
        data.user_key !== data.user_key.trim() || data.user_key !== data.user_key.toLowerCase())
      return snReply({ok: false, code: 'INPUT'});
    const writing = ['put', 'delete'].includes(data.action);
    if (['get', 'put', 'delete'].includes(data.action) && !snId(data.note_id))
      return snReply({ok: false, code: 'INPUT'});
    if (data.action === 'get' && data.version !== undefined &&
        (!Number.isSafeInteger(data.version) || data.version < 1))
      return snReply({ok: false, code: 'INPUT'});
    if (writing && (!Number.isSafeInteger(data.base_version) || data.base_version < 0 ||
        !Number.isSafeInteger(data.base_version + 1) || !snId(data.request_id)))
      return snReply({ok: false, code: 'INPUT'});
    if (data.action === 'put' && !snValidNote(data.note)) return snReply({ok: false, code: 'NOTE'});
    if (data.action === 'delete' && data.note !== undefined) return snReply({ok: false, code: 'INPUT'});
    if (data.action === 'list' && (
        (data.after !== undefined && data.after !== '' && !snId(data.after)) ||
        (data.limit !== undefined && (!Number.isInteger(data.limit) || data.limit < 1 || data.limit > 100)) ||
        (data.index_version !== undefined && (!Number.isSafeInteger(data.index_version) || data.index_version < 0))))
      return snReply({ok: false, code: 'INPUT'});

    // All index operations use the same lock. Drive has no multi-file transaction:
    // write an immutable revision FIRST, then publish it through index.json.
    lock = LockService.getScriptLock();
    if (!lock.tryLock(20000)) return snReply({ok: false, code: 'BUSY', retryable: true});
    const account = snAccount(data.user_key, data.action === 'put');
    const index = account ? snReadIndex(account, data.user_key) : snEmptyIndex(data.user_key);
    if (data.action === 'list') {
      if (data.index_version !== undefined && data.index_version !== index.version)
        return snReply({ok: false, code: 'CURSOR_EXPIRED', index_version: index.version});
      const ids = Object.keys(index.notes).sort().filter(id => id > (data.after || ''));
      const page = ids.slice(0, data.limit || 100);
      return snReply({ok: true, index_version: index.version,
        notes: page.map(id => snSummary(index.notes[id])),
        next_after: ids.length > page.length ? page[page.length - 1] : null});
    }
    const current = index.notes[data.note_id];
    if (data.action === 'get') {
      if (!current) return snReply({ok: false, code: 'NOT_FOUND'});
      if (data.version !== undefined && data.version !== current.version)
        return snReply({ok: false, code: 'CHANGED', current: snSummary(current)});
      return snReply({ok: true, record: snReadRecord(account, current, data.user_key)});
    }
    const fingerprint = snHash(snCanonical({action: data.action, base_version: data.base_version,
      note: data.action === 'put' ? data.note : null}));
    if (current && current.request_id === data.request_id) {
      if (current.fingerprint !== fingerprint) return snReply({ok: false, code: 'REQUEST_REUSED'});
      return snReply({ok: true, item: snSummary(current), replayed: true});
    }
    if ((current ? current.version : 0) !== data.base_version)
      return snReply({ok: false, code: 'CONFLICT', current: current ? snSummary(current) : null});
    if (data.action === 'delete' && !current) return snReply({ok: false, code: 'NOT_FOUND'});
    const version = data.base_version + 1;
    const name = data.note_id + '-v' + version + '-' + data.request_id + '.json';
    const revisions = snFolder(account, 'revisions', true);
    let file = snFile(revisions, name);
    let record;
    if (file) {
      // Retry an interrupted publication using the already-uploaded revision.
      record = snReadJson(file, SN_MAX_REQUEST_BYTES);
      if (record.user_key !== data.user_key || record.note_id !== data.note_id ||
          record.version !== version || record.request_id !== data.request_id ||
          record.fingerprint !== fingerprint || record.schema_version !== 1 ||
          record.deleted !== (data.action === 'delete') ||
          snHash(snCanonical({action: data.action, base_version: data.base_version,
            note: record.note})) !== fingerprint) throw Error('Staged revision mismatch.');
    } else {
      record = {schema_version: 1, user_key: data.user_key, note_id: data.note_id,
        version: version, request_id: data.request_id, fingerprint: fingerprint,
        created_at: current ? current.created_at : new Date().toISOString(),
        updated_at: new Date().toISOString(), deleted: data.action === 'delete',
        note: data.action === 'put' ? data.note : null};
      file = revisions.createFile(Utilities.newBlob(JSON.stringify(record), 'application/json', name));
    }
    const item = {note_id: data.note_id, version: version, request_id: data.request_id,
      fingerprint: fingerprint, file_name: name, sha256: snHash(JSON.stringify(record)),
      created_at: record.created_at, updated_at: record.updated_at, deleted: record.deleted,
      theme: record.deleted ? current.theme : record.note.theme,
      text: record.deleted ? current.text : record.note.text};
    index.notes[data.note_id] = item;
    index.version += 1;
    index.updated_at = record.updated_at;
    const encoded = JSON.stringify(index);
    if (snBytes(encoded) > SN_MAX_INDEX_BYTES) return snReply({ok: false, code: 'INDEX_FULL'});
    const indexFile = snFile(account, 'index.json');
    if (indexFile) indexFile.setContent(encoded);
    else account.createFile(Utilities.newBlob(encoded, 'application/json', 'index.json'));
    return snReply({ok: true, item: snSummary(item), replayed: false});
  } catch (error) {
    // Do not log request bodies, tokens, account keys, sermon text or Drive IDs.
    console.error('Sermon notes storage operation failed. Inspect Drive access and quota.');
    return snReply({ok: false, code: 'STORAGE', retryable: true});
  } finally {
    if (lock && lock.hasLock()) lock.releaseLock();
  }
}

function snReply(value) {
  return ContentService.createTextOutput(JSON.stringify(value)).setMimeType(ContentService.MimeType.JSON);
}

function snObject(value) { return value !== null && typeof value === 'object' && !Array.isArray(value); }
function snId(value) { return typeof value === 'string' && SN_ID.test(value); }
function snBytes(value) { return Utilities.newBlob(value).getBytes().length; }
function snHash(value) {
  return Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, value, Utilities.Charset.UTF_8)
    .map(byte => (byte & 255).toString(16).padStart(2, '0')).join('');
}
function snEqual(a, b) {
  let difference = a.length ^ b.length;
  for (let i = 0; i < a.length; i++) difference |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return difference === 0;
}
function snCanonical(value) {
  if (Array.isArray(value)) return '[' + value.map(snCanonical).join(',') + ']';
  if (snObject(value)) return '{' + Object.keys(value).sort()
    .map(key => JSON.stringify(key) + ':' + snCanonical(value[key])).join(',') + '}';
  return JSON.stringify(value);
}
function snValidNote(note) {
  return snObject(note) && note.schema_version === 1 &&
    typeof note.theme === 'string' && note.theme.trim().length > 0 && note.theme.length <= 500 &&
    typeof note.text === 'string' && note.text.trim().length > 0 && note.text.length <= 500 &&
    snObject(note.content) && snBytes(JSON.stringify(note)) <= SN_MAX_NOTE_BYTES;
}
function snFolder(parent, name, create) {
  const found = parent.getFoldersByName(name);
  const folder = found.hasNext() ? found.next() : null;
  if (found.hasNext()) throw Error('Duplicate folder.');
  return folder || (create ? parent.createFolder(name) : null);
}
function snFile(parent, name) {
  const found = parent.getFilesByName(name);
  const file = found.hasNext() ? found.next() : null;
  if (found.hasNext()) throw Error('Duplicate file.');
  return file;
}
function snAccount(userKey, create) {
  const root = DriveApp.getFolderById(SERMON_NOTES_FOLDER_ID);
  const users = snFolder(root, 'users', create);
  if (!users) return null;
  const username = userKey.slice(userKey.indexOf(':') + 1);
  const label = username.replace(/[^a-z0-9._-]+/g, '_').slice(0, 60) || 'user';
  return snFolder(users, label + '--' + snHash(userKey), create);
}
function snEmptyIndex(userKey) {
  return {schema_version: 1, user_key: userKey, version: 0, updated_at: null, notes: {}};
}
function snReadJson(file, limit) {
  if (file.getSize() > limit) throw Error('Stored file too large.');
  const value = JSON.parse(file.getBlob().getDataAsString('UTF-8'));
  if (!snObject(value)) throw Error('Invalid stored JSON.');
  return value;
}
function snReadIndex(account, userKey) {
  const file = snFile(account, 'index.json');
  if (!file) return snEmptyIndex(userKey);
  const index = snReadJson(file, SN_MAX_INDEX_BYTES);
  if (index.schema_version !== 1 || index.user_key !== userKey || !snObject(index.notes) ||
      !Number.isSafeInteger(index.version) || index.version < 0) throw Error('Invalid index.');
  for (const id of Object.keys(index.notes)) {
    const item = index.notes[id];
    if (!snId(id) || !snObject(item) || item.note_id !== id ||
        !Number.isSafeInteger(item.version) || item.version < 1 || !snId(item.request_id) ||
        !/^[a-f0-9]{64}$/.test(item.sha256) || !/^[a-f0-9]{64}$/.test(item.fingerprint) ||
        typeof item.deleted !== 'boolean' || typeof item.theme !== 'string' || typeof item.text !== 'string' ||
        item.file_name !== id + '-v' + item.version + '-' + item.request_id + '.json')
      throw Error('Invalid index entry.');
  }
  return index;
}
function snReadRecord(account, item, userKey) {
  const revisions = snFolder(account, 'revisions', false);
  const file = revisions && snFile(revisions, item.file_name);
  if (!file) throw Error('Missing revision.');
  const record = snReadJson(file, SN_MAX_REQUEST_BYTES);
  if (snHash(JSON.stringify(record)) !== item.sha256 || record.user_key !== userKey ||
      record.schema_version !== 1 || record.note_id !== item.note_id || record.version !== item.version ||
      record.fingerprint !== item.fingerprint || record.request_id !== item.request_id ||
      record.deleted !== item.deleted || (record.deleted ? record.note !== null : !snValidNote(record.note)))
    throw Error('Damaged revision.');
  return record;
}
function snSummary(item) {
  return {note_id: item.note_id, version: item.version, sha256: item.sha256,
    theme: item.theme, text: item.text, deleted: item.deleted,
    created_at: item.created_at, updated_at: item.updated_at};
}
