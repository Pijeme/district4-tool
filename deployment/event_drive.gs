/** Event-only uploads into the folder owner's Google Drive.
 * Create a separate Apps Script project. Execute as Me; access Anyone.
 * Run setupEventDrive once and keep EVENT_DRIVE_API_TOKEN private.
 */
const EVENT_DRIVE_FOLDER = '1pvwfSIS0kcvjp__O9--9-1-2f7zIBcEI';

function setupEventDrive() {
  const properties = PropertiesService.getScriptProperties();
  const token = properties.getProperty('EVENT_DRIVE_API_TOKEN') ||
    (Utilities.getUuid() + Utilities.getUuid()).replace(/-/g, '');
  properties.setProperty('EVENT_DRIVE_API_TOKEN', token);
  console.log('EVENT_DRIVE_API_TOKEN=' + token);
  console.log('Event folder: ' + DriveApp.getFolderById(EVENT_DRIVE_FOLDER).getName());
}

function doPost(event) {
  const reply = value => ContentService.createTextOutput(JSON.stringify(value))
    .setMimeType(ContentService.MimeType.JSON);
  let lock;
  try {
    let data;
    try {
      data = JSON.parse(event && event.postData && event.postData.contents);
    } catch (_) {
      return reply({ok:false, code:'INPUT'});
    }
    if (!data || typeof data !== 'object' || Array.isArray(data)) return reply({ok:false, code:'INPUT'});
    const token = PropertiesService.getScriptProperties().getProperty('EVENT_DRIVE_API_TOKEN');
    if (!token || token.length < 32 || typeof data.token !== 'string') return reply({ok:false, code:'AUTH'});
    const expected = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, token);
    const supplied = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, data.token);
    let difference = 0;
    for (let i=0; i<expected.length; i++) difference |= expected[i] ^ supplied[i];
    if (difference) return reply({ok:false, code:'AUTH'});
    if (data.purpose !== 'event') return reply({ok:false, code:'EVENT_ONLY'});
    if (data.action === 'delete') {
      if (typeof data.record_key !== 'string' || !/^[a-f0-9]{32}$/.test(data.record_key) ||
          typeof data.event_key !== 'string' || !/^[a-f0-9]{32}$/.test(data.event_key)) {
        return reply({ok:false, code:'INPUT'});
      }
      lock = LockService.getScriptLock();
      lock.waitLock(25000);
      // A timed-out older upload must not recreate a deleted registration later.
      PropertiesService.getScriptProperties().setProperty('deleted:' + data.event_key + ':' + data.record_key, '1');
      const folder = DriveApp.getFolderById(EVENT_DRIVE_FOLDER);
      const prefix = 'Event:' + data.event_key + ':' + data.record_key + ':';
      const files = folder.getFiles();
      const pattern = /^(photo|id):[a-f0-9]{64}$/;
      while (files.hasNext()) {
        const file = files.next();
        const description = file.getDescription();
        if (description.startsWith(prefix) && pattern.test(description.slice(prefix.length))) {
          file.setTrashed(true);
        }
      }
      // Confirm all versions are gone from the active event folder, including a retry.
      const remaining = folder.getFiles();
      while (remaining.hasNext()) {
        const description = remaining.next().getDescription();
        if (description.startsWith(prefix) && pattern.test(description.slice(prefix.length))) {
          return reply({ok:false, code:'DRIVE'});
        }
      }
      return reply({ok:true, deleted:true, purpose:'event', event_key:data.event_key,
                    record_key:data.record_key, folder_id:EVENT_DRIVE_FOLDER});
    }
    if (data.action && data.action !== 'upload') return reply({ok:false, code:'INPUT'});
    if (typeof data.record_key !== 'string' || !/^[a-f0-9]{32}$/.test(data.record_key) ||
        typeof data.event_key !== 'string' || !/^[a-f0-9]{32}$/.test(data.event_key) ||
        !['photo','id'].includes(data.side) || typeof data.filename !== 'string' ||
        data.filename.length > 240 || typeof data.image !== 'string' ||
        data.image.length > 12*1024*1024) return reply({ok:false, code:'INPUT'});
    const pattern = data.side === 'id'
      ? /^Event>[^<>/\\\x00-\x1f]+>[1-9][0-9]*-ID\.png$/
      : /^Event>[^<>/\\\x00-\x1f]+>[1-9][0-9]*\.png$/;
    if (!pattern.test(data.filename)) return reply({ok:false, code:'INPUT'});
    let bytes;
    try {
      bytes = Utilities.base64Decode(data.image);
    } catch (_) {
      return reply({ok:false, code:'IMAGE'});
    }
    const signature = [137,80,78,71,13,10,26,10];
    if (bytes.length > 8*1024*1024 || signature.some((b,i) => (bytes[i] & 255) !== b)) return reply({ok:false, code:'IMAGE'});
    const digest = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, bytes)
      .map(b => (b & 255).toString(16).padStart(2,'0')).join('');
    const identity = 'Event:' + data.event_key + ':' + data.record_key + ':' + data.side + ':' + digest;
    lock = LockService.getScriptLock();
    lock.waitLock(25000);
    if (PropertiesService.getScriptProperties().getProperty('deleted:' + data.event_key + ':' + data.record_key)) {
      return reply({ok:false, code:'DELETED'});
    }
    const folder = DriveApp.getFolderById(EVENT_DRIVE_FOLDER);
    const files = folder.getFilesByName(data.filename);
    let file;
    while (files.hasNext()) {
      const candidate = files.next();
      if (candidate.getDescription() === identity) { file = candidate; break; }
    }
    if (!file) {
      file = folder.createFile(Utilities.newBlob(bytes, 'image/png', data.filename));
      file.setDescription(identity);
    }
    return reply({ok:true, purpose:'event', file_id:file.getId(), name:data.filename,
                  folder_id:EVENT_DRIVE_FOLDER, side:data.side});
  } catch (error) {
    console.error('Event Drive upload failed: ' + String(error && error.message || error));
    return reply({ok:false, code:'DRIVE'});
  } finally {
    if (lock && lock.hasLock()) lock.releaseLock();
  }
}
