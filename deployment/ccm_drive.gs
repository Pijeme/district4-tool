/** CCM-only image uploads into the folder owner's Google Drive.
 * Deploy as a Web app: execute as Me; access Anyone.
 * The private API token, not the shared CCM admin password, authorizes uploads.
 * Event uploads require their own script and folder connection.
 */
const CCM_DRIVE_FOLDER = '1IYlGZPuHL7CUW7HqwWEHnxuaG63gscEY';

function setupCCMDrive() {
  // Run once as the folder owner. Re-running preserves the existing token.
  const properties = PropertiesService.getScriptProperties();
  const token = properties.getProperty('CCM_DRIVE_API_TOKEN') ||
    (Utilities.getUuid() + Utilities.getUuid()).replace(/-/g, '');
  properties.setProperty('CCM_DRIVE_API_TOKEN', token);
  console.log('CCM_DRIVE_API_TOKEN=' + token);
  console.log('Folder: ' + DriveApp.getFolderById(CCM_DRIVE_FOLDER).getName());
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
    const token = PropertiesService.getScriptProperties().getProperty('CCM_DRIVE_API_TOKEN');
    if (!token || token.length < 32 || typeof data.token !== 'string') return reply({ok:false, code:'AUTH'});
    const expected = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, token);
    const supplied = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, data.token);
    let difference = 0;
    for (let i=0; i<expected.length; i++) difference |= expected[i] ^ supplied[i];
    if (difference) return reply({ok:false, code:'AUTH'});
    // Older CCM clients omit purpose. Event markers must never fall back to CCM.
    if ((data.purpose !== undefined && data.purpose !== 'ccm') ||
        Object.prototype.hasOwnProperty.call(data, 'event_key') ||
        Object.prototype.hasOwnProperty.call(data, 'filename')) {
      return reply({ok:false, code:'CCM_ONLY', message:'This endpoint accepts CCM photos and IDs only. Use the separate Event upload endpoint.'});
    }
    if (typeof data.record_key !== 'string' || !/^[a-f0-9]{32}$/.test(data.record_key) || !['photo','front','back'].includes(data.side) ||
        typeof data.image !== 'string' || data.image.length > 12*1024*1024) return reply({ok:false, code:'INPUT'});
    let bytes;
    try {
      bytes = Utilities.base64Decode(data.image);
    } catch (_) {
      return reply({ok:false, code:'IMAGE'});
    }
    const signature = [137,80,78,71,13,10,26,10];
    if (bytes.length > 8*1024*1024 || signature.some((b,i) => (bytes[i] & 255) !== b)) return reply({ok:false, code:'IMAGE'});
    const digest = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, bytes)
      .map(b => (b & 255).toString(16).padStart(2,'0')).join('').slice(0,16);
    const name = 'CCM-' + data.record_key + '-' + data.side + '-' + digest + '.png';
    lock = LockService.getScriptLock();
    lock.waitLock(25000);
    const folder = DriveApp.getFolderById(CCM_DRIVE_FOLDER);
    const files = folder.getFilesByName(name);
    let file = files.hasNext() ? files.next() : null;
    if (!file) {
      file = folder.createFile(Utilities.newBlob(bytes, 'image/png', name));
      file.setDescription('CCM:' + data.record_key + ':' + data.side + ':' + digest);
    }
    return reply({ok:true, file_id:file.getId(), name:name});
  } catch (error) {
    console.error('CCM Drive upload failed: ' + String(error && error.message || error));
    return reply({ok:false, code:'DRIVE'});
  } finally {
    if (lock && lock.hasLock()) lock.releaseLock();
  }
}
