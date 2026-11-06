(() => {
  'use strict';
  const $ = id => document.getElementById(id);
  const shell = document.querySelector('.event-shell');
  if (!shell) return;
  document.body.classList.add('event-page');
  const csrf = shell.dataset.csrf;
  const normalize = value => String(value || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLocaleLowerCase();
  const collator = new Intl.Collator(undefined, {numeric:true, sensitivity:'base'});
  function error(id, message) {
    const target = $(id);
    target.textContent = message;
    target.hidden = !message;
  }
  async function post(url, data = new FormData()) {
    const response = await fetch(url, {method:'POST', body:data, credentials:'same-origin', headers:{'X-CSRF-Token':csrf}});
    if (!(response.headers.get('Content-Type') || '').includes('application/json')) {
      throw new Error(response.status === 403 ? 'Your access expired. Reload the page and unlock Admin if needed.' : 'The request was not confirmed. Reload this page and try again.');
    }
    const result = await response.json();
    if (!response.ok || !result.ok) throw new Error(result.error || 'The request could not be completed. Please retry.');
    return result;
  }
  function formData(values) {
    const data = new FormData();
    Object.entries(values).forEach(([key,value]) => data.set(key, String(value)));
    return data;
  }
  function confirmStatus(message, options = {}) {
    const dialog = $('eventConfirmDialog');
    dialog.querySelector('h2').textContent = options.title || 'Change status?';
    $('eventConfirmYes').textContent = options.button || 'Yes, change status';
    $('eventConfirmYes').classList.toggle('danger',!!options.danger);
    $('eventConfirmMessage').textContent = message;
    dialog.returnValue = '';
    return new Promise(resolve => {
      dialog.addEventListener('close', () => resolve(dialog.returnValue === 'yes'), {once:true});
      dialog.showModal();
    });
  }
  $('eventConfirmYes').addEventListener('click', () => $('eventConfirmDialog').close('yes'));
  $('eventConfirmCancel').addEventListener('click', () => $('eventConfirmDialog').close('cancel'));

  const registration = $('eventRegistrationForm');
  if (registration) {
    const churches = JSON.parse($('eventChurchData').textContent);
    const area = $('eventArea'), church = $('eventChurch'), newChurch = $('eventNewChurch');
    const manual = $('eventManualChurch'), address = $('eventAddress'), pastor = $('eventPastor');
    area.value = area.dataset.selected;
    function fillDetails() {
      if (newChurch.checked) return;
      const selected = churches.find(c => c.key === church.value);
      address.value = selected?.address || '';
      pastor.value = selected?.pastor || '';
    }
    function fillChurches(initial = false) {
      church.replaceChildren(new Option('Choose church', ''));
      churches.filter(c => c.area === area.value).forEach(c => {
        const option = new Option(c.church_id + ' · ' + c.pastor, c.key);
        option.selected = initial && c.church_id === church.dataset.name && c.address === church.dataset.address && c.pastor === church.dataset.pastor;
        church.add(option);
      });
      fillDetails();
    }
    function mode() {
      const isNew = newChurch.checked;
      $('eventChurchSelectLabel').hidden = isNew;
      church.disabled = isNew;
      church.required = !isNew;
      $('eventManualChurchLabel').hidden = !isNew;
      manual.disabled = !isNew;
      manual.required = isNew;
      address.readOnly = pastor.readOnly = !isNew;
      address.required = pastor.required = isNew;
      fillDetails();
    }
    fillChurches(true);
    mode();
    area.addEventListener('change', () => fillChurches());
    church.addEventListener('change', fillDetails);
    newChurch.addEventListener('change', mode);
    let busy = false;
    registration.addEventListener('submit', async event => {
      event.preventDefault();
      if (busy || !registration.reportValidity()) return;
      busy = true;
      const button = registration.querySelector('[type=submit]');
      button.disabled = true;
      error('eventRegistrationError','');
      try {
        const result = await post(registration.action, new FormData(registration));
        window.location.assign(result.redirect);
      } catch (failure) { error('eventRegistrationError',failure.message); }
      finally { busy = false; button.disabled = false; }
    });
  }

  const cameraForm = $('eventPhotoForm');
  if (cameraForm) {
    const video = $('eventCamera'), facing = $('eventCameraFacing'), preview = $('eventCapturedPhoto');
    let stream = null, generation = 0, captured = null, capturedURL = null, busy = false, opening = false;
    try { facing.value = localStorage.getItem('eventCameraFacing') === 'environment' ? 'environment' : 'user'; } catch {}
    function stop() {
      generation++;
      stream?.getTracks().forEach(track => track.stop());
      stream = null;
      video.srcObject = null;
      video.hidden = true;
      $('eventCameraCapture').hidden = true;
    }
    async function start() {
      stop();
      const current = generation;
      opening = true;
      $('eventCameraOpen').disabled = $('eventCameraRetake').disabled = true;
      error('eventCameraError','');
      try {
        if (!navigator.mediaDevices?.getUserMedia) throw new Error('A live camera needs HTTPS or localhost. Open the secure website on a device with a camera.');
        const candidate = await navigator.mediaDevices.getUserMedia({audio:false, video:{facingMode:{ideal:facing.value},width:{ideal:1400},height:{ideal:1400}}});
        if (current !== generation) { candidate.getTracks().forEach(track => track.stop()); return; }
        stream = candidate;
        video.srcObject = stream;
        await video.play();
        if (current !== generation) return;
        video.classList.toggle('front', facing.value === 'user');
        video.hidden = false;
        preview.hidden = true;
        $('eventCameraPlaceholder').hidden = true;
        $('eventCameraCapture').hidden = false;
        $('eventCameraOpen').hidden = true;
        $('eventCameraRetake').hidden = true;
        captured = null;
        $('eventPhotoSave').disabled = true;
      } catch (failure) {
        if (current !== generation) return;
        stop();
        const messages = {
          NotAllowedError:'Camera permission was blocked. Allow camera access in your browser settings, then try again.',
          NotFoundError:'No camera was found. Please use a device with a working camera.',
          NotReadableError:'The camera is busy. Close other apps using it, then try again.'
        };
        error('eventCameraError',messages[failure.name] || failure.message || 'The camera could not start. Please retry.');
        $('eventCameraOpen').hidden = false;
        $('eventCameraPlaceholder').hidden = !!captured;
        preview.hidden = !captured;
      } finally {
        opening = false;
        $('eventCameraOpen').disabled = $('eventCameraRetake').disabled = false;
      }
    }
    $('eventCameraOpen').addEventListener('click', () => { if (!opening) start(); });
    $('eventCameraRetake').addEventListener('click', () => { if (!opening) start(); });
    facing.addEventListener('change', () => {
      try { localStorage.setItem('eventCameraFacing',facing.value); } catch {}
      if (stream || opening) start();
    });
    $('eventCameraCapture').addEventListener('click', () => {
      if (!video.videoWidth || !video.videoHeight || busy) return;
      const canvas = $('eventCameraCanvas'), size = Math.min(video.videoWidth,video.videoHeight);
      canvas.width = canvas.height = Math.min(1400,size);
      canvas.getContext('2d').drawImage(video,(video.videoWidth-size)/2,(video.videoHeight-size)/2,size,size,0,0,canvas.width,canvas.height);
      const current = generation;
      canvas.toBlob(blob => {
        if (current !== generation) return;
        if (!blob) { error('eventCameraError','The picture could not be captured. Please try again.'); return; }
        captured = blob;
        if (capturedURL) URL.revokeObjectURL(capturedURL);
        capturedURL = URL.createObjectURL(blob);
        preview.src = capturedURL;
        preview.hidden = false;
        stop();
        $('eventCameraRetake').hidden = false;
        $('eventPhotoSave').disabled = false;
      },'image/png');
    });
    cameraForm.addEventListener('submit', async event => {
      event.preventDefault();
      if (busy || !captured) return;
      busy = true;
      $('eventPhotoSave').disabled = true;
      error('eventCameraError','');
      const data = new FormData(cameraForm);
      data.set('camera_capture','1');
      data.set('photo',captured,'camera-photo.png');
      try { const result = await post(cameraForm.action,data); window.location.assign(result.redirect); }
      catch (failure) { error('eventCameraError',failure.message); }
      finally { busy = false; $('eventPhotoSave').disabled = false; }
    });
    window.addEventListener('pagehide', () => { stop(); if (capturedURL) URL.revokeObjectURL(capturedURL); });
  }

  const photoBackup = $('eventPhotoBackupStatus');
  if (photoBackup) {
    let busy = false;
    async function backupPhoto() {
      if (busy) return;
      busy = true;
      $('eventPhotoBackupRetry').disabled = true;
      photoBackup.textContent = 'Your registration is saved. Backing up the photo to Google Drive…';
      try {
        await post(photoBackup.dataset.url);
        photoBackup.textContent = 'Photo backed up to Google Drive.';
        $('eventPhotoBackupRetry').hidden = true;
      } catch (failure) {
        photoBackup.textContent = failure.message;
        $('eventPhotoBackupRetry').hidden = false;
      } finally { busy = false; $('eventPhotoBackupRetry').disabled = false; }
    }
    $('eventPhotoBackupRetry').addEventListener('click',backupPhoto);
    if (photoBackup.dataset.needed === '1') backupPhoto();
  }

  const rows = [...document.querySelectorAll('[data-attendee-row]')];
  function pendingDeletions(count) {
    if (!$('eventPendingDeletionCount')) return;
    $('eventPendingDeletionCount').textContent = count;
    $('eventPendingDeletionNote').hidden = !count;
    if ($('eventBackupButton')) $('eventBackupButton').disabled = !rows.length && !count;
  }
  if ($('eventSearch')) {
    function filter() {
      const query = normalize($('eventSearch').value.trim()), area = $('eventFilterArea').value;
      let count = 0;
      rows.forEach(row => { row.hidden = !normalize(row.dataset.search).includes(query) || !!(area && row.dataset.area !== area); if (!row.hidden) count++; });
      $('eventResultCount').textContent = count;
      $('eventNoResults').hidden = count !== 0;
    }
    $('eventSearch').addEventListener('input',filter);
    $('eventFilterArea').addEventListener('change',filter);
    $('eventSort').addEventListener('change', () => {
      const field = $('eventSort').value;
      [...rows].sort((a,b) => collator.compare(a.dataset[field],b.dataset[field]) || Number(a.dataset.number)-Number(b.dataset.number)).forEach(row => $('eventAttendeeRows').append(row));
    });
    rows.forEach(row => row.querySelectorAll('.event-status').forEach(button => button.addEventListener('click',async () => {
      const field = button.dataset.field, enabled = button.dataset.value === '1';
      if (enabled && !await confirmStatus(field === 'paid' ? 'Change this attendee back to not paid?' : 'Change this attendee back to not released?')) return;
      const controls = row.querySelectorAll('button');
      controls.forEach(control => { control.disabled = true; });
      error('eventGlobalError','');
      try {
        const result = await post(row.dataset.statusUrl,formData({field,value:enabled?0:1,confirm:enabled?'yes':'',revision:row.dataset.revision}));
        row.dataset.revision = result.revision;
        button.dataset.value = enabled ? '0':'1';
        button.classList.toggle('on',!enabled);
        button.setAttribute('aria-pressed',String(!enabled));
        button.textContent = field === 'paid' ? (enabled?'Paid':'✓ Paid') : (enabled?'Release':'✓ Released');
      } catch (failure) { error('eventGlobalError',failure.message); }
      finally { controls.forEach(control => { control.disabled = false; }); }
    })));
    rows.forEach(row => row.querySelector('.event-delete-attendee').addEventListener('click',async () => {
      const accepted = await confirmStatus('Delete '+row.dataset.name+' from this event? The registration and QR will be removed now. Google Sheets removal will be included in the next submission.',
        {title:'Delete registration?',button:'Yes, delete registration',danger:true});
      if (!accepted) return;
      const controls = row.querySelectorAll('button');
      controls.forEach(control => { control.disabled = true; });
      error('eventGlobalError','');
      try {
        const result = await post(row.dataset.deleteUrl,formData({confirm:'yes',revision:row.dataset.revision}));
        rows.splice(rows.indexOf(row),1);
        row.remove();
        filter();
        pendingDeletions(result.pending_deletions);
      } catch (failure) { error('eventGlobalError',failure.message); }
      finally { controls.forEach(control => { control.disabled = false; }); }
    }));
  }
  $('eventAdminSelect')?.addEventListener('change',event => window.location.assign(event.target.value));
  let backupRunning = false;
  const backupButton = $('eventBackupButton');
  backupButton?.addEventListener('click',async () => {
    if (backupRunning) return;
    backupRunning = true;
    backupButton.disabled = true;
    $('eventAdminSelect').disabled = true;
    const progress = $('eventBackupProgress'), status = $('eventBackupStatus');
    progress.hidden = false;
    progress.value = 0;
    let completed = 0, total = 0, removed = 0;
    try {
      status.textContent = 'Preparing local registrations…';
      const queue = await post(backupButton.dataset.url,formData({event_id:backupButton.dataset.event}));
      total = queue.keys.length;
      for (const key of queue.keys) {
        status.textContent = 'Synchronizing registration '+(completed+1)+' of '+total+' with Google Sheets…';
        const result = await post(backupButton.dataset.url+'/'+encodeURIComponent(key));
        completed++;
        if (result.removed) removed++;
        progress.value = Math.floor(completed/total*100);
        pendingDeletions(result.pending_deletions);
      }
      progress.value = 100;
      status.textContent = completed+' updates synchronized with Google Sheets.'+(removed?' '+removed+' deleted registration'+(removed===1?'':'s')+' removed.':'');
    } catch (failure) { status.textContent = completed+' of '+total+' submitted. '+failure.message; }
    finally { backupRunning = false; backupButton.disabled = !rows.length && !Number($('eventPendingDeletionCount').textContent); $('eventAdminSelect').disabled = false; }
  });
  $('eventDeleteEventButton')?.addEventListener('click',async event => {
    if (backupRunning) { error('eventGlobalError','Wait for the Google submission to finish before deleting this event.'); return; }
    const button = event.currentTarget;
    const accepted = await confirmStatus('Delete '+button.dataset.name+' and all its registrations? Registration for this event will close, its QR codes will stop working, and Google row removal will be queued.',
      {title:'Delete event?',button:'Yes, delete event',danger:true});
    if (!accepted) return;
    button.disabled = true;
    try {
      const result = await post(button.dataset.url,formData({confirm:'yes',updated_at:button.dataset.updated}));
      window.location.assign(result.redirect);
    } catch (failure) { error('eventGlobalError',failure.message); button.disabled = false; }
  });
  window.addEventListener('beforeunload',event => { if (backupRunning) { event.preventDefault(); event.returnValue = ''; } });

  const configForm = $('eventConfigForm');
  if (configForm) {
    const dialog = $('eventCreateDialog');
    let busy = false, previewURL = null;
    function openConfig(editing) {
      if (backupRunning) return;
      configForm.reset();
      const config = editing ? JSON.parse($('eventSelectedConfig').textContent) : null;
      configForm.elements.event_id.value = config?.id || '';
      for (const field of ['kind','start_date','end_date','theme','scripture']) if (config) configForm.elements[field].value = config[field];
      configForm.elements.background.required = !editing;
      configForm.elements.end_date.min = config?.start_date || '';
      $('eventDialogTitle').textContent = editing?'Edit Event':'Create Event';
      $('eventConfigSave').textContent = editing?'Save event':'Create Event';
      $('eventActivationNote').hidden = editing;
      $('eventConfigPreview').hidden = true;
      error('eventConfigError','');
      dialog.showModal();
    }
    $('eventCreateButton').addEventListener('click',() => openConfig(false));
    $('eventEditButton')?.addEventListener('click',() => openConfig(true));
    dialog.querySelectorAll('[data-close-dialog]').forEach(button => button.addEventListener('click',() => { if (!busy) dialog.close(); }));
    dialog.addEventListener('cancel',event => { if (busy) event.preventDefault(); });
    configForm.elements.start_date.addEventListener('change',() => { configForm.elements.end_date.min = configForm.elements.start_date.value; });
    configForm.addEventListener('input',() => { $('eventConfigPreview').hidden = true; $('eventPreviewHint').textContent = 'Preview again to see the updated layout.'; });
    async function sendConfig(preview) {
      if (busy || !configForm.reportValidity()) return;
      busy = true;
      $('eventPreviewButton').disabled = $('eventConfigSave').disabled = true;
      error('eventConfigError','');
      try {
        const data = new FormData(configForm);
        if (preview) {
          const response = await fetch(configForm.dataset.previewUrl,{method:'POST',body:data,credentials:'same-origin',headers:{'X-CSRF-Token':csrf}});
          if (!response.ok || !(response.headers.get('Content-Type') || '').includes('image/png')) {
            const result = (response.headers.get('Content-Type') || '').includes('application/json') ? await response.json() : {};
            throw new Error(result.error || 'Preview could not load. Reload Admin and retry.');
          }
          const blob = await response.blob();
          if (previewURL) URL.revokeObjectURL(previewURL);
          previewURL = URL.createObjectURL(blob);
          $('eventConfigPreview').src = previewURL;
          $('eventConfigPreview').hidden = false;
          $('eventPreviewHint').textContent = 'Sample attendee. The actual ID uses the captured portrait and attendee details.';
        } else { const result = await post(configForm.action,data); window.location.assign(result.redirect); }
      } catch (failure) { error('eventConfigError',failure.message); }
      finally { busy = false; $('eventPreviewButton').disabled = $('eventConfigSave').disabled = false; }
    }
    $('eventPreviewButton').addEventListener('click',() => sendConfig(true));
    configForm.addEventListener('submit',event => { event.preventDefault(); sendConfig(false); });
    window.addEventListener('pagehide',() => { if (previewURL) URL.revokeObjectURL(previewURL); });
  }

  const printable = $('eventPrintable');
  if (printable) {
    document.body.classList.add('event-id-print');
    function ready() { $('eventPrintNow').disabled = !printable.complete || !printable.naturalWidth; }
    printable.addEventListener('load',ready);
    printable.addEventListener('error',() => { ready(); error('eventGlobalError','The ID image could not load. Reload before printing.'); });
    ready();
    $('eventPrintNow').addEventListener('click',() => {
      if (!printable.complete || !printable.naturalWidth) return;
      window.print();
      $('eventPrintStatus').textContent = 'Check the box after confirming the ID printed successfully.';
    });
    $('eventPrintWidth').addEventListener('input',event => {
      const width = Number(event.target.value);
      if (width >= 30 && width <= 180) printable.parentElement.style.setProperty('--event-card-width',width+'mm');
    });
    $('eventDownloadID').addEventListener('click',async event => {
      event.preventDefault();
      const button = event.currentTarget;
      if (button.dataset.busy) return;
      button.dataset.busy = '1';
      try {
        const response = await fetch(button.href,{credentials:'same-origin'});
        if (!response.ok || !(response.headers.get('Content-Type') || '').includes('image/png')) throw new Error('The download could not be prepared. Reload Admin and retry.');
        const url = URL.createObjectURL(await response.blob()), link = document.createElement('a');
        link.href = url;
        link.download = 'Event-ID.png';
        document.body.append(link);
        link.click();
        link.remove();
        setTimeout(() => URL.revokeObjectURL(url),30000);
        $('eventPrintStatus').textContent = 'Download prepared. Check the box after confirming it was saved successfully.';
      } catch (failure) { error('eventGlobalError',failure.message); }
      finally { delete button.dataset.busy; }
    });
    const checkbox = $('eventPrintComplete');
    checkbox.addEventListener('change',async () => {
      const value = checkbox.checked;
      checkbox.disabled = true;
      try {
        const result = await post(checkbox.dataset.statusUrl,formData({field:'printed',value:value?1:0,revision:checkbox.dataset.revision}));
        checkbox.dataset.revision = result.revision;
        $('eventPrintStatus').textContent = value?'Successful print or download recorded.':'Completion mark cleared.';
      } catch (failure) { checkbox.checked = !value; $('eventPrintStatus').textContent = failure.message; }
      finally { checkbox.disabled = false; }
    });
  }
})();
