(() => {
  'use strict';
  const $ = id => document.getElementById(id);
  const search = $('ccmSearch'), areaFilter = $('ccmFilterArea');
  function filterWorkers() {
    let count = 0;
    document.querySelectorAll('.ccm-worker-card').forEach(card => {
      card.hidden = !card.dataset.search.includes(search.value.trim().toLowerCase()) || (areaFilter.value && card.dataset.area !== areaFilter.value);
      if (!card.hidden) count++;
    });
    $('ccmNoWorkers').hidden = count !== 0;
  }
  search?.addEventListener('input', filterWorkers);
  areaFilter?.addEventListener('change', filterWorkers);
  $('ccmManageSearch')?.addEventListener('input', event => document.querySelectorAll('.ccm-account-row').forEach(row => { row.hidden = !row.dataset.search.includes(event.target.value.trim().toLowerCase()); }));
  document.querySelectorAll('.ccm-delete-form').forEach(form => form.addEventListener('submit', event => {
    if (!window.confirm('Delete this CCM account? Its public listing and QR will be deactivated now. The deletion will be sent to Google with your next backup.')) event.preventDefault();
  }));

  let backupRunning = false;
  const backupButton = $('ccmBackupButton');
  if (backupButton) {
    const status = $('ccmBackupStatus');
    async function postBackup(url) {
      const response = await fetch(url, {method:'POST', credentials:'same-origin', headers:{'X-CSRF-Token':backupButton.dataset.csrf}});
      if (!(response.headers.get('Content-Type') || '').includes('application/json')) throw new Error('Reload Admin and unlock it again, then retry backup. Your local accounts are kept.');
      const result = await response.json();
      if (!response.ok || !result.ok) throw new Error(result.error || 'Backup was not confirmed. Your local accounts are kept.');
      return result;
    }
    backupButton.addEventListener('click', async () => {
      if (backupRunning) return;
      backupRunning = true;
      backupButton.disabled = true;
      $('ccmRefreshGoogle').disabled = true;
      $('ccmBackupProgress').hidden = false;
      $('ccmBackupBar').value = 0;
      $('ccmBackupPercent').textContent = '0%';
      status.className = '';
      status.textContent = 'Preparing your backup collection…';
      let completed = 0, total = 0, remaining = Number($('ccmPendingCount').textContent);
      try {
        const queue = await postBackup(backupButton.dataset.url);
        total = queue.keys.length;
        remaining = total;
        $('ccmPendingCount').textContent = remaining;
        $('ccmBackupCount').textContent = '0 of ' + total + ' accounts backed up';
        for (let i=0; i<total; i++) {
          status.textContent = 'Backing up account ' + (i+1) + ' of ' + total + ': uploading pictures, verifying Google records and clearing confirmed local files…';
          const result = await postBackup(backupButton.dataset.url + '/' + encodeURIComponent(queue.keys[i]));
          remaining = result.remaining;
          $('ccmPendingCount').textContent = remaining;
          if (result.cleaned) {
            completed++;
            const row = document.querySelector('.ccm-account-row[data-key="' + queue.keys[i] + '"] .ccm-storage-status');
            if (row) row.textContent = 'Backed up to Google';
          }
          const percent = Math.floor(completed / total * 100);
          $('ccmBackupBar').value = percent;
          $('ccmBackupPercent').textContent = percent + '%';
          $('ccmBackupCount').textContent = completed + ' of ' + total + ' accounts backed up';
        }
        if (!total) { $('ccmBackupBar').value = 100; $('ccmBackupPercent').textContent = '100%'; }
        status.className = 'ccm-notice ' + (remaining ? '' : 'success');
        status.textContent = remaining ? completed + ' accounts backed up. ' + remaining + ' newer or remaining changes are saved locally; run backup again to include them.' : 'Backup complete. All accounts and pictures are confirmed in Google, and their local image files have been removed.';
      } catch (error) {
        status.className = 'ccm-notice error';
        status.textContent = completed + ' of ' + total + ' accounts backed up. ' + error.message + ' Unconfirmed accounts and local files remain available for retry.';
      } finally {
        backupRunning = false;
        backupButton.disabled = remaining === 0;
        $('ccmRefreshGoogle').disabled = false;
      }
    });
    window.addEventListener('beforeunload', event => { if (backupRunning) { event.preventDefault(); event.returnValue = ''; } });
  }

  const form = $('ccmWorkerForm');
  if (form) {
    const churches = JSON.parse($('ccmChurchData').textContent);
    const area = $('ccmArea'), church = $('ccmChurch'), photoInput = $('ccmPhoto');
    const areas = [...new Set(churches.map(c => c.area).filter(Boolean))].sort((a,b) => a.localeCompare(b, undefined, {numeric:true}));
    areas.forEach(value => area.add(new Option(value, value)));
    area.value = area.dataset.selected;
    function fillChurches(initial = false) {
      church.replaceChildren(new Option('Choose church', ''));
      churches.filter(c => c.area === area.value).forEach(c => {
        const option = new Option(c.address + ' — ' + c.pastor, c.key);
        option.selected = initial && c.church_id === church.dataset.churchId && c.address === church.dataset.address && c.pastor === church.dataset.pastor;
        church.add(option);
      });
      fillPastor();
    }
    function fillPastor() { $('ccmPastor').value = churches.find(c => c.key === church.value)?.pastor || ''; }
    area.addEventListener('change', () => fillChurches());
    church.addEventListener('change', fillPastor);
    fillChurches(true);
    let stream = null, cameraGeneration = 0, previewURL = null, busy = false, openingCamera = null;
    let preparedPhoto = null, photoGeneration = 0, photoTask = Promise.resolve(true);
    const video = $('ccmCamera');
    const cameraFacing = $('ccmCameraFacing');
    try { cameraFacing.value = localStorage.getItem('ccmCameraFacing') === 'environment' ? 'environment' : 'user'; } catch {}
    function showError(message) { $('ccmFormError').textContent = message; $('ccmFormError').hidden = !message; }
    function stopCamera() {
      cameraGeneration++;
      stream?.getTracks().forEach(track => track.stop());
      stream = null;
      video.srcObject = null;
      video.hidden = true;
      $('ccmCameraButtons').hidden = true;
      $('ccmOpenCamera').disabled = false;
      $('ccmPhotoPreview').hidden = !$('ccmPhotoPreview').getAttribute('src');
      $('ccmPhotoPlaceholder').hidden = !$('ccmPhotoPreview').hidden;
    }
    function showPhoto(file) {
      if (!file) return photoTask;
      const generation = ++photoGeneration;
      preparedPhoto = null;
      showError('');
      photoTask = (async () => {
        if (file.size > 40 * 1024 * 1024) throw new Error('Choose a photo smaller than 40 MB, or use Take photo.');
        stopCamera();
        $('ccmFormStatus').textContent = 'Resizing your photo for a faster upload…';
        const original = URL.createObjectURL(file);
        let image;
        try {
          image = new Image();
          image.src = original;
          await image.decode();
        } finally { URL.revokeObjectURL(original); }
        if (generation !== photoGeneration) return false;
        const scale = Math.min(1, 1400 / Math.max(image.naturalWidth, image.naturalHeight));
        const canvas = document.createElement('canvas');
        canvas.width = Math.max(1, Math.round(image.naturalWidth * scale));
        canvas.height = Math.max(1, Math.round(image.naturalHeight * scale));
        const context = canvas.getContext('2d');
        context.fillStyle = 'white'; context.fillRect(0, 0, canvas.width, canvas.height);
        context.drawImage(image, 0, 0, canvas.width, canvas.height);
        const blob = await new Promise(resolve => canvas.toBlob(resolve, 'image/jpeg', 0.87));
        if (!blob) throw new Error('This photo could not be prepared. Try another photo or use Take photo.');
        if (generation !== photoGeneration) return false;
        preparedPhoto = new File([blob], 'worker-photo.jpg', {type:'image/jpeg'});
        if (previewURL) URL.revokeObjectURL(previewURL);
        previewURL = URL.createObjectURL(blob);
        $('ccmPhotoPreview').src = previewURL;
        $('ccmPhotoPreview').hidden = false;
        $('ccmPhotoPlaceholder').hidden = true;
        $('ccmIDPreview').hidden = true;
        $('ccmFormStatus').textContent = 'Photo ready.';
        return true;
      })().catch(error => {
        if (generation === photoGeneration) {
          photoInput.value = '';
          showError(error.message.includes('decode') ? 'Your browser could not read this photo. Choose a JPEG, PNG or WebP image, or use Take photo.' : error.message);
          $('ccmFormStatus').textContent = '';
        }
        return false;
      });
      return photoTask;
    }
    photoInput.addEventListener('change', () => showPhoto(photoInput.files[0]));
    async function openCamera() {
      if (busy) return;
      showError('');
      if (!window.isSecureContext || !navigator.mediaDevices?.getUserMedia) { showError('The camera needs HTTPS or localhost. You can upload a photo instead.'); return; }
      stopCamera();
      const generation = cameraGeneration;
      const facing = cameraFacing.value;
      const previous = openingCamera;
      $('ccmOpenCamera').disabled = true;
      const pending = (async () => {
        if (previous) { try { await previous; } catch {} }
        if (generation !== cameraGeneration) return null;
        return navigator.mediaDevices.getUserMedia({video: {facingMode:{ideal:facing}, width:{ideal:1400}, height:{ideal:1400}}, audio:false});
      })();
      openingCamera = pending;
      try {
        const next = await pending;
        if (!next) return;
        if (generation !== cameraGeneration) { next.getTracks().forEach(t => t.stop()); return; }
        stream = next;
        const actualFacing = stream.getVideoTracks()[0]?.getSettings?.().facingMode || facing;
        video.classList.toggle('ccm-camera-front', actualFacing === 'user');
        video.srcObject = stream;
        video.hidden = false;
        $('ccmPhotoPreview').hidden = true;
        $('ccmPhotoPlaceholder').hidden = true;
        $('ccmCameraButtons').hidden = false;
        await video.play();
      } catch (error) {
        if (generation !== cameraGeneration) return;
        stopCamera();
        showError('The camera could not open. Allow camera access or upload a photo.');
      } finally { if (openingCamera === pending) openingCamera = null; }
    }
    $('ccmOpenCamera').addEventListener('click', openCamera);
    cameraFacing.addEventListener('change', () => {
      try { localStorage.setItem('ccmCameraFacing', cameraFacing.value); } catch {}
      if (stream || openingCamera) openCamera();
    });
    $('ccmCloseCamera').addEventListener('click', stopCamera);
    $('ccmCapture').addEventListener('click', () => {
      if (!video.videoWidth || !stream) return;
      const canvas = $('ccmPhotoCanvas');
      const length = Math.min(video.videoWidth, video.videoHeight);
      canvas.width = canvas.height = Math.min(length, 1400);
      canvas.getContext('2d').drawImage(video, (video.videoWidth-length)/2, (video.videoHeight-length)/2, length, length, 0, 0, canvas.width, canvas.height);
      canvas.toBlob(blob => {
        if (!blob) { showError('The photo could not be captured. Please try again.'); return; }
        const file = new File([blob], 'worker-photo.png', {type:'image/png'});
        showPhoto(file);
      }, 'image/png');
    });
    async function send(preview) {
      if (busy || !form.reportValidity()) return;
      if (backupRunning) { showError('Wait for this backup to finish before leaving the page to create an account.'); return; }
      busy = true; stopCamera(); showError('');
      $('ccmSaveButton').disabled = $('ccmPreviewButton').disabled = true;
      try {
        if (!await photoTask) return;
        if (!preparedPhoto && !form.elements.record_key.value) { showError('Take or upload a worker photo.'); return; }
        $('ccmFormStatus').textContent = preview ? 'Preparing ID preview…' : 'Saving the account and ID locally…';
        const data = new FormData(form);
        if (preparedPhoto) data.set('photo', preparedPhoto, preparedPhoto.name);
        const response = await fetch(preview ? form.dataset.previewUrl : form.action, {method:'POST', body:data, credentials:'same-origin'});
        const type = response.headers.get('Content-Type') || '';
        if (!type.includes('application/json')) throw new Error(response.status === 403 ? 'Your admin session expired. Open Admin in another tab to unlock it, then retry this form.' : 'The server could not confirm this request. Retry this same form.');
        const result = await response.json();
        if (!response.ok || !result.ok) throw new Error(result.error || 'The request failed. Please retry this same form.');
        if (preview) {
          $('ccmPreviewBack').src = result.images.back;
          $('ccmPreviewFront').src = result.images.front;
          $('ccmIDPreview').hidden = false;
          $('ccmIDPreview').scrollIntoView({behavior:'smooth',block:'start'});
          $('ccmFormStatus').textContent = 'Preview only. Create the account to save it locally.';
        } else { window.location.assign(result.redirect); }
      } catch (error) { showError(error.message); $('ccmFormStatus').textContent = ''; }
      finally { busy = false; $('ccmSaveButton').disabled = $('ccmPreviewButton').disabled = false; }
    }
    $('ccmPreviewButton').addEventListener('click', () => send(true));
    form.addEventListener('submit', event => { event.preventDefault(); send(false); });
    form.addEventListener('input', event => { if (event.target !== photoInput) $('ccmIDPreview').hidden = true; });
    window.addEventListener('pagehide', () => { stopCamera(); if (previewURL) URL.revokeObjectURL(previewURL); });
  }

  const printImage = $('ccmPrintImage');
  if (printImage) {
    document.body.classList.add('ccm-id-print');
    let side = 'back', printRequested = false;
    const printButton = $('ccmPrintNow');
    function imageReady() { printButton.disabled = !printImage.complete || !printImage.naturalWidth; }
    printImage.addEventListener('load', imageReady);
    printImage.addEventListener('error', () => { printButton.disabled = true; $('ccmPrintMessage').textContent = 'The ID image could not load. Reload the page before printing.'; });
    function setSide(next) {
      side = next; printRequested = false;
      printImage.src = printImage.dataset[side];
      printImage.alt = (side === 'back' ? 'Back' : 'Front') + ' of worker ID';
      $('ccmPrintStep').textContent = side === 'back' ? 'Step 1 of 2 · Back' : 'Step 2 of 2 · Front';
      printButton.textContent = 'Print ' + side;
      $('ccmPrintNext').hidden = side === 'front';
      $('ccmPrintPrevious').hidden = side === 'back';
      $('ccmPrintMessage').textContent = side === 'back' ? 'The back is ready. You can print it again before continuing.' : 'The front is ready. Check how the card is loaded before printing.';
      imageReady();
    }
    $('ccmPrintNext').addEventListener('click', () => setSide('front'));
    $('ccmPrintPrevious').addEventListener('click', () => setSide('back'));
    printButton.addEventListener('click', async () => {
      if (!printImage.complete || !printImage.naturalWidth) return;
      try { await printImage.decode(); } catch { return; }
      printRequested = true; window.print();
    });
    window.addEventListener('afterprint', () => {
      if (!printRequested) return;
      printButton.textContent = 'Print ' + side + ' again';
      $('ccmPrintMessage').textContent = 'Check the printed ' + side + '. You can print it again' + (side === 'back' ? ' or choose Next: front.' : ' or download either side below.');
    });
    $('ccmPrintWidth').addEventListener('input', event => {
      const width = Number(event.target.value);
      if (width >= 30 && width <= 180) {
        $('ccmPrintable').style.setProperty('--ccm-card-width', width + 'mm');
        $('ccmPrintHeight').textContent = (width * 1500 / 1050).toFixed(1);
      }
    });
    imageReady();
  }
})();
