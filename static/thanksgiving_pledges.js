(() => {
  'use strict';

  const $ = id => document.getElementById(id);

  const hideGlobalLoading = () => {
    const overlay = document.getElementById('globalLoadingOverlay');
    if (!overlay) return;
    overlay.classList.remove('show');
    overlay.setAttribute('aria-hidden', 'true');
  };

  const showPledgeLoading = message => {
    if (typeof window.showGlobalLoading === 'function') {
      window.showGlobalLoading(message);
      return;
    }

    const overlay = document.getElementById('globalLoadingOverlay');
    const title = document.getElementById('globalLoadingTitle');
    if (!overlay) return;
    if (title) title.textContent = message || 'Please wait...';
    overlay.classList.add('show');
    overlay.setAttribute('aria-hidden', 'false');
  };

  const form = $('pledgeForm');
  const dialog = $('pledgeDialog');
  const camera = $('pledgeCameraDialog');
  const accounts = JSON.parse($('pledgeAccounts').textContent);
  const area = $('pledgeArea');
  const church = $('pledgeChurch');

  let stream = null;
  let photo = null;
  let busy = false;
  let cameraStarting = false;
  let cameraRequestId = 0;
  let cameraOpenChain = Promise.resolve();

  const error = (id, message) => {
    const el = $(id);
    el.textContent = message;
    el.hidden = !message;
  };

  const stopStream = () => {
    if (stream) {
      stream.getTracks().forEach(track => track.stop());
    }
    stream = null;

    const video = $('pledgeVideo');
    if (video) {
      video.pause();
      video.srcObject = null;
    }
  };

  const cancelCameraRequest = () => {
    cameraRequestId += 1;
    cameraStarting = false;
    stopStream();
  };

  const setCameraButtonsDisabled = disabled => {
    ['pledgeStartCamera', 'pledgeRetake'].forEach(id => {
      const button = $(id);
      if (button) button.disabled = disabled;
    });
  };

  const cameraErrorMessage = err => {
    switch (err?.name) {
      case 'NotAllowedError':
      case 'SecurityError':
        return 'Camera permission was blocked. Allow camera access for this website in your browser settings, then click Open Camera again.';
      case 'NotFoundError':
      case 'DevicesNotFoundError':
        return 'No camera was found on this device.';
      case 'NotReadableError':
      case 'TrackStartError':
        return 'The camera is busy or unavailable. Close other apps using the camera, then try again.';
      case 'OverconstrainedError':
      case 'ConstraintNotSatisfiedError':
        return 'The camera could not start with the requested settings. Please try again.';
      default:
        return 'Camera access is unavailable. Check camera permission, close other camera apps, then click Open Camera again.';
    }
  };

  document.querySelectorAll('[data-unix]').forEach(t => {
    t.textContent = new Date(Number(t.dataset.unix) * 1000).toLocaleString('en-PH', {
      timeZone: 'Asia/Manila'
    });
  });

  // ---------------------------------------------------------
  // Public pledge filters: instant, client-side, no buttons.
  // ---------------------------------------------------------
  const filterArea = $('pledgeFilterArea');
  const filterSearch = $('pledgeFilterSearch');
  const filterSummary = $('pledgeFilterSummary');
  const noResults = $('pledgeNoResults');
  const pledgeRows = [...document.querySelectorAll('[data-pledge-row]')];

  const normalize = value =>
    String(value || '')
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .toLocaleLowerCase();

  const money = value =>
    new Intl.NumberFormat('en-PH', {
      style: 'currency',
      currency: 'PHP',
      minimumFractionDigits: 2,
      maximumFractionDigits: 2
    }).format(value);

  const applyFilters = () => {
    if (!filterArea || !filterSearch) return;

    const selectedArea = filterArea.value.trim();
    const query = normalize(filterSearch.value.trim());

    let visibleCount = 0;
    let subtotal = 0;

    pledgeRows.forEach(row => {
      const areaMatches = !selectedArea || row.dataset.area === selectedArea;
      const searchMatches = !query || normalize(row.dataset.search).includes(query);
      const visible = areaMatches && searchMatches;

      row.hidden = !visible;

      if (visible) {
        visibleCount += 1;
        const cash = Number(row.dataset.cash || 0);
        if (Number.isFinite(cash)) subtotal += cash;
      }
    });

    if (noResults) {
      noResults.hidden = visibleCount !== 0;
    }

    const hasFilter = Boolean(selectedArea || query);
    if (filterSummary) {
      filterSummary.hidden = !hasFilter;
      if (hasFilter) {
        filterSummary.innerHTML =
          `${visibleCount} matching entr${visibleCount === 1 ? 'y' : 'ies'} · ` +
          `Cash subtotal: <strong>${money(subtotal)}</strong>`;
      }
    }

    // Keep the current filters in the URL without reloading the page.
    const url = new URL(window.location.href);
    if (selectedArea) {
      url.searchParams.set('area', selectedArea);
    } else {
      url.searchParams.delete('area');
    }

    const rawQuery = filterSearch.value.trim();
    if (rawQuery) {
      url.searchParams.set('q', rawQuery);
    } else {
      url.searchParams.delete('q');
    }

    window.history.replaceState({}, '', `${url.pathname}${url.search}${url.hash}`);
  };

  filterArea?.addEventListener('change', applyFilters);
  filterSearch?.addEventListener('input', applyFilters);
  applyFilters();

  // ---------------------------------------------------------
  // Pastor/account selectors.
  // ---------------------------------------------------------
  [...new Set(accounts.map(a => a.area))]
    .sort((a, b) => a.localeCompare(b, undefined, { numeric: true }))
    .forEach(a => area.add(new Option(`Area ${a}`, a)));

  area.addEventListener('change', () => {
    church.replaceChildren(new Option('Select your church', ''));
    $('pledgePastorName').value = '';

    accounts
      .filter(a => a.area === area.value)
      .forEach(a => church.add(new Option(`${a.church} — ${a.name}`, a.key)));
  });

  church.addEventListener('change', () => {
    $('pledgePastorName').value =
      accounts.find(a => a.key === church.value)?.name || '';
  });

  form.querySelectorAll('[name=kind]').forEach(radio => {
    radio.addEventListener('change', () => {
      const pastor = form.elements.kind.value === 'Pastor';

      $('pledgePastorFields').hidden = !pastor;
      $('pledgeOtherFields').hidden = pastor;

      area.disabled = church.disabled = !pastor;
      area.required = church.required = pastor;

      ['name', 'church'].forEach(name => {
        form.elements[name].disabled = pastor;
        form.elements[name].required = !pastor;
      });
    });
  });

  // ---------------------------------------------------------
  // Pledge form dialog.
  // ---------------------------------------------------------
  $('pledgeOpen').addEventListener('click', () => dialog.showModal());

  document.querySelectorAll('[data-close]').forEach(button => {
    button.addEventListener('click', () => $(button.dataset.close).close());
  });

  const showCapturedPhotoState = () => {
    const video = $('pledgeVideo');
    const preview = $('pledgePhotoPreview');

    video.hidden = true;
    $('pledgeCapture').hidden = true;

    if (photo) {
      preview.hidden = false;
      $('pledgeStartCamera').hidden = true;
      $('pledgeRetake').hidden = false;
      $('pledgeConfirm').hidden = false;
    } else {
      preview.hidden = true;
      $('pledgeStartCamera').hidden = false;
      $('pledgeRetake').hidden = true;
      $('pledgeConfirm').hidden = true;
    }
  };

  form.addEventListener('submit', event => {
    event.preventDefault();
    error('pledgeFormError', '');

    if (!form.reportValidity()) return;

    if (
      !(Number(form.elements.cash.value) > 0) &&
      !form.elements.livestock.value.trim() &&
      !form.elements.goods.value.trim()
    ) {
      error('pledgeFormError', 'Please complete at least one pledge category.');
      return;
    }

    cancelCameraRequest();
    error('pledgeCameraError', '');
    $('pledgeSaveStatus').textContent = '';
    setCameraButtonsDisabled(false);
    showCapturedPhotoState();

    dialog.close();
    camera.showModal();
  });

  const back = () => {
    if (busy) return;

    cancelCameraRequest();
    error('pledgeCameraError', '');
    $('pledgeSaveStatus').textContent = '';

    camera.close();
    dialog.showModal();
  };

  $('pledgeCameraClose').addEventListener('click', back);

  camera.addEventListener('cancel', event => {
    event.preventDefault();
    back();
  });

  // ---------------------------------------------------------
  // Camera: only one camera-opening request may own the UI.
  // An older request is discarded if the dialog is closed or
  // another request starts before it finishes.
  // ---------------------------------------------------------
  async function startCamera() {
    if (busy || cameraStarting) return;

    error('pledgeCameraError', '');

    if (!window.isSecureContext || !navigator.mediaDevices?.getUserMedia) {
      error(
        'pledgeCameraError',
        'The camera needs HTTPS (or localhost). Open the secure website in your browser. Your form answers are still here.'
      );
      return;
    }

    const requestId = ++cameraRequestId;
    cameraStarting = true;
    setCameraButtonsDisabled(true);
    $('pledgeSaveStatus').textContent = 'Opening camera…';

    stopStream();

    let newStream = null;

    try {
      // Serialize getUserMedia calls. If a previous camera-open request is
      // still waiting on browser permission/device release, this one waits
      // for it to finish instead of opening a second overlapping request.
      const openPromise = cameraOpenChain
        .catch(() => undefined)
        .then(async () => {
          if (requestId !== cameraRequestId || !camera.open) {
            return null;
          }

          return navigator.mediaDevices.getUserMedia({
            video: {
              facingMode: 'user',
              width: { ideal: 800 },
              height: { ideal: 800 }
            },
            audio: false
          });
        });

      cameraOpenChain = openPromise.then(
        () => undefined,
        () => undefined
      );

      newStream = await openPromise;

      if (!newStream) {
        return;
      }

      // This request became stale while permission/device access was pending.
      if (requestId !== cameraRequestId || !camera.open) {
        newStream.getTracks().forEach(track => track.stop());
        return;
      }

      stream = newStream;

      const video = $('pledgeVideo');
      video.srcObject = stream;
      video.hidden = false;

      await video.play();

      if (requestId !== cameraRequestId || !camera.open) {
        stopStream();
        return;
      }

      // The new camera is now active; discard the old captured photo.
      photo = null;
      const preview = $('pledgePhotoPreview');
      preview.hidden = true;

      $('pledgeStartCamera').hidden = true;
      $('pledgeRetake').hidden = true;
      $('pledgeConfirm').hidden = true;
      $('pledgeCapture').hidden = false;
      $('pledgeSaveStatus').textContent = '';
    } catch (err) {
      if (newStream && newStream !== stream) {
        newStream.getTracks().forEach(track => track.stop());
      }

      if (requestId !== cameraRequestId) return;

      stopStream();
      showCapturedPhotoState();
      error('pledgeCameraError', cameraErrorMessage(err));
      $('pledgeSaveStatus').textContent = '';
    } finally {
      if (requestId === cameraRequestId) {
        cameraStarting = false;
        setCameraButtonsDisabled(false);
      }
    }
  }

  $('pledgeStartCamera').addEventListener('click', startCamera);
  $('pledgeRetake').addEventListener('click', startCamera);

  $('pledgeCapture').addEventListener('click', () => {
    const video = $('pledgeVideo');
    const canvas = $('pledgeCanvas');

    if (!video.videoWidth || !video.videoHeight) {
      error('pledgeCameraError', 'The camera is not ready yet. Please wait a moment and try again.');
      return;
    }

    const scale = Math.min(
      1,
      800 / Math.max(video.videoWidth, video.videoHeight)
    );

    canvas.width = Math.round(video.videoWidth * scale);
    canvas.height = Math.round(video.videoHeight * scale);

    const context = canvas.getContext('2d');
    context.drawImage(video, 0, 0, canvas.width, canvas.height);

    canvas.toBlob(blob => {
      if (!blob) {
        error('pledgeCameraError', 'The selfie could not be captured. Please try again.');
        return;
      }

      photo = blob;

      const preview = $('pledgePhotoPreview');
      if (preview.src.startsWith('blob:')) {
        URL.revokeObjectURL(preview.src);
      }

      preview.src = URL.createObjectURL(blob);
      preview.hidden = false;

      cancelCameraRequest();

      video.hidden = true;
      $('pledgeCapture').hidden = true;
      $('pledgeStartCamera').hidden = true;
      $('pledgeRetake').hidden = false;
      $('pledgeConfirm').hidden = false;
      $('pledgeSaveStatus').textContent = '';
    }, 'image/jpeg', 0.8);
  });

  $('pledgeConfirm').addEventListener('click', async () => {
    if (!photo || busy) return;

    busy = true;
    cancelCameraRequest();
    error('pledgeCameraError', '');
    $('pledgeSaveStatus').textContent =
      'Saving your pledge and selfie… Please keep this window open.';

    camera.querySelectorAll('button').forEach(button => {
      button.disabled = true;
    });

    // Native <dialog> elements live in the browser's top layer, above normal
    // fixed overlays. Close the camera dialog first so the global loading bar
    // is actually visible while the pledge + selfie are uploading.
    if (camera.open) {
      camera.close();
    }

    await new Promise(resolve => requestAnimationFrame(resolve));
    showPledgeLoading('Uploading your pledge and selfie, please wait...');

    try {
      const data = new FormData(form);
      data.append('selfie', photo, 'selfie.jpg');

      const response = await fetch('/thanksgiving-pledges/submit', {
        method: 'POST',
        body: data,
        headers: {
          'X-CSRF-Token': form.elements.csrf_token.value
        },
        credentials: 'same-origin'
      });

      const result = await response.json().catch(() => ({
        ok: false,
        error: 'The form or connection expired. Please reload if retrying does not work.'
      }));

      if (!response.ok || !result.ok) {
        throw new Error(result.error || 'Unable to confirm your submission. Please retry.');
      }

      $('pledgeSaveStatus').textContent =
        'Saved! Your pledge is awaiting approval.';

      showPledgeLoading('Finalizing your pledge, please wait...');
      $('pledgeSuccessForm').submit();
    } catch (err) {
      hideGlobalLoading();

      // Re-open the selfie dialog on failure. The captured photo and pledge
      // fields are kept so the user can retry without starting over.
      if (!camera.open) {
        camera.showModal();
      }
      showCapturedPhotoState();

      error(
        'pledgeCameraError',
        err.message || 'Connection interrupted. Please retry.'
      );
      $('pledgeSaveStatus').textContent =
        'Your form and selfie are retained here for retry.';

      busy = false;
      camera.querySelectorAll('button').forEach(button => {
        button.disabled = false;
      });
    }
  });

  window.addEventListener('pagehide', () => {
    cancelCameraRequest();

    const preview = $('pledgePhotoPreview');
    if (preview?.src?.startsWith('blob:')) {
      URL.revokeObjectURL(preview.src);
    }
  });
})();
