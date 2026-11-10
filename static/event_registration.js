(() => {
  'use strict';
  const $ = id => document.getElementById(id);
  const shell = document.querySelector('.event-shell');
  if (!shell) return;
  document.body.classList.add('event-page');
  const csrf = shell.dataset.csrf;
  let pendingWrites = 0, writeGeneration = 0;
  let applyProgress = () => {};
  const normalize = value => String(value || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLocaleLowerCase();
  const collator = new Intl.Collator(undefined, {numeric:true, sensitivity:'base'});
  function error(id, message) {
    const target = $(id);
    target.textContent = message;
    target.hidden = !message;
  }
  async function post(url, data = new FormData()) {
    pendingWrites++; writeGeneration++;
    try {
      const response = await fetch(url, {method:'POST', body:data, credentials:'same-origin', headers:{'X-CSRF-Token':csrf}});
      if (!(response.headers.get('Content-Type') || '').includes('application/json')) {
        throw new Error(response.status === 403 ? 'Your access expired. Reload the page and unlock Admin if needed.' : 'The request was not confirmed. Reload this page and try again.');
      }
      const result = await response.json();
      if (!response.ok || !result.ok) throw new Error(result.error || 'The request could not be completed. Please retry.');
      return result;
    } finally { pendingWrites--; writeGeneration++; }
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
    $('eventConfirmCancel').textContent = options.cancel || 'Cancel';
    $('eventConfirmYes').classList.toggle('danger',!!options.danger);
    $('eventConfirmMessage').textContent = message;
    const password = $('eventConfirmPassword');
    password.value = '';
    password.required = !!options.password;
    $('eventConfirmPasswordLabel').hidden = !options.password;
    dialog.returnValue = '';
    return new Promise(resolve => {
      dialog.addEventListener('close', () => {
        const accepted = dialog.returnValue === 'yes';
        const result = options.password ? {accepted,password:accepted?password.value:''} : accepted;
        password.value = '';
        password.required = false;
        $('eventConfirmPasswordLabel').hidden = true;
        resolve(result);
      }, {once:true});
      dialog.showModal();
      if (options.password) password.focus();
    });
  }
  $('eventConfirmYes').addEventListener('click', () => {
    if ($('eventConfirmPassword').required && !$('eventConfirmPassword').reportValidity()) return;
    $('eventConfirmDialog').close('yes');
  });
  $('eventConfirmCancel').addEventListener('click', () => $('eventConfirmDialog').close('cancel'));
  $('eventConfirmPassword').addEventListener('keydown',event => {
    if (event.key==='Enter') { event.preventDefault(); $('eventConfirmYes').click(); }
  });

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
    try { facing.value = localStorage.getItem('eventCameraFacing') === 'environment' ? 'environment' : 'default'; } catch {}
    function stop() {
      generation++;
      stream?.getTracks().forEach(track => track.stop());
      stream = null;
      video.srcObject = null;
      video.hidden = true;
      $('eventCameraCapture').hidden = true;
    }
    async function listCameras(current) {
      if (!navigator.mediaDevices?.enumerateDevices) return;
      try {
        const devices = await navigator.mediaDevices.enumerateDevices();
        if (current !== generation) return;
        facing.querySelector('optgroup')?.remove();
        const cameras = devices.filter(device => device.kind === 'videoinput' && device.deviceId);
        if (!cameras.length) return;
        const group = document.createElement('optgroup');
        group.label = 'Connected cameras';
        cameras.forEach((device,index) => group.append(new Option(device.label || 'Camera ' + (index + 1), 'device:' + device.deviceId)));
        facing.append(group);
        const active = stream?.getVideoTracks()[0]?.getSettings?.().deviceId;
        if (cameras.some(device => device.deviceId === active)) facing.value = 'device:' + active;
      } catch {} // Device labels are optional; default/front/back capture still works.
    }
    async function start() {
      stop();
      const current = generation;
      opening = true;
      $('eventCameraOpen').disabled = $('eventCameraRetake').disabled = true;
      facing.disabled = true;
      error('eventCameraError','');
      try {
        if (!navigator.mediaDevices?.getUserMedia) throw new Error('A live camera needs HTTPS or localhost. Open the secure website on a device with a camera.');
        const selection = facing.value;
        const constraints = {width:{ideal:1400},height:{ideal:1400}};
        if (selection.startsWith('device:')) constraints.deviceId = {exact:selection.slice(7)};
        else if (selection === 'user' || selection === 'environment') constraints.facingMode = {ideal:selection};
        let candidate;
        try {
          candidate = await navigator.mediaDevices.getUserMedia({audio:false,video:constraints});
        } catch (failure) {
          if (current !== generation) return;
          // Older webcams and unplugged remembered devices can reject constraints.
          if (failure.name !== 'OverconstrainedError' && !(selection.startsWith('device:') && failure.name === 'NotFoundError')) throw failure;
          candidate = await navigator.mediaDevices.getUserMedia({audio:false,video:true});
        }
        if (current !== generation) { candidate.getTracks().forEach(track => track.stop()); return; }
        stream = candidate;
        video.srcObject = stream;
        await video.play();
        if (current !== generation) return;
        const actualFacing = stream.getVideoTracks()[0]?.getSettings?.().facingMode;
        video.classList.toggle('front', actualFacing ? actualFacing === 'user' : selection !== 'environment');
        video.hidden = false;
        preview.hidden = true;
        $('eventCameraPlaceholder').hidden = true;
        $('eventCameraCapture').hidden = false;
        $('eventCameraOpen').hidden = true;
        $('eventCameraRetake').hidden = true;
        captured = null;
        $('eventPhotoSave').disabled = true;
        await listCameras(current);
      } catch (failure) {
        if (current !== generation) return;
        stop();
        const messages = {
          NotAllowedError:'Camera permission was blocked. Allow camera access for this site in your browser and in Windows Settings > Privacy & security > Camera, then try again.',
          NotFoundError:'No camera was found. Connect or enable your desktop webcam, then click Open camera again.',
          NotReadableError:'The camera is busy or unavailable. Close Teams, Zoom, or other apps using it, check your webcam privacy switch, then try again.'
        };
        error('eventCameraError',messages[failure.name] || failure.message || 'The camera could not start. Please retry.');
        $('eventCameraOpen').hidden = false;
        $('eventCameraPlaceholder').hidden = !!captured;
        preview.hidden = !captured;
      } finally {
        if (current === generation || !stream) {
          opening = false;
          facing.disabled = false;
          $('eventCameraOpen').disabled = $('eventCameraRetake').disabled = false;
        }
      }
    }
    $('eventCameraOpen').addEventListener('click', () => { if (!opening) start(); });
    $('eventCameraRetake').addEventListener('click', () => { if (!opening) start(); });
    facing.addEventListener('change', () => {
      try { localStorage.setItem('eventCameraFacing',facing.value === 'environment' ? 'environment' : 'default'); } catch {}
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
      $('eventPhotoCancel').disabled = true;
      error('eventCameraError','');
      const data = new FormData(cameraForm);
      data.set('camera_capture','1');
      data.set('photo',captured,'camera-photo.png');
      try { const result = await post(cameraForm.action,data); window.location.assign(result.redirect); }
      catch (failure) { error('eventCameraError',failure.message); }
      finally { busy = false; $('eventPhotoSave').disabled = !captured; $('eventPhotoCancel').disabled = false; }
    });
    $('eventPhotoCancel').addEventListener('click',async () => {
      if (busy) return;
      busy = true;
      $('eventPhotoSave').disabled = $('eventPhotoCancel').disabled = true;
      error('eventCameraError','');
      try {
        const accepted = await confirmStatus('Are you sure you want to cancel your registration? Your entered details and captured photo will be discarded.',
          {title:'Cancel submission?',button:'Yes, cancel',cancel:'Back',danger:true});
        if (!accepted) return;
        const result = await post($('eventPhotoCancel').dataset.url,formData({confirm:'yes'}));
        stop();
        if (capturedURL) { URL.revokeObjectURL(capturedURL); capturedURL = null; }
        captured = null;
        window.location.assign(result.redirect);
      } catch (failure) { error('eventCameraError',failure.message); }
      finally { busy = false; $('eventPhotoSave').disabled = !captured; $('eventPhotoCancel').disabled = false; }
    });
    window.addEventListener('pagehide', () => { stop(); if (capturedURL) URL.revokeObjectURL(capturedURL); });
    navigator.mediaDevices?.addEventListener?.('devicechange', () => { if (stream && !opening) listCameras(generation); });
  }

  const rows = [...document.querySelectorAll('[data-attendee-row]')];
  function pendingDeletions(count) {
    if (!$('eventPendingDeletionCount')) return;
    $('eventPendingDeletionCount').textContent = count;
    $('eventPendingDeletionNote').hidden = !count;
    if ($('eventBackupButton') && !backupRunning) $('eventBackupButton').disabled = !rows.length && !count && $('eventBackupButton').dataset.hasMeals!=='1';
  }
  if ($('eventSearch')) {
    const admin = shell.dataset.admin === '1', tbody = $('eventAttendeeRows');
    let stage = 'processing';
    const processed = row => ['paid','printed','released'].every(field => row.dataset[field] === '1');
    function summary() {
      if (!admin) return;
      const completed = rows.filter(processed).length;
      $('eventProcessingCount').textContent = rows.length-completed;
      $('eventCompletedCount').textContent = $('eventTotalCompleted').textContent = completed;
      $('eventTotalRegistered').textContent = rows.length;
      const areas = new Map();
      rows.forEach(row => {
        if (!areas.has(row.dataset.area)) areas.set(row.dataset.area,{registered:0,completed:0});
        const area = areas.get(row.dataset.area);
        area.registered++; if (processed(row)) area.completed++;
      });
      const highest = Math.max(0,...[...areas.values()].map(area=>area.completed));
      const leaders = [...areas].filter(([,area])=>highest && area.completed===highest).map(([area])=>'Area '+area);
      $('eventLeadingArea').textContent = highest ? leaders.join(', ')+' ('+highest+')' : 'No completed delegates yet';
      $('eventAreaSummaryRows').replaceChildren();
      [...areas].sort(([a],[b])=>collator.compare(a,b)).forEach(([name,area])=>{
        const tr = document.createElement('tr');
        ['Area '+name,area.registered,area.completed].forEach(value=>{const td=document.createElement('td');td.textContent=value;tr.append(td);});
        $('eventAreaSummaryRows').append(tr);
      });
    }
    function filter() {
      const query = normalize($('eventSearch').value.trim()), area = $('eventFilterArea').value;
      let count = 0;
      rows.forEach(row => {
        row.hidden = (admin && processed(row)!==(stage==='completed')) || !normalize(row.dataset.number+' '+row.dataset.search).includes(query) || !!(area && row.dataset.area !== area);
        if (!row.hidden) count++;
      });
      const field = $('eventSort').value;
      const sorted = [...rows].sort((a,b) => collator.compare(a.dataset[field],b.dataset[field]) || Number(a.dataset.number)-Number(b.dataset.number));
      sorted.forEach((row,index) => { if (tbody.children[index]!==row) tbody.insertBefore(row,tbody.children[index]||null); });
      $('eventResultCount').textContent = count;
      $('eventNoResults').hidden = count !== 0;
      summary();
    }
    function makeRow(person) {
      const row = document.createElement('tr');
      row.dataset.attendeeRow = '';
      Object.assign(row.dataset,{number:person.number,name:person.name,area:person.area,church:person.church,
        search:[person.name,person.area,person.church,admin?person.address:''].join(' ')});
      if (admin) Object.assign(row.dataset,{key:person.key,revision:person.revision,paid:person.paid,printed:person.physical_printed,released:person.released,statusUrl:person.status_url,deleteUrl:person.delete_url});
      function cell(text, className='', feature='', label='') {
        const td = document.createElement('td');
        td.textContent = text; td.className = className;
        if (feature) td.dataset.feature = feature;
        if (label) td.dataset.label = label;
        row.append(td); return td;
      }
      cell(person.number,'event-number');
      const name = cell('','event-person'), strong = document.createElement('strong');
      if (admin) {const img=document.createElement('img');img.src=person.photo_url;img.alt='';img.loading='lazy';name.append(img);}
      strong.textContent=person.name;name.append(strong);
      cell(person.area,'event-area','','Area');cell(person.church,'event-church','','Church');
      if (admin) {
        cell(person.address,'','extra','Address');
        const paid = document.createElement('button');
        paid.type='button';paid.className='event-status'+(person.paid?' on':'');paid.dataset.field='paid';paid.dataset.value=String(person.paid);
        paid.setAttribute('aria-pressed',String(!!person.paid));paid.textContent=person.paid?'\u2713 Paid':'Payment';cell('','','payment').append(paid);
        const print = document.createElement('a');print.className='event-btn event-print-link'+(person.physical_printed?' printed':'');print.href=person.print_url;print.textContent=person.physical_printed?'\u2713 Printed':'Print';cell('','','print').append(print);
        const release = document.createElement('a');release.className='event-status event-release-link'+(person.released?' on':'');release.href=person.release_url;release.target='_blank';release.rel='noopener';release.textContent=person.released?'\u2713 Released':'Release / Share';cell('','','release').append(release);
        const details=document.createElement('button');details.type='button';details.className='event-btn secondary event-delegate-details';details.dataset.url=person.details_url;details.textContent='View details';cell('','','extra').append(details);
        const remove=document.createElement('button');remove.type='button';remove.className='event-btn danger event-delete-attendee';remove.textContent='Delete';cell('','','extra').append(remove);
      }
      return row;
    }
    applyProgress = data => {
      if (!data.attendees) return;
      const existing=new Map(rows.map(row=>[admin?row.dataset.key:row.dataset.number,row]));
      const next=[];
      data.attendees.forEach(person=>{
        const key=String(admin?person.key:person.number), old=existing.get(key);
        const unchanged=old && old.dataset.name===person.name && old.dataset.area===person.area && old.dataset.church===person.church && (!admin || old.dataset.revision===String(person.revision));
        const row=unchanged?old:makeRow(person);
        if (!unchanged) { if(old) old.replaceWith(row); else tbody.append(row); }
        existing.delete(key); next.push(row);
      });
      existing.forEach(row=>row.remove());rows.splice(0,rows.length,...next);
      const select=$('eventFilterArea'), selected=select.value;
      const areas=[...new Set(rows.map(row=>row.dataset.area))];
      if(selected && !areas.includes(selected)) areas.push(selected);
      areas.sort(collator.compare);
      select.replaceChildren(new Option('All areas',''),...areas.map(area=>new Option('Area '+area,area)));
      select.value=selected;
      if(admin) pendingDeletions(data.pending_deletions);
      filter();
    };
    document.querySelectorAll('[data-stage]').forEach(button=>button.addEventListener('click',()=>{
      stage = button.dataset.stage;
      document.querySelectorAll('[data-stage]').forEach(tab=>{tab.classList.toggle('active',tab===button);tab.setAttribute('aria-selected',String(tab===button));});
      $('eventCompletedSummary').hidden = $('eventMealSummary').hidden = stage!=='completed';
      $('eventStageCaption').textContent = stage==='completed' ? 'completed delegates' : 'delegates for processing';
      filter();
    }));
    const view=$('eventAdminView');
    if(view) {
      try {const saved=localStorage.getItem('eventAdminView');if(['all','payment','print','release'].includes(saved)) view.value=saved;} catch {}
      shell.dataset.mode=view.value;
      view.addEventListener('change',()=>{shell.dataset.mode=view.value;try{localStorage.setItem('eventAdminView',view.value);}catch{}});
    }
    filter();
    $('eventSearch').addEventListener('input',filter);
    $('eventFilterArea').addEventListener('change',filter);
    $('eventSort').addEventListener('change',filter);
    const detailsDialog = $('eventDelegateDialog');
    detailsDialog?.querySelector('[data-close-dialog]').addEventListener('click',()=>detailsDialog.close());
    let detailGeneration = 0;
    tbody.addEventListener('click',async event=>{
      const button=event.target.closest('button'),row=button?.closest('[data-attendee-row]');
      if(!button || !row || !admin || row.dataset.busy) return;
      if(button.matches('.event-status')) {
        const enabled=row.dataset.paid==='1';
        row.dataset.busy='1';
        try {
          if(enabled && !await confirmStatus('Change this attendee back to not paid?')) return;
          button.disabled=true;error('eventGlobalError','');
          const result=await post(row.dataset.statusUrl,formData({field:'paid',value:enabled?0:1,confirm:enabled?'yes':'',revision:row.dataset.revision}));
          row.dataset.revision=result.revision;row.dataset.paid=enabled?'0':'1';button.dataset.value=row.dataset.paid;
          button.classList.toggle('on',!enabled);button.setAttribute('aria-pressed',String(!enabled));button.textContent=enabled?'Payment':'\u2713 Paid';filter();
        } catch(failure) {error('eventGlobalError',failure.message);}
        finally {button.disabled=false;delete row.dataset.busy;refreshProgress();}
      } else if(button.matches('.event-delete-attendee')) {
        row.dataset.busy='1';
        try {
          if(!await confirmStatus('Delete '+row.dataset.name+'? This removes the local registration immediately. Backup Data will remove its Google Sheets row and Google Drive IDs.',{title:'Delete registration?',button:'Yes, delete registration',danger:true})) return;
          button.disabled=true;error('eventGlobalError','');
          const result=await post(row.dataset.deleteUrl,formData({confirm:'yes',revision:row.dataset.revision}));
          rows.splice(rows.indexOf(row),1);row.remove();filter();pendingDeletions(result.pending_deletions);
          if($('eventLiveCount')) $('eventLiveCount').textContent=rows.length;
        } catch(failure) {error('eventGlobalError',failure.message);}
        finally {button.disabled=false;delete row.dataset.busy;refreshProgress();}
      } else if(button.matches('.event-delegate-details')) {
        const current=++detailGeneration,content=$('eventDelegateContent');
        content.textContent='Loading delegate details...';detailsDialog.showModal();
        try {
          const response=await fetch(button.dataset.url,{credentials:'same-origin',cache:'no-store'});
          if(!response.ok || !(response.headers.get('Content-Type')||'').includes('application/json')) throw new Error('Unlock Admin again to view delegate details.');
          const data=await response.json();if(current!==detailGeneration) return;content.replaceChildren();
        const dl = document.createElement('dl'); dl.className='event-details-grid';
        const labels={number:'Attendee number',name:'Full name',birthday:'Birthday',contact:'Contact number',area:'Area',church:'Church',address:'Church address',pastor:'Pastor',created_at:'Registration created',paid:'Paid',physical_printed:'Physical ID printed',released:'Released'};
        Object.entries(labels).forEach(([key,label])=>{
          const dt=document.createElement('dt'),dd=document.createElement('dd');dt.textContent=label;
          dd.textContent=['paid','physical_printed','released'].includes(key)?(data.delegate[key]?'Yes':'No'):(data.delegate[key]||'Not supplied');dl.append(dt,dd);
        }); content.append(dl);
        const heading=document.createElement('h3');heading.textContent='Meal records';content.append(heading);
        if (!data.meals.length) {const p=document.createElement('p');p.textContent='No meal records have been synced for this delegate.';content.append(p);}
        data.meals.forEach(meal=>{
          const p=document.createElement('p');p.className='event-meal-record';
          p.textContent=[meal.meal_name||meal.claim_id,meal.service_date,meal.served_at,'Scanner: '+meal.scanner_id,meal.status.replaceAll('_',' ')].join(' · ');content.append(p);
        });
        if (data.audit.length) {const h=document.createElement('h3');h.textContent='Corrections and disputes';content.append(h);}
        data.audit.forEach(record=>{const p=document.createElement('p');p.textContent=record.occurred_at+' · '+record.action+' · '+record.reason;content.append(p);});

        } catch(failure) {if(current===detailGeneration) content.textContent=failure.message;}
      }
    });
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
    let completed = 0, total = 0, removed = 0, changed = 0;
    try {
      status.textContent = 'Preparing local registrations…';
      const queue = await post(backupButton.dataset.url,formData({event_id:backupButton.dataset.event}));
      total = queue.keys.length;
      for (const key of queue.keys) {
        status.textContent = 'Backing up attendee '+(completed+1)+' of '+total+': printable ID and registration details…';
        const result = await post(backupButton.dataset.url+'/'+encodeURIComponent(key));
        completed++;
        if (result.removed) removed++;
        if (!result.clean) changed++;
        progress.value = Math.floor(completed/total*100);
        pendingDeletions(result.pending_deletions);
      }
      if (backupButton.dataset.event && backupButton.dataset.mealUrl) {
        status.textContent = 'Backing up synced meal and audit records…';
        await post(backupButton.dataset.mealUrl,formData({event_id:backupButton.dataset.event}));
      }
      progress.value = 100;
      status.textContent = completed+' registrations processed.'+(completed>removed?' Printable IDs saved to Google Drive; details saved to Google Sheets.':'')+(removed?' '+removed+' deleted registration'+(removed===1?'':'s')+' removed.':'')+(changed?' '+changed+' changed during backup. Click Backup Data again to save the latest changes.':'');
    } catch (failure) { status.textContent = completed+' of '+total+' backed up. '+failure.message; }
    finally { backupRunning = false; backupButton.disabled = !rows.length && !Number($('eventPendingDeletionCount').textContent) && backupButton.dataset.hasMeals!=='1'; $('eventAdminSelect').disabled = false; refreshProgress(); }
  });
  $('eventDeleteEventButton')?.addEventListener('click',async event => {
    if (backupRunning) { error('eventGlobalError','Wait for the Google submission to finish before deleting this event.'); return; }
    const button = event.currentTarget;
    const confirmation = await confirmStatus('Delete '+button.dataset.name+' and all its registrations? Registration will close, its QR codes will stop working, and Google row removal will be queued. Enter the event admin password to confirm.',
      {title:'Delete event?',button:'Yes, delete event',danger:true,password:true});
    if (!confirmation.accepted) return;
    button.disabled = true;
    try {
      const data = formData({confirm:'yes',updated_at:button.dataset.updated,password:confirmation.password});
      confirmation.password = '';
      const result = await post(button.dataset.url,data);
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
        $('eventPrintStatus').textContent = 'Download prepared. Confirm printing only after the physical ID is printed.';
      } catch (failure) { error('eventGlobalError',failure.message); }
      finally { delete button.dataset.busy; }
    });
    const checkbox = $('eventPrintComplete');
    checkbox.addEventListener('change',async () => {
      const value = checkbox.checked;
      checkbox.disabled = true;
      try {
        if (!await confirmStatus(value ? 'Confirm that the physical ID has been printed successfully. A download alone does not qualify.' : 'Clear the physical ID printing confirmation?',{title:'Physical ID printing',button:value?'Yes, physically printed':'Clear confirmation'})) {
          checkbox.checked = !value; return;
        }
        const result = await post(checkbox.dataset.statusUrl,formData({field:'physical_printed',value:value?1:0,physical_confirmed:value?'yes':'',confirm:'yes',revision:checkbox.dataset.revision}));
        checkbox.dataset.revision = result.revision;
        $('eventPrintNow').classList.toggle('printed',value);
        $('eventPrintStatus').textContent = value?'Physical ID printing confirmed.':'Physical printing confirmation cleared.';
      } catch (failure) { checkbox.checked = !value; $('eventPrintStatus').textContent = failure.message; }
      finally { checkbox.disabled = false; }
    });
  }

  const releasePanel = $('eventReleasePanel');
  if (releasePanel) {
    const shareButton=$('eventShareID'),releaseButton=$('eventReleaseID'),undo=$('eventUndoRelease');
    let shareFile=null,shareURL=null,shareGeneration=0,sharing=false,releasing=false;
    async function prepareShare() {
      const generation=++shareGeneration;
      shareFile=null;shareButton.disabled=true;
      $('eventShareStatus').textContent='Preparing the ID picture…';
      try {
        const response=await fetch(releasePanel.dataset.shareUrl,{credentials:'same-origin',cache:'no-store'});
        if(!response.ok || !(response.headers.get('Content-Type')||'').includes('image/png')) throw new Error('The ID picture could not load. Press Refresh to retry.');
        const blob=await response.blob();if(generation!==shareGeneration) return;
        const temporary=releasePanel.dataset.printed!=='1';
        shareFile=new File([blob],temporary?'Temporary-Event-ID.png':'Event-ID.png',{type:'image/png'});
        if(shareURL) URL.revokeObjectURL(shareURL);
        shareURL=URL.createObjectURL(blob);$('eventSharedPreview').src=shareURL;
        shareButton.disabled=false;
        $('eventShareStatus').textContent=temporary?'Temporary ID ready to share. Sharing does not release the physical ID.':'ID picture ready to share.';
      } catch(failure) {if(generation===shareGeneration) $('eventShareStatus').textContent=failure.message;}
    }
    function updateRelease(person) {
      const printed=String(person.physical_printed),changed=releasePanel.dataset.printed!==printed;
      Object.assign(releasePanel.dataset,{revision:person.revision,paid:person.paid,printed,released:person.released});
      releaseButton.disabled=releasing || !!person.released || !person.paid || !person.physical_printed;
      releaseButton.classList.toggle('printed',!!person.released);
      releaseButton.textContent=person.released?'✓ Physical ID released':'Release ID';
      undo.hidden=!person.released;undo.disabled=releasing;
      $('eventReleasePaid').textContent='Payment '+(person.paid?'confirmed':'pending');
      $('eventReleasePaid').classList.toggle('success',!!person.paid);
      $('eventReleasePrinted').textContent='Physical print '+(person.physical_printed?'confirmed':'pending');
      $('eventReleasePrinted').classList.toggle('success',!!person.physical_printed);
      shareButton.textContent=person.physical_printed?'Share ID':'Share Temporary ID';
      if(changed || !shareFile && !shareButton.dataset.loading) {
        shareButton.dataset.loading='1';prepareShare().finally(()=>{delete shareButton.dataset.loading;});
      }
    }
    applyProgress=data=>{
      const person=data.attendees?.find(person=>person.key===releasePanel.dataset.key);
      if(person) updateRelease(person);
      else if(data.attendees) {
        shareGeneration++;shareFile=null;shareButton.disabled=releaseButton.disabled=undo.disabled=true;
        $('eventShareStatus').textContent='This attendee was deleted. Return to the attendee list.';
        $('eventSharedPreview').removeAttribute('src');
      }
    };
    async function setRelease(value) {
      if(releasing) return;releasing=true;
      try {
        if(!await confirmStatus(value?'Have you handed the physical ID to this attendee?':'Undo the physical ID release?',{title:value?'Release physical ID':'Undo release',button:value?'Yes, ID handed over':'Undo release'})) return;
        releaseButton.disabled=undo.disabled=true;
        const result=await post(releasePanel.dataset.statusUrl,formData({field:'released',value:value?1:0,confirm:'yes',revision:releasePanel.dataset.revision}));
        releasing=false;
        updateRelease({revision:result.revision,paid:Number(releasePanel.dataset.paid),physical_printed:Number(releasePanel.dataset.printed),released:value?1:0});
      } catch(failure) {error('eventGlobalError',failure.message);}
      finally {releasing=false;undo.disabled=false;releaseButton.disabled=releasePanel.dataset.released==='1' || releasePanel.dataset.paid!=='1' || releasePanel.dataset.printed!=='1';refreshProgress();}
    }
    releaseButton.addEventListener('click',()=>setRelease(true));undo.addEventListener('click',()=>setRelease(false));
    shareButton.addEventListener('click',async()=>{
      if(!shareFile || sharing) return;
      sharing=true;shareButton.disabled=true;
      try {
        const data={title:'Event ID',files:[shareFile]};
        if(navigator.share && (!navigator.canShare || navigator.canShare({files:[shareFile]}))) {
          await navigator.share(data);$('eventShareStatus').textContent='Share menu completed. Physical release status is unchanged.';
        } else {
          const link=document.createElement('a');link.href=shareURL;link.download=shareFile.name;document.body.append(link);link.click();link.remove();
          $('eventShareStatus').textContent='Picture downloaded. Attach it in your messaging app. Physical release status is unchanged.';
        }
      } catch(failure) {$('eventShareStatus').textContent=failure.name==='AbortError'?'Sharing cancelled.':failure.message;}
      finally {sharing=false;shareButton.disabled=!shareFile;}
    });
    updateRelease({revision:Number(releasePanel.dataset.revision),paid:Number(releasePanel.dataset.paid),physical_printed:Number(releasePanel.dataset.printed),released:Number(releasePanel.dataset.released)});
    $('eventRefresh').addEventListener('click',()=>{if(!shareFile) prepareShare();});
    window.addEventListener('pagehide',()=>{shareGeneration++;if(shareURL) URL.revokeObjectURL(shareURL);});
  }

  let liveCursor='',liveBusy=false,liveStopped=false,liveTimer;
  async function refreshProgress() {
    if(!shell.dataset.liveUrl || liveStopped || liveBusy || document.hidden || pendingWrites || backupRunning || document.querySelector('dialog[open]') || document.querySelector('[data-attendee-row][data-busy]')) return;
    liveBusy=true;
    const generation=writeGeneration,button=$('eventRefresh'),status=$('eventLiveStatus');
    if(button) button.disabled=true;
    try {
      const url=new URL(shell.dataset.liveUrl,location.href);if(liveCursor) url.searchParams.set('cursor',liveCursor);
      const response=await fetch(url,{credentials:'same-origin',cache:'no-store',signal:AbortSignal.timeout(12000)});
      if(!response.ok) {
        if(response.status===403) {liveStopped=true;throw new Error('Admin access expired. Unlock Admin again to resume updates.');}
        throw new Error('Updates are temporarily unavailable. Retrying automatically.');
      }
      const data=await response.json();
      if(generation!==writeGeneration || pendingWrites || backupRunning || document.querySelector('dialog[open]') || document.querySelector('[data-attendee-row][data-busy]')) return;
      if(String(data.event_id??'')!==shell.dataset.eventId) {
        liveStopped=true;error('eventGlobalError','This event has changed or closed. Reload this page to see the current event. Your entered details have not been cleared.');return;
      }
      if(data.changed) {
        applyProgress(data);
        if($('eventLiveCount')) $('eventLiveCount').textContent=data.count;
      }
      liveCursor=data.cursor;
      if(status) status.textContent='Checked '+new Date().toLocaleTimeString([],{hour:'2-digit',minute:'2-digit',second:'2-digit'})+' · Auto updates on';
    } catch(failure) {if(status) status.textContent=liveStopped?failure.message:'Could not check updates. Retrying automatically; you can also press Refresh.';}
    finally {liveBusy=false;if(button) button.disabled=liveStopped;}
  }
  if(shell.dataset.liveUrl) {
    function schedule() {clearTimeout(liveTimer);if(!document.hidden && !liveStopped) liveTimer=setTimeout(async()=>{await refreshProgress();schedule();},7000);}
    $('eventRefresh')?.addEventListener('click',()=>{liveCursor='';refreshProgress();});
    document.addEventListener('visibilitychange',()=>{if(!document.hidden) refreshProgress();schedule();});
    window.addEventListener('focus',()=>refreshProgress());
    window.addEventListener('pagehide',()=>clearTimeout(liveTimer));
    window.addEventListener('pageshow',()=>{refreshProgress();schedule();});
    refreshProgress();schedule();
  }
})();
