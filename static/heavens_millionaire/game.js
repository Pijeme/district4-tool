/* Same-origin game UI. Correct answers, scoring and deadlines belong to the server. */
(() => {
  'use strict';
  const TITLE = 'Who Wants to Be Heaven’s Millionaire?';
  const ROOT = '/static/heavens_millionaire/';
  const ASSETS = ROOT + 'images/';
  const $ = (selector, root = document) => root.querySelector(selector);
  const app = $('#app'), modal = $('#modal'), modalContent = $('#modal-content');
  const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const number = n => Number(n || 0).toLocaleString('en-US');
  const uuid = () => crypto.randomUUID ? crypto.randomUUID() : Array.from(crypto.getRandomValues(new Uint8Array(20)), b => b.toString(16).padStart(2, '0')).join('');
  const stored = (key, fallback = '') => { try { return localStorage.getItem(key) ?? fallback; } catch { return fallback; } };
  const store = (key, value) => { try { localStorage.setItem(key, value); } catch {} };
  const date = stamp => new Date(stamp * 1000).toLocaleString('en-PH', {timeZone:'Asia/Manila',month:'short',day:'numeric',year:'numeric',hour:'numeric',minute:'2-digit'});
  let boot, me, csrf = '', current = null, view = 'welcome', busy = false, offset = 0, clockBase = 0, perfBase = 0;
  let musicEnabled = stored('hm_music', 'on') !== 'off', audio = null, track = 0, timeoutBusy = false, warningShown = 0;
  let ranking = {view:'top',area:'',church:'',period:'all',sort:'score',page:1}, profilePage = 1, adminQuestionPage = 1;
  let loadingInterval, noticeTimeout, hiddenEvent = null, hiddenTimer, lastInterruption = 0;
  const letters = ['A','B','C','D'];
  const lifelines = [['fifty','50:50','fifty'],['audience','Audience','audience'],['hint','Bible Hint','bible'],['change','Change question','refresh']];
  const messages = [
    'This is for fun, learning, and friendly competition. The points have no cash value.',
    'Play honestly and enjoy the challenge. Your knowledge grows with every question.',
    'Proverbs 10:9 · Original paraphrase: An honest walk is a confident walk.',
    'Proverbs 11:1 · Original paraphrase: God values fairness in what we do.',
    'Colossians 3:23 · Original paraphrase: Give your best with a wholehearted spirit.',
    'Leaving an active question triggers an interruption warning. A second interruption disqualifies the attempt. The clock keeps running.'
  ];

  function toast(message, duration = 6500) {
    clearTimeout(noticeTimeout); $('#notice').textContent = message; $('#notice').hidden = false;
    noticeTimeout = setTimeout(() => { $('#notice').hidden = true; }, duration);
  }
  async function api(path, data, method) {
    const options = {credentials:'same-origin', headers:{}};
    if (data !== undefined) {
      options.method = method || 'POST'; options.headers['X-CSRF-Token'] = csrf;
      if (data instanceof FormData) options.body = data;
      else { options.headers['Content-Type'] = 'application/json'; options.body = JSON.stringify(data); }
    }
    let response;
    try { response = await fetch('/bible-games/api/' + path, options); }
    catch { throw new Error('Connection interrupted. Your game stays saved and the question clock continues. Reconnect to resume.'); }
    let value;
    try { value = await response.json(); } catch { throw new Error('The server is temporarily unavailable. Please try again.'); }
    if (!response.ok) throw new Error(value.error || 'That request could not be completed.');
    if (value.csrf) csrf = value.csrf;
    return value;
  }
  function setView(name, html) {
    view = name; document.body.dataset.gameView = name; clearInterval(loadingInterval); app.innerHTML = html; app.focus({preventScroll:true});
    window.scrollTo({top:0, behavior:'instant'}); $('#account-nav').hidden = !me;
  }
  function showModal(html, closeable = true) {
    modalContent.innerHTML = html; $('#close-modal').hidden = !closeable;
    if (!modal.open) modal.showModal();
  }
  function closeModal() { if (modal.open) modal.close(); }
  $('#close-modal').onclick = closeModal;
  modal.addEventListener('cancel', e => { if ($('#close-modal').hidden) e.preventDefault(); });
  function syncSound() {
    $('#sound').setAttribute('aria-pressed', String(musicEnabled));
    $('#sound span').textContent = musicEnabled ? 'Music on' : 'Music off';
    $('#sound').title = musicEnabled ? 'Mute background music' : 'Play background music';
  }
  function playMusic(stage = current?.position || 1, userGesture = false) {
    const wanted = stage >= 11 ? 2 : 1;
    if (!audio || track !== wanted) {
      if (audio) audio.pause();
      audio = new Audio(ROOT + `music-${wanted}.mp3`); audio.loop = true; audio.volume = .22; audio.preload = 'auto'; track = wanted;
    }
    if (musicEnabled) audio.play().catch(() => { if (userGesture) toast('Tap the music button to enable sound on this browser.'); });
    else audio.pause();
    syncSound();
  }
  $('#sound').onclick = () => { musicEnabled = !musicEnabled; store('hm_music', musicEnabled ? 'on' : 'off'); playMusic(current?.position || 1, true); };
  syncSound();

  function welcome() {
    setView('welcome', `<section class="hero"><div class="hero-copy"><div class="eyebrow">The District 4 Bible challenge</div>
      <h1>Who Wants to Be <span class="gold">Heaven’s Millionaire?</span></h1>
      <p>Step into the studio. Put your Bible knowledge to the test, take a chance on a lifeline, and share the joy of learning with your church.</p>
      <div class="hero-buttons"><button class="primary" data-auth="register">Take the challenge <span aria-hidden="true">→</span></button><button class="secondary" data-auth="login">Returning player</button></div>
      ${boot?.site_user ? `<button class="quiet" data-site-login>Continue as ${esc(boot.site_user.name)}</button>` : ''}${boot?.admin?'<button class="quiet" data-nav="admin">Game administration</button>':''}
      <div class="features"><div><strong>15</strong><span>questions to the top</span></div><div><strong>4</strong><span>lifelines to help</span></div><div><strong>1 million</strong><span>points to aim for</span></div></div>
      <p class="integrity">A game of knowledge and integrity. Play for fun. Grow in understanding.<br><button class="quiet" data-nav="instructions">How to play</button></p></div>
      <div class="hero-art"><div class="studio-arch"></div><img class="hero-host" src="${ROOT}host_full_clean.webp" alt="Your welcoming Bible quiz host"><div class="float-label questions-label"><small>Your journey</small><strong>One question<br>at a time.</strong></div><div class="float-label prize-label"><small>The ultimate milestone</small><strong>1,000,000 points</strong></div></div></section>`);
  }
  const areas = () => [...new Set((boot?.directory || []).map(c => c.area))];
  function churchFields(area = '', church = '') {
    return `<div class="field"><label for="area">Area</label><select id="area" name="area" required><option value="">Choose your Area</option>${areas().map(a => `<option value="${esc(a)}" ${a===area?'selected':''}>Area ${esc(a)}</option>`).join('')}</select></div>
      <div class="field"><label for="church">Church / Area Overseer</label><select id="church" name="church_id" required><option value="">Choose your church or Area Overseer</option>${(boot.directory || []).filter(c=>c.area===area).map(c=>`<option value="${esc(c.id)}" ${c.id===church?'selected':''}>${esc(c.church)}</option>`).join('')}</select></div><div class="full pastor-display" id="pastor">Your pastor’s name will appear when you select your church.</div>`;
  }
  function portraitFields() {
    return `<div class="full"><label for="photo">Take your player photo</label><input id="photo" name="photo" type="file" accept="image/jpeg,image/png,image/webp" capture="user" required><img id="photo-preview" class="photo-preview" alt="Your portrait preview" hidden><p class="form-note">Take a clear portrait or choose an existing photo. We store a compressed 320 × 320 portrait on the local server, up to 60 KB. Please use your own photo.</p></div><label class="check full"><input name="consent" type="checkbox" value="true" required><span>I agree that my name, church, portrait and game results will appear on scoreboards visible to signed-in players. My photo is used for my game profile, not camera monitoring.</span></label>`;
  }
  function wireChurch() {
    const area = $('#area'), church = $('#church');
    if (area && church) {
      const pastor = () => { const selected = boot.directory.find(c => c.id===church.value); $('#pastor').textContent = selected ? (selected.church==='Area Overseer' ? `Area Overseer · Area ${selected.area}` : `Pastor: ${selected.pastor || 'Not recorded in the church directory'}`) : 'Your pastor’s name will appear when you select your church.'; };
      area.onchange = () => { church.innerHTML = `<option value="">Choose your church or Area Overseer</option>${boot.directory.filter(c=>c.area===area.value).map(c=>`<option value="${esc(c.id)}">${esc(c.church)}</option>`).join('')}`; pastor(); };
      church.onchange = pastor; pastor();
    }
    const input = $('#photo');
    if (input) input.onchange = () => {
      const preview = $('#photo-preview');
      if (preview.dataset.url) URL.revokeObjectURL(preview.dataset.url);
      if (input.files[0]) { preview.src = URL.createObjectURL(input.files[0]); preview.dataset.url = preview.src; preview.hidden = false; }
    };
  }
  function auth(mode = 'register') {
    const register = mode==='register', recovery = mode==='recover', help = mode==='help';
    setView('auth', `<section class="panel narrow"><div class="eyebrow">Your place in the studio</div><h2>${register?'Join the challenge':recovery?'Recover your account':help?'Ask for recovery help':'Welcome back'}</h2>
      <div class="tabs"><button class="tab ${register?'active':''}" data-auth="register">New player</button><button class="tab ${!register&&!recovery&&!help?'active':''}" data-auth="login">Sign in</button></div>
      <form id="auth-form"><div class="form-grid">${register?`<div class="full"><label for="name">Complete name</label><input id="name" name="name" autocomplete="name" minlength="2" maxlength="100" required></div>${churchFields()}`:`<div class="full"><label for="player-code">Player ID</label><input id="player-code" name="player_code" placeholder="HM-…" autocomplete="username" required></div>`}
      ${recovery?'<div class="full"><label for="recovery-code">Recovery code</label><input id="recovery-code" name="recovery_code" autocomplete="off" required></div>':''}
      ${help?'<div class="full"><label for="note">A short note for the administrator</label><textarea id="note" name="note" maxlength="300" placeholder="Please help me recover my account."></textarea><p class="form-note">Your pastor or administrator must verify you in person. Church details alone cannot reset a password.</p></div>':`<div class="full"><label for="password">${recovery?'New password':'Password'}</label><input id="password" name="password" type="password" minlength="8" maxlength="128" autocomplete="${register||recovery?'new-password':'current-password'}" required></div>`}
      ${register?portraitFields():''}</div><div class="form-actions"><button class="primary" type="submit">${register?'Create my player account':recovery?'Reset password':help?'Request help':'Sign in'} →</button><button type="button" class="quiet" data-nav="welcome">Back</button></div></form>
      ${!register&&!help?'<button class="quiet" data-auth="recover">I forgot my password</button><button class="quiet" data-auth="help">I lost my recovery code</button>':''}
      ${register?'<p class="form-note">No email address or phone number required. You’ll receive a Player ID and recovery code to save.</p>':''}
      ${boot.site_user?`<p><button class="quiet" data-site-login>Use my existing website account</button></p>`:''}
      ${!boot.directory.length?'<p class="form-note">The local church directory is empty. An administrator needs to refresh the website’s directory before registration.</p>':''}</section>`);
    wireChurch();
    $('#auth-form').onsubmit = async event => {
      event.preventDefault(); const form=event.currentTarget, submit=$('[type=submit]',form); submit.disabled=true;
      try {
        const data=new FormData(form);
        if (register) await compressPhoto(data);
        const result=await api(register?'register':recovery?'recover':help?'recovery-help':'login', register?data:Object.fromEntries(data));
        if (help) { toast(result.message,12000); auth('login'); return; }
        if (recovery) { credentials(result.player_code,result.recovery_code,()=>auth('login'),true); return; }
        me=result.player; if (register) credentials(me.player_code,result.recovery_code,()=>prepare());
        else await afterLogin();
      } catch(error) { toast(error.message); submit.disabled=false; }
    };
  }
  async function compressPhoto(data) {
    const file=data.get('photo'); if (!(file instanceof File) || !file.size) return;
    // Server re-encodes independently. This only reduces camera uploads on mobile.
    try {
      const url=URL.createObjectURL(file), image=new Image();
      try { image.src=url; await image.decode(); const canvas=document.createElement('canvas');canvas.width=canvas.height=640;
        const context=canvas.getContext('2d'), size=Math.min(image.naturalWidth,image.naturalHeight);
        context.drawImage(image,(image.naturalWidth-size)/2,(image.naturalHeight-size)/2,size,size,0,0,640,640);
        const blob=await new Promise(resolve=>canvas.toBlob(resolve,'image/jpeg',.88));if(blob)data.set('photo',blob,'portrait.jpg');
      } finally { URL.revokeObjectURL(url); }
    } catch { /* Server validates the original upload if client decoding fails. */ }
  }
  function credentials(code,recovery,next,reset=false) {
    setView('credentials',`<section class="panel narrow"><div class="eyebrow">${reset?'Password updated':'You’re on the guest list'}</div><h2>Save your way back.</h2><p class="muted">Keep these somewhere safe. Your recovery code can restore your account without an email address or phone number. It will only be shown here.</p><div class="credential"><label>Player ID</label><code>${esc(code)}</code></div><div class="credential"><label>Recovery code · keep private</label><code>${esc(recovery)}</code></div><div class="form-actions"><button id="save-code" class="secondary">Download recovery details</button><button id="continue" class="primary">I’ve saved them →</button></div></section>`);
    $('#save-code').onclick=()=>{const url=URL.createObjectURL(new Blob([`${TITLE}\nPlayer ID: ${code}\nRecovery code: ${recovery}\nKeep this recovery code private.\n`],{type:'text/plain'}));const link=document.createElement('a');link.href=url;link.download='heavens-millionaire-recovery.txt';link.click();setTimeout(()=>URL.revokeObjectURL(url),1000);};
    $('#continue').onclick=next;
  }
  async function siteLogin() { try { const result=await api('site-login',{});me=result.player;await afterLogin(); } catch(error){toast(error.message);} }
  function photoGate() {
    const affiliation = me.linked ? '' : churchFields(me.area,boot.directory.find(c=>c.church===me.church&&c.area===me.area)?.id||'');
    const introduction = me.linked
      ? (me.church==='Area Overseer' ? 'Just add your portrait. Your Area and Area Overseer role are already taken from your website account.' : 'Just add your portrait. Your Area, church and pastor are already taken from your website account.')
      : 'Add your portrait and confirm your Area and church or role before your first game.';
    setView('photo',`<section class="panel narrow"><div class="eyebrow">One more thing before the spotlight</div><h2>Let’s put a face to your name.</h2><p class="muted">Welcome, ${esc(me.name)}. ${introduction}</p><form id="photo-form"><div class="form-grid">${affiliation}${portraitFields()}</div><div class="form-actions"><button class="primary" type="submit">Save & prepare the studio →</button></div></form></section>`);
    wireChurch();$('#photo-form').onsubmit=async event=>{event.preventDefault();const button=$('[type=submit]',event.currentTarget);button.disabled=true;try{const data=new FormData(event.currentTarget);await compressPhoto(data);me=(await api('photo',data)).player;await prepare();}catch(error){toast(error.message);button.disabled=false;}};
  }
  async function afterLogin() {
    boot=await api('bootstrap');me=boot.player;
    if (!me) return welcome();
    if (!me.photo||!me.church) return photoGate();
    if (boot.active) {applyState(boot.active);return;}
    await prepare();
  }
  async function prepare() {
    if (!me?.photo) return photoGate();
    setView('loading',`<section class="loading-stage"><img class="loading-host" src="${ASSETS}Batch_02/02_host/expressions/host_encouraging.webp" alt="Your host getting ready"><div class="eyebrow">${esc(TITLE)}</div><h2>Preparing your place<br>in the studio.</h2><p class="muted">A few things to get ready. A million points to aim for.</p><div class="progress-track" role="progressbar" aria-label="Game files prepared" aria-valuemin="0" aria-valuemax="100" aria-valuenow="0"><div class="progress-fill"></div></div><div class="loading-percent"><span id="file-count">Preparing game files…</span><strong id="load-percent">0%</strong></div><p class="loading-message">${esc(messages[0])}</p><div id="load-complete"></div></section>`);
    let message=0;loadingInterval=setInterval(()=>{if($('.loading-message'))$('.loading-message').textContent=messages[++message%messages.length];},2200);
    const files=['Batch_02/03_backgrounds/base/bg_studio_clean.webp','Batch_02/03_backgrounds/stages/bg_beginner.webp','Batch_02/02_host/expressions/host_presenting.webp',...lifelines.map(l=>`Batch_04/05_lifelines/icons/lifeline_${l[2]}.webp`)];
    let done=0,failed=0;const total=files.length+1;
    const progress=()=>{done++;if(view!=='loading')return;const percent=Math.round(done/total*100);$('.progress-fill').style.width=percent+'%';$('.progress-track').setAttribute('aria-valuenow',String(percent));$('#load-percent').textContent=percent+'%';$('#file-count').textContent=`${done} of ${total} files prepared`;};
    await Promise.all(files.map(src=>new Promise(resolve=>{const image=new Image();let completed=false;const finish=ok=>{if(completed)return;completed=true;clearTimeout(timer);if(!ok)failed++;progress();resolve();};const timer=setTimeout(()=>finish(false),10000);image.onload=()=>finish(true);image.onerror=()=>finish(false);image.src=ASSETS+src;})).concat([new Promise(resolve=>{
      const preload=new Audio(ROOT+'music-1.mp3');preload.preload='auto';let completed=false;const finish=()=>{if(completed)return;completed=true;clearTimeout(timer);progress();resolve();};const timer=setTimeout(finish,8000);preload.addEventListener('canplay',finish,{once:true});preload.addEventListener('error',finish,{once:true});preload.load();
    })]));
    if(view!=='loading')return;clearInterval(loadingInterval);$('#load-complete').innerHTML=`${failed?'<p class="form-note">Some artwork could not load. You can retry, or continue with the functional game controls.</p><button class="quiet" id="retry-load">Retry files</button>':''}<button class="primary" id="enter-studio">Enter the studio →</button>`;
    $('#enter-studio').onclick=()=>{playMusic(1,true);menu();};if($('#retry-load'))$('#retry-load').onclick=prepare;
  }
  function menu() {
    if(!me)return welcome();if(!me.photo)return photoGate();
    setView('menu',`<div class="menu-welcome"><img class="avatar" src="${esc(me.photo_url)}" alt="Your portrait"><div><h2>Welcome, ${esc(me.name.split(' ')[0])}.</h2><p>${esc(me.church)} · Area ${esc(me.area)}</p></div></div><section class="menu-grid"><div class="panel menu-main"><div class="eyebrow">Your next great challenge</div><h2>Knowledge is<br>a wonderful<br><span class="gold">kind of wealth.</span></h2><p>Fifteen questions. Four lifelines.<br>How far will you go today?</p><button class="primary" id="start-game">${current?.status==='active'?'Resume your game':'Let’s play'} →</button><img src="${ROOT}host_full_clean.webp" alt="Your host"></div><div class="menu-tiles"><button class="panel menu-tile" data-nav="leaderboard"><span class="tile-symbol">♛</span><span><h3>The scoreboard</h3><p>Celebrate every player and every attempt.</p></span></button><button class="panel menu-tile" data-nav="profile"><span class="tile-symbol">✦</span><span><h3>Your journey</h3><p>Scores, milestones, and the stories behind them.</p></span></button><button class="panel menu-tile" data-nav="instructions"><span class="tile-symbol">?</span><span><h3>A little guidance</h3><p>Get to know the rules and your four lifelines.</p></span></button></div></section><p class="integrity">Proverbs 10:9 · An honest walk is a confident walk. <span class="muted">(Original paraphrase)</span></p><div class="form-actions"><button class="quiet" id="game-logout">Sign out of game</button>${boot.admin?'<button class="quiet" data-nav="admin">Game administration</button>':''}</div>`);
    $('#start-game').onclick=async()=>{playMusic(current?.position||1,true);if(current?.status==='active'){await refresh();return;}try{const result=await api('start',{request_id:uuid()});applyState(result.state);}catch(error){toast(error.message);}};
    $('#game-logout').onclick=async()=>{try{await api('logout',{});me=null;current=null;audio?.pause();welcome();}catch(error){toast(error.message);}};
  }
  function instructions() {
    setView('instructions',`<section class="panel narrow"><div class="eyebrow">Before you take your seat</div><h2>A little knowledge.<br>A little courage.</h2><ol class="rules-list"><li>Answer 15 questions across five levels, from Beginner to Master. Each level mixes questions randomly across all topics. Consecutive questions have different categories. Questions you have already seen stay excluded from future games. Questions 1–5 allow 30 seconds; 6–10 allow 45; 11–15 allow 60.</li><li>One wrong answer or an expired timer ends the game. Correctly reach Question 5 to guarantee 1,000 points, or Question 10 to guarantee 32,000. Question 15 earns 1,000,000.</li><li>Use each lifeline once: <strong>50:50</strong> removes two wrong choices; <strong>Audience</strong> shows a simulated poll that may be wrong; <strong>Bible Hint</strong> gives a source clue; verse-identification questions show the full passage context without its reference; <strong>Change Question</strong> replaces the question without resetting your remaining time.</li><li>Lifeline animations have a fixed two-second pause. Reading a result does not stop the clock indefinitely.</li><li>Walk away before committing an answer to keep your earned points. A configured sweetened offer may add a bonus. Offer dialogs keep the question clock running.</li><li>Stay on the active game screen. The first interruption gives a warning; a second disqualifies the attempt from official rankings. Calls, app switching and tab changes may trigger this rule. The timer continues while hidden and after refresh.</li><li>All attempts are saved, including zero-point results. Play honestly, enjoy learning, and remember: these are game points with no cash value.</li></ol><button class="primary" data-nav="menu">I’m ready →</button></section>`);
  }
  function serverNow(){return clockBase+(performance.now()-perfBase)/1000;}
  function applyState(value) {
    if(current?.id===value.id&&value.server_time<current.server_time)return;
    current=value;clockBase=value.server_time;perfBase=performance.now();offset=value.server_time-Date.now()/1000;
    if(value.status==='active'&&value.outcome===null)questionScreen();else resultScreen();
    if(value.interruptions>warningShown&&value.status==='active'){warningShown=value.interruptions;toast('Game interrupted: this is your first warning. Another interruption disqualifies this attempt. The timer has continued.',12000);}
    if(value.status!=='active'){closeModal();store('hm_pending_interruption','');}
  }
  function questionScreen() {
    const s=current, bg=s.stage.toLowerCase();
    playMusic(s.position);
    setView('question',`<section class="play-layout"><div class="play-main"><div class="round-top"><span class="stage-pill">${esc(s.stage)} stage</span><span class="round-count">QUESTION <strong>${s.position}</strong> / 15</span></div><div class="studio" style="background-image:url('${ASSETS}Batch_02/03_backgrounds/stages/bg_${bg}.webp')"><div class="studio-brand"><small>WHO WANTS TO BE</small>HEAVEN’S<br>MILLIONAIRE?</div><picture class="host-wrap"><source media="(min-width:980px) and (orientation:landscape)" srcset="${ROOT}host_full_clean.webp"><img class="host" src="${ASSETS}Batch_02/02_host/expressions/host_presenting.webp" alt="Your host presenting the question"></picture><div class="timer" role="timer" aria-label="Seconds remaining">${s.total_seconds}</div></div><div class="question-area"><h2 class="question-text">${esc(s.question)}</h2><div class="answers">${s.options.map((option,i)=>`<button class="answer ${s.removed.includes(i)?'eliminated':''}" data-choice="${i}" ${s.removed.includes(i)?'disabled':''}><strong>${letters[i]}</strong><span>${esc(option)}</span></button>`).join('')}</div></div><div class="lifelines">${lifelines.map(([kind,label,icon])=>`<button class="lifeline" data-action="${kind}" ${s.lifelines.includes(kind)?'disabled':''}><img src="${ASSETS}Batch_04/05_lifelines/icons/lifeline_${icon}.webp" alt="">${label}</button>`).join('')}</div><div class="round-actions"><button class="quiet" data-walk>Walk away · ${number(s.earned)}</button>${s.offer_available&&(!s.offer||s.offer.status==='pending')?'<button class="quiet gold" data-offer>✦ Sweetened offer</button>':''}<button class="quiet ladder-toggle" id="show-ladder">Prize ladder</button></div></div><aside class="prize-ladder"><h3>THE PATH TO A MILLION</h3><ol class="ladder-list">${boot.points.map((p,i)=>`<li class="${[4,9,14].includes(i)?'milestone ':''}${i+1<s.position?'passed ':''}${i+1===s.position?'current':''}"><span>${i+1}</span><strong>${number(p)}</strong></li>`).join('')}</ol><p class="guarantee">Guaranteed so far<strong>${number(s.guaranteed)}</strong></p></aside></section>`);
    if($('#show-ladder'))$('#show-ladder').onclick=()=>$('.prize-ladder').classList.toggle('open');
    tick();
  }
  function resultScreen() {
    const s=current, correct=s.outcome==='correct', ongoing=s.status==='active';
    const milestone=correct&&[5,10].includes(s.position);
    let title=ongoing?(milestone?'A milestone worth celebrating.':'That’s the right answer!'):
      ({won:'You’re Heaven’s Millionaire!',wrong:'A good try. A little wiser.',timeout:'Time’s up. Keep learning.',walkaway:'You knew when to walk away.',sweetened:'A sweet ending to your game.',disqualified:'Attempt disqualified under competition rules.'}[s.status]||'Your game is saved.');
    let art=correct?'host_victory':'host_thinking';
    const host=s.status==='won'?ASSETS+'Batch_03/02_results/screen_grand_victory.webp':milestone?ASSETS+`Batch_03/02_results/screen_milestone_question_${s.position}.webp`:ASSETS+'Batch_02/02_host/expressions/'+art+'.webp';
    setView('result',`<section class="panel result-panel"><img class="result-art" src="${host}" alt="Your host’s reaction"><div class="eyebrow">${ongoing?`Question ${s.position} complete`:esc(TITLE)}</div><h2>${esc(title)}</h2><div class="result-points">${number(ongoing?s.earned:s.final_points)}</div><p class="muted">${ongoing?'points earned':s.status==='disqualified'?'Saved in your history · excluded from official rankings':'final points · your attempt has been saved'}</p>${milestone?`<p class="gold">${number(s.guaranteed)} points are now guaranteed.</p>`:''}<div class="explanation"><strong>${esc(s.question)}</strong>${s.correct_choice!==undefined?`<p>Correct answer: <span class="gold">${letters[s.correct_choice]}. ${esc(s.options[s.correct_choice])}</span></p>`:''}<div>${esc(s.explanation)}</div><div class="source">${esc(s.source)}</div></div><div class="result-actions">${ongoing?'<button class="primary" data-action="next">Next question →</button>':'<button class="primary" data-nav="menu">Back to the studio →</button><button class="secondary" data-nav="profile">My game history</button>'}</div></section>`);
  }
  async function refresh(){if(!current)return;const result=await api('state/'+current.id);applyState(result.state);}
  async function act(kind,choice,choiceText) {
    if(busy||!current)return;busy=true;
    try {
      const result=await api('action',{session_id:current.id,token:current.token,action:kind,...(choice===undefined?{}:{choice}),...(choiceText===undefined?{}:{choice_text:choiceText})});
      applyState(result.state);
      if(lifelines.some(l=>l[0]===kind)&&current.status==='active'){
        showModal('<div class="initial-load" style="margin:20px"><span class="spinner"></span><p>Your lifeline is on its way…</p></div>',false);
        const pause=Math.max(0,(current.pause_until-serverNow())*1000);await new Promise(resolve=>setTimeout(resolve,Math.min(2000,pause)));
        if(current.status!=='active'){closeModal();return;}
        const info=current.lifeline_results[kind]||{};
        if(kind==='audience')showModal(`<div class="eyebrow">A little help from the crowd</div><h2>Ask the Audience</h2><p class="muted">A simulated poll, not live voting. The audience can be wrong.</p>${(info.percentages||[]).map((v,i)=>`<div class="poll-row"><strong>${letters[i]}</strong><div class="poll-track"><span style="width:${Number(v)}%"></span></div><span>${v}%</span></div>`).join('')}`);
        else if(kind==='hint')showModal(`<div class="eyebrow">A little light on the question</div><h2>Bible Hint</h2><p class="bible-hint-context">${esc(info.hint)}</p><p class="form-note">The clock resumes after the fixed animation pause.</p>`);
        else {closeModal();toast(kind==='change'?'A fresh question. Your remaining time is preserved.':'Two incorrect choices have been removed.');}
      }
      if(kind==='offer'&&current.status==='active'&&current.offer?.status==='pending')offerDialog();
    }catch(error){toast(error.message);}
    finally{busy=false;}
  }
  function offerDialog(){showModal(`<div class="eyebrow">A little extra to take home</div><h2>A sweetened offer.</h2><img class="loading-host" style="height:160px;display:block;margin:auto" src="${ASSETS}Batch_02/02_host/expressions/host_deal_invitation.webp" alt="Your host offering a deal"><div class="result-points">${number(current.offer.total)}</div><p class="muted">Accept to finish with these points, or carry on toward a million. Your question clock keeps running.</p><div class="form-actions"><button class="primary" data-action="accept_offer">Accept & finish</button><button class="secondary" data-action="reject_offer">Keep playing</button></div>`);}
  function tick(){
    if(view!=='question'||!current||current.status!=='active')return;
    const remaining=Math.max(0,Math.ceil(current.deadline-serverNow())),paused=current.pause_until>serverNow();const timer=$('.timer');
    if(timer){timer.textContent=paused?'Pause':remaining;timer.classList.toggle('urgent',remaining<=10&&!paused);timer.classList.toggle('paused',paused);}
    if(!remaining&&!timeoutBusy){timeoutBusy=true;refresh().catch(error=>toast(error.message)).finally(()=>setTimeout(()=>{timeoutBusy=false;},2000));}
  }
  setInterval(tick,150);
  setInterval(()=>{if(current?.status==='active'&&view==='question'&&!busy&&!document.hidden)refresh().catch(()=>{});},10000);

  function filters() {return `<div class="filters"><div><label for="filter-area">Area</label><select id="filter-area"><option value="">All Areas</option>${areas().map(a=>`<option value="${esc(a)}" ${ranking.area===a?'selected':''}>Area ${esc(a)}</option>`).join('')}</select></div><div><label for="filter-church">Church</label><select id="filter-church"><option value="">All churches</option>${[...new Set(boot.directory.filter(c=>!ranking.area||c.area===ranking.area).map(c=>c.church))].map(c=>`<option ${ranking.church===c?'selected':''}>${esc(c)}</option>`).join('')}</select></div><div><label for="filter-period">Time period</label><select id="filter-period">${[['all','All time'],['today','Today'],['week','This week'],['month','This month']].map(([v,t])=>`<option value="${v}" ${ranking.period===v?'selected':''}>${t}</option>`).join('')}</select></div><div><label for="filter-sort">Order</label><select id="filter-sort"><option value="score" ${ranking.sort==='score'?'selected':''}>Highest scores</option><option value="newest" ${ranking.sort==='newest'?'selected':''}>Newest attempts</option></select></div></div>`;}
  const resultLabel = value=>({won:'Millionaire',wrong:'Wrong answer',timeout:'Time expired',walkaway:'Walked away',sweetened:'Sweetened offer',disqualified:'Disqualified',active:'In progress'}[value]||value);
  function duration(ms) {
    if(ms===null||ms===undefined||!Number.isFinite(Number(ms)))return 'Not recorded';
    const total=Math.max(0,Math.round(Number(ms))),seconds=Math.floor(total/1000),minutes=Math.floor(seconds/60);
    return `${String(minutes).padStart(2,'0')}:${String(seconds%60).padStart(2,'0')}.${String(total%1000).padStart(3,'0')}`;
  }
  function recordsTable(data,top=false) {
    if(!data.records.length)return '<div class="empty">Every journey starts somewhere.<br>No game results match this view yet.</div>';
    return `<div class="table-wrap"><table><thead><tr>${top?'<th>Rank</th>':''}<th>Player</th><th>Points</th><th class="completion-time-heading">Completion time</th><th>Result</th><th>Played · Manila time</th></tr></thead><tbody>${data.records.map(r=>`<tr>${top?`<td><span class="rank ${r.rank<=3?'top':''}">${r.rank<=3?'♛ ':''}${r.rank}</span></td>`:''}<td class="player-cell">${r.photo_url?`<img class="avatar" loading="lazy" src="${esc(r.photo_url)}" alt="${esc(r.name)}’s portrait">`:'<span class="tile-symbol">✦</span>'}<span>${esc(r.name)}<small>${esc(r.church)} · Area ${esc(r.area)}</small><small class="mobile-completion">Time: ${esc(duration(r.elapsed_ms))}</small></span></td><td><span class="points">${number(r.points)}</span><small>Question ${r.position} / 15</small></td><td class="completion-time">${esc(duration(r.elapsed_ms))}</td><td><span class="status ${esc(r.result)}">${esc(resultLabel(r.result))}</span>${!r.eligible?'<small>Not ranked</small>':''}</td><td>${esc(date(r.date))}</td></tr>`).join('')}</tbody></table></div>`;
  }
  function pager(data,type){return `<div class="pagination"><button class="quiet" data-page="${data.page-1}" data-page-type="${type}" ${data.page<=1?'disabled':''}>← Previous</button><span>Page ${data.page} of ${data.pages} · ${number(data.total)} results</span><button class="quiet" data-page="${data.page+1}" data-page-type="${type}" ${data.page>=data.pages?'disabled':''}>Next →</button></div>`;}
  async function scoreboard(){
    if(!me){auth('login');toast('Sign in to view player portraits and scoreboards.');return;}
    const data=await api('leaderboard?'+new URLSearchParams(ranking));
    setView('leaderboard',`<div class="page-heading"><div><div class="eyebrow">${esc(TITLE)}</div><h2>Every score tells a story.</h2><p>Celebrate the journey. Cheer for your church.</p></div><button class="quiet" data-nav="menu">Back to studio</button></div><div class="tabs"><button class="tab ${ranking.view==='top'?'active':''}" data-ranking="top">Top scores</button><button class="tab ${ranking.view==='all'?'active':''}" data-ranking="all">All game results</button></div>${filters()}${recordsTable(data,ranking.view==='top')}${pager(data,'leaderboard')}<p class="form-note">Top Scores shows each player’s best eligible attempt. Equal points rank by fastest completion time, measured from game start to finish. Equal points and times share a rank. Disqualified attempts remain visible in All Game Results. Dates use Philippine time.</p>`);
    for(const key of ['area','church','period','sort'])$('#filter-'+key).onchange=async e=>{ranking[key]=e.target.value;if(key==='area')ranking.church='';ranking.page=1;try{await scoreboard();}catch(error){toast(error.message);}};
  }
  async function profile(){
    if(!me)return auth('login');const data=await api('profile?page='+profilePage);me=data.player;
    setView('profile',`<div class="profile-heading"><img class="avatar" src="${esc(me.photo_url)}" alt="Your portrait"><div><div class="eyebrow">Your journey</div><h2>${esc(me.name)}</h2><p>${esc(me.church)} · Area ${esc(me.area)}${(data.current_church?.pastor||me.pastor)?`<br>Pastor: ${esc(data.current_church?.pastor||me.pastor)}`:''}<br>Player ID: ${esc(me.player_code)}</p></div></div><div class="stats">${[[data.stats.games,'Games played'],[number(data.stats.highest),'Highest eligible score'],[number(data.stats.cumulative),'Cumulative eligible points'],[data.stats.victories||0,'Millionaire finishes']].map(([v,t])=>`<div class="stat"><strong>${esc(v||0)}</strong><span>${t}</span></div>`).join('')}</div><p class="muted">Most recent score: ${number(data.stats.recent)} · Highest stage: ${esc(data.stats.highest_stage)} ? Question ${data.stats.highest_question||0} / 15 · Registered ${esc(date(me.created_at))}</p><div class="page-heading"><h3>My game history</h3><button class="quiet" id="change-photo">${me.linked?'Update portrait':'Update portrait / church'}</button></div>${recordsTable(data.history)}${pager(data.history,'profile')}<button class="secondary" data-nav="menu">Back to studio</button>`);
    $('#change-photo').onclick=photoGate;
  }
  async function admin(search=''){
    if(!boot.admin)throw new Error('Administrator access is required.');const data=await api('admin?search='+encodeURIComponent(search));
    setView('admin',`<div class="page-heading"><div><div class="eyebrow">District game administration</div><h2>The studio desk.</h2><p>Game records stay separate from ministry reporting and Google Sheets.</p></div><a class="quiet" href="/bible-games/api/admin/export">Export results CSV</a></div><div class="stats">${[[data.stats.players,'Registered players'],[data.stats.attempts,'All attempts'],[data.stats.active||0,'Active games'],[data.stats.disqualified||0,'Disqualified']].map(([v,t])=>`<div class="stat"><strong>${number(v)}</strong><span>${t}</span></div>`).join('')}</div><div class="admin-grid"><section class="panel"><h3>Sweetened offer rules</h3><p class="form-note">Changes apply to new games. In-progress games keep their saved rules.</p><form id="policy-form"><label class="check"><input type="checkbox" name="offers_enabled" ${data.policy.offers_enabled?'checked':''}>Enable sweetened offers</label>${[['bonus_points','Bonus points'],['minimum_points','Minimum earned points'],['maximum_offer','Maximum total offer']].map(([key,label])=>`<label for="${key}">${label}</label><input id="${key}" name="${key}" type="number" min="0" max="1000000" value="${data.policy[key]}" required>`).join('')}<label for="offer_questions">Eligible question numbers (comma-separated)</label><input id="offer_questions" name="offer_questions" value="${data.policy.offer_questions.join(',')}" placeholder="6,11,14"><div class="form-actions"><button class="primary" type="submit">Save offer rules</button></div></form></section><section class="panel"><h3>Account recovery requests</h3><p class="form-note">Independently verify the person before approving. Church details alone are insufficient. Reset codes expire after 15 minutes.</p><div class="admin-list">${data.recovery_requests.map(r=>`<div class="admin-item"><strong>${esc(r.name)} · ${esc(r.player_code)}</strong><p>${esc(r.church)} · Area ${esc(r.area)}<br>${esc(r.note)}</p>${r.status==='pending'?`<label for="verify-${r.id}">Verification note</label><input id="verify-${r.id}" placeholder="How did you verify this person?"><button class="quiet" data-approve="${r.id}">Approve & create reset code</button>`:'<span class="status">Approved · awaiting reset</span>'}</div>`).join('')||'<p class="muted">No pending requests.</p>'}</div></section></div><section class="panel"><div class="page-heading"><h3>Find a player</h3><button class="quiet" id="review-questions">Review question bank</button></div><form id="player-search" class="form-actions"><input name="search" value="${esc(search)}" placeholder="Name or Player ID"><button class="secondary">Search</button></form><div class="admin-list">${data.players.map(p=>`<div class="admin-item"><strong>${esc(p.name)}</strong><p>${esc(p.player_code)} · ${esc(p.church)} · Area ${esc(p.area)}</p><button class="quiet" data-admin-player="${esc(p.id)}">History & interruption audit</button></div>`).join('')}</div></section><div class="admin-grid"><section class="panel"><h3>Games by date</h3>${data.by_date.map(r=>`<p class="form-note">${esc(r.date)} · ${number(r.games)} games</p>`).join('')||'<p class="muted">No attempts yet.</p>'}</section><section class="panel"><h3>Scores by church</h3>${data.by_church.map(r=>`<p class="form-note">Area ${esc(r.area)} · ${esc(r.church)} · ${number(r.games)} games · Best ${number(r.highest)}</p>`).join('')||'<p class="muted">No completed attempts yet.</p>'}</section></div><section class="panel"><h3>Recent administrative audit</h3><div class="admin-list">${data.audit.map(r=>`<div class="admin-item">${esc(date(r.created_at))} · ${esc(r.action)}<p>${esc(r.actor)} · ${esc(r.details)}</p></div>`).join('')}</div></section>`);
    $('#policy-form').onsubmit=async e=>{e.preventDefault();const form=new FormData(e.currentTarget);try{await api('admin/policy',{offers_enabled:form.has('offers_enabled'),bonus_points:Number(form.get('bonus_points')),minimum_points:Number(form.get('minimum_points')),maximum_offer:Number(form.get('maximum_offer')),offer_questions:String(form.get('offer_questions')).split(',').filter(v=>v.trim()).map(Number)});toast('Offer rules saved for new games.');}catch(error){toast(error.message);}};
    $('#player-search').onsubmit=e=>{e.preventDefault();admin(new FormData(e.currentTarget).get('search')).catch(error=>toast(error.message));};
    $('#review-questions').onclick=()=>reviewQuestions().catch(error=>toast(error.message));
  }
  async function reviewQuestions(){const data=await api('admin/questions?page='+adminQuestionPage);showModal(`<div class="eyebrow">${data.total.toLocaleString()} questions included</div><h2>Question bank</h2>${data.questions.map(q=>`<div class="question-review"><strong>${esc(q.id)} · ${esc(q.stage)} · ${esc(q.category)} · Difficulty ${q.difficulty_rating}/5</strong><p>${esc(q.question)}</p><ol type="A">${q.options.map((o,i)=>`<li class="${i===q.correct_index?'gold':''}">${esc(o)} ${i===q.correct_index?'✓':''}</li>`).join('')}</ol><p>${esc(q.explanation)}</p><small>${esc(q.difficulty_reason)}</small></div>`).join('')}<div class="form-actions"><button class="quiet" data-question-page="${adminQuestionPage-1}" ${adminQuestionPage<=1?'disabled':''}>Previous</button><span>${adminQuestionPage} / ${Math.ceil(data.total/20)}</span><button class="quiet" data-question-page="${adminQuestionPage+1}" ${adminQuestionPage>=Math.ceil(data.total/20)?'disabled':''}>Next</button></div>`);}

  async function reportInterruption(event){
    if(!current||current.status!=='active')return;
    try{const data=await api('interruption',{session_id:event.session_id||current.id,event_id:event.id,kind:event.kind||'visibility'});store('hm_pending_interruption','');if(current?.id===data.state.id)applyState(data.state);}catch{store('hm_pending_interruption',JSON.stringify(event));}
  }
  function left(kind){
    if(view!=='question'||current?.status!=='active'||current.outcome!==null||hiddenEvent)return;
    hiddenEvent={id:uuid(),session_id:current.id,kind,started:Date.now()};store('hm_pending_interruption',JSON.stringify(hiddenEvent));
    hiddenTimer=setTimeout(()=>{if(hiddenEvent&&!hiddenEvent.reported){hiddenEvent.reported=true;lastInterruption=Date.now();reportInterruption(hiddenEvent);}},800);
  }
  function returned(){
    clearTimeout(hiddenTimer);if(hiddenEvent){const event=hiddenEvent;hiddenEvent=null;if(!event.reported){if(Date.now()-event.started>=800){lastInterruption=Date.now();reportInterruption(event);}else store('hm_pending_interruption','');}}
    if(current?.status==='active'&&view==='question')refresh().catch(()=>{});
  }
  document.addEventListener('visibilitychange',()=>document.hidden?left('visibility'):returned());
  window.addEventListener('blur',()=>{if(Date.now()-lastInterruption>2000)left('focus');});
  window.addEventListener('focus',()=>{if(!document.hidden)returned();});
  window.addEventListener('online',async()=>{try{const pending=JSON.parse(stored('hm_pending_interruption','null'));if(pending)await reportInterruption(pending);if(current?.status==='active')await refresh();}catch{}});
  async function navigate(target){
    if(view==='question'&&current?.status==='active'){
      showModal('<h2>Leave the game screen?</h2><p class="muted">The question clock will keep running. Leaving counts as an interruption: the first gives a warning, the second disqualifies this attempt.</p><button class="danger" id="leave-game">Leave this screen</button>');
      $('#leave-game').onclick=async()=>{closeModal();await reportInterruption({id:uuid(),kind:'navigation',session_id:current.id});await go(target);};return;
    }
    await go(target);
  }
  async function go(target){closeModal();if(target==='welcome')welcome();else if(target==='menu')menu();else if(target==='leaderboard')await scoreboard();else if(target==='profile')await profile();else if(target==='instructions')instructions();else if(target==='admin')await admin();}
  document.addEventListener('click',async e=>{
    const el=e.target.closest('button,a');if(!el||el.disabled)return;
    try{
      if(el.classList.contains('wordmark')){e.preventDefault();await navigate('menu');}
      else if(el.dataset.nav){e.preventDefault();await navigate(el.dataset.nav);}
      else if(el.dataset.auth){auth(el.dataset.auth);}
      else if(el.hasAttribute('data-site-login'))await siteLogin();
      else if(el.dataset.choice!==undefined){
        if(busy)return;const choice=Number(el.dataset.choice),choiceText=$('span',el).textContent,choiceLabel=$('strong',el).textContent;showModal(`<div class="eyebrow">Your final answer?</div><h2>${esc(choiceLabel)}. ${esc(choiceText)}</h2><p class="form-note">The clock keeps running until your answer reaches the server.</p><button class="primary" id="confirm-answer">Lock in my answer →</button>`);
        const questionToken=current.token;$('#confirm-answer').onclick=()=>{closeModal();if(current.token!==questionToken){toast('The question has changed. Please select an answer again.');return;}act('answer',choice,choiceText);};
      }else if(el.dataset.action){closeModal();await act(el.dataset.action);}
      else if(el.hasAttribute('data-walk'))showModal(`<h2>Leave with ${number(current.earned)} points?</h2><p class="muted">You’ll keep your earned points and this attempt will be saved. The clock keeps running while you decide.</p><button class="primary" data-action="walk">Walk away</button>`);
      else if(el.hasAttribute('data-offer'))await act('offer');
      else if(el.dataset.ranking){ranking.view=el.dataset.ranking;ranking.page=1;await scoreboard();}
      else if(el.dataset.page){if(el.dataset.pageType==='profile'){profilePage=Number(el.dataset.page);await profile();}else{ranking.page=Number(el.dataset.page);await scoreboard();}}
      else if(el.dataset.approve){const result=await api('admin/recovery',{request_id:Number(el.dataset.approve),verification_note:$('#verify-'+el.dataset.approve).value});showModal(`<h2>One-time reset code</h2><p>Give this privately to the verified player. It expires in 15 minutes.</p><div class="credential"><code>${esc(result.reset_code)}</code></div>`);}
      else if(el.dataset.adminPlayer){const data=await api('admin/player/'+el.dataset.adminPlayer);showModal(`<h2>${esc(data.player.name)}</h2>${recordsTable(data.history)}<h3>Interruption records</h3><div class="admin-history"><pre>${esc(JSON.stringify(data.interruptions,null,2))}</pre></div>`);}
      else if(el.dataset.questionPage){adminQuestionPage=Number(el.dataset.questionPage);await reviewQuestions();}
    }catch(error){toast(error.message);}
  });
  async function initialize(){
    try{boot=await api('bootstrap');csrf=boot.csrf;me=boot.player;$('#account-nav').hidden=!me;
      if(boot.site_user&&(!me||me.linked)){await api('site-login',{});boot=await api('bootstrap');me=boot.player;$('#account-nav').hidden=!me;}
      if(me&&boot.active){current=boot.active;const pending=JSON.parse(stored('hm_pending_interruption','null'));if(pending&&pending.session_id===current.id&&Date.now()-pending.started>=800)await reportInterruption(pending);else store('hm_pending_interruption','');applyState(current);}
      else if(me){if(me.photo)menu();else photoGate();}else welcome();
    }catch(error){setView('error',`<section class="panel narrow"><h2>The studio is taking a moment.</h2><p>${esc(error.message)}</p><button class="primary" id="retry-bootstrap">Try again</button></section>`);$('#retry-bootstrap').onclick=initialize;}
  }
  initialize();
})();
