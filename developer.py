from flask import Blueprint, render_template_string

# -----------------------------------------------------------------------------
# IOWO District 4 Tool - About Developer Page
# One-file implementation: Python + HTML + CSS + JavaScript
# -----------------------------------------------------------------------------

developer_bp = Blueprint("developer", __name__)

GCASH_NUMBER = "09171655575"
GCASH_DISPLAY = "0917 165 5575"
GCASH_NAME = "Pijeme Franco Walawala"

DEVELOPERS = [
    {
        "name": "Pijeme Franco Walawala",
        "role": "Project Creator & Lead Developer",
        "image": "/static/developers/pijeme.png",
        "short": (
            "A full-time servant of God, Area Overseer of International One Way "
            "Outreach (IOWO) District 4 – Area 7, musician, music producer, and "
            "part-time programmer who uses technology to help make ministry more "
            "organized and efficient."
        ),
        "background": (
            "A full-time servant of God, Area Overseer of International One Way "
            "Outreach (IOWO) District 4 – Area 7, musician, music producer, and "
            "part-time programmer."
        ),
        "education": [
            "STI — Diploma in Information Technology",
            "Immanuel Bible College — AB Music, Undergraduate",
        ],
        "skills": ["Programming", "Website Design"],
        "contribution": (
            "Pijeme is the project creator and lead developer of the IOWO District 4 "
            "Tool. He programmed and developed the website with the guidance and "
            "assistance of friends and co-developers, together with modern development tools."
        ),
        "story": (
            "The District 4 Tool began through casual conversations with programmer "
            "friends who would eventually become co-developers of the project. The "
            "conversations were filled with ‘What if we could...?’ ideas. Those questions "
            "gradually became challenges, and the challenges eventually became ‘Why not?’ "
            "What started as an idea became a real project when Pijeme took the initiative "
            "to lead the team in developing the system."
        ),
        "goal": (
            "The original goal was simple: make monthly reporting easier for the pastors "
            "under Area 7. As development continued, one idea led to another. The project "
            "grew beyond an online reporting system and became a more comprehensive church "
            "and ministry tool. Today, the vision is to help the whole of IOWO District 4 "
            "use technology to make ministry more organized, accessible, and efficient."
        ),
        "intro": (
            "I am first and foremost a servant of God. Ministry has always been at the "
            "center of what I do, whether I am preaching, leading, making music, producing "
            "songs, or working with technology. Programming is not simply about building "
            "websites for me. I enjoy looking at problems we encounter in ministry and "
            "asking, ‘Can we build something that will make this easier?’"
        ),
        "verse": "Colossians 3:23",
        "verse_text": (
            "Whatever you do, work at it with all your heart, as working for the Lord, "
            "not for human masters."
        ),
        "verse_note": (
            "This has always been my go-to verse whenever I do something. It reminds me "
            "to give my very best—my 101%—because ultimately, I am doing it for the Lord."
        ),
        "message": (
            "To all our pastors, workers, leaders, and members using the District 4 Tool: "
            "this system was created for you and for the ministry we share. This project "
            "is not about showing what technology can do. It is about using technology to "
            "make our work in the ministry a little easier, more organized, and more "
            "efficient. My prayer is that every feature we develop will ultimately serve "
            "one greater purpose: to help us serve God and His people better. To God be all the glory!"
        ),
        "contacts": [
            {"label": "Mobile", "value": "0917 165 5575", "href": "tel:+639171655575"},
            {"label": "Facebook", "value": "Pijeme Franco Walawala", "href": "https://www.facebook.com/"},
            {"label": "YouTube", "value": "Pijeme550", "href": "https://www.youtube.com/"},
            {"label": "Email", "value": "pijeme.walawala@gmail.com", "href": "mailto:pijeme.walawala@gmail.com"},
        ],
    },
    {
        "name": "Bernard Balansag",
        "role": "Deployment, Infrastructure & Technical Support",
        "image": "/static/developers/bernard.png",
        "short": "A software developer and IOWO Miramonte Church guitarist with experience in web and mobile applications, database management, server hosting, deployment, technical support, and application maintenance.",
        "background": "A software developer with experience in developing and maintaining web and mobile applications. He also serves as a guitarist at IOWO Miramonte Church and enjoys using technology to create practical solutions for people and organizations.",
        "education": ["Bachelor of Science in Information Technology — Polytechnic University of the Philippines"],
        "skills": ["Web and Mobile Application Development", "Application Maintenance and Support", "Database Management", "Server Hosting and Deployment"],
        "contribution": "Bernard contributes to the District 4 Tool through hosting, deployment, technical support, and maintenance of the system, helping keep the application reliable and available.",
        "story": "Bernard became involved by helping with the technical setup, hosting, deployment, and ongoing maintenance of the District 4 Tool.",
        "goal": "His goal is to help provide a reliable and easy-to-use system that supports the needs of District 4, with the hope that the system can eventually become useful to IOWO churches around the world.",
        "intro": "I am a software developer with experience in developing and maintaining web and mobile applications. I enjoy using technology to create practical solutions that can help people and organizations with their daily work.",
        "verse": "Matthew 11:29",
        "verse_text": "Take my yoke upon you and learn from me, for I am gentle and humble in heart, and you will find rest for your souls.",
        "verse_note": "Matthew 11:29 is Bernard’s favorite Bible verse and reflects a life of learning from Christ while serving with humility.",
        "message": "Thank you so much for being part of this project and for supporting the District 4 Tool. May God bless each and every one of you with strength, knowledge, wisdom, and guidance as we continue to serve the Lord together.",
        "contacts": [{"label": "GitHub", "value": "github.com/nhardbalansag", "href": "https://github.com/nhardbalansag"}],
    },
    {
        "name": "Mervin Pabiran",
        "role": "Project Contributor",
        "image": "/static/developers/mervin.png",
        "short": "Profile information is being prepared. More details about Mervin’s contribution to the District 4 Tool will be added soon.",
        "pending": True,
        "background": "",
        "education": [],
        "skills": [],
        "contribution": "",
        "story": "",
        "goal": "",
        "intro": "",
        "verse": "",
        "verse_text": "",
        "verse_note": "",
        "message": "",
        "contacts": [],
    },
]

PAGE_HTML = r"""{% extends "base.html" %}

{% block title %}About Developer | District 4 Tool{% endblock %}

{% block content %}
<style>
  :root{
    --dev-bg:#03100e;
    --dev-panel:rgba(7,28,24,.90);
    --dev-mint:#39f5c5;
    --dev-cyan:#59e8ff;
    --dev-text:#eefcf8;
    --dev-muted:#9bb8b0;
    --dev-line:rgba(84,255,207,.18);
    --dev-shadow:0 24px 80px rgba(0,0,0,.38);
  }

  /* Keep all developer-page styling inside this page so base.html stays untouched. */
  .app-main{padding:0 !important;max-width:none !important;width:100% !important;}
  .dev-tech-page{
    min-height:calc(100vh - 72px);
    position:relative;
    color:var(--dev-text);
    background:
      radial-gradient(circle at 50% -10%,rgba(0,255,184,.18),transparent 36%),
      linear-gradient(180deg,#041512,#020b0a);
    overflow:hidden;
  }
  .dev-tech-page:before{
    content:"";position:absolute;inset:0;pointer-events:none;opacity:.22;
    background-image:
      linear-gradient(rgba(71,255,207,.11) 1px,transparent 1px),
      linear-gradient(90deg,rgba(71,255,207,.11) 1px,transparent 1px);
    background-size:48px 48px;
    mask-image:linear-gradient(to bottom,#000,transparent 94%);
  }
  .dev-tech-page:after{
    content:"";position:absolute;width:520px;height:520px;border-radius:50%;
    right:-220px;top:180px;background:rgba(0,255,187,.08);filter:blur(90px);
    pointer-events:none;animation:devDrift 9s ease-in-out infinite alternate;
  }
  @keyframes devDrift{to{transform:translate(-70px,80px) scale(1.15)}}

  .dev-hero{position:relative;z-index:1;padding:76px 20px 108px;text-align:center;}
  .dev-kicker{
    display:inline-flex;gap:9px;align-items:center;color:var(--dev-mint);
    font-size:12px;font-weight:700;letter-spacing:.22em;text-transform:uppercase;
  }
  .dev-kicker:before{content:"";width:34px;height:1px;background:var(--dev-mint);box-shadow:0 0 12px var(--dev-mint)}
  .dev-hero h1{
    font-size:clamp(38px,6vw,70px);line-height:1.02;margin:18px auto;
    max-width:980px;letter-spacing:-.04em;
  }
  .dev-hero h1 span{color:var(--dev-mint);text-shadow:0 0 28px rgba(57,245,197,.24)}
  .dev-hero p{max-width:790px;margin:auto;color:var(--dev-muted);font-size:17px;line-height:1.75}

  .dev-wrap{position:relative;z-index:2;width:min(1080px,calc(100% - 34px));margin:-52px auto 80px}
  .dev-glass{
    background:linear-gradient(145deg,rgba(9,38,32,.92),rgba(3,18,15,.90));
    border:1px solid var(--dev-line);box-shadow:var(--dev-shadow),inset 0 1px rgba(255,255,255,.04);
    backdrop-filter:blur(18px);
  }
  .dev-story{border-radius:28px;padding:32px 34px;margin-bottom:28px;position:relative;overflow:hidden}
  .dev-story:after{content:"";position:absolute;inset:auto 0 0;height:2px;background:linear-gradient(90deg,transparent,var(--dev-mint),transparent);opacity:.7}
  .dev-section-tag{font-size:11px;letter-spacing:.2em;text-transform:uppercase;color:var(--dev-mint);font-weight:700}
  .dev-story h2{font-size:28px;margin:8px 0 10px}
  .dev-story p{margin:0;color:var(--dev-muted);line-height:1.8}

  /* Vertical developer list */
  .dev-list{display:flex;flex-direction:column;gap:24px}
  .dev-card{
    width:100%;border-radius:28px;padding:30px 34px;position:relative;overflow:hidden;
    transition:transform .35s,border-color .35s,box-shadow .35s;
  }
  .dev-card:hover{border-color:rgba(57,245,197,.4);box-shadow:0 30px 90px rgba(0,0,0,.5),0 0 36px rgba(57,245,197,.08)}
  .dev-summary{display:grid;grid-template-columns:180px 1fr;gap:30px;align-items:center}
  .dev-photo-ring{
    width:168px;height:168px;border-radius:50%;padding:4px;
    background:conic-gradient(from 180deg,var(--dev-mint),rgba(57,245,197,.15),var(--dev-cyan),var(--dev-mint));
    box-shadow:0 0 0 8px rgba(57,245,197,.035),0 0 38px rgba(57,245,197,.18);
  }
  .dev-photo-ring img{width:100%;height:100%;object-fit:cover;border-radius:50%;border:4px solid #061512;background:#0b211d}
  .dev-summary-copy{text-align:left}
  .dev-index{color:#668f84;font-size:11px;letter-spacing:.18em;text-transform:uppercase;margin-bottom:6px}
  .dev-card h3{font-size:27px;margin:0 0 7px;color:var(--dev-text)}
  .dev-role{color:var(--dev-mint);font-size:13px;font-weight:700;margin-bottom:14px}
  .dev-short{color:var(--dev-muted);line-height:1.72;max-width:760px;margin:0 0 20px}
  .dev-btn{
    border:0;cursor:pointer;font-weight:500;border-radius:13px;padding:12px 20px;
    transition:.25s;font-family:inherit;
  }
  .dev-btn-primary{color:#02110e;background:linear-gradient(135deg,var(--dev-mint),#66ffe0);box-shadow:0 10px 30px rgba(57,245,197,.18)}
  .dev-btn-primary:hover{transform:translateY(-2px);box-shadow:0 15px 38px rgba(57,245,197,.3)}

  .dev-details{
    max-height:0;opacity:0;overflow:hidden;text-align:left;margin-top:0;padding-top:0;
    border-top:1px solid transparent;transform:translateY(-12px);
    transition:max-height .78s cubic-bezier(.2,.8,.2,1),opacity .38s,transform .52s,margin .52s,padding .52s,border-color .52s;
  }
  .dev-details.open{
    max-height:4600px;opacity:1;transform:none;margin-top:28px;padding-top:27px;border-color:var(--dev-line);
  }
  .dev-section{margin-bottom:25px;opacity:0;transform:translateY(12px);transition:opacity .4s ease,transform .4s ease}
  .dev-details.open .dev-section{opacity:1;transform:none}
  .dev-details.open .dev-section:nth-child(2){transition-delay:.04s}
  .dev-details.open .dev-section:nth-child(3){transition-delay:.08s}
  .dev-details.open .dev-section:nth-child(4){transition-delay:.12s}
  .dev-section h4{margin:0 0 9px;color:var(--dev-mint);font-size:12px;letter-spacing:.14em;text-transform:uppercase}
  .dev-section p,.dev-section li{color:#b5cbc5;line-height:1.75}
  .dev-section ul{margin:7px 0 0;padding-left:20px}
  .dev-quote{border:1px solid rgba(57,245,197,.18);border-left:3px solid var(--dev-mint);background:rgba(57,245,197,.055);padding:16px 18px;border-radius:4px 14px 14px 4px;color:#d9f9f0;font-style:italic}
  .dev-contacts{display:grid;gap:9px}
  .dev-contact{display:flex;gap:12px;align-items:center;padding:12px 14px;color:#dffbf4;text-decoration:none;border:1px solid rgba(57,245,197,.11);background:rgba(255,255,255,.025);border-radius:12px;overflow-wrap:anywhere}
  .dev-contact:hover{border-color:rgba(57,245,197,.35);background:rgba(57,245,197,.06)}
  .dev-contact strong{min-width:76px;color:var(--dev-mint)}
  .dev-show-less{display:block;margin:22px auto 0;background:rgba(255,255,255,.06);color:#d9eee9;border:1px solid var(--dev-line)}
  .dev-pending{color:#7ea097;font-size:13px;margin-top:10px}
  .dev-footer{position:relative;z-index:2;text-align:center;color:#718f87;font-size:13px;padding:0 20px 38px}

  /* Futuristic donation portal */
  .dev-modal-backdrop{
    position:fixed;inset:0;z-index:19999;display:grid;place-items:center;padding:20px;
    background:rgba(0,8,7,.88);backdrop-filter:blur(13px);
    transition:opacity .42s,visibility .42s;
  }
  .dev-modal-backdrop.hidden{opacity:0;visibility:hidden;pointer-events:none}
  .dev-support-shell{
    width:min(680px,100%);position:relative;padding:1px;border-radius:30px;
    background:linear-gradient(135deg,rgba(57,245,197,.9),rgba(57,245,197,.08) 32%,rgba(89,232,255,.45) 70%,rgba(57,245,197,.75));
    box-shadow:0 0 80px rgba(57,245,197,.14),0 35px 110px rgba(0,0,0,.62);
    animation:devPortalIn .65s cubic-bezier(.16,1,.3,1);
  }
  @keyframes devPortalIn{from{opacity:0;transform:translateY(28px) scale(.94)}to{opacity:1;transform:none}}
  .dev-support-modal{
    position:relative;overflow:hidden;border-radius:29px;padding:38px 40px;
    background:linear-gradient(150deg,rgba(5,30,25,.99),rgba(2,14,12,.99));text-align:left;
  }
  .dev-support-modal:before{
    content:"";position:absolute;inset:0;opacity:.22;
    background-image:linear-gradient(rgba(57,245,197,.12) 1px,transparent 1px),linear-gradient(90deg,rgba(57,245,197,.12) 1px,transparent 1px);
    background-size:32px 32px;
  }
  .dev-support-content{position:relative;z-index:2}
  .dev-support-top{display:flex;align-items:center;gap:18px;margin-bottom:20px}
  .dev-support-icon{
    width:64px;height:64px;border-radius:20px;display:grid;place-items:center;font-size:26px;color:#03110e;
    background:linear-gradient(135deg,var(--dev-mint),#8affdf);box-shadow:0 0 30px rgba(57,245,197,.28);
    animation:devPulse 2.2s ease-in-out infinite;flex:0 0 auto;
  }
  @keyframes devPulse{50%{box-shadow:0 0 50px rgba(57,245,197,.48);transform:scale(1.035)}}
  .dev-support-label{font-size:10px;letter-spacing:.22em;color:var(--dev-mint);font-weight:700;text-transform:uppercase;margin-bottom:5px}
  .dev-support-title{
    font-family:"Brush Script MT","Segoe Script","Lucida Handwriting",cursive;
    font-weight:400;font-size:clamp(34px,6vw,52px);line-height:1.05;margin:0;color:#effffb;
    letter-spacing:.01em;
  }
  .dev-support-copy{color:#9ebbb3;line-height:1.7;margin:0 0 18px}
  .dev-voluntary{display:inline-flex;align-items:center;gap:7px;color:#91b6ac;font-size:11px}
  .dev-voluntary:before{content:"";width:6px;height:6px;border-radius:50%;background:var(--dev-mint);box-shadow:0 0 10px var(--dev-mint)}
  .dev-gcash-panel{
    border:1px solid rgba(57,245,197,.2);background:linear-gradient(135deg,rgba(57,245,197,.075),rgba(255,255,255,.025));
    border-radius:20px;padding:18px 22px;margin:20px 0;
  }
  .dev-gcash-head{display:flex;align-items:center;justify-content:space-between;gap:16px;margin-bottom:8px}
  .dev-gcash-logo{display:block;width:154px;max-width:50%;height:auto;object-fit:contain;filter:drop-shadow(0 0 2px rgba(255,255,255,.95)) drop-shadow(0 0 7px rgba(255,255,255,.45))}
  .dev-secure-copy{color:#7fa79c;font-size:11px;text-transform:uppercase;letter-spacing:.13em}
  .dev-gcash-number{font-size:clamp(27px,7vw,38px);font-weight:400;letter-spacing:.055em;color:#eafff9;margin:8px 0 4px}
  .dev-gcash-name{color:var(--dev-mint);font-size:13px}
  .dev-modal-actions{display:grid;grid-template-columns:1fr 1.2fr;gap:10px}
  .dev-copy,.dev-proceed{font-weight:400}
  .dev-copy{color:#dffaf3;background:rgba(255,255,255,.055);border:1px solid rgba(57,245,197,.16)}
  .dev-proceed{color:#02110e;background:linear-gradient(135deg,var(--dev-mint),#71ffe2);box-shadow:0 12px 32px rgba(57,245,197,.18)}
  .dev-note{font-size:12px;color:#75948c;line-height:1.55;margin:16px 0 0;text-align:center}

  @media(max-width:700px){
    .dev-hero{padding:58px 18px 92px}
    .dev-wrap{width:min(100% - 20px,1080px)}
    .dev-story,.dev-card{border-radius:20px;padding:23px}
    .dev-summary{grid-template-columns:1fr;text-align:center;gap:20px}
    .dev-photo-ring{width:150px;height:150px;margin:auto}
    .dev-summary-copy{text-align:center}
    .dev-support-modal{padding:26px 23px}
    .dev-modal-actions{grid-template-columns:1fr}
    .dev-support-top{align-items:flex-start}
    .dev-gcash-number{font-size:27px}
    .dev-gcash-logo{width:134px;max-width:58%}
  }
  @media(prefers-reduced-motion:reduce){
    .dev-tech-page:after,.dev-support-icon,.dev-support-shell{animation:none!important}
    .dev-details,.dev-section{transition-duration:.01ms!important}
  }
</style>

<div class="dev-modal-backdrop" id="supportModal" role="dialog" aria-modal="true" aria-labelledby="supportTitle">
  <div class="dev-support-shell">
    <div class="dev-support-modal">
      <div class="dev-support-content">
        <div class="dev-support-top">
          <div class="dev-support-icon">♥</div>
          <div>
            <div class="dev-support-label">Support the mission</div>
            <h2 class="dev-support-title" id="supportTitle">Help keep the tool moving forward.</h2>
          </div>
        </div>

        <p class="dev-support-copy">
          If this tool has been a blessing to your ministry, you may consider supporting its continued development and maintenance.
          Voluntary donations help cover hosting, domain, maintenance, and service fees.
        </p>
        <span class="dev-voluntary">Completely voluntary — access is never paywalled</span>

        <div class="dev-gcash-panel">
          <div class="dev-gcash-head">
            <img class="dev-gcash-logo" src="{{ url_for('static', filename='developers/Gcash Logo.png') }}" alt="GCash">
            <span class="dev-secure-copy">Secure copy</span>
          </div>
          <div class="dev-gcash-number">{{ gcash_display }}</div>
          <div class="dev-gcash-name">{{ gcash_name }}</div>
        </div>

        <div class="dev-modal-actions">
          <button type="button" class="dev-btn dev-copy" id="copyBtn" onclick="copyGCash()">Copy GCash Number</button>
          <button type="button" class="dev-btn dev-proceed" onclick="proceedToPage()">Continue →</button>
        </div>

        <p class="dev-note">Your prayers and support are greatly appreciated. To God be all the glory!</p>
      </div>
    </div>
  </div>
</div>

<div class="dev-tech-page">
  <header class="dev-hero">
    <div class="dev-kicker">Built for ministry</div>
    <h1>The People Behind <span>the Tool</span></h1>
    <p>Built through ministry, friendship, shared ideas, and a desire to use technology to make the work of the church more organized, accessible, and efficient.</p>
  </header>

  <main class="dev-wrap">
    <section class="dev-story dev-glass">
      <div class="dev-section-tag">Origin / Collaboration</div>
      <h2>Our Story</h2>
      <p>The District 4 Tool grew from a simple goal: make ministry reporting easier. Through conversations, ideas, and collaboration, that simple reporting solution developed into a growing digital tool for pastors, workers, churches, and the whole District 4. This page recognizes the people who helped turn those ideas into something useful.</p>
    </section>

    <section class="dev-list">
      {% for dev in developers %}
      <article class="dev-card dev-glass" data-developer-card>
        <div class="dev-summary">
          <div class="dev-photo-ring">
            <img src="{{ dev.image }}" alt="{{ dev.name }}" onerror="this.style.opacity='.22'">
          </div>
          <div class="dev-summary-copy">
            <div class="dev-index">Developer {{ loop.index|string }}</div>
            <h3>{{ dev.name }}</h3>
            <div class="dev-role">{{ dev.role }}</div>
            <p class="dev-short">{{ dev.short }}</p>

            {% if dev.pending %}
              <div class="dev-pending">Full profile coming soon.</div>
            {% else %}
              <button type="button" class="dev-btn dev-btn-primary know-more" onclick="toggleProfile(this)">Know More →</button>
            {% endif %}
          </div>
        </div>

        {% if not dev.pending %}
        <div class="dev-details">
          <div class="dev-section"><h4>Professional Background</h4><p>{{ dev.background }}</p></div>
          <div class="dev-section"><h4>Education</h4><ul>{% for item in dev.education %}<li>{{ item }}</li>{% endfor %}</ul></div>
          <div class="dev-section"><h4>Technical Skills</h4><ul>{% for item in dev.skills %}<li>{{ item }}</li>{% endfor %}</ul></div>
          <div class="dev-section"><h4>Contribution to the District 4 Tool</h4><p>{{ dev.contribution }}</p></div>
          <div class="dev-section"><h4>How the Project Started</h4><p>{{ dev.story }}</p></div>
          <div class="dev-section"><h4>The Goal</h4><p>{{ dev.goal }}</p></div>
          <div class="dev-section"><h4>A Little About Me</h4><p>{{ dev.intro }}</p></div>
          <div class="dev-section"><h4>Favorite Bible Verse — {{ dev.verse }}</h4><div class="dev-quote">“{{ dev.verse_text }}”</div><p>{{ dev.verse_note }}</p></div>
          <div class="dev-section"><h4>Message to District 4 Tool Users</h4><p>{{ dev.message }}</p></div>
          <div class="dev-section"><h4>Contact</h4><div class="dev-contacts">{% for c in dev.contacts %}<a class="dev-contact" href="{{ c.href }}" target="_blank" rel="noopener"><strong>{{ c.label }}</strong><span>{{ c.value }}</span></a>{% endfor %}</div></div>
          <button type="button" class="dev-btn dev-show-less" onclick="toggleProfile(this)">Show Less ↑</button>
        </div>
        {% endif %}
      </article>
      {% endfor %}
    </section>
  </main>

  <footer class="dev-footer">Built to serve the ministry • To God be all the glory!</footer>
</div>

<script>
  const GCASH = {{ gcash_number|tojson }};

  function proceedToPage(){
    document.getElementById('supportModal').classList.add('hidden');
    document.body.style.overflow = '';
  }

  async function copyGCash(){
    const btn = document.getElementById('copyBtn');
    try{
      await navigator.clipboard.writeText(GCASH);
    }catch(e){
      const t = document.createElement('textarea');
      t.value = GCASH;
      document.body.appendChild(t);
      t.select();
      document.execCommand('copy');
      t.remove();
    }
    const old = btn.textContent;
    btn.textContent = '✓ Number Copied';
    setTimeout(() => btn.textContent = old, 1700);
  }

  function closeDeveloperCard(card, scrollBack){
    const details = card.querySelector('.dev-details');
    const topBtn = card.querySelector('.know-more');
    if(!details || !details.classList.contains('open')) return;
    details.classList.remove('open');
    if(topBtn) topBtn.style.display = 'inline-block';
    if(scrollBack){
      setTimeout(() => card.scrollIntoView({behavior:'smooth', block:'start'}), 120);
    }
  }

  function toggleProfile(btn){
    const card = btn.closest('[data-developer-card]');
    const details = card.querySelector('.dev-details');
    const topBtn = card.querySelector('.know-more');
    if(!details) return;

    const opening = !details.classList.contains('open');

    if(opening){
      /* Only one Know More profile can be open at a time. */
      document.querySelectorAll('[data-developer-card]').forEach(otherCard => {
        if(otherCard !== card) closeDeveloperCard(otherCard, false);
      });
      details.classList.add('open');
      if(topBtn) topBtn.style.display = 'none';
      setTimeout(() => card.scrollIntoView({behavior:'smooth', block:'start'}), 120);
    }else{
      closeDeveloperCard(card, true);
    }
  }

  document.body.style.overflow = 'hidden';
</script>
{% endblock %}
"""

@developer_bp.route("/about-developer")
def developer_page():
    return render_template_string(
        PAGE_HTML,
        developers=DEVELOPERS,
        gcash_number=GCASH_NUMBER,
        gcash_display=GCASH_DISPLAY,
        gcash_name=GCASH_NAME,
    )


def register_developer_routes(app):
    """Register the About Developer blueprint with the main Flask app."""
    app.register_blueprint(developer_bp)
