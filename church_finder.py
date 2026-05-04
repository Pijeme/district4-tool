from flask import render_template_string
import json


CHURCH_FINDER_HTML = """
{% extends "base.html" %}

{% block title %}Church Finder{% endblock %}

{% block content %}
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>

<style>
  .church-finder-wrap{max-width:1000px;margin:0 auto;padding:18px;color:#1f2937;}
  .church-finder-card{background:#fff;border:1px solid #e5e7eb;border-radius:16px;padding:18px;box-shadow:0 8px 30px rgba(15,23,42,.05);}
  .church-finder-card h1{margin:0 0 6px;font-size:28px;}
  .church-finder-sub{color:#64748b;margin-bottom:16px;}
  .view-tabs{display:flex;gap:10px;margin-bottom:14px;flex-wrap:wrap;}
  .view-tab,.download-map-btn{border:none;border-radius:10px;padding:10px 14px;font-weight:800;cursor:pointer;background:#e5e7eb;color:#1f2937;text-decoration:none;display:inline-block;}
  .view-tab.active{background:#2563eb;color:white;}
  .download-map-btn{background:#16a34a;color:#fff;}
  .church-finder-grid{display:grid;grid-template-columns:1fr 1fr;gap:10px;}
  .church-finder-input,.church-finder-select{width:100%;box-sizing:border-box;border:1px solid #cbd5e1;border-radius:10px;padding:11px 12px;font:inherit;margin-bottom:10px;}
  .church-card{border:1px solid #e5e7eb;border-radius:14px;padding:14px;margin-top:10px;background:#fff;}
  .church-card-title{font-weight:800;font-size:17px;}
  .church-card-meta{color:#475569;line-height:1.5;margin-top:6px;font-size:14px;}
  .church-map-btn,.nearest-btn{display:inline-block;margin-top:10px;background:#2563eb;color:white;text-decoration:none;padding:9px 12px;border-radius:10px;font-weight:700;border:none;cursor:pointer;}
  .nearest-btn{margin-bottom:10px;}
  .church-finder-muted{color:#64748b;text-align:center;margin-top:20px;}
  #mapView{display:block;}
  #churchMap{width:100%;height:560px;border-radius:16px;border:1px solid #e5e7eb;overflow:hidden;margin-top:10px;}
  .area-legend{display:flex;flex-wrap:wrap;gap:8px;margin:10px 0;}
  .legend-item{display:flex;align-items:center;gap:6px;font-size:13px;color:#475569;background:#f8fafc;border:1px solid #e5e7eb;border-radius:999px;padding:6px 9px;}
  .legend-dot{width:13px;height:13px;border-radius:999px;display:inline-block;}
  .nearest-loading{display:none;margin:12px 0;height:10px;background:#dbeafe;border-radius:999px;overflow:hidden;}
  .nearest-loading-bar{height:100%;width:0%;background:#2563eb;animation:nearestLoading 1.2s infinite;}
  @keyframes nearestLoading{0%{width:0%;}50%{width:70%;}100%{width:100%;}}
  .nearest-result{display:none;background:#eff6ff;border:1px solid #bfdbfe;border-radius:12px;padding:12px;margin:8px 0 12px;color:#1e3a8a;font-size:14px;}
  .custom-marker{width:22px;height:22px;border-radius:50% 50% 50% 0;transform:rotate(-45deg);border:2px solid #fff;box-shadow:0 2px 8px rgba(0,0,0,.35);}
  .custom-marker span{display:block;transform:rotate(45deg);color:#fff;font-size:11px;line-height:18px;text-align:center;font-weight:900;}
  @media(max-width:700px){.church-finder-grid{grid-template-columns:1fr;}#churchMap{height:480px;}.view-tabs{flex-direction:column;}.view-tab,.download-map-btn{width:100%;box-sizing:border-box;text-align:center;}}
</style>

<div class="church-finder-wrap">
  <div class="church-finder-card">
    <h1>Church Finder</h1>
    <div class="church-finder-sub">Search church details or view churches on the map.</div>

    <div class="view-tabs">
      <button type="button" id="mapTab" class="view-tab active">Map View</button>
      <button type="button" id="searchTab" class="view-tab">Search View</button>
      <a class="download-map-btn" href="{{ url_for('church_finder_export_map') }}" target="_blank" rel="noopener">⬇ Download HD Map Image</a>
    </div>

    <div class="church-finder-grid">
      <input id="searchBox" class="church-finder-input" type="text" placeholder="Search church, pastor, address...">
      <select id="areaFilter" class="church-finder-select">
        <option value="">All Areas</option>
        {% for area in areas %}
          <option value="{{ area }}">Area {{ area }}</option>
        {% endfor %}
      </select>
    </div>

    <div id="searchView" style="display:none;">
      <div id="churchList">
        {% for c in churches %}
          <div class="church-card"
            data-search="{{ (c.church_id ~ ' ' ~ c.name ~ ' ' ~ c.church_address ~ ' ' ~ c.area_number ~ ' ' ~ c.contact_number)|lower }}"
            data-area="{{ c.area_number }}">
            <div class="church-card-title">{{ c.church_id or 'Unnamed Church' }}</div>
            <div class="church-card-meta">
              <strong>Address:</strong> {{ c.church_address or '-' }}<br>
              <strong>Pastor:</strong> {{ c.name or '-' }}<br>
              <strong>Area:</strong> {{ c.area_number or '-' }}<br>
              <strong>Contact:</strong> {{ c.contact_number or '-' }}
            </div>
            {% if c.google_pin_location %}
              <a class="church-map-btn" href="{{ c.google_pin_location }}" target="_blank" rel="noopener">📍 View Location</a>
            {% endif %}
          </div>
        {% endfor %}
      </div>
      <div id="emptyMsg" class="church-finder-muted" style="display:none;">No church found.</div>
    </div>

    <div id="mapView">
      <button type="button" id="nearestBtn" class="nearest-btn">📍 Find Churches Near Me</button>
      <div class="nearest-loading" id="nearestLoading"><div class="nearest-loading-bar"></div></div>
      <div id="nearestResult" class="nearest-result"></div>
      <div class="area-legend" id="areaLegend"></div>
      <div id="churchMap"></div>
      <div class="church-finder-muted" id="mapEmptyMsg" style="display:none;">No churches with latitude and longitude found.</div>
    </div>
  </div>
</div>

<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>

<script>
  const churchData = {{ churches_json|safe }};
  const areaColors = {"1":"#f59e0b","2":"#2563eb","3":"#16a34a","4":"#9333ea","5":"#ea580c","6":"#0891b2","7":"#be123c","8":"#4f46e5","9":"#65a30d","10":"#ca8a04"};
  function getAreaColor(area){ return areaColors[String(area || "").trim()] || "#334155"; }

  const searchTab=document.getElementById("searchTab"), mapTab=document.getElementById("mapTab"), searchView=document.getElementById("searchView"), mapView=document.getElementById("mapView");
  const searchBox=document.getElementById("searchBox"), areaFilter=document.getElementById("areaFilter"), churches=Array.from(document.querySelectorAll(".church-card"));
  const emptyMsg=document.getElementById("emptyMsg"), mapEmptyMsg=document.getElementById("mapEmptyMsg"), nearestBtn=document.getElementById("nearestBtn"), nearestResult=document.getElementById("nearestResult"), nearestLoading=document.getElementById("nearestLoading");

  let map=null, markersLayer=null, markerRefs=[];

  function applyFilters(){
    const q=(searchBox.value||"").toLowerCase().trim(), area=areaFilter.value||"";
    let shown=0;
    churches.forEach(card=>{
      const haystack=card.dataset.search||"", cardArea=card.dataset.area||"";
      const ok=(!q||haystack.includes(q))&&(!area||cardArea===area);
      card.style.display=ok?"":"none";
      if(ok) shown++;
    });
    emptyMsg.style.display=shown?"none":"block";
    if(map) renderMarkers();
  }

  searchBox.addEventListener("input", applyFilters);
  areaFilter.addEventListener("change", applyFilters);

  searchTab.addEventListener("click",()=>{searchTab.classList.add("active");mapTab.classList.remove("active");searchView.style.display="block";mapView.style.display="none";});
  mapTab.addEventListener("click",()=>{mapTab.classList.add("active");searchTab.classList.remove("active");searchView.style.display="none";mapView.style.display="block";setTimeout(()=>{initMap();renderMarkers();if(map)map.invalidateSize();},100);});

  setTimeout(()=>{initMap();renderMarkers();if(map)map.invalidateSize();},100);

  function initMap(){
    if(map) return;
    map=L.map("churchMap").setView([7.8,123.3],9);
    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",{maxZoom:19,attribution:"&copy; OpenStreetMap"}).addTo(map);
    markersLayer=L.layerGroup().addTo(map);
    buildLegend();
  }

  function markerIcon(area){
    const color=getAreaColor(area);
    return L.divIcon({className:"",html:`<div class="custom-marker" style="background:${color};"><span>${area||""}</span></div>`,iconSize:[24,24],iconAnchor:[12,24],popupAnchor:[0,-24]});
  }

  function filteredChurchesWithCoords(){
    const q=(searchBox.value||"").toLowerCase().trim(), area=areaFilter.value||"";
    return churchData.filter(c=>{
      const lat=parseFloat(c.latitude), lng=parseFloat(c.longitude);
      if(isNaN(lat)||isNaN(lng)) return false;
      const haystack=((c.church_id||"")+" "+(c.name||"")+" "+(c.church_address||"")+" "+(c.area_number||"")+" "+(c.contact_number||"")).toLowerCase();
      return (!q||haystack.includes(q))&&(!area||String(c.area_number||"")===area);
    });
  }

  function renderMarkers(){
    if(!map||!markersLayer) return;
    markersLayer.clearLayers(); markerRefs=[];
    const points=filteredChurchesWithCoords();
    if(points.length===0){mapEmptyMsg.style.display="block";return;}
    mapEmptyMsg.style.display="none";
    const bounds=[];
    points.forEach(c=>{
      const lat=parseFloat(c.latitude), lng=parseFloat(c.longitude);
      const popup=`<div style="min-width:220px;"><strong>${escapeHtml(c.church_id||"Unnamed Church")}</strong><br><span>Pastor: ${escapeHtml(c.name||"-")}</span><br><span>Area: ${escapeHtml(c.area_number||"-")}</span><br><span>Address: ${escapeHtml(c.church_address||"-")}</span><br><span>Contact: ${escapeHtml(c.contact_number||"-")}</span><br>${c.google_pin_location?`<a href="${escapeAttr(c.google_pin_location)}" target="_blank" rel="noopener">📍 Open in Google Maps</a>`:""}</div>`;
      const marker=L.marker([lat,lng],{icon:markerIcon(c.area_number)}).bindPopup(popup);
      marker.addTo(markersLayer); markerRefs.push({church:c,marker,lat,lng}); bounds.push([lat,lng]);
    });
    if(bounds.length===1) map.setView(bounds[0],14); else map.fitBounds(bounds,{padding:[30,30]});
  }

  function buildLegend(){
    const legend=document.getElementById("areaLegend");
    const areas=[...new Set(churchData.map(c=>String(c.area_number||"").trim()).filter(Boolean))].sort((a,b)=>Number(a)-Number(b));
    legend.innerHTML=areas.map(area=>`<div class="legend-item"><span class="legend-dot" style="background:${getAreaColor(area)}"></span>Area ${escapeHtml(area)}</div>`).join("");
  }

  function distanceKm(lat1,lng1,lat2,lng2){
    const R=6371,dLat=toRad(lat2-lat1),dLng=toRad(lng2-lng1);
    const a=Math.sin(dLat/2)**2+Math.cos(toRad(lat1))*Math.cos(toRad(lat2))*Math.sin(dLng/2)**2;
    return R*(2*Math.atan2(Math.sqrt(a),Math.sqrt(1-a)));
  }
  function toRad(value){return value*Math.PI/180;}

  nearestBtn.addEventListener("click",()=>{
    nearestLoading.style.display="block"; nearestResult.style.display="block"; nearestResult.innerHTML="Finding nearby churches...";
    if(!navigator.geolocation){nearestLoading.style.display="none"; nearestResult.innerHTML="Your browser does not support location detection."; return;}
    navigator.geolocation.getCurrentPosition(pos=>{
      const userLat=pos.coords.latitude,userLng=pos.coords.longitude,points=filteredChurchesWithCoords();
      if(points.length===0){nearestLoading.style.display="none"; nearestResult.innerHTML="No churches with coordinates found."; return;}
      const nearby=points.map(c=>{const lat=parseFloat(c.latitude),lng=parseFloat(c.longitude); return {church:c,distance:distanceKm(userLat,userLng,lat,lng),lat,lng};}).sort((a,b)=>a.distance-b.distance);
      const top5=nearby.slice(0,5), nearest=top5[0]; nearestLoading.style.display="none";
      nearestResult.innerHTML=`<strong>5 Nearest Churches Near You</strong><br><br>${top5.map((item,index)=>`<div style="padding:10px;margin-bottom:8px;background:white;border-radius:10px;border:1px solid #bfdbfe;"><strong>${index+1}. ${escapeHtml(item.church.church_id||"Unnamed Church")}</strong><br>Pastor: ${escapeHtml(item.church.name||"-")}<br>Distance: ${item.distance.toFixed(2)} km<br><a href="https://www.google.com/maps/dir/?api=1&destination=${item.lat},${item.lng}" target="_blank" rel="noopener">📍 Navigate</a></div>`).join("")}`;
      if(map&&nearest){map.setView([nearest.lat,nearest.lng],13); const found=markerRefs.find(item=>item.church.church_id===nearest.church.church_id); if(found) found.marker.openPopup();}
    },()=>{nearestLoading.style.display="none"; nearestResult.innerHTML="Location permission was denied or unavailable.";});
  });

  function escapeHtml(str){return String(str||"").replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;").replaceAll('"',"&quot;").replaceAll("'","&#039;");}
  function escapeAttr(str){return escapeHtml(str);}
</script>
{% endblock %}
"""


EXPORT_MAP_HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>International One Way Outreach Church District 4 Location Map</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link rel="preconnect" href="https://unpkg.com">
  <link rel="preconnect" href="https://cdn.jsdelivr.net">
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>

  <style>
    html,body{
      margin:0;
      padding:0;
      background:#e5e7eb;
      font-family:"Trebuchet MS","Segoe UI",Arial,sans-serif;
      color:#f8fafc;
      overflow-x:auto;
    }

    .toolbar{
      position:fixed;
      top:14px;
      left:50%;
      transform:translateX(-50%);
      z-index:9999;
      background:white;
      border:1px solid #cbd5e1;
      border-radius:14px;
      padding:10px;
      box-shadow:0 10px 30px rgba(15,23,42,.18);
      display:flex;
      gap:8px;
      flex-wrap:wrap;
      justify-content:center;
    }

    .toolbar button,.toolbar a{
      border:0;
      border-radius:10px;
      padding:10px 14px;
      font-weight:800;
      text-decoration:none;
      cursor:pointer;
      background:#2563eb;
      color:#fff;
      font-size:14px;
      white-space:nowrap;
    }

    .toolbar .green{background:#16a34a;}
    .toolbar .gray{background:#475569;}

    #exportCanvas{
      width:1400px;
      height:1980px;
      margin:90px auto 40px;
      box-shadow:0 18px 50px rgba(0,0,0,.22);
      position:relative;
      overflow:hidden;
      background:linear-gradient(180deg,#6fd0ff 0%,#1382d1 42%,#073b82 100%);
    }

    .header{
      height:250px;
      padding:34px 72px 16px;
      box-sizing:border-box;
      background:linear-gradient(180deg,rgba(255,255,255,.20),rgba(255,255,255,.03));
      color:#ffffff;
      text-align:center;
    }

    .brand-row{
      display:flex;
      align-items:center;
      justify-content:center;
      gap:16px;
      margin-bottom:10px;
      max-width:1180px;
      margin-left:auto;
      margin-right:auto;
    }

    .logo{
      width:82px;
      height:82px;
      object-fit:contain;
      flex:0 0 auto;
      filter:drop-shadow(0 4px 8px rgba(0,0,0,.35));
    }

    .title-block{
      text-align:left;
      min-width:0;
    }

    .title1{
      font-family:Georgia,"Times New Roman",serif;
      font-size:34px;
      font-weight:900;
      line-height:1.06;
      letter-spacing:.1px;
      text-shadow:0 3px 9px rgba(0,0,0,.32);
      white-space:normal;
    }

    .title2{
      font-size:26px;
      font-weight:900;
      margin-top:4px;
      letter-spacing:1px;
      text-shadow:0 3px 8px rgba(0,0,0,.30);
    }

    .verse{
      font-family:Georgia,"Times New Roman",serif;
      font-size:20px;
      font-style:italic;
      font-weight:700;
      line-height:1.25;
      margin-top:10px;
      color:#fef9c3;
      text-shadow:0 2px 6px rgba(0,0,0,.34);
    }

    .subtitle{
      font-size:16px;
      color:#e0f2fe;
      font-style:italic;
      margin-top:8px;
      text-shadow:0 2px 5px rgba(0,0,0,.25);
    }

    #map{
      position:absolute;
      left:66px;
      top:272px;
      width:1268px;
      height:1400px;
      border:3px solid rgba(255,255,255,.74);
      border-radius:28px;
      overflow:hidden;
      background:#dbeafe;
      box-shadow:0 18px 35px rgba(0,0,0,.22);
    }

    .legend{
      position:absolute;
      left:66px;
      right:66px;
      bottom:158px;
      min-height:104px;
      color:#f8fafc;
      background:rgba(3,30,70,.44);
      border:1px solid rgba(255,255,255,.25);
      border-radius:18px;
      padding:20px 24px;
      box-sizing:border-box;
      backdrop-filter:blur(3px);
    }

    .legend-title{
      font-size:20px;
      font-weight:900;
      margin-bottom:16px;
      letter-spacing:.2px;
    }

    .legend-items{
      display:flex;
      flex-wrap:wrap;
      gap:22px;
    }

    .legend-item{
      display:flex;
      align-items:center;
      gap:8px;
      font-size:17px;
      color:#e0f2fe;
      font-weight:700;
    }

    .legend-dot{
      width:15px;
      height:15px;
      border-radius:50%;
      display:inline-block;
      border:.5px solid rgba(255,255,255,.55);
    }

    .footer{
      position:absolute;
      left:66px;
      right:66px;
      bottom:42px;
      text-align:center;
      color:#dbeafe;
      font-size:18px;
      line-height:1.35;
      font-weight:700;
      text-shadow:0 2px 6px rgba(0,0,0,.28);
    }

    .footer small{
      display:block;
      font-size:14px;
      font-weight:500;
      margin-top:8px;
      color:#bfdbfe;
    }

    .export-marker{
      width:18px;
      height:18px;
      border-radius:50% 50% 50% 0;
      transform:rotate(-45deg);
      border:1.5px solid #ffffff;
      box-shadow:0 2px 8px rgba(0,0,0,.35);
    }

    .export-marker span{
      display:block;
      transform:rotate(45deg);
      color:#ffffff;
      font-size:9px;
      line-height:14px;
      text-align:center;
      font-weight:900;
      text-shadow:0 1px 2px rgba(0,0,0,.45);
    }

    .leaflet-control-attribution{
      font-size:10px!important;
    }

    .saving{
      display:none;
      position:fixed;
      inset:0;
      background:rgba(15,23,42,.5);
      z-index:10000;
      align-items:center;
      justify-content:center;
      color:#fff;
      font-size:24px;
      font-weight:900;
    }

    @media(max-width:900px){
      #exportCanvas{
        transform:scale(.48);
        transform-origin:top center;
        margin-bottom:-970px;
      }

      .toolbar{
        width:calc(100% - 30px);
      }
    }
  </style>
</head>

<body>

  <div class="toolbar">
    <button class="green" id="downloadBtn">Download HD JPG</button>
    <button id="fitCountryBtn">Fit Entire Country</button>
    <button id="fitAllBtn">Fit All Churches</button>
    <a class="gray" href="{{ url_for('church_finder') }}">Back</a>
  </div>

  <div class="saving" id="savingOverlay">
    Preparing HD image...
  </div>

  <div id="exportCanvas">

    <div class="header">
      <div class="brand-row">
        <img class="logo"
             src="{{ url_for('static', filename='img/logo.png') }}"
             alt="District 4 Logo">

        <div class="title-block">
          <div class="title1">
            International One Way Outreach Church
          </div>

          <div class="title2">
            District 4 Location Map
          </div>
        </div>
      </div>

      <div class="verse">
        “Don’t you have a saying, ‘It’s still four months until harvest’? I tell you, open your eyes and look at the fields! They are ripe for harvest.” — John 4:35
      </div>

      <div class="subtitle">
        A ministry visualization map for prayer, encouragement, and harvest vision.
      </div>
    </div>

    <div id="map"></div>

    <div class="legend">
      <div class="legend-title">
        Area Color Legend
      </div>

      <div class="legend-items" id="legendItems"></div>
    </div>

    <div class="footer">
      This printed map is for ministry visualization only and may not show exact navigation-level accuracy.
      <small>For accurate church location and directions, please visit the Church Finder page in the official website.</small>
    </div>

  </div>

  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/html-to-image@1.11.11/dist/html-to-image.min.js"></script>

  <script>
    const churchData={{ churches_json|safe }};

    const areaColors={
      "1":"#f59e0b",
      "2":"#2563eb",
      "3":"#16a34a",
      "4":"#9333ea",
      "5":"#ea580c",
      "6":"#0891b2",
      "7":"#be123c",
      "8":"#4f46e5",
      "9":"#65a30d",
      "10":"#ca8a04"
    };

    function getAreaColor(area){
      return areaColors[String(area||"").trim()]||"#334155";
    }

    function escapeHtml(str){
      return String(str||"")
        .replaceAll("&","&amp;")
        .replaceAll("<","&lt;")
        .replaceAll(">","&gt;")
        .replaceAll('"',"&quot;")
        .replaceAll("'","&#039;");
    }

    function validChurches(){
      return churchData.filter(c =>
        !isNaN(parseFloat(c.latitude)) &&
        !isNaN(parseFloat(c.longitude))
      );
    }

    const map=L.map("map",{
      zoomControl:true,
      preferCanvas:true,
      worldCopyJump:false
    }).setView([12.8797,121.7740],6);

    L.tileLayer(
      "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
      {
        maxZoom:19,
        crossOrigin:true,
        attribution:"&copy; OpenStreetMap",
        noWrap:true,
        bounds:[[-90,-180],[90,180]]
      }
    ).addTo(map);

    const markerLayer=L.layerGroup().addTo(map);
    const bounds=[];

    function markerIcon(area){
      const color=getAreaColor(area);

      return L.divIcon({
        className:"",
        html:`<div class="export-marker" style="background:${color};"><span>${area||""}</span></div>`,
        iconSize:[20,20],
        iconAnchor:[10,20],
        popupAnchor:[0,-20]
      });
    }

    validChurches().forEach(c=>{

      const lat=parseFloat(c.latitude);
      const lng=parseFloat(c.longitude);

      const marker=L.marker(
        [lat,lng],
        {icon:markerIcon(c.area_number)}
      );

      marker.bindPopup(`
        <strong>${escapeHtml(c.church_id||"Unnamed Church")}</strong><br>
        Pastor: ${escapeHtml(c.name||"-")}<br>
        Area: ${escapeHtml(c.area_number||"-")}
      `);

      marker.addTo(markerLayer);
      bounds.push([lat,lng]);
    });

    function fitEntireCountry(){

      // This is the preferred whole-Philippines ministry view.
      // It is intentionally similar to the wider view shown in your example.
      map.fitBounds(
        [[4.2,116.0],[21.5,127.3]],
        {
          padding:[18,18],
          maxZoom:6
        }
      );

      setTimeout(()=>{
        map.invalidateSize();
      },250);
    }

    function fitAllChurches(){

      if(bounds.length===1){

        map.setView(bounds[0],9);

      }else if(bounds.length>1){

        // This focuses on the actual church spread only:
        // Pampanga/top churches near the top and Mindanao churches near the bottom.
        map.fitBounds(bounds,{
          paddingTopLeft:[55,35],
          paddingBottomRight:[55,35],
          maxZoom:8
        });
      }

      setTimeout(()=>{
        map.invalidateSize();
      },250);
    }

    document
      .getElementById("fitCountryBtn")
      .addEventListener("click",fitEntireCountry);

    document
      .getElementById("fitAllBtn")
      .addEventListener("click",fitAllChurches);

    const areas=[...new Set(
      churchData
        .map(c=>String(c.area_number||"").trim())
        .filter(Boolean)
    )].sort((a,b)=>Number(a)-Number(b));

    document.getElementById("legendItems").innerHTML =
      areas.map(area=>`
        <div class="legend-item">
          <span class="legend-dot" style="background:${getAreaColor(area)}"></span>
          Area ${escapeHtml(area)}
        </div>
      `).join("");

    setTimeout(()=>{
      map.invalidateSize();
      fitEntireCountry();
    },900);

    document
      .getElementById("downloadBtn")
      .addEventListener("click",async()=>{

        const overlay=document.getElementById("savingOverlay");
        overlay.style.display="flex";

        try{

          // Preserve the exact map zoom and position currently shown.
          // Do NOT call fitEntireCountry() or fitAllChurches() here.
          map.invalidateSize();

          await new Promise(resolve=>setTimeout(resolve,1200));

          const node=document.getElementById("exportCanvas");

          const dataUrl=await htmlToImage.toJpeg(
            node,
            {
              quality:0.98,
              pixelRatio:2,
              cacheBust:true,
              backgroundColor:"#073b82",
              width:node.scrollWidth,
              height:node.scrollHeight,
              canvasWidth:node.scrollWidth * 2,
              canvasHeight:node.scrollHeight * 2,
              style:{
                transform:"none",
                transformOrigin:"top left",
                margin:"0"
              }
            }
          );

          const link=document.createElement("a");
          link.download="International_One_Way_Outreach_Church_District_4_Location_Map_HD.jpg";
          link.href=dataUrl;
          link.click();

        }catch(err){

          alert(
            "Unable to export image automatically. Please try again or use a browser screenshot. Error: "+err
          );

        }finally{

          overlay.style.display="none";
        }
      });
  </script>
</body>
</html>
"""


def _fetch_churches(appmod):
    db = appmod.get_db()
    appmod.sync_from_sheets_if_needed(force=False)
    rows = db.execute(
        """
        SELECT
          TRIM(COALESCE(age,'')) AS area_number,
          TRIM(COALESCE(sex,'')) AS church_id,
          TRIM(COALESCE(church_address,'')) AS church_address,
          TRIM(COALESCE(name,'')) AS name,
          TRIM(COALESCE(contact,'')) AS contact_number,
          TRIM(COALESCE(google_pin_location,'')) AS google_pin_location,
          TRIM(COALESCE(latitude,'')) AS latitude,
          TRIM(COALESCE(longitude,'')) AS longitude
        FROM sheet_accounts_cache
        WHERE LOWER(TRIM(COALESCE(position,'')))='pastor'
        ORDER BY CAST(age AS INTEGER), sex
        """
    ).fetchall()
    return [dict(r) for r in rows]


def register_church_finder_routes(app):

    @app.route("/church-finder")
    def church_finder():
        appmod = __import__("app")
        churches = _fetch_churches(appmod)
        areas = sorted(
            {c["area_number"] for c in churches if c["area_number"]},
            key=lambda x: int(x) if str(x).isdigit() else 999
        )
        return render_template_string(
            CHURCH_FINDER_HTML,
            churches=churches,
            churches_json=json.dumps(churches),
            areas=areas
        )

    @app.route("/church-finder/export-map")
    def church_finder_export_map():
        appmod = __import__("app")
        churches = _fetch_churches(appmod)
        return render_template_string(
            EXPORT_MAP_HTML,
            churches_json=json.dumps(churches)
        )
