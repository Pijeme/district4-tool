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
  .view-tabs{display:flex;gap:10px;margin-bottom:14px;}
  .view-tab{border:none;border-radius:10px;padding:10px 14px;font-weight:800;cursor:pointer;background:#e5e7eb;color:#1f2937;}
  .view-tab.active{background:#2563eb;color:white;}
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
  @media(max-width:700px){.church-finder-grid{grid-template-columns:1fr;}#churchMap{height:480px;}.view-tabs{flex-direction:column;}}
</style>

<div class="church-finder-wrap">
  <div class="church-finder-card">
    <h1>Church Finder</h1>
    <div class="church-finder-sub">Search church details or view churches on the map.</div>

    <div class="view-tabs">
      <button type="button" id="mapTab" class="view-tab active">Map View</button>
      <button type="button" id="searchTab" class="view-tab">Search View</button>
      
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

  const areaColors = {
    "1": "#f59e0b",
    "2": "#2563eb",
    "3": "#16a34a",
    "4": "#9333ea",
    "5": "#ea580c",
    "6": "#0891b2",
    "7": "#be123c",
    "8": "#4f46e5",
    "9": "#65a30d",
    "10": "#ca8a04"
  };

  function getAreaColor(area){
    return areaColors[String(area || "").trim()] || "#334155";
  }

  const searchTab = document.getElementById("searchTab");
  const mapTab = document.getElementById("mapTab");
  const searchView = document.getElementById("searchView");
  const mapView = document.getElementById("mapView");
  const searchBox = document.getElementById("searchBox");
  const areaFilter = document.getElementById("areaFilter");
  const churches = Array.from(document.querySelectorAll(".church-card"));
  const emptyMsg = document.getElementById("emptyMsg");
  const mapEmptyMsg = document.getElementById("mapEmptyMsg");
  const nearestBtn = document.getElementById("nearestBtn");
  const nearestResult = document.getElementById("nearestResult");
  const nearestLoading = document.getElementById("nearestLoading");

  let map = null;
  let markersLayer = null;
  let markerRefs = [];

  function applyFilters(){
    const q = (searchBox.value || "").toLowerCase().trim();
    const area = areaFilter.value || "";
    let shown = 0;

    churches.forEach(card => {
      const haystack = card.dataset.search || "";
      const cardArea = card.dataset.area || "";
      const matchesText = !q || haystack.includes(q);
      const matchesArea = !area || cardArea === area;

      if(matchesText && matchesArea){
        card.style.display = "";
        shown++;
      }else{
        card.style.display = "none";
      }
    });

    emptyMsg.style.display = shown ? "none" : "block";

    if(map){
      renderMarkers();
    }
  }

  searchBox.addEventListener("input", applyFilters);
  areaFilter.addEventListener("change", applyFilters);

  searchTab.addEventListener("click", function(){
    searchTab.classList.add("active");
    mapTab.classList.remove("active");
    searchView.style.display = "block";
    mapView.style.display = "none";
  });

  mapTab.addEventListener("click", function(){
    mapTab.classList.add("active");
    searchTab.classList.remove("active");
    searchView.style.display = "none";
    mapView.style.display = "block";

    setTimeout(function(){
      initMap();
      renderMarkers();
      if(map){ map.invalidateSize(); }
    }, 100);
  });

  // Auto-open map view by default
setTimeout(function(){
  initMap();
  renderMarkers();

  if(map){
    map.invalidateSize();
  }
}, 100);

  function initMap(){
    if(map) return;

    map = L.map("churchMap").setView([7.8, 123.3], 9);

    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      maxZoom: 19,
      attribution: "&copy; OpenStreetMap"
    }).addTo(map);

    markersLayer = L.layerGroup().addTo(map);
    buildLegend();
  }

  function markerIcon(area){
    const color = getAreaColor(area);
    return L.divIcon({
      className: "",
      html: `<div class="custom-marker" style="background:${color};"><span>${area || ""}</span></div>`,
      iconSize: [24, 24],
      iconAnchor: [12, 24],
      popupAnchor: [0, -24]
    });
  }

  function filteredChurchesWithCoords(){
    const q = (searchBox.value || "").toLowerCase().trim();
    const area = areaFilter.value || "";

    return churchData.filter(c => {
      const lat = parseFloat(c.latitude);
      const lng = parseFloat(c.longitude);

      if(isNaN(lat) || isNaN(lng)) return false;

      const haystack = (
        (c.church_id || "") + " " +
        (c.name || "") + " " +
        (c.church_address || "") + " " +
        (c.area_number || "") + " " +
        (c.contact_number || "")
      ).toLowerCase();

      const matchesText = !q || haystack.includes(q);
      const matchesArea = !area || String(c.area_number || "") === area;

      return matchesText && matchesArea;
    });
  }

  function renderMarkers(){
    if(!map || !markersLayer) return;

    markersLayer.clearLayers();
    markerRefs = [];

    const points = filteredChurchesWithCoords();

    if(points.length === 0){
      mapEmptyMsg.style.display = "block";
      return;
    }

    mapEmptyMsg.style.display = "none";

    const bounds = [];

    points.forEach(c => {
      const lat = parseFloat(c.latitude);
      const lng = parseFloat(c.longitude);

      const popup = `
        <div style="min-width:220px;">
          <strong>${escapeHtml(c.church_id || "Unnamed Church")}</strong><br>
          <span>Pastor: ${escapeHtml(c.name || "-")}</span><br>
          <span>Area: ${escapeHtml(c.area_number || "-")}</span><br>
          <span>Address: ${escapeHtml(c.church_address || "-")}</span><br>
          <span>Contact: ${escapeHtml(c.contact_number || "-")}</span><br>
          ${c.google_pin_location ? `<a href="${escapeAttr(c.google_pin_location)}" target="_blank" rel="noopener">📍 Open in Google Maps</a>` : ""}
        </div>
      `;

      const marker = L.marker([lat, lng], {
        icon: markerIcon(c.area_number)
      }).bindPopup(popup);

      marker.addTo(markersLayer);
      markerRefs.push({ church: c, marker, lat, lng });
      bounds.push([lat, lng]);
    });

    if(bounds.length === 1){
      map.setView(bounds[0], 14);
    }else{
      map.fitBounds(bounds, { padding: [30, 30] });
    }
  }

  function buildLegend(){
    const legend = document.getElementById("areaLegend");
    const areas = [...new Set(churchData.map(c => String(c.area_number || "").trim()).filter(Boolean))]
      .sort((a,b) => Number(a) - Number(b));

    legend.innerHTML = areas.map(area => `
      <div class="legend-item">
        <span class="legend-dot" style="background:${getAreaColor(area)}"></span>
        Area ${escapeHtml(area)}
      </div>
    `).join("");
  }

  function distanceKm(lat1, lng1, lat2, lng2){
    const R = 6371;
    const dLat = toRad(lat2 - lat1);
    const dLng = toRad(lng2 - lng1);
    const a =
      Math.sin(dLat/2) * Math.sin(dLat/2) +
      Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) *
      Math.sin(dLng/2) * Math.sin(dLng/2);

    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
    return R * c;
  }

  function toRad(value){
    return value * Math.PI / 180;
  }

  nearestBtn.addEventListener("click", function(){
    nearestLoading.style.display = "block";
    nearestResult.style.display = "block";
    nearestResult.innerHTML = "Finding nearby churches...";

    if(!navigator.geolocation){
      nearestLoading.style.display = "none";
      nearestResult.innerHTML = "Your browser does not support location detection.";
      return;
    }

    navigator.geolocation.getCurrentPosition(function(pos){
      const userLat = pos.coords.latitude;
      const userLng = pos.coords.longitude;
      const points = filteredChurchesWithCoords();

      if(points.length === 0){
        nearestLoading.style.display = "none";
        nearestResult.innerHTML = "No churches with coordinates found.";
        return;
      }

      const nearby = points.map(c => {
        const lat = parseFloat(c.latitude);
        const lng = parseFloat(c.longitude);
        return {
          church: c,
          distance: distanceKm(userLat, userLng, lat, lng),
          lat: lat,
          lng: lng
        };
      }).sort((a,b) => a.distance - b.distance);

      const top5 = nearby.slice(0, 5);
      const nearest = top5[0];

      nearestLoading.style.display = "none";

      nearestResult.innerHTML = `
        <strong>5 Nearest Churches Near You</strong><br><br>
        ${top5.map((item,index) => `
          <div style="padding:10px;margin-bottom:8px;background:white;border-radius:10px;border:1px solid #bfdbfe;">
            <strong>${index+1}. ${escapeHtml(item.church.church_id || "Unnamed Church")}</strong><br>
            Pastor: ${escapeHtml(item.church.name || "-")}<br>
            Distance: ${item.distance.toFixed(2)} km<br>
            <a href="https://www.google.com/maps/dir/?api=1&destination=${item.lat},${item.lng}" target="_blank" rel="noopener">📍 Navigate</a>
          </div>
        `).join("")}
      `;

      if(map && nearest){
        map.setView([nearest.lat, nearest.lng], 13);
        const found = markerRefs.find(item =>
          item.church.church_id === nearest.church.church_id
        );
        if(found){
          found.marker.openPopup();
        }
      }

    }, function(){
      nearestLoading.style.display = "none";
      nearestResult.innerHTML = "Location permission was denied or unavailable.";
    });
  });

  function escapeHtml(str){
    return String(str || "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#039;");
  }

  function escapeAttr(str){
    return escapeHtml(str);
  }
</script>
{% endblock %}
"""


def register_church_finder_routes(app):

    @app.route("/church-finder")
    def church_finder():

        appmod = __import__("app")
        db = appmod.get_db()
        appmod.sync_from_sheets_if_needed(force=False)

        rows = db.execute(
            '''
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
            '''
        ).fetchall()

        churches = [dict(r) for r in rows]

        areas = sorted(
            {c["area_number"] for c in churches if c["area_number"]},
            key=lambda x: int(x) if str(x).isdigit() else 999
        )

        return render_template_string(
            CHURCH_FINDER_HTML,
            churches=churches,
            churches_json=json.dumps(churches),
            areas=areas,
        )