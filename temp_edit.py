import io
import json
import os
import sys
import uuid
from collections import defaultdict
from datetime import datetime

from flask import request, redirect, url_for, flash, render_template_string, send_file, abort


TEMP_EDIT_USER_TOKEN = os.environ.get("TEMP_EDIT_USER_TOKEN", "temp-edit-user-2026")
TEMP_EDIT_ADMIN_TOKEN = os.environ.get("TEMP_EDIT_ADMIN_TOKEN", "temp-edit-admin-2026")
TEMP_EDIT_SHEET_NAME = "Temporary Edit"


def _appmod():
    mod = sys.modules.get("app") or sys.modules.get("__main__")
    if mod is None:
        raise RuntimeError("App module is not loaded yet.")
    return mod


def get_db():
    return _appmod().get_db()


def get_gs_client():
    return _appmod().get_gs_client()


def sync_from_sheets_if_needed(force=False):
    return _appmod().sync_from_sheets_if_needed(force=force)


def _ensure_accounts_headers(ws):
    return _appmod()._ensure_accounts_headers(ws)


def _build_account_row_from_headers(headers, payload):
    return _appmod()._build_account_row_from_headers(headers, payload)


def _ensure_temp_edit_tables():
    db = get_db()
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS temp_edit_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id TEXT NOT NULL,
            editor_name TEXT NOT NULL,
            selfie_blob BLOB NOT NULL,
            selfie_mime TEXT NOT NULL,
            submitted_date TEXT NOT NULL,
            submitted_time TEXT NOT NULL,
            area_number TEXT NOT NULL,
            church_id TEXT NOT NULL,
            old_church_address TEXT,
            new_church_address TEXT,
            old_name TEXT,
            new_name TEXT,
            old_contact TEXT,
            new_contact TEXT,
            old_birthday TEXT,
            new_birthday TEXT,
            old_google_pin_location TEXT,
            new_google_pin_location TEXT,
            decision TEXT NOT NULL DEFAULT 'pending'
        )
        """
    )

    cols = [row[1] for row in db.execute("PRAGMA table_info(temp_edit_requests)").fetchall()]
    if "old_church_address" not in cols:
        try:
            db.execute("ALTER TABLE temp_edit_requests ADD COLUMN old_church_address TEXT")
        except Exception:
            pass
    if "new_church_address" not in cols:
        try:
            db.execute("ALTER TABLE temp_edit_requests ADD COLUMN new_church_address TEXT")
        except Exception:
            pass

    db.execute("CREATE INDEX IF NOT EXISTS idx_temp_edit_batch ON temp_edit_requests(batch_id)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_temp_edit_decision ON temp_edit_requests(decision)")
    db.commit()


def _authorized(token: str, kind: str) -> bool:
    token = str(token or "").strip()
    if kind == "user":
        return token == TEMP_EDIT_USER_TOKEN
    return token == TEMP_EDIT_ADMIN_TOKEN


def _all_account_rows():
    sync_from_sheets_if_needed(force=False)
    db = get_db()
    rows = db.execute(
        """
        SELECT
            TRIM(COALESCE(age, '')) AS area_number,
            TRIM(COALESCE(sex, '')) AS church_id,
            TRIM(COALESCE(church_address, '')) AS church_address,
            TRIM(COALESCE(name, '')) AS name,
            TRIM(COALESCE(contact, '')) AS contact_number,
            TRIM(COALESCE(birthday, '')) AS birthday,
            TRIM(COALESCE(google_pin_location, '')) AS google_pin_location,
            TRIM(COALESCE(position, '')) AS position,
            TRIM(COALESCE(username, '')) AS username,
            TRIM(COALESCE(password, '')) AS password,
            TRIM(COALESCE(sub_area, '')) AS sub_area,
            sheet_row
        FROM sheet_accounts_cache
        WHERE TRIM(COALESCE(age, '')) != ''
          AND TRIM(COALESCE(sex, '')) != ''
        ORDER BY age, sex, name
        """
    ).fetchall()

    out = []
    seen = set()
    for r in rows:
        area_number = str(r["area_number"] or "").strip()
        church_id = str(r["church_id"] or "").strip()
        key = (area_number.lower(), church_id.lower())
        if not area_number or not church_id or key in seen:
            continue
        seen.add(key)
        out.append({
            "area_number": area_number,
            "church_id": church_id,
            "church_address": str(r["church_address"] or "").strip(),
            "name": str(r["name"] or "").strip(),
            "contact_number": str(r["contact_number"] or "").strip(),
            "birthday": str(r["birthday"] or "").strip(),
            "google_pin_location": str(r["google_pin_location"] or "").strip(),
            "position": str(r["position"] or "").strip(),
            "username": str(r["username"] or "").strip(),
            "password": str(r["password"] or "").strip(),
            "sub_area": str(r["sub_area"] or "").strip(),
            "sheet_row": int(r["sheet_row"] or 0),
        })
    return out


def _find_account(area_number: str, church_id: str):
    area_number = str(area_number or "").strip()
    church_id = str(church_id or "").strip()
    for row in _all_account_rows():
        if row["area_number"] == area_number and row["church_id"] == church_id:
            return row
    return None


def _compress_image_lowest(file_storage):
    raw = file_storage.read()
    if not raw:
        raise ValueError("Please take a selfie picture before submitting.")

    try:
        from PIL import Image
        img = Image.open(io.BytesIO(raw))
        img = img.convert("RGB")
        width, height = img.size
        max_side = 480
        if max(width, height) > max_side:
            ratio = max_side / float(max(width, height))
            img = img.resize((max(1, int(width * ratio)), max(1, int(height * ratio))))
        out = io.BytesIO()
        img.save(out, format="JPEG", quality=18, optimize=True)
        return out.getvalue(), "image/jpeg"
    except Exception:
        mime = getattr(file_storage, "mimetype", None) or "application/octet-stream"
        return raw, mime


def _ensure_temp_edit_sheet():
    client = get_gs_client()
    sh = client.open("District4 Data")
    try:
        ws = sh.worksheet(TEMP_EDIT_SHEET_NAME)
    except Exception:
        ws = sh.add_worksheet(title=TEMP_EDIT_SHEET_NAME, rows=1000, cols=5)

    values = ws.get_all_values()
    headers = ["Name", "Date", "Time", "Activity", "submission batch id"]
    if not values:
        ws.append_row(headers, value_input_option="USER_ENTERED")
        return ws

    current = list(values[0])
    changed = False
    for i, h in enumerate(headers):
        if i >= len(current) or str(current[i]).strip() != h:
            while len(current) <= i:
                current.append("")
            current[i] = h
            changed = True
    if changed:
        end_col = chr(ord("A") + len(current) - 1)
        ws.update(f"A1:{end_col}1", [current], value_input_option="USER_ENTERED")
    return ws


def _log_temp_edit_to_sheet(editor_name: str, submitted_date: str, submitted_time: str, activity: str, batch_id: str):
    ws = _ensure_temp_edit_sheet()
    ws.append_row(
        [editor_name, submitted_date, submitted_time, activity, batch_id],
        value_input_option="USER_ENTERED",
    )


def _update_account_in_sheet(old_row: dict, new_values: dict):
    client = get_gs_client()
    sh = client.open("District4 Data")
    ws = sh.worksheet("Accounts")
    headers = _ensure_accounts_headers(ws)
    payload = {
        "full_name": new_values.get("name", old_row.get("name", "")),
        "age": old_row.get("area_number", ""),
        "sex": old_row.get("church_id", ""),
        "church_address": new_values.get("church_address", old_row.get("church_address", "")),
        "contact_number": new_values.get("contact_number", old_row.get("contact_number", "")),
        "birthday": new_values.get("birthday", old_row.get("birthday", "")),
        "username": old_row.get("username", ""),
        "password": old_row.get("password", ""),
        "position": old_row.get("position", ""),
        "sub_area": old_row.get("sub_area", ""),
        "google_pin_location": new_values.get("google_pin_location", old_row.get("google_pin_location", "")),
    }
    row_values = [_build_account_row_from_headers(headers, payload)]
    end_col = chr(ord("A") + len(headers) - 1)
    ws.update(
        f"A{int(old_row['sheet_row'])}:{end_col}{int(old_row['sheet_row'])}",
        row_values,
        value_input_option="USER_ENTERED",
    )


def _admin_groups():
    _ensure_temp_edit_tables()
    db = get_db()
    rows = db.execute(
        """
        SELECT *
        FROM temp_edit_requests
        ORDER BY submitted_date DESC, submitted_time DESC, id DESC
        """
    ).fetchall()

    groups = defaultdict(list)
    for r in rows:
        groups[str(r["batch_id"])].append(r)

    out = []
    for batch_id, items in groups.items():
        first = items[0]
        activity = ", ".join([str(x["church_id"] or "").strip() for x in items])
        out.append({
            "batch_id": batch_id,
            "editor_name": str(first["editor_name"] or "").strip(),
            "submitted_date": str(first["submitted_date"] or "").strip(),
            "submitted_time": str(first["submitted_time"] or "").strip(),
            "activity": activity,
            "items": items,
            "count": len(items),
        })
    return out


USER_PAGE_HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Temporary Account Edit</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    body { font-family: Arial, sans-serif; background:#f8fafc; margin:0; padding:20px; color:#1f2937; }
    .wrap { max-width:980px; margin:0 auto; }
    .card { background:#fff; border:1px solid #e5e7eb; border-radius:16px; padding:18px; box-shadow:0 8px 30px rgba(15,23,42,.05); }
    h1 { margin:0 0 6px; font-size:28px; }
    .sub { color:#6b7280; margin-bottom:16px; }
    .warn { background:#fff7ed; border:1px solid #fdba74; color:#9a3412; padding:12px 14px; border-radius:12px; margin-bottom:16px; font-weight:700; }
    .flash { padding:12px 14px; border-radius:12px; margin-bottom:12px; font-weight:700; }
    .flash.success { background:#ecfdf5; color:#166534; border:1px solid #bbf7d0; }
    .flash.error { background:#fef2f2; color:#991b1b; border:1px solid #fecaca; }
    label { display:block; font-weight:700; margin-bottom:6px; }
    input, select, textarea { width:100%; box-sizing:border-box; border:1px solid #cbd5e1; border-radius:10px; padding:10px 12px; font:inherit; }
    .topbar { display:flex; justify-content:flex-end; margin-bottom:14px; gap:10px; flex-wrap:wrap; }
    button { background:#2563eb; color:#fff; border:none; border-radius:10px; padding:10px 16px; font-weight:700; cursor:pointer; }
    button:hover { background:#1d4ed8; }
    .btn-secondary { background:#e5e7eb; color:#1f2937; }
    .btn-secondary:hover { background:#d1d5db; }
    .row { display:grid; grid-template-columns:1fr 1fr; gap:14px; margin-bottom:14px; }
    .church-list { display:flex; flex-direction:column; gap:10px; margin-top:16px; }
    .church-item { border:1px solid #e5e7eb; border-radius:12px; padding:12px; background:#fff; cursor:pointer; transition:.15s; }
    .church-item:hover { border-color:#93c5fd; box-shadow:0 4px 16px rgba(37,99,235,.08); }
    .church-item.edited { background:#eff6ff; border-color:#2563eb; box-shadow:0 0 0 2px #bfdbfe inset; }
    .church-id { font-weight:800; color:#1f2937; }
    .church-meta { margin-top:6px; color:#475569; line-height:1.5; font-size:14px; }
    .empty-note { margin-top:16px; color:#6b7280; }
    .modal-backdrop { position:fixed; inset:0; background:rgba(15,23,42,.55); display:none; align-items:center; justify-content:center; padding:20px; z-index:1000; }
    .modal-backdrop.open { display:flex; }
    .modal-card { width:min(620px,100%); background:#fff; border-radius:18px; box-shadow:0 18px 50px rgba(15,23,42,.24); border:1px solid #e5e7eb; overflow:hidden; }
    .modal-head { padding:18px 18px 10px; border-bottom:1px solid #e5e7eb; }
    .modal-body { padding:18px; }
    .modal-actions { display:flex; gap:10px; justify-content:flex-end; margin-top:16px; flex-wrap:wrap; }
    .camera-preview { width:100%; max-width:220px; border-radius:12px; border:1px solid #cbd5e1; display:none; margin-top:12px; }
    .camera-note { color:#6b7280; font-size:12px; line-height:1.5; margin-top:8px; }
    .mini-help { font-size:12px; color:#6b7280; margin-top:6px; }
    @media (max-width:700px){
      .row { grid-template-columns:1fr; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    {% for category, message in messages %}
      <div class="flash {{ 'success' if category == 'success' else 'error' }}">{{ message }}</div>
    {% endfor %}

    <div class="card">
      <div class="topbar">
        <button type="button" onclick="openIdentityModal()">Submit All Changes</button>
      </div>

      <h1>Temporary Account Edit</h1>
      <div class="sub">Only people with this direct link can open this page.</div>
      <div class="warn">Please make sure all information you submit is true and correct. False, incomplete, or misleading details may be rejected.</div>

      <form id="tempEditForm" method="POST" enctype="multipart/form-data" novalidate>
        <input type="hidden" id="editor_name" name="editor_name">
        <input type="hidden" id="edited_payload" name="edited_payload">
        <input type="file" id="selfie" name="selfie" accept="image/*" capture="user" style="display:none;">

        <div class="row">
          <div>
            <label>Area Number *</label>
            <select id="area_number" name="area_number" required>
              <option value="">Choose area</option>
            </select>
          </div>
          <div>
            <label>Edited Churches</label>
            <input type="text" id="edited_count" value="0 church edited" readonly>
          </div>
        </div>

        <div class="church-list" id="church_list"></div>
        <div class="empty-note" id="empty_area_note">Choose an area to show all churches.</div>
      </form>
    </div>
  </div>

  <div class="modal-backdrop" id="editChurchModal">
    <div class="modal-card">
      <div class="modal-head">
        <h3 style="margin:0;" id="editChurchTitle">Edit Church</h3>
        <div class="sub" style="margin:6px 0 0;">Area Number and Church ID are fixed. Update the editable details below.</div>
      </div>
      <div class="modal-body">
        <div class="row">
          <div>
            <label>Area Number</label>
            <input type="text" id="modal_area_number" readonly>
          </div>
          <div>
            <label>Church ID</label>
            <input type="text" id="modal_church_id" readonly>
          </div>
        </div>

        <div class="row">
          <div class="full">
            <label>Church Address</label>
            <input type="text" id="modal_church_address">
          </div>
          <div>
            <label>Name *</label>
            <input type="text" id="modal_name">
          </div>
          <div>
            <label>Contact #</label>
            <input type="text" id="modal_contact_number">
          </div>
          <div>
            <label>Birth Day</label>
            <input type="text" id="modal_birthday">
          </div>
          <div>
            <label>GooglePinLocation</label>
            <input type="text" id="modal_google_pin_location">
          </div>
        </div>

        <div class="modal-actions">
          <button type="button" class="btn-secondary" onclick="closeEditChurchModal()">Cancel</button>
          <button type="button" onclick="saveChurchEdit()">Save Church Edit</button>
        </div>
      </div>
    </div>
  </div>

  <div class="modal-backdrop" id="identityModal">
    <div class="modal-card">
      <div class="modal-head">
        <h3 style="margin:0;">Before You Submit</h3>
        <div class="sub" style="margin:6px 0 0;">Please enter your name, then take a selfie using your camera.</div>
      </div>
      <div class="modal-body">
        <label>Your Name *</label>
        <input type="text" id="editor_name_prompt" placeholder="Enter your full name">

        <div style="margin-top:14px;">
          <button type="button" onclick="openCameraCapture()">Open Camera</button>
          <div class="camera-note">On most phones, this opens the camera directly. On some browsers or computers, it may open a camera chooser instead.</div>
          <img id="cameraPreview" class="camera-preview" alt="Selfie preview">
        </div>

        <div class="modal-actions">
          <button type="button" class="btn-secondary" onclick="closeIdentityModal()">Cancel</button>
          <button type="button" id="finalSubmitBtn" onclick="finalSubmitTempEdit()" disabled>Final Submit</button>
        </div>
      </div>
    </div>
  </div>

  <script>
    const accountRows = {{ account_rows|tojson }};
    const areaEl = document.getElementById("area_number");
    const churchListEl = document.getElementById("church_list");
    const emptyAreaNoteEl = document.getElementById("empty_area_note");
    const editedCountEl = document.getElementById("edited_count");
    const formEl = document.getElementById("tempEditForm");
    const editedPayloadEl = document.getElementById("edited_payload");
    const identityModal = document.getElementById("identityModal");
    const editorNamePromptEl = document.getElementById("editor_name_prompt");
    const hiddenEditorNameEl = document.getElementById("editor_name");
    const selfieEl = document.getElementById("selfie");
    const finalSubmitBtn = document.getElementById("finalSubmitBtn");
    const cameraPreview = document.getElementById("cameraPreview");
    const editChurchModal = document.getElementById("editChurchModal");

    const modalAreaEl = document.getElementById("modal_area_number");
    const modalChurchIdEl = document.getElementById("modal_church_id");
    const modalChurchAddressEl = document.getElementById("modal_church_address");
    const modalNameEl = document.getElementById("modal_name");
    const modalContactEl = document.getElementById("modal_contact_number");
    const modalBirthdayEl = document.getElementById("modal_birthday");
    const modalPinEl = document.getElementById("modal_google_pin_location");
    const editChurchTitleEl = document.getElementById("editChurchTitle");

    let currentEditingKey = "";
    const editedChanges = {};

    function uniqueAreas() {
      const seen = new Set();
      const out = [];
      accountRows.forEach(r => {
        const key = String(r.area_number || "").trim();
        if (!key || seen.has(key)) return;
        seen.add(key);
        out.push(key);
      });
      return out;
    }

    function escapeHtml(value) {
      return String(value || "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;");
    }

    function makeKey(areaNumber, churchId) {
      return `${String(areaNumber || "").trim()}||${String(churchId || "").trim()}`;
    }

    function fillAreas() {
      const areas = uniqueAreas();
      areaEl.innerHTML = '<option value="">Choose area</option>' + areas.map(a => `<option value="${escapeHtml(a)}">${escapeHtml(a)}</option>`).join('');
    }

    function getRowsForSelectedArea() {
      const area = String(areaEl.value || "").trim();
      if (!area) return [];
      return accountRows.filter(r => String(r.area_number || "").trim() === area);
    }

    function renderChurchList() {
      const rows = getRowsForSelectedArea();
      churchListEl.innerHTML = "";

      if (!rows.length) {
        emptyAreaNoteEl.style.display = "block";
        emptyAreaNoteEl.textContent = areaEl.value ? "No churches found in this area." : "Choose an area to show all churches.";
        return;
      }

      emptyAreaNoteEl.style.display = "none";

      rows.forEach(row => {
        const key = makeKey(row.area_number, row.church_id);
        const isEdited = !!editedChanges[key];
        const card = document.createElement("button");
        card.type = "button";
        card.className = "church-item" + (isEdited ? " edited" : "");
        card.onclick = () => openEditChurchModal(row.area_number, row.church_id);

        const effective = isEdited ? editedChanges[key] : row;
        card.innerHTML = `
          <div class="church-id">${escapeHtml(row.church_id)}</div>
          <div class="church-meta">
            <div><strong>Address:</strong> ${escapeHtml(effective.church_address || "-")}</div>
            <div><strong>Name:</strong> ${escapeHtml(effective.name || "-")}</div>
            <div><strong>Contact:</strong> ${escapeHtml(effective.contact_number || "-")}</div>
          </div>
        `;
        churchListEl.appendChild(card);
      });

      updateEditedCount();
    }

    function updateEditedCount() {
      const count = Object.keys(editedChanges).length;
      editedCountEl.value = count === 1 ? "1 church edited" : `${count} churches edited`;
    }

    function openEditChurchModal(areaNumber, churchId) {
      const base = accountRows.find(r => String(r.area_number || "").trim() === String(areaNumber || "").trim() && String(r.church_id || "").trim() === String(churchId || "").trim());
      if (!base) return;

      currentEditingKey = makeKey(areaNumber, churchId);
      const effective = editedChanges[currentEditingKey] || {
        area_number: base.area_number,
        church_id: base.church_id,
        church_address: base.church_address || "",
        name: base.name || "",
        contact_number: base.contact_number || "",
        birthday: base.birthday || "",
        google_pin_location: base.google_pin_location || "",
      };

      modalAreaEl.value = effective.area_number || "";
      modalChurchIdEl.value = effective.church_id || "";
      modalChurchAddressEl.value = effective.church_address || "";
      modalNameEl.value = effective.name || "";
      modalContactEl.value = effective.contact_number || "";
      modalBirthdayEl.value = effective.birthday || "";
      modalPinEl.value = effective.google_pin_location || "";
      editChurchTitleEl.textContent = `Edit ${effective.church_id}`;
      editChurchModal.classList.add("open");
    }

    function closeEditChurchModal() {
      editChurchModal.classList.remove("open");
      currentEditingKey = "";
    }

    function saveChurchEdit() {
      if (!currentEditingKey) return;

      const areaNumber = String(modalAreaEl.value || "").trim();
      const churchId = String(modalChurchIdEl.value || "").trim();
      const base = accountRows.find(r => String(r.area_number || "").trim() === areaNumber && String(r.church_id || "").trim() === churchId);
      if (!base) {
        closeEditChurchModal();
        return;
      }

      const newValues = {
        area_number: areaNumber,
        church_id: churchId,
        church_address: String(modalChurchAddressEl.value || "").trim(),
        name: String(modalNameEl.value || "").trim(),
        contact_number: String(modalContactEl.value || "").trim(),
        birthday: String(modalBirthdayEl.value || "").trim(),
        google_pin_location: String(modalPinEl.value || "").trim(),
      };

      if (!newValues.name) {
        alert("Name is required.");
        modalNameEl.focus();
        return;
      }

      const unchanged =
        newValues.church_address === String(base.church_address || "").trim() &&
        newValues.name === String(base.name || "").trim() &&
        newValues.contact_number === String(base.contact_number || "").trim() &&
        newValues.birthday === String(base.birthday || "").trim() &&
        newValues.google_pin_location === String(base.google_pin_location || "").trim();

      if (unchanged) {
        delete editedChanges[currentEditingKey];
      } else {
        editedChanges[currentEditingKey] = newValues;
      }

      closeEditChurchModal();
      renderChurchList();
    }

    function openIdentityModal() {
      if (!areaEl.value) {
        alert("Please choose an Area Number first.");
        areaEl.focus();
        return;
      }
      if (Object.keys(editedChanges).length === 0) {
        alert("Please edit at least one church before submitting.");
        return;
      }
      identityModal.classList.add("open");
      finalSubmitBtn.disabled = !(String(editorNamePromptEl.value || "").trim() && selfieEl.files && selfieEl.files.length);
      setTimeout(() => editorNamePromptEl.focus(), 50);
    }

    function closeIdentityModal() {
      identityModal.classList.remove("open");
    }

    function openCameraCapture() {
      const editorName = String(editorNamePromptEl.value || "").trim();
      if (!editorName) {
        alert("Please enter your name first.");
        editorNamePromptEl.focus();
        return;
      }
      hiddenEditorNameEl.value = editorName;
      selfieEl.click();
    }

    selfieEl.addEventListener("change", function() {
      const file = selfieEl.files && selfieEl.files[0];
      const editorName = String(editorNamePromptEl.value || "").trim();
      hiddenEditorNameEl.value = editorName;
      finalSubmitBtn.disabled = !(file && editorName);
      if (file) {
        const reader = new FileReader();
        reader.onload = function(e) {
          cameraPreview.src = e.target.result;
          cameraPreview.style.display = "block";
        };
        reader.readAsDataURL(file);
      } else {
        cameraPreview.removeAttribute("src");
        cameraPreview.style.display = "none";
      }
    });

    editorNamePromptEl.addEventListener("input", function() {
      const editorName = String(editorNamePromptEl.value || "").trim();
      hiddenEditorNameEl.value = editorName;
      finalSubmitBtn.disabled = !(editorName && selfieEl.files && selfieEl.files.length);
    });

    function finalSubmitTempEdit() {
      const editorName = String(editorNamePromptEl.value || "").trim();
      if (!editorName) {
        alert("Please enter your name.");
        editorNamePromptEl.focus();
        return;
      }
      if (!(selfieEl.files && selfieEl.files.length)) {
        alert("Please take a selfie first.");
        return;
      }
      if (Object.keys(editedChanges).length === 0) {
        alert("Please edit at least one church before submitting.");
        return;
      }
      hiddenEditorNameEl.value = editorName;
      editedPayloadEl.value = JSON.stringify(Object.values(editedChanges));
      formEl.submit();
    }

    fillAreas();
    areaEl.addEventListener("change", renderChurchList);

    editChurchModal.addEventListener("click", function(e) {
      if (e.target === editChurchModal) {
        closeEditChurchModal();
      }
    });

    identityModal.addEventListener("click", function(e) {
      if (e.target === identityModal) {
        closeIdentityModal();
      }
    });
  </script>
</body>
</html>
"""

ADMIN_PAGE_HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Temporary Edit Approval</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    body { font-family: Arial, sans-serif; background:#f8fafc; margin:0; padding:20px; color:#1f2937; }
    .wrap { max-width:1100px; margin:0 auto; }
    .flash { padding:12px 14px; border-radius:12px; margin-bottom:12px; font-weight:700; }
    .flash.success { background:#ecfdf5; color:#166534; border:1px solid #bbf7d0; }
    .flash.error { background:#fef2f2; color:#991b1b; border:1px solid #fecaca; }
    .list-card { background:#fff; border:1px solid #e5e7eb; border-radius:16px; padding:16px; margin-bottom:14px; box-shadow:0 8px 30px rgba(15,23,42,.05); display:flex; justify-content:space-between; gap:14px; align-items:center; }
    .list-title { font-weight:800; color:#1f2937; margin-bottom:6px; }
    .list-meta { color:#475569; line-height:1.6; }
    .modal-backdrop { position:fixed; inset:0; background:rgba(15,23,42,.55); display:none; align-items:center; justify-content:center; padding:20px; z-index:1000; }
    .modal-backdrop.open { display:flex; }
    .modal-card { width:min(980px,100%); max-height:90vh; overflow:auto; background:#fff; border-radius:18px; box-shadow:0 18px 50px rgba(15,23,42,.24); border:1px solid #e5e7eb; overflow:hidden; }
    .modal-head { padding:18px 18px 10px; border-bottom:1px solid #e5e7eb; display:flex; justify-content:space-between; gap:12px; align-items:flex-start; }
    .modal-body { padding:18px; }
    .item { border:1px solid #e5e7eb; border-radius:12px; padding:14px; margin-top:12px; background:#f8fafc; }
    .title { font-weight:800; margin-bottom:10px; }
    .grid { display:grid; grid-template-columns:180px 1fr 1fr; gap:8px 12px; }
    .label { font-weight:700; color:#334155; }
    .old { color:#7c2d12; }
    .new { color:#166534; }
    .actions { margin-top:12px; display:flex; gap:16px; flex-wrap:wrap; }
    .bottom { position:sticky; bottom:0; background:#f8fafc; padding-top:10px; margin-top:16px; }
    button { background:#2563eb; color:#fff; border:none; border-radius:10px; padding:10px 16px; font-weight:700; cursor:pointer; }
    button:hover { background:#1d4ed8; }
    .btn-secondary { background:#e5e7eb; color:#1f2937; }
    .btn-secondary:hover { background:#d1d5db; }
    .muted { color:#6b7280; }
    img { width:96px; height:96px; object-fit:cover; border-radius:12px; border:1px solid #cbd5e1; background:#fff; }
    .head-grid { display:grid; grid-template-columns:96px 1fr; gap:16px; align-items:start; }
    @media (max-width:900px){
      .grid { grid-template-columns:1fr; }
      .head-grid { grid-template-columns:1fr; }
      .list-card { flex-direction:column; align-items:stretch; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    {% for category, message in messages %}
      <div class="flash {{ 'success' if category == 'success' else 'error' }}">{{ message }}</div>
    {% endfor %}

    <form method="POST">
      {% if groups %}
        {% for group in groups %}
          <div class="list-card">
            <div>
              <div class="list-title">{{ group.editor_name }}</div>
              <div class="list-meta">
                Date: {{ group.submitted_date }}<br>
                Time: {{ group.submitted_time }}<br>
                Churches: {{ group.activity }}<br>
                Batch ID: {{ group.batch_id }}<br>
                Total edits: {{ group.count }}
              </div>
            </div>
            <div>
              <button type="button" onclick="openBatchModal('{{ group.batch_id }}')">View Submission</button>
            </div>
          </div>

          <div class="modal-backdrop" id="batchModal_{{ group.batch_id }}">
            <div class="modal-card">
              <div class="modal-head">
                <div>
                  <h3 style="margin:0;">Submission Details</h3>
                  <div class="muted" style="margin-top:6px;">Review all church edits in this submission. Approve or reject each one individually.</div>
                </div>
                <button type="button" class="btn-secondary" onclick="closeBatchModal('{{ group.batch_id }}')">Close</button>
              </div>
              <div class="modal-body">
                <div class="head-grid">
                  <div>
                    <img src="{{ url_for('temp_edit_selfie', batch_id=group.batch_id) }}?token={{ token }}" alt="Selfie">
                  </div>
                  <div class="list-meta">
                    <div><strong>Name:</strong> {{ group.editor_name }}</div>
                    <div><strong>Date:</strong> {{ group.submitted_date }}</div>
                    <div><strong>Time:</strong> {{ group.submitted_time }}</div>
                    <div><strong>Activity:</strong> {{ group.activity }}</div>
                    <div><strong>Submission Batch ID:</strong> {{ group.batch_id }}</div>
                  </div>
                </div>

                {% for item in group["items"] %}
                  <div class="item">
                    <div class="title">{{ item['church_id'] }} — {{ item['old_church_address'] or item['new_church_address'] or '-' }}</div>
                    <div class="grid">
                      <div class="label"></div>
                      <div class="label old">Old Data</div>
                      <div class="label new">New Data</div>

                      <div class="label">Church Address</div>
                      <div class="old">{{ item['old_church_address'] or '-' }}</div>
                      <div class="new">{{ item['new_church_address'] or '-' }}</div>

                      <div class="label">Name</div>
                      <div class="old">{{ item['old_name'] or '-' }}</div>
                      <div class="new">{{ item['new_name'] or '-' }}</div>

                      <div class="label">Contact #</div>
                      <div class="old">{{ item['old_contact'] or '-' }}</div>
                      <div class="new">{{ item['new_contact'] or '-' }}</div>

                      <div class="label">Birth Day</div>
                      <div class="old">{{ item['old_birthday'] or '-' }}</div>
                      <div class="new">{{ item['new_birthday'] or '-' }}</div>

                      <div class="label">GooglePinLocation</div>
                      <div class="old">{{ item['old_google_pin_location'] or '-' }}</div>
                      <div class="new">{{ item['new_google_pin_location'] or '-' }}</div>
                    </div>

                    <div class="actions">
                      <label><input type="radio" name="decision_{{ item['id'] }}" value="pending" {% if item['decision'] == 'pending' %}checked{% endif %}> Pending</label>
                      <label><input type="radio" name="decision_{{ item['id'] }}" value="approved" {% if item['decision'] == 'approved' %}checked{% endif %}> Approve</label>
                      <label><input type="radio" name="decision_{{ item['id'] }}" value="rejected" {% if item['decision'] == 'rejected' %}checked{% endif %}> Reject</label>
                    </div>
                  </div>
                {% endfor %}
              </div>
            </div>
          </div>
        {% endfor %}

        <div class="bottom">
          <button type="submit">Save Changes</button>
        </div>
      {% else %}
        <div class="list-card"><div class="muted">No pending temporary edit requests.</div></div>
      {% endif %}
    </form>
  </div>

  <script>
    function openBatchModal(batchId) {
      const el = document.getElementById("batchModal_" + batchId);
      if (el) el.classList.add("open");
    }

    function closeBatchModal(batchId) {
      const el = document.getElementById("batchModal_" + batchId);
      if (el) el.classList.remove("open");
    }

    document.querySelectorAll(".modal-backdrop").forEach(el => {
      el.addEventListener("click", function(e) {
        if (e.target === el) {
          el.classList.remove("open");
        }
      });
    });
  </script>
</body>
</html>
"""


def register_temp_edit_routes(app):
    @app.route("/temp-edit", methods=["GET", "POST"])
    def temp_edit():
        token = request.args.get("token", "")
        if not _authorized(token, "user"):
            abort(404)

        _ensure_temp_edit_tables()

        if request.method == "POST":
            editor_name = str(request.form.get("editor_name") or "").strip()
            edited_payload_raw = str(request.form.get("edited_payload") or "").strip()
            selfie = request.files.get("selfie")

            if not editor_name or not edited_payload_raw or not selfie:
                flash("Please edit at least one church, enter your name, and take a selfie picture.", "error")
                return redirect(url_for("temp_edit", token=token))

            try:
                edited_payload = json.loads(edited_payload_raw)
            except Exception:
                flash("Invalid edited data submitted.", "error")
                return redirect(url_for("temp_edit", token=token))

            if not isinstance(edited_payload, list) or not edited_payload:
                flash("Please edit at least one church before submitting.", "error")
                return redirect(url_for("temp_edit", token=token))

            try:
                selfie_blob, selfie_mime = _compress_image_lowest(selfie)
            except Exception as e:
                flash(str(e), "error")
                return redirect(url_for("temp_edit", token=token))

            now = datetime.now()
            submitted_date = now.strftime("%Y-%m-%d")
            submitted_time = now.strftime("%I:%M %p")
            batch_id = uuid.uuid4().hex[:12].upper()

            db = get_db()
            activity_list = []

            for item in edited_payload:
                area_number = str(item.get("area_number") or "").strip()
                church_id = str(item.get("church_id") or "").strip()
                new_church_address = str(item.get("church_address") or "").strip()
                new_name = str(item.get("name") or "").strip()
                new_contact = str(item.get("contact_number") or "").strip()
                new_birthday = str(item.get("birthday") or "").strip()
                new_pin = str(item.get("google_pin_location") or "").strip()

                if not area_number or not church_id or not new_name:
                    continue

                account = _find_account(area_number, church_id)
                if not account:
                    continue

                unchanged = (
                    new_church_address == str(account["church_address"] or "").strip() and
                    new_name == str(account["name"] or "").strip() and
                    new_contact == str(account["contact_number"] or "").strip() and
                    new_birthday == str(account["birthday"] or "").strip() and
                    new_pin == str(account["google_pin_location"] or "").strip()
                )
                if unchanged:
                    continue

                db.execute(
                    """
                    INSERT INTO temp_edit_requests (
                        batch_id, editor_name, selfie_blob, selfie_mime,
                        submitted_date, submitted_time, area_number, church_id,
                        old_church_address, new_church_address,
                        old_name, new_name,
                        old_contact, new_contact,
                        old_birthday, new_birthday,
                        old_google_pin_location, new_google_pin_location,
                        decision
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
                    """,
                    (
                        batch_id,
                        editor_name,
                        selfie_blob,
                        selfie_mime,
                        submitted_date,
                        submitted_time,
                        area_number,
                        church_id,
                        str(account["church_address"] or "").strip(),
                        new_church_address,
                        str(account["name"] or "").strip(),
                        new_name,
                        str(account["contact_number"] or "").strip(),
                        new_contact,
                        str(account["birthday"] or "").strip(),
                        new_birthday,
                        str(account["google_pin_location"] or "").strip(),
                        new_pin,
                    ),
                )
                activity_list.append(church_id)

            if not activity_list:
                flash("No actual changes were detected to submit.", "error")
                return redirect(url_for("temp_edit", token=token))

            db.commit()

            try:
                _log_temp_edit_to_sheet(
                    editor_name,
                    submitted_date,
                    submitted_time,
                    ", ".join(activity_list),
                    batch_id,
                )
            except Exception as e:
                print("❌ Temporary Edit sheet log failed:", e)

            flash("Your change request was submitted successfully.", "success")
            return redirect(url_for("temp_edit", token=token))

        try:
            from flask import get_flashed_messages
            flashed = get_flashed_messages(with_categories=True)
        except Exception:
            flashed = []

        account_rows = [
            {
                "area_number": r["area_number"],
                "church_id": r["church_id"],
                "church_address": r["church_address"],
                "name": r["name"],
                "contact_number": r["contact_number"],
                "birthday": r["birthday"],
                "google_pin_location": r["google_pin_location"],
            }
            for r in _all_account_rows()
        ]
        return render_template_string(USER_PAGE_HTML, account_rows=account_rows, messages=flashed)

    @app.route("/temp-edit-admin", methods=["GET", "POST"])
    def temp_edit_admin():
        token = request.args.get("token", "")
        if not _authorized(token, "admin"):
            abort(404)

        _ensure_temp_edit_tables()
        db = get_db()

        if request.method == "POST":
            rows = db.execute("SELECT id FROM temp_edit_requests").fetchall()
            for row in rows:
                req_id = int(row["id"])
                decision = str(request.form.get(f"decision_{req_id}") or "pending").strip().lower()
                if decision not in {"pending", "approved", "rejected"}:
                    decision = "pending"
                db.execute("UPDATE temp_edit_requests SET decision = ? WHERE id = ?", (decision, req_id))
            db.commit()

            process_rows = db.execute(
                "SELECT * FROM temp_edit_requests WHERE decision IN ('approved', 'rejected') ORDER BY id ASC"
            ).fetchall()

            for item in process_rows:
                if str(item["decision"]) == "approved":
                    account = _find_account(item["area_number"], item["church_id"])
                    if account:
                        _update_account_in_sheet(
                            account,
                            {
                                "church_address": item["new_church_address"],
                                "name": item["new_name"],
                                "contact_number": item["new_contact"],
                                "birthday": item["new_birthday"],
                                "google_pin_location": item["new_google_pin_location"],
                            },
                        )

            if process_rows:
                db.execute("DELETE FROM temp_edit_requests WHERE decision IN ('approved', 'rejected')")
                db.commit()
                try:
                    sync_from_sheets_if_needed(force=True)
                except Exception as e:
                    print("❌ Sync after temp edit approval failed:", e)
                flash("Approved changes were applied to Google Sheets. Rejected items were discarded.", "success")
            else:
                flash("No approved or rejected items to process.", "error")

            return redirect(url_for("temp_edit_admin", token=token))

        try:
            from flask import get_flashed_messages
            flashed = get_flashed_messages(with_categories=True)
        except Exception:
            flashed = []
        return render_template_string(ADMIN_PAGE_HTML, groups=_admin_groups(), messages=flashed, token=token)

    @app.route("/temp-edit-selfie/<batch_id>")
    def temp_edit_selfie(batch_id):
        token = request.args.get("token", "")
        if not _authorized(token, "admin"):
            abort(404)

        _ensure_temp_edit_tables()
        db = get_db()
        row = db.execute(
            "SELECT selfie_blob, selfie_mime FROM temp_edit_requests WHERE batch_id = ? ORDER BY id ASC LIMIT 1",
            (str(batch_id or "").strip(),),
        ).fetchone()
        if not row or not row["selfie_blob"]:
            abort(404)

        return send_file(
            io.BytesIO(row["selfie_blob"]),
            mimetype=str(row["selfie_mime"] or "image/jpeg"),
            as_attachment=False,
            download_name=f"{batch_id}.jpg",
        )