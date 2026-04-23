import calendar
import re
import sys
from datetime import datetime, date

import gspread
from flask import render_template, request, redirect, url_for, session, flash


def _appmod():
    mod = sys.modules.get("app") or sys.modules.get("__main__")
    if mod is None:
        raise RuntimeError("App module is not loaded yet.")
    return mod


def get_db():
    return _appmod().get_db()


def get_gs_client():
    return _appmod().get_gs_client()


def parse_sheet_date(value):
    return _appmod().parse_sheet_date(value)


def ensure_schedule_cache_loaded():
    return _appmod().ensure_schedule_cache_loaded()


def sync_from_sheets_if_needed(force=False):
    return _appmod().sync_from_sheets_if_needed(force=force)


def any_user_logged_in():
    return _appmod().any_user_logged_in()


def _ensure_accounts_headers(ws):
    return _appmod()._ensure_accounts_headers(ws)


def _build_account_row_from_headers(headers, payload):
    return _appmod()._build_account_row_from_headers(headers, payload)

DISTRICT_SCHEDULE_SHEET_NAME = "DistrictSchedule"
DISTRICT_SECRETARY_CODE = "District_Secretary_444"
CHAIN_PRAYER_SCHEDULE_SHEET_NAME = "ChainPrayerSchedules"
OTHER_ACTIVITY_TYPES = [
    "Convention",
    "Area Activities",
    "District Prayer & Fasting",
    "Others",
]
ALL_ACTIVITY_TYPES = ["Thanksgiving"] + OTHER_ACTIVITY_TYPES


def _is_valid_google_maps_link(url: str):
    url = str(url or '').strip()
    if not url:
        return True
    url_l = url.lower()
    return (
        url_l.startswith('https://maps.app.goo.gl/')
        or url_l.startswith('http://maps.app.goo.gl/')
        or url_l.startswith('https://www.google.com/maps')
        or url_l.startswith('http://www.google.com/maps')
        or url_l.startswith('https://google.com/maps')
        or url_l.startswith('http://google.com/maps')
        or url_l.startswith('https://maps.google.com/')
        or url_l.startswith('http://maps.google.com/')
    )

def _safe_schedule_end_date(start_raw: str, end_raw: str):
    start_dt = parse_sheet_date(start_raw)
    end_dt = parse_sheet_date(end_raw) if str(end_raw or "").strip() else None
    if not start_dt:
        return None, None
    if not end_dt:
        end_dt = start_dt
    if end_dt < start_dt:
        end_dt = start_dt
    return start_dt, end_dt

def _schedule_type_class(activity_type: str):
    t = str(activity_type or "").strip().lower()
    if t == "thanksgiving":
        return "type-thanksgiving"
    if t == "convention":
        return "type-convention"
    if t == "area activities":
        return "type-area"
    if t == "district prayer & fasting":
        return "type-prayer"
    return "type-other"

def _parse_joining_names(raw_value: str):
    return [x.strip() for x in str(raw_value or "").split(",") if x.strip()]

def _unique_joining_names(names):
    cleaned = []
    seen = set()
    for name in names:
        key = str(name or "").strip().lower()
        if key and key not in seen:
            seen.add(key)
            cleaned.append(str(name).strip())
    return cleaned

def _normalize_schedule_search_value(value):
    text = str(value or "").strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text

def _ensure_district_schedule_headers():
    client = get_gs_client()
    sh = client.open("District4 Data")
    try:
        ws = sh.worksheet(DISTRICT_SCHEDULE_SHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=DISTRICT_SCHEDULE_SHEET_NAME, rows=1000, cols=12)

    values = ws.get_all_values()
    headers = [
        "Church Name",
        "Church Address",
        "Pastor's Name",
        "Contact Number",
        "Activity Date Start",
        "Activity Date End",
        "Activity Type",
        "Note",
        "Joining",
        "Theme",
        "Text",
    ]
    if not values:
        ws.append_row(headers, value_input_option="USER_ENTERED")
        return ws

    current = list(values[0])
    changed = False
    for i, name in enumerate(headers):
        if name not in current:
            while len(current) <= i:
                current.append("")
            current[i] = name
            changed = True
    if changed:
        rng = f"A1:{chr(ord('A') + len(current) - 1)}1"
        ws.update(rng, [current], value_input_option="USER_ENTERED")
    return ws

def _append_district_schedule_row(payload: dict):
    ws = _ensure_district_schedule_headers()
    ws.append_row([
        payload.get("church_name", ""),
        payload.get("church_address", ""),
        payload.get("pastor_name", ""),
        payload.get("contact_number", ""),
        payload.get("activity_date_start", ""),
        payload.get("activity_date_end", ""),
        payload.get("activity_type", ""),
        payload.get("note", ""),
        payload.get("joining", ""),
        payload.get("theme", ""),
        payload.get("text", ""),
    ], value_input_option="USER_ENTERED")

def _update_district_schedule_row(sheet_row: int, payload: dict):
    ws = _ensure_district_schedule_headers()
    ws.update(f"A{sheet_row}:K{sheet_row}", [[
        payload.get("church_name", ""),
        payload.get("church_address", ""),
        payload.get("pastor_name", ""),
        payload.get("contact_number", ""),
        payload.get("activity_date_start", ""),
        payload.get("activity_date_end", ""),
        payload.get("activity_type", ""),
        payload.get("note", ""),
        payload.get("joining", ""),
        payload.get("theme", ""),
        payload.get("text", ""),
    ]], value_input_option="USER_ENTERED")

def _delete_district_schedule_row(sheet_row: int):
    ws = _ensure_district_schedule_headers()
    if int(sheet_row) > 1:
        ws.delete_rows(int(sheet_row))

def _get_schedule_row_from_cache(sheet_row: int):
    db = get_db()
    return db.execute("SELECT * FROM sheet_district_schedule_cache WHERE sheet_row = ?", (sheet_row,)).fetchone()

def _get_account_match(area_number: str, church_name: str):
    db = get_db()
    area_number = str(area_number or '').strip()
    church_name = str(church_name or '').strip()
    if area_number:
        row = db.execute(
            """
            SELECT *
            FROM sheet_accounts_cache
            WHERE TRIM(COALESCE(age,'')) = TRIM(?)
              AND TRIM(COALESCE(sex,'')) = TRIM(?)
            LIMIT 1
            """,
            (area_number, church_name),
        ).fetchone()
        if row:
            return row
    return db.execute(
        """
        SELECT *
        FROM sheet_accounts_cache
        WHERE TRIM(COALESCE(sex,'')) = TRIM(?)
        LIMIT 1
        """,
        (church_name,),
    ).fetchone()

def _update_account_google_pin(area_number: str, church_name: str, google_pin_location: str):
    acct = _get_account_match(area_number, church_name)
    if not acct or not acct['sheet_row']:
        return False
    payload = {
        'full_name': acct['name'] or '',
        'age': acct['age'] or '',
        'sex': acct['sex'] or '',
        'church_address': acct['church_address'] or '',
        'contact_number': acct['contact'] or '',
        'birthday': acct['birthday'] or '',
        'username': acct['username'] or '',
        'password': acct['password'] or '',
        'position': acct['position'] or '',
        'sub_area': acct['sub_area'] or '',
        'google_pin_location': google_pin_location or '',
    }
    client = get_gs_client()
    sh = client.open("District4 Data")
    ws = sh.worksheet("Accounts")
    headers = _ensure_accounts_headers(ws)
    row = [_build_account_row_from_headers(headers, payload)]
    end_col = chr(ord('A') + len(headers) - 1)
    ws.update(f"A{int(acct['sheet_row'])}:{end_col}{int(acct['sheet_row'])}", row, value_input_option="USER_ENTERED")
    return True

def get_schedule_search_accounts():
    db = get_db()
    rows = db.execute(
        """
        SELECT
            TRIM(age) AS area_number,
            TRIM(sex) AS church_name,
            TRIM(name) AS pastor_name,
            TRIM(church_address) AS church_address,
            TRIM(contact) AS contact_number,
            TRIM(COALESCE(google_pin_location, '')) AS google_pin_location,
            TRIM(position) AS position
        FROM sheet_accounts_cache
        WHERE TRIM(COALESCE(age, '')) != ''
          AND TRIM(COALESCE(sex, '')) != ''
          AND LOWER(TRIM(COALESCE(position, ''))) != 'area overseer'
        ORDER BY age, sex, name
        """
    ).fetchall()
    items, seen = [], set()
    for r in rows:
        area_number = str(r['area_number'] or '').strip()
        church_name = str(r['church_name'] or '').strip()
        pastor_name = str(r['pastor_name'] or '').strip()
        if not area_number or not church_name:
            continue
        key = (_normalize_schedule_search_value(area_number), _normalize_schedule_search_value(church_name))
        if key in seen:
            continue
        seen.add(key)
        items.append({
            'area_number': area_number,
            'church_name': church_name,
            'pastor_name': pastor_name,
            'church_address': str(r['church_address'] or '').strip(),
            'contact_number': str(r['contact_number'] or '').strip(),
            'google_pin_location': str(r['google_pin_location'] or '').strip(),
        })
    return items

def search_schedule_rows(area_number='', church_name='', pastor_name=''):
    db = get_db()
    area_key = _normalize_schedule_search_value(area_number)
    church_key = _normalize_schedule_search_value(church_name)
    pastor_key = _normalize_schedule_search_value(pastor_name)
    account_rows = get_schedule_search_accounts()
    matched_accounts = []
    for row in account_rows:
        row_area = _normalize_schedule_search_value(row['area_number'])
        row_church = _normalize_schedule_search_value(row['church_name'])
        row_pastor = _normalize_schedule_search_value(row['pastor_name'])
        if area_key and row_area != area_key:
            continue
        if church_key and row_church != church_key:
            continue
        if pastor_key and row_pastor and row_pastor != pastor_key:
            continue
        matched_accounts.append(row)
    allowed_churches = {_normalize_schedule_search_value(r['church_name']) for r in matched_accounts if str(r.get('church_name') or '').strip()}
    allowed_pastors = {_normalize_schedule_search_value(r['pastor_name']) for r in matched_accounts if str(r.get('pastor_name') or '').strip()}
    district_rows_all = db.execute("SELECT * FROM sheet_district_schedule_cache ORDER BY activity_date_start ASC, church_name ASC").fetchall()
    chain_rows_all = db.execute("SELECT * FROM sheet_chain_prayer_schedule_cache ORDER BY prayer_date ASC, church_name_assigned ASC").fetchall()
    district_rows, chain_rows = [], []
    for row in district_rows_all:
        row_church = _normalize_schedule_search_value(row['church_name'])
        row_pastor = _normalize_schedule_search_value(row['pastor_name'])
        if allowed_churches and row_church not in allowed_churches:
            continue
        if pastor_key and allowed_pastors and row_pastor and row_pastor not in allowed_pastors:
            continue
        district_rows.append(row)
    for row in chain_rows_all:
        row_church = _normalize_schedule_search_value(row['church_name_assigned'])
        row_pastor = _normalize_schedule_search_value(row['pastor_name'])
        if allowed_churches and row_church not in allowed_churches:
            continue
        if pastor_key and allowed_pastors and row_pastor and row_pastor not in allowed_pastors:
            continue
        chain_rows.append(row)
    return district_rows, chain_rows

def _get_schedule_rows_for_month(year: int, month: int):
    db = get_db()
    rows = db.execute("SELECT * FROM sheet_district_schedule_cache ORDER BY activity_date_start ASC, church_name ASC").fetchall()
    filtered = []
    for r in rows:
        start_dt, end_dt = _safe_schedule_end_date(r['activity_date_start'], r['activity_date_end'])
        if not start_dt:
            continue
        if start_dt.year == year and start_dt.month == month:
            filtered.append(r)
            continue
        if end_dt and end_dt.year == year and end_dt.month == month:
            filtered.append(r)
    return filtered

def build_schedule_month(year: int, month: int):
    ensure_schedule_cache_loaded()
    db = get_db()
    rows = db.execute("SELECT * FROM sheet_district_schedule_cache ORDER BY activity_date_start ASC, church_name ASC").fetchall()
    cal = calendar.Calendar(firstweekday=6)
    month_days = cal.monthdatescalendar(year, month)
    event_lookup, day_map = {}, {}
    for idx, r in enumerate(rows, start=1):
        start_dt, end_dt = _safe_schedule_end_date(r['activity_date_start'], r['activity_date_end'])
        if not start_dt:
            continue
        event_id = f"evt_{idx}"
        event_obj = {
            'id': event_id,
            'sheet_row': r['sheet_row'],
            'church_name': str(r['church_name'] or '').strip(),
            'church_address': str(r['church_address'] or '').strip(),
            'pastor_name': str(r['pastor_name'] or '').strip(),
            'contact_number': str(r['contact_number'] or '').strip(),
            'activity_date_start': str(r['activity_date_start'] or '').strip(),
            'activity_date_end': str(r['activity_date_end'] or '').strip(),
            'activity_type': str(r['activity_type'] or '').strip() or 'Others',
            'note': str(r['note'] or '').strip(),
            'joining': str(r['joining'] or '').strip(),
            'joining_count': len(_unique_joining_names(_parse_joining_names(r['joining']))),
            'type_class': _schedule_type_class(r['activity_type']),
            'theme': str(r['theme'] or '').strip(),
            'text': str(r['text'] or '').strip(),
            'google_pin_location': '',
        }
        acct = _get_account_match('', event_obj['church_name'])
        # fallback by church id only
        db_row = db.execute("SELECT TRIM(COALESCE(google_pin_location,'')) AS pin FROM sheet_accounts_cache WHERE TRIM(COALESCE(sex,'')) = TRIM(?) LIMIT 1", (event_obj['church_name'],)).fetchone()
        event_obj['google_pin_location'] = str((db_row['pin'] if db_row else '') or '').strip()
        event_lookup[event_id] = event_obj
        current = start_dt
        while current <= end_dt:
            day_map.setdefault(current.isoformat(), []).append(event_obj)
            current = current.fromordinal(current.toordinal() + 1)
    weeks = []
    for week in month_days:
        week_cells = []
        for d in week:
            key = d.isoformat()
            items = day_map.get(key, [])
            week_cells.append({'date': d, 'iso': key, 'in_month': d.month == month, 'events': items, 'visible_events': items[:2], 'extra_count': max(0, len(items)-2), 'is_today': d == date.today()})
        weeks.append(week_cells)
    month_title = datetime(year, month, 1).strftime('%B %Y')
    prev_year, prev_month = (year - 1, 12) if month == 1 else (year, month - 1)
    next_year, next_month = (year + 1, 1) if month == 12 else (year, month + 1)
    return {'month_title': month_title, 'year': year, 'month': month, 'weeks': weeks, 'event_lookup': event_lookup, 'prev_year': prev_year, 'prev_month': prev_month, 'next_year': next_year, 'next_month': next_month}

def build_chain_prayer_month(year: int, month: int):
    ensure_schedule_cache_loaded()
    db = get_db()
    rows = db.execute("SELECT * FROM sheet_chain_prayer_schedule_cache ORDER BY prayer_date ASC, church_name_assigned ASC").fetchall()
    cal = calendar.Calendar(firstweekday=6)
    month_days = cal.monthdatescalendar(year, month)
    day_map, event_lookup = {}, {}
    for idx, r in enumerate(rows, start=1):
        prayer_dt = parse_sheet_date(r['prayer_date'])
        if not prayer_dt:
            continue
        event_id = f"cp_{idx}"
        event_obj = {'id': event_id, 'sheet_row': r['sheet_row'], 'church_name_assigned': str(r['church_name_assigned'] or '').strip(), 'pastor_name': str(r['pastor_name'] or '').strip(), 'prayer_date': str(r['prayer_date'] or '').strip(), 'type_class': 'type-prayer'}
        event_lookup[event_id] = event_obj
        day_map.setdefault(prayer_dt.isoformat(), []).append(event_obj)
    weeks = []
    for week in month_days:
        week_cells = []
        for d in week:
            key = d.isoformat()
            items = day_map.get(key, [])
            week_cells.append({'date': d, 'iso': key, 'in_month': d.month == month, 'events': items, 'visible_events': items[:2], 'extra_count': max(0, len(items)-2), 'is_today': d == date.today()})
        weeks.append(week_cells)
    month_title = datetime(year, month, 1).strftime('%B %Y')
    prev_year, prev_month = (year - 1, 12) if month == 1 else (year, month - 1)
    next_year, next_month = (year + 1, 1) if month == 12 else (year, month + 1)
    return {'month_title': month_title, 'year': year, 'month': month, 'weeks': weeks, 'event_lookup': event_lookup, 'prev_year': prev_year, 'prev_month': prev_month, 'next_year': next_year, 'next_month': next_month}

def _join_schedule(sheet_row: int, join_name: str):
    join_name = str(join_name or '').strip()
    if not join_name:
        return False
    row = _get_schedule_row_from_cache(sheet_row)
    if not row:
        return False
    existing = _unique_joining_names(_parse_joining_names(row['joining']))
    existing_keys = {x.lower() for x in existing}
    if join_name.lower() not in existing_keys:
        existing.append(join_name)
    payload = {
        'church_name': row['church_name'] or '', 'church_address': row['church_address'] or '', 'pastor_name': row['pastor_name'] or '',
        'contact_number': row['contact_number'] or '', 'activity_date_start': row['activity_date_start'] or '', 'activity_date_end': row['activity_date_end'] or '',
        'activity_type': row['activity_type'] or '', 'note': row['note'] or '', 'joining': ', '.join(existing), 'theme': row['theme'] or '', 'text': row['text'] or ''
    }
    _update_district_schedule_row(sheet_row, payload)
    return True

def _append_note_audit(note: str, editor_name: str):
    note = str(note or '').rstrip()
    stamp = datetime.now().strftime('%B %d, %Y %I:%M %p')
    audit = f"[Edited by: {editor_name.strip()} | {stamp}]"
    if note:
        return f"{note}\n\n{audit}"
    return audit

def register_schedule_routes(app):
    @app.route('/schedules', methods=['GET', 'POST'])
    def schedules():
        if request.method == 'POST':
            action = (request.form.get('action') or '').strip()
            if action == 'district_secretary_access':
                access_code = (request.form.get('access_code') or '').strip()
                year = (request.form.get('year') or '').strip()
                month = (request.form.get('month') or '').strip()
                view = (request.form.get('view') or 'district').strip()
                if access_code == DISTRICT_SECRETARY_CODE:
                    session['district_secretary_ok'] = True
                    flash('District Secretary access granted.', 'success')
                else:
                    flash('Invalid access code.', 'error')
                return redirect(url_for('schedules', year=year, month=month, view=view))

            if action == 'create_schedule':
                if not session.get('district_secretary_ok'):
                    flash('District Secretary access is required.', 'error')
                    return redirect(url_for('schedules'))
                schedule_mode = (request.form.get('schedule_mode') or 'other').strip().lower()
                year = (request.form.get('year') or '').strip()
                month = (request.form.get('month') or '').strip()
                view = (request.form.get('view') or 'district').strip()
                note = (request.form.get('note') or '').strip()
                try:
                    if schedule_mode == 'thanksgiving':
                        area_number = (request.form.get('tg_area_number') or '').strip()
                        church_name = (request.form.get('tg_church_name') or '').strip()
                        activity_date_start = (request.form.get('tg_activity_date') or '').strip()
                        theme = (request.form.get('theme') or '').strip()
                        text = (request.form.get('text') or '').strip()
                        google_pin_location = (request.form.get('google_pin_location') or '').strip()
                        if not area_number or not church_name or not activity_date_start:
                            flash('Please complete all Thanksgiving required fields.', 'error')
                            return redirect(url_for('schedules', year=year, month=month, view=view))
                        if not _is_valid_google_maps_link(google_pin_location):
                            flash('Please enter a valid Google Maps link.', 'error')
                            return redirect(url_for('schedules', year=year, month=month, view=view))
                        account = _get_account_match(area_number, church_name)
                        if not account:
                            flash('Selected church was not found in Accounts.', 'error')
                            return redirect(url_for('schedules', year=year, month=month, view=view))
                        payload = {
                            'church_name': church_name,
                            'church_address': str(account['church_address'] or '').strip(),
                            'pastor_name': str(account['name'] or '').strip(),
                            'contact_number': str(account['contact'] or '').strip(),
                            'activity_date_start': activity_date_start,
                            'activity_date_end': '',
                            'activity_type': 'Thanksgiving',
                            'note': note,
                            'joining': '',
                            'theme': theme,
                            'text': text,
                        }
                        _append_district_schedule_row(payload)
                        _update_account_google_pin(area_number, church_name, google_pin_location)
                    else:
                        church_name = (request.form.get('church_name') or '').strip()
                        church_address = (request.form.get('church_address') or '').strip()
                        pastor_name = (request.form.get('pastor_name') or '').strip()
                        contact_number = (request.form.get('contact_number') or '').strip()
                        activity_date_start = (request.form.get('activity_date_start') or '').strip()
                        activity_date_end = (request.form.get('activity_date_end') or '').strip()
                        activity_type = (request.form.get('activity_type') or '').strip()
                        google_pin_location = (request.form.get('google_pin_location_other') or '').strip()
                        if not church_name or not activity_date_start or not activity_type:
                            flash('Please complete all required fields marked with *.', 'error')
                            return redirect(url_for('schedules', year=year, month=month, view=view))
                        if not _is_valid_google_maps_link(google_pin_location):
                            flash('Please enter a valid Google Maps link.', 'error')
                            return redirect(url_for('schedules', year=year, month=month, view=view))
                        start_dt, end_dt = _safe_schedule_end_date(activity_date_start, activity_date_end)
                        if not start_dt:
                            flash('Activity Date Start is invalid.', 'error')
                            return redirect(url_for('schedules', year=year, month=month, view=view))
                        payload = {
                            'church_name': church_name, 'church_address': church_address, 'pastor_name': pastor_name, 'contact_number': contact_number,
                            'activity_date_start': activity_date_start, 'activity_date_end': activity_date_end if end_dt and end_dt != start_dt else '',
                            'activity_type': activity_type, 'note': note, 'joining': '', 'theme': '', 'text': ''
                        }
                        _append_district_schedule_row(payload)
                        _update_account_google_pin('', church_name, google_pin_location)
                    sync_from_sheets_if_needed(force=True)
                    flash('Schedule created successfully.', 'success')
                except Exception as e:
                    print('❌ Create schedule failed:', e)
                    flash('Failed to create schedule.', 'error')
                return redirect(url_for('schedules', year=year, month=month, view=view))

            if action == 'edit_schedule':
                if not session.get('district_secretary_ok'):
                    flash('District Secretary access is required.', 'error')
                    return redirect(url_for('schedules'))
                sheet_row = int(request.form.get('sheet_row') or 0)
                year = (request.form.get('year') or '').strip()
                month = (request.form.get('month') or '').strip()
                view = (request.form.get('view') or 'district').strip()
                row = _get_schedule_row_from_cache(sheet_row)
                if not row:
                    flash('Schedule not found.', 'error')
                    return redirect(url_for('schedules', year=year, month=month, view=view))
                payload = {
                    'church_name': (request.form.get('church_name') or '').strip(),
                    'church_address': (request.form.get('church_address') or '').strip(),
                    'pastor_name': (request.form.get('pastor_name') or '').strip(),
                    'contact_number': (request.form.get('contact_number') or '').strip(),
                    'activity_date_start': (request.form.get('activity_date_start') or '').strip(),
                    'activity_date_end': (request.form.get('activity_date_end') or '').strip(),
                    'activity_type': (request.form.get('activity_type') or '').strip(),
                    'note': (request.form.get('note') or '').strip(),
                    'joining': row['joining'] or '',
                    'theme': row['theme'] or '',
                    'text': row['text'] or '',
                }
                google_pin_location = (request.form.get('google_pin_location') or '').strip()
                if not _is_valid_google_maps_link(google_pin_location):
                    flash('Please enter a valid Google Maps link.', 'error')
                    return redirect(url_for('schedules', year=year, month=month, view=view))
                try:
                    _update_district_schedule_row(sheet_row, payload)
                    _update_account_google_pin('', payload['church_name'], google_pin_location)
                    sync_from_sheets_if_needed(force=True)
                    flash('Schedule updated successfully.', 'success')
                except Exception as e:
                    print('❌ Edit schedule failed:', e)
                    flash('Failed to update schedule.', 'error')
                return redirect(url_for('schedules', year=year, month=month, view=view))

            if action == 'public_edit_schedule_details':
                sheet_row = int(request.form.get('sheet_row') or 0)
                editor_name = (request.form.get('editor_name') or '').strip()
                year = (request.form.get('year') or '').strip()
                month = (request.form.get('month') or '').strip()
                view = (request.form.get('view') or 'district').strip()
                if not sheet_row or not editor_name:
                    flash('Your name is required before saving edits.', 'error')
                    return redirect(url_for('schedules', year=year, month=month, view=view))
                row = _get_schedule_row_from_cache(sheet_row)
                if not row:
                    flash('Schedule not found.', 'error')
                    return redirect(url_for('schedules', year=year, month=month, view=view))
                note = (request.form.get('note') or '').strip()
                payload = {
                    'church_name': row['church_name'] or '', 'church_address': row['church_address'] or '', 'pastor_name': row['pastor_name'] or '',
                    'contact_number': row['contact_number'] or '', 'activity_date_start': row['activity_date_start'] or '', 'activity_date_end': row['activity_date_end'] or '',
                    'activity_type': row['activity_type'] or '', 'joining': row['joining'] or '',
                    'theme': (request.form.get('theme') or '').strip(), 'text': (request.form.get('text') or '').strip(),
                    'note': _append_note_audit(note, editor_name),
                }
                try:
                    _update_district_schedule_row(sheet_row, payload)
                    sync_from_sheets_if_needed(force=True)
                    flash('Schedule details updated successfully.', 'success')
                except Exception as e:
                    print('❌ Public schedule edit failed:', e)
                    flash('Failed to update schedule details.', 'error')
                return redirect(url_for('schedules', year=year, month=month, view=view))

            if action == 'delete_schedule':
                if not session.get('district_secretary_ok'):
                    flash('District Secretary access is required.', 'error')
                    return redirect(url_for('schedules'))
                sheet_row = int(request.form.get('sheet_row') or 0)
                year = (request.form.get('year') or '').strip()
                month = (request.form.get('month') or '').strip()
                view = (request.form.get('view') or 'district').strip()
                try:
                    _delete_district_schedule_row(sheet_row)
                    sync_from_sheets_if_needed(force=True)
                    flash('Schedule deleted successfully.', 'success')
                except Exception as e:
                    print('❌ Delete schedule failed:', e)
                    flash('Failed to delete schedule.', 'error')
                return redirect(url_for('schedules', year=year, month=month, view=view))

            if action == 'join_schedule':
                sheet_row = int(request.form.get('sheet_row') or 0)
                join_name = (request.form.get('join_name') or '').strip()
                year = (request.form.get('year') or '').strip()
                month = (request.form.get('month') or '').strip()
                view = (request.form.get('view') or 'district').strip()
                remembered_name = (session.get('schedule_join_name') or '').strip()
                effective_name = join_name or remembered_name
                if not effective_name:
                    flash('Name is required to join this activity.', 'error')
                    return redirect(url_for('schedules', year=year, month=month, view=view))
                try:
                    _join_schedule(sheet_row, effective_name)
                    session['schedule_join_name'] = effective_name
                    sync_from_sheets_if_needed(force=True)
                    flash('You have been added as joining.', 'success')
                except Exception as e:
                    print('❌ Join schedule failed:', e)
                    flash('Failed to join this activity.', 'error')
                return redirect(url_for('schedules', year=year, month=month, view=view))

        today = date.today()
        try:
            year = int(request.args.get('year', today.year))
        except Exception:
            year = today.year
        try:
            month = int(request.args.get('month', today.month))
        except Exception:
            month = today.month
        if month < 1 or month > 12:
            month = today.month
        ensure_schedule_cache_loaded()
        sync_from_sheets_if_needed()
        view = (request.args.get('view') or 'district').strip().lower()
        if view not in ('district', 'chain_prayer'):
            view = 'district'
        search_area_number = (request.args.get('search_area_number') or '').strip()
        search_church_name = (request.args.get('search_church_name') or '').strip()
        search_pastor_name = (request.args.get('search_pastor_name') or '').strip()
        account_search_rows = get_schedule_search_accounts()
        search_applied = any([search_area_number, search_church_name, search_pastor_name])
        if search_applied:
            district_search_rows, chain_search_rows = search_schedule_rows(area_number=search_area_number, church_name=search_church_name, pastor_name=search_pastor_name)
        else:
            district_search_rows, chain_search_rows = [], []
        if view == 'chain_prayer':
            calendar_data = build_chain_prayer_month(year, month)
            month_rows = []
        else:
            calendar_data = build_schedule_month(year, month)
            month_rows = _get_schedule_rows_for_month(year, month)
        return render_template('schedules.html', month_title=calendar_data['month_title'], view=view, year=calendar_data['year'], month=calendar_data['month'], weeks=calendar_data['weeks'], event_lookup=calendar_data['event_lookup'], prev_year=calendar_data['prev_year'], prev_month=calendar_data['prev_month'], next_year=calendar_data['next_year'], next_month=calendar_data['next_month'], district_secretary_ok=session.get('district_secretary_ok', False), user_logged_in=any_user_logged_in(), month_rows=month_rows, search_area_number=search_area_number, search_church_name=search_church_name, search_pastor_name=search_pastor_name, account_search_rows=account_search_rows, search_applied=search_applied, district_search_rows=district_search_rows, chain_search_rows=chain_search_rows, activity_types=ALL_ACTIVITY_TYPES, other_activity_types=OTHER_ACTIVITY_TYPES)
