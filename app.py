import os
import sqlite3
from datetime import datetime, date
import calendar
import urllib.parse

import requests
import gspread
from google.oauth2.service_account import Credentials
from flask import (
    Flask,
    g,
    render_template,
    request,
    redirect,
    url_for,
    abort,
    session,
)

DATABASE = "app_v2.db"

# ------------------------
# DB helpers
# ------------------------
def get_db():
    db = getattr(g, "_database", None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE, check_same_thread=False)
        db.row_factory = sqlite3.Row
    return db


def init_db():
    db = get_db()
    cursor = db.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS monthly_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            submitted INTEGER DEFAULT 0,
            approved INTEGER DEFAULT 0,
            submitted_at TEXT,
            approved_at TEXT,
            UNIQUE(year, month)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS sunday_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            monthly_report_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            is_complete INTEGER DEFAULT 0,
            attendance_adult REAL,
            attendance_youth REAL,
            attendance_children REAL,
            attendance_total REAL,
            tithes_church REAL,
            offering REAL,
            mission REAL,
            tithes_personal REAL,
            FOREIGN KEY (monthly_report_id) REFERENCES monthly_reports(id),
            UNIQUE(monthly_report_id, date)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS church_progress (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            monthly_report_id INTEGER NOT NULL UNIQUE,
            bible_new INTEGER,
            bible_existing INTEGER,
            received_christ INTEGER,
            baptized_water INTEGER,
            baptized_holy_spirit INTEGER,
            healed INTEGER,
            child_dedication INTEGER,
            is_complete INTEGER DEFAULT 0,
            FOREIGN KEY (monthly_report_id) REFERENCES monthly_reports(id)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS verses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT UNIQUE NOT NULL,
            reference TEXT NOT NULL,
            text TEXT NOT NULL
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS pastors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            full_name TEXT NOT NULL,
            age INTEGER,
            sex TEXT,
            church_address TEXT,
            contact_number TEXT,
            birthday TEXT,
            username TEXT UNIQUE,
            password TEXT
        )
        """
    )

    cursor.execute("PRAGMA table_info(pastors)")
    cols = [row[1] for row in cursor.fetchall()]
    if "birthday" not in cols:
        cursor.execute("ALTER TABLE pastors ADD COLUMN birthday TEXT")

    db.commit()


def get_or_create_monthly_report(year: int, month: int):
    db = get_db()
    cursor = db.cursor()

    cursor.execute(
        "SELECT * FROM monthly_reports WHERE year = ? AND month = ?",
        (year, month),
    )
    row = cursor.fetchone()
    if row:
        return row

    cursor.execute(
        """
        INSERT INTO monthly_reports (year, month, submitted, approved)
        VALUES (?, ?, 0, 0)
        """,
        (year, month),
    )
    db.commit()

    cursor.execute(
        "SELECT * FROM monthly_reports WHERE year = ? AND month = ?",
        (year, month),
    )
    return cursor.fetchone()


def generate_sundays_for_month(year: int, month: int):
    sundays = []
    cal = calendar.Calendar(firstweekday=calendar.SUNDAY)
    for week in cal.monthdatescalendar(year, month):
        for d in week:
            if d.month == month and d.weekday() == calendar.SUNDAY:
                sundays.append(d)
    return sundays


def ensure_sunday_reports(monthly_report_id: int, year: int, month: int):
    db = get_db()
    cursor = db.cursor()

    sundays = generate_sundays_for_month(year, month)
    for d in sundays:
        date_str = d.isoformat()
        cursor.execute(
            """
            INSERT OR IGNORE INTO sunday_reports (monthly_report_id, date)
            VALUES (?, ?)
            """,
            (monthly_report_id, date_str),
        )

    db.commit()


def get_sunday_reports(monthly_report_id: int):
    db = get_db()
    cursor = db.cursor()
    cursor.execute(
        """
        SELECT * FROM sunday_reports
        WHERE monthly_report_id = ?
        ORDER BY date
        """,
        (monthly_report_id,),
    )
    return cursor.fetchall()


def get_month_status(monthly_report):
    submitted = bool(monthly_report["submitted"])
    approved = bool(monthly_report["approved"])
    if not submitted:
        return "not_submitted"
    if submitted and not approved:
        return "pending"
    if submitted and approved:
        return "approved"
    return "not_submitted"


def set_month_submitted(year: int, month: int):
    db = get_db()
    cursor = db.cursor()
    now_str = datetime.utcnow().isoformat()
    cursor.execute(
        """
        UPDATE monthly_reports
        SET submitted = 1,
            submitted_at = ?,
            approved = 0,
            approved_at = NULL
        WHERE year = ? AND month = ?
        """,
        (now_str, year, month),
    )
    db.commit()


def set_month_approved(year: int, month: int):
    db = get_db()
    cursor = db.cursor()
    now_str = datetime.utcnow().isoformat()
    cursor.execute(
        """
        UPDATE monthly_reports
        SET approved = 1,
            approved_at = ?
        WHERE year = ? AND month = ?
        """,
        (now_str, year, month),
    )
    db.commit()


def set_month_pending(year: int, month: int):
    db = get_db()
    cursor = db.cursor()
    cursor.execute(
        """
        UPDATE monthly_reports
        SET approved = 0,
            approved_at = NULL
        WHERE year = ? AND month = ?
        """,
        (year, month),
    )
    db.commit()


def all_sundays_complete(monthly_report_id: int) -> bool:
    db = get_db()
    cursor = db.cursor()
    cursor.execute(
        """
        SELECT COUNT(*) as total,
               SUM(is_complete) as complete
        FROM sunday_reports
        WHERE monthly_report_id = ?
        """,
        (monthly_report_id,),
    )
    row = cursor.fetchone()
    if row["total"] == 0:
        return False
    complete = row["complete"] or 0
    return complete == row["total"]


def ensure_church_progress(monthly_report_id: int):
    db = get_db()
    cursor = db.cursor()
    cursor.execute(
        "SELECT * FROM church_progress WHERE monthly_report_id = ?",
        (monthly_report_id,),
    )
    row = cursor.fetchone()
    if row:
        return row

    cursor.execute(
        """
        INSERT INTO church_progress (monthly_report_id, is_complete)
        VALUES (?, 0)
        """,
        (monthly_report_id,),
    )
    db.commit()

    cursor.execute(
        "SELECT * FROM church_progress WHERE monthly_report_id = ?",
        (monthly_report_id,),
    )
    return cursor.fetchone()


# ------------------------
# Google Sheets integration
# ------------------------
GOOGLE_SHEETS_CREDENTIALS_FILE = "service_account.json"

GOOGLE_SHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def get_gs_client():
    creds = Credentials.from_service_account_file(
        GOOGLE_SHEETS_CREDENTIALS_FILE,
        scopes=GOOGLE_SHEETS_SCOPES,
    )
    return gspread.authorize(creds)


def _safe_get(rec: dict, *keys, default=""):
    for k in keys:
        if k in rec:
            return rec.get(k, default)
    norm = {str(kk).strip().lower(): kk for kk in rec.keys()}
    for k in keys:
        kk = str(k).strip().lower()
        if kk in norm:
            return rec.get(norm[kk], default)
    return default


def refresh_pastor_from_sheets():
    username = session.get("pastor_username")
    if not username:
        return False

    try:
        client = get_gs_client()
        sh = client.open("District4 Data")
        ws = sh.worksheet("Accounts")
        records = ws.get_all_records()
    except Exception as e:
        print("Error refreshing pastor data from Sheets:", e)
        return False

    for rec in records:
        sheet_username = str(_safe_get(rec, "UserName", "username")).strip()
        if sheet_username == username:
            session["pastor_name"] = _safe_get(rec, "Name", "full_name", default="")
            session["pastor_church_address"] = _safe_get(
                rec, "Church Address", "church_address", default=""
            )
            return True

    return False


def append_account_to_sheet(pastor_data: dict):
    client = get_gs_client()
    sh = client.open("District4 Data")

    try:
        worksheet = sh.worksheet("Accounts")
    except gspread.WorksheetNotFound:
        worksheet = sh.add_worksheet(title="Accounts", rows=100, cols=10)

    row = [
        pastor_data.get("full_name", ""),
        pastor_data.get("age", ""),
        pastor_data.get("sex", ""),
        pastor_data.get("church_address", ""),
        pastor_data.get("contact_number", ""),
        pastor_data.get("birthday", ""),
        pastor_data.get("username", ""),
        pastor_data.get("password", ""),
    ]
    worksheet.append_row(row)


def append_report_to_sheet(report_data: dict):
    client = get_gs_client()
    sh = client.open("District4 Data")

    try:
        worksheet = sh.worksheet("Report")
    except gspread.WorksheetNotFound:
        worksheet = sh.add_worksheet(title="Report", rows=1000, cols=25)

    row = [
        report_data.get("church", ""),
        report_data.get("pastor", ""),
        report_data.get("address", ""),
        report_data.get("adult", ""),
        report_data.get("youth", ""),
        report_data.get("children", ""),
        report_data.get("tithes", ""),
        report_data.get("offering", ""),
        report_data.get("personal_tithes", ""),
        report_data.get("mission_offering", ""),
        report_data.get("received_jesus", ""),
        report_data.get("existing_bible_study", ""),
        report_data.get("new_bible_study", ""),
        report_data.get("water_baptized", ""),
        report_data.get("holy_spirit_baptized", ""),
        report_data.get("childrens_dedication", ""),
        report_data.get("healed", ""),
        report_data.get("activity_date", ""),
        report_data.get("amount_to_send", ""),
        report_data.get("status", ""),
    ]
    worksheet.append_row(row)


def update_sheet_status_for_month(year: int, month: int, status_label: str):
    try:
        client = get_gs_client()
        sh = client.open("District4 Data")
        ws = sh.worksheet("Report")
        values = ws.get_all_values()
    except Exception as e:
        print("Error accessing Report sheet for status update:", e)
        return

    if not values:
        return

    target_prefix = f"{year:04d}-{month:02d}-"
    for i in range(1, len(values)):
        row = values[i]
        if len(row) < 18:
            continue
        activity_date = row[17]
        if str(activity_date).startswith(target_prefix):
            sheet_row = i + 1
            try:
                ws.update_cell(sheet_row, 20, status_label)
            except Exception as e:
                print(f"Error updating status for row {sheet_row}:", e)
# ------------------------
# Sheets → DB Sync (CACHE)
# ------------------------

SYNC_INTERVAL_SECONDS = 120  # 2 minutes


def ensure_sync_table():
    db = get_db()
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS sync_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            last_sync TEXT
        )
        """
    )
    db.execute("INSERT OR IGNORE INTO sync_state (id, last_sync) VALUES (1, NULL)")
    db.commit()


def last_sync_time():
    row = get_db().execute("SELECT last_sync FROM sync_state WHERE id = 1").fetchone()
    if row and row["last_sync"]:
        return datetime.fromisoformat(row["last_sync"])
    return None


def update_sync_time():
    get_db().execute(
        "UPDATE sync_state SET last_sync = ? WHERE id = 1",
        (datetime.utcnow().isoformat(),),
    )
    get_db().commit()


def sync_from_sheets_if_needed(force=False):
    ensure_sync_table()

    last = last_sync_time()
    if not force and last and (datetime.utcnow() - last).total_seconds() < SYNC_INTERVAL_SECONDS:
        return

    print("🔄 Syncing from Google Sheets…")

    client = get_gs_client()
    sh = client.open("District4 Data")

    db = get_db()
    cur = db.cursor()

    # ---------- ACCOUNTS ----------
    try:
        ws_accounts = sh.worksheet("Accounts")
        accounts = ws_accounts.get_all_records()
    except Exception as e:
        print("Accounts sync failed:", e)
        accounts = []

    cur.execute("DELETE FROM pastors")

    for r in accounts:
        cur.execute(
            """
            INSERT OR REPLACE INTO pastors
            (full_name, age, sex, church_address, contact_number, birthday, username, password)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                r.get("Name"),
                r.get("Age"),
                r.get("Sex"),
                r.get("Church Address"),
                r.get("Contact #"),
                r.get("Birth Day"),
                r.get("UserName"),
                r.get("Password"),
            ),
        )

    # ---------- REPORT ----------
    try:
        ws_report = sh.worksheet("Report")
        reports = ws_report.get_all_records()
    except Exception as e:
        print("Report sync failed:", e)
        reports = []

    cur.execute("DELETE FROM monthly_reports")
    cur.execute("DELETE FROM sunday_reports")
    cur.execute("DELETE FROM church_progress")

    for r in reports:
        try:
            d = date.fromisoformat(str(r.get("activity_date")))
        except Exception:
            continue

        mr = get_or_create_monthly_report(d.year, d.month)
        mrid = mr["id"]

        cur.execute(
            """
            INSERT OR IGNORE INTO sunday_reports
            (monthly_report_id, date, is_complete,
             attendance_adult, attendance_youth, attendance_children,
             tithes_church, offering, mission, tithes_personal)
            VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                mrid,
                d.isoformat(),
                r.get("adult", 0),
                r.get("youth", 0),
                r.get("children", 0),
                r.get("tithes", 0),
                r.get("offering", 0),
                r.get("mission offering", 0),
                r.get("personal tithes", 0),
            ),
        )

        cur.execute(
            """
            INSERT OR REPLACE INTO church_progress
            (monthly_report_id, bible_existing, bible_new, received_christ,
             baptized_water, baptized_holy_spirit, healed, child_dedication, is_complete)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
            """,
            (
                mrid,
                r.get("existing bible study", 0),
                r.get("new bible study", 0),
                r.get("received jesus", 0),
                r.get("water baptized", 0),
                r.get("holy spirit baptized", 0),
                r.get("healed", 0),
                r.get("childrens dedication", 0),
            ),
        )

        status = str(r.get("status", "")).lower()
        cur.execute(
            """
            UPDATE monthly_reports
            SET submitted = 1,
                approved = ?
            WHERE id = ?
            """,
            (1 if "approved" in status else 0, mrid),
        )

    db.commit()
    update_sync_time()
    print("✅ Sync complete")


def parse_float(value):
    try:
        s = str(value).strip()
        if s == "":
            return 0.0
        s = s.replace(",", "")
        return float(s)
    except Exception:
        return 0.0


def get_report_stats_for_month(year: int, month: int):
    stats = {
        "church": "",
        "pastor": "",
        "address": "",
        "rows": 0,
        "avg": {
            "adult": 0.0,
            "youth": 0.0,
            "children": 0.0,
            "received_jesus": 0.0,
            "existing_bible_study": 0.0,
            "new_bible_study": 0.0,
            "water_baptized": 0.0,
            "holy_spirit_baptized": 0.0,
            "childrens_dedication": 0.0,
            "healed": 0.0,
        },
        "totals": {
            "tithes": 0.0,
            "offering": 0.0,
            "personal_tithes": 0.0,
            "mission_offering": 0.0,
            "amount_to_send": 0.0,
        },
        "sheet_status": "",
    }

    try:
        client = get_gs_client()
        sh = client.open("District4 Data")
        ws = sh.worksheet("Report")
        records = ws.get_all_records()
    except Exception as e:
        print("Error reading Report sheet for stats:", e)
        return stats

    prefix = f"{year:04d}-{month:02d}-"

    sum_fields = {k: 0.0 for k in stats["avg"].keys()}
    totals = {k: 0.0 for k in stats["totals"].keys()}
    statuses = set()
    count = 0

    for rec in records:
        activity_date = str(_safe_get(rec, "activity_date", "Activity Date", "activity date")).strip()
        if not activity_date.startswith(prefix):
            continue

        if count == 0:
            stats["church"] = _safe_get(rec, "church", "Church", default="")
            stats["pastor"] = _safe_get(rec, "pastor", "Pastor", default="")
            stats["address"] = _safe_get(rec, "address", "Address", default="")

        sum_fields["adult"] += parse_float(_safe_get(rec, "adult", "Adult"))
        sum_fields["youth"] += parse_float(_safe_get(rec, "youth", "Youth"))
        sum_fields["children"] += parse_float(_safe_get(rec, "children", "Children"))

        sum_fields["received_jesus"] += parse_float(_safe_get(rec, "received jesus", "received_jesus"))
        sum_fields["existing_bible_study"] += parse_float(_safe_get(rec, "existing bible study", "existing_bible_study"))
        sum_fields["new_bible_study"] += parse_float(_safe_get(rec, "new bible study", "new_bible_study"))
        sum_fields["water_baptized"] += parse_float(_safe_get(rec, "water baptized", "water_baptized"))
        sum_fields["holy_spirit_baptized"] += parse_float(_safe_get(rec, "holy spirit baptized", "holy_spirit_baptized"))
        sum_fields["childrens_dedication"] += parse_float(_safe_get(rec, "childrens dedication", "childrens_dedication"))
        sum_fields["healed"] += parse_float(_safe_get(rec, "healed", "Healed"))

        totals["tithes"] += parse_float(_safe_get(rec, "tithes", "Tithes"))
        totals["offering"] += parse_float(_safe_get(rec, "offering", "Offering"))
        totals["personal_tithes"] += parse_float(_safe_get(rec, "personal tithes", "personal_tithes"))
        totals["mission_offering"] += parse_float(_safe_get(rec, "mission offering", "mission_offering"))
        totals["amount_to_send"] += parse_float(_safe_get(rec, "amount to send", "amount_to_send"))

        status_val = str(_safe_get(rec, "status", "Status")).strip()
        if status_val:
            statuses.add(status_val)

        count += 1

    stats["rows"] = count
    if count > 0:
        for k in stats["avg"].keys():
            stats["avg"][k] = sum_fields[k] / count

    stats["totals"] = totals

    if len(statuses) == 1:
        stats["sheet_status"] = list(statuses)[0]
    elif len(statuses) > 1:
        stats["sheet_status"] = "Mixed"
    else:
        stats["sheet_status"] = ""

    return stats


def export_month_to_sheet(year: int, month: int, status_label: str):
    db = get_db()
    cursor = db.cursor()

    cursor.execute(
        "SELECT * FROM monthly_reports WHERE year = ? AND month = ?",
        (year, month),
    )
    monthly_report = cursor.fetchone()
    if not monthly_report:
        return

    monthly_report_id = monthly_report["id"]
    cp_row = ensure_church_progress(monthly_report_id)

    cursor.execute(
        """
        SELECT * FROM sunday_reports
        WHERE monthly_report_id = ?
        ORDER BY date
        """,
        (monthly_report_id,),
    )
    sunday_rows = cursor.fetchall()
    if not sunday_rows:
        return

    refresh_pastor_from_sheets()
    pastor_name = session.get("pastor_name", "")
    church_address = session.get("pastor_church_address", "")

    bible_new = cp_row["bible_new"] or 0
    bible_existing = cp_row["bible_existing"] or 0
    received_christ = cp_row["received_christ"] or 0
    baptized_water = cp_row["baptized_water"] or 0
    baptized_holy_spirit = cp_row["baptized_holy_spirit"] or 0
    healed = cp_row["healed"] or 0
    child_dedication = cp_row["child_dedication"] or 0

    for row in sunday_rows:
        d = datetime.fromisoformat(row["date"]).date()
        activity_date = d.isoformat()

        tithes_church = row["tithes_church"] or 0
        offering = row["offering"] or 0
        mission = row["mission"] or 0
        tithes_personal = row["tithes_personal"] or 0

        amount_to_send = tithes_church + offering + mission + tithes_personal

        report_data = {
            "church": church_address,
            "pastor": pastor_name,
            "address": church_address,
            "adult": row["attendance_adult"] or 0,
            "youth": row["attendance_youth"] or 0,
            "children": row["attendance_children"] or 0,
            "tithes": tithes_church,
            "offering": offering,
            "personal_tithes": tithes_personal,
            "mission_offering": mission,
            "received_jesus": received_christ,
            "existing_bible_study": bible_existing,
            "new_bible_study": bible_new,
            "water_baptized": baptized_water,
            "holy_spirit_baptized": baptized_holy_spirit,
            "childrens_dedication": child_dedication,
            "healed": healed,
            "activity_date": activity_date,
            "amount_to_send": amount_to_send,
            "status": status_label,
        }

        try:
            append_report_to_sheet(report_data)
        except Exception as e:
            print("Error sending Sunday row to Google Sheets:", e)


def sync_from_sheets_for_pastor():
    username = session.get("pastor_username")
    if not username:
        return

    refresh_pastor_from_sheets()
    pastor_name = (session.get("pastor_name") or "").strip()
    church_address = (session.get("pastor_church_address") or "").strip()

    if not pastor_name and not church_address:
        return

    try:
        client = get_gs_client()
        sh = client.open("District4 Data")
        ws = sh.worksheet("Report")
        records = ws.get_all_records()
    except Exception as e:
        print("Error syncing from Sheets for pastor:", e)
        return

    db = get_db()
    cursor = db.cursor()

    cp_cache = {}
    statuses_by_month = {}

    for rec in records:
        rec_pastor = str(_safe_get(rec, "pastor", "Pastor")).strip()
        rec_address = str(_safe_get(rec, "address", "Address")).strip()

        if church_address and rec_address != church_address:
            continue
        if pastor_name and rec_pastor and rec_pastor != pastor_name:
            continue

        activity_date_str = str(_safe_get(rec, "activity_date", "Activity Date", "activity date")).strip()
        if not activity_date_str:
            continue

        try:
            d = date.fromisoformat(activity_date_str)
        except ValueError:
            continue

        y = d.year
        m = d.month
        day_iso = d.isoformat()

        monthly_report = get_or_create_monthly_report(y, m)
        mrid = monthly_report["id"]

        cursor.execute(
            """
            SELECT * FROM sunday_reports
            WHERE monthly_report_id = ? AND date = ?
            """,
            (mrid, day_iso),
        )
        srow = cursor.fetchone()

        adult = parse_float(_safe_get(rec, "adult", "Adult"))
        youth = parse_float(_safe_get(rec, "youth", "Youth"))
        children = parse_float(_safe_get(rec, "children", "Children"))

        tithes_church = parse_float(_safe_get(rec, "tithes", "Tithes"))
        offering = parse_float(_safe_get(rec, "offering", "Offering"))
        mission = parse_float(_safe_get(rec, "mission offering", "mission_offering"))
        tithes_personal = parse_float(_safe_get(rec, "personal tithes", "personal_tithes"))

        if not srow:
            cursor.execute(
                """
                INSERT INTO sunday_reports
                (monthly_report_id, date, is_complete,
                 attendance_adult, attendance_youth, attendance_children,
                 tithes_church, offering, mission, tithes_personal)
                VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    mrid,
                    day_iso,
                    adult,
                    youth,
                    children,
                    tithes_church,
                    offering,
                    mission,
                    tithes_personal,
                ),
            )
        else:
            cursor.execute(
                """
                UPDATE sunday_reports
                SET is_complete = 1,
                    attendance_adult = ?,
                    attendance_youth = ?,
                    attendance_children = ?,
                    tithes_church = ?,
                    offering = ?,
                    mission = ?,
                    tithes_personal = ?
                WHERE id = ?
                """,
                (
                    adult,
                    youth,
                    children,
                    tithes_church,
                    offering,
                    mission,
                    tithes_personal,
                    srow["id"],
                ),
            )

        key = (y, m)
        if key not in cp_cache:
            cp_cache[key] = {
                "bible_new": int(parse_float(_safe_get(rec, "new bible study", "new_bible_study"))),
                "bible_existing": int(parse_float(_safe_get(rec, "existing bible study", "existing_bible_study"))),
                "received_christ": int(parse_float(_safe_get(rec, "received jesus", "received_jesus"))),
                "baptized_water": int(parse_float(_safe_get(rec, "water baptized", "water_baptized"))),
                "baptized_holy_spirit": int(parse_float(_safe_get(rec, "holy spirit baptized", "holy_spirit_baptized"))),
                "healed": int(parse_float(_safe_get(rec, "healed", "Healed"))),
                "child_dedication": int(parse_float(_safe_get(rec, "childrens dedication", "childrens_dedication"))),
            }

        status_val = str(_safe_get(rec, "status", "Status")).strip()
        if status_val:
            statuses_by_month.setdefault(key, set()).add(status_val)

    for (y, m), cpvals in cp_cache.items():
        monthly_report = get_or_create_monthly_report(y, m)
        mrid = monthly_report["id"]

        cursor.execute(
            "SELECT * FROM church_progress WHERE monthly_report_id = ?",
            (mrid,),
        )
        cprow = cursor.fetchone()

        if not cprow:
            cursor.execute(
                """
                INSERT INTO church_progress
                (monthly_report_id, bible_new, bible_existing, received_christ,
                 baptized_water, baptized_holy_spirit, healed, child_dedication, is_complete)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
                """,
                (
                    mrid,
                    cpvals["bible_new"],
                    cpvals["bible_existing"],
                    cpvals["received_christ"],
                    cpvals["baptized_water"],
                    cpvals["baptized_holy_spirit"],
                    cpvals["healed"],
                    cpvals["child_dedication"],
                ),
            )
        else:
            cursor.execute(
                """
                UPDATE church_progress
                SET bible_new = ?,
                    bible_existing = ?,
                    received_christ = ?,
                    baptized_water = ?,
                    baptized_holy_spirit = ?,
                    healed = ?,
                    child_dedication = ?,
                    is_complete = 1
                WHERE id = ?
                """,
                (
                    cpvals["bible_new"],
                    cpvals["bible_existing"],
                    cpvals["received_christ"],
                    cpvals["baptized_water"],
                    cpvals["baptized_holy_spirit"],
                    cpvals["healed"],
                    cpvals["child_dedication"],
                    cprow["id"],
                ),
            )

        statuses = statuses_by_month.get((y, m), set())
        submitted = 1 if statuses else 0

        approved = 0
        for s in statuses:
            if "approved" in s.lower():  # works with "Approved"
                approved = 1
                break

        cursor.execute(
            """
            UPDATE monthly_reports
            SET submitted = ?, approved = ?
            WHERE id = ?
            """,
            (submitted, approved, mrid),
        )

    db.commit()


# ------------------------
# AO Church Status (old dropdown design)
# ------------------------
def get_all_churches_from_accounts():
    churches = []
    try:
        client = get_gs_client()
        sh = client.open("District4 Data")
        ws = sh.worksheet("Accounts")
        records = ws.get_all_records()
    except Exception as e:
        print("Error reading Accounts for churches:", e)
        return churches

    seen = set()
    for rec in records:
        church = str(_safe_get(rec, "Church Address", "church_address")).strip()
        if church and church not in seen:
            seen.add(church)
            churches.append(church)

    churches.sort()
    return churches


def get_report_stats_for_month_and_church(year: int, month: int, church_key: str):
    stats = {
        "church": church_key,
        "rows": 0,
        "avg": {
            "adult": 0.0,
            "youth": 0.0,
            "children": 0.0,
            "received_jesus": 0.0,
            "existing_bible_study": 0.0,
            "new_bible_study": 0.0,
            "water_baptized": 0.0,
            "holy_spirit_baptized": 0.0,
            "childrens_dedication": 0.0,
            "healed": 0.0,
        },
        "totals": {
            "tithes": 0.0,
            "offering": 0.0,
            "personal_tithes": 0.0,
            "mission_offering": 0.0,
            "amount_to_send": 0.0,
        },
        "sheet_status": "",
    }

    prefix = f"{year:04d}-{month:02d}-"

    try:
        client = get_gs_client()
        sh = client.open("District4 Data")
        ws = sh.worksheet("Report")
        records = ws.get_all_records()
    except Exception as e:
        print("Error reading Report for church stats:", e)
        return stats

    sum_fields = {k: 0.0 for k in stats["avg"].keys()}
    totals = {k: 0.0 for k in stats["totals"].keys()}
    statuses = set()
    count = 0

    for rec in records:
        activity_date = str(_safe_get(rec, "activity_date", "Activity Date", "activity date")).strip()
        if not activity_date.startswith(prefix):
            continue

        address_val = str(_safe_get(rec, "address", "Address")).strip()
        church_val = str(_safe_get(rec, "church", "Church")).strip()

        if church_key and not (address_val == church_key or church_val == church_key):
            continue

        sum_fields["adult"] += parse_float(_safe_get(rec, "adult", "Adult"))
        sum_fields["youth"] += parse_float(_safe_get(rec, "youth", "Youth"))
        sum_fields["children"] += parse_float(_safe_get(rec, "children", "Children"))

        sum_fields["received_jesus"] += parse_float(_safe_get(rec, "received jesus", "received_jesus"))
        sum_fields["existing_bible_study"] += parse_float(_safe_get(rec, "existing bible study", "existing_bible_study"))
        sum_fields["new_bible_study"] += parse_float(_safe_get(rec, "new bible study", "new_bible_study"))
        sum_fields["water_baptized"] += parse_float(_safe_get(rec, "water baptized", "water_baptized"))
        sum_fields["holy_spirit_baptized"] += parse_float(_safe_get(rec, "holy spirit baptized", "holy_spirit_baptized"))
        sum_fields["childrens_dedication"] += parse_float(_safe_get(rec, "childrens dedication", "childrens_dedication"))
        sum_fields["healed"] += parse_float(_safe_get(rec, "healed", "Healed"))

        totals["tithes"] += parse_float(_safe_get(rec, "tithes", "Tithes"))
        totals["offering"] += parse_float(_safe_get(rec, "offering", "Offering"))
        totals["personal_tithes"] += parse_float(_safe_get(rec, "personal tithes", "personal_tithes"))
        totals["mission_offering"] += parse_float(_safe_get(rec, "mission offering", "mission_offering"))
        totals["amount_to_send"] += parse_float(_safe_get(rec, "amount to send", "amount_to_send"))

        status_val = str(_safe_get(rec, "status", "Status")).strip()
        if status_val:
            statuses.add(status_val)

        count += 1

    stats["rows"] = count
    if count > 0:
        for k in stats["avg"].keys():
            stats["avg"][k] = sum_fields[k] / count

    stats["totals"] = totals

    if len(statuses) == 1:
        stats["sheet_status"] = list(statuses)[0]
    elif len(statuses) > 1:
        stats["sheet_status"] = "Mixed"
    else:
        stats["sheet_status"] = ""

    return stats


def update_sheet_status_for_church_month(year: int, month: int, church_key: str, status_label: str):
    try:
        client = get_gs_client()
        sh = client.open("District4 Data")
        ws = sh.worksheet("Report")
        values = ws.get_all_values()
    except Exception as e:
        print("Error accessing Report for church status update:", e)
        return

    if not values or len(values) < 2:
        return

    headers = values[0]

    def find_col(name):
        for i, h in enumerate(headers):
            if str(h).strip().lower() == str(name).strip().lower():
                return i
        return None

    idx_activity = find_col("activity_date")
    idx_status = find_col("status")
    idx_address = find_col("address")
    idx_church = find_col("church")

    if idx_activity is None or idx_status is None:
        print("Report sheet missing activity_date/status column headers.")
        return

    target_prefix = f"{year:04d}-{month:02d}-"

    for r in range(1, len(values)):
        row = values[r]
        if idx_activity >= len(row):
            continue
        activity_date = str(row[idx_activity]).strip()
        if not activity_date.startswith(target_prefix):
            continue

        address_val = str(row[idx_address]).strip() if (idx_address is not None and idx_address < len(row)) else ""
        church_val = str(row[idx_church]).strip() if (idx_church is not None and idx_church < len(row)) else ""

        if church_key and (address_val == church_key or church_val == church_key):
            sheet_row = r + 1
            try:
                ws.update_cell(sheet_row, idx_status + 1, status_label)
            except Exception as e:
                print(f"Error updating status row {sheet_row}:", e)


# ------------------------
# Bible verse of the day
# ------------------------
VERSE_REFERENCES = [
    "John 3:16",
    "Psalm 23:1",
    "Romans 8:28",
    "Proverbs 3:5-6",
    "Isaiah 40:31",
    "Philippians 4:6-7",
    "Jeremiah 29:11",
    "Matthew 5:16",
    "Ephesians 2:8-9",
    "Psalm 46:1",
]


def get_verse_of_the_day():
    today_str = date.today().isoformat()
    db = get_db()
    cursor = db.cursor()

    cursor.execute("SELECT * FROM verses WHERE date = ?", (today_str,))
    row = cursor.fetchone()
    if row:
        return row["reference"], row["text"]

    idx = date.today().toordinal() % len(VERSE_REFERENCES)
    reference = VERSE_REFERENCES[idx]

    verse_text = reference
    try:
        encoded_ref = urllib.parse.quote(reference)
        resp = requests.get(f"https://bible-api.com/{encoded_ref}", timeout=5)
        resp.raise_for_status()
        data = resp.json()
        if "text" in data and data["text"].strip():
            verse_text = data["text"].strip()
    except Exception:
        verse_text = reference

    cursor.execute(
        """
        INSERT OR REPLACE INTO verses (date, reference, text)
        VALUES (?, ?, ?)
        """,
        (today_str, reference, verse_text),
    )
    db.commit()

    return reference, verse_text


# ------------------------
# Auth helpers
# ------------------------
def ao_logged_in():
    return session.get("ao_logged_in") is True


def pastor_logged_in():
    return session.get("pastor_logged_in") is True


def generate_pastor_credentials(full_name: str, age: int):
    db = get_db()
    cursor = db.cursor()

    name = (full_name or "").strip()
    parts = name.split()
    if not parts:
        base = "pastor"
        first_name_clean = "Pastor"
    else:
        first = parts[0]
        base = "".join(ch for ch in first if ch.isalpha()).lower() or "pastor"
        first_name_clean = first.title()

    username = base
    suffix = 1
    while True:
        cursor.execute("SELECT 1 FROM pastors WHERE username = ?", (username,))
        if cursor.fetchone() is None:
            break
        suffix += 1
        username = f"{base}{suffix}"

    try:
        age_int = int(age)
    except (TypeError, ValueError):
        age_int = 0

    if age_int > 0:
        password = f"{first_name_clean}{age_int}"
    else:
        password = f"{first_name_clean}123"

    return username, password


# ------------------------
# Flask app
# ------------------------
app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "change-this-secret-key-123")


@app.before_request
def before_request():
    init_db()


@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, "_database", None)
    if db is not None:
        db.close()


@app.route("/")
def splash():
    return render_template("splash.html")


@app.route("/bulletin")
def bulletin():
    reference, text = get_verse_of_the_day()
    today_str = date.today().strftime("%B %d, %Y")
    return render_template(
        "bulletin.html",
        verse_reference=reference,
        verse_text=text,
        today_str=today_str,
    )


# ------------------------
# Pastor login
# ------------------------
@app.route("/pastor-login", methods=["GET", "POST"])
def pastor_login():
    error = None
    next_url = request.args.get("next") or url_for("pastor_tool")

    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = (request.form.get("password") or "").strip()

        if not username or not password:
            error = "Username and password are required."
        else:
            try:
                client = get_gs_client()
                sh = client.open("District4 Data")
                ws = sh.worksheet("Accounts")
                records = ws.get_all_records()

                matched = None
                for rec in records:
                    sheet_username = str(_safe_get(rec, "UserName", "username")).strip()
                    sheet_password = str(_safe_get(rec, "Password", "password")).strip()
                    if username == sheet_username and password == sheet_password:
                        matched = rec
                        break

                if matched:
                    session["pastor_logged_in"] = True
                    session["pastor_username"] = username
                    session["pastor_name"] = _safe_get(matched, "Name", "full_name", default="")
                    session["pastor_church_address"] = _safe_get(
                        matched, "Church Address", "church_address", default=""
                    )
                    session.permanent = True

                    try:
                        sync_from_sheets_for_pastor()
                    except Exception as e:
                        print("Error syncing from Sheets after login:", e)

                    form_next = request.form.get("next")
                    return redirect(form_next or next_url)
                else:
                    error = "Invalid username or password."
            except Exception as e:
                error = f"Error accessing Google Sheets: {e}"

    return render_template("pastor_login.html", error=error, next_url=next_url)


# ------------------------
# Pastor Tool
# ------------------------
@app.route("/pastor-tool", methods=["GET", "POST"])
def pastor_tool():
    sync_from_sheets_if_needed()
    if not pastor_logged_in():
        return redirect(url_for("pastor_login", next=request.path))

    refresh_pastor_from_sheets()
    try:
        sync_from_sheets_for_pastor()
    except Exception as e:
        print("Error syncing from Sheets on pastor_tool:", e)

    today = date.today()
    year = request.args.get("year", type=int) or today.year
    month = request.args.get("month", type=int) or today.month

    monthly_report = get_or_create_monthly_report(year, month)
    ensure_sunday_reports(monthly_report["id"], year, month)
    sunday_rows = get_sunday_reports(monthly_report["id"])

    cp_row = ensure_church_progress(monthly_report["id"])
    cp_complete = bool(cp_row["is_complete"])

    sunday_list = []
    for row in sunday_rows:
        d = datetime.fromisoformat(row["date"]).date()
        sunday_list.append(
            {
                "id": row["id"],
                "date": row["date"],
                "display": d.strftime("%B %d"),
                "year": d.year,
                "month": d.month,
                "day": d.day,
                "is_complete": bool(row["is_complete"]),
            }
        )

    db = get_db()
    cursor = db.cursor()

    cursor.execute(
        """
        SELECT
            SUM(
                COALESCE(tithes_church, 0) +
                COALESCE(offering, 0) +
                COALESCE(mission, 0) +
                COALESCE(tithes_personal, 0)
            ) AS total_amount
        FROM sunday_reports
        WHERE monthly_report_id = ?
          AND is_complete = 1
        """,
        (monthly_report["id"],),
    )
    row = cursor.fetchone()
    monthly_total = row["total_amount"] or 0.0

    year_options = list(range(today.year - 1, today.year + 4))

    month_names = [
        ("January", 1), ("February", 2), ("March", 3), ("April", 4),
        ("May", 5), ("June", 6), ("July", 7), ("August", 8),
        ("September", 9), ("October", 10), ("November", 11), ("December", 12),
    ]

    sundays_ok = all_sundays_complete(monthly_report["id"])
    can_submit = sundays_ok and cp_complete
    status_key = get_month_status(monthly_report)

    cursor.execute(
        "SELECT year, month, submitted FROM monthly_reports WHERE year = ?",
        (year,),
    )
    submitted_map = {
        (r["year"], r["month"]): bool(r["submitted"])
        for r in cursor.fetchall()
    }

    if request.method == "POST":
        if can_submit:
            set_month_submitted(year, month)
            try:
                export_month_to_sheet(year, month, "Pending AO approval")
            except Exception as e:
                print("Error exporting month to sheet on submit:", e)
        return redirect(url_for("pastor_tool", year=year, month=month))

    return render_template(
        "pastor_tool.html",
        year=year,
        month=month,
        year_options=year_options,
        month_names=month_names,
        monthly_report=monthly_report,
        sunday_list=sunday_list,
        can_submit=can_submit,
        status_key=status_key,
        submitted_map=submitted_map,
        cp_complete=cp_complete,
        sundays_ok=sundays_ok,
        monthly_total=monthly_total,
        pastor_name=session.get("pastor_name", ""),
    )


@app.route("/pastor-tool/<int:year>/<int:month>/<int:day>", methods=["GET", "POST"])
def sunday_detail(year, month, day):
    if not pastor_logged_in():
        return redirect(url_for("pastor_login", next=request.path))

    refresh_pastor_from_sheets()

    try:
        d = date(year, month, day)
    except ValueError:
        abort(404)

    monthly_report = get_or_create_monthly_report(year, month)
    ensure_sunday_reports(monthly_report["id"], year, month)

    db = get_db()
    cursor = db.cursor()
    cursor.execute(
        """
        SELECT * FROM sunday_reports
        WHERE monthly_report_id = ? AND date = ?
        """,
        (monthly_report["id"], d.isoformat()),
    )
    sunday = cursor.fetchone()
    if not sunday:
        abort(404)

    error = None
    values = {}

    if request.method == "POST":
        fields = [
            "attendance_adult",
            "attendance_youth",
            "attendance_children",
            "tithes_church",
            "offering",
            "mission",
            "tithes_personal",
        ]

        numeric_values = {}
        for field in fields:
            raw = (request.form.get(field) or "").strip()
            values[field] = raw
            if raw == "":
                error = "All fields are required."
                break
            try:
                numeric_values[field] = float(raw)
            except ValueError:
                error = "Please enter numbers only in all fields."
                break

        if not error:
            cursor.execute(
                """
                UPDATE sunday_reports
                SET attendance_adult = ?,
                    attendance_youth = ?,
                    attendance_children = ?,
                    attendance_total = NULL,
                    tithes_church = ?,
                    offering = ?,
                    mission = ?,
                    tithes_personal = ?,
                    is_complete = 1
                WHERE id = ?
                """,
                (
                    numeric_values["attendance_adult"],
                    numeric_values["attendance_youth"],
                    numeric_values["attendance_children"],
                    numeric_values["tithes_church"],
                    numeric_values["offering"],
                    numeric_values["mission"],
                    numeric_values["tithes_personal"],
                    sunday["id"],
                ),
            )
            db.commit()

            return redirect(url_for("pastor_tool", year=year, month=month))

    if not values:
        values = {
            "attendance_adult": sunday["attendance_adult"] or "",
            "attendance_youth": sunday["attendance_youth"] or "",
            "attendance_children": sunday["attendance_children"] or "",
            "tithes_church": sunday["tithes_church"] or "",
            "offering": sunday["offering"] or "",
            "mission": sunday["mission"] or "",
            "tithes_personal": sunday["tithes_personal"] or "",
        }

    date_str = d.strftime("%B %d, %Y")
    return render_template(
        "sunday_detail.html",
        year=year,
        month=month,
        day=day,
        date_str=date_str,
        values=values,
        error=error,
    )


@app.route("/pastor-tool/<int:year>/<int:month>/progress", methods=["GET", "POST"])
def church_progress_view(year, month):
    if not pastor_logged_in():
        return redirect(url_for("pastor_login", next=request.path))

    refresh_pastor_from_sheets()

    monthly_report = get_or_create_monthly_report(year, month)
    cp_row = ensure_church_progress(monthly_report["id"])

    db = get_db()
    cursor = db.cursor()

    error = None
    values = {}

    if request.method == "POST":
        fields = [
            "bible_new",
            "bible_existing",
            "received_christ",
            "baptized_water",
            "baptized_holy_spirit",
            "healed",
            "child_dedication",
        ]
        numeric_values = {}
        for field in fields:
            raw = (request.form.get(field) or "").strip()
            values[field] = raw
            if raw == "":
                error = "All fields are required for Church Progress."
                break
            try:
                numeric_values[field] = int(raw)
            except ValueError:
                error = "Please enter whole numbers only in all Church Progress fields."
                break

        if not error:
            cursor.execute(
                """
                UPDATE church_progress
                SET bible_new = ?,
                    bible_existing = ?,
                    received_christ = ?,
                    baptized_water = ?,
                    baptized_holy_spirit = ?,
                    healed = ?,
                    child_dedication = ?,
                    is_complete = 1
                WHERE id = ?
                """,
                (
                    numeric_values["bible_new"],
                    numeric_values["bible_existing"],
                    numeric_values["received_christ"],
                    numeric_values["baptized_water"],
                    numeric_values["baptized_holy_spirit"],
                    numeric_values["healed"],
                    numeric_values["child_dedication"],
                    cp_row["id"],
                ),
            )
            db.commit()

            return redirect(url_for("pastor_tool", year=year, month=month))

    if not values:
        values = {
            "bible_new": cp_row["bible_new"] or "",
            "bible_existing": cp_row["bible_existing"] or "",
            "received_christ": cp_row["received_christ"] or "",
            "baptized_water": cp_row["baptized_water"] or "",
            "baptized_holy_spirit": cp_row["baptized_holy_spirit"] or "",
            "healed": cp_row["healed"] or "",
            "child_dedication": cp_row["child_dedication"] or "",
        }

    date_label = date(year, month, 1).strftime("%B %Y")
    return render_template(
        "church_progress.html",
        year=year,
        month=month,
        date_label=date_label,
        values=values,
        error=error,
    )


# ------------------------
# AO login & tools
# ------------------------
@app.route("/ao-login", methods=["GET", "POST"])
def ao_login():
    error = None
    next_url = request.args.get("next") or url_for("ao_tool")

    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = (request.form.get("password") or "").strip()

        if username == "Pijeme" and password == "Area 7":
            session["ao_logged_in"] = True
            session["ao_username"] = username
            session.permanent = True
            form_next = request.form.get("next")
            return redirect(form_next or next_url)
        else:
            error = "Invalid username or password."

    return render_template("ao_login.html", error=error, next_url=next_url)


@app.route("/ao-tool")
def ao_tool():
    if not ao_logged_in():
        return redirect(url_for("ao_login", next=request.path))

    return render_template("ao_tool.html")


@app.route("/ao-tool/create-account", methods=["GET", "POST"])
def ao_create_account():
    if not ao_logged_in():
        return redirect(url_for("ao_login", next=request.path))

    error = None
    success = None
    generated_username = None
    generated_password = None

    values = {
        "full_name": "",
        "age": "",
        "sex": "",
        "church_address": "",
        "contact_number": "",
        "birthday": "",
    }

    if request.method == "POST":
        full_name = (request.form.get("full_name") or "").strip()
        age_raw = (request.form.get("age") or "").strip()
        sex = (request.form.get("sex") or "").strip()
        church_address = (request.form.get("church_address") or "").strip()
        contact_number = (request.form.get("contact_number") or "").strip()
        birthday_raw = (request.form.get("birthday") or "").strip()

        values.update(
            {
                "full_name": full_name,
                "age": age_raw,
                "sex": sex,
                "church_address": church_address,
                "contact_number": contact_number,
                "birthday": birthday_raw,
            }
        )

        if (
            not full_name
            or not age_raw
            or not sex
            or not church_address
            or not contact_number
            or not birthday_raw
        ):
            error = "All fields are required."
        else:
            try:
                age_int = int(age_raw)
            except ValueError:
                error = "Age must be a number."
            else:
                db = get_db()
                cursor = db.cursor()

                cursor.execute(
                    """
                    SELECT id FROM pastors
                    WHERE full_name = ? AND church_address = ?
                    """,
                    (full_name, church_address),
                )
                existing = cursor.fetchone()
                if existing:
                    error = "Account already exists for this pastor and church."
                else:
                    username, password = generate_pastor_credentials(full_name, age_int)

                    try:
                        cursor.execute(
                            """
                            INSERT INTO pastors
                            (full_name, age, sex, church_address, contact_number, birthday, username, password)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                full_name,
                                age_int,
                                sex,
                                church_address,
                                contact_number,
                                birthday_raw,
                                username,
                                password,
                            ),
                        )
                        db.commit()
                        success = "Account created successfully."
                        generated_username = username
                        generated_password = password

                        pastor_data = {
                            "full_name": full_name,
                            "age": age_int,
                            "sex": sex,
                            "church_address": church_address,
                            "contact_number": contact_number,
                            "birthday": birthday_raw,
                            "username": username,
                            "password": password,
                        }
                        try:
                            append_account_to_sheet(pastor_data)
                        except Exception as e:
                            print("Error sending account to Google Sheets:", e)

                    except sqlite3.IntegrityError:
                        error = "Unable to create account (username conflict). Please try again."

    return render_template(
        "ao_create_account.html",
        error=error,
        success=success,
        values=values,
        generated_username=generated_username,
        generated_password=generated_password,
    )


# ------------------------
# AO Church Status (your design)
# ------------------------
@app.route("/ao-tool/church-status")
def ao_church_status():
    sync_from_sheets_if_needed()
    if not ao_logged_in():
        return redirect(url_for("ao_login", next=request.path))

    today = date.today()
    year = request.args.get("year", type=int) or today.year
    year_options = list(range(today.year - 1, today.year + 4))

    month_names = [
        ("January", 1), ("February", 2), ("March", 3), ("April", 4),
        ("May", 5), ("June", 6), ("July", 7), ("August", 8),
        ("September", 9), ("October", 10), ("November", 11), ("December", 12),
    ]

    all_churches = get_all_churches_from_accounts()

    months = []
    prev_avg_attendance_by_church = {}

    for name, m in month_names:
        church_items = []
        all_reported = True

        for church in all_churches:
            stats = get_report_stats_for_month_and_church(year, m, church)
            has_data = stats["rows"] > 0
            if not has_data:
                all_reported = False

            sheet_status = (stats.get("sheet_status") or "").lower()
            if not has_data:
                status_key = "not_submitted"
            elif "approved" in sheet_status:
                status_key = "approved"
            else:
                status_key = "pending"

            avg_att = stats["avg"]["adult"] + stats["avg"]["youth"] + stats["avg"]["children"]
            prev_att = prev_avg_attendance_by_church.get(church)

            if prev_att is None or prev_att == 0 or not has_data:
                attendance_change = None
            else:
                attendance_change = ((avg_att - prev_att) / prev_att) * 100.0

            if has_data:
                prev_avg_attendance_by_church[church] = avg_att

            church_items.append(
                {
                    "church": church,
                    "rows": stats["rows"],
                    "avg": stats["avg"],
                    "totals": stats["totals"],
                    "status_key": status_key,
                    "attendance_change": attendance_change,
                }
            )

        months.append(
            {
                "name": name,
                "month": m,
                "year": year,
                "all_reported": all_reported,
                "churches": church_items,
            }
        )

    return render_template(
        "ao_church_status.html",
        year=year,
        year_options=year_options,
        months=months,
    )


@app.route("/ao-tool/church-status/approve", methods=["POST"])
def ao_church_status_approve():
    if not ao_logged_in():
        return redirect(url_for("ao_login", next=request.path))

    year = int(request.form.get("year") or 0)
    month = int(request.form.get("month") or 0)
    church = (request.form.get("church") or "").strip()

    if year and month and church:
        try:
            update_sheet_status_for_church_month(year, month, church, "Approved")
        except Exception as e:
            print("Error approving church month:", e)

    return redirect(url_for("ao_church_status", year=year))


# ------------------------
# Other pages
# ------------------------
@app.route("/prayer-request")
def prayer_request():
    return render_template("prayer_request.html")


@app.route("/event-registration")
def event_registration():
    return render_template("event_registration.html")


@app.route("/schedules")
def schedules():
    return render_template("schedules.html")


if __name__ == "__main__":
    app.run(debug=True)
