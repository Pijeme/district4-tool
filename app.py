import os
import sqlite3
from datetime import datetime, date, timezone
import calendar
import urllib.parse
import uuid
import traceback

from zoneinfo import ZoneInfo

import requests
import gspread

from google.oauth2.service_account import Credentials
from google.auth.transport.requests import Request as GoogleAuthRequest
from flask import (
    Flask,
    g,
    render_template,
    request,
    redirect,
    url_for,
    abort,
    session,
    flash,
    jsonify,
    make_response,
    send_file,
)

from area_progress_monitor import register_area_progress_monitor
from schedule import register_schedule_routes
from temp_edit import register_temp_edit_routes

DATABASE = os.path.join(os.path.dirname(__file__), "app_v2.db")
def init_db():
    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS pastors (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        church TEXT,
        phone TEXT,
        email TEXT
    )
    """)

    conn.commit()
    conn.close()
try:
    PH_TZ = ZoneInfo("Asia/Manila")
except Exception:
    PH_TZ = None


def utc_now():
    return datetime.now(timezone.utc)


def utc_now_iso():
    return utc_now().isoformat()

# ========================
# DB helpers
# ========================


def get_db():
    db = getattr(g, "_database", None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE, check_same_thread=False)
        db.row_factory = sqlite3.Row
    return db
def migrate_monthly_reports_scope_to_pastor():
    """One-time SQLite migration.

    Old schema: monthly_reports UNIQUE(year, month)
    New schema: monthly_reports UNIQUE(year, month, pastor_username)

    Because Google Sheets is the source of truth, we can safely rebuild the table.
    We preserve existing IDs so FK references (sunday_reports, church_progress) remain valid.
    Old rows are tagged with pastor_username='__legacy__'.
    """
    db = get_db()
    cur = db.cursor()

    cols = [r["name"] for r in cur.execute("PRAGMA table_info(monthly_reports)").fetchall()]
    if "pastor_username" in cols:
        return

    cur.execute("ALTER TABLE monthly_reports RENAME TO monthly_reports_old")

    cur.execute(
        """
        CREATE TABLE monthly_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            pastor_username TEXT NOT NULL,
            submitted INTEGER DEFAULT 0,
            approved INTEGER DEFAULT 0,
            submitted_at TEXT,
            approved_at TEXT,
            UNIQUE(year, month, pastor_username)
        )
        """
    )

    cur.execute(
        """
        INSERT INTO monthly_reports (id, year, month, pastor_username, submitted, approved, submitted_at, approved_at)
        SELECT id, year, month, '__legacy__', submitted, approved, submitted_at, approved_at
        FROM monthly_reports_old
        """
    )

    cur.execute("DROP TABLE monthly_reports_old")
    db.commit()

def init_db():
    db = get_db()
    cursor = db.cursor()

    # Pastor tool tables (local)
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS monthly_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            pastor_username TEXT NOT NULL,
            submitted INTEGER DEFAULT 0,
            approved INTEGER DEFAULT 0,
            submitted_at TEXT,
            approved_at TEXT,
            UNIQUE(year, month, pastor_username)
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

    # ==========================
    # ✅ CACHE TABLES (Sheets → DB)
    # ==========================
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS sync_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            last_sync TEXT
        )
        """
    )
    cursor.execute("INSERT OR IGNORE INTO sync_state (id, last_sync) VALUES (1, NULL)")

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS print_report_jobs (
            id TEXT PRIMARY KEY,
            user_area TEXT NOT NULL,
            sub_area TEXT,
            report_type TEXT NOT NULL DEFAULT 'ao',
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'queued',
            created_at TEXT NOT NULL,
            started_at TEXT,
            finished_at TEXT,
            error_message TEXT,
            result_path TEXT
        )
        """
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_print_report_jobs_status_created ON print_report_jobs(status, created_at)"
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS submit_report_jobs (
            id TEXT PRIMARY KEY,
            pastor_username TEXT NOT NULL,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'queued',
            progress_message TEXT,
            created_at TEXT NOT NULL,
            started_at TEXT,
            finished_at TEXT,
            error_message TEXT
        )
        """
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_submit_report_jobs_user_month ON submit_report_jobs(pastor_username, year, month, status)"
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS sheet_accounts_cache (
            username TEXT PRIMARY KEY,
            name TEXT,
            church_address TEXT,
            password TEXT,
            age TEXT,
            sex TEXT,
            contact TEXT,
            birthday TEXT,
            position TEXT,
            sub_area TEXT,
            google_pin_location TEXT,
            sheet_row INTEGER
        )
        """
    )

    # Ensure new columns exist (safe migrations)
    cursor.execute("PRAGMA table_info(sheet_accounts_cache)")
    _acc_cols=[row[1] for row in cursor.fetchall()]
    if "position" not in _acc_cols:
        try:
            cursor.execute("ALTER TABLE sheet_accounts_cache ADD COLUMN position TEXT")
        except Exception:
            pass
    if "sub_area" not in _acc_cols:
        try:
            cursor.execute("ALTER TABLE sheet_accounts_cache ADD COLUMN sub_area TEXT")
        except Exception:
            pass
    if "google_pin_location" not in _acc_cols:
        try:
            cursor.execute("ALTER TABLE sheet_accounts_cache ADD COLUMN google_pin_location TEXT")
        except Exception:
            pass

    cursor.execute("PRAGMA table_info(print_report_jobs)")
    _pr_cols=[row[1] for row in cursor.fetchall()]
    if "sub_area" not in _pr_cols:
        try:
            cursor.execute("ALTER TABLE print_report_jobs ADD COLUMN sub_area TEXT")
        except Exception:
            pass
    if "report_type" not in _pr_cols:
        try:
            cursor.execute("ALTER TABLE print_report_jobs ADD COLUMN report_type TEXT NOT NULL DEFAULT 'ao'")
        except Exception:
            pass
    if "print_action" not in _pr_cols:
        try:
            cursor.execute("ALTER TABLE print_report_jobs ADD COLUMN print_action TEXT NOT NULL DEFAULT 'main'")
        except Exception:
            pass

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS sheet_report_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sheet_row INTEGER,
            year INTEGER,
            month INTEGER,
            activity_date TEXT,

            church TEXT,
            pastor TEXT,
            address TEXT,

            adult REAL,
            youth REAL,
            children REAL,

            tithes REAL,
            offering REAL,
            personal_tithes REAL,
            mission_offering REAL,

            received_jesus REAL,
            existing_bible_study REAL,
            new_bible_study REAL,
            water_baptized REAL,
            holy_spirit_baptized REAL,
            childrens_dedication REAL,
            healed REAL,

            amount_to_send REAL,
            status TEXT,
            report_status TEXT
        )
        """
    )

    # -----------------------
    # ✅ AOPT CACHE (AO Personal Tithes)
    # -----------------------
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS sheet_aopt_cache (
            month TEXT NOT NULL,
            area_number TEXT NOT NULL DEFAULT '',
            amount REAL,
            sheet_row INTEGER,
            PRIMARY KEY (month, area_number)
        )
        """
    )

    cursor.execute("PRAGMA table_info(sheet_aopt_cache)")
    _aopt_cols = [row[1] for row in cursor.fetchall()]
    if "area_number" not in _aopt_cols:
        try:
            cursor.execute("ALTER TABLE sheet_aopt_cache ADD COLUMN area_number TEXT NOT NULL DEFAULT ''")
        except Exception:
            pass
    try:
        cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_aopt_month_area ON sheet_aopt_cache(month, area_number)")
    except Exception:
        pass

    cursor.execute("PRAGMA table_info(sheet_aopt_cache)")
    _aopt_cols2 = [row[1] for row in cursor.fetchall()]
    if "sub_area" not in _aopt_cols2:
        try:
            cursor.execute("ALTER TABLE sheet_aopt_cache ADD COLUMN sub_area TEXT NOT NULL DEFAULT ''")
        except Exception:
            pass
    try:
        cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_aopt_month_area_sub ON sheet_aopt_cache(month, area_number, sub_area)")
    except Exception:
        pass

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS sheet_prayer_request_cache (
            request_id TEXT PRIMARY KEY,
            church_name TEXT,
            submitted_by TEXT,
            title TEXT,
            request_date TEXT,
            request_text TEXT,
            status TEXT,
            pastors_praying TEXT,
            answered_date TEXT,
            sheet_row INTEGER
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS sheet_announcement_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            announcement TEXT,
            announcement_date TEXT,
            area TEXT,
            sub_area TEXT,
            author_username TEXT,
            author_name TEXT,
            sheet_row INTEGER
        )
        """
    )
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_announcement_scope ON sheet_announcement_cache(area, sub_area)")

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS developer_visit_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            ip_address TEXT,
            user_agent TEXT,
            actor_username TEXT,
            actor_name TEXT,
            actor_role TEXT,
            request_path TEXT,
            page_label TEXT,
            method TEXT
        )
        """
    )
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_dev_logs_created_at ON developer_visit_logs(created_at)")

    # -----------------------
    # ✅ PRAYER REQUEST CACHE (PrayerRequest sheet → local cache)
    # Sheet tab: "PrayerRequest"
    # Columns:
    # Church Name | Submitted By | Request ID | Prayer Request Title | Prayer Request Date
    # Prayer Request | Status | Pastor's Praying | Answered Date
    # -----------------------
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS sheet_district_schedule_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            church_name TEXT,
            church_address TEXT,
            pastor_name TEXT,
            contact_number TEXT,
            activity_date_start TEXT,
            activity_date_end TEXT,
            activity_type TEXT,
            note TEXT,
            joining TEXT,
            theme TEXT,
            text TEXT,
            sheet_row INTEGER
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS sheet_chain_prayer_schedule_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            church_name_assigned TEXT,
            pastor_name TEXT,
            prayer_date TEXT,
            sheet_row INTEGER
        )
        """
    )

    cursor.execute("PRAGMA table_info(sheet_chain_prayer_schedule_cache)")
    _cp_cols = [row[1] for row in cursor.fetchall()]
    if "pastor_name" not in _cp_cols:
        try:
            cursor.execute("ALTER TABLE sheet_chain_prayer_schedule_cache ADD COLUMN pastor_name TEXT")
        except Exception:
            pass

    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_chain_prayer_date ON sheet_chain_prayer_schedule_cache(prayer_date)"
    )
    cursor.execute("PRAGMA table_info(sheet_district_schedule_cache)")
    _ds_cols = [row[1] for row in cursor.fetchall()]
    if "joining" not in _ds_cols:
        try:
            cursor.execute("ALTER TABLE sheet_district_schedule_cache ADD COLUMN joining TEXT")
        except Exception:
            pass
    if "theme" not in _ds_cols:
        try:
            cursor.execute("ALTER TABLE sheet_district_schedule_cache ADD COLUMN theme TEXT")
        except Exception:
            pass
    if "text" not in _ds_cols:
        try:
            cursor.execute("ALTER TABLE sheet_district_schedule_cache ADD COLUMN text TEXT")
        except Exception:
            pass

    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_district_schedule_start ON sheet_district_schedule_cache(activity_date_start)"
    )
    
    cursor.execute("PRAGMA table_info(sheet_report_cache)")
    _rep_cols = [row[1] for row in cursor.fetchall()]
    if "report_status" not in _rep_cols:
        try:
            cursor.execute("ALTER TABLE sheet_report_cache ADD COLUMN report_status TEXT")
        except Exception:
            pass

    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_report_ym_addr ON sheet_report_cache(year, month, address)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_report_ym_church ON sheet_report_cache(year, month, church)"
    )
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_report_ym ON sheet_report_cache(year, month)")

    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_prayer_submitted_by ON sheet_prayer_request_cache(submitted_by)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_prayer_status ON sheet_prayer_request_cache(status)"
    )

    migrate_monthly_reports_scope_to_pastor()
    db.commit()


# ========================
# Monthly / Sunday helpers
# ========================


def get_or_create_monthly_report(year: int, month: int, pastor_username: str):
    pastor_username = (pastor_username or "").strip()
    if not pastor_username:
        raise ValueError("Missing pastor_username in session.")

    db = get_db()
    cursor = db.cursor()

    cursor.execute(
        "SELECT * FROM monthly_reports WHERE year = ? AND month = ? AND pastor_username = ?",
        (year, month, pastor_username),
    )
    row = cursor.fetchone()
    if row:
        return row

    cursor.execute(
        """
        INSERT INTO monthly_reports (year, month, pastor_username, submitted, approved)
        VALUES (?, ?, ?, 0, 0)
        """,
        (year, month, pastor_username),
    )
    db.commit()

    cursor.execute(
        "SELECT * FROM monthly_reports WHERE year = ? AND month = ? AND pastor_username = ?",
        (year, month, pastor_username),
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
        cursor.execute(
            """
            INSERT OR IGNORE INTO sunday_reports (monthly_report_id, date)
            VALUES (?, ?)
            """,
            (monthly_report_id, d.isoformat()),
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


def set_month_submitted(year: int, month: int, pastor_username: str):
    pastor_username = (pastor_username or "").strip()
    if not pastor_username:
        raise ValueError("Missing pastor_username in session.")

    db = get_db()
    cursor = db.cursor()
    now_str = utc_now_iso()
    cursor.execute(
        """
        UPDATE monthly_reports
        SET submitted = 1,
            submitted_at = ?,
            approved = 0,
            approved_at = NULL
        WHERE year = ? AND month = ? AND pastor_username = ?
        """,
        (now_str, year, month, pastor_username),
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


# ========================
# Google Sheets integration
# ========================

GOOGLE_SHEETS_CREDENTIALS_FILE = "service_account.json"
GOOGLE_SHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

APPS_SCRIPT_PREP_URL = "https://script.google.com/macros/s/AKfycbxaDvRol8XtOTynQKbO2qF395qhW0W832bggGcPX2FjcRVfUjqqc8vxTNY-kdGkDTRA/exec"
APPS_SCRIPT_PREP_TOKEN = "psr550pijeme"
def _prepare_report_print_via_apps_script(area_number: str, year: int, month: int, report_type: str = "ao", sub_area: str = ""):
    payload = {
        "action": "prepare_sub_area_report_print" if report_type == "sub_area" else "prepare_ao_report_print",
        "token": APPS_SCRIPT_PREP_TOKEN,
        "area_number": str(area_number or "").strip(),
        "year": int(year),
        "month": int(month),
    }
    if report_type == "sub_area":
        payload["sub_area"] = str(sub_area or "").strip()

    resp = requests.post(
        APPS_SCRIPT_PREP_URL,
        json=payload,
        timeout=60,
    )
    resp.raise_for_status()

    data = resp.json()
    if not data.get("ok"):
        raise RuntimeError(data.get("error") or "Apps Script prepare failed.")

    return data

def get_gs_client():
    creds = Credentials.from_service_account_file(
        GOOGLE_SHEETS_CREDENTIALS_FILE,
        scopes=GOOGLE_SHEETS_SCOPES,
    )
    return gspread.authorize(creds)


def parse_float(value):
    try:
        s = str(value).strip()
        if s == "":
            return 0.0
        s = s.replace(",", "")
        return float(s)
    except Exception:
        return 0.0


def format_php_currency(value):
    try:
        amount = float(value or 0)
    except Exception:
        amount = 0.0
    return f"₱{amount:,.2f}"


def parse_sheet_date(value):
    """Parse a date string from Google Sheets.

    Accepts both:
    - ISO: YYYY-MM-DD
    - Slash: M/D/YYYY (e.g., 1/25/2026)
    """
    s = str(value or "").strip()
    if not s:
        return None
    # Try ISO first
    try:
        return date.fromisoformat(s)
    except Exception:
        pass
    # Try M/D/YYYY
    try:
        parts = s.split("/")
        if len(parts) == 3:
            m = int(parts[0])
            d = int(parts[1])
            y = int(parts[2])
            return date(y, m, d)
    except Exception:
        return None
    return None
def _lower(s):
    return str(s or "").strip().lower()


def _find_col(headers, wanted):
    wanted = _lower(wanted)
    for i, h in enumerate(headers):
        if _lower(h) == wanted:
            return i
    return None


# ==========================
# ✅ Sheets → DB cache sync
# ==========================

SYNC_INTERVAL_SECONDS = 120  # 2 minutes


def _last_sync_time_utc():
    row = get_db().execute("SELECT last_sync FROM sync_state WHERE id = 1").fetchone()
    if row and row["last_sync"]:
        try:
            dt = datetime.fromisoformat(row["last_sync"])
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            return None
    return None


def get_last_sync_display_ph():
    """
    Returns a user-friendly PH time string from sync_state.last_sync (stored as UTC ISO).
    """
    dt_utc = _last_sync_time_utc()
    if not dt_utc:
        return "Never"
    try:
        dt_utc = dt_utc.replace(tzinfo=ZoneInfo("UTC"))
        dt_ph = dt_utc.astimezone(PH_TZ)
        return dt_ph.strftime("%b %d, %Y %I:%M %p")
    except Exception:
        return "Unknown"


def _update_sync_time():
    get_db().execute(
        "UPDATE sync_state SET last_sync = ? WHERE id = 1",
        (utc_now_iso(),),
    )
    get_db().commit()
    
def ensure_schedule_cache_loaded():
    """
    For the Schedule page only:
    - if either district schedule cache or chain prayer cache is empty,
      load once from Sheets
    - otherwise use local DB only
    """
    db = get_db()

    row_ds = db.execute(
        "SELECT COUNT(*) AS cnt FROM sheet_district_schedule_cache"
    ).fetchone()
    district_count = int(row_ds["cnt"] or 0) if row_ds else 0

    row_cp = db.execute(
        "SELECT COUNT(*) AS cnt FROM sheet_chain_prayer_schedule_cache"
    ).fetchone()
    chain_count = int(row_cp["cnt"] or 0) if row_cp else 0

    if district_count <= 0 or chain_count <= 0:
        sync_from_sheets_if_needed(force=True)


def sync_from_sheets_if_needed(force=False):
    """
    Reads Google Sheets ONLY once per interval, stores into cache tables.
    AO pages read ONLY from cache tables (no quota spam).
    """
    last = _last_sync_time_utc()
    if not force and last and (utc_now() - last).total_seconds() < SYNC_INTERVAL_SECONDS:
        return

    try:
        client = get_gs_client()
        sh = client.open("District4 Data")
    except Exception as e:
        print("❌ Sync failed (open sheet):", e)
        return

    db = get_db()
    cur = db.cursor()

    # -----------------------
    # ACCOUNTS (with sheet_row)
    # -----------------------
    try:
        ws_accounts = sh.worksheet("Accounts")
        acc_values = ws_accounts.get_all_values()
    except Exception as e:
        print("❌ Accounts sync failed:", e)
        acc_values = []

    cur.execute("DELETE FROM sheet_accounts_cache")

    if acc_values and len(acc_values) >= 2:
        headers = acc_values[0]
        i_name = _find_col(headers, "Name")
        i_user = _find_col(headers, "UserName")
        i_pass = _find_col(headers, "Password")
        i_addr = _find_col(headers, "Church Address")
        i_age = _find_col(headers, "Area Number")
        if i_age is None:
            i_age = _find_col(headers, "Age")
        i_sex = _find_col(headers, "Church ID")
        if i_sex is None:
            i_sex = _find_col(headers, "Sex")
        i_contact = _find_col(headers, "Contact #")
        i_bday = _find_col(headers, "Birth Day")
        i_pos = _find_col(headers, "Position")
        i_sub = _find_col(headers, "Sub Area")
        if i_sub is None:
            i_sub = _find_col(headers, "SubArea")
        i_pin = _find_col(headers, "GooglePinLocation")

        def cell(row, idx):
            if idx is None:
                return ""
            if idx < len(row):
                return row[idx]
            return ""

        for r in range(1, len(acc_values)):
            row = acc_values[r]

            username = str(cell(row, i_user)).strip()
            password = str(cell(row, i_pass)).strip()
            full_name = str(cell(row, i_name)).strip()
            church_address = str(cell(row, i_addr)).strip()
            area_number = str(cell(row, i_age)).strip()
            church_id = str(cell(row, i_sex)).strip()
            contact = str(cell(row, i_contact)).strip()
            birthday = str(cell(row, i_bday)).strip()
            position = str(cell(row, i_pos)).strip()
            sub_area = str(cell(row, i_sub)).strip()
            google_pin_location = str(cell(row, i_pin)).strip()

            # keep rows that have the search essentials even if username/password are blank
            if not area_number and not church_id and not full_name and not church_address:
                continue

            cur.execute(
                """
                INSERT OR REPLACE INTO sheet_accounts_cache
                (username, name, church_address, password, age, sex, contact, birthday, position, sub_area, google_pin_location, sheet_row)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    username,
                    full_name,
                    church_address,
                    password,
                    area_number,
                    church_id,
                    contact,
                    birthday,
                    position,
                    sub_area,
                    google_pin_location,
                    r + 1,
                ),
            )

    # -----------------------
    # REPORT (with sheet_row)
    # -----------------------
    try:
        ws_report = sh.worksheet("Report")
        rep_values = ws_report.get_all_values()
    except Exception as e:
        print("❌ Report sync failed:", e)
        rep_values = []

    cur.execute("DELETE FROM sheet_report_cache")

    if rep_values and len(rep_values) >= 2:
        headers = rep_values[0]

        i_activity = _find_col(headers, "activity_date")

        # Church approval status (Approved / Pending) for Church Status colors
        i_status = _find_col(headers, "Status")
        if i_status is None:
            i_status = _find_col(headers, "status")

        # Print workflow status (MainPrint / LatePrint / Received)
        i_report_status = _find_col(headers, "ReportStatus")

        i_church = _find_col(headers, "church")
        i_pastor = _find_col(headers, "pastor")
        i_address = _find_col(headers, "address")

        i_adult = _find_col(headers, "adult")
        i_youth = _find_col(headers, "youth")
        i_children = _find_col(headers, "children")

        i_tithes = _find_col(headers, "tithes")
        i_offering = _find_col(headers, "offering")
        i_personal = _find_col(headers, "personal tithes")
        i_mission = _find_col(headers, "mission offering")

        i_recv = _find_col(headers, "received jesus")
        i_exist = _find_col(headers, "existing bible study")
        i_new = _find_col(headers, "new bible study")
        i_water = _find_col(headers, "water baptized")
        i_holy = _find_col(headers, "holy spirit baptized")
        i_ded = _find_col(headers, "childrens dedication")
        i_healed = _find_col(headers, "healed")

        i_send = _find_col(headers, "amount to send")

        def cell(row, idx):
            if idx is None:
                return ""
            if idx < len(row):
                return row[idx]
            return ""

        for r in range(1, len(rep_values)):
            row = rep_values[r]
            activity = str(cell(row, i_activity)).strip()
            if not activity:
                continue
            d = parse_sheet_date(activity)
            if not d:
                continue

            cur.execute(
                """
                INSERT INTO sheet_report_cache (
                    sheet_row, year, month, activity_date,
                    church, pastor, address,
                    adult, youth, children,
                    tithes, offering, personal_tithes, mission_offering,
                    received_jesus, existing_bible_study, new_bible_study,
                    water_baptized, holy_spirit_baptized, childrens_dedication, healed,
                    amount_to_send, status, report_status
                ) VALUES (
                    ?, ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?
                )
                """,
                (
                    r + 1,
                    d.year,
                    d.month,
                    d.isoformat(),
                    str(cell(row, i_church)).strip(),
                    str(cell(row, i_pastor)).strip(),
                    str(cell(row, i_address)).strip(),
                    parse_float(cell(row, i_adult)),
                    parse_float(cell(row, i_youth)),
                    parse_float(cell(row, i_children)),
                    parse_float(cell(row, i_tithes)),
                    parse_float(cell(row, i_offering)),
                    parse_float(cell(row, i_personal)),
                    parse_float(cell(row, i_mission)),
                    parse_float(cell(row, i_recv)),
                    parse_float(cell(row, i_exist)),
                    parse_float(cell(row, i_new)),
                    parse_float(cell(row, i_water)),
                    parse_float(cell(row, i_holy)),
                    parse_float(cell(row, i_ded)),
                    parse_float(cell(row, i_healed)),
                    parse_float(cell(row, i_send)),
                    str(cell(row, i_status)).strip(),
                    str(cell(row, i_report_status)).strip(),
                ),
            )

    # -----------------------
    # AOPT (AO Personal Tithes)
    # -----------------------
    try:
        ws_aopt = sh.worksheet("AOPT")
        aopt_values = ws_aopt.get_all_values()
    except Exception as e:
        print("❌ AOPT sync failed:", e)
        aopt_values = []

    cur.execute("DELETE FROM sheet_aopt_cache")

    if aopt_values and len(aopt_values) >= 2:
        headers = aopt_values[0]
        i_month = _find_col(headers, "Month")
        i_amount = _find_col(headers, "Amount")
        i_area = _find_col(headers, "Area Number")
        if i_area is None:
            i_area = _find_col(headers, "Area")
        i_sub_area = _find_col(headers, "Sub Area")
        if i_sub_area is None:
            i_sub_area = _find_col(headers, "SubArea")

        def cell(row, idx):
            if idx is None:
                return ""
            if idx < len(row):
                return row[idx]
            return ""

        for r in range(1, len(aopt_values)):
            row = aopt_values[r]
            month_label = str(cell(row, i_month)).strip()
            if not month_label:
                continue
            amount_val = parse_float(cell(row, i_amount))
            area_number = str(cell(row, i_area)).strip()
            sub_area = str(cell(row, i_sub_area)).strip()
            cur.execute(
                """
                INSERT OR REPLACE INTO sheet_aopt_cache (month, area_number, sub_area, amount, sheet_row)
                VALUES (?, ?, ?, ?, ?)
                """,
                (month_label, area_number, sub_area, amount_val, r + 1),
            )

    # -----------------------
    # PRAYER REQUEST (PrayerRequest)
    # -----------------------
    try:
        ws_pr = sh.worksheet("PrayerRequest")
        pr_values = ws_pr.get_all_values()
    except Exception as e:
        print("❌ PrayerRequest sync failed:", e)
        pr_values = []

    cur.execute("DELETE FROM sheet_prayer_request_cache")

    if pr_values and len(pr_values) >= 2:
        headers = pr_values[0]

        i_church = _find_col(headers, "Church Name")
        i_submitted_by = _find_col(headers, "Submitted By")
        i_request_id = _find_col(headers, "Request ID")
        i_title = _find_col(headers, "Prayer Request Title")
        i_request_date = _find_col(headers, "Prayer Request Date")
        i_request_text = _find_col(headers, "Prayer Request")
        i_status = _find_col(headers, "status")
        if i_status is None:
            i_status = _find_col(headers, "status")
        i_praying = _find_col(headers, "Pastor's Praying")
        i_answered = _find_col(headers, "Answered Date")

        def cell(row, idx):
            if idx is None:
                return ""
            if idx < len(row):
                return row[idx]
            return ""

        for r in range(1, len(pr_values)):
            row = pr_values[r]
            req_id = str(cell(row, i_request_id)).strip()
            if not req_id:
                continue

            cur.execute(
                """
                INSERT OR REPLACE INTO sheet_prayer_request_cache (
                    request_id, church_name, submitted_by, title, request_date,
                    request_text, status, pastors_praying, answered_date, sheet_row
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    req_id,
                    str(cell(row, i_church)).strip(),
                    str(cell(row, i_submitted_by)).strip(),
                    str(cell(row, i_title)).strip(),
                    str(cell(row, i_request_date)).strip(),
                    str(cell(row, i_request_text)).strip(),
                    str(cell(row, i_status)).strip(),
                    str(cell(row, i_praying)).strip(),
                    str(cell(row, i_answered)).strip(),
                    r + 1,
                ),
            )

    db.commit()
    # -----------------------
    # DISTRICT SCHEDULE
    # -----------------------
    try:
        ws_ds = sh.worksheet("DistrictSchedule")
        ds_values = ws_ds.get_all_values()
    except Exception as e:
        print("❌ DistrictSchedule sync failed:", e)
        ds_values = []

    cur.execute("DELETE FROM sheet_district_schedule_cache")

    if ds_values and len(ds_values) >= 2:
        headers = ds_values[0]

        i_church_name = _find_col(headers, "Church Name")
        i_church_address = _find_col(headers, "Church Address")
        i_pastor_name = _find_col(headers, "Pastor's Name")
        i_contact_number = _find_col(headers, "Contact Number")
        i_activity_start = _find_col(headers, "Activity Date Start")
        i_activity_end = _find_col(headers, "Activity Date End")
        i_activity_type = _find_col(headers, "Activity Type")
        i_note = _find_col(headers, "Note")
        i_joining = _find_col(headers, "Joining")
        i_theme = _find_col(headers, "Theme")
        i_text = _find_col(headers, "Text")

        def ds_cell(row, idx):
            if idx is None:
                return ""
            return row[idx].strip() if idx < len(row) else ""

        for rnum, row in enumerate(ds_values[1:], start=2):
            church_name = ds_cell(row, i_church_name)
            activity_start = ds_cell(row, i_activity_start)

            if not church_name or not activity_start:
                continue

            cur.execute(
                """
                INSERT INTO sheet_district_schedule_cache (
                    church_name,
                    church_address,
                    pastor_name,
                    contact_number,
                    activity_date_start,
                    activity_date_end,
                    activity_type,
                    note,
                    joining,
                    theme,
                    text,
                    sheet_row
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    church_name,
                    ds_cell(row, i_church_address),
                    ds_cell(row, i_pastor_name),
                    ds_cell(row, i_contact_number),
                    activity_start,
                    ds_cell(row, i_activity_end),
                    ds_cell(row, i_activity_type),
                    ds_cell(row, i_note),
                    ds_cell(row, i_joining),
                    ds_cell(row, i_theme),
                    ds_cell(row, i_text),
                    rnum,
                ),
            )
        # -----------------------
    # CHAIN PRAYER SCHEDULE
    # -----------------------
    try:
        ws_cp = sh.worksheet("ChainPrayerSchedules")
        cp_values = ws_cp.get_all_values()
    except Exception as e:
        print("❌ ChainPrayerSchedules sync failed:", e)
        cp_values = []

    cur.execute("DELETE FROM sheet_chain_prayer_schedule_cache")

    if cp_values and len(cp_values) >= 2:
        headers = cp_values[0]

        i_church_name_assigned = _find_col(headers, "ChurchNameAssigned")
        i_pastor_name = _find_col(headers, "Pastor")
        i_prayer_date = _find_col(headers, "Date")

        def cp_cell(row, idx):
            if idx is None:
                return ""
            return row[idx].strip() if idx < len(row) else ""

        for rnum, row in enumerate(cp_values[1:], start=2):
            church_name_assigned = cp_cell(row, i_church_name_assigned)
            pastor_name = cp_cell(row, i_pastor_name)
            prayer_date = cp_cell(row, i_prayer_date)

            if not church_name_assigned or not prayer_date:
                continue

            cur.execute(
                """
                INSERT INTO sheet_chain_prayer_schedule_cache (
                    church_name_assigned,
                    pastor_name,
                    prayer_date,
                    sheet_row
                )
                VALUES (?, ?, ?, ?)
                """,
                (
                    church_name_assigned,
                    pastor_name,
                    prayer_date,
                    rnum,
                ),
            )

    # -----------------------
    # ANOUNCEMENT
    # -----------------------
    try:
        ws_ann = sh.worksheet("Anouncement")
        ann_values = ws_ann.get_all_values()
    except Exception as e:
        print("❌ Anouncement sync failed:", e)
        ann_values = []

    cur.execute("DELETE FROM sheet_announcement_cache")

    if ann_values and len(ann_values) >= 2:
        headers = ann_values[0]
        i_title = _find_col(headers, "Title")
        i_announcement = _find_col(headers, "Announcement")
        i_date = _find_col(headers, "Date")
        i_area = _find_col(headers, "Area")
        i_sub = _find_col(headers, "SubArea")
        if i_sub is None:
            i_sub = _find_col(headers, "Sub Area")
        i_author_u = _find_col(headers, "Author Username")
        i_author_n = _find_col(headers, "Author Name")

        def ann_cell(row, idx):
            if idx is None:
                return ""
            return row[idx].strip() if idx < len(row) else ""

        for rnum, row in enumerate(ann_values[1:], start=2):
            title = ann_cell(row, i_title)
            body = ann_cell(row, i_announcement)
            if not title and not body:
                continue
            cur.execute(
                """
                INSERT INTO sheet_announcement_cache (
                    title, announcement, announcement_date, area, sub_area,
                    author_username, author_name, sheet_row
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    title,
                    body,
                    ann_cell(row, i_date),
                    ann_cell(row, i_area),
                    ann_cell(row, i_sub),
                    ann_cell(row, i_author_u),
                    ann_cell(row, i_author_n),
                    rnum,
                ),
            )


    _update_sync_time()
    print("✅ Sheets cache sync done.")


# ========================
# Cache-based helpers (NO Sheets calls)
# ========================

def get_aopt_amount_from_cache(month_label: str, area_number: str = "", sub_area: str = ""):
    area_number = str(area_number or "").strip()
    sub_area = str(sub_area or "").strip()
    db = get_db()
    if sub_area:
        row = db.execute(
            """
            SELECT amount
            FROM sheet_aopt_cache
            WHERE month = ? AND TRIM(area_number) = TRIM(?) AND TRIM(COALESCE(sub_area,'')) = TRIM(?)
            ORDER BY sheet_row DESC
            LIMIT 1
            """,
            (month_label, area_number, sub_area),
        ).fetchone()
        if row:
            return row["amount"]

    row = db.execute(
        """
        SELECT amount
        FROM sheet_aopt_cache
        WHERE month = ? AND TRIM(area_number) = TRIM(?) AND TRIM(COALESCE(sub_area,'')) = ''
        ORDER BY sheet_row DESC
        LIMIT 1
        """,
        (month_label, area_number),
    ).fetchone()
    if row:
        return row["amount"]

    if not area_number:
        row = db.execute(
            "SELECT amount FROM sheet_aopt_cache WHERE month = ? ORDER BY sheet_row DESC LIMIT 1",
            (month_label,),
        ).fetchone()
        return row["amount"] if row else None
    return None


def _ensure_aopt_headers(ws):
    values = ws.get_all_values()
    headers = ["Month", "Amount", "Area", "SubArea"]
    if not values:
        ws.append_row(headers, value_input_option="USER_ENTERED")
        return headers

    current = list(values[0])
    needed = [("Month",0), ("Amount",1), ("Area",2), ("SubArea",3)]
    changed = False
    for name, pos in needed:
        if _find_col(current, name) is None and not (name=="Area" and _find_col(current, "Area Number") is not None) and not (name=="SubArea" and _find_col(current, "Sub Area") is not None):
            while len(current) <= pos:
                current.append("")
            current[pos] = name
            changed = True
    if changed:
        rng = f"A1:{chr(ord('A') + len(current) - 1)}1"
        ws.update(rng, [current], value_input_option="USER_ENTERED")
        values = ws.get_all_values()
        return values[0]
    return current


def _get_report_print_sheet(report_type: str = "ao", print_action: str = "main"):
    client = get_gs_client()
    sh = client.open("District4 Data")
    report_type = str(report_type or "ao").strip()
    print_action = str(print_action or "main").strip().lower()

    if report_type == "sub_area":
        sheet_name = "Late SubAreaPrint" if print_action == "late" else "SubAreaPrint"
    else:
        sheet_name = "Late AO Report Print" if print_action == "late" else "AO Report Print"

    ws = sh.worksheet(sheet_name)
    return client, sh, ws


def _prepare_report_print_sheet_direct(area_number: str, year: int, month: int, report_type: str = "ao", sub_area: str = "", print_action: str = "main"):
    """Fallback direct sheet preparation for main/late print tabs.
    Keeps the existing Apps Script flow for main print, but also allows late sheets to receive the correct controls.
    """
    _, _, ws = _get_report_print_sheet(report_type=report_type, print_action=print_action)
    month_name = calendar.month_name[int(month)]
    ws.update("I1", [[str(area_number or "").strip()]])
    if str(report_type or "").strip() == "sub_area":
        ws.update("H2", [[str(sub_area or "").strip()]])
    ws.update("K2", [[month_name]])
    ws.update("M2", [[int(year)]])
    return ws


def _get_report_status_column(ws):
    values = ws.get_all_values()
    if not values:
        return None, []
    headers = values[0]
    idx = _find_col(headers, "ReportStatus")
    if idx is None:
        idx = _find_col(headers, "status")
    return idx, headers


def _get_scope_church_keys(area_number: str, sub_area: str = ""):
    db = get_db()
    params = [str(area_number or "").strip()]
    extra = ""
    sub_area = str(sub_area or "").strip()
    if sub_area:
        extra = " AND TRIM(COALESCE(sub_area, '')) = TRIM(?)"
        params.append(sub_area)
    rows = db.execute(
        f"""
        SELECT DISTINCT TRIM(sex) AS church_key
        FROM sheet_accounts_cache
        WHERE TRIM(age) = TRIM(?)
          AND LOWER(TRIM(COALESCE(position, ''))) = 'pastor'
          AND TRIM(sex) != ''
          {extra}
        """,
        tuple(params),
    ).fetchall()
    return {str(r["church_key"] or "").strip() for r in rows if str(r["church_key"] or "").strip()}



def _get_report_scope_sheet_rows(year: int, month: int, area_number: str, sub_area: str = "", status_mode: str = "all"):
    """
    Returns actual Report sheet row numbers for the selected scope.

    status_mode:
      - "all": all submitted rows in scope
      - "blank": only rows whose ReportStatus is blank
      - "printed": only rows whose ReportStatus is non-blank
    """
    sync_from_sheets_if_needed(force=False)

    db = get_db()
    area_number = str(area_number or "").strip()
    sub_area = str(sub_area or "").strip()
    status_mode = str(status_mode or "all").strip().lower()

    if not area_number:
        return []

    params = [int(year), int(month), area_number]
    sub_sql = ""
    if sub_area:
        sub_sql = " AND TRIM(COALESCE(a.sub_area, '')) = TRIM(?) "
        params.append(sub_area)

    rows = db.execute(
        f"""
        SELECT DISTINCT
            r.sheet_row,
            TRIM(COALESCE(r.report_status, '')) AS report_status
        FROM sheet_report_cache r
        JOIN sheet_accounts_cache a
          ON (
               TRIM(COALESCE(a.sex, '')) = TRIM(COALESCE(r.address, ''))
            OR TRIM(COALESCE(a.sex, '')) = TRIM(COALESCE(r.church, ''))
            OR TRIM(COALESCE(a.church_address, '')) = TRIM(COALESCE(r.address, ''))
            OR TRIM(COALESCE(a.church_address, '')) = TRIM(COALESCE(r.church, ''))
          )
        WHERE r.year = ?
          AND r.month = ?
          AND r.sheet_row IS NOT NULL
          AND TRIM(COALESCE(a.age, '')) = TRIM(?)
          AND LOWER(TRIM(COALESCE(a.position, ''))) = 'pastor'
          {sub_sql}
        ORDER BY r.sheet_row ASC
        """,
        tuple(params),
    ).fetchall()

    out = []
    for row in rows:
        status_val = str(row["report_status"] or "").strip()
        if status_mode == "blank" and status_val:
            continue
        if status_mode == "printed" and not status_val:
            continue
        out.append(int(row["sheet_row"] or 0))

    return sorted(set(r for r in out if r))


def _get_report_status_records_for_scope(year: int, month: int, area_number: str, sub_area: str = ""):
    """
    Uses LOCAL CACHE ONLY.
    Returns latest cached ReportStatus rows for the selected area/sub-area scope.
    """
    sync_from_sheets_if_needed(force=False)

    db = get_db()
    area_number = str(area_number or "").strip()
    sub_area = str(sub_area or "").strip()

    if not area_number:
        return []

    params = [int(year), int(month), area_number]
    sub_sql = ""
    if sub_area:
        sub_sql = " AND TRIM(COALESCE(a.sub_area, '')) = TRIM(?) "
        params.append(sub_area)

    rows = db.execute(
        f"""
        SELECT
            TRIM(COALESCE(a.sex, '')) AS church_id,
            TRIM(COALESCE(a.church_address, '')) AS church_address,
            TRIM(COALESCE(r.report_status, '')) AS report_status,
            r.sheet_row
        FROM sheet_report_cache r
        JOIN sheet_accounts_cache a
          ON (
               TRIM(COALESCE(a.sex, '')) = TRIM(COALESCE(r.address, ''))
            OR TRIM(COALESCE(a.sex, '')) = TRIM(COALESCE(r.church, ''))
            OR TRIM(COALESCE(a.church_address, '')) = TRIM(COALESCE(r.address, ''))
            OR TRIM(COALESCE(a.church_address, '')) = TRIM(COALESCE(r.church, ''))
          )
        WHERE r.year = ?
          AND r.month = ?
          AND TRIM(COALESCE(a.age, '')) = TRIM(?)
          AND LOWER(TRIM(COALESCE(a.position, ''))) = 'pastor'
          {sub_sql}
        ORDER BY r.sheet_row DESC
        """,
        tuple(params),
    ).fetchall()

    records = []
    seen = set()
    for row in rows:
        church_key = (row["church_id"] or "").strip() or (row["church_address"] or "").strip()
        if not church_key:
            continue
        if church_key in seen:
            continue
        seen.add(church_key)
        records.append({
            "church_key": church_key,
            "status": (row["report_status"] or "").strip(),
            "sheet_row": int(row["sheet_row"] or 0),
        })
    return records


def _get_report_status_map_for_scope(year: int, month: int, area_number: str, sub_area: str = ""):
    return {rec["church_key"]: rec["status"] for rec in _get_report_status_records_for_scope(year, month, area_number, sub_area)}


def _sheet_batch_update_report_status_rows(sheet_rows, status_label: str):
    if not sheet_rows:
        return
    client = get_gs_client()
    sh = client.open("District4 Data")
    ws = sh.worksheet("Report")
    idx, headers = _get_report_status_column(ws)
    if idx is None:
        raise RuntimeError("Report sheet missing ReportStatus/status header")
    col_letter = chr(ord('A') + idx)
    requests_body = []
    for r in sorted(set(int(x) for x in sheet_rows if x)):
        requests_body.append({"range": f"{col_letter}{r}", "values": [[status_label]]})
    if requests_body:
        ws.batch_update(requests_body)



def _sync_report_cache_only():
    """
    Refresh only the local Report cache from Google Sheets.
    Keeps Church Status/print buttons in sync without reloading all sheets.
    """
    try:
        client = get_gs_client()
        sh = client.open("District4 Data")
        ws_report = sh.worksheet("Report")
        rep_values = ws_report.get_all_values()
    except Exception as e:
        print("❌ Report-only sync failed:", e)
        return

    db = get_db()
    cur = db.cursor()
    cur.execute("DELETE FROM sheet_report_cache")

    if rep_values and len(rep_values) >= 2:
        headers = rep_values[0]

        i_activity = _find_col(headers, "activity_date")
        i_status = _find_col(headers, "Status")
        if i_status is None:
            i_status = _find_col(headers, "status")
        i_report_status = _find_col(headers, "ReportStatus")

        i_church = _find_col(headers, "church")
        i_pastor = _find_col(headers, "pastor")
        i_address = _find_col(headers, "address")

        i_adult = _find_col(headers, "adult")
        i_youth = _find_col(headers, "youth")
        i_children = _find_col(headers, "children")

        i_tithes = _find_col(headers, "tithes")
        i_offering = _find_col(headers, "offering")
        i_personal = _find_col(headers, "personal tithes")
        i_mission = _find_col(headers, "mission offering")

        i_recv = _find_col(headers, "received jesus")
        i_exist = _find_col(headers, "existing bible study")
        i_new = _find_col(headers, "new bible study")
        i_water = _find_col(headers, "water baptized")
        i_holy = _find_col(headers, "holy spirit baptized")
        i_ded = _find_col(headers, "childrens dedication")
        i_healed = _find_col(headers, "healed")

        i_send = _find_col(headers, "amount to send")

        def cell(row, idx):
            if idx is None:
                return ""
            return row[idx] if idx < len(row) else ""

        for r in range(1, len(rep_values)):
            row = rep_values[r]
            activity = str(cell(row, i_activity)).strip()
            if not activity:
                continue
            d = parse_sheet_date(activity)
            if not d:
                continue

            cur.execute(
                """
                INSERT INTO sheet_report_cache (
                    sheet_row, year, month, activity_date,
                    church, pastor, address,
                    adult, youth, children,
                    tithes, offering, personal_tithes, mission_offering,
                    received_jesus, existing_bible_study, new_bible_study,
                    water_baptized, holy_spirit_baptized, childrens_dedication, healed,
                    amount_to_send, status, report_status
                ) VALUES (
                    ?, ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?
                )
                """,
                (
                    r + 1,
                    d.year,
                    d.month,
                    d.isoformat(),
                    str(cell(row, i_church)).strip(),
                    str(cell(row, i_pastor)).strip(),
                    str(cell(row, i_address)).strip(),
                    parse_float(cell(row, i_adult)),
                    parse_float(cell(row, i_youth)),
                    parse_float(cell(row, i_children)),
                    parse_float(cell(row, i_tithes)),
                    parse_float(cell(row, i_offering)),
                    parse_float(cell(row, i_personal)),
                    parse_float(cell(row, i_mission)),
                    parse_float(cell(row, i_recv)),
                    parse_float(cell(row, i_exist)),
                    parse_float(cell(row, i_new)),
                    parse_float(cell(row, i_water)),
                    parse_float(cell(row, i_holy)),
                    parse_float(cell(row, i_ded)),
                    parse_float(cell(row, i_healed)),
                    parse_float(cell(row, i_send)),
                    str(cell(row, i_status)).strip(),
                    str(cell(row, i_report_status)).strip(),
                ),
            )

    db.commit()
    _update_sync_time()


def _reset_report_status_for_scope(year: int, month: int, area_number: str, sub_area: str = ""):
    records = _get_report_status_records_for_scope(year, month, area_number, sub_area)
    if any(str(rec["status"]).strip().lower() == 'received' for rec in records):
        raise RuntimeError("Cannot refresh print after status is Received.")

    sheet_rows = _get_report_scope_sheet_rows(year, month, area_number, sub_area, status_mode="all")
    if not sheet_rows:
        return

    _sheet_batch_update_report_status_rows(sheet_rows, "")

    try:
        _sync_report_cache_only()
    except Exception:
        pass



def _mark_scope_rows_for_print(job, print_label: str):
    year = int(job["year"])
    month = int(job["month"])
    area = job["user_area"]
    sub_area = str(job["sub_area"] or "").strip()

    # Only mark rows whose ReportStatus is still blank
    pending_rows = _get_report_scope_sheet_rows(year, month, area, sub_area, status_mode="blank")
    if not pending_rows:
        return 0

    _sheet_batch_update_report_status_rows(pending_rows, print_label)

    try:
        _sync_report_cache_only()
    except Exception:
        pass

    return len(pending_rows)


def _get_scope_submitted_count(year: int, month: int, area_number: str, sub_area: str = "") -> int:
    db = get_db()
    params = [int(year), int(month), str(area_number or "").strip()]
    extra = ""
    if str(sub_area or "").strip():
        extra = " AND TRIM(COALESCE(sub_area, '')) = TRIM(?)"
        params.append(str(sub_area or "").strip())
    row = db.execute(
        f"""
        SELECT COUNT(DISTINCT TRIM(sex)) AS cnt
        FROM sheet_accounts_cache
        WHERE LOWER(TRIM(COALESCE(position, ''))) = 'pastor'
          AND TRIM(age) = TRIM(?)
          AND TRIM(sex) IN (
              SELECT DISTINCT TRIM(COALESCE(address, church))
              FROM sheet_report_cache
              WHERE year = ? AND month = ?
                AND TRIM(COALESCE(address, church)) != ''
          )
          {extra}
        """,
        tuple([params[2], params[0], params[1], *params[3:]])
    ).fetchone()
    return int((row["cnt"] or 0) if row else 0)


def _get_print_button_state(year: int, month: int, area_number: str, sub_area: str = "", total_churches: int = 0):
    """
    Decides enable/disable state of Main / Late / Refresh buttons
    using cached report statuses within the selected area/sub-area scope.
    """
    status_map = _get_report_status_map_for_scope(year, month, area_number, sub_area)

    statuses = [str(v or "").strip() for v in status_map.values()]
    has_main = any(s == "MainPrint" for s in statuses)
    has_late = any(s == "LatePrint" for s in statuses)
    has_received = any(s == "Received" for s in statuses)
    has_blank = any(not s for s in statuses)
    has_any_submitted = len(status_map) > 0
    unresolved_no_report = max(int(total_churches or 0) - len(status_map), 0) > 0

    main_enabled = (not has_received) and has_any_submitted and (not has_main)
    late_enabled = (not has_received) and has_main and (has_blank or unresolved_no_report)
    refresh_enabled = (not has_received) and (has_main or has_late)

    return {
        "main_enabled": main_enabled,
        "late_enabled": late_enabled,
        "refresh_enabled": refresh_enabled,
        "has_received": has_received,
    }


def _export_gsheet_worksheet_pdf(spreadsheet_id: str, worksheet_gid: str):
    creds = Credentials.from_service_account_file(
        GOOGLE_SHEETS_CREDENTIALS_FILE,
        scopes=GOOGLE_SHEETS_SCOPES,
    )
    creds.refresh(GoogleAuthRequest())

    export_url = (
        f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export"
        f"?format=pdf&gid={worksheet_gid}"
        f"&size=legal"
        f"&portrait=false"
        f"&fitw=true"
        f"&sheetnames=false"
        f"&printtitle=false"
        f"&pagenumbers=false"
        f"&gridlines=false"
        f"&fzr=false"
        f"&top_margin=0.30"
        f"&bottom_margin=0.30"
        f"&left_margin=0.30"
        f"&right_margin=0.30"
    )

    resp = requests.get(
        export_url,
        headers={"Authorization": f"Bearer {creds.token}"},
        timeout=60,
    )
    resp.raise_for_status()
    return resp.content











def _create_print_report_job(user_area: str, year: int, month: int, report_type: str = "ao", sub_area: str = "", print_action: str = "main"):
    job_id = str(uuid.uuid4())
    get_db().execute(
        """
        INSERT INTO print_report_jobs (id, user_area, sub_area, report_type, print_action, year, month, status, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, 'queued', ?)
        """,
        (job_id, str(user_area or "").strip(), str(sub_area or "").strip(), str(report_type or "ao").strip(), str(print_action or "main").strip(), int(year), int(month), utc_now_iso()),
    )
    get_db().commit()
    return job_id


def _get_print_report_job(job_id: str):
    return get_db().execute(
        "SELECT * FROM print_report_jobs WHERE id = ?",
        (job_id,),
    ).fetchone()


def _count_jobs_ahead(job_row):
    if not job_row:
        return 0
    queued_before = get_db().execute(
        """
        SELECT COUNT(*) AS cnt
        FROM print_report_jobs
        WHERE status = 'queued' AND datetime(created_at) < datetime(?)
        """,
        (job_row["created_at"],),
    ).fetchone()
    processing = get_db().execute(
        "SELECT COUNT(*) AS cnt FROM print_report_jobs WHERE status = 'processing'"
    ).fetchone()
    return int((queued_before["cnt"] or 0) + (processing["cnt"] or 0))


def _claim_next_print_report_job():
    db = get_db()
    cur = db.cursor()
    cur.execute("BEGIN IMMEDIATE")
    processing = cur.execute(
        "SELECT id FROM print_report_jobs WHERE status = 'processing' LIMIT 1"
    ).fetchone()
    if processing:
        db.commit()
        return None

    next_job = cur.execute(
        """
        SELECT * FROM print_report_jobs
        WHERE status = 'queued'
        ORDER BY datetime(created_at) ASC, id ASC
        LIMIT 1
        """
    ).fetchone()
    if not next_job:
        db.commit()
        return None

    cur.execute(
        """
        UPDATE print_report_jobs
        SET status = 'processing', started_at = ?, error_message = NULL
        WHERE id = ? AND status = 'queued'
        """,
        (utc_now_iso(), next_job["id"]),
    )
    db.commit()
    return next_job["id"]


def _process_print_report_job(job_id: str):
    job = _get_print_report_job(job_id)
    if not job:
        return

    try:
        report_type = str(job["report_type"] or "ao").strip()
        sub_area = str(job["sub_area"] or "").strip()
        print_action = str(job["print_action"] or "main").strip().lower()

        if print_action == "refresh":
            _reset_report_status_for_scope(int(job["year"]), int(job["month"]), job["user_area"], sub_area)
            try:
                _sync_report_cache_only()
            except Exception:
                pass
            get_db().execute(
                """
                UPDATE print_report_jobs
                SET status = 'done', finished_at = ?, result_path = '', error_message = NULL
                WHERE id = ?
                """,
                (utc_now_iso(), job_id),
            )
            get_db().commit()
            return

        # Keep existing Apps Script flow for main print. For late print, prepare the copied late tab directly.
        if print_action == "main":
            _prepare_report_print_via_apps_script(
                job["user_area"],
                int(job["year"]),
                int(job["month"]),
                report_type=report_type,
                sub_area=sub_area,
            )
        else:
            _prepare_report_print_sheet_direct(
                job["user_area"],
                int(job["year"]),
                int(job["month"]),
                report_type=report_type,
                sub_area=sub_area,
                print_action=print_action,
            )

        _, sh, ws = _get_report_print_sheet(report_type=report_type, print_action=print_action)
        pdf_bytes = _export_gsheet_worksheet_pdf(sh.id, str(ws.id))

        out_dir = os.path.join(os.path.dirname(__file__), "generated_reports")
        os.makedirs(out_dir, exist_ok=True)
        action_prefix = "Late" if print_action == "late" else "Main"
        prefix = "SubAreaPrint" if str(job["report_type"] or "").strip() == "sub_area" else "AO_Report_Print"
        extra = f"_{str(job['sub_area'] or '').strip()}" if str(job["report_type"] or "").strip() == "sub_area" and str(job['sub_area'] or '').strip() else ""
        filename = f"{action_prefix}_{prefix}_Area_{job['user_area']}{extra}_{calendar.month_name[int(job['month'])]}_{int(job['year'])}_{job_id}.pdf".replace(" ", "_")
        result_path = os.path.join(out_dir, filename)

        with open(result_path, "wb") as f:
            f.write(pdf_bytes)

        get_db().execute(
            """
            UPDATE print_report_jobs
            SET status = 'done', finished_at = ?, result_path = ?, error_message = NULL
            WHERE id = ?
            """,
            (utc_now_iso(), result_path, job_id),
        )
        get_db().commit()

    except Exception as e:
        get_db().execute(
            """
            UPDATE print_report_jobs
            SET status = 'failed', finished_at = ?, error_message = ?
            WHERE id = ?
            """,
            (utc_now_iso(), str(e), job_id),
        )
        get_db().commit()
        print("❌ Error processing print report job:", e)
        
def _advance_print_report_queue():
    next_job_id = _claim_next_print_report_job()
    if next_job_id:
        _process_print_report_job(next_job_id)



def refresh_pastor_from_cache():
    username = session.get("pastor_username")
    if not username:
        return False

    row = get_db().execute(
        "SELECT name, church_address, sex FROM sheet_accounts_cache WHERE username = ?",
        (username,),
    ).fetchone()
    if not row:
        return False

    session["pastor_name"] = row["name"] or ""
    session["pastor_church_address"] = row["church_address"] or ""
    session["pastor_church_id"] = row["sex"] or ""
    return True


def _current_user_key():
    """
    We store 'Submitted By' as username (preferred) so filtering is stable.
    """
    if session.get("pastor_logged_in"):
        return (session.get("pastor_username") or "").strip() or "pastor"
    if session.get("ao_logged_in"):
        return (session.get("ao_username") or "").strip() or "ao"
    return ""


def _current_user_display():
    if session.get("pastor_logged_in"):
        return (session.get("pastor_name") or "").strip() or (session.get("pastor_username") or "Pastor")
    if session.get("ao_logged_in"):
        return (session.get("ao_username") or "").strip() or "AO"
    return "Unknown"


def _current_user_church_name():
    if session.get("pastor_logged_in"):
        refresh_pastor_from_cache()
        church_id = (session.get("pastor_church_id") or "").strip()
        if church_id:
            return church_id
        return (session.get("pastor_church_address") or "").strip()
    if session.get("ao_logged_in"):
        return (session.get("ao_church_id") or "").strip()
    return ""


# ========================
# ✅ IMPORTANT FIX:
# Cache → Local upsert for Pastor Tool display
# ========================
def _dirty_key(year: int, month: int) -> str:
    return f"dirty_{year}_{month}"

def mark_month_dirty(year: int, month: int):
    session[_dirty_key(year, month)] = True

def clear_month_dirty(year: int, month: int):
    session.pop(_dirty_key(year, month), None)

def is_month_dirty(year: int, month: int) -> bool:
    return session.get(_dirty_key(year, month)) is True

def sync_local_month_from_cache_for_pastor(year: int, month: int):
    # ✅ If user edited local data, DO NOT overwrite it with cache/sheets data
    if is_month_dirty(year, month):
        return

    refresh_pastor_from_cache()

    pastor_username = (session.get("pastor_username") or "").strip()
    if not pastor_username:
        return

    pastor_name = (session.get("pastor_name") or "").strip()
    church_address = (session.get("pastor_church_address") or "").strip()
    church_id = (session.get("pastor_church_id") or "").strip()
    church_key = church_id or church_address
    if not pastor_name and not church_address:
        return

    db = get_db()
    cur = db.cursor()

    cached_rows = db.execute(
        """
        SELECT *
        FROM sheet_report_cache
        WHERE year = ? AND month = ?
          AND (
            TRIM(address) = TRIM(?) OR TRIM(church) = TRIM(?)
            OR TRIM(pastor) = TRIM(?)
          )
        ORDER BY activity_date
        """,
        (year, month, church_key, church_key, pastor_name),
    ).fetchall()

    if not cached_rows:
        return

    mr = get_or_create_monthly_report(year, month, pastor_username)
    mrid = mr["id"]

    ensure_sunday_reports(mrid, year, month)

    statuses = set()
    cp_seed = None

    for r in cached_rows:
        activity_date = str(r["activity_date"] or "").strip()
        if not activity_date:
            continue
        d = parse_sheet_date(activity_date)
        if not d:
            continue

        statuses.add(str(r["status"] or "").strip())

        if cp_seed is None:
            cp_seed = r

        srow = db.execute(
            """
            SELECT id FROM sunday_reports
            WHERE monthly_report_id = ? AND date = ?
            """,
            (mrid, d.isoformat()),
        ).fetchone()

        adult = float(r["adult"] or 0)
        youth = float(r["youth"] or 0)
        children = float(r["children"] or 0)

        tithes_church = float(r["tithes"] or 0)
        offering = float(r["offering"] or 0)
        mission = float(r["mission_offering"] or 0)
        personal = float(r["personal_tithes"] or 0)

        if not srow:
            cur.execute(
                """
                INSERT INTO sunday_reports
                (monthly_report_id, date, is_complete,
                 attendance_adult, attendance_youth, attendance_children,
                 tithes_church, offering, mission, tithes_personal)
                VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    mrid,
                    d.isoformat(),
                    adult,
                    youth,
                    children,
                    tithes_church,
                    offering,
                    mission,
                    personal,
                ),
            )
        else:
            cur.execute(
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
                    personal,
                    srow["id"],
                ),
            )

    cp = ensure_church_progress(mrid)
    if cp_seed is not None:
        cur.execute(
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
                int(float(cp_seed["new_bible_study"] or 0)),
                int(float(cp_seed["existing_bible_study"] or 0)),
                int(float(cp_seed["received_jesus"] or 0)),
                int(float(cp_seed["water_baptized"] or 0)),
                int(float(cp_seed["holy_spirit_baptized"] or 0)),
                int(float(cp_seed["healed"] or 0)),
                int(float(cp_seed["childrens_dedication"] or 0)),
                cp["id"],
            ),
        )

    submitted = 1 if any(s.strip() for s in statuses) else 1
    approved = 0
    for s in statuses:
        if "approved" in str(s).lower():
            approved = 1
            break

    cur.execute(
        """
        UPDATE monthly_reports
        SET submitted = ?, approved = ?
        WHERE id = ?
        """,
        (submitted, approved, mrid),
    )

    db.commit()


# ========================
# AO cache helpers
# ========================


def get_all_churches_from_cache():
    db = get_db()
    ao_area = (session.get("ao_area_number") or "").strip() if ao_logged_in() else ""
    ao_sub_area = (session.get("ao_sub_area") or "").strip() if ao_is_sub_area_overseer() else ""

    if ao_area:
        params = [ao_area]
        extra = ""
        if ao_sub_area:
            extra = " AND TRIM(COALESCE(sub_area, '')) = TRIM(?)"
            params.append(ao_sub_area)
        rows = db.execute(
            f"""
            SELECT DISTINCT TRIM(sex) AS c
            FROM sheet_accounts_cache
            WHERE TRIM(sex) != ''
              AND TRIM(age) = TRIM(?)
              AND LOWER(TRIM(COALESCE(position, ''))) = 'pastor'
              {extra}
            ORDER BY c
            """,
            tuple(params),
        ).fetchall()
    else:
        rows = db.execute(
            """
            SELECT DISTINCT TRIM(sex) AS c
            FROM sheet_accounts_cache
            WHERE TRIM(sex) != ''
              AND LOWER(TRIM(COALESCE(position, ''))) = 'pastor'
            ORDER BY c
            """
        ).fetchall()

    return [r["c"] for r in rows]

def get_accounts_for_area_cache(area_number: str):
    db = get_db()
    ao_sub_area = (session.get("ao_sub_area") or "").strip() if ao_is_sub_area_overseer() else ""
    params = [area_number]
    extra = ""
    if ao_sub_area:
        extra = " AND TRIM(COALESCE(sub_area, '')) = TRIM(?)"
        params.append(ao_sub_area)
    rows = db.execute(
        f"""
        SELECT
            TRIM(username) AS username,
            TRIM(name) AS name,
            TRIM(age) AS area_number,
            TRIM(sex) AS church_id,
            TRIM(church_address) AS church_address,
            TRIM(contact) AS contact,
            TRIM(birthday) AS birthday,
            TRIM(password) AS password,
            TRIM(position) AS position,
            TRIM(COALESCE(sub_area, '')) AS sub_area,
            sheet_row
        FROM sheet_accounts_cache
        WHERE TRIM(age) = TRIM(?)
          AND TRIM(username) != ''
          AND LOWER(TRIM(COALESCE(position, ''))) = 'pastor'
          {extra}
        ORDER BY church_id, church_address, name
        """,
        tuple(params),
    ).fetchall()
    return rows

def get_area_summary_for_month_cache(year: int, month: int, church_items, aopt_amount: float, is_sub_ao: bool = False):
    base_aopt_amount = float(aopt_amount or 0.0)

    summary = {
        "submitted_count": 0,
        "total_churches": len(church_items),
        "attendance_adult": 0.0,
        "attendance_youth": 0.0,
        "attendance_children": 0.0,
        "attendance_general": 0.0,
        "church_tithes": 0.0,
        "offering": 0.0,
        "mission_total": 0.0,
        "no_mission": 0.0,
        "ao_mission": 0.0,
        "pastor_personal_tithes": 0.0,
        "ao_personal_tithes": base_aopt_amount,
        "total_all_money_received": 0.0,
        "bible_old": 0.0,
        "bible_new": 0.0,
        "bible_total": 0.0,
        "received_holy_spirit": 0.0,
        "received_jesus": 0.0,
        "baptized_water": 0.0,
        "healed": 0.0,
        "children_dedicated": 0.0,
        "total_money_no": 0.0,
        "total_money_ao": 0.0,
    }

    for item in church_items:
        if item.get("rows", 0) > 0:
            summary["submitted_count"] += 1

        avg = item.get("avg") or {}
        totals = item.get("totals") or {}

        adult = float(avg.get("adult") or 0)
        youth = float(avg.get("youth") or 0)
        children = float(avg.get("children") or 0)

        bible_old = float(avg.get("existing_bible_study") or 0)
        bible_new = float(avg.get("new_bible_study") or 0)
        received_jesus = float(avg.get("received_jesus") or 0)
        baptized_water = float(avg.get("water_baptized") or 0)
        received_holy_spirit = float(avg.get("holy_spirit_baptized") or 0)
        children_dedicated = float(avg.get("childrens_dedication") or 0)
        healed = float(avg.get("healed") or 0)

        summary["attendance_adult"] += adult
        summary["attendance_youth"] += youth
        summary["attendance_children"] += children
        summary["attendance_general"] += adult + youth + children

        summary["church_tithes"] += float(totals.get("tithes") or 0)
        summary["offering"] += float(totals.get("offering") or 0)
        summary["mission_total"] += float(totals.get("mission_offering") or 0)
        summary["pastor_personal_tithes"] += float(totals.get("personal_tithes") or 0)
        summary["total_all_money_received"] += float(totals.get("amount_to_send") or 0)

        summary["bible_old"] += bible_old
        summary["bible_new"] += bible_new
        summary["received_jesus"] += received_jesus
        summary["baptized_water"] += baptized_water
        summary["received_holy_spirit"] += received_holy_spirit
        summary["children_dedicated"] += children_dedicated
        summary["healed"] += healed

    summary["bible_total"] = summary["bible_old"] + summary["bible_new"]
    summary["no_mission"] = summary["mission_total"] * 0.20
    summary["ao_mission"] = summary["mission_total"] * 0.80
    if is_sub_ao:
        summary["total_money_no"] = (
            summary["church_tithes"]
            + summary["offering"]
            + summary["mission_total"]
            + (summary["pastor_personal_tithes"] * 0.20)
            + summary["ao_personal_tithes"]
        )
        summary["total_money_ao"] = summary["pastor_personal_tithes"] * 0.80
    else:
        summary["total_money_no"] = (
            summary["church_tithes"]
            + summary["offering"]
            + summary["no_mission"]
            + (summary["pastor_personal_tithes"] * 0.10)
            + summary["ao_personal_tithes"]
            - 3000
        )
        summary["total_money_ao"] = (
            (summary["pastor_personal_tithes"] * 0.90)
            + summary["ao_mission"]
        )
    return summary

def get_report_stats_for_month_and_church_cache(year: int, month: int, church_key: str):
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

    db = get_db()

    rows = db.execute(
        """
        SELECT *
        FROM sheet_report_cache
        WHERE year = ? AND month = ?
          AND (
            TRIM(address) = TRIM(?) OR TRIM(church) = TRIM(?)
          )
        """,
        (year, month, church_key, church_key),
    ).fetchall()

    if not rows:
        return stats

    stats["rows"] = len(rows)

    sum_fields = {k: 0.0 for k in stats["avg"].keys()}
    totals = {k: 0.0 for k in stats["totals"].keys()}
    statuses = set()

    for r in rows:
        sum_fields["adult"] += float(r["adult"] or 0)
        sum_fields["youth"] += float(r["youth"] or 0)
        sum_fields["children"] += float(r["children"] or 0)

        sum_fields["received_jesus"] += float(r["received_jesus"] or 0)
        sum_fields["existing_bible_study"] += float(r["existing_bible_study"] or 0)
        sum_fields["new_bible_study"] += float(r["new_bible_study"] or 0)
        sum_fields["water_baptized"] += float(r["water_baptized"] or 0)
        sum_fields["holy_spirit_baptized"] += float(r["holy_spirit_baptized"] or 0)
        sum_fields["childrens_dedication"] += float(r["childrens_dedication"] or 0)
        sum_fields["healed"] += float(r["healed"] or 0)

        totals["tithes"] += float(r["tithes"] or 0)
        totals["offering"] += float(r["offering"] or 0)
        totals["personal_tithes"] += float(r["personal_tithes"] or 0)
        totals["mission_offering"] += float(r["mission_offering"] or 0)
        totals["amount_to_send"] += float(r["amount_to_send"] or 0)

        s = str(r["status"] or "").strip()
        if s:
            statuses.add(s)

    for k in stats["avg"].keys():
        stats["avg"][k] = sum_fields[k] / stats["rows"]

    stats["totals"] = totals

    if len(statuses) == 1:
        stats["sheet_status"] = list(statuses)[0]
    elif len(statuses) > 1:
        stats["sheet_status"] = "Mixed"
    else:
        stats["sheet_status"] = ""

    return stats


def cache_update_status_for_church_month(year: int, month: int, church_key: str, status_label: str):
    db = get_db()
    db.execute(
        """
        UPDATE sheet_report_cache
        SET status = ?
        WHERE year = ? AND month = ?
          AND (TRIM(address) = TRIM(?) OR TRIM(church) = TRIM(?))
        """,
        (status_label, year, month, church_key, church_key),
    )
    db.commit()


def sheet_batch_update_status_for_church_month(year: int, month: int, church_key: str, status_label: str):
    db = get_db()
    rows = db.execute(
        """
        SELECT sheet_row
        FROM sheet_report_cache
        WHERE year = ? AND month = ?
          AND (TRIM(address) = TRIM(?) OR TRIM(church) = TRIM(?))
        """,
        (year, month, church_key, church_key),
    ).fetchall()

    sheet_rows = [int(r["sheet_row"]) for r in rows if r["sheet_row"]]
    if not sheet_rows:
        return

    client = get_gs_client()
    sh = client.open("District4 Data")
    ws = sh.worksheet("Report")

    values = ws.get_all_values()
    if not values:
        return
    headers = values[0]
    idx_status = _find_col(headers, "status")
    if idx_status is None:
        print("❌ Report sheet missing status header")
        return

    requests_body = []
    col_letter = chr(ord("A") + idx_status)
    for r in sheet_rows:
        a1 = f"{col_letter}{r}"
        requests_body.append({"range": a1, "values": [[status_label]]})

    ws.batch_update(requests_body)


# ========================
# Append / Export to sheet
# ========================

def _delete_report_rows_for_month_in_sheet(year: int, month: int, church_key: str, pastor_name: str):
    """
    Deletes existing rows in Google Sheets 'Report' that match:
      - same year/month (based on activity_date)
      - same church (matches either 'church' or 'address' column)
      - same pastor name (extra safety)
    Uses local cache sheet_report_cache to find exact sheet_row numbers.
    """
    db = get_db()

    rows = db.execute(
        """
        SELECT sheet_row
        FROM sheet_report_cache
        WHERE year = ? AND month = ?
          AND (
                TRIM(church) = TRIM(?)
             OR TRIM(address) = TRIM(?)
          )
          AND TRIM(pastor) = TRIM(?)
          AND sheet_row IS NOT NULL
        """,
        (year, month, church_key, church_key, pastor_name),
    ).fetchall()

    sheet_rows = sorted({int(r["sheet_row"]) for r in rows if r["sheet_row"]}, reverse=True)
    if not sheet_rows:
        return

    client = get_gs_client()
    sh = client.open("District4 Data")
    ws = sh.worksheet("Report")

    # Delete from bottom to top so row numbers stay correct
    for r in sheet_rows:
        if r > 1:  # never delete header row
            ws.delete_rows(r)


def _ensure_accounts_headers(ws):
    values = ws.get_all_values()
    headers = ["Name", "Area Number", "Church ID", "Church Address", "Contact #", "Birth Day", "UserName", "Password", "Position", "Sub Area", "GooglePinLocation"]
    if not values:
        ws.append_row(headers, value_input_option="USER_ENTERED")
        return headers
    current = list(values[0])
    changed = False
    for i, name in enumerate(headers):
        if _find_col(current, name) is None:
            while len(current) <= i:
                current.append("")
            current[i] = name
            changed = True
    if changed:
        rng = f"A1:{chr(ord('A') + len(current) - 1)}1"
        ws.update(rng, [current], value_input_option="USER_ENTERED")
        values = ws.get_all_values()
        return values[0]
    return current


def _build_account_row_from_headers(headers, payload: dict):
    row = [""] * len(headers)
    mapping = {
        "Name": payload.get("full_name", ""),
        "Area Number": payload.get("age", ""),
        "Age": payload.get("age", ""),
        "Church ID": payload.get("sex", ""),
        "Sex": payload.get("sex", ""),
        "Church Address": payload.get("church_address", ""),
        "Contact #": payload.get("contact_number", ""),
        "Birth Day": payload.get("birthday", ""),
        "UserName": payload.get("username", ""),
        "Password": payload.get("password", ""),
        "Position": payload.get("position", "Pastor"),
        "Sub Area": payload.get("sub_area", ""),
        "SubArea": payload.get("sub_area", ""),
        "GooglePinLocation": payload.get("google_pin_location", ""),
    }
    for i, h in enumerate(headers):
        if h in mapping:
            row[i] = mapping[h]
    return row

def append_account_to_sheet(pastor_data: dict):
    client = get_gs_client()
    sh = client.open("District4 Data")

    try:
        worksheet = sh.worksheet("Accounts")
    except gspread.WorksheetNotFound:
        worksheet = sh.add_worksheet(title="Accounts", rows=100, cols=12)

    # Ensure headers exist and start at Column A
    headers = _ensure_accounts_headers(worksheet)

    # Build row aligned with headers
    row = _build_account_row_from_headers(headers, pastor_data)

    # ✅ FORCE WRITE FROM COLUMN A (FIX)
    worksheet.append_rows(
        [row],
        value_input_option="USER_ENTERED",
        table_range="A1"   # 🔥 THIS IS THE FIX
    )


def _ensure_report_sheet_headers(ws):
    """
    Ensures the Report sheet has a header row.
    If headers already exist, it does nothing.
    """
    values = ws.get_all_values()
    if values:
        return values

    headers = [
        "church",
        "pastor",
        "address",
        "adult",
        "youth",
        "children",
        "tithes",
        "offering",
        "personal tithes",
        "mission offering",
        "received jesus",
        "existing bible study",
        "new bible study",
        "water baptized",
        "holy spirit baptized",
        "childrens dedication",
        "healed",
        "activity_date",
        "amount to send",
        "status",
    ]
    ws.append_row(headers)
    return ws.get_all_values()


def append_report_to_sheet(report_data: dict):
    client = get_gs_client()
    sh = client.open("District4 Data")

    try:
        ws = sh.worksheet("Report")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title="Report", rows=1000, cols=25)

    _ensure_report_sheet_headers(ws)

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

    # ✅ Force writing starting at column A by using a fixed range "A:..."
    ws.append_rows([row], value_input_option="USER_ENTERED", table_range="A1")



def export_month_to_sheet(year: int, month: int, status_label: str):
    db = get_db()
    cursor = db.cursor()

    pastor_username = (session.get("pastor_username") or "").strip()
    if not pastor_username:
        return

    cursor.execute(
        "SELECT * FROM monthly_reports WHERE year = ? AND month = ? AND pastor_username = ?",
        (year, month, pastor_username),
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

    sync_from_sheets_if_needed()
    refresh_pastor_from_cache()

    pastor_name = session.get("pastor_name", "")
    church_address = session.get("pastor_church_address", "")
    church_id = (session.get("pastor_church_id") or "").strip()

    bible_new = cp_row["bible_new"] or 0
    bible_existing = cp_row["bible_existing"] or 0
    received_christ = cp_row["received_christ"] or 0
    baptized_water = cp_row["baptized_water"] or 0
    baptized_holy_spirit = cp_row["baptized_holy_spirit"] or 0
    healed = cp_row["healed"] or 0
    child_dedication = cp_row["child_dedication"] or 0

        # ✅ RESUBMIT BEHAVIOR:
    # If this month already exists on Sheets for this pastor/church, delete it first then re-upload.
    # (This prevents duplicates on resubmit.)
    church_key = church_id or church_address
    _delete_report_rows_for_month_in_sheet(year, month, church_key, pastor_name)

    for row in sunday_rows:
        d = datetime.fromisoformat(row["date"]).date()

        # ✅ IMPORTANT: keep date like 12/21/2025 (NOT =DATE(...))
        activity_date = f"{d.month}/{d.day}/{d.year}"


        tithes_church = row["tithes_church"] or 0
        offering = row["offering"] or 0
        mission = row["mission"] or 0
        tithes_personal = row["tithes_personal"] or 0
        amount_to_send = tithes_church + offering + mission + tithes_personal

        report_data = {
            "church": church_id or church_address,
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
            print("❌ Pastor export failed:", repr(e))
            traceback.print_exc()


# ========================
# Prayer Request → Sheets helpers
# ========================

PRAYER_SHEET_NAME = "PrayerRequest"


def _ensure_prayer_sheet_headers(ws):
    """
    Ensures header row exists (doesn't overwrite if already there).
    """
    values = ws.get_all_values()
    if values:
        return values

    headers = [
        "Church Name",
        "Submitted By",
        "Request ID",
        "Prayer Request Title",
        "Prayer Request Date",
        "Prayer Request",
        "Status",
        "Pastor's Praying",
        "Answered Date",
    ]
    ws.append_row(headers)
    return ws.get_all_values()


def _append_prayer_request_to_sheet(church_name, submitted_by, request_id, title, request_date, request_text):
    client = get_gs_client()
    sh = client.open("District4 Data")
    try:
        ws = sh.worksheet(PRAYER_SHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=PRAYER_SHEET_NAME, rows=1000, cols=12)

    _ensure_prayer_sheet_headers(ws)
    ws.append_row(
        [
            church_name,
            submitted_by,
            request_id,
            title,
            request_date,
            request_text,
            "Pending",
            "",   # Pastor's Praying
            "",   # Answered Date
        ]
    )


def _update_prayer_request_cells_in_sheet(request_id, updates: dict):
    """
    updates keys among: church_name, submitted_by, title, request_date, request_text,
    status, pastors_praying, answered_date
    """
    db = get_db()
    cached = db.execute(
        "SELECT sheet_row FROM sheet_prayer_request_cache WHERE request_id = ?",
        (request_id,),
    ).fetchone()
    if not cached or not cached["sheet_row"]:
        return False

    sheet_row = int(cached["sheet_row"])

    client = get_gs_client()
    sh = client.open("District4 Data")
    ws = sh.worksheet(PRAYER_SHEET_NAME)

    values = ws.get_all_values()
    if not values:
        return False
    headers = values[0]

    col_map = {
        "church_name": "Church Name",
        "submitted_by": "Submitted By",
        "request_id": "Request ID",
        "title": "Prayer Request Title",
        "request_date": "Prayer Request Date",
        "request_text": "Prayer Request",
        "status": "Status",
        "pastors_praying": "Pastor's Praying",
        "answered_date": "Answered Date",
    }

    body = []
    for k, v in updates.items():
        header = col_map.get(k)
        if not header:
            continue
        idx = _find_col(headers, header)
        if idx is None:
            continue
        col_letter = chr(ord("A") + idx)
        body.append({"range": f"{col_letter}{sheet_row}", "values": [[v]]})

    if not body:
        return False

    ws.batch_update(body)
    return True


def _delete_prayer_request_row_in_sheet(request_id):
    db = get_db()
    cached = db.execute(
        "SELECT sheet_row FROM sheet_prayer_request_cache WHERE request_id = ?",
        (request_id,),
    ).fetchone()
    if not cached or not cached["sheet_row"]:
        return False

    sheet_row = int(cached["sheet_row"])
    if sheet_row <= 1:
        return False

    client = get_gs_client()
    sh = client.open("District4 Data")
    ws = sh.worksheet(PRAYER_SHEET_NAME)

    ws.delete_rows(sheet_row)
    return True


# ========================
# Prayer Request cache queries
# ========================

def get_prayer_requests_for_user(submitted_by: str, include_answered=False):
    db = get_db()
    if include_answered:
        rows = db.execute(
            """
            SELECT *
            FROM sheet_prayer_request_cache
            WHERE TRIM(submitted_by) = TRIM(?)
            ORDER BY request_date DESC, sheet_row DESC
            """,
            (submitted_by,),
        ).fetchall()
    else:
        rows = db.execute(
            """
            SELECT *
            FROM sheet_prayer_request_cache
            WHERE TRIM(submitted_by) = TRIM(?)
              AND (status IS NULL OR TRIM(status) != 'Answered')
            ORDER BY request_date DESC, sheet_row DESC
            """,
            (submitted_by,),
        ).fetchall()
    return rows


def get_answered_prayer_requests_for_user(submitted_by: str):
    db = get_db()
    return db.execute(
        """
        SELECT *
        FROM sheet_prayer_request_cache
        WHERE TRIM(submitted_by) = TRIM(?)
          AND TRIM(status) = 'Answered'
        ORDER BY answered_date DESC, request_date DESC, sheet_row DESC
        """,
        (submitted_by,),
    ).fetchall()


def get_pending_prayers_for_ao():
    db = get_db()
    rows = db.execute(
        """
        SELECT *
        FROM sheet_prayer_request_cache
        WHERE TRIM(status) = 'Pending'
        ORDER BY request_date DESC, sheet_row DESC
        """
    ).fetchall()
    if not ao_logged_in():
        return rows
    area = str(session.get("ao_area_number") or "").strip()
    filtered = []
    for r in rows:
        church_name = str(r["church_name"] or "").strip()
        row = db.execute(
            "SELECT age FROM sheet_accounts_cache WHERE TRIM(sex)=TRIM(?) OR TRIM(church_address)=TRIM(?) LIMIT 1",
            (church_name, church_name),
        ).fetchone()
        if row and str(row["age"] or "").strip() == area:
            filtered.append(r)
    return filtered


# ========================
# Bible verse of the day
# ========================

VERSE_REFERENCES = [
    # Holy Living (1–52)
    "Leviticus 20:7",
    "Psalm 24:3-4",
    "Psalm 34:14",
    "Psalm 51:10",
    "Psalm 119:9",
    "Psalm 119:105",
    "Proverbs 4:23",
    "Proverbs 16:3",
    "Isaiah 1:16",
    "Isaiah 35:8",
    "Matthew 5:8",
    "Matthew 5:16",
    "Matthew 6:33",
    "John 15:4",
    "Romans 6:12",
    "Romans 12:1",
    "Romans 12:2",
    "1 Corinthians 6:19-20",
    "2 Corinthians 7:1",
    "Galatians 2:20",
    "Galatians 5:16",
    "Galatians 5:22-23",
    "Ephesians 4:22-24",
    "Ephesians 5:8",
    "Philippians 1:27",
    "Colossians 1:10",
    "Colossians 3:2",
    "Colossians 3:5",
    "1 Thessalonians 4:3",
    "1 Thessalonians 5:22",
    "2 Timothy 2:19",
    "Titus 2:11-12",
    "Hebrews 12:14",
    "James 1:27",
    "James 4:8",
    "1 Peter 1:15-16",
    "1 Peter 2:12",
    "1 John 1:7",
    "1 John 2:6",
    "1 John 3:3",
    "Jude 1:21",
    "Psalm 15:1-2",
    "Proverbs 21:21",
    "Isaiah 52:11",
    "Micah 6:8",
    "Matthew 7:20",
    "Luke 1:75",
    "John 17:17",
    "Romans 13:14",
    "2 Corinthians 5:17",
    "Ephesians 2:10",
    "Hebrews 13:21",

    # Pastoral Leadership (53–104)
    "Numbers 27:17",
    "Deuteronomy 31:6",
    "Joshua 1:9",
    "1 Samuel 12:24",
    "2 Samuel 23:3",
    "1 Kings 3:9",
    "Psalm 78:72",
    "Proverbs 11:14",
    "Proverbs 27:23",
    "Isaiah 40:11",
    "Jeremiah 3:15",
    "Jeremiah 23:4",
    "Ezekiel 34:2",
    "Micah 5:4",
    "Matthew 9:36",
    "Matthew 20:26-28",
    "John 10:11",
    "John 13:14-15",
    "Acts 6:4",
    "Acts 20:28",
    "Romans 12:8",
    "1 Corinthians 4:2",
    "1 Corinthians 9:19",
    "2 Corinthians 4:5",
    "Galatians 6:1",
    "Ephesians 4:11-12",
    "Philippians 2:3-4",
    "Colossians 4:17",
    "1 Thessalonians 2:7-8",
    "2 Thessalonians 3:9",
    "1 Timothy 1:12",
    "1 Timothy 3:1-7",
    "1 Timothy 4:12",
    "1 Timothy 4:16",
    "2 Timothy 1:7",
    "2 Timothy 2:2",
    "2 Timothy 2:15",
    "2 Timothy 4:2",
    "Titus 1:7-9",
    "Titus 2:7",
    "Hebrews 13:7",
    "Hebrews 13:17",
    "James 3:1",
    "1 Peter 5:2-3",
    "1 Peter 5:4",
    "Revelation 2:10",
    "Psalm 23:1",
    "Isaiah 6:8",
    "Matthew 28:20",
    "Luke 12:42",
    "John 21:15",
    "Acts 13:2",

    # Soul Winning (105–156)
    "Genesis 12:3",
    "Psalm 67:2",
    "Psalm 96:3",
    "Proverbs 11:30",
    "Isaiah 52:7",
    "Isaiah 61:1",
    "Daniel 12:3",
    "Matthew 4:19",
    "Matthew 9:37-38",
    "Matthew 10:7",
    "Matthew 28:19-20",
    "Mark 1:17",
    "Mark 16:15",
    "Luke 10:2",
    "Luke 19:10",
    "John 3:16",
    "John 4:35",
    "John 12:32",
    "Acts 1:8",
    "Acts 4:20",
    "Acts 8:4",
    "Acts 13:47",
    "Acts 20:24",
    "Romans 1:16",
    "Romans 10:14-15",
    "1 Corinthians 1:18",
    "1 Corinthians 9:22",
    "2 Corinthians 5:18-20",
    "Galatians 5:13",
    "Ephesians 6:15",
    "Philippians 1:5",
    "Colossians 1:28",
    "Colossians 4:3",
    "1 Thessalonians 1:8",
    "2 Timothy 4:5",
    "Titus 3:8",
    "Philemon 1:6",
    "Hebrews 2:3",
    "James 5:20",
    "1 Peter 3:15",
    "1 John 4:14",
    "Revelation 1:5",
    "Revelation 5:9",
    "Revelation 7:9",
    "Psalm 126:5-6",
    "Isaiah 49:6",
    "Matthew 5:14",
    "Luke 8:1",
    "John 15:8",
    "Acts 11:21",
    "Romans 15:20",

    # Faith & Endurance (157–208)
    "Genesis 15:6",
    "Exodus 14:14",
    "Deuteronomy 7:9",
    "Psalm 27:14",
    "Psalm 31:24",
    "Psalm 37:5",
    "Psalm 66:10",
    "Psalm 112:7",
    "Isaiah 40:31",
    "Isaiah 41:10",
    "Isaiah 43:2",
    "Lamentations 3:31-33",
    "Habakkuk 2:4",
    "Matthew 17:20",
    "Matthew 24:13",
    "Mark 9:23",
    "Luke 18:1",
    "John 16:33",
    "Acts 14:22",
    "Romans 5:3-5",
    "Romans 8:18",
    "Romans 8:25",
    "1 Corinthians 16:13",
    "2 Corinthians 4:16-18",
    "Galatians 6:9",
    "Ephesians 6:13",
    "Philippians 3:14",
    "Philippians 4:13",
    "Colossians 1:11",
    "1 Thessalonians 1:3",
    "2 Thessalonians 3:13",
    "2 Timothy 2:3",
    "Hebrews 6:12",
    "Hebrews 10:23",
    "Hebrews 11:1",
    "Hebrews 12:1",
    "James 1:3-4",
    "James 5:11",
    "1 Peter 1:7",
    "1 Peter 5:10",
    "1 John 5:4",
    "Revelation 3:11",
    "Revelation 14:12",
    "Psalm 118:6",
    "Proverbs 3:5-6",
    "Isaiah 26:3",
    "Matthew 7:7",
    "Luke 21:19",
    "Romans 15:13",
    "2 Corinthians 12:9",
    "Hebrews 3:14",

    # Prayer & Intimacy (209–260)
    "Genesis 18:19",
    "Exodus 33:11",
    "1 Samuel 1:27",
    "2 Chronicles 7:14",
    "Psalm 5:3",
    "Psalm 16:11",
    "Psalm 25:4",
    "Psalm 27:8",
    "Psalm 42:1",
    "Psalm 63:1",
    "Psalm 91:1",
    "Psalm 119:10",
    "Psalm 145:18",
    "Proverbs 15:8",
    "Isaiah 55:6",
    "Jeremiah 29:12",
    "Jeremiah 33:3",
    "Daniel 6:10",
    "Matthew 6:6",
    "Matthew 7:11",
    "Matthew 26:41",
    "Mark 1:35",
    "Luke 5:16",
    "Luke 11:9",
    "Luke 22:40",
    "John 14:13",
    "John 15:7",
    "John 17:3",
    "Acts 2:42",
    "Acts 12:5",
    "Romans 8:26",
    "Ephesians 1:17",
    "Ephesians 6:18",
    "Philippians 4:6",
    "Colossians 4:2",
    "1 Thessalonians 5:17",
    "2 Timothy 1:3",
    "Hebrews 4:16",
    "James 1:5",
    "James 5:16",
    "1 Peter 3:12",
    "1 John 5:14",
    "Revelation 8:4",
    "Psalm 34:17",
    "Psalm 66:18",
    "Isaiah 65:24",
    "Matthew 18:20",
    "Luke 10:21",
    "John 4:23",
    "Romans 12:12",
    "Jude 1:20",
    "Zephaniah 3:17",

    # God’s Provision (261–312)
    "Genesis 22:14",
    "Exodus 16:4",
    "Deuteronomy 8:3",
    "Psalm 23:1",
    "Psalm 34:10",
    "Psalm 37:25",
    "Psalm 84:11",
    "Psalm 104:27-28",
    "Psalm 127:1",
    "Proverbs 10:22",
    "Proverbs 22:9",
    "Isaiah 1:19",
    "Isaiah 58:11",
    "Matthew 6:25-26",
    "Matthew 6:31-33",
    "Matthew 7:11",
    "Luke 6:38",
    "John 6:35",
    "John 10:10",
    "Acts 4:34",
    "Romans 8:32",
    "2 Corinthians 9:8",
    "Galatians 6:7",
    "Philippians 4:19",
    "Colossians 2:10",
    "1 Timothy 6:6",
    "Hebrews 13:5",
    "James 1:17",
    "1 Peter 5:7",
    "Psalm 145:15",
    "Isaiah 41:17",
    "Jeremiah 31:14",
    "Ezekiel 34:26",
    "Haggai 2:8",
    "Matthew 14:19",
    "Luke 12:24",
    "John 21:6",
    "Acts 20:35",
    "Romans 11:36",
    "2 Corinthians 8:9",
    "Ephesians 3:20",
    "Philippians 1:6",
    "Psalm 68:19",
    "Psalm 132:15",
    "Proverbs 3:9-10",
    "Isaiah 30:23",
    "Malachi 3:10",
    "Matthew 19:29",
    "Luke 22:35",
    "John 1:16",
    "Hebrews 11:6",
    "Revelation 21:6",

    # Discouragement & Burnout Recovery (313–365)
    "Deuteronomy 33:27",
    "1 Kings 19:4-5",
    "Nehemiah 8:10",
    "Psalm 3:3",
    "Psalm 27:1",
    "Psalm 42:11",
    "Psalm 46:1",
    "Psalm 55:22",
    "Psalm 73:26",
    "Psalm 94:19",
    "Psalm 119:50",
    "Isaiah 12:2",
    "Isaiah 26:3",
    "Isaiah 40:29",
    "Isaiah 41:13",
    "Isaiah 43:1",
    "Isaiah 49:15",
    "Isaiah 54:10",
    "Lamentations 3:22-23",
    "Matthew 11:28",
    "Matthew 14:31",
    "Mark 4:39",
    "Luke 1:37",
    "Luke 12:32",
    "John 14:1",
    "John 14:27",
    "Acts 18:9-10",
    "Romans 8:1",
    "Romans 8:37",
    "2 Corinthians 1:3-4",
    "2 Corinthians 4:8-9",
    "2 Corinthians 7:6",
    "2 Corinthians 12:10",
    "Galatians 6:2",
    "Ephesians 3:16",
    "Philippians 1:6",
    "Philippians 4:7",
    "Colossians 3:15",
    "2 Thessalonians 2:16-17",
    "Hebrews 4:15",
    "Hebrews 12:11",
    "James 5:13",
    "1 Peter 5:7",
    "Revelation 2:3",
    "Revelation 3:8",
    "Revelation 21:4",
    "Psalm 121:1-2",
    "Psalm 138:3",
    "Isaiah 66:13",
    "Jeremiah 20:11",
    "John 16:33",
    "Romans 15:5"
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


# ========================
# Auth helpers
# ========================


def ao_logged_in():
    return session.get("ao_logged_in") is True


def pastor_logged_in():
    return session.get("pastor_logged_in") is True


def any_user_logged_in():
    return pastor_logged_in() or ao_logged_in()


def ao_is_sub_area_overseer():
    return ao_logged_in() and str(session.get("ao_role") or "").strip().lower() == "sub area overseer"


def current_ao_scope():
    return {
        "area": str(session.get("ao_area_number") or "").strip(),
        "sub_area": str(session.get("ao_sub_area") or "").strip(),
        "role": str(session.get("ao_role") or "").strip().lower(),
    }


def _account_row_in_current_ao_scope(row):
    if not row:
        return False
    scope = current_ao_scope()
    row_area = str(row["age"] or "").strip()
    row_sub = str(row["sub_area"] or "").strip() if "sub_area" in row.keys() else ""
    if row_area != scope["area"]:
        return False
    if ao_is_sub_area_overseer() and row_sub != scope["sub_area"]:
        return False
    return True


def _pastor_username_in_current_ao_scope(username: str):
    row = get_db().execute(
        "SELECT username, age, sub_area, position FROM sheet_accounts_cache WHERE TRIM(username)=TRIM(?)",
        ((username or "").strip(),),
    ).fetchone()
    if not row:
        return False
    if str(row["position"] or "").strip().lower() != "pastor":
        return False
    return _account_row_in_current_ao_scope(row)


def _church_in_current_ao_scope(church_key: str):
    row = get_db().execute(
        """
        SELECT age, sub_area
        FROM sheet_accounts_cache
        WHERE TRIM(sex)=TRIM(?) OR TRIM(church_address)=TRIM(?)
        LIMIT 1
        """,
        ((church_key or "").strip(), (church_key or "").strip()),
    ).fetchone()
    if not row:
        return False
    scope = current_ao_scope()
    if str(row["age"] or "").strip() != scope["area"]:
        return False
    if ao_is_sub_area_overseer() and str(row["sub_area"] or "").strip() != scope["sub_area"]:
        return False
    return True


def _prayer_in_current_ao_manage_scope(prayer_row):
    if not prayer_row:
        return False
    return _church_in_current_ao_scope(str(prayer_row["church_name"] or "").strip())


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


# ========================
# Flask app
# ========================

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "change-this-secret-key-123")

register_area_progress_monitor(app)
register_schedule_routes(app)
register_temp_edit_routes(app)

@app.template_filter("phpeso")
def phpeso_filter(value):
    return format_php_currency(value)


def _cleanup_old_visit_logs():
    cutoff = datetime.now(timezone.utc).timestamp() - (2 * 24 * 60 * 60)
    cutoff_iso = datetime.fromtimestamp(cutoff, timezone.utc).isoformat()
    get_db().execute("DELETE FROM developer_visit_logs WHERE datetime(created_at) < datetime(?)", (cutoff_iso,))
    get_db().commit()


def _page_label_from_path(path: str):
    path = str(path or "")
    if path == "/":
        return "Splash/Login"
    mappings = [
        ("/pastor-tool", "Pastor Tool"),
        ("/ao-tool/church-status", "Church Status"),
        ("/ao-tool/prayer-requests", "AO Prayer Requests"),
        ("/ao-tool", "AO Tool"),
        ("/prayer-request", "Prayer Request"),
        ("/schedules", "Schedules"),
        ("/bulletin", "Bulletin Board"),
        ("/event-registration", "Event Registration"),
        ("/pastor-login", "Pastor Login"),
        ("/ao-login", "AO Login"),
    ]
    for prefix, label in mappings:
        if path.startswith(prefix):
            return label
    return path


def _log_visit_if_needed():
    path = request.path or ""
    if path.startswith("/static/") or path == "/favicon.ico":
        return
    actor_username = ""
    actor_name = ""
    actor_role = ""
    if pastor_logged_in():
        actor_username = (session.get("pastor_username") or "").strip()
        actor_name = (session.get("pastor_name") or "").strip()
        actor_role = str(session.get("selected_position") or "Pastor")
    elif ao_logged_in():
        actor_username = (session.get("ao_username") or "").strip()
        actor_name = (session.get("ao_name") or "").strip()
        actor_role = str(session.get("ao_role") or "Area Overseer")
    get_db().execute(
        """
        INSERT INTO developer_visit_logs (
            created_at, ip_address, user_agent, actor_username, actor_name, actor_role, request_path, page_label, method
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            utc_now_iso(),
            str(request.headers.get("X-Forwarded-For") or request.remote_addr or "").split(",")[0].strip(),
            str(request.headers.get("User-Agent") or "").strip(),
            actor_username,
            actor_name,
            actor_role,
            path,
            _page_label_from_path(path),
            request.method,
        ),
    )
    get_db().commit()
    _cleanup_old_visit_logs()


@app.before_request
def before_request():
    init_db()
    _log_visit_if_needed()


@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, "_database", None)
    if db is not None:
        db.close()


@app.route("/", methods=["GET", "POST"])
def splash():
    """Splash page login with role selection."""

    logged_in = bool(session.get("pastor_logged_in")) or bool(session.get("ao_logged_in"))
    error = None

    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = (request.form.get("password") or "").strip()
        selected_role = (request.form.get("position") or "Pastor").strip().lower()

        if not username or not password:
            error = "Username and password are required."
        else:
            sync_from_sheets_if_needed(force=True)
            row = get_db().execute(
                """
                SELECT username, password, name, church_address, sex, age, position, sub_area
                FROM sheet_accounts_cache
                WHERE username = ?
                """,
                (username,),
            ).fetchone()

            stored_role = str((row["position"] if row and "position" in row.keys() else "") or "").strip().lower()

            if row and str(row["password"] or "").strip() == password:
                session.clear()
                session.permanent = True

                # AO login flow now supports both Area Overseer and Sub Area Overseer
                # as distinct session roles. For backward compatibility, if the UI still
                # posts "area overseer" for a Sub Area Overseer account, we still allow
                # login and store the real role from Accounts.
                if selected_role in ("area overseer", "sub area overseer"):
                    if stored_role == "area overseer" and selected_role == "sub area overseer":
                        error = "This account is not registered as Sub Area Overseer."
                    elif stored_role not in ("area overseer", "sub area overseer"):
                        error = "This account is not registered as Area Overseer."
                    else:
                        real_role = (row["position"] or "").strip()
                        session["selected_position"] = real_role.lower()
                        session["ao_logged_in"] = True
                        session["ao_username"] = username
                        session["ao_name"] = row["name"] or ""
                        session["ao_area_number"] = (row["age"] or "").strip()
                        session["ao_church_id"] = (row["sex"] or "").strip()
                        session["ao_role"] = real_role
                        session["ao_sub_area"] = (row["sub_area"] or "").strip()
                        return redirect(url_for("ao_tool"))
                else:
                    session["selected_position"] = selected_role
                    session["pastor_logged_in"] = True
                    session["pastor_username"] = username
                    session["pastor_name"] = row["name"] or ""
                    session["pastor_church_address"] = row["church_address"] or ""
                    session["pastor_church_id"] = (row["sex"] or "").strip()
                    session["pastor_area_number"] = (row["age"] or "").strip()
                    if selected_role == "member":
                        return redirect(url_for("bulletin"))
                    return redirect(url_for("pastor_tool"))
            else:
                error = "Invalid username or password."

    return render_template("splash.html", logged_in=logged_in, error=error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("splash"))


@app.route("/sync-area-data-log")
def sync_area_data_log():
    rows = get_db().execute(
        "SELECT * FROM developer_visit_logs ORDER BY datetime(created_at) DESC, id DESC LIMIT 500"
    ).fetchall()
    lines = ["District 4 Tool Developer Log", "=" * 32, ""]
    for r in rows:
        who = str(r["actor_name"] or r["actor_username"] or "Guest").strip() or "Guest"
        role = str(r["actor_role"] or "Unregistered").strip() or "Unregistered"
        ip = str(r["ip_address"] or "").strip()
        ua = str(r["user_agent"] or "").strip()
        lines.extend([
            f"[{r['created_at']}] {who} ({role})",
            f"Page: {r['page_label']}  Method: {r['method']}",
            f"Path: {r['request_path']}",
            f"IP: {ip}",
            f"User-Agent: {ua}",
            "-" * 72,
        ])
    body = "\n".join(lines)
    return make_response(f"<html><body style='background:#000;color:#fff;font-family:monospace;white-space:pre-wrap;padding:16px'>{body}</body></html>")

def _normalize_key(value):
    return str(value or "").strip().lower()


def _current_user_area_number():
    db = get_db()

    if pastor_logged_in():
        username = (session.get("pastor_username") or "").strip()
        if not username:
            return ""
        row = db.execute(
            "SELECT age FROM sheet_accounts_cache WHERE username = ?",
            (username,),
        ).fetchone()
        return str(row["age"] or "").strip() if row else ""

    if ao_logged_in():
        return (session.get("ao_area_number") or "").strip()

    return ""


def _get_area_directory(area_number: str):
    db = get_db()
    rows = db.execute(
        """
        SELECT
            TRIM(name) AS pastor_name,
            TRIM(birthday) AS birthday,
            TRIM(church_address) AS church_address,
            TRIM(sex) AS church_id,
            TRIM(position) AS position
        FROM sheet_accounts_cache
        WHERE TRIM(age) = TRIM(?)
        ORDER BY church_id, church_address, pastor_name
        """,
        (area_number,),
    ).fetchall()

    keys_to_display = {}
    churches = []
    pastors = []

    for r in rows:
        church_id = str(r["church_id"] or "").strip()
        church_address = str(r["church_address"] or "").strip()
        display_church = church_id or church_address

        if display_church:
            churches.append(display_church)
            keys_to_display[_normalize_key(display_church)] = display_church
        if church_address:
            keys_to_display[_normalize_key(church_address)] = display_church or church_address

        pastors.append(
            {
                "pastor_name": str(r["pastor_name"] or "").strip(),
                "birthday": str(r["birthday"] or "").strip(),
                "church_name": display_church,
                "position": str(r["position"] or "").strip(),
            }
        )

    unique_churches = []
    seen = set()
    for c in churches:
        n = _normalize_key(c)
        if n and n not in seen:
            seen.add(n)
            unique_churches.append(c)

    return {
        "churches": unique_churches,
        "keys_to_display": keys_to_display,
        "pastors": pastors,
    }


def _match_area_church_display(church_name: str, area_directory=None):
    church_name = str(church_name or "").strip()
    if not church_name:
        return ""

    norm = _normalize_key(church_name)

    if area_directory:
        return area_directory["keys_to_display"].get(norm, "")

    db = get_db()
    row = db.execute(
        """
        SELECT TRIM(sex) AS church_id, TRIM(church_address) AS church_address
        FROM sheet_accounts_cache
        WHERE TRIM(sex) = TRIM(?) OR TRIM(church_address) = TRIM(?)
        LIMIT 1
        """,
        (church_name, church_name),
    ).fetchone()

    if not row:
        return ""

    return str(row["church_id"] or row["church_address"] or "").strip()


def _parse_csv_churches(raw_value: str):
    return [x.strip() for x in str(raw_value or "").split(",") if x.strip()]


def _ordinal(n: int):
    if 10 <= (n % 100) <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def _sort_date_desc(post):
    d = post.get("sort_date")
    if isinstance(d, date):
        return d
    return date.min


def _build_birthday_posts(area_directory, today):
    posts = []

    for p in area_directory["pastors"]:
        birthday_raw = str(p.get("birthday") or "").strip()
        pastor_name = str(p.get("pastor_name") or "").strip()
        church_name = str(p.get("church_name") or "").strip()

        if not birthday_raw or not pastor_name or not church_name:
            continue

        bday = parse_sheet_date(birthday_raw)
        if not bday:
            continue

        if bday.month != today.month:
            continue

        age_turning = today.year - bday.year

        message = (
            f"Happy Birthday Ptr. {pastor_name} of {church_name} Church "
            f"on your {_ordinal(age_turning)} birthday! "
            f"We thank God for your life and ministry. "
            f"May He continue to bless and strengthen you."
        )

        posts.append(
            {
                "type": "birthday",
                "church_name": church_name,
                "title": f"Birthday Greeting • {church_name}",
                "summary": message,
                "meta": bday.strftime("%B %d"),
                "sort_date": date(today.year, today.month, bday.day),
            }
        )

    return posts


def _build_prayer_posts(area_directory, current_user_key, current_church_display):
    posts = []
    db = get_db()

    rows = db.execute(
        """
        SELECT *
        FROM sheet_prayer_request_cache
        WHERE TRIM(status) IN ('Approved', 'Answered')
        ORDER BY sheet_row DESC
        """
    ).fetchall()

    for r in rows:
        church_display = _match_area_church_display(r["church_name"], area_directory)
        if not church_display:
            continue

        request_date = parse_sheet_date(r["request_date"]) or date.today()
        answered_date = parse_sheet_date(r["answered_date"]) if r["answered_date"] else None

        raw_praying = _parse_csv_churches(r["pastors_praying"])
        normalized_seen = set()
        praying_churches = []

        for item in raw_praying:
            normalized = _match_area_church_display(item, area_directory) or item
            key = _normalize_key(normalized)
            if key and key not in normalized_seen:
                normalized_seen.add(key)
                praying_churches.append(normalized)

        already_praying = _normalize_key(current_church_display) in {
            _normalize_key(x) for x in praying_churches
        }

        is_owner = str(r["submitted_by"] or "").strip() == str(current_user_key or "").strip()

        status = str(r["status"] or "").strip()

        if status == "Approved":
            posts.append(
                {
                    "type": "prayer_request",
                    "request_id": r["request_id"],
                    "church_name": church_display,
                    "title": str(r["title"] or "").strip(),
                    "request_text": str(r["request_text"] or "").strip(),
                    "meta": request_date.strftime("%B %d, %Y"),
                    "sort_date": request_date,
                    "praying_churches": praying_churches,
                    "praying_count": len(praying_churches),
                    "already_praying": already_praying,
                    "can_pray": bool(current_church_display) and not already_praying and (_normalize_key(current_church_display) != _normalize_key(church_display)),
                    "is_owner": is_owner,
                }
            )

        elif status == "Answered":
            message = (
                "Hallelujah! God answered our prayer. "
                f"Request: {str(r['title'] or '').strip()}. "
                f"Date Requested: {str(r['request_date'] or '').strip()}. "
                f"Date Answered: {str(r['answered_date'] or '').strip()}. "
                "Thank you for your prayers."
            )

            posts.append(
                {
                    "type": "answered_prayer",
                    "church_name": church_display,
                    "title": f"Answered Prayer • {church_display}",
                    "summary": message,
                    "meta": (answered_date or request_date).strftime("%B %d, %Y"),
                    "sort_date": answered_date or request_date,
                }
            )

    return posts


def _build_report_recognition_posts(area_number, area_directory, today):
    posts = []

    expected_churches = area_directory["churches"]
    if not expected_churches:
        return posts

    key_map = area_directory["keys_to_display"]
    expected_keys = {_normalize_key(c) for c in expected_churches}

    db = get_db()
    report_rows = db.execute(
        """
        SELECT sheet_row, church, address
        FROM sheet_report_cache
        WHERE year = ? AND month = ?
        ORDER BY sheet_row ASC
        """,
        (today.year, today.month),
    ).fetchall()

    reported_churches = set()
    first_reporting_church = ""

    for r in report_rows:
        candidates = [
            str(r["church"] or "").strip(),
            str(r["address"] or "").strip(),
        ]

        matched = ""
        for c in candidates:
            if not c:
                continue
            matched = key_map.get(_normalize_key(c), "")
            if matched:
                break

        if matched:
            reported_churches.add(matched)
            if not first_reporting_church:
                first_reporting_church = matched

    all_reported = bool(expected_keys) and expected_keys.issubset({_normalize_key(c) for c in reported_churches})
    month_label = today.strftime("%B %Y")

    # Show the first-submission congratulations as soon as there is a first report
    # for the current month in this area. It no longer waits for all churches.
    if first_reporting_church:
        posts.append(
            {
                "type": "recognition",
                "church_name": first_reporting_church,
                "title": "First Report Submitted",
                "summary": (
                    f"Congratulations to church {first_reporting_church} for being the first to submit "
                    f"its report for {month_label}. Thank you for your diligence and faithfulness in the ministry!"
                ),
                "meta": month_label,
                "sort_date": today,
            }
        )

    # Keep the attendance-increase recognition only after all churches in the area
    # have submitted for the month, so those comparisons are based on a complete set.
    if not all_reported:
        return posts

    prev_year = today.year
    prev_month = today.month - 1
    if prev_month == 0:
        prev_month = 12
        prev_year -= 1

    for church in expected_churches:
        current_stats = get_report_stats_for_month_and_church_cache(today.year, today.month, church)
        prev_stats = get_report_stats_for_month_and_church_cache(prev_year, prev_month, church)

        if current_stats["rows"] <= 0 or prev_stats["rows"] <= 0:
            continue

        current_att = (
            float(current_stats["avg"]["adult"])
            + float(current_stats["avg"]["youth"])
            + float(current_stats["avg"]["children"])
        )
        prev_att = (
            float(prev_stats["avg"]["adult"])
            + float(prev_stats["avg"]["youth"])
            + float(prev_stats["avg"]["children"])
        )

        if prev_att <= 0:
            continue

        change_pct = ((current_att - prev_att) / prev_att) * 100.0
        if change_pct <= 0:
            continue

        posts.append(
            {
                "type": "recognition",
                "church_name": church,
                "title": "Attendance Increase",
                "summary": (
                    f"Congratulations to church {church} for a {round(change_pct)}% increase in attendance "
                    f"this month. Glory to God!"
                ),
                "meta": month_label,
                "sort_date": today,
            }
        )

    return posts


def build_bulletin_board_posts():
    area_number = _current_user_area_number()
    if not area_number:
        return [], ""

    today = date.today()
    area_directory = _get_area_directory(area_number)
    current_user_key = _current_user_key()
    current_church_display = _match_area_church_display(_current_user_church_name(), area_directory) or _current_user_church_name()

    posts = []
    posts.extend(_build_birthday_posts(area_directory, today))
    for a in get_announcements_for_current_user():
        ann_date = parse_sheet_date(a["announcement_date"]) or today
        posts.append({
            "type": "announcement",
            "church_name": "Area Announcement" if not str(a["sub_area"] or "").strip() else f"Sub Area {str(a['sub_area'] or '').strip()} Announcement",
            "title": str(a["title"] or "").strip(),
            "summary": str(a["announcement"] or "").strip(),
            "meta": ann_date.strftime("%B %d, %Y"),
            "sort_date": ann_date,
        })
    posts.extend(_build_prayer_posts(area_directory, current_user_key, current_church_display))
    posts.extend(_build_report_recognition_posts(area_number, area_directory, today))

    posts.sort(key=_sort_date_desc, reverse=True)

    return posts, current_church_display

@app.route("/bulletin")
def bulletin():
    if not any_user_logged_in():
        return redirect(url_for("pastor_login", next=request.path))

    sync_from_sheets_if_needed(force=True)

    reference, text = get_verse_of_the_day()
    today_str = date.today().strftime("%B %d, %Y")
    area_number = _current_user_area_number()
    posts, current_church = build_bulletin_board_posts()

    return render_template(
        "bulletin.html",
        verse_reference=reference,
        verse_text=text,
        today_str=today_str,
        area_number=area_number,
        posts=posts,
        current_church=current_church,
    )

@app.route("/bulletin/pray/<request_id>", methods=["POST"])
def bulletin_pray(request_id):
    if not any_user_logged_in():
        return redirect(url_for("pastor_login", next=request.path))

    sync_from_sheets_if_needed(force=True)

    area_number = _current_user_area_number()
    if not area_number:
        abort(403)

    area_directory = _get_area_directory(area_number)
    current_church = _match_area_church_display(_current_user_church_name(), area_directory) or _current_user_church_name()
    if not current_church:
        abort(403)

    db = get_db()
    row = db.execute(
        """
        SELECT *
        FROM sheet_prayer_request_cache
        WHERE request_id = ?
        """,
        (request_id,),
    ).fetchone()

    if not row:
        abort(404)

    if str(row["status"] or "").strip() != "Approved":
        abort(403)

    owner_church = _match_area_church_display(row["church_name"], area_directory)
    if not owner_church:
        abort(403)

    if _normalize_key(owner_church) == _normalize_key(current_church):
        return redirect(url_for("bulletin"))

    existing = _parse_csv_churches(row["pastors_praying"])
    normalized_existing = set()
    cleaned_existing = []

    for item in existing:
        normalized_item = _match_area_church_display(item, area_directory) or item
        key = _normalize_key(normalized_item)
        if key and key not in normalized_existing:
            normalized_existing.add(key)
            cleaned_existing.append(normalized_item)

    current_key = _normalize_key(current_church)
    if current_key not in normalized_existing:
        cleaned_existing.append(current_church)
        _update_prayer_request_cells_in_sheet(
            request_id,
            {"pastors_praying": ", ".join(cleaned_existing)},
        )
        sync_from_sheets_if_needed(force=True)

    return redirect(url_for("bulletin"))

# ========================
# Pastor login (uses CACHE)
# ========================

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
            row = get_db().execute(
                "SELECT username, password, name, church_address, sex FROM sheet_accounts_cache WHERE username = ?",
                (username,),
            ).fetchone()

            if row and str(row["password"] or "").strip() == password:
                session["pastor_logged_in"] = True
                session["pastor_username"] = username
                session["pastor_name"] = row["name"] or ""
                session["pastor_church_address"] = row["church_address"] or ""
                session.permanent = True
                return redirect(request.form.get("next") or next_url)

            try:
                client = get_gs_client()
                sh = client.open("District4 Data")
                ws = sh.worksheet("Accounts")
                records = ws.get_all_records()

                matched = None
                for rec in records:
                    u = str(rec.get("UserName", "")).strip()
                    p = str(rec.get("Password", "")).strip()
                    if username == u and password == p:
                        matched = rec
                        break

                if matched:
                    session["pastor_logged_in"] = True
                    session["pastor_username"] = username
                    session["pastor_name"] = matched.get("Name", "")
                    session["pastor_church_address"] = matched.get("Church Address", "")
                    session["pastor_church_id"] = matched.get("Church ID", "") or matched.get("Sex", "")
                    session.permanent = True

                    sync_from_sheets_if_needed(force=True)
                    return redirect(request.form.get("next") or next_url)

                error = "Invalid username or password."
            except Exception as e:
                error = f"Error accessing Google Sheets: {e}"

    return render_template("pastor_login.html", error=error, next_url=next_url)


# ========================
# Pastor Tool
# ========================

def _get_cached_pastor_account(pastor_username: str):
    return get_db().execute(
        """
        SELECT username, name, church_address, sex
        FROM sheet_accounts_cache
        WHERE username = ?
        """,
        ((pastor_username or "").strip(),),
    ).fetchone()


def _report_exists_for_pastor_month_from_cache(pastor_username: str, year: int, month: int) -> bool:
    acc = _get_cached_pastor_account(pastor_username)
    if not acc:
        return False

    pastor_name = str(acc["name"] or "").strip()
    church_address = str(acc["church_address"] or "").strip()
    church_id = str(acc["sex"] or "").strip()
    church_key = church_id or church_address

    row = get_db().execute(
        """
        SELECT COUNT(*) AS cnt
        FROM sheet_report_cache
        WHERE year = ? AND month = ?
          AND (
                TRIM(church) = TRIM(?)
             OR TRIM(address) = TRIM(?)
             OR TRIM(pastor) = TRIM(?)
          )
        """,
        (int(year), int(month), church_key, church_key, pastor_name),
    ).fetchone()
    return bool(row and int(row["cnt"] or 0) > 0)


def _export_month_to_sheet_for_pastor(pastor_username: str, year: int, month: int, status_label: str):
    db = get_db()
    cursor = db.cursor()

    pastor_username = (pastor_username or "").strip()
    if not pastor_username:
        return False

    monthly_report = cursor.execute(
        "SELECT * FROM monthly_reports WHERE year = ? AND month = ? AND pastor_username = ?",
        (year, month, pastor_username),
    ).fetchone()
    if not monthly_report:
        return False

    monthly_report_id = monthly_report["id"]
    cp_row = ensure_church_progress(monthly_report_id)
    sunday_rows = cursor.execute(
        """
        SELECT * FROM sunday_reports
        WHERE monthly_report_id = ?
        ORDER BY date
        """,
        (monthly_report_id,),
    ).fetchall()
    if not sunday_rows:
        return False

    sync_from_sheets_if_needed(force=True)
    acc = _get_cached_pastor_account(pastor_username)
    if not acc:
        return False

    pastor_name = str(acc["name"] or "").strip()
    church_address = str(acc["church_address"] or "").strip()
    church_id = str(acc["sex"] or "").strip()

    bible_new = cp_row["bible_new"] or 0
    bible_existing = cp_row["bible_existing"] or 0
    received_christ = cp_row["received_christ"] or 0
    baptized_water = cp_row["baptized_water"] or 0
    baptized_holy_spirit = cp_row["baptized_holy_spirit"] or 0
    healed = cp_row["healed"] or 0
    child_dedication = cp_row["child_dedication"] or 0

    church_key = church_id or church_address
    _delete_report_rows_for_month_in_sheet(year, month, church_key, pastor_name)

    for row in sunday_rows:
        d = datetime.fromisoformat(row["date"]).date()
        activity_date = f"{d.month}/{d.day}/{d.year}"

        tithes_church = row["tithes_church"] or 0
        offering = row["offering"] or 0
        mission = row["mission"] or 0
        tithes_personal = row["tithes_personal"] or 0
        amount_to_send = tithes_church + offering + mission + tithes_personal

        report_data = {
            "church": church_key,
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
        append_report_to_sheet(report_data)

    return True


def _get_existing_submit_report_job(pastor_username: str, year: int, month: int):
    return get_db().execute(
        """
        SELECT *
        FROM submit_report_jobs
        WHERE pastor_username = ? AND year = ? AND month = ?
          AND status IN ('queued', 'processing')
        ORDER BY datetime(created_at) DESC
        LIMIT 1
        """,
        ((pastor_username or "").strip(), int(year), int(month)),
    ).fetchone()


def _create_submit_report_job(pastor_username: str, year: int, month: int):
    existing = _get_existing_submit_report_job(pastor_username, year, month)
    if existing:
        return existing["id"]

    job_id = str(uuid.uuid4())
    get_db().execute(
        """
        INSERT INTO submit_report_jobs (id, pastor_username, year, month, status, progress_message, created_at)
        VALUES (?, ?, ?, ?, 'queued', ?, ?)
        """,
        (job_id, (pastor_username or "").strip(), int(year), int(month), 'Request received...', utc_now_iso()),
    )
    get_db().commit()
    return job_id


def _get_submit_report_job(job_id: str):
    return get_db().execute(
        "SELECT * FROM submit_report_jobs WHERE id = ?",
        (job_id,),
    ).fetchone()


def _process_submit_report_job(job_id: str):
    db = get_db()
    job = _get_submit_report_job(job_id)
    if not job or str(job["status"] or "") not in ("queued", "processing"):
        return

    pastor_username = str(job["pastor_username"] or "").strip()
    year = int(job["year"] or 0)
    month = int(job["month"] or 0)

    db.execute(
        """
        UPDATE submit_report_jobs
        SET status = 'processing', started_at = COALESCE(started_at, ?), progress_message = ?
        WHERE id = ?
        """,
        (utc_now_iso(), 'Checking Google Sheets...', job_id),
    )
    db.commit()

    try:
        sync_from_sheets_if_needed(force=True)
        if _report_exists_for_pastor_month_from_cache(pastor_username, year, month):
            db.execute(
                """
                UPDATE submit_report_jobs
                SET status = 'done', finished_at = ?, progress_message = ?
                WHERE id = ?
                """,
                (utc_now_iso(), 'Report already found in Google Sheets.', job_id),
            )
            db.commit()
            return

        db.execute(
            "UPDATE submit_report_jobs SET progress_message = ? WHERE id = ?",
            ('Submitting report...', job_id),
        )
        db.commit()

        set_month_submitted(year, month, pastor_username)
        _export_month_to_sheet_for_pastor(pastor_username, year, month, 'Pending AO approval')

        db.execute(
            "UPDATE submit_report_jobs SET progress_message = ? WHERE id = ?",
            ('Syncing with Google Sheets...', job_id),
        )
        db.commit()

        sync_from_sheets_if_needed(force=True)
        exists_now = _report_exists_for_pastor_month_from_cache(pastor_username, year, month)
        final_message = 'Submission complete.' if exists_now else 'Submitted. Waiting for cache refresh.'

        db.execute(
            """
            UPDATE submit_report_jobs
            SET status = 'done', finished_at = ?, progress_message = ?
            WHERE id = ?
            """,
            (utc_now_iso(), final_message, job_id),
        )
        db.commit()
    except Exception as e:
        db.execute(
            """
            UPDATE submit_report_jobs
            SET status = 'failed', finished_at = ?, error_message = ?, progress_message = ?
            WHERE id = ?
            """,
            (utc_now_iso(), str(e), 'Submission failed.', job_id),
        )
        db.commit()
        print('❌ Error processing submit report job:', e)


@app.route("/pastor-tool/submit/start", methods=["POST"])
def pastor_tool_submit_start():
    if not (pastor_logged_in() or ao_logged_in()):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    pastor_username = (request.form.get("church") or session.get("pastor_username") or "").strip()
    year = int(request.form.get("year") or 0)
    month = int(request.form.get("month") or 0)
    if not (pastor_username and year and month):
        return jsonify({"ok": False, "error": "Missing parameters"}), 400
    if ao_logged_in() and not _pastor_username_in_current_ao_scope(pastor_username):
        return jsonify({"ok": False, "error": "Forbidden"}), 403

    monthly_report = get_or_create_monthly_report(year, month, pastor_username)
    if bool(monthly_report["approved"]):
        return jsonify({"ok": False, "error": "This report has already been approved by AO."}), 400

    ensure_sunday_reports(monthly_report["id"], year, month)
    if not (all_sundays_complete(monthly_report["id"]) and bool(ensure_church_progress(monthly_report["id"])["is_complete"])):
        return jsonify({"ok": False, "error": "Complete all Sundays and Church Progress first."}), 400

    try:
        job_id = _create_submit_report_job(pastor_username, year, month)
        return jsonify({
            "ok": True,
            "job_id": job_id,
            "status_url": url_for("pastor_tool_submit_status", job_id=job_id),
        })
    except Exception as e:
        print('❌ Error starting submit report job:', e)
        return jsonify({"ok": False, "error": "Unable to start submit job."}), 500


@app.route("/pastor-tool/submit/status/<job_id>")
def pastor_tool_submit_status(job_id):
    if not (pastor_logged_in() or ao_logged_in()):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    job = _get_submit_report_job(job_id)
    if not job:
        return jsonify({"ok": False, "error": "Job not found"}), 404

    current_pastor = (session.get("pastor_username") or "").strip()
    if str(job["pastor_username"] or "").strip() != current_pastor:
        return jsonify({"ok": False, "error": "Forbidden"}), 403

    if str(job["status"] or "") == 'queued':
        _process_submit_report_job(job_id)
        job = _get_submit_report_job(job_id)

    return jsonify({
        "ok": True,
        "job_id": job_id,
        "status": str(job["status"] or 'queued'),
        "message": str(job["progress_message"] or 'Working...'),
        "error": str(job["error_message"] or ''),
        "redirect_url": url_for('pastor_tool', year=int(job['year']), month=int(job['month']), church=(current_pastor if ao_logged_in() else None)),
    })


@app.route("/pastor-tool", methods=["GET", "POST"])

def pastor_tool():
    # ✅ AO dropdown support + stable defaults
    ao_mode = False
    ao_church_choices = []      # list of pastor usernames
    ao_church_labels = {}       # username -> Church ID (from Accounts sheet)
    selected_church = None      # selected pastor username when AO is using dropdown

    if not (pastor_logged_in() or ao_logged_in()):
        return redirect(url_for("pastor_login", next=request.path))

    # --- AO MODE: AO can act as pastor for churches in the same Area Number ---
    if ao_logged_in():
        ao_mode = True
        session.setdefault("pastor_logged_in", True)

        ao_area = (session.get("ao_area_number") or "").strip()

        rows = get_db().execute(
            """
            SELECT username, sex
            FROM sheet_accounts_cache
            WHERE TRIM(age) = TRIM(?)
              AND LOWER(COALESCE(position,'')) = 'pastor'
              AND TRIM(username) != ''
              AND (? = '' OR TRIM(COALESCE(sub_area,'')) = TRIM(?))
            ORDER BY TRIM(sex), TRIM(username)
            """,
            (ao_area, (session.get("ao_sub_area") or "").strip() if ao_is_sub_area_overseer() else "", (session.get("ao_sub_area") or "").strip() if ao_is_sub_area_overseer() else ""),
        ).fetchall()

        ao_church_choices = [r["username"] for r in rows]
        ao_church_labels = {r["username"]: (r["sex"] or "").strip() for r in rows}

        # keep selection from URL (?church=username) or keep current session pastor_username
        selected_church = (request.args.get("church") or "").strip() or (session.get("pastor_username") or "").strip()
        if (not selected_church) and ao_church_choices:
            selected_church = ao_church_choices[0]

        # Safety: AO can only choose within list
        if selected_church and selected_church not in ao_church_choices:
            abort(403)

        if selected_church:
            session["pastor_username"] = selected_church

    # --- Normal pastor flow (or AO acting as selected pastor) ---
    refresh_pastor_from_cache()

    pastor_username = (session.get("pastor_username") or "").strip()

    today = date.today()
    year = request.args.get("year", type=int) or today.year
    month = request.args.get("month", type=int) or today.month

    sync_local_month_from_cache_for_pastor(year, month)

    monthly_report = get_or_create_monthly_report(year, month, pastor_username)
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

    year_options = list(range(today.year - 10, today.year + 4))

    month_names = [
        ("January", 1), ("February", 2), ("March", 3), ("April", 4),
        ("May", 5), ("June", 6), ("July", 7), ("August", 8),
        ("September", 9), ("October", 10), ("November", 11), ("December", 12),
    ]

    sundays_ok = all_sundays_complete(monthly_report["id"])
    can_submit = sundays_ok and cp_complete
    status_key = get_month_status(monthly_report)

    # ✅ Build checkmarks based on Google Sheets cache (not local DB)
    db = get_db()
    cursor = db.cursor()

    refresh_pastor_from_cache()
    pastor_name = (session.get("pastor_name") or "").strip()
    church_address = (session.get("pastor_church_address") or "").strip()
    church_id = (session.get("pastor_church_id") or "").strip()
    church_key = church_id or church_address

    rows = db.execute(
        """
        SELECT month, COUNT(*) AS cnt
        FROM sheet_report_cache
        WHERE year = ?
          AND (
                TRIM(church) = TRIM(?)
             OR TRIM(address) = TRIM(?)
             OR TRIM(pastor) = TRIM(?)
          )
        GROUP BY month
        """,
        (year, church_key, church_key, pastor_name),
    ).fetchall()

    month_has_data = {int(r["month"]): (int(r["cnt"] or 0) > 0) for r in rows}
    submitted_map = {(year, m): bool(month_has_data.get(m)) for m in range(1, 13)}

    existing_submit_job = _get_existing_submit_report_job(pastor_username, year, month)
    submit_job_id = existing_submit_job["id"] if existing_submit_job else ""

    if request.method == "POST":
        if bool(monthly_report["approved"]):
            if ao_mode and selected_church:
                return redirect(url_for("pastor_tool", year=year, month=month, church=selected_church))
            return redirect(url_for("pastor_tool", year=year, month=month))

        if can_submit:
            try:
                sync_from_sheets_if_needed(force=True)
                if not _report_exists_for_pastor_month_from_cache(pastor_username, year, month):
                    set_month_submitted(year, month, pastor_username)
                    _export_month_to_sheet_for_pastor(pastor_username, year, month, "Pending AO approval")
                    clear_month_dirty(year, month)
                    sync_from_sheets_if_needed(force=True)
                    sync_local_month_from_cache_for_pastor(year, month)
            except Exception as e:
                print("Error exporting month to sheet on submit:", e)

        if ao_mode and selected_church:
            return redirect(url_for("pastor_tool", year=year, month=month, church=selected_church))
        return redirect(url_for("pastor_tool", year=year, month=month))

    return render_template(
        "pastor_tool.html",
        year=year,
        month=month,
        year_options=year_options,
        month_names=month_names,
        monthly_report=monthly_report,
        sunday_list=sunday_list,
        can_submit=can_submit and not bool(monthly_report["approved"]),
        status_key=status_key,
        submitted_map=submitted_map,
        cp_complete=cp_complete,
        sundays_ok=sundays_ok,
        monthly_total=monthly_total,
        pastor_name=session.get("pastor_name", ""),
        ao_mode=ao_mode,
        ao_church_choices=ao_church_choices,
        ao_church_labels=ao_church_labels,
        selected_church=selected_church,
        submit_job_id=submit_job_id,
    )


@app.route("/pastor-tool/<int:year>/<int:month>/<int:day>", methods=["GET", "POST"], endpoint="sunday_detail")

def sunday_detail(year, month, day):
    if not (pastor_logged_in() or ao_logged_in()):
        return redirect(url_for("pastor_login", next=request.path))

    # ✅ AO MODE: keep selected church via ?church=username
    church = (request.args.get("church") or "").strip()

    if ao_logged_in():
        session.setdefault("pastor_logged_in", True)
        if church:
            if not _pastor_username_in_current_ao_scope(church):
                abort(403)
            session["pastor_username"] = church
        else:
            session.setdefault("pastor_username", session.get("ao_username", "ao"))

    refresh_pastor_from_cache()
    pastor_username = (session.get("pastor_username") or "").strip()

    try:
        d = date(year, month, day)
    except ValueError:
        abort(404)

    sync_local_month_from_cache_for_pastor(year, month)

    monthly_report = get_or_create_monthly_report(year, month, pastor_username)
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
            # ✅ HARD REPLACE (prevents duplicates / stale rows)
            cursor.execute(
                """
                DELETE FROM sunday_reports
                WHERE monthly_report_id = ? AND date = ?
                """,
                (monthly_report["id"], d.isoformat()),
            )

            cursor.execute(
                """
                INSERT INTO sunday_reports (
                    monthly_report_id,
                    date,
                    is_complete,
                    attendance_adult,
                    attendance_youth,
                    attendance_children,
                    attendance_total,
                    tithes_church,
                    offering,
                    mission,
                    tithes_personal
                ) VALUES (?, ?, 1, ?, ?, ?, NULL, ?, ?, ?, ?)
                """,
                (
                    monthly_report["id"],
                    d.isoformat(),
                    numeric_values["attendance_adult"],
                    numeric_values["attendance_youth"],
                    numeric_values["attendance_children"],
                    numeric_values["tithes_church"],
                    numeric_values["offering"],
                    numeric_values["mission"],
                    numeric_values["tithes_personal"],
                ),
            )

            db.commit()
            mark_month_dirty(year, month)

            # ✅ keep AO church selection after save
            if ao_logged_in() and church:
                return redirect(url_for("pastor_tool", year=year, month=month, church=church))
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
        church=church,
    )


@app.route("/pastor-tool/<int:year>/<int:month>/progress", methods=["GET", "POST"])

def church_progress_view(year, month):
    if not (pastor_logged_in() or ao_logged_in()):
        return redirect(url_for("pastor_login", next=request.path))

    church = (request.args.get("church") or "").strip()

    if ao_logged_in():
        session.setdefault("pastor_logged_in", True)
        if church:
            if not _pastor_username_in_current_ao_scope(church):
                abort(403)
            session["pastor_username"] = church
        else:
            session.setdefault("pastor_username", session.get("ao_username", "ao"))

    refresh_pastor_from_cache()
    pastor_username = (session.get("pastor_username") or "").strip()

    sync_local_month_from_cache_for_pastor(year, month)

    monthly_report = get_or_create_monthly_report(year, month, pastor_username)
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
            mark_month_dirty(year, month)

            if ao_logged_in() and church:
                return redirect(url_for("pastor_tool", year=year, month=month, church=church))
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
        church=church,
    )


@app.route("/ao-login", methods=["GET", "POST"])
def ao_login():
    error = None
    next_url = request.args.get("next") or url_for("ao_tool")

    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = (request.form.get("password") or "").strip()

        # Ensure cache is fresh enough for login
        sync_from_sheets_if_needed(force=True)

        row = get_db().execute(
            """
            SELECT username, password, name, age, sex, position, sub_area
            FROM sheet_accounts_cache
            WHERE username = ?
            """,
            (username,),
        ).fetchone()

        if row and (str(row["password"] or "").strip() == password):
            pos = str((row["position"] if "position" in row.keys() else "") or "").strip().lower()
            if pos in ("area overseer", "sub area overseer"):
                session["ao_logged_in"] = True
                session["ao_username"] = username
                session["ao_name"] = row["name"] or ""
                session["ao_area_number"] = (row["age"] or "").strip()
                session["ao_church_id"] = (row["sex"] or "").strip()
                session["ao_role"] = (row["position"] or "").strip()
                session["ao_sub_area"] = (row["sub_area"] or "").strip()
                session.permanent = True
                return redirect(request.form.get("next") or next_url)

        error = "Invalid username or password."

    return render_template("ao_login.html", error=error, next_url=next_url)


@app.route("/ao-tool")
def ao_tool():
    if not ao_logged_in():
        return redirect(url_for("ao_login", next=request.path))

    last_sync_ph = get_last_sync_display_ph()
    area_number = (session.get("ao_area_number") or "").strip()
    sub_area = (session.get("ao_sub_area") or "").strip() if ao_is_sub_area_overseer() else ""
    area_accounts = get_accounts_for_area_cache(area_number)

    selected_username = (request.args.get("edit_username") or "").strip()
    selected_account = None
    if selected_username:
        for row in area_accounts:
            if str(row["username"] or "").strip() == selected_username:
                selected_account = row
                break

    announcement_rows = get_db().execute(
        "SELECT * FROM sheet_announcement_cache WHERE TRIM(area) = TRIM(?) AND (? = '' OR TRIM(COALESCE(sub_area,'')) = TRIM(?)) ORDER BY sheet_row DESC",
        (area_number, (session.get("ao_sub_area") or "").strip() if ao_is_sub_area_overseer() else "", (session.get("ao_sub_area") or "").strip() if ao_is_sub_area_overseer() else ""),
    ).fetchall()
    return render_template(
        "ao_tool.html",
        last_sync_ph=last_sync_ph,
        area_accounts=area_accounts,
        selected_account=selected_account,
        ao_area_number=area_number,
        ao_is_sub_area=ao_is_sub_area_overseer(),
        ao_sub_area=(session.get("ao_sub_area") or "").strip(),
        announcement_rows=announcement_rows,
    )

def _get_account_cache_row(username: str):
    return get_db().execute(
        """
        SELECT *
        FROM sheet_accounts_cache
        WHERE TRIM(username) = TRIM(?)
        """,
        (username,),
    ).fetchone()

@app.route("/ao-tool/edit-account/save", methods=["POST"])
def ao_edit_account_save():
    if not ao_logged_in():
        return redirect(url_for("ao_login", next=request.path))

    area_number = (session.get("ao_area_number") or "").strip()
    original_username = (request.form.get("original_username") or "").strip()

    row = _get_account_cache_row(original_username)
    if not row or not _account_row_in_current_ao_scope(row):
        abort(403)

    full_name = (request.form.get("full_name") or "").strip()
    church_id = (request.form.get("sex") or "").strip()
    church_address = (request.form.get("church_address") or "").strip()
    contact_number = (request.form.get("contact_number") or "").strip()
    birthday = (request.form.get("birthday") or "").strip()
    username = (request.form.get("username") or "").strip()
    password = (request.form.get("password") or "").strip()

    if not all([full_name, church_id, church_address, contact_number, birthday, username, password]):
        flash("All account fields are required.", "error")
        return redirect(url_for("ao_tool", edit_username=original_username))

    existing_username = _get_account_cache_row(username)
    if existing_username and str(existing_username["username"] or "").strip() != original_username:
        flash("Username already exists. Please choose a different username.", "error")
        return redirect(url_for("ao_tool", edit_username=original_username))

    payload = {
        "full_name": full_name,
        "age": area_number,
        "sex": church_id,
        "church_address": church_address,
        "contact_number": contact_number,
        "birthday": birthday,
        "username": username,
        "password": password,
        "position": "Pastor",
        "sub_area": (session.get("ao_sub_area") or "").strip() if ao_is_sub_area_overseer() else str(row["sub_area"] or "").strip(),
    }

    try:
        _update_account_in_sheet(original_username, payload)
        sync_from_sheets_if_needed(force=True)
        flash("Account updated successfully.", "success")
        return redirect(url_for("ao_tool", edit_username=username))
    except Exception as e:
        print("❌ Edit account failed:", e)
        flash("Failed to update account.", "error")
        return redirect(url_for("ao_tool", edit_username=original_username))

@app.route("/ao-tool/edit-account/delete", methods=["POST"])
def ao_edit_account_delete():
    if not ao_logged_in():
        return redirect(url_for("ao_login", next=request.path))

    area_number = (session.get("ao_area_number") or "").strip()
    username = (request.form.get("original_username") or "").strip()

    row = _get_account_cache_row(username)
    if not row or not _account_row_in_current_ao_scope(row):
        abort(403)

    try:
        _delete_account_row_in_sheet(username)
        sync_from_sheets_if_needed(force=True)
        flash("Account deleted successfully.", "success")
    except Exception as e:
        print("❌ Delete account failed:", e)
        flash("Failed to delete account.", "error")

    return redirect(url_for("ao_tool"))

@app.route("/ao-tool/announcements/create", methods=["POST"])
def ao_announcement_create():
    if not ao_logged_in():
        return redirect(url_for("ao_login", next=request.path))
    title = (request.form.get("title") or "").strip()
    body = (request.form.get("announcement") or "").strip()
    if not title or not body:
        flash("Title and announcement are required.", "error")
        return redirect(url_for("ao_tool"))
    payload = {
        "title": title,
        "announcement": body,
        "announcement_date": date.today().isoformat(),
        "area": (session.get("ao_area_number") or "").strip(),
        "sub_area": (session.get("ao_sub_area") or "").strip() if ao_is_sub_area_overseer() else "",
        "author_username": (session.get("ao_username") or "").strip(),
        "author_name": (session.get("ao_name") or "").strip(),
    }
    try:
        _append_announcement_to_sheet(payload)
        sync_from_sheets_if_needed(force=True)
        flash("Announcement submitted successfully.", "success")
    except Exception as e:
        print("❌ Error creating announcement:", e)
        flash("Failed to submit announcement.", "error")
    return redirect(url_for("ao_tool"))


@app.route("/ao-tool/announcements/update", methods=["POST"])
def ao_announcement_update():
    if not ao_logged_in():
        return redirect(url_for("ao_login", next=request.path))
    sheet_row = int(request.form.get("sheet_row") or 0)
    row = _get_announcement_row(sheet_row)
    if not row:
        abort(404)
    if str(row["area"] or "").strip() != (session.get("ao_area_number") or "").strip():
        abort(403)
    if ao_is_sub_area_overseer() and str(row["sub_area"] or "").strip() != (session.get("ao_sub_area") or "").strip():
        abort(403)
    payload = {
        "title": (request.form.get("title") or "").strip(),
        "announcement": (request.form.get("announcement") or "").strip(),
        "announcement_date": str(row["announcement_date"] or date.today().isoformat()).strip() or date.today().isoformat(),
        "area": str(row["area"] or "").strip(),
        "sub_area": str(row["sub_area"] or "").strip(),
        "author_username": str(row["author_username"] or session.get("ao_username") or "").strip(),
        "author_name": str(row["author_name"] or session.get("ao_name") or "").strip(),
    }
    if not payload["title"] or not payload["announcement"]:
        flash("Title and announcement are required.", "error")
        return redirect(url_for("ao_tool"))
    try:
        _update_announcement_in_sheet(sheet_row, payload)
        sync_from_sheets_if_needed(force=True)
        flash("Announcement updated successfully.", "success")
    except Exception as e:
        print("❌ Error updating announcement:", e)
        flash("Failed to update announcement.", "error")
    return redirect(url_for("ao_tool"))


@app.route("/ao-tool/announcements/delete", methods=["POST"])
def ao_announcement_delete():
    if not ao_logged_in():
        return redirect(url_for("ao_login", next=request.path))
    sheet_row = int(request.form.get("sheet_row") or 0)
    row = _get_announcement_row(sheet_row)
    if not row:
        abort(404)
    if str(row["area"] or "").strip() != (session.get("ao_area_number") or "").strip():
        abort(403)
    if ao_is_sub_area_overseer() and str(row["sub_area"] or "").strip() != (session.get("ao_sub_area") or "").strip():
        abort(403)
    try:
        _delete_announcement_in_sheet(sheet_row)
        sync_from_sheets_if_needed(force=True)
        flash("Announcement deleted successfully.", "success")
    except Exception as e:
        print("❌ Error deleting announcement:", e)
        flash("Failed to delete announcement.", "error")
    return redirect(url_for("ao_tool"))


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
        age_raw = (session.get("ao_area_number") or request.form.get("age") or "").strip()
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

        if not all([full_name, age_raw, sex, church_address, contact_number, birthday_raw]):
            error = "All fields are required."
        else:
            try:
                age_int = int(age_raw)
            except ValueError:
                error = "Age must be a number."
            else:
                db = get_db()
                cursor = db.cursor()

                existing = get_db().execute(
                    """
                    SELECT username FROM sheet_accounts_cache
                    WHERE TRIM(name) = TRIM(?) AND TRIM(church_address) = TRIM(?)
                    LIMIT 1
                    """,
                    (full_name, church_address),
                ).fetchone()
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
                            "position": "Pastor",
                            "sub_area": (session.get("ao_sub_area") or "").strip() if ao_is_sub_area_overseer() else "",
                        }
                        try:
                            append_account_to_sheet(pastor_data)
                            sync_from_sheets_if_needed(force=True)
                        except Exception as e:
                            print("Error sending account to Google Sheets:", e)

                    except sqlite3.IntegrityError:
                        error = "Unable to create account (username conflict). Please try again."

    return render_template(
       "ao_create_account.html",
    ao_area_number=session.get("ao_area_number"),
        error=error,
        success=success,
        values=values,
        generated_username=generated_username,
        generated_password=generated_password,
    )


# ========================
# AO Church Status (DB-cache only)
# ========================

@app.route("/ao-tool/church-status")
def ao_church_status():
    if not ao_logged_in():
        return redirect(url_for("ao_login", next=request.path))

    sync_from_sheets_if_needed(force=False)

    today = date.today()
    year = request.args.get("year", type=int) or today.year

    year_options = list(range(2025, 2036))

    month_names = [
        ("January", 1), ("February", 2), ("March", 3), ("April", 4),
        ("May", 5), ("June", 6), ("July", 7), ("August", 8),
        ("September", 9), ("October", 10), ("November", 11), ("December", 12),
    ]

    all_churches = get_all_churches_from_cache()
    ao_area_number = (session.get("ao_area_number") or "").strip()

    months = []
    prev_avg_attendance_by_church = {}

    for name, m in month_names:
        church_items = []
        all_reported = True

        month_label = f"{name} {year}"
        aopt_amount = get_aopt_amount_from_cache(month_label, ao_area_number, (session.get("ao_sub_area") or "").strip() if ao_is_sub_area_overseer() else "")

        for church in all_churches:
            stats = get_report_stats_for_month_and_church_cache(year, m, church)
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

        area_summary = get_area_summary_for_month_cache(year, m, church_items, aopt_amount or 0.0, is_sub_ao=ao_is_sub_area_overseer())
        print_state = _get_print_button_state(
            year,
            m,
            ao_area_number,
            (session.get("ao_sub_area") or "").strip() if ao_is_sub_area_overseer() else "",
            total_churches=len(all_churches),
        )

        months.append(
            {
                "name": name,
                "month": m,
                "year": year,
                "label": month_label,
                "aopt_amount": aopt_amount,
                "all_reported": all_reported,
                "churches": church_items,
                "area_summary": area_summary,
                "print_state": print_state,
            }
        )

    open_month = request.args.get("open_month", type=int)

    return render_template(
        "ao_church_status.html",
        year=year,
        year_options=year_options,
        months=months,
        open_month=open_month,
        ao_is_sub_area=ao_is_sub_area_overseer(),
        ao_sub_area=(session.get("ao_sub_area") or "").strip(),
        ao_role=(session.get("ao_role") or "").strip(),
    )

@app.route("/ao-tool/church-status/aopt", methods=["POST"])
def ao_aopt_submit():
    if not ao_logged_in():
        return redirect(url_for("ao_login", next=request.path))

    year = int(request.form.get("year") or 0)
    month = int(request.form.get("month") or 0)
    amount_raw = (request.form.get("amount") or "").strip()

    if not (year and month):
        return redirect(url_for("ao_church_status", year=year or date.today().year))

    month_label = f"{calendar.month_name[month]} {year}"
    amount_val = parse_float(amount_raw)
    area_number = (session.get("ao_area_number") or "").strip()
    sub_area = (session.get("ao_sub_area") or "").strip() if ao_is_sub_area_overseer() else ""

    try:
        client = get_gs_client()
        sh = client.open("District4 Data")
        ws = sh.worksheet("AOPT")

        headers = _ensure_aopt_headers(ws)
        idx_month = _find_col(headers, "Month")
        idx_amount = _find_col(headers, "Amount")
        idx_area = _find_col(headers, "Area")
        if idx_area is None:
            idx_area = _find_col(headers, "Area Number")
        idx_sub = _find_col(headers, "SubArea")
        if idx_sub is None:
            idx_sub = _find_col(headers, "Sub Area")

        if idx_amount is None or idx_month is None or idx_area is None or idx_sub is None:
            print("❌ AOPT sheet missing required headers")
        else:
            db = get_db()
            cached = db.execute(
                """
                SELECT sheet_row
                FROM sheet_aopt_cache
                WHERE month = ? AND TRIM(area_number) = TRIM(?) AND TRIM(COALESCE(sub_area,'')) = TRIM(?)
                """,
                (month_label, area_number, sub_area),
            ).fetchone()

            if cached and cached["sheet_row"]:
                sheet_row = int(cached["sheet_row"])
                end_col = chr(ord('A') + max(idx_month, idx_amount, idx_area, idx_sub))
                row_values = [""] * (max(idx_month, idx_amount, idx_area, idx_sub) + 1)
                row_values[idx_month] = month_label
                row_values[idx_amount] = amount_val
                row_values[idx_area] = area_number
                row_values[idx_sub] = sub_area
                ws.update(
                    f"A{sheet_row}:{end_col}{sheet_row}",
                    [row_values],
                    value_input_option="USER_ENTERED",
                )
            else:
                row_values = [""] * (max(idx_month, idx_amount, idx_area, idx_sub) + 1)
                row_values[idx_month] = month_label
                row_values[idx_amount] = amount_val
                row_values[idx_area] = area_number
                row_values[idx_sub] = sub_area
                ws.append_row(row_values, value_input_option="USER_ENTERED")

            sync_from_sheets_if_needed(force=True)

    except Exception as e:
        print("❌ Error saving AOPT:", e)

    return redirect(url_for("ao_church_status", year=year, open_month=month))


@app.route("/ao-tool/church-status/approve", methods=["POST"])
def ao_church_status_approve():
    if not ao_logged_in():
        return redirect(url_for("ao_login", next=request.path))

    year = int(request.form.get("year") or 0)
    month = int(request.form.get("month") or 0)
    church = (request.form.get("church") or "").strip()

    ok = False
    if year and month and church and _church_in_current_ao_scope(church):
        try:
            sheet_batch_update_status_for_church_month(year, month, church, "Approved")
            cache_update_status_for_church_month(year, month, church, "Approved")
            ok = True
        except Exception as e:
            print("Error approving church month:", e)

    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return jsonify({"ok": ok, "year": year, "month": month, "church": church})

    return redirect(url_for("ao_church_status", year=year, open_month=month))


@app.route("/ao-tool/church-status/print-report/start", methods=["POST"])
def ao_church_status_print_report_start():
    if not ao_logged_in():
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    payload = request.get_json(silent=True) or request.form
    year = int((payload.get("year") if payload else 0) or 0)
    month = int((payload.get("month") if payload else 0) or 0)
    action = str((payload.get("action") if payload else "main") or "main").strip().lower()
    if action not in {"main", "late", "refresh"}:
        action = "main"

    area_number = (session.get("ao_area_number") or "").strip()
    sub_area = (session.get("ao_sub_area") or "").strip() if ao_is_sub_area_overseer() else ""
    if not (year and month and area_number):
        return jsonify({"ok": False, "error": "Missing parameters"}), 400

    try:
        report_type = "sub_area" if ao_is_sub_area_overseer() else "ao"
        job_id = _create_print_report_job(
            area_number,
            year,
            month,
            report_type=report_type,
            sub_area=sub_area,
            print_action=action,
        )
        return jsonify({
            "ok": True,
            "job_id": job_id,
            "action": action,
            "status_url": url_for("ao_church_status_print_report_status", job_id=job_id),
            "download_url": url_for("ao_church_status_print_report_download", job_id=job_id),
        })
    except Exception as e:
        print("❌ Error starting print report job:", e)
        return jsonify({"ok": False, "error": "Unable to start print job"}), 500


@app.route("/ao-tool/church-status/print-report/status/<job_id>")
def ao_church_status_print_report_status(job_id):
    if not ao_logged_in():
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    _advance_print_report_queue()
    job = _get_print_report_job(job_id)
    if not job:
        return jsonify({"ok": False, "error": "Job not found"}), 404

    status = str(job["status"] or "queued")
    queue_ahead = _count_jobs_ahead(job) if status == "queued" else 0
    message = "Request received..."
    if status == "queued":
        message = "Waiting in queue..." if queue_ahead > 0 else "Preparing report..."
    elif status == "processing":
        action = str(job["print_action"] or "main").strip().lower() if "print_action" in job.keys() else "main"
        if action == "refresh":
            message = "Refreshing print history..."
        else:
            message = "Generating PDF..."
    elif status == "done":
        action = str(job["print_action"] or "main").strip().lower() if "print_action" in job.keys() else "main"
        if action == "refresh":
            message = "Print history refreshed."
        else:
            message = "Download ready..."
    elif status == "downloaded":
        message = "Download already started."
    elif status == "failed":
        message = job["error_message"] or "Failed to generate report."

    return jsonify({
        "ok": True,
        "job_id": job_id,
        "status": status,
        "queue_ahead": queue_ahead,
        "message": message,
        "download_url": url_for("ao_church_status_print_report_download", job_id=job_id) if status == "done" and str(job["print_action"] or "main").strip().lower() != "refresh" else "",
        "error": job["error_message"] or "",
    })


@app.route("/ao-tool/church-status/print-report/finalize/<job_id>", methods=["POST"])
def ao_church_status_print_report_finalize(job_id):
    if not ao_logged_in():
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    job = _get_print_report_job(job_id)
    if not job:
        return jsonify({"ok": False, "error": "Job not found"}), 404

    print_action = str(job["print_action"] or "main").strip().lower()
    if print_action == "refresh":
        return jsonify({"ok": True, "finalized": True, "message": "Refresh completed."})

    label = "LatePrint" if print_action == "late" else "MainPrint"
    try:
        marked = _mark_scope_rows_for_print(job, label)
    except Exception as mark_err:
        return jsonify({"ok": False, "error": str(mark_err)}), 500

    try:
        get_db().execute(
            "UPDATE print_report_jobs SET status = 'downloaded' WHERE id = ? AND status = 'done'",
            (job_id,),
        )
        get_db().commit()
    except Exception:
        pass

    return jsonify({"ok": True, "finalized": True, "marked": marked, "message": "ReportStatus updated."})

import threading

download_locks = {}
download_done = {}

def get_lock(job_id):
    if job_id not in download_locks:
        download_locks[job_id] = threading.Lock()
    return download_locks[job_id]


@app.route("/ao-tool/church-status/print-report/download/<job_id>")
def ao_church_status_print_report_download(job_id):
    if not ao_logged_in():
        return redirect(url_for("ao_login", next=request.path))

    job = _get_print_report_job(job_id)
    if not job:
        abort(404)

    print_action = str(job["print_action"] or "main").strip().lower()
    if print_action == "refresh":
        return redirect(url_for("ao_church_status", year=int(job["year"]), open_month=int(job["month"])))

    result_path = str(job["result_path"] or "").strip()
    if not result_path or not os.path.exists(result_path):
        abort(404)

    lock = get_lock(job_id)

    with lock:
        if download_done.get(job_id):
            return ("", 204)

        try:
            response = make_response(send_file(result_path, as_attachment=True))
            download_done[job_id] = True
            return response
        except Exception as e:
            return str(e), 500


# ========================
# Prayer Request (menu + pages)
# ========================


@app.route("/prayer-request")
def prayer_request():
    if not any_user_logged_in():
        return redirect(url_for("pastor_login", next=request.path))
    return render_template("prayer_request.html")


@app.route("/prayer-request/write", methods=["GET", "POST"])
def prayer_request_write():
    if not any_user_logged_in():
        return redirect(url_for("pastor_login", next=request.path))

    if request.method == "POST":
        title = (request.form.get("title") or "").strip()

        # ✅ Accept multiple possible textarea names (prevents the “required” error)
        body = (
            request.form.get("prayer_request")
            or request.form.get("request_text")
            or request.form.get("prayerRequest")
            or ""
        ).strip()

        if not title or not body:
            return render_template(
                "prayer_write.html",
                error="Title and Prayer Request are required.",
                values={"title": title, "prayer_request": body},
            )

        req_id = str(uuid.uuid4())
        submitted_by = _current_user_key()
        church_name = _current_user_church_name()
        req_date = date.today().isoformat()

        try:
            _append_prayer_request_to_sheet(
                church_name=church_name,
                submitted_by=submitted_by,
                request_id=req_id,
                title=title,
                request_date=req_date,
                request_text=body,
            )
            sync_from_sheets_if_needed(force=True)
        except Exception as e:
            print("❌ Error submitting prayer request:", e)

        return redirect(url_for("prayer_request_status"))

    return render_template(
        "prayer_write.html",
        error=None,
        values={"title": "", "prayer_request": ""},
    )


@app.route("/prayer-request/status")
def prayer_request_status():
    if not any_user_logged_in():
        return redirect(url_for("pastor_login", next=request.path))

    # ✅ Force refresh so newly submitted requests always show immediately
    sync_from_sheets_if_needed(force=True)

    submitted_by = _current_user_key()
    rows = get_prayer_requests_for_user(submitted_by, include_answered=False)

    items = []
    for r in rows:
        items.append(
            {
                "request_id": r["request_id"],
                "church_name": r["church_name"] or "",
                "submitted_by": r["submitted_by"] or "",
                "title": r["title"] or "",
                "request_date": r["request_date"] or "",
                "request_text": r["request_text"] or "",
                "status": r["status"] or "Pending",
                "pastors_praying": r["pastors_praying"] or "",
                "answered_date": r["answered_date"] or "",
            }
        )

    return render_template("prayer_status.html", items=items, user_display=_current_user_display())


@app.route("/prayer-request/answered")
def prayer_request_answered():
    if not any_user_logged_in():
        return redirect(url_for("pastor_login", next=request.path))

    # ✅ Refresh so answered requests show immediately
    sync_from_sheets_if_needed(force=True)

    submitted_by = _current_user_key()
    rows = get_answered_prayer_requests_for_user(submitted_by)

    answered = []
    for r in rows:
        answered.append(
            {
                "request_id": r["request_id"],
                "church_name": r["church_name"] or "",
                "submitted_by": r["submitted_by"] or "",
                "title": r["title"] or "",
                "request_date": r["request_date"] or "",
                "request_text": r["request_text"] or "",
                "status": r["status"] or "Answered",
                "pastors_praying": r["pastors_praying"] or "",
                "answered_date": r["answered_date"] or "",
            }
        )

    return render_template(
        "prayer_answered.html",
        answered=answered,
        user_display=_current_user_display(),
    )


@app.route("/prayer-request/edit/<request_id>", methods=["GET", "POST"])
def prayer_request_edit(request_id):
    if not any_user_logged_in():
        return redirect(url_for("pastor_login", next=request.path))

    submitted_by = _current_user_key()
    db = get_db()
    row = db.execute(
        "SELECT * FROM sheet_prayer_request_cache WHERE request_id = ?",
        (request_id,),
    ).fetchone()

    if not row:
        abort(404)

    # permission: only owner or AO can edit
    if not ao_logged_in() and (str(row["submitted_by"] or "").strip() != submitted_by):
        abort(403)

    if request.method == "POST":
        title = (request.form.get("title") or "").strip()
        body = (
            request.form.get("prayer_request")
            or request.form.get("request_text")
            or ""
        ).strip()

        if not title or not body:
            return render_template(
                "prayer_edit.html",
                error="Title and Prayer Request are required.",
                item={"request_id": request_id, "title": title, "request_text": body},
            )

        try:
            _update_prayer_request_cells_in_sheet(
                request_id,
                {"title": title, "request_text": body},
            )
            sync_from_sheets_if_needed(force=True)
        except Exception as e:
            print("❌ Error editing prayer request:", e)

        return redirect(url_for("prayer_request_status"))

    return render_template(
        "prayer_edit.html",
        error=None,
        item={
            "request_id": request_id,
            "title": row["title"] or "",
            "request_text": row["request_text"] or "",
        },
    )


@app.route("/prayer-request/delete/<request_id>", methods=["POST"])
def prayer_request_delete(request_id):
    if not any_user_logged_in():
        return redirect(url_for("pastor_login", next=request.path))

    submitted_by = _current_user_key()
    db = get_db()
    row = db.execute(
        "SELECT submitted_by FROM sheet_prayer_request_cache WHERE request_id = ?",
        (request_id,),
    ).fetchone()
    if not row:
        return redirect(url_for("prayer_request_status"))

    if not ao_logged_in() and str(row["submitted_by"] or "").strip() != submitted_by:
        abort(403)

    try:
        _delete_prayer_request_row_in_sheet(request_id)
        sync_from_sheets_if_needed(force=True)
    except Exception as e:
        print("❌ Error deleting prayer request:", e)

    return redirect(url_for("prayer_request_status"))


@app.route("/prayer-request/mark-answered/<request_id>", methods=["POST"])
def prayer_request_mark_answered(request_id):
    if not any_user_logged_in():
        return redirect(url_for("pastor_login", next=request.path))

    submitted_by = _current_user_key()
    db = get_db()

    row = db.execute(
        "SELECT submitted_by, status FROM sheet_prayer_request_cache WHERE request_id = ?",
        (request_id,),
    ).fetchone()

    if not row:
        return redirect(url_for("prayer_request_status"))

    # owner or AO only
    if not ao_logged_in() and str(row["submitted_by"] or "").strip() != submitted_by:
        abort(403)

    status = (str(row["status"] or "")).strip().lower()

    # ✅ Only allow "Answered" when Approved (AO can still do it anytime if you want)
    if "approved" not in status and not ao_logged_in():
        return redirect(url_for("prayer_request_status"))

    try:
        _update_prayer_request_cells_in_sheet(
            request_id,
            {"status": "Answered", "answered_date": date.today().isoformat()},
        )
        sync_from_sheets_if_needed(force=True)
    except Exception as e:
        print("❌ Error marking answered:", e)

    return redirect(url_for("prayer_request_status"))


# ========================
# AO Tool → Prayer Request Approval
# ========================

@app.route("/ao-tool/prayer-requests")
def ao_prayer_requests():
    if not ao_logged_in():
        return redirect(url_for("ao_login", next=request.path))

    # ✅ Always refresh so AO sees latest submissions
    sync_from_sheets_if_needed(force=True)

    rows = get_pending_prayers_for_ao()
    items = []
    for r in rows:
        items.append(
            {
                "request_id": r["request_id"],
                "church_name": r["church_name"] or "",
                "submitted_by": r["submitted_by"] or "",
                "title": r["title"] or "",
                "request_date": r["request_date"] or "",
                "request_text": r["request_text"] or "",
                "status": r["status"] or "Pending",
            }
        )

    return render_template("ao_prayer_approval.html", items=items)


@app.route("/ao-tool/prayer-requests/approve/<request_id>", methods=["POST"])
def ao_prayer_requests_approve(request_id):
    if not ao_logged_in():
        return redirect(url_for("ao_login", next=request.path))

    try:
        prayer_row = get_db().execute("SELECT * FROM sheet_prayer_request_cache WHERE request_id = ?", (request_id,)).fetchone()
        if not _prayer_in_current_ao_manage_scope(prayer_row):
            abort(403)
        _update_prayer_request_cells_in_sheet(request_id, {"status": "Approved"})
        sync_from_sheets_if_needed(force=True)
    except Exception as e:
        print("❌ Error approving prayer request:", e)

    return redirect(url_for("ao_prayer_requests"))


@app.route("/ao-tool/prayer-requests/reject/<request_id>", methods=["POST"])
def ao_prayer_requests_reject(request_id):
    if not ao_logged_in():
        return redirect(url_for("ao_login", next=request.path))

    # ✅ simple reject = delete row
    try:
        prayer_row = get_db().execute("SELECT * FROM sheet_prayer_request_cache WHERE request_id = ?", (request_id,)).fetchone()
        if not _prayer_in_current_ao_manage_scope(prayer_row):
            abort(403)
        _delete_prayer_request_row_in_sheet(request_id)
        sync_from_sheets_if_needed(force=True)
    except Exception as e:
        print("❌ Error rejecting prayer request:", e)

    return redirect(url_for("ao_prayer_requests"))


@app.route("/ao-tool/prayer-requests/approve-all", methods=["POST"])
def ao_prayer_requests_approve_all():
    if not ao_logged_in():
        return redirect(url_for("ao_login", next=request.path))

    try:
        # refresh then approve everything still pending
        sync_from_sheets_if_needed(force=True)
        rows = get_pending_prayers_for_ao()

        for r in rows:
            req_id = (r["request_id"] or "").strip()
            if not req_id or not _prayer_in_current_ao_manage_scope(r):
                continue
            _update_prayer_request_cells_in_sheet(req_id, {"status": "Approved"})

        sync_from_sheets_if_needed(force=True)
    except Exception as e:
        print("❌ Error approving all prayer requests:", e)

    return redirect(url_for("ao_prayer_requests"))



def _ensure_announcement_sheet_headers(ws):
    values = ws.get_all_values()
    headers = ["Title", "Announcement", "Date", "Area", "SubArea", "Author Username", "Author Name"]
    if not values:
        ws.append_row(headers, value_input_option="USER_ENTERED")
        return headers
    current = list(values[0])
    changed = False
    for i, name in enumerate(headers):
        if _find_col(current, name) is None:
            while len(current) <= i:
                current.append("")
            current[i] = name
            changed = True
    if changed:
        rng = f"A1:{chr(ord('A') + len(current) - 1)}1"
        ws.update(rng, [current], value_input_option="USER_ENTERED")
        values = ws.get_all_values()
        return values[0]
    return current


def _append_announcement_to_sheet(payload: dict):
    client = get_gs_client()
    sh = client.open("District4 Data")
    try:
        ws = sh.worksheet("Anouncement")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title="Anouncement", rows=1000, cols=10)
    headers = _ensure_announcement_sheet_headers(ws)
    row = [""] * len(headers)
    mapping = {
        "Title": payload.get("title", ""),
        "Announcement": payload.get("announcement", ""),
        "Date": payload.get("announcement_date", ""),
        "Area": payload.get("area", ""),
        "SubArea": payload.get("sub_area", ""),
        "Sub Area": payload.get("sub_area", ""),
        "Author Username": payload.get("author_username", ""),
        "Author Name": payload.get("author_name", ""),
    }
    for i, h in enumerate(headers):
        if h in mapping:
            row[i] = mapping[h]
    ws.append_row(row, value_input_option="USER_ENTERED")


def _get_announcement_row(sheet_row: int):
    return get_db().execute("SELECT * FROM sheet_announcement_cache WHERE sheet_row = ?", (int(sheet_row),)).fetchone()


def _update_announcement_in_sheet(sheet_row: int, payload: dict):
    client = get_gs_client()
    sh = client.open("District4 Data")
    ws = sh.worksheet("Anouncement")
    headers = _ensure_announcement_sheet_headers(ws)
    row = [""] * len(headers)
    mapping = {
        "Title": payload.get("title", ""),
        "Announcement": payload.get("announcement", ""),
        "Date": payload.get("announcement_date", ""),
        "Area": payload.get("area", ""),
        "SubArea": payload.get("sub_area", ""),
        "Sub Area": payload.get("sub_area", ""),
        "Author Username": payload.get("author_username", ""),
        "Author Name": payload.get("author_name", ""),
    }
    for i, h in enumerate(headers):
        if h in mapping:
            row[i] = mapping[h]
    end_col = chr(ord('A') + len(headers) - 1)
    ws.update(f"A{int(sheet_row)}:{end_col}{int(sheet_row)}", [row], value_input_option="USER_ENTERED")


def _delete_announcement_in_sheet(sheet_row: int):
    if int(sheet_row) <= 1:
        return False
    client = get_gs_client()
    sh = client.open("District4 Data")
    ws = sh.worksheet("Anouncement")
    ws.delete_rows(int(sheet_row))
    return True


def _announcement_in_current_scope(row):
    if not row:
        return False
    area = str(row["area"] or "").strip()
    sub_area = str(row["sub_area"] or "").strip()
    my_area = _current_user_area_number()
    if area and my_area and area != my_area:
        return False
    current_sub = ""
    if pastor_logged_in():
        u = (session.get("pastor_username") or "").strip()
        acc = get_db().execute("SELECT sub_area FROM sheet_accounts_cache WHERE username = ?", (u,)).fetchone()
        current_sub = str(acc["sub_area"] or "").strip() if acc else ""
    elif ao_logged_in():
        current_sub = (session.get("ao_sub_area") or "").strip()
    if sub_area and sub_area != current_sub:
        return False
    return True


def get_announcements_for_current_user():
    rows = get_db().execute(
        "SELECT * FROM sheet_announcement_cache ORDER BY sheet_row DESC"
    ).fetchall()
    return [r for r in rows if _announcement_in_current_scope(r)]



# ========================
# Other pages
# ========================

@app.route("/event-registration")
def event_registration():
    return render_template("event_registration.html")



# ===============================
# AO ACCOUNT EDIT HELPERS
# ===============================

def _update_account_in_sheet(original_username: str, payload: dict):
    db = get_db()
    cached = db.execute(
        "SELECT sheet_row FROM sheet_accounts_cache WHERE TRIM(username) = TRIM(?)",
        (original_username,),
    ).fetchone()
    if not cached or not cached["sheet_row"]:
        return False

    sheet_row = int(cached["sheet_row"])

    client = get_gs_client()
    sh = client.open("District4 Data")
    ws = sh.worksheet("Accounts")
    headers = _ensure_accounts_headers(ws)
    row = [_build_account_row_from_headers(headers, payload)]
    end_col = chr(ord('A') + len(headers) - 1)
    ws.update(f"A{sheet_row}:{end_col}{sheet_row}", row, value_input_option="USER_ENTERED")
    return True


def _delete_account_row_in_sheet(username: str):
    db = get_db()
    cached = db.execute(
        "SELECT sheet_row FROM sheet_accounts_cache WHERE TRIM(username) = TRIM(?)",
        (username,),
    ).fetchone()
    if not cached or not cached["sheet_row"]:
        return False

    sheet_row = int(cached["sheet_row"])
    if sheet_row <= 1:
        return False

    client = get_gs_client()
    sh = client.open("District4 Data")
    ws = sh.worksheet("Accounts")

    ws.delete_rows(sheet_row)
    return True

if __name__ == "__main__":
    with app.app_context():
        init_db()
        print("Database initialized")
        print("Timezone:", PH_TZ)

    app.run(debug=True)
