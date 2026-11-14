import os
import re
import sqlite3
from datetime import datetime, date, timezone, timedelta
import calendar
import urllib.parse
import uuid
import traceback
import secrets
import tempfile
import hmac
import shutil
import threading
import json
import time
import io
import base64
from pathlib import Path
from contextlib import closing
from functools import lru_cache, wraps

from sheet_cache import DATASETS, CacheSyncError, init_cache_tables, sync_cache
from login_events import ensure_login_event_schema, insert_login_event

from zoneinfo import ZoneInfo

import requests
import gspread

from runtime_config import data_path, google_credentials
from google.auth.transport.requests import Request as GoogleAuthRequest
from flask import (
    Flask,
    g,
    render_template,
    render_template_string,
    request,
    redirect,
    url_for,
    abort,
    session,
    flash,
    jsonify,
    make_response,
    send_file,
    has_request_context,
)
from church_finder import register_church_finder_routes
from area_progress_monitor import register_area_progress_monitor, ensure_area_progress_monitor_tables
from church_progress import register_church_progress
from schedule import register_schedule_routes
from thanksgiving_pledges import register_thanksgiving_pledges
from temp_edit import register_temp_edit_routes
from pastor_resources import register_pastor_resources_routes
from bible_study import daily_maxwell_insight, get_indexed_niv_passage, register_bible_study_routes
from daily_devotional import ORIGINAL_SOURCE, bible_study_passage, compose_devotional, get_daily_entry
from mobile_bible_api import register_mobile_bible_routes
from sermon_ebooks import register_sermon_ebooks_routes
from pastor_report_pdf import build_monthly_activity_report_pdf, monthly_report_filename
from ai_assistant import register_ai_assistant
from developer import register_developer_routes

DATABASE = data_path("app_v2.db")
AI_DATABASE = os.getenv("AI_DATABASE") or data_path("ai_index.db")
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
            latitude TEXT,
            longitude TEXT,
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
    if "latitude" not in _acc_cols:
        try:
            cursor.execute("ALTER TABLE sheet_accounts_cache ADD COLUMN latitude TEXT")
        except Exception:
            pass

    if "longitude" not in _acc_cols:
        try:
            cursor.execute("ALTER TABLE sheet_accounts_cache ADD COLUMN longitude TEXT")
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

    ensure_login_event_schema(db)

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

    init_website_activity_tables(db)
    init_cache_tables(db)
    migrate_monthly_reports_scope_to_pastor()
    ensure_area_progress_monitor_tables(db)
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
    creds = google_credentials(
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
    # Header-only sheets are valid initialized caches; do not re-read empty ones.
    sync_from_sheets_if_needed()


def refresh_sheet_cache(*datasets):
    """Strict refresh: source fetch/validation failures leave every cache intact."""
    return sync_cache(get_db(), DATABASE, get_gs_client, parse_float,
                      parse_sheet_date, utc_now_iso, datasets or None)


def queue_sheet_cache_refresh(*datasets):
    if any(name not in DATASETS for name in datasets):
        raise ValueError("Unknown cache dataset")
    db = get_db()
    db.executemany("INSERT OR REPLACE INTO sheet_cache_pending_refresh VALUES (?,?)",
                   [(name, utc_now_iso()) for name in datasets])
    db.commit()


def refresh_after_sheet_write(*datasets):
    """Queue recovery after a confirmed Sheet write, without reporting it as failed."""
    queue_sheet_cache_refresh(*datasets)
    try:
        refresh_sheet_cache(*datasets)
        return True
    except CacheSyncError:
        app.logger.exception("Google Sheets write succeeded but cache refresh failed")
        if has_request_context():
            flash("Saved to Google Sheets. The local view could not refresh yet; it will retry automatically.", "error")
        return False


def verify_sheet_row(ws, dataset, sheet_row, headers=None, values=None):
    """Reject writes to rows that moved since the page's mirror was built."""
    if values is not None:
        headers = values[0] if values else []
        source_row = values[sheet_row - 1] if 1 < sheet_row <= len(values) else []
    elif headers is not None:
        source_row = ws.row_values(sheet_row)
    else:
        values = ws.get_all_values()
        headers = values[0] if values else []
        source_row = values[sheet_row - 1] if 1 < sheet_row <= len(values) else []
    cached = get_db().execute(f"SELECT * FROM {DATASETS[dataset]} WHERE sheet_row = ?", (sheet_row,)).fetchone()
    identities = {
        "Accounts": [("username", ("UserName",)), ("name", ("Name",)),
                     ("sex", ("Church ID", "Sex")), ("church_address", ("Church Address",))],
        "PrayerRequest": [("request_id", ("Request ID",))],
        "DistrictSchedule": [("church_name", ("Church Name",)), ("activity_date_start", ("Activity Date Start",))],
        "Anouncement": [("title", ("Title",)), ("announcement", ("Announcement",)), ("area", ("Area",))],
        "Report": [("activity_date", ("activity_date",)), ("church", ("church",)),
                   ("address", ("address",)), ("pastor", ("pastor",))],
        "AOPT": [("month", ("Month",)), ("area_number", ("Area Number", "Area")),
                 ("sub_area", ("Sub Area", "SubArea"))],
    }
    matches = bool(cached and source_row and sheet_row > 1)
    if matches:
        for column, alternatives in identities[dataset]:
            index = next((index for header in alternatives
                          if (index := _find_col(headers, header)) is not None), None)
            source_value = str(source_row[index]).strip() if index is not None and index < len(source_row) else ""
            cached_value = str(cached[column] or "").strip()
            if column in ("activity_date", "activity_date_start"):
                matches = bool(parse_sheet_date(source_value)) and parse_sheet_date(source_value) == parse_sheet_date(cached_value)
            else:
                matches = source_value == cached_value
            if not matches:
                break
    if not matches:
        queue_sheet_cache_refresh(dataset)
        raise RuntimeError("Google Sheets data changed. Reload the page before trying again.")


def sync_from_sheets_if_needed(force=False):
    """Normal requests use initialized mirrors; only bootstrap/recovery reads Sheets."""
    if force:
        return refresh_sheet_cache()
    db = get_db()
    initialized = {row[0] for row in db.execute("SELECT dataset FROM sheet_cache_sync_state")}
    pending = {row[0] for row in db.execute("SELECT dataset FROM sheet_cache_pending_refresh")}
    needed = [name for name in DATASETS if name not in initialized or name in pending]
    if not needed:
        return {}
    try:
        return sync_cache(db, DATABASE, get_gs_client, parse_float, parse_sheet_date,
                          utc_now_iso, needed, only_missing=not pending)
    except CacheSyncError:
        app.logger.exception("Cache bootstrap/recovery failed; existing mirrors retained")
        return False


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
    source_values = ws.get_all_values()
    for r in sorted(set(int(x) for x in sheet_rows if x)):
        verify_sheet_row(ws, "Report", r, values=source_values)
        requests_body.append({"range": f"{col_letter}{r}", "values": [[status_label]]})
    if requests_body:
        ws.batch_update(requests_body)
        queue_sheet_cache_refresh("Report")



def _sync_report_cache_only():
    return refresh_after_sheet_write("Report")


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
    creds = google_credentials(
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

        out_dir = data_path("generated_reports")
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
    Identity is based on the account that actually logged in.
    AO/DO takes priority over any temporary Pastor's Tool working context.
    """
    if session.get("ao_logged_in"):
        return (session.get("ao_username") or "").strip() or "ao"
    if session.get("pastor_logged_in"):
        return (session.get("pastor_username") or "").strip() or "pastor"
    if session.get("member_logged_in") is True:
        return str(session.get("username") or "").strip()
    return ""


def _current_user_display():
    # AO/DO identity must not become the temporarily selected pastor.
    if session.get("ao_logged_in"):
        return (session.get("ao_name") or "").strip() or (session.get("ao_username") or "AO")
    if session.get("pastor_logged_in"):
        return (session.get("pastor_name") or "").strip() or (session.get("pastor_username") or "Pastor")
    if session.get("member_logged_in") is True:
        return str(session.get("name") or session.get("username") or "Member").strip()
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
    if session.get("member_logged_in") is True:
        return str(session.get("church_id") or session.get("church_address") or "").strip()
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
        raise RuntimeError("No matching report rows to approve.")

    client = get_gs_client()
    sh = client.open("District4 Data")
    ws = sh.worksheet("Report")

    values = ws.get_all_values()
    if not values:
        raise RuntimeError("The Report sheet has no headers.")
    headers = values[0]
    idx_status = _find_col(headers, "status")
    if idx_status is None:
        raise RuntimeError("The Report sheet is missing its status header.")

    requests_body = []
    col_letter = chr(ord("A") + idx_status)
    for r in sheet_rows:
        verify_sheet_row(ws, "Report", r, values=values)
        a1 = f"{col_letter}{r}"
        requests_body.append({"range": a1, "values": [[status_label]]})

    ws.batch_update(requests_body)
    queue_sheet_cache_refresh("Report")


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

    source_values = ws.get_all_values()
    for r in sheet_rows:
        verify_sheet_row(ws, "Report", r, values=source_values)
    # Delete from bottom to top so row numbers stay correct
    for r in sheet_rows:
        if r > 1:  # never delete header row
            ws.delete_rows(r)
            queue_sheet_cache_refresh("Report")


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
    queue_sheet_cache_refresh("Report")



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
    record_website_event("Prayer request submitted", "Prayer request submitted: "+str(request_id))


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
        raise RuntimeError("The prayer request is missing from the cache.")

    sheet_row = int(cached["sheet_row"])

    client = get_gs_client()
    sh = client.open("District4 Data")
    ws = sh.worksheet(PRAYER_SHEET_NAME)

    values = ws.get_all_values()
    if not values:
        raise RuntimeError("The PrayerRequest sheet has no headers.")
    headers = values[0]

    verify_sheet_row(ws, "PrayerRequest", sheet_row, values=values)
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
            raise RuntimeError(f"The PrayerRequest sheet is missing its {header} header.")
        col_letter = chr(ord("A") + idx)
        body.append({"range": f"{col_letter}{sheet_row}", "values": [[v]]})

    if not body:
        raise RuntimeError("The PrayerRequest sheet is missing the required update headers.")

    ws.batch_update(body)
    record_website_event("Prayer request updated", "Prayer request updated: "+str(request_id), details={"fields": [key for key in updates if key != "request_text"]})
    return True


def _delete_prayer_request_row_in_sheet(request_id):
    db = get_db()
    cached = db.execute(
        "SELECT sheet_row FROM sheet_prayer_request_cache WHERE request_id = ?",
        (request_id,),
    ).fetchone()
    if not cached or not cached["sheet_row"]:
        raise RuntimeError("The prayer request is missing from the cache.")

    sheet_row = int(cached["sheet_row"])
    if sheet_row <= 1:
        raise RuntimeError("Invalid prayer request row.")

    client = get_gs_client()
    sh = client.open("District4 Data")
    ws = sh.worksheet(PRAYER_SHEET_NAME)

    verify_sheet_row(ws, "PrayerRequest", sheet_row)
    ws.delete_rows(sheet_row)
    record_website_event("Prayer request deleted", "Prayer request deleted: "+str(request_id))
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

def get_verse_of_the_day(today=None):
    today = today or datetime.now(PH_TZ).date()
    reference = get_daily_entry(today)["reference"]
    verse_text = get_indexed_niv_passage(
        reference, app.config['BIBLE_STUDY_DATABASE'], static_folder=app.static_folder,
        protected_paths=(DATABASE, AI_DATABASE),
    )
    return reference, verse_text or "Today's NIV verse is temporarily unavailable."


# ========================
# Auth helpers
# ========================


def ao_logged_in():
    return session.get("ao_logged_in") is True


def pastor_logged_in():
    return session.get("pastor_logged_in") is True


def any_user_logged_in():
    return (pastor_logged_in() or ao_logged_in()
            or session.get("member_logged_in") is True and bool(str(session.get("username") or "").strip()))


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
        local_exists = cursor.fetchone() is not None
        cached_exists = db.execute("SELECT 1 FROM sheet_accounts_cache WHERE LOWER(TRIM(username)) = LOWER(?)", (username,)).fetchone()
        if not local_exists and not cached_exists:
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
app.config.update(
    DATABASE=DATABASE,
    AI_DATABASE=AI_DATABASE,
    BIBLE_STUDY_LIBRARY_DATABASE=DATABASE,
    BIBLE_STUDY_DRIVE_FOLDER_ID=os.environ.get("BIBLE_STUDY_DRIVE_FOLDER_ID", ""),
    BIBLE_STUDY_DATABASE=os.environ.get(
        "BIBLE_STUDY_DATABASE", str(Path(data_path("bible_study_data")) / "bible_study.db")),
)

@app.before_request
def start_request_timer():
    g.request_started_at = time.perf_counter()


@app.after_request
def log_slow_request(response):
    started = getattr(g, 'request_started_at', None)
    if started is None:
        return response
    elapsed = time.perf_counter() - started
    if request.endpoint != 'healthz' and elapsed >= app.config.get('SLOW_REQUEST_SECONDS', 1.0):
        # Endpoint names avoid logging chat messages, query strings or credentials.
        app.logger.warning('slow_request endpoint=%s method=%s status=%s seconds=%.3f',
                           request.endpoint, request.method, response.status_code, elapsed)
    return response


register_area_progress_monitor(app)
register_church_progress(app)
register_schedule_routes(app)
register_thanksgiving_pledges(app)
register_temp_edit_routes(app)
register_church_finder_routes(app)
register_pastor_resources_routes(app)
register_bible_study_routes(app)
register_mobile_bible_routes(app)
register_sermon_ebooks_routes(app)
register_ai_assistant(app)
register_developer_routes(app)

def init_website_activity_tables(db):
    db.execute("""CREATE TABLE IF NOT EXISTS website_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT, created_at TEXT NOT NULL,
        actor_key TEXT NOT NULL, actor_username TEXT NOT NULL, actor_name TEXT NOT NULL,
        actor_role TEXT NOT NULL, action TEXT NOT NULL, description TEXT NOT NULL,
        page_path TEXT NOT NULL, is_notification INTEGER NOT NULL DEFAULT 0,
        read_at TEXT, event_key TEXT UNIQUE, details_json TEXT NOT NULL DEFAULT '{}')""")
    db.execute("CREATE INDEX IF NOT EXISTS idx_website_events_actor ON website_events(actor_key,id DESC)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_website_events_unread ON website_events(is_notification,read_at,id)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_website_events_created ON website_events(created_at)")
    db.execute('''CREATE TABLE IF NOT EXISTS daily_devotion_posts (
        day TEXT PRIMARY KEY, title TEXT NOT NULL, message TEXT NOT NULL,
        verse_reference TEXT NOT NULL, verse_text TEXT NOT NULL, source_title TEXT NOT NULL,
        source_passage TEXT NOT NULL DEFAULT '')''')
    if 'source_passage' not in {row[1] for row in db.execute('PRAGMA table_info(daily_devotion_posts)')}:
        db.execute("ALTER TABLE daily_devotion_posts ADD COLUMN source_passage TEXT NOT NULL DEFAULT ''")
    if 'plan_key' not in {row[1] for row in db.execute('PRAGMA table_info(daily_devotion_posts)')}:
        db.execute("ALTER TABLE daily_devotion_posts ADD COLUMN plan_key TEXT NOT NULL DEFAULT ''")
    db.execute('''CREATE TABLE IF NOT EXISTS daily_devotion_reactions (
        day TEXT NOT NULL REFERENCES daily_devotion_posts(day), actor_key TEXT NOT NULL,
        kind TEXT NOT NULL CHECK(kind IN ('like','share')), actor_name TEXT NOT NULL,
        church_name TEXT NOT NULL, created_at TEXT NOT NULL, method TEXT NOT NULL DEFAULT '',
        PRIMARY KEY(day,actor_key,kind))''')
    db.execute('''CREATE TABLE IF NOT EXISTS daily_devotion_comments (
        id INTEGER PRIMARY KEY AUTOINCREMENT, day TEXT NOT NULL REFERENCES daily_devotion_posts(day),
        actor_key TEXT NOT NULL, actor_name TEXT NOT NULL, church_name TEXT NOT NULL,
        content TEXT NOT NULL, created_at TEXT NOT NULL)''')
    db.execute('CREATE INDEX IF NOT EXISTS idx_devotion_comments_day ON daily_devotion_comments(day,id)')
    db.execute('''CREATE TABLE IF NOT EXISTS pastor_daily_reminders (
        actor_key TEXT NOT NULL, day TEXT NOT NULL, shown_at TEXT NOT NULL,
        PRIMARY KEY(actor_key,day))''')


def website_owner():
    return session.get("ao_logged_in") is True and str(session.get("ao_username") or "").strip().casefold() == "pijeme"


def website_actor():
    # An AO may temporarily select a pastor. Always attribute to the login.
    if session.get("ao_logged_in"):
        username = session.get("ao_username") or session.get("username")
        name, role = session.get("ao_name"), session.get("ao_role") or "Area Overseer"
    elif session.get("pastor_logged_in"):
        username = session.get("username") or session.get("pastor_username")
        name, role = session.get("name") or session.get("pastor_name"), "Pastor"
    elif session.get("member_logged_in"):
        username = session.get("username")
        name, role = session.get("name"), "Member"
    else:
        return {"key": "guest", "username": "", "name": "Guest", "role": "Guest"}
    username = str(username or "").strip()[:150]
    return {"key": username.casefold(), "username": username, "name": str(name or username)[:150], "role": str(role)[:80]}


def record_website_event(action, description, *, notification=True, page_path="", event_key=None, details=None):
    """Queue confirmed actions; the response hook persists them after application writes.

    Never capture entire forms, credentials, photos, private notes or AI prompts.
    """
    if not has_request_context():
        return
    actor = website_actor()
    if actor['key'] == 'pijeme':
        return
    events = getattr(g, "website_events", None)
    if events is None:
        events = g.website_events = []
    events.append((action[:80], str(description)[:500], notification, page_path or request.path,
                   event_key, details or {}))


# Explicit allowlist of user mutations. Success requires JSON ok=true or a new
# success flash; merely receiving a redirect never counts as a successful change.
WEBSITE_CHANGE_ACTIONS = {
    "ao_edit_account_save": "Account updated", "ao_edit_account_delete": "Account deleted",
    "ao_announcement_create": "Announcement created", "ao_announcement_update": "Announcement updated",
    "ao_announcement_delete": "Announcement deleted", "sunday_detail": "Sunday report updated",
    "church_progress_view": "Church progress updated", "ao_aopt_submit": "AOPT updated",
    "ao_church_status_approve": "Church report approved", "pastor_tool": "Pastor report updated",
    "prayer_request_write": "Prayer request submitted", "prayer_request_edit": "Prayer request updated",
    "prayer_request_delete": "Prayer request deleted", "prayer_request_mark_answered": "Prayer marked answered",
    "ao_prayer_requests_approve": "Prayer request approved", "ao_prayer_requests_reject": "Prayer request rejected",
    "ao_prayer_requests_approve_all": "Prayer requests approved", "bulletin_pray": "Prayer support updated",
    "pledge_submit": "Pledge submitted", "pledge_decision": "Pledge reviewed",
    "temp_edit": "Account edit submitted", "temp_edit_admin": "Account edit reviewed",
    "church_progress.member_create": "Member account created", "church_progress.member_update": "Member account updated",
    "church_progress.member_delete": "Member account deleted",
    "area_progress_monitor.area_progress_monitor_save_remarks": "Church remarks updated",
    "pastor_resources_admin_edit": "Book details updated", "pastor_resources_admin_recommend": "Book recommendation updated",
    "pastor_resources_admin_hide": "Book hidden", "pastor_resources_admin_restore": "Book restored",
    "pastor_resources_favorite": "Book favorite updated", "pastor_resources_completed": "Book completion updated",
    "pastor_resources_bookmarks": "Book bookmark added", "pastor_resources_delete_bookmark": "Book bookmark deleted",
    "pastor_resources_annotations": "Book annotation added", "pastor_resources_annotation_item": "Book annotation changed",
    "sermon_ebooks_edit": "Sermon details updated", "sermon_ebooks_delete": "Sermon hidden",
    "sermon_ebooks_restore": "Sermon restored", "sermon_ebooks_annotations": "Sermon annotation added",
    "sermon_ebooks_annotation_update": "Sermon annotation updated", "sermon_ebooks_annotation_delete": "Sermon annotation deleted",
    "sermon_ebooks_bookmarks": "Sermon bookmark added", "sermon_ebooks_bookmark_delete": "Sermon bookmark deleted",
    "bible_study.study_save_create": "Bible study saved", "bible_study.study_saved_item_api": "Saved Bible study changed",
    "bible_study.annotation_create": "Bible annotation added", "bible_study.bookmark_create": "Bible bookmark added",
    "bible_study.personal_item_api": "Bible annotation or bookmark changed",
    "pastor_resources_sync_books_live": "Book synchronization requested", "pastor_resources_sync_books": "Books synchronized",
    "pastor_resources_sync_books_cancel": "Book synchronization cancellation requested",
    "sermon_ebooks_sync": "Sermon synchronization requested", "bible_study.sync_start": "Bible synchronization requested",
}

# Personal reading marks stay in the readers; they do not notify the developer.
WEBSITE_QUIET_ACTIONS = {label for endpoint, label in WEBSITE_CHANGE_ACTIONS.items()
                       if any(word in endpoint for word in ('annotation', 'bookmark', 'study_save', 'study_saved', 'personal_item'))}
WEBSITE_CHANGE_ACTIONS = {endpoint: label for endpoint, label in WEBSITE_CHANGE_ACTIONS.items()
                          if label not in WEBSITE_QUIET_ACTIONS}


@app.context_processor
def website_activity_context():
    owner = website_owner()
    if owner and "website_activity_csrf" not in session:
        session["website_activity_csrf"] = secrets.token_urlsafe(32)
    district_user = any_user_logged_in() or session.get('member_logged_in') is True
    if district_user and 'district_interaction_csrf' not in session:
        session['district_interaction_csrf'] = secrets.token_urlsafe(32)
    return {"website_activity_owner": owner, "website_activity_csrf": session.get("website_activity_csrf", "") if owner else "",
            'district_interaction_csrf': session.get('district_interaction_csrf', '') if district_user else '',
            'pastor_reminder_enabled': pastor_logged_in() and not ao_logged_in(),
            'developer_working_area': session.get('ao_area_number', '') if owner else '',
            'developer_home_area': session.get('developer_home_area', session.get('ao_area_number', '')) if owner else ''}


@app.after_request
def collect_website_activity(response):
    if (response.direct_passthrough or request.path.startswith("/ai/")
            or request.endpoint in {'healthz','event_progress','event_thumbnail','event_opening_status'} or request.blueprint == 'bible_mobile'):
        return response
    actor = website_actor()
    old_actor = getattr(g, "website_actor", actor)
    if not actor["username"] and old_actor["username"]:
        actor = old_actor
    if actor['key'] == 'pijeme':
        return response
    if 200 <= response.status_code < 400:
        endpoint = request.endpoint or ""
        mutating = request.method in {"POST", "PATCH", "DELETE", "PUT"}
        payload = response.get_json(silent=True) if response.is_json else None
        previous = getattr(g, "website_flashes_before", [])
        new_flashes = list(session.get("_flashes", []))
        # Ignore previously displayed flashes from earlier requests.
        for item in previous:
            if item in new_flashes:
                new_flashes.remove(item)
        success = (isinstance(payload, dict) and payload.get("ok") is True) or any(c == "success" for c, _ in new_flashes)
        action = WEBSITE_CHANGE_ACTIONS.get(endpoint)
        if mutating and action and success and not getattr(g, "website_events", None):
            data = request.get_json(silent=True) if request.is_json else request.form
            data = data if isinstance(data, dict) or hasattr(data, "get") else {}
            # Only short, non-sensitive subject labels are retained.
            subject = next((str(data.get(k))[:150] for k in ("original_username", "username", "full_name", "name", "title", "submission_id", "request_id") if data.get(k)), "")
            if endpoint == "pledge_decision":
                subject = str(data.get("decision") or "")[:40] + " " + str(data.get("submission_id") or "")[:80]
            details = {}
            if getattr(g, "website_before", None):
                aliases = {"name":"full_name", "contact":"contact_number"}
                details["before"] = g.website_before
                if endpoint != "ao_edit_account_delete":
                    details["after"] = {key: str(data.get(aliases.get(key,key), value))[:150] for key,value in g.website_before.items()}
            record_website_event(action, action+(": "+subject if subject else ""), details=details,
                                 event_key=("pledge:"+str(payload.get("reference"))) if endpoint == "pledge_submit" and isinstance(payload, dict) and payload.get("reference") else None,
                                 page_path=request.path)
        if endpoint in {"splash", "pastor_login", "ao_login"} and request.method == "POST" and actor["username"] and response.status_code in (302,303):
            record_website_event("login", "Signed in", notification=False, page_path="/bulletin")
        elif endpoint == "logout" and actor["username"]:
            record_website_event("logout", "Signed out", notification=False, page_path="/logout")
        if (request.method == "GET" and response.status_code == 200 and response.mimetype == "text/html"
                and actor["username"] and not request.path.startswith("/ao-tool/activity")
                and not request.path.startswith("/static/") and endpoint not in {"logout"}):
            record_website_event("visit", "Opened "+_page_label_from_path(request.path), notification=False)
    events = getattr(g, "website_events", [])
    if events:
        try:
            with closing(sqlite3.connect(DATABASE, timeout=1)) as db, db:
                now = utc_now_iso()
                for action, description, notification, path, event_key, details in events:
                    db.execute("""INSERT OR IGNORE INTO website_events
                        (created_at,actor_key,actor_username,actor_name,actor_role,action,description,page_path,is_notification,event_key,details_json)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?)""", (now,actor["key"],actor["username"],actor["name"],actor["role"],action,description,path[:500],int(notification),event_key,json.dumps(details,ensure_ascii=False)))
                    if action == "visit":
                        # Preserve the existing developer visit dashboard too.
                        db.execute("""INSERT INTO developer_visit_logs
                            (created_at,ip_address,user_agent,actor_username,actor_name,actor_role,request_path,page_label,method)
                            VALUES (?,?,?,?,?,?,?,?,?)""", (now,request.remote_addr or "",str(request.user_agent)[:300],actor["username"],actor["name"],actor["role"],path[:500],description.removeprefix("Opened "),"GET"))
                # Indexed time comparisons, at most hourly per process.
                global _website_last_cleanup
                if time.monotonic()-_website_last_cleanup > 3600:
                    cutoff = datetime.fromtimestamp(time.time()-90*86400, timezone.utc).isoformat()
                    db.execute("DELETE FROM website_events WHERE is_notification=0 AND created_at < ?", (cutoff,))
                    db.execute("DELETE FROM developer_visit_logs WHERE created_at < ?", (cutoff,))
                    _website_last_cleanup = time.monotonic()
        except sqlite3.Error:
            app.logger.exception("Website activity could not be persisted endpoint=%s", request.endpoint)
    return response


_website_last_cleanup = 0.0


def _require_website_owner(write=False):
    if not website_owner():
        abort(403)
    if write:
        supplied = request.headers.get("X-CSRF-Token", "") or request.form.get("csrf_token", "")
        expected = session.get("website_activity_csrf", "")
        if not expected or not hmac.compare_digest(str(supplied), expected):
            abort(400)


def _website_date_filter():
    clauses, values = [], []
    for field, operator in (("start", ">="), ("end", "<=")):
        value = request.args.get(field, "")
        if value:
            try:
                parsed = date.fromisoformat(value)
            except ValueError:
                abort(400)
            # UTC+8 local calendar boundaries, stored as UTC ISO timestamps.
            from datetime import timedelta
            stamp = datetime(parsed.year, parsed.month, parsed.day, tzinfo=PH_TZ or timezone(timedelta(hours=8)))
            if field == "end":
                stamp += timedelta(days=1)
                operator = "<"
            clauses.append("created_at "+operator+" ?")
            values.append(stamp.astimezone(timezone.utc).isoformat())
    return clauses, values


@app.get("/ao-tool/activity")
def website_activity_page():
    _require_website_owner()
    return render_template("activity_updates.html")


@app.get("/ao-tool/activity/api")
def website_activity_api():
    _require_website_owner()
    db = get_db()
    view = request.args.get("view", "accounts")
    clauses, values = _website_date_filter()
    excluded = sorted(WEBSITE_QUIET_ACTIONS)
    clauses.extend(["actor_key<>?", "action NOT IN (" + ','.join('?' for _ in excluded) + ")"])
    values.extend(['pijeme', *excluded])
    page = max(1, min(request.args.get("page", 1, type=int) or 1, 100000))
    limit = 30
    if view == "summary":
        row = db.execute("SELECT COUNT(*),COALESCE(MAX(id),0) FROM website_events WHERE "
                         + " AND ".join(clauses) + " AND is_notification=1 AND read_at IS NULL", values).fetchone()
        return jsonify(ok=True, unread=row[0], latest_id=row[1])
    if view == "notifications":
        clauses.append("is_notification=1")
    elif view == "activities":
        clauses.append("actor_key=?")
        values.append(request.args.get("account", ""))
    elif view != "accounts":
        abort(400)
    query = request.args.get("q", "").strip()[:100]
    if query:
        clauses.append("(instr(lower(actor_username),lower(?))>0 OR instr(lower(actor_name),lower(?))>0)")
        values.extend((query,query))
    # Activity retention is enforced on reads even before hourly cleanup runs.
    cutoff = datetime.fromtimestamp(time.time()-90*86400, timezone.utc).isoformat()
    if view in {"accounts", "activities"}:
        clauses.append("(is_notification=1 OR created_at>=?)")
        values.append(cutoff)
    where = " WHERE "+" AND ".join(clauses) if clauses else ""
    if view == "accounts":
        count = db.execute("SELECT COUNT(DISTINCT actor_key) FROM website_events"+where, values).fetchone()[0]
        rows = db.execute("""SELECT actor_key,MAX(actor_username) AS actor_username,MAX(actor_name) AS actor_name,
            MAX(actor_role) AS actor_role,MAX(created_at) AS last_activity,COUNT(*) AS activity_count
            FROM website_events"""+where+" GROUP BY actor_key ORDER BY last_activity DESC LIMIT ? OFFSET ?", (*values,limit,(page-1)*limit)).fetchall()
    else:
        count = db.execute("SELECT COUNT(*) FROM website_events"+where, values).fetchone()[0]
        rows = db.execute("SELECT * FROM website_events"+where+" ORDER BY id DESC LIMIT ? OFFSET ?", (*values,limit,(page-1)*limit)).fetchall()
    items = []
    for row in rows:
        item = dict(row)
        if "details_json" in item:
            item["details"] = json.loads(item.pop("details_json"))
            item.pop("event_key", None)
        items.append(item)
    return jsonify(ok=True,items=items,page=page,pages=max(1,(count+limit-1)//limit),total=count)


@app.post("/ao-tool/activity/read")
def website_activity_read():
    _require_website_owner(write=True)
    data = request.get_json(silent=True) or {}
    ids = data.get("ids") if isinstance(data,dict) else None
    if not isinstance(ids,list) or len(ids)>30 or any(type(value) is not int or value<1 for value in ids):
        abort(400)
    if ids:
        placeholders = ",".join("?" for _ in ids)
        get_db().execute("UPDATE website_events SET read_at=? WHERE is_notification=1 AND read_at IS NULL AND id IN ("+placeholders+")", (utc_now_iso(),*ids))
        get_db().commit()
    return jsonify(ok=True)


@app.post("/ao-tool/activity/clear")
def website_activity_clear():
    _require_website_owner(write=True)
    get_db().execute("DELETE FROM website_events")
    get_db().execute("DELETE FROM developer_visit_logs")
    get_db().commit()
    return jsonify(ok=True)


@app.post("/activity/ebook")
def website_ebook_activity():
    if not (any_user_logged_in() or session.get("member_logged_in")):
        abort(401)
    if request.headers.get("Sec-Fetch-Site") == "cross-site":
        abort(403)
    data = request.get_json(silent=True) or {}
    if not isinstance(data,dict) or data.get("action") not in {"download", "share", "copy_link"}:
        abort(400)
    path = urllib.parse.urlsplit(str(data.get("url") or "")).path
    match = re.fullmatch(r"/pastor-resources/(download|read)/(\d+)", path)
    sermon = re.fullmatch(r"/pastor-resources/sermon-ebooks/(download|read)/(\d+)", path)
    if match:
        from pastor_resources import get_effective_book, is_resource_admin
        book = get_effective_book(int(match[2]), include_hidden=is_resource_admin())
        if not book:
            abort(404)
        title = book.get("title") or "Ebook"
    elif sermon:
        from sermon_ebooks import get_sermon_row, is_sermon_admin
        if not is_sermon_admin():
            abort(403)
        book = get_sermon_row(int(sermon[2]))
        if not book:
            abort(404)
        title = dict(book).get("title") or dict(book).get("filename") or "Sermon ebook"
    else:
        abort(400)
    descriptions = {"download": "Started saving ebook", "share": "Shared ebook through device share menu", "copy_link": "Copied ebook link"}
    record_website_event(data["action"], descriptions[data["action"]]+": "+str(title), notification=False, page_path=path)
    return jsonify(ok=True)


@app.template_filter("phpeso")
def phpeso_filter(value):
    return format_php_currency(value)


def _cleanup_old_visit_logs():
    cutoff = datetime.fromtimestamp(time.time()-90*86400, timezone.utc).isoformat()
    get_db().execute("DELETE FROM developer_visit_logs WHERE created_at < ?", (cutoff,))
    get_db().execute("DELETE FROM website_events WHERE is_notification=0 AND created_at < ?", (cutoff,))
    get_db().commit()


def _page_label_from_path(path: str):
    path = str(path or "")
    if path == "/":
        return "Splash/Login"
    mappings = [
        ("/pastor-resources/bible-study", "Bible Study"),
        ("/pastor-resources/sermon-ebooks", "Sermon eBooks"),
        ("/pastor-resources/read/", "Ebook Reader"),
        ("/pastor-resources/my-library", "My Library"),
        ("/pastor-resources", "Pastor's Resources"),
        ("/thanksgiving-pledges", "Thanksgiving Pledges"),
        ("/church-progress", "Church Progress"),
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
    # Successful HTML visits are recorded in the response hook below.
    return


def _record_pastor_login_event(row):
    # Credentials have already been verified. Optional history must not turn
    # a successful login into a 500 response with an authenticated session.
    try:
        _insert_pastor_login_event(row)
    except sqlite3.Error:
        get_db().rollback()
        app.logger.exception("Could not record pastor login history")


def _insert_pastor_login_event(row):
    """Record ONLY real Pastor account logins.

    This is intentionally separate from developer_visit_logs so AO actions,
    opening Pastor Tool for a church, and normal page visits are not mistaken
    as pastor login notifications.
    """
    if not row:
        return

    role = str((row["position"] if "position" in row.keys() else "Pastor") or "Pastor").strip()
    role_key = role.lower()

    # Only real Pastor accounts should create pastor-login notifications.
    # Member, AO, Sub Area Overseer, DO, and AO-selected pastor tool views are ignored.
    if role_key and role_key != "pastor":
        return

    username = str((row["username"] if "username" in row.keys() else "") or "").strip()
    if not username:
        return

    now_iso = utc_now_iso()
    ip_address = str(request.headers.get("X-Forwarded-For") or request.remote_addr or "").split(",")[0].strip()
    user_agent = str(request.headers.get("User-Agent") or "").strip()

    # Prevent double inserts if the same browser resubmits/refreshes quickly.
    recent = get_db().execute(
        """
        SELECT id
        FROM user_login_events
        WHERE username = ?
          AND datetime(created_at) >= datetime(?, '-90 seconds')
        ORDER BY id DESC
        LIMIT 1
        """,
        (username, now_iso),
    ).fetchone()
    if recent:
        return

    insert_login_event(
        get_db(),
        {
            "created_at": now_iso,
            "username": username,
            "full_name": str((row["name"] if "name" in row.keys() else "") or "").strip(),
            "role": role or "Pastor",
            "church_id": str((row["sex"] if "sex" in row.keys() else "") or "").strip(),
            "church_address": str((row["church_address"] if "church_address" in row.keys() else "") or "").strip(),
            "area_number": str((row["age"] if "age" in row.keys() else "") or "").strip(),
            "sub_area": str((row["sub_area"] if "sub_area" in row.keys() else "") or "").strip(),
            "ip_address": ip_address,
            "user_agent": user_agent,
        },
    )
    get_db().commit()


@app.before_request
def restrict_member_staff_tools():
    if session.get("member_logged_in") is not True:
        return None
    # Protect all subroutes and JSON endpoints, not just the menu links.
    staff_paths = ("/pastor-tool", "/ao-tool", "/api/ao-tool", "/church-progress", "/do-tool", "/sync-area-data-log")
    if any(request.path == prefix or request.path.startswith(prefix + "/") for prefix in staff_paths):
        abort(403)


@app.before_request
def before_request():
    if request.endpoint == "healthz":
        return
    if request.endpoint in ('event_progress','event_thumbnail','event_opening_status'):
        # Polling and small roster photos never log visits or sync Google.
        return
    if request.blueprint == 'bible_mobile':
        # Bearer-token calls authenticate in their blueprint. Only mobile login
        # refreshes the account mirror; note polling must not trigger Sheets sync
        # or browser-session visit logging before the device is authenticated.
        return
    if request.path == "/api/v2/meal-scanner" or request.path.startswith("/api/v2/meal-scanner/"):
        # Scanner downloads must not trigger Google synchronization or visit logging.
        return
    if request.path.startswith("/ai/"):
        # Chat must not wait on schema writes or bootstrap network calls. The
        # schema is normally ready from the page that opened the chat.
        return
    g.website_flashes_before = list(session.get("_flashes", []))
    g.website_actor = website_actor()
    g.website_before = None
    if request.endpoint in {"ao_edit_account_save", "ao_edit_account_delete"} and request.method == "POST":
        row = get_db().execute("SELECT username,name,church_address,contact,birthday FROM sheet_accounts_cache WHERE username=?",
                               (request.form.get("original_username", ""),)).fetchone()
        g.website_before = dict(row) if row else None
    if request.endpoint not in {'static', 'devotion_interactions', 'website_activity_api', 'website_activity_read'} and not (request.endpoint or '').startswith(('ccm_', 'event_')):
        sync_from_sheets_if_needed()
    _log_visit_if_needed()


@app.get("/healthz")
def healthz():
    # Process liveness must not trigger Sheets sync, schema changes or visit logs.
    return jsonify(status="ok")


@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, "_database", None)
    if db is not None:
        db.close()


@app.after_request
def cache_public_images(response):
    # Only public static images/fonts receive shared browser caching.
    # Private Drive streams and verification photos retain their own headers.
    if request.endpoint == "static" and response.status_code in (200, 304):
        extension = os.path.splitext((request.view_args or {}).get("filename", ""))[1].lower()
        if extension in {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".ico", ".avif", ".woff", ".woff2"}:
            if not response.cache_control.no_store and not response.cache_control.private:
                response.cache_control.public = True
                response.cache_control.max_age = 864000
                response.cache_control.no_cache = None
                response.expires = datetime.fromtimestamp(utc_now().timestamp() + 864000, timezone.utc)
    if request.endpoint in {"temp_edit_selfie", "pledge_selfie", "ao_download_local_db", "ao_full_cache_sync"}:
        response.headers["Cache-Control"] = "private, no-store"
    return response


@app.after_request
def add_page_loading_screen(response):
    """Give standalone HTML pages the same loading UI as base.html pages."""
    if (response.status_code != 200 or response.mimetype != "text/html" or response.is_streamed
            or response.direct_passthrough
            or response.headers.get("Content-Disposition")):
        return response

    html = response.get_data(as_text=True)
    if re.search(r'\bid=[\"\']globalLoadingOverlay[\"\']', html):
        return response
    body = re.search(r"<body\b[^>]*>", html, re.IGNORECASE)
    if not body:
        return response

    # The legacy login, account edit, map export, log and pending pages do
    # not extend base.html. Reuse its UI macros without adding project files.
    head_ui = render_template_string(
        "{% from 'ui_icons.html' import loading_head %}{{ loading_head() }}"
    )
    body_ui = render_template_string(
        "{% from 'ui_icons.html' import loading_overlay %}{{ loading_overlay() }}"
    )
    html = html[:body.end()] + body_ui + html[body.end():]
    head = re.search(r"<head\b[^>]*>", html, re.IGNORECASE)
    head_end = re.search(r"</head\s*>", html, re.IGNORECASE)
    if head and head_end:
        # Keep standalone pages' own styles after the common stylesheet so
        # their login, map and account-edit layouts retain precedence.
        asset = re.search(r"<(?:link|style|script)\b",
                          html[head.end():head_end.start()], re.IGNORECASE)
        position = head.end() + asset.start() if asset else head_end.start()
        html = html[:position] + head_ui + html[position:]
    else:
        # The developer log is a minimal document without a head element.
        body = re.search(r"<body\b[^>]*>", html, re.IGNORECASE)
        html = html[:body.start()] + "<head>" + head_ui + "</head>" + html[body.start():]
    response.set_data(html)
    return response


@app.route("/", methods=["GET", "POST"])
def splash():
    """Splash page login. Role is auto-detected from Accounts cache."""

    logged_in = bool(session.get("pastor_logged_in")) or bool(session.get("ao_logged_in")) or bool(session.get("member_logged_in"))
    error = None

    # If a user is already logged in and opens the main website, show Bulletin Board first.
    if request.method == "GET" and logged_in:
        return redirect(url_for("bulletin"))

    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = (request.form.get("password") or "").strip()

        if not username or not password:
            error = "Username and password are required."
        else:
            # Website account changes refresh this mirror; manual Sheet edits need developer sync.
            sync_from_sheets_if_needed()
            row = get_db().execute(
                """
                SELECT username, password, name, church_address, sex, age, position, sub_area, contact, birthday
                FROM sheet_accounts_cache
                WHERE LOWER(TRIM(username)) = LOWER(TRIM(?))
                """,
                (username,),
            ).fetchone()

            if row and str(row["password"] or "").strip() == password:
                username = str(row["username"] or "").strip()
                session.clear()
                session.permanent = True

                real_role = str(row["position"] or "").strip()
                role_key = real_role.lower()

                # Common session fields for all account types
                session["username"] = username
                session["pledge_actor"] = username
                session["name"] = row["name"] or ""
                session["church_address"] = row["church_address"] or ""
                session["church_id"] = (row["sex"] or "").strip()
                session["area"] = (row["age"] or "").strip()
                session["contact"] = row["contact"] if "contact" in row.keys() else ""
                session["birthday"] = row["birthday"] if "birthday" in row.keys() else ""
                session["selected_position"] = role_key

                # Normalized role used by base.html menu filtering
                if role_key == "pastor":
                    session["role"] = "Pastor"
                    session["pastor_logged_in"] = True
                    session["pastor_username"] = username
                    session["pastor_name"] = row["name"] or ""
                    session["pastor_church_address"] = row["church_address"] or ""
                    session["pastor_church_id"] = (row["sex"] or "").strip()
                    session["pastor_area_number"] = (row["age"] or "").strip()
                    _record_pastor_login_event(row)
                    return redirect(url_for("bulletin"))

                if role_key == "member":
                    session["role"] = "Member"
                    session["member_logged_in"] = True
                    return redirect(url_for("bulletin"))

                if role_key in ("area overseer", "ao", "sub area overseer", "district overseer", "do"):
                    if role_key in ("district overseer", "do"):
                        session["role"] = "DO"
                    elif role_key == "sub area overseer":
                        session["role"] = "Sub Area Overseer"
                    else:
                        session["role"] = "AO"

                    session["ao_logged_in"] = True
                    session["ao_username"] = username
                    session["ao_name"] = row["name"] or ""
                    session["ao_area_number"] = (row["age"] or "").strip()
                    session["ao_church_id"] = (row["sex"] or "").strip()
                    session["ao_role"] = real_role
                    session["ao_sub_area"] = (row["sub_area"] or "").strip() if "sub_area" in row.keys() else ""
                    return redirect(url_for("bulletin"))

                # Unknown/blank role: login but keep safest limited access.
                session["role"] = real_role or "Member"
                session["member_logged_in"] = True
                return redirect(url_for("bulletin"))

            elif row is None:
                # Accounts owns its username namespace. Never let a member row
                # override an existing staff account with the same login name.
                members = get_db().execute(
                    """SELECT username,password,name,bday,church_id,church_address,area_number,pastor
                       FROM sheet_members_account_cache
                       WHERE LOWER(TRIM(username))=LOWER(TRIM(?)) LIMIT 2""", (username,)
                ).fetchall()
                if (len(members) == 1 and str(members[0]["username"] or "").strip()
                        and hmac.compare_digest(str(members[0]["password"] or "").strip().encode(), password.encode())):
                    member = members[0]
                    session.clear()
                    session.permanent = True
                    session.update(
                        username=str(member["username"]).strip(), pledge_actor=str(member["username"]).strip(),
                        name=member["name"] or "", birthday=member["bday"] or "",
                        church_id=member["church_id"] or "", church_address=member["church_address"] or "",
                        area=member["area_number"] or "", member_pastor=member["pastor"] or "",
                        role="Member", selected_position="member", member_logged_in=True,
                    )
                    return redirect(url_for("bulletin"))
                error = "Invalid username or password."
            else:
                error = "Invalid username or password."

    return render_template("splash.html", logged_in=logged_in, error=error)



def pending_page(title):
    return f"""
    <!doctype html>
    <html>
    <head>
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <title>{title}</title>

      <style>
        body {{
          font-family: Arial, sans-serif;
          background: #f8fafc;
          margin: 0;
          padding: 24px;
          color: #1f2937;
          text-align: center;
        }}

        .card {{
          max-width: 520px;
          margin: 80px auto;
          background: #fff;
          border: 1px solid #e5e7eb;
          border-radius: 18px;
          padding: 28px;
          box-shadow: 0 10px 30px rgba(15,23,42,.08);
        }}

        h1 {{
          margin-top: 0;
        }}

        p {{
          color: #64748b;
        }}

        a {{
          display: inline-block;
          margin-top: 18px;
          background: #2563eb;
          color: white;
          text-decoration: none;
          padding: 12px 18px;
          border-radius: 10px;
          font-weight: 700;
        }}
      </style>
    </head>

    <body>
      <div class="card">
        <h1>{title}</h1>

        <p>
          This feature is still pending and will be available soon.
        </p>

        <a href="{url_for('bulletin')}">
          Back to Bulletin Board
        </a>
      </div>
    </body>
    </html>
    """


@app.route('/church-progress')
def church_progress_redirect():
    if session.get("pastor_logged_in"):
        church_id = (session.get("pastor_church_id") or "").strip()
        if not church_id:
            refresh_pastor_from_cache()
            church_id = (session.get("pastor_church_id") or "").strip()

        if church_id:
            return redirect(url_for("church_progress.church_progress", church_id=church_id))

    if session.get("ao_logged_in"):
        church_id = (session.get("ao_church_id") or "").strip()
        if church_id:
            return redirect(url_for("church_progress.church_progress", church_id=church_id))

    return pending_page("Church Progress")


@app.route('/do-tool')
def do_tool():
    return pending_page("DO Tool")


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

    # The original AO/DO login is authoritative even while Pastor's Tool
    # is displaying/editing a selected pastor's report.
    if ao_logged_in():
        return (session.get("ao_area_number") or "").strip()

    if pastor_logged_in():
        username = (session.get("pastor_username") or "").strip()
        if not username:
            return ""
        row = db.execute(
            "SELECT age FROM sheet_accounts_cache WHERE username = ?",
            (username,),
        ).fetchone()
        return str(row["age"] or "").strip() if row else ""

    if session.get('member_logged_in'):
        return str(session.get('area') or '').strip()

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
    if not (any_user_logged_in() or session.get('member_logged_in')):
        return redirect(url_for("pastor_login", next=request.path))

    sync_from_sheets_if_needed(force=False)

    today = datetime.now(PH_TZ).date()
    reference, text = get_verse_of_the_day(today=today)
    leadership_insight, insight_pending = daily_maxwell_insight(
        app.config['BIBLE_STUDY_DATABASE'], AI_DATABASE, reference, text, today.isoformat())
    devotion_plan = get_daily_entry(today)
    leadership_insight = compose_devotional(devotion_plan, leadership_insight)
    leadership_insight = _save_daily_devotion(today.isoformat(), reference, text, leadership_insight,
                                             plan_key=devotion_plan['key'])
    selected_day = request.args.get('devotion_day', today.isoformat())
    archived = get_db().execute('SELECT * FROM daily_devotion_posts WHERE day=?', (selected_day,)).fetchone()
    if archived and selected_day != today.isoformat():
        today = date.fromisoformat(selected_day)
        reference, text = archived['verse_reference'], archived['verse_text']
        leadership_insight = _devotion_insight(archived)
        insight_pending = False
        devotion_plan = None
    today_str = today.strftime("%B %d, %Y")
    area_number = _current_user_area_number()
    posts, current_church = build_bulletin_board_posts()

    return render_template(
        "bulletin.html",
        verse_reference=reference,
        verse_text=text,
        verse_day=today.isoformat(),
        leadership_insight=leadership_insight,
        insight_pending=insight_pending,
        devotion_plan=devotion_plan,
        bible_study_link=url_for('bible_study.page', **bible_study_passage(reference)),
        today_str=today_str,
        area_number=area_number,
        posts=posts,
        current_church=current_church,
        developer_areas=_developer_area_choices() if website_owner() else [],
    )

@app.route("/bulletin/leadership-insight")
def bulletin_leadership_insight():
    if not (any_user_logged_in() or session.get('member_logged_in')):
        return jsonify(ok=False, error="Login required."), 401
    today = datetime.now(PH_TZ).date()
    reference, text = get_verse_of_the_day(today=today)
    insight, pending = daily_maxwell_insight(
        app.config['BIBLE_STUDY_DATABASE'], AI_DATABASE, reference, text, today.isoformat())
    entry = get_daily_entry(today)
    insight = _save_daily_devotion(today.isoformat(), reference, text, compose_devotional(entry, insight),
                                   plan_key=entry['key'])
    response = jsonify(ok=True, insight=insight, pending=pending, reference=reference, day=today.isoformat())
    response.headers['Cache-Control'] = 'private, no-store'
    return response


def _district_today():
    return datetime.now(PH_TZ or timezone(timedelta(hours=8))).date()


def _require_district_user(write=False):
    if not (any_user_logged_in() or session.get('member_logged_in') is True):
        abort(401)
    if write:
        expected = session.get('district_interaction_csrf', '')
        supplied = request.headers.get('X-CSRF-Token', '') or request.form.get('csrf_token', '')
        if not expected or not hmac.compare_digest(str(expected), str(supplied)):
            abort(400)


def _developer_area_choices():
    rows = get_db().execute("SELECT DISTINCT TRIM(age) FROM sheet_accounts_cache WHERE TRIM(COALESCE(age,''))<>''").fetchall()
    return sorted((row[0] for row in rows), key=lambda value: (not value.isdigit(), int(value) if value.isdigit() else value))


@app.post('/bulletin/working-area')
def developer_working_area():
    _require_website_owner(write=True)
    selected = str(request.form.get('area') or '').strip()
    if selected not in _developer_area_choices():
        abort(400)
    session.setdefault('developer_home_area', session.get('ao_area_number', ''))
    session['ao_area_number'] = selected
    # Every existing area guard uses this working scope; the Accounts row stays intact.
    for key in tuple(session):
        if key.startswith(('pastor_', 'dirty_')):
            session.pop(key, None)
    return redirect(url_for('bulletin'))


def _devotion_insight(post):
    body = post['message']
    return dict(title=post['title'], content=body, excerpt=body.split('\n', 1)[0][:600],
                passage=post['source_passage'] or post['verse_reference'], source_title=post['source_title'])


def _save_daily_devotion(day, reference, verse_text, insight, plan_key=''):
    """Publish the current plan without deleting its district conversation.

    Archived entries stay unchanged. A ready Maxwell note may enrich today's
    prepared reflection; a later source outage must not replace it with fallback.
    """
    db = get_db()
    post = db.execute('SELECT * FROM daily_devotion_posts WHERE day=?', (day,)).fetchone()
    if post and (not plan_key or day != datetime.now(PH_TZ).date().isoformat()):
        return _devotion_insight(post)
    if (post and post['plan_key'] == plan_key and post['source_title'] != ORIGINAL_SOURCE
            and insight and insight['source_title'] == ORIGINAL_SOURCE):
        return _devotion_insight(post)
    if insight:
        values = (day, insight['title'], insight['content'], reference, verse_text,
                  insight.get('source_title') or ORIGINAL_SOURCE, insight.get('passage') or reference, plan_key)
        if not post or tuple(post[key] for key in
                ('day', 'title', 'message', 'verse_reference', 'verse_text', 'source_title', 'source_passage', 'plan_key')) != values:
            db.execute('''INSERT INTO daily_devotion_posts
                (day,title,message,verse_reference,verse_text,source_title,source_passage,plan_key)
                VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(day) DO UPDATE SET
                title=excluded.title,message=excluded.message,verse_reference=excluded.verse_reference,
                verse_text=excluded.verse_text,source_title=excluded.source_title,
                source_passage=excluded.source_passage,plan_key=excluded.plan_key''', values)
            db.commit()
    post = db.execute('SELECT * FROM daily_devotion_posts WHERE day=?', (day,)).fetchone()
    return _devotion_insight(post) if post else None


def _devotion_identity():
    actor = website_actor()
    row = get_db().execute('SELECT name,sex FROM sheet_accounts_cache WHERE lower(trim(username))=?', (actor['key'],)).fetchone()
    return actor['key'], (row['name'] if row else actor['name']) or actor['username'], (row['sex'] if row else session.get('church_id')) or ''


def _devotion_counts(day):
    db = get_db()
    counts = {row['kind']: row['total'] for row in db.execute(
        'SELECT kind,COUNT(*) AS total FROM daily_devotion_reactions WHERE day=? GROUP BY kind', (day,))}
    liked = db.execute("SELECT 1 FROM daily_devotion_reactions WHERE day=? AND actor_key=? AND kind='like'",
                       (day, website_actor()['key'])).fetchone()
    return dict(likes=counts.get('like', 0), shares=counts.get('share', 0),
                comments=db.execute('SELECT COUNT(*) FROM daily_devotion_comments WHERE day=?', (day,)).fetchone()[0],
                liked=bool(liked))


@app.route('/bulletin/devotion/<day>/interactions', methods=['GET', 'POST'])
def devotion_interactions(day):
    _require_district_user(write=request.method == 'POST')
    db = get_db()
    post = db.execute('SELECT * FROM daily_devotion_posts WHERE day=?', (day,)).fetchone()
    if not post:
        return jsonify(ok=False, error='This devotional is not available.'), 404
    kind = request.args.get('kind', 'counts')
    if kind not in {'counts', 'likes', 'shares', 'comments'}:
        abort(400)
    if request.method == 'POST':
        data = request.get_json(silent=True)
        if not isinstance(data, dict):
            abort(400)
        action = data.get('action')
        actor_key, actor_name, church = _devotion_identity()
        now, changed = utc_now_iso(), False
        db.execute('BEGIN IMMEDIATE')
        if action == 'like' and type(data.get('liked')) is bool:
            if data['liked']:
                changed = db.execute('''INSERT OR IGNORE INTO daily_devotion_reactions
                    (day,actor_key,kind,actor_name,church_name,created_at) VALUES(?,?,'like',?,?,?)''',
                    (day, actor_key, actor_name, church, now)).rowcount > 0
            else:
                db.execute("DELETE FROM daily_devotion_reactions WHERE day=? AND actor_key=? AND kind='like'", (day, actor_key))
        elif action == 'share' and data.get('method') == 'share_menu':
            changed = db.execute('''INSERT OR IGNORE INTO daily_devotion_reactions
                VALUES(?,?,'share',?,?,?,?)''', (day, actor_key, actor_name, church, now, data['method'])).rowcount > 0
        elif action == 'comment':
            content = data.get('content')
            if not isinstance(content, str) or not 1 <= len(content.strip()) <= 2000:
                db.rollback()
                return jsonify(ok=False, error='Write a comment of 1–2,000 characters.'), 400
            recent = db.execute('SELECT created_at FROM daily_devotion_comments WHERE actor_key=? ORDER BY id DESC LIMIT 1', (actor_key,)).fetchone()
            if recent and (utc_now() - datetime.fromisoformat(recent['created_at'])).total_seconds() < 5:
                db.rollback()
                return jsonify(ok=False, error='Please wait a few seconds before commenting again.'), 429
            db.execute('''INSERT INTO daily_devotion_comments(day,actor_key,actor_name,church_name,content,created_at)
                VALUES(?,?,?,?,?,?)''', (day, actor_key, actor_name, church, content.strip(), now))
            changed = True
        elif action == 'delete_comment' and type(data.get('id')) is int:
            row = db.execute('SELECT actor_key FROM daily_devotion_comments WHERE day=? AND id=?', (day, data['id'])).fetchone()
            if not row or (row['actor_key'] != actor_key and not website_owner()):
                db.rollback()
                abort(403)
            db.execute('DELETE FROM daily_devotion_comments WHERE day=? AND id=?', (day, data['id']))
        else:
            db.rollback()
            abort(400)
        db.commit()
        if changed:
            labels = {'like': 'Liked daily devotional', 'share': 'Used devotional share menu',
                      'comment': 'Commented on daily devotional'}
            record_website_event('devotion_' + action, labels[action] + ': ' + post['title'] + ' (' + day + ')',
                                 page_path=url_for('bulletin', devotion_day=day),
                                 event_key=('devotion:'+action+':'+day+':'+actor_key) if action in {'like','share'} else None,
                                 details=dict(devotional=post['title'], day=day, verse=post['verse_reference'], church=church))
    result = dict(ok=True, **_devotion_counts(day))
    if kind in {'likes', 'shares'}:
        offset = max(0, request.args.get('offset', 0, type=int))
        rows = db.execute('''SELECT actor_name,church_name,created_at,method FROM daily_devotion_reactions
            WHERE day=? AND kind=? ORDER BY created_at DESC,actor_key LIMIT 50 OFFSET ?''',
            (day, 'like' if kind == 'likes' else 'share', offset)).fetchall()
        result.update(people=[dict(row) for row in rows], next_offset=offset+50 if offset+50 < result[kind] else None)
    elif kind == 'comments':
        before = request.args.get('before', 0, type=int)
        rows = db.execute('''SELECT * FROM daily_devotion_comments WHERE day=? AND (?=0 OR id<?)
            ORDER BY id DESC LIMIT 31''', (day, before, before)).fetchall()
        comments = []
        for row in rows[:30]:
            item = {key: row[key] for key in ('id', 'actor_name', 'church_name', 'content', 'created_at')}
            item['can_delete'] = row['actor_key'] == website_actor()['key'] or website_owner()
            comments.append(item)
        result.update(entries=comments, next_before=rows[29]['id'] if len(rows)>30 else None)
    elif kind != 'counts':
        abort(400)
    response = jsonify(result)
    response.headers['Cache-Control'] = 'private, no-store'
    return response


def _missing_pastor_reports(username, today):
    # Only opened reporting cycles in this calendar year are eligible.
    db = get_db()
    account = _get_cached_pastor_account(username)
    if not account:
        return []
    submissions = {(int(row['year']), int(row['month'])) for row in db.execute(
        'SELECT year,month FROM monthly_reports WHERE pastor_username=? AND submitted=1', (username,))}
    missing = []
    for month in range(1, today.month + 1):
        last_day = date(today.year, month, calendar.monthrange(today.year, month)[1])
        last_sunday = last_day - timedelta(days=(last_day.weekday() + 1) % 7)
        if today < last_sunday:
            continue
        reported = (today.year, month) in submissions or _report_exists_for_pastor_month_from_cache(username, today.year, month)
        if not reported:
            missing.append(dict(label=last_day.strftime('%B %Y'), overdue=today > last_sunday + timedelta(days=7),
                                url=url_for('pastor_tool', year=today.year, month=month)))
    return missing


@app.post('/pastor-tool/report-reminder')
def pastor_report_reminder():
    _require_district_user(write=True)
    if not pastor_logged_in() or ao_logged_in():
        abort(403)
    username = str(session.get('pastor_username') or '').strip()
    today, db = _district_today(), get_db()
    key = username.casefold()
    if db.execute('SELECT 1 FROM pastor_daily_reminders WHERE actor_key=? AND day=?', (key, today.isoformat())).fetchone():
        return jsonify(ok=True, show=False)
    missing = _missing_pastor_reports(username, today)
    if not missing:
        return jsonify(ok=True, show=False)
    claimed = db.execute('INSERT OR IGNORE INTO pastor_daily_reminders VALUES(?,?,?)',
                         (key, today.isoformat(), utc_now_iso())).rowcount > 0
    db.commit()
    return jsonify(ok=True, show=claimed, reports=missing if claimed else [],
                   church=str(session.get('pastor_church_id') or ''), day=today.isoformat())


@app.route("/bulletin/pray/<request_id>", methods=["POST"])
def bulletin_pray(request_id):
    if not any_user_logged_in():
        return redirect(url_for("pastor_login", next=request.path))

    sync_from_sheets_if_needed()

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
        refresh_after_sheet_write("PrayerRequest")

    return redirect(url_for("bulletin"))

# ========================
# Pastor login (uses CACHE)
# ========================

@app.route("/pastor-login", methods=["GET", "POST"])
def pastor_login():
    error = None
    next_url = request.args.get("next") or url_for("bulletin")

    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = (request.form.get("password") or "").strip()

        sync_from_sheets_if_needed()

        if not username or not password:
            error = "Username and password are required."
        else:
            row = get_db().execute(
                "SELECT username, password, name, church_address, sex, age, position, sub_area FROM sheet_accounts_cache WHERE username = ?",
                (username,),
            ).fetchone()

            if (row and str(row["password"] or "").strip() == password
                    and str(row["position"] or "").strip().lower() == "pastor"):
                session.clear()
                session["pledge_actor"] = username
                session["pastor_logged_in"] = True
                session["pastor_username"] = username
                session["pastor_name"] = row["name"] or ""
                session["pastor_church_address"] = row["church_address"] or ""
                session["pastor_church_id"] = (row["sex"] or "").strip()
                session["pastor_area_number"] = (row["age"] or "").strip() if "age" in row.keys() else ""
                session["selected_position"] = "pastor"
                session.permanent = True
                _record_pastor_login_event(row)
                return redirect(url_for("bulletin"))

            error = "Invalid username or password."

    return render_template("pastor_login.html", error=error, next_url=next_url)


# ========================
# Pastor Tool
# ========================

def _get_cached_pastor_account(pastor_username: str):
    return get_db().execute(
        """
        SELECT username, name, church_address, sex, age
        FROM sheet_accounts_cache
        WHERE username = ?
        """,
        ((pastor_username or "").strip(),),
    ).fetchone()


def _get_pastor_monthly_report_pdf_data(pastor_username: str, year: int, month: int):
    """Collect only data that has successfully reached the Report sheet cache.

    This keeps the PDF tied to a successful submission. If a pastor edits local
    data after submitting but has not successfully resubmitted yet, the PDF
    still represents the last successfully submitted report. Church Progress
    stays monthly (Option B) and is placed only in the PDF MONTHLY TOTAL row.
    """
    pastor_username = (pastor_username or "").strip()
    if not pastor_username:
        return None

    acc = _get_cached_pastor_account(pastor_username)
    if not acc:
        return None

    pastor_name = str(acc["name"] or "").strip()
    church_address = str(acc["church_address"] or "").strip()
    church_id = str(acc["sex"] or "").strip()
    church_key = church_id or church_address

    db = get_db()
    cached_rows = db.execute(
        """
        SELECT *
        FROM sheet_report_cache
        WHERE year = ? AND month = ?
          AND (
                TRIM(church) = TRIM(?)
             OR TRIM(address) = TRIM(?)
             OR TRIM(pastor) = TRIM(?)
          )
        ORDER BY activity_date
        """,
        (int(year), int(month), church_key, church_key, pastor_name),
    ).fetchall()

    if not cached_rows:
        return None

    sundays = []
    for row in cached_rows:
        report_date = parse_sheet_date(str(row["activity_date"] or "").strip())
        if not report_date:
            report_date = str(row["activity_date"] or "").strip()

        sundays.append({
            "date": report_date,
            "attendance_adult": row["adult"] or 0,
            "attendance_youth": row["youth"] or 0,
            "attendance_children": row["children"] or 0,
            "tithes_church": row["tithes"] or 0,
            "offering": row["offering"] or 0,
            "mission": row["mission_offering"] or 0,
            "tithes_personal": row["personal_tithes"] or 0,
            # Use the submitted Report-sheet remittance value so the PDF
            # follows the app's official Amount to Send calculation.
            "amount_to_send": row["amount_to_send"] or 0,
        })

    # Church Progress is repeated on each Google Sheets Sunday row by the
    # current submission workflow. Read it once and place it in MONTHLY TOTAL.
    cp_seed = cached_rows[0]
    church_progress = {
        "bible_new": cp_seed["new_bible_study"] or 0,
        "bible_existing": cp_seed["existing_bible_study"] or 0,
        "received_christ": cp_seed["received_jesus"] or 0,
        "baptized_water": cp_seed["water_baptized"] or 0,
        "child_dedication": cp_seed["childrens_dedication"] or 0,
    }

    month_name = calendar.month_name[int(month)]
    church_name = church_id or church_address

    return {
        "district_no": "4",
        "area_no": str(acc["age"] or "").strip(),
        "month_name": month_name,
        "year": int(year),
        "church_name": church_name,
        "church_address": church_address,
        "pastor_name": pastor_name,
        "sunday_rows": sundays,
        "church_progress": church_progress,
    }


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
                (?<>'' AND TRIM(church) = TRIM(?))
             OR (?<>'' AND TRIM(address) = TRIM(?))
             OR (?<>'' AND TRIM(pastor) = TRIM(?))
          )
        """,
        (int(year), int(month), church_key, church_key, church_key, church_key, pastor_name, pastor_name),
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

    sync_from_sheets_if_needed()
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
        refresh_sheet_cache("Report")
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

        if not _export_month_to_sheet_for_pastor(pastor_username, year, month, 'Pending AO approval'):
            raise RuntimeError("No complete local report or pastor account was available to submit.")
        set_month_submitted(year, month, pastor_username)

        db.execute(
            "UPDATE submit_report_jobs SET progress_message = ? WHERE id = ?",
            ('Syncing with Google Sheets...', job_id),
        )
        db.commit()

        refresh_after_sheet_write("Report")
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
        record_website_event("Report submitted", f"Monthly report submitted: {pastor_username} / {year}-{month:02d}", event_key="report-submitted:"+str(job_id))
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
        app.logger.exception("Error processing submit report job")


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


@app.route("/pastor-tool/report/<int:year>/<int:month>.pdf")
def pastor_tool_monthly_report_pdf(year, month):
    if not (pastor_logged_in() or ao_logged_in()):
        return redirect(url_for("pastor_login", next=request.path))

    if month < 1 or month > 12:
        abort(404)

    if ao_logged_in():
        pastor_username = (
            request.args.get("church")
            or session.get("pastor_username")
            or ""
        ).strip()
        if not pastor_username or not _pastor_username_in_current_ao_scope(pastor_username):
            abort(403)
    else:
        pastor_username = (session.get("pastor_username") or "").strip()

    report_data = _get_pastor_monthly_report_pdf_data(
        pastor_username,
        int(year),
        int(month),
    )
    if not report_data:
        abort(404)

    pdf_bytes = build_monthly_activity_report_pdf(report_data)
    filename = monthly_report_filename(
        report_data.get("church_name"),
        report_data.get("month_name"),
        report_data.get("year"),
    )

    response = make_response(pdf_bytes)
    response.headers["Content-Type"] = "application/pdf"
    response.headers["Content-Length"] = str(len(pdf_bytes))
    response.headers["Cache-Control"] = "private, no-store"
    response.headers["Content-Disposition"] = (
        "attachment; filename*=UTF-8''"
        + urllib.parse.quote(filename)
    )
    return response


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

        # IMPORTANT:
        # An AO is only SELECTING a pastor record for Pastor's Tool.
        # Do NOT mark the AO session as pastor_logged_in.
        # This keeps the original logged-in AO identity authoritative
        # everywhere else in the website (including Pij).
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

    today = _district_today()
    year = request.args.get("year", type=int) or min(2034, max(2024, today.year))
    month = request.args.get("month", type=int) or today.month
    if not 2024 <= year <= 2034 or not 1 <= month <= 12:
        abort(400)

    # ✅ AO fallback: if AO opens Pastor's Tool without choosing a church yet,
    # automatically select the first church in AO scope to avoid blank pastor_username.
    if ao_logged_in() and not (session.get("pastor_username") or "").strip():
        if ao_church_choices:
            session["pastor_username"] = ao_church_choices[0]
            selected_church = ao_church_choices[0]
            refresh_pastor_from_cache()
        else:
            return "No churches found under your area for Pastor's Tool access.", 404

    pastor_username = (session.get("pastor_username") or "").strip()
    if not pastor_username:
        return redirect(url_for("splash"))

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

    year_options = list(range(2024, 2035))

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

    # The PDF becomes available only after the submitted rows are confirmed
    # in the Google Sheets cache, not merely when the local submitted flag flips.
    report_available = bool(month_has_data.get(month))

    existing_submit_job = _get_existing_submit_report_job(pastor_username, year, month)
    submit_job_id = existing_submit_job["id"] if existing_submit_job else ""

    if request.method == "POST":
        if bool(monthly_report["approved"]):
            if ao_mode and selected_church:
                return redirect(url_for("pastor_tool", year=year, month=month, church=selected_church))
            return redirect(url_for("pastor_tool", year=year, month=month))

        if can_submit:
            try:
                refresh_sheet_cache("Report")
                if not _report_exists_for_pastor_month_from_cache(pastor_username, year, month):
                    if not _export_month_to_sheet_for_pastor(pastor_username, year, month, "Pending AO approval"):
                        raise RuntimeError("No complete local report or pastor account was available to submit.")
                    set_month_submitted(year, month, pastor_username)
                    clear_month_dirty(year, month)
                    refresh_after_sheet_write("Report")
                    sync_local_month_from_cache_for_pastor(year, month)
            except Exception as e:
                print("Error exporting month to sheet on submit:", e)
                flash("The report could not be submitted to Google Sheets. Please try again.", "error")

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
        report_available=report_available,
        report_pdf_url=(
            url_for(
                "pastor_tool_monthly_report_pdf",
                year=year,
                month=month,
                church=(selected_church if ao_mode else None),
            )
            if report_available
            else ""
        ),
        report_filename=(
            monthly_report_filename(
                church_id or church_address or pastor_name,
                calendar.month_name[month],
                year,
            )
            if report_available
            else ""
        ),
    )


@app.route("/pastor-tool/<int:year>/<int:month>/<int:day>", methods=["GET", "POST"], endpoint="sunday_detail")

def sunday_detail(year, month, day):
    if not (pastor_logged_in() or ao_logged_in()):
        return redirect(url_for("pastor_login", next=request.path))

    # ✅ AO MODE: keep selected church via ?church=username
    church = (request.args.get("church") or "").strip()

    if ao_logged_in():
        # Keep AO as the authenticated identity. `church` is only the
        # selected Pastor's Tool working context.
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
            record_website_event("Sunday report updated", f"Sunday report updated: {year}-{month:02d}-{day:02d}")

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
        # Keep AO as the authenticated identity. `church` is only the
        # selected Pastor's Tool working context.
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
            record_website_event("Church progress updated", f"Church progress updated: {year}-{month:02d}")

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
    next_url = request.args.get("next") or url_for("bulletin")

    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = (request.form.get("password") or "").strip()

        # Authenticate against the SQLite mirror.
        sync_from_sheets_if_needed()

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
                session.clear()
                session["pledge_actor"] = username
                session["ao_logged_in"] = True
                session["ao_username"] = username
                session["ao_name"] = row["name"] or ""
                session["ao_area_number"] = (row["age"] or "").strip()
                session["ao_church_id"] = (row["sex"] or "").strip()
                session["ao_role"] = (row["position"] or "").strip()
                session["ao_sub_area"] = (row["sub_area"] or "").strip()
                session.permanent = True
                return redirect(url_for("bulletin"))

        error = "Invalid username or password."

    return render_template("ao_login.html", error=error, next_url=next_url)


def is_cache_developer():
    if not session.get("ao_logged_in"):
        return False
    username = str(session.get("ao_username") or "").strip()
    role = str(session.get("ao_role") or "").strip().lower()
    if username.lower() != "pijeme" or role != "area overseer":
        return False
    # Also honor role revocation in the current Accounts mirror.
    row = get_db().execute(
        "SELECT position FROM sheet_accounts_cache WHERE LOWER(TRIM(username)) = ?", ("pijeme",)
    ).fetchone()
    return bool(row and str(row["position"] or "").strip().lower() == "area overseer")


def cache_control_token():
    token = session.get("cache_control_token")
    if not token:
        token = session["cache_control_token"] = secrets.token_urlsafe(32)
    return token


def _require_cache_developer():
    if not is_cache_developer():
        abort(403)
    expected = str(session.get("cache_control_token") or "")
    supplied = str(request.form.get("cache_control_token") or "")
    if not expected or not hmac.compare_digest(expected, supplied):
        abort(400)


@app.route("/ao-tool/cache/full-sync", methods=["POST"])
def ao_full_cache_sync():
    _require_cache_developer()
    try:
        counts = refresh_sheet_cache()
        summary = ", ".join(f"{name}: {count}" for name, count in counts.items())
        flash(f"Google Sheets synced to Local DB. {summary}", "success")
    except CacheSyncError as exc:
        app.logger.exception("Developer full cache sync failed")
        flash(f"Sync failed. {exc} Existing cached data was kept.", "error")
    return redirect(url_for("ao_tool"))


@app.route("/ao-tool/cache/download-db", methods=["POST"])
def ao_download_local_db():
    _require_cache_developer()
    download = tempfile.SpooledTemporaryFile(max_size=8 * 1024 * 1024, mode="w+b")
    try:
        # backup() creates a consistent snapshot, including committed WAL data.
        # Clean up the named snapshot before streaming; the response owns download.
        with tempfile.TemporaryDirectory(prefix="district4_backup_") as directory:
            snapshot_path = os.path.join(directory, "snapshot.db")
            source = sqlite3.connect(Path(DATABASE).resolve().as_uri() + "?mode=ro", uri=True)
            target = sqlite3.connect(snapshot_path)
            try:
                source.backup(target, pages=256)
            finally:
                target.close()
                source.close()
            with open(snapshot_path, "rb") as snapshot:
                shutil.copyfileobj(snapshot, download)
        size = download.tell()
        download.seek(0)
        timestamp = datetime.now(PH_TZ).strftime("%Y%m%d_%H%M%S")
        response = send_file(download, mimetype="application/vnd.sqlite3", as_attachment=True,
                             download_name=f"district4_app_v2_backup_{timestamp}.db", conditional=False)
        response.content_length = size
        response.headers["Cache-Control"] = "private, no-store"
        response.call_on_close(download.close)
        return response
    except Exception:
        download.close()
        app.logger.exception("Local DB snapshot download failed")
        flash("Unable to create the database backup. Please try again.", "error")
        return redirect(url_for("ao_tool"))


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
        is_cache_developer=is_cache_developer(),
        cache_control_token=cache_control_token() if is_cache_developer() else "",
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
        refresh_after_sheet_write('Accounts')
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
        refresh_after_sheet_write('Accounts')
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
        refresh_after_sheet_write('Anouncement')
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
        refresh_after_sheet_write('Anouncement')
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
        refresh_after_sheet_write('Anouncement')
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

                    pastor_data = {
                        "full_name": full_name, "age": age_int, "sex": sex,
                        "church_address": church_address, "contact_number": contact_number,
                        "birthday": birthday_raw, "username": username, "password": password,
                        "position": "Pastor",
                        "sub_area": (session.get("ao_sub_area") or "").strip() if ao_is_sub_area_overseer() else "",
                    }
                    try:
                        append_account_to_sheet(pastor_data)
                    except Exception:
                        app.logger.exception("Account source write failed")
                        error = "Unable to save the account to Google Sheets. Please try again."
                        return render_template("ao_create_account.html", ao_area_number=session.get("ao_area_number"),
                                               error=error, success=None, values=values,
                                               generated_username=None, generated_password=None)

                    record_website_event("Account created", "Account created: "+username)
                    refresh_after_sheet_write("Accounts")
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

                    except sqlite3.IntegrityError:
                        db.rollback()
                        success = "Account saved to Google Sheets."
                        generated_username = username
                        generated_password = password
                        flash("Account saved, but its local pastor record could not update. Run developer sync before creating another account.", "error")

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
                verify_sheet_row(ws, "AOPT", sheet_row, headers=headers)
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

            refresh_after_sheet_write('AOPT')

    except Exception as e:
        app.logger.exception("Google Sheets action failed")
        flash("The change could not be saved to Google Sheets. Reload the page and try again.", "error")

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
            refresh_after_sheet_write("Report")
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
            refresh_after_sheet_write("PrayerRequest")
        except Exception as e:
            app.logger.exception("Google Sheets action failed")
            flash("The change could not be saved to Google Sheets. Reload the page and try again.", "error")

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

    # Website writes refresh this dataset before redirecting here.
    sync_from_sheets_if_needed()

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

    # Read the mirror refreshed by the write flow.
    sync_from_sheets_if_needed()

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
            refresh_after_sheet_write("PrayerRequest")
        except Exception as e:
            app.logger.exception("Google Sheets action failed")
            flash("The change could not be saved to Google Sheets. Reload the page and try again.", "error")

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
        refresh_after_sheet_write("PrayerRequest")
    except Exception as e:
        app.logger.exception("Google Sheets action failed")
        flash("The change could not be saved to Google Sheets. Reload the page and try again.", "error")

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
        refresh_after_sheet_write("PrayerRequest")
    except Exception as e:
        app.logger.exception("Google Sheets action failed")
        flash("The change could not be saved to Google Sheets. Reload the page and try again.", "error")

    return redirect(url_for("prayer_request_status"))


# ========================
# AO Tool → Prayer Request Approval
# ========================

@app.route("/ao-tool/prayer-requests")
def ao_prayer_requests():
    if not ao_logged_in():
        return redirect(url_for("ao_login", next=request.path))

    # Read locally; submissions and approvals refresh this dataset.
    sync_from_sheets_if_needed()

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
        refresh_after_sheet_write("PrayerRequest")
    except Exception as e:
        app.logger.exception("Google Sheets action failed")
        flash("The change could not be saved to Google Sheets. Reload the page and try again.", "error")

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
        refresh_after_sheet_write("PrayerRequest")
    except Exception as e:
        app.logger.exception("Google Sheets action failed")
        flash("The change could not be saved to Google Sheets. Reload the page and try again.", "error")

    return redirect(url_for("ao_prayer_requests"))


@app.route("/ao-tool/prayer-requests/approve-all", methods=["POST"])
def ao_prayer_requests_approve_all():
    if not ao_logged_in():
        return redirect(url_for("ao_login", next=request.path))

    try:
        # refresh then approve everything still pending
        sync_from_sheets_if_needed()
        rows = get_pending_prayers_for_ao()

        for r in rows:
            req_id = (r["request_id"] or "").strip()
            if not req_id or not _prayer_in_current_ao_manage_scope(r):
                continue
            _update_prayer_request_cells_in_sheet(req_id, {"status": "Approved"})

        refresh_after_sheet_write("PrayerRequest")
    except Exception as e:
        app.logger.exception("Google Sheets action failed")
        flash("The change could not be saved to Google Sheets. Reload the page and try again.", "error")

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
    verify_sheet_row(ws, "Anouncement", int(sheet_row))
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
        raise ValueError("Invalid announcement row.")
    client = get_gs_client()
    sh = client.open("District4 Data")
    ws = sh.worksheet("Anouncement")
    verify_sheet_row(ws, "Anouncement", int(sheet_row))
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
    if website_owner():
        return not area or area == my_area
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
# Campus Christian Movement
# New accounts live locally until an explicit, verified Google backup.
# ========================

CCM_FOLDER_ID = "1IYlGZPuHL7CUW7HqwWEHnxuaG63gscEY"
EVENT_FOLDER_ID = "1pvwfSIS0kcvjp__O9--9-1-2f7zIBcEI"
CCM_BASE_URL = "https://iowod4tool.coresite.pro/ccm"
CCM_HEADERS = ["Record Key", "ID Number", "Name", "Area Number", "Church ID", "Church Address",
               "Pastor", "Campus", "Phone", "Email", "Status", "Created At", "Updated At",
               "Photo File ID", "Front File ID", "Back File ID", "Verification URL", "Expiration Date"]


class CCMStorageError(RuntimeError):
    pass


def ccm_sheet():
    try:
        book = get_gs_client().open("District4 Data")
        try:
            sheet = book.worksheet("CCM Workers")
        except gspread.WorksheetNotFound:
            try:
                sheet = book.add_worksheet(title="CCM Workers", rows=1000, cols=len(CCM_HEADERS))
            except gspread.exceptions.APIError:
                sheet = book.worksheet("CCM Workers")
        headers = sheet.row_values(1)
        if not headers or headers == CCM_HEADERS[:-1]:
            # Append the new field without moving or rewriting existing worker data.
            if sheet.col_count < len(CCM_HEADERS):
                sheet.resize(cols=len(CCM_HEADERS))
            sheet.update(range_name="A1:R1", values=[CCM_HEADERS], value_input_option="RAW")
            sheet.freeze(rows=1)
        elif headers != CCM_HEADERS:
            raise CCMStorageError("The CCM Workers sheet headers have changed. Restore the original columns before saving.")
        return sheet
    except CCMStorageError:
        raise
    except Exception as exc:
        app.logger.warning("CCM spreadsheet unavailable: %s", type(exc).__name__)
        raise CCMStorageError("Google Sheets is unavailable. Please try again; your form has not been cleared.") from None


def ccm_tables():
    db = get_db()
    db.execute("CREATE TABLE IF NOT EXISTS ccm_directory_cache (id INTEGER PRIMARY KEY CHECK(id=1), payload TEXT NOT NULL, updated REAL NOT NULL)")
    db.execute("CREATE TABLE IF NOT EXISTS ccm_pending_workers (key TEXT PRIMARY KEY, payload TEXT NOT NULL, error TEXT NOT NULL DEFAULT '')")
    db.execute("CREATE TABLE IF NOT EXISTS ccm_backup_leases (key TEXT PRIMARY KEY, token TEXT NOT NULL, expires REAL NOT NULL)")
    db.commit()


def ccm_pending():
    ccm_tables()
    return {r['key']: json.loads(r['payload']) for r in get_db().execute('SELECT key,payload FROM ccm_pending_workers ORDER BY rowid')}


def ccm_cache_write(records, read_started=None):
    ccm_tables()
    db = get_db()
    db.execute('BEGIN IMMEDIATE')
    try:
        current = db.execute('SELECT payload,updated FROM ccm_directory_cache WHERE id=1').fetchone()
        if read_started is not None and current and current['updated'] > read_started:
            db.rollback()
            return json.loads(current['payload'])
        # Neither a Google refresh nor an older in-flight backup may put a
        # locally deleted profile back into the device's directory cache.
        deleted = {row['key'] for row in db.execute('SELECT key,payload FROM ccm_pending_workers')
                   if json.loads(row['payload']).get('Status') == 'Deleted'}
        records = [record for record in records
                   if record.get('Status') != 'Deleted' and record['Record Key'] not in deleted]
        db.execute("INSERT OR REPLACE INTO ccm_directory_cache VALUES (1,?,?)", (json.dumps(records), time.time()))
        db.commit()
        return records
    except Exception:
        db.rollback()
        raise


def ccm_sheet_records(values):
    return [dict(zip(CCM_HEADERS, row + [''] * max(0, len(CCM_HEADERS) - len(row))))
            for row in values[1:] if row and row[0]]


def ccm_records(force=False):
    ccm_tables()
    db = get_db()
    cached = db.execute("SELECT payload,updated FROM ccm_directory_cache WHERE id=1").fetchone()
    local = ccm_pending()
    records, stale = json.loads(cached['payload']) if cached else [], False
    # Normal navigation uses local records and the last confirmed Google index.
    # Only the first import, Refresh Google, backup or QR verification needs Google.
    if force or (not cached and not local):
        started = time.time()
        try:
            records = ccm_cache_write(ccm_sheet_records(ccm_sheet().get_all_values()), read_started=started)
        except Exception as exc:
            if not cached and not local:
                raise CCMStorageError('Google accounts could not be loaded. New accounts can still be saved locally.') from None
            app.logger.warning('CCM directory refresh failed: %s', type(exc).__name__)
            stale = True
    merged = {r['Record Key']: r for r in records}
    merged.update(local)
    return list(merged.values()), stale


def ccm_admin():
    return bool(session.get('ccm_admin_until', 0) > time.time())


@app.template_filter('ccm_date')
def ccm_date(value):
    try:
        dt = datetime.fromisoformat(value)
        return dt.astimezone(PH_TZ or timezone(timedelta(hours=8))).strftime('%b %d, %Y, %I:%M %p') + ' (UTC+08:00)'
    except (TypeError, ValueError):
        return value or '—'


def ccm_id_expiration(created):
    """Use the creation date's anniversary in Philippine time (Feb 29 becomes Feb 28)."""
    issued = datetime.fromisoformat(created).astimezone(PH_TZ or timezone(timedelta(hours=8))).date()
    year = issued.year + 1
    return issued.replace(year=year, day=min(issued.day, calendar.monthrange(year, issued.month)[1])).isoformat()


@app.template_filter('ccm_expiration')
def ccm_expiration(value):
    try:
        return date.fromisoformat(value).strftime('%b %d, %Y')
    except (TypeError, ValueError):
        return 'Invalid date' if value else 'Not set'


@app.template_filter('ccm_expired')
def ccm_expired(value):
    if not value:
        return False  # Existing IDs remain valid until an expiration is assigned.
    try:
        today = utc_now().astimezone(PH_TZ or timezone(timedelta(hours=8))).date()
        return date.fromisoformat(value) < today
    except (TypeError, ValueError):
        return True


def ccm_csrf():
    if 'ccm_csrf' not in session:
        session['ccm_csrf'] = secrets.token_urlsafe(32)
    return session['ccm_csrf']


def ccm_guard(admin=True):
    if admin and not ccm_admin():
        abort(403)
    supplied = request.form.get('csrf_token', '') or request.headers.get('X-CSRF-Token', '')
    if not supplied or not hmac.compare_digest(supplied.encode('utf-8'), session.get('ccm_csrf', '').encode('utf-8')):
        abort(400, 'Reload the CCM page before trying again.')


def ccm_churches():
    # Select explicit public ministry fields, never credentials or birthdays.
    rows = get_db().execute("""SELECT name,age,sex,church_address FROM sheet_accounts_cache
        WHERE LOWER(TRIM(position))='pastor' AND TRIM(COALESCE(church_address,''))<>''
        ORDER BY age,church_address,name""").fetchall()
    return [{'key': uuid.uuid5(uuid.NAMESPACE_URL, json.dumps(list(r))).hex, 'pastor': r['name'] or '', 'area': r['age'] or '',
             'church_id': r['sex'] or '', 'address': r['church_address'] or ''} for r in rows]


def ccm_worker(key, fresh=False):
    if not re.fullmatch(r'[a-f0-9]{32}', key):
        abort(404)
    records, stale = ccm_records(force=fresh)
    row = next((r for r in records if r['Record Key'] == key and r['Status'] != 'Deleted'), None)
    if not row:
        abort(404)
    return row, stale


def ccm_drive(method, path, **kwargs):
    try:
        creds = google_credentials(GOOGLE_SHEETS_CREDENTIALS_FILE, scopes=GOOGLE_SHEETS_SCOPES)
        creds.refresh(GoogleAuthRequest())
        response = requests.request(method, 'https://www.googleapis.com/' + path,
            headers={'Authorization': 'Bearer ' + creds.token, **kwargs.pop('headers', {})},
            timeout=(10, 45), **kwargs)
        if not response.ok:
            app.logger.warning('CCM Drive request failed: %s %s', method, response.status_code)
            try:
                quota = any(e.get('reason') == 'storageQuotaExceeded' for e in response.json().get('error', {}).get('errors', []))
            except ValueError:
                quota = False
            if quota:
                raise CCMStorageError("Google Drive rejected the service-account upload. Set CCM_DRIVE_SCRIPT_URL and CCM_DRIVE_API_TOKEN in the running server's .env to use the folder owner's Apps Script. For Docker, recreate the district4 container after updating .env. The photo and form are still available for retry.")
            raise CCMStorageError('Google Drive could not save or retrieve this ID. Check folder access and try again.')
        return response
    except Exception as exc:
        if isinstance(exc, CCMStorageError):
            raise
        app.logger.warning('CCM Drive unavailable: %s', type(exc).__name__)
        raise CCMStorageError('Google Drive is unavailable. Please try again; your form has not been cleared.') from None


def ccm_asset_path(key, side):
    if not re.fullmatch(r'[a-f0-9]{32}', key) or side not in ('photo', 'front', 'back'):
        abort(404)
    directory = Path(data_path('ccm_assets'))
    directory.mkdir(parents=True, exist_ok=True)
    return directory / (key + '-' + side + '.png')


def ccm_local_asset_path(filename):
    if not re.fullmatch(r'[a-f0-9]{32}-(photo|front|back)-[a-f0-9]{16}\.png', filename):
        raise CCMStorageError('Invalid local image reference.')
    root = ccm_asset_path(filename[:32], 'photo').parent.resolve()
    path = (root / filename).resolve()
    if path.parent != root:
        raise CCMStorageError('Invalid local image location.')
    return path


def ccm_local_lock(key):
    from sheet_cache import _locked_sync
    return _locked_sync(str(ccm_asset_path(key, 'photo').parent / (key + '.local')))


def ccm_write_local_assets(row, images):
    import hashlib
    references = {}
    for side, body in images.items():
        name = row['Record Key'] + '-' + side + '-' + hashlib.sha256(body).hexdigest()[:16] + '.png'
        path = ccm_local_asset_path(name)
        if not path.exists():
            temporary = path.with_suffix('.' + uuid.uuid4().hex + '.tmp')
            try:
                with temporary.open('wb') as handle:
                    handle.write(body)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temporary, path)
            finally:
                temporary.unlink(missing_ok=True)
        references[side] = name
    row['_local_assets'] = references


def ccm_remove_local_assets(key):
    root = ccm_asset_path(key, 'photo').parent.resolve()
    for path in root.glob(key + '-*.png'):
        if path.resolve().parent != root:
            raise CCMStorageError('Local cleanup stopped because an image path was outside the image folder.')
        path.unlink(missing_ok=True)


def ccm_upload(key, side, data, *, filename=None, event_key=None):
    # Content-addressed filenames make retries idempotent and keep old records
    # intact if a later Sheets update fails during an edit.
    import hashlib
    digest = hashlib.sha256(data).hexdigest()
    name = filename or 'CCM-' + key + '-' + side + '-' + digest[:16] + '.png'
    properties = {'event_key': event_key, 'record_key': key, 'sha256': digest} if filename else {}
    is_event = filename is not None or event_key is not None
    connection = 'EVENT' if is_event else 'CCM'
    script_file = 'event_drive.gs' if is_event else 'ccm_drive.gs'
    script_url = os.getenv(connection + '_DRIVE_SCRIPT_URL', '').strip()
    token = os.getenv(connection + '_DRIVE_API_TOKEN', '').strip()
    if is_event:
        if (not filename or not isinstance(event_key,str) or not re.fullmatch(r'[a-f0-9]{32}',event_key)
                or side not in ('photo','id')):
            raise CCMStorageError('Invalid Event upload details. Your local files are kept for retry.')
        if not script_url or not token:
            raise CCMStorageError("Set EVENT_DRIVE_SCRIPT_URL and EVENT_DRIVE_API_TOKEN in the running server's .env, then restart the app or recreate the Docker container. Event uploads require their own connection; your local files are kept for retry.")
    if token and not script_url:
        raise CCMStorageError("CCM_DRIVE_SCRIPT_URL is missing from the running server's environment. Set it beside CCM_DRIVE_API_TOKEN in the server's .env, then restart the app or recreate the Docker container. Your local files are kept for retry.")
    if script_url:
        if not re.fullmatch(r'https://script\.google\.com/macros/s/[A-Za-z0-9_-]+/exec', script_url) or len(token) < 32:
            raise CCMStorageError(f"Check {connection}_DRIVE_SCRIPT_URL (the deployed /exec URL) and {connection}_DRIVE_API_TOKEN in the running server's .env, then restart the app or recreate the Docker container.")
        try:
            payload = {'token': token, 'record_key': key, 'side': side, 'image': base64.b64encode(data).decode('ascii')}
            if filename:
                payload.update(purpose='event', filename=name, event_key=event_key)
            response = requests.post(script_url, json=payload, timeout=(10, 60))
            response.raise_for_status()
            result = response.json()
            if not result.get('ok'):
                errors = {
                    'AUTH': f"The Drive upload script rejected its API token. Match {connection}_DRIVE_API_TOKEN in the running server's .env to the Apps Script property, then restart the app or recreate the Docker container.",
                    'INPUT': f'The Drive upload script rejected the upload details. Check that the latest deployment/{script_file} is published in the web app deployment.',
                    'IMAGE': 'The Drive upload script rejected this PNG image or its size. Your local files are kept for retry.',
                    'DRIVE': "The upload script could not save to Drive. Check its Apps Script Executions log, folder access and storage, and that the web app executes as the folder owner.",
                    'CCM_ONLY': 'This is the CCM-only upload endpoint. Set EVENT_DRIVE_SCRIPT_URL to the separate Event script deployment.',
                    'EVENT_ONLY': 'This is the Event-only upload endpoint. Check the separate CCM and Event script URLs in the server configuration.',
                    'DELETED': 'This Event registration was deleted. Its ID cannot be uploaded again.',
                }
                if result.get('code') in errors:
                    raise CCMStorageError(errors[result['code']])
            if not result.get('ok') or not re.fullmatch(r'[A-Za-z0-9_-]+', str(result.get('file_id', ''))):
                raise ValueError('Upload not acknowledged')
            if is_event and (result.get('name') != name or result.get('folder_id') != EVENT_FOLDER_ID
                             or result.get('purpose') != 'event' or result.get('side') != side):
                raise CCMStorageError('The Event upload was not confirmed in the correct folder. Deploy deployment/event_drive.gs and check EVENT_DRIVE_SCRIPT_URL, then retry backup. Your local files are kept.')
            return result['file_id']
        except (requests.RequestException, ValueError, AttributeError):
            raise CCMStorageError(f'The {connection} Google Drive upload was not confirmed. Retry this same form; existing uploads will be reused.') from None
    escaped_name = name.replace('\\', '\\\\').replace("'", "\\'")
    query = "'" + CCM_FOLDER_ID + "' in parents and trashed=false and name='" + escaped_name + "'"
    for field, value in properties.items():
        query += " and appProperties has { key='" + field + "' and value='" + value + "' }"
    found = ccm_drive('GET', 'drive/v3/files', params={'q': query,
        'fields': 'files(id)', 'supportsAllDrives': 'true', 'includeItemsFromAllDrives': 'true'}).json()['files']
    if found:
        return found[0]['id']
    else:
        boundary = 'ccm_' + secrets.token_hex(16)
        metadata = {'name': name, 'parents': [CCM_FOLDER_ID]}
        if properties:
            metadata['appProperties'] = properties
        metadata = json.dumps(metadata)
        body = ('--' + boundary + '\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n' + metadata +
                '\r\n--' + boundary + '\r\nContent-Type: image/png\r\n\r\n').encode() + data + ('\r\n--' + boundary + '--\r\n').encode()
        file_id = ccm_drive('POST', 'upload/drive/v3/files', params={'uploadType': 'multipart', 'fields': 'id', 'supportsAllDrives': 'true'},
            headers={'Content-Type': 'multipart/related; boundary=' + boundary}, data=body).json()['id']
    return file_id


@lru_cache(maxsize=24)
def ccm_cloud_asset(file_id):
    # Bounded memory cache only: viewing a backed-up image does not recreate disk files.
    if not re.fullmatch(r'[A-Za-z0-9_-]+', file_id):
        raise CCMStorageError('This image is not available in Google Drive.')
    return ccm_drive('GET', 'drive/v3/files/' + file_id, params={'alt': 'media', 'supportsAllDrives': 'true'}).content


def ccm_asset(record, side):
    local_name = record.get('_local_assets', {}).get(side)
    if local_name:
        path = ccm_local_asset_path(local_name)
        if not path.is_file():
            if record.get('_backup_confirmed_revision') == record['Updated At']:
                return ccm_cloud_asset(record[side.title() + ' File ID'])
            raise CCMStorageError('A local image is missing. Keep the remaining files and restore the missing image before backup.')
        return path.read_bytes()
    path = ccm_asset_path(record['Record Key'], side)
    if path.exists():
        return path.read_bytes()
    return ccm_cloud_asset(record[side.title() + ' File ID'])


def ccm_photo(upload):
    from PIL import Image, ImageOps, UnidentifiedImageError
    if not upload or not upload.filename:
        raise ValueError('Take or upload a worker photo.')
    raw = upload.read(12 * 1024 * 1024 + 1)
    if len(raw) > 12 * 1024 * 1024:
        raise ValueError('This photo is too large to upload. Choose it again so the browser can resize it, or take a new photo.')
    try:
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter('error', Image.DecompressionBombWarning)
            with Image.open(io.BytesIO(raw)) as photo:
                if photo.format not in ('JPEG', 'MPO', 'PNG', 'WEBP'):
                    raise ValueError('Choose a JPEG, PNG or WebP photo, or use Take photo.')
                if photo.width * photo.height > 80000000:
                    raise ValueError('Choose this photo again so the browser can resize it, or take a new photo.')
                photo.draft('RGB', (1400, 1400))
                photo = ImageOps.exif_transpose(photo)
                photo.thumbnail((1400, 1400))
                photo = photo.convert('RGB')
                out = io.BytesIO()
                photo.save(out, 'PNG', compress_level=3)
                return out.getvalue()
    except (UnidentifiedImageError, OSError, Image.DecompressionBombWarning, Image.DecompressionBombError):
        raise ValueError('The photo could not be read. Retake it or choose another image.') from None


def ccm_render_id(record, photo):
    """Fill the ID artwork with worker details, validity and authorized signatories."""
    from PIL import Image, ImageDraw, ImageFont, ImageOps
    from reportlab.graphics.barcode.qrencoder import QRCode, QRErrorCorrectLevel
    root = Path(app.static_folder) / 'ccm'
    font_path = str(root / 'Tinos-Bold.ttf')

    def text(draw, value, box, size, color='#071e48', center=False):
        value = str(value)
        while True:
            font = ImageFont.truetype(font_path, size)
            bounds = draw.textbbox((0, 0), value, font=font)
            if size <= 5 or (bounds[2] - bounds[0] <= box[2] - box[0] and bounds[3] - bounds[1] <= box[3] - box[1]):
                break
            size -= 1
        x = (box[0] + box[2] - (bounds[2] - bounds[0])) / 2 if center else box[0]
        draw.text((x, box[1] - bounds[1]), value, font=font, fill=color)

    with Image.open(root / 'id-front-template.jpg') as original:
        front = original.convert('RGB')
    with Image.open(io.BytesIO(photo)) as portrait:
        portrait = ImageOps.fit(portrait.convert('RGB'), (386, 388), centering=(0.5, 0.4))
        mask = Image.new('L', portrait.size, 0)
        ImageDraw.Draw(mask).rounded_rectangle((0, 0, 385, 387), radius=14, fill=255)
        front.paste(portrait, (332, 566), mask)
    draw = ImageDraw.Draw(front)
    draw.rectangle((120, 975, 935, 1068), fill='white')
    text(draw, record['Name'], (120, 986, 935, 1059), 80, color='black', center=True)
    # Retain the banner border and sample its own navy background for the role.
    strip = front.crop((170, 1094, 920, 1095)).resize((750, 48))
    front.paste(strip, (170, 1095))
    text(draw, 'C A M P U S  W O R K E R', (170, 1100, 920, 1140), 40, color='white', center=True)
    draw.rectangle((378, 1174, 925, 1306), fill='white')
    issued = datetime.fromisoformat(record['Created At']).astimezone(PH_TZ or timezone(timedelta(hours=8)))
    text(draw, record['ID Number'], (383, 1181, 920, 1215), 36)
    text(draw, issued.strftime('%b. %d, %Y'), (383, 1227, 920, 1260), 36)
    expiry = record.get('Expiration Date', '')
    text(draw, ccm_expiration(expiry) if expiry else 'No expiration date', (383, 1270, 920, 1304), 36)
    with Image.open(root / 'id-back-template.jpg') as original:
        back = original.convert('RGB')
    draw = ImageDraw.Draw(back)
    # Replace the placeholder signatory labels; leave the holder's signature blank.
    draw.rectangle((80, 790, 985, 1002), fill='white')
    with Image.open(root / 'overseer-signatures.png') as source:
        source = source.convert('RGBA')
        # Exclude the captions embedded below each signature in the supplied image.
        for name, role, crop, left, right in [
            ('Cosme Walawala', 'District Overseer', (0, 1000, source.width, 1580), 90, 475),
            ('Ferlor Anger Rudrigues', 'Campus Ministry Overseer', (0, 0, source.width, 685), 540, 975),
        ]:
            signature = source.crop(crop)
            bounds = signature.getbbox()
            if bounds:
                signature = signature.crop(bounds)
                signature.thumbnail((right - left - 30, 115), Image.Resampling.LANCZOS)
                back.paste(signature, ((left + right - signature.width) // 2, 900 - signature.height), signature)
            draw.line((left + 10, 910, right - 10, 910), fill='#071e48', width=2)
            text(draw, name, (left, 922, right, 958), 34, center=True)
            text(draw, role, (left, 968, right, 997), 28, center=True)
    draw.rectangle((108, 1153, 294, 1338), fill='white')
    qr = QRCode(None, QRErrorCorrectLevel.M)
    qr.addData(record['Verification URL'])
    qr.make()
    scale = 4
    qr_image = Image.new('RGB', ((qr.moduleCount + 8) * scale,) * 2, 'white')
    qdraw = ImageDraw.Draw(qr_image)
    for y, row in enumerate(qr.modules):
        for x, dark in enumerate(row):
            if dark:
                qdraw.rectangle(((x + 4) * scale, (y + 4) * scale, (x + 5) * scale - 1, (y + 5) * scale - 1), fill='black')
    back.paste(qr_image, (int(201 - qr_image.width / 2), 1155))
    # Cover only variable text inside the original contact panel, preserving icons.
    background = back.crop((375, 1200, 930, 1201)).resize((555, 120))
    back.paste(background, (375, 1202))
    text(draw, '(062) 945 0217', (381, 1206, 927, 1233), 28, color='black')
    text(draw, 'campuschristianmovement2c518@gmail.com', (381, 1241, 927, 1263), 23, color='black')
    text(draw, 'International One Way Outreach District 4', (381, 1270, 927, 1292), 24, color='black')
    text(draw, 'Convention Center Lala Pagadian City.', (381, 1296, 927, 1318), 24, color='black')
    result = {}
    for side, image in [('front', front), ('back', back)]:
        buffer = io.BytesIO()
        image.save(buffer, 'PNG', dpi=(300, 300), compress_level=3)
        result[side] = buffer.getvalue()
    return result


def ccm_registration():
    key = request.form.get('record_key', '')
    editing = bool(key)
    old = ccm_worker(key)[0] if editing else None
    if old and request.form.get('revision', '') != old['Updated At']:
        raise ValueError('This worker changed since you opened the form. Reload before editing.')
    draft = session.get('ccm_draft') or {}
    if not editing:
        key = request.form.get('form_key', '')
        if not re.fullmatch(r'[a-f0-9]{32}', key) or not hmac.compare_digest(key, draft.get('key', '')):
            raise ValueError('This registration form expired. Reload the page.')
    name = request.form.get('name', '').strip()
    if not name or len(name) > 150:
        raise ValueError('Enter a name of up to 150 characters.')
    church = next((c for c in ccm_churches() if c['key'] == request.form.get('church_key') and c['area'] == request.form.get('area')), None)
    if not church:
        raise ValueError('Choose an area and church from the district records.')
    now = utc_now_iso()
    row = dict(old) if old else dict.fromkeys(CCM_HEADERS, '')
    row.update({'Record Key': key, 'Name': name, 'Area Number': church['area'], 'Church ID': church['church_id'],
        'Church Address': church['address'], 'Pastor': church['pastor'], 'Updated At': now})
    for field, limit in [('Campus', 200), ('Phone', 60), ('Email', 150)]:
        value = request.form.get(field.lower(), '').strip()
        if len(value) > limit:
            raise ValueError(field + ' is too long.')
        row[field] = value
    row['Status'] = request.form.get('status', 'Active')
    if row['Status'] not in ('Active', 'Inactive'):
        raise ValueError('Choose Active or Inactive.')
    if not old:
        row['Created At'] = now
        row['ID Number'] = 'CCM-' + datetime.fromisoformat(now).strftime('%Y') + '-' + key[:12].upper()
        row['Verification URL'] = CCM_BASE_URL + '/verify/' + key
    row['Expiration Date'] = ccm_id_expiration(row['Created At'])
    if old:
        row['_revision'] = old['Updated At']
        row['_cloud_revision'] = old.get('_cloud_revision', old['Updated At'])
    else:
        row['_cloud_revision'] = ''
    upload = request.files.get('photo')
    photo = ccm_photo(upload) if upload and upload.filename else ccm_asset(old, 'photo') if old else ccm_photo(None)
    return row, photo, editing


@app.get('/ccm')
def ccm_page():
    tab = request.args.get('tab', 'about')
    if tab not in ('about', 'workers', 'admin'):
        tab = 'about'
    records, error, stale, selected = [], None, False, None
    if tab != 'about':
        try:
            records, stale = ccm_records()
        except CCMStorageError as exc:
            error = str(exc)
    visible = [r for r in records if r['Status'] != 'Deleted']
    churches, names = [], []
    if tab == 'admin' and ccm_admin():
        churches = ccm_churches()
        names = [r['name'] for r in get_db().execute("SELECT DISTINCT name FROM sheet_members_account_cache WHERE TRIM(COALESCE(name,''))<>'' ORDER BY name")]
        key = request.args.get('edit', '')
        selected = next((r for r in visible if r['Record Key'] == key), None)
        if key and not selected:
            abort(404)
        if 'ccm_draft' not in session:
            session['ccm_draft'] = {'key': uuid.uuid4().hex, 'created': utc_now_iso()}
    manual = None
    if tab == 'about':
        manual = json.loads((Path(app.static_folder) / 'ccm' / 'manual.json').read_text(encoding='utf-8'))
    response = make_response(render_template('ccm.html', tab=tab, workers=visible, stale=stale, error=error,
        admin=ccm_admin(), csrf=ccm_csrf(), churches=churches, member_names=names, selected=selected,
        draft=session.get('ccm_draft', {}), manual=manual, pending=ccm_pending() if tab == 'admin' and ccm_admin() else {},
        expiration_date=ccm_id_expiration(selected['Created At'] if selected else utc_now_iso())))
    response.headers['Cache-Control'] = 'no-store'
    return response


@app.post('/ccm/admin/login')
def ccm_login():
    ccm_guard(admin=False)
    # Rate limiting is persistent across browser sessions and server workers.
    db = get_db()
    db.execute('CREATE TABLE IF NOT EXISTS ccm_login_attempts (key TEXT PRIMARY KEY, started REAL NOT NULL, count INTEGER NOT NULL)')
    address = request.remote_addr or 'unknown'
    now = time.time()
    db.execute('INSERT INTO ccm_login_attempts VALUES (?,?,1) ON CONFLICT(key) DO UPDATE SET count=CASE WHEN started<? THEN 1 ELSE count+1 END, started=CASE WHEN started<? THEN excluded.started ELSE started END',
               (address, now, now - 600, now - 600))
    db.commit()
    count = db.execute('SELECT count FROM ccm_login_attempts WHERE key=?', (address,)).fetchone()[0]
    if count > 10:
        flash('Too many attempts. Try again in ten minutes.', 'error')
    elif hmac.compare_digest(request.form.get('password', '').encode('utf-8'), b'CCMDistric4'):
        session['ccm_admin_until'] = now + 4 * 3600
        session['ccm_csrf'] = secrets.token_urlsafe(32)
        db.execute('DELETE FROM ccm_login_attempts WHERE key=?', (address,))
        db.commit()
    else:
        flash('Incorrect CCM admin password.', 'error')
    return redirect(url_for('ccm_page', tab='admin'))


@app.post('/ccm/admin/logout')
def ccm_logout():
    ccm_guard()
    session.pop('ccm_admin_until', None)
    session.pop('ccm_draft', None)
    session.pop('ccm_last_created', None)
    session.pop('ccm_csrf', None)
    return redirect(url_for('ccm_page', tab='workers' if request.form.get('exit') == '1' else 'admin'))


@app.post('/ccm/admin/preview')
def ccm_preview():
    ccm_guard()
    if request.content_length is None or request.content_length > 13 * 1024 * 1024:
        return jsonify(ok=False, error='Choose the photo again so it can be resized before upload.'), 413
    try:
        row, photo, _ = ccm_registration()
        images = ccm_render_id(row, photo)
        return jsonify(ok=True, images={side: 'data:image/png;base64,' + base64.b64encode(data).decode() for side, data in images.items()})
    except (ValueError, CCMStorageError) as exc:
        return jsonify(ok=False, error=str(exc)), 400


def ccm_store_local(row, images=None, expected_revision=None):
    ccm_tables()
    db, key = get_db(), row['Record Key']
    with ccm_local_lock(key):
        db.execute('BEGIN IMMEDIATE')
        try:
            pending = db.execute('SELECT payload FROM ccm_pending_workers WHERE key=?', (key,)).fetchone()
            cached = db.execute('SELECT payload FROM ccm_directory_cache WHERE id=1').fetchone()
            current = json.loads(pending['payload']) if pending else next(
                (r for r in json.loads(cached['payload']) if r['Record Key'] == key), None) if cached else None
            if expected_revision is None and current:
                db.rollback()
                return current
            if expected_revision is not None and (not current or current['Status'] == 'Deleted' or current['Updated At'] != expected_revision):
                raise ValueError('This account changed or was deleted. Reload before editing.')
            if images is None:
                row = {**current, 'Status': row['Status'], 'Updated At': row['Updated At']}
            row['_cloud_revision'] = current.get('_cloud_revision', current['Updated At']) if current else ''
            row.pop('_revision', None)
            if images is not None:
                ccm_write_local_assets(row, images)
            deleting = row['Status'] == 'Deleted'
            if deleting:
                row = {field: row[field] for field in
                       ('Record Key', 'ID Number', 'Created At', 'Status', 'Updated At', '_cloud_revision') if field in row}
                if cached:
                    records = [record for record in json.loads(cached['payload']) if record['Record Key'] != key]
                    db.execute('UPDATE ccm_directory_cache SET payload=?,updated=? WHERE id=1',
                               (json.dumps(records), time.time()))
            db.execute("INSERT OR REPLACE INTO ccm_pending_workers (key,payload,error) VALUES (?,?,'')", (key, json.dumps(row)))
            db.commit()
            if deleting:
                try:
                    ccm_remove_local_assets(key)
                except OSError:
                    raise CCMStorageError('Account removed. Some local images are still in use; retry Backup to Google to finish cleanup.') from None
            return row
        except Exception:
            db.rollback()
            raise


@app.post('/ccm/admin/save')
def ccm_save():
    ccm_guard()
    if request.content_length is None or request.content_length > 13 * 1024 * 1024:
        return jsonify(ok=False, error='Choose the photo again so it can be resized before upload.'), 413
    try:
        if not request.form.get('record_key') and request.form.get('form_key') == session.get('ccm_last_created'):
            return jsonify(ok=True, redirect=url_for('ccm_saved', key=session['ccm_last_created']))
        row, photo, editing = ccm_registration()
        revision = row.pop('_revision', None)
        images = {'photo': photo, **ccm_render_id(row, photo)}
        ccm_store_local(row, images, expected_revision=revision)
        if not editing:
            session['ccm_last_created'] = row['Record Key']
            session.pop('ccm_draft', None)
        return jsonify(ok=True, redirect=url_for('ccm_saved', key=row['Record Key']))
    except (ValueError, CCMStorageError, CacheSyncError) as exc:
        return jsonify(ok=False, error=str(exc)), 400
    except Exception as exc:
        app.logger.warning('CCM local save failed: %s', type(exc).__name__)
        return jsonify(ok=False, error='The local save could not be confirmed. Keep this form open and retry.'), 503


@app.get('/ccm/saved/<key>')
def ccm_saved(key):
    if not ccm_admin():
        return redirect(url_for('ccm_page', tab='admin'))
    worker, _ = ccm_worker(key)
    response = make_response(render_template('ccm_saved.html', worker=worker, pending=key in ccm_pending(), csrf=ccm_csrf()))
    response.headers['Cache-Control'] = 'no-store'
    return response


def ccm_verify_upload(file_id, body, *, allow_trashed=False):
    import hashlib
    info = ccm_drive('GET', 'drive/v3/files/' + file_id,
        params={'fields': 'id,size,md5Checksum,trashed', 'supportsAllDrives': 'true'}).json()
    if (info.get('trashed') and not allow_trashed) or str(info.get('size')) != str(len(body)) or info.get('md5Checksum') != hashlib.md5(body).hexdigest():
        raise CCMStorageError('Google Drive has not confirmed a complete image backup. Local files were kept.')
    return info


def ccm_backup_worker(key):
    from concurrent.futures import ThreadPoolExecutor
    from sheet_cache import _locked_sync
    if not re.fullmatch(r'[a-f0-9]{32}', key):
        abort(404)
    ccm_tables()
    db, token = get_db(), uuid.uuid4().hex
    db.execute('BEGIN IMMEDIATE')
    try:
        found = db.execute('SELECT payload FROM ccm_pending_workers WHERE key=?', (key,)).fetchone()
        if not found:
            db.rollback()
            return {'cleaned': True, 'remaining': len(ccm_pending())}
        lease = db.execute('SELECT expires FROM ccm_backup_leases WHERE key=?', (key,)).fetchone()
        if lease and lease['expires'] > time.time():
            raise CCMStorageError('This account is already being backed up. Wait for that backup to finish.')
        db.execute('INSERT OR REPLACE INTO ccm_backup_leases VALUES (?,?,?)', (key, token, time.time() + 240))
        db.commit()
    except Exception:
        db.rollback()
        raise
    snapshot = json.loads(found['payload'])
    revision = snapshot['Updated At']
    try:
        # Immutable files let admins create/edit accounts while this snapshot uploads.
        bodies = {} if snapshot['Status'] == 'Deleted' or snapshot.get('_backup_confirmed_revision') == revision else {
            side: ccm_asset(snapshot, side) for side in snapshot.get('_local_assets', {})}
        cloud = {h: snapshot.get(h, '') for h in CCM_HEADERS}

        def upload(side):
            body = bodies[side]
            file_id = ccm_upload(key, side, body)
            ccm_verify_upload(file_id, body)
            return side, file_id

        with ThreadPoolExecutor(max_workers=3) as pool:
            for side, file_id in pool.map(upload, bodies):
                cloud[side.title() + ' File ID'] = file_id
        with _locked_sync(DATABASE):
            sheet = ccm_sheet()
            values = sheet.get_all_values()
            matching = [(i, dict(zip(CCM_HEADERS, r + [''] * max(0, len(CCM_HEADERS) - len(r)))))
                        for i, r in enumerate(values[1:], 2) if r and r[0] == key]
            if len(matching) > 1:
                raise CCMStorageError('Google Sheets contains duplicate copies of this account. Local files were kept.')
            if matching:
                number, previous = matching[0]
                if previous['Updated At'] not in (snapshot.get('_cloud_revision', ''), revision):
                    raise CCMStorageError('This account changed in Google since the local edit. Local files were kept for review.')
                if snapshot['Status'] == 'Deleted':
                    # The local deletion marker carries no profile or images;
                    # preserve existing cloud history when recording deletion.
                    cloud = {header: previous.get(header, '') for header in CCM_HEADERS}
                    cloud.update(Status='Deleted', **{'Updated At': revision})
                sheet.update(range_name=f'A{number}:R{number}', values=[[cloud[h] for h in CCM_HEADERS]], value_input_option='RAW')
            else:
                if any(r['ID Number'] == cloud['ID Number'] for r in ccm_sheet_records(values)):
                    raise CCMStorageError('This ID number already exists in Google. Local files were kept for review.')
                if snapshot.get('_cloud_revision') and snapshot['Status'] != 'Deleted':
                    # Backup is also recovery when a Sheets row was removed.
                    # Retained Drive references must still resolve to verified
                    # images before recreating the same account and ID number.
                    for side in ('photo', 'front', 'back'):
                        if side not in bodies:
                            file_id = cloud[side.title() + ' File ID']
                            if not file_id:
                                raise CCMStorageError('An original image backup is missing. Local files were kept for review.')
                            body = ccm_asset(snapshot, side)
                            info = ccm_verify_upload(file_id, body, allow_trashed=True)
                            if info.get('trashed'):
                                # A shared service account cannot untrash the
                                # owner's file. Back up its verified bytes again
                                # through the existing owner upload connection.
                                file_id = ccm_upload(key, side, body)
                                ccm_verify_upload(file_id, body)
                                cloud[side.title() + ' File ID'] = file_id
                sheet.append_row([cloud[h] for h in CCM_HEADERS], value_input_option='RAW')
            confirmed = ccm_sheet_records(sheet.get_all_values())
            saved = [r for r in confirmed if r['Record Key'] == key]
            if len(saved) != 1 or any(saved[0][h] != cloud[h] for h in CCM_HEADERS):
                raise CCMStorageError('Google Sheets has not confirmed the complete account. Local files were kept.')
            ccm_cache_write(confirmed)
        cleaned = False
        with ccm_local_lock(key):
            db.execute('BEGIN IMMEDIATE')
            current = db.execute('SELECT payload FROM ccm_pending_workers WHERE key=?', (key,)).fetchone()
            latest = json.loads(current['payload']) if current else None
            if latest and latest['Updated At'] == revision:
                # Persist verified Drive references before cleanup. If a file is
                # locked, retry can finish cleanup without losing an image source.
                if latest['Status'] == 'Deleted':
                    latest = {field: latest[field] for field in
                              ('Record Key', 'ID Number', 'Created At', 'Status', 'Updated At') if field in latest}
                else:
                    latest.update(cloud)
                latest['_cloud_revision'] = revision
                latest['_backup_confirmed_revision'] = revision
                db.execute('UPDATE ccm_pending_workers SET payload=? WHERE key=?', (json.dumps(latest), key))
                db.commit()
                ccm_remove_local_assets(key)
                db.execute('DELETE FROM ccm_pending_workers WHERE key=?', (key,))
                db.commit()
                cleaned = True
            elif latest:
                # A newer local edit remains pending; it starts from this confirmed cloud revision.
                latest['_cloud_revision'] = revision
                db.execute('UPDATE ccm_pending_workers SET payload=? WHERE key=?', (json.dumps(latest), key))
                db.commit()
            else:
                db.commit()
                cleaned = True
        return {'cleaned': cleaned, 'remaining': len(ccm_pending())}
    except Exception as exc:
        db.rollback()
        current = db.execute('SELECT payload FROM ccm_pending_workers WHERE key=?', (key,)).fetchone()
        if current and json.loads(current['payload'])['Updated At'] == revision:
            db.execute('UPDATE ccm_pending_workers SET error=? WHERE key=?', (str(exc)[:300], key))
            db.commit()
        if isinstance(exc, (CCMStorageError, CacheSyncError)):
            raise
        app.logger.warning('CCM backup could not be confirmed: %s', type(exc).__name__)
        raise CCMStorageError('The backup could not be confirmed. Local files were kept; retry Backup to Google.') from None
    finally:
        db.execute('DELETE FROM ccm_backup_leases WHERE key=? AND token=?', (key, token))
        db.commit()


@app.post('/ccm/admin/backup')
def ccm_backup_queue():
    ccm_guard()
    return jsonify(ok=True, keys=list(ccm_pending()))


@app.post('/ccm/admin/backup/<key>')
def ccm_backup_one(key):
    ccm_guard()
    try:
        return jsonify(ok=True, **ccm_backup_worker(key))
    except (CCMStorageError, CacheSyncError) as exc:
        return jsonify(ok=False, error=str(exc)), 503


@app.post('/ccm/admin/refresh')
def ccm_refresh():
    ccm_guard()
    try:
        _, stale = ccm_records(force=True)
        flash('Google is unavailable; local accounts are still available.' if stale else 'Backed-up accounts refreshed from Google.', 'error' if stale else 'success')
    except CCMStorageError as exc:
        flash(str(exc), 'error')
    return redirect(url_for('ccm_page', tab='admin', mode='manage'))


@app.post('/ccm/admin/delete/<key>')
def ccm_delete(key):
    ccm_guard()
    try:
        record, _ = ccm_worker(key)
        revision = record['Updated At']
        record.update({'Status': 'Deleted', 'Updated At': utc_now_iso()})
        ccm_store_local(record, expected_revision=revision)
        flash('Account removed from this device and QR invalidated. Back Up to Google will save the deletion to its history.', 'success')
    except (CCMStorageError, CacheSyncError) as exc:
        flash(str(exc), 'error')
    except Exception as exc:
        app.logger.warning('CCM deletion could not be confirmed: %s', type(exc).__name__)
        flash('The Google update could not be confirmed. Refresh the accounts list before trying again.', 'error')
    return redirect(url_for('ccm_page', tab='admin', mode='manage'))


@app.get('/ccm/image/<key>/<side>.png')
def ccm_image(key, side):
    if side not in ('photo', 'front', 'back'):
        abort(404)
    if side != 'photo' and not ccm_admin():
        abort(403)
    try:
        record, _ = ccm_worker(key)
        body = ccm_asset(record, side)
    except CCMStorageError:
        abort(503, 'Google Drive image unavailable. Try again shortly.')
    response = make_response(body)
    response.headers.update({'Content-Type': 'image/png', 'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff'})
    if request.args.get('download') == '1' and side != 'photo':
        response.headers['Content-Disposition'] = 'attachment; filename="' + record['ID Number'] + '-' + side + '.png"'
    return response


@app.get('/ccm/print/<key>')
def ccm_print(key):
    if not ccm_admin():
        return redirect(url_for('ccm_page', tab='admin'))
    try:
        record, _ = ccm_worker(key)
    except CCMStorageError as exc:
        abort(503, str(exc))
    response = make_response(render_template('ccm_print.html', worker=record))
    response.headers['Cache-Control'] = 'no-store'
    return response


@app.get('/ccm/verify/<key>')
def ccm_verify(key):
    if not re.fullmatch(r'[a-f0-9]{32}', key):
        abort(404)
    try:
        # Pending local changes are authoritative, including immediate revocation.
        # Once backed up, verification checks Google rather than attesting from cache.
        local = ccm_pending().get(key)
        if local:
            record, stale = (local if local['Status'] != 'Deleted' else None), False
        else:
            records, stale = ccm_records(force=True)
            record = next((r for r in records if r['Record Key'] == key and r['Status'] != 'Deleted'), None)
        response = make_response(render_template('ccm_verify.html', worker=record,
            verified=bool(record and not stale and record['Status'] == 'Active'
                          and not ccm_expired(record.get('Expiration Date'))), stale=stale), 200 if record or stale else 404)
    except CCMStorageError:
        response = make_response(render_template('ccm_verify.html', worker=None, verified=False, stale=True), 503)
    response.headers['Cache-Control'] = 'no-store'
    return response


# ========================
# Event registration — local records are authoritative.
# ========================

EVENT_HEADERS = ['Name', 'Bday', 'Contact #', 'Area', 'Church Name', 'Church Address',
                 'Pastor', 'Date created', 'Paid', 'Print', 'Release', 'Event']
EVENT_TYPES = ('District Convention', 'Youth Convention')
EVENT_PRINT_WIDTH_MM, EVENT_PRINT_HEIGHT_MM = 100, 148
EVENT_HARVEST_THEME = 'The Urgent Call For The Great Harvest'
EVENT_HARVEST_SCRIPTURE = 'Matt. 9:35–38'
EVENT_HARVEST_TEMPLATE = 'event/harvest-convention-2026-template.png'
EVENT_REGISTRATION_OPENS_AT = datetime.fromisoformat('2026-11-15T18:00:00+08:00')
EVENT_REGISTRATION_POSTER = 'event/general-convention-registration-opening.jpg'


def event_tables():
    db = get_db()
    db.executescript('''
        CREATE TABLE IF NOT EXISTS registration_events (
            id INTEGER PRIMARY KEY, key TEXT NOT NULL UNIQUE, name TEXT NOT NULL UNIQUE,
            kind TEXT NOT NULL, start_date TEXT NOT NULL, end_date TEXT NOT NULL,
            theme TEXT NOT NULL, scripture TEXT NOT NULL, background TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1, next_number INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
        CREATE UNIQUE INDEX IF NOT EXISTS registration_one_active_event ON registration_events(active) WHERE active=1;
        CREATE TABLE IF NOT EXISTS event_attendees (
            key TEXT PRIMARY KEY, event_id INTEGER NOT NULL REFERENCES registration_events(id),
            number INTEGER, name TEXT NOT NULL, birthday TEXT NOT NULL, contact TEXT NOT NULL DEFAULT '',
            area TEXT NOT NULL, church TEXT NOT NULL, address TEXT NOT NULL, pastor TEXT NOT NULL,
            new_church INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
            photo TEXT NOT NULL DEFAULT '', photo_drive_id TEXT NOT NULL DEFAULT '',
            complete INTEGER NOT NULL DEFAULT 0, paid INTEGER NOT NULL DEFAULT 0,
            printed INTEGER NOT NULL DEFAULT 0, released INTEGER NOT NULL DEFAULT 0,
            revision INTEGER NOT NULL DEFAULT 1, backed_up_revision INTEGER NOT NULL DEFAULT 0,
            sheet_row INTEGER, cloud_snapshot TEXT NOT NULL DEFAULT '', backup_error TEXT NOT NULL DEFAULT '',
            UNIQUE(event_id,number));
        CREATE TABLE IF NOT EXISTS event_request_limits (
            key TEXT PRIMARY KEY, started REAL NOT NULL, count INTEGER NOT NULL);
    ''')
    for table in ('registration_events','event_attendees'):
        if 'deleted_at' not in {r['name'] for r in db.execute(f'PRAGMA table_info({table})')}:
            try:
                db.execute(f"ALTER TABLE {table} ADD COLUMN deleted_at TEXT NOT NULL DEFAULT ''")
            except sqlite3.OperationalError:
                if 'deleted_at' not in {r['name'] for r in db.execute(f'PRAGMA table_info({table})')}:
                    raise
    if 'id_design' not in {r['name'] for r in db.execute('PRAGMA table_info(registration_events)')}:
        try:
            db.execute("ALTER TABLE registration_events ADD COLUMN id_design TEXT NOT NULL DEFAULT 'classic'")
        except sqlite3.OperationalError:
            if 'id_design' not in {r['name'] for r in db.execute('PRAGMA table_info(registration_events)')}:
                raise
    for column in ('id_drive_id','id_backup_hash'):
        if column not in {r['name'] for r in db.execute('PRAGMA table_info(event_attendees)')}:
            try:
                db.execute(f"ALTER TABLE event_attendees ADD COLUMN {column} TEXT NOT NULL DEFAULT ''")
            except sqlite3.OperationalError:
                if column not in {r['name'] for r in db.execute('PRAGMA table_info(event_attendees)')}:
                    raise
    if 'physical_printed' not in {r['name'] for r in db.execute('PRAGMA table_info(event_attendees)')}:
        try:
            # Legacy printed marks also meant downloaded; require explicit confirmation.
            db.execute('ALTER TABLE event_attendees ADD COLUMN physical_printed INTEGER NOT NULL DEFAULT 0')
        except sqlite3.OperationalError:
            if 'physical_printed' not in {r['name'] for r in db.execute('PRAGMA table_info(event_attendees)')}:
                raise
    db.executescript('''
        CREATE TABLE IF NOT EXISTS event_deletion_queue (
            key TEXT PRIMARY KEY, event_id INTEGER NOT NULL, payload TEXT NOT NULL,
            backup_error TEXT NOT NULL DEFAULT '');
        CREATE TABLE IF NOT EXISTS event_roster_versions (event_id INTEGER PRIMARY KEY, version INTEGER NOT NULL);
        CREATE INDEX IF NOT EXISTS event_attendee_roster ON event_attendees(event_id,complete,deleted_at,number);
        CREATE TRIGGER IF NOT EXISTS event_roster_insert AFTER INSERT ON event_attendees BEGIN
            INSERT INTO event_roster_versions VALUES (NEW.event_id,1)
            ON CONFLICT(event_id) DO UPDATE SET version=version+1;
        END;
        CREATE TRIGGER IF NOT EXISTS event_roster_update AFTER UPDATE OF
            name,number,area,church,address,photo,complete,paid,printed,physical_printed,released,revision,deleted_at
            ON event_attendees BEGIN
            INSERT INTO event_roster_versions VALUES (NEW.event_id,1)
            ON CONFLICT(event_id) DO UPDATE SET version=version+1;
        END;
        CREATE TRIGGER IF NOT EXISTS event_roster_delete AFTER DELETE ON event_attendees BEGIN
            INSERT INTO event_roster_versions VALUES (OLD.event_id,1)
            ON CONFLICT(event_id) DO UPDATE SET version=version+1;
        END;
        CREATE TRIGGER IF NOT EXISTS event_roster_event AFTER UPDATE ON registration_events BEGIN
            INSERT INTO event_roster_versions VALUES (NEW.id,1)
            ON CONFLICT(event_id) DO UPDATE SET version=version+1;
        END;
        CREATE TRIGGER IF NOT EXISTS event_roster_cleanup AFTER DELETE ON event_deletion_queue BEGIN
            INSERT INTO event_roster_versions VALUES (OLD.event_id,1)
            ON CONFLICT(event_id) DO UPDATE SET version=version+1;
        END;
    ''')
    db.commit()


def event_pending_deletions():
    return get_db().execute('''SELECT (SELECT COUNT(*) FROM event_deletion_queue)
        + (SELECT COUNT(*) FROM event_attendees WHERE deleted_at<>'')''').fetchone()[0]


def event_deleted_records():
    """Admin display labels for both legacy deleted rows and durable cleanup items."""
    db = get_db()
    records = {row['key']:dict(row) for row in db.execute("SELECT * FROM event_attendees WHERE deleted_at<>''")}
    for queued in db.execute('SELECT key,payload,backup_error FROM event_deletion_queue'):
        records[queued['key']] = {**json.loads(queued['payload']),'backup_error':queued['backup_error']}
    events = {row['id']:dict(row) for row in db.execute('SELECT id,kind,start_date FROM registration_events')}
    result = []
    for row in records.values():
        snapshot = json.loads(row.get('cloud_snapshot') or '{}')
        event = events.get(row['event_id'])
        event_name = event['kind']+' '+event['start_date'][:4] if event else 'Previous event'
        result.append(dict(key=row['key'],event_id=row['event_id'],event_name=event_name,
            number=row.get('number'),name=row.get('name') or snapshot.get('Name') or 'Unfinished registration',
            area=row.get('area',''),church=row.get('church',''),deleted_at=row.get('deleted_at',''),
            revision=row['revision'],error=row.get('backup_error',''),
            retry_url=url_for('event_backup_one',key=row['key']),
            forget_url=url_for('event_forget_deletion',key=row['key'])))
    return sorted(result,key=lambda row:(row['event_id'],row['deleted_at'],row['name'].casefold()))


def event_queue_deletion(attendee):
    """Keep only cloud cleanup identifiers, then remove the local registration."""
    fields = ('key','event_id','number','created_at','sheet_row','cloud_snapshot',
              'id_drive_id','photo_drive_id','revision','deleted_at','name','area','church')
    payload = {field:attendee[field] for field in fields}
    if payload['cloud_snapshot']:
        previous = json.loads(payload['cloud_snapshot'])
        payload['cloud_snapshot'] = json.dumps({field:previous.get(field,'') for field in ('Name','Bday','Event')})
    payload['deleted_at'] = payload['deleted_at'] or utc_now_iso()
    get_db().execute('INSERT OR IGNORE INTO event_deletion_queue(key,event_id,payload) VALUES (?,?,?)',
                     (attendee['key'],attendee['event_id'],json.dumps(payload)))
    get_db().execute('DELETE FROM event_attendees WHERE key=?',(attendee['key'],))


def event_admin():
    return session.get('event_admin_until', 0) > time.time()


def event_password_matches(value):
    return hmac.compare_digest(value.encode('utf-8'),b'EventAdminD4')


def event_meal_tables():
    get_db().executescript('''
        CREATE TABLE IF NOT EXISTS event_meal_sets (
            event_id INTEGER NOT NULL REFERENCES registration_events(id), key TEXT NOT NULL,
            name TEXT NOT NULL, revision INTEGER NOT NULL, definition_hash TEXT NOT NULL,
            master_id TEXT NOT NULL, updated_at TEXT NOT NULL, PRIMARY KEY(event_id,key));
        CREATE TABLE IF NOT EXISTS event_meal_sessions (
            event_id INTEGER NOT NULL, key TEXT NOT NULL, set_key TEXT NOT NULL,
            name TEXT NOT NULL, service_date TEXT NOT NULL, enabled INTEGER NOT NULL,
            updated_at TEXT NOT NULL, PRIMARY KEY(event_id,key),
            FOREIGN KEY(event_id,set_key) REFERENCES event_meal_sets(event_id,key));
        CREATE TABLE IF NOT EXISTS event_meal_claims (
            key TEXT PRIMARY KEY, event_id INTEGER NOT NULL REFERENCES registration_events(id),
            meal_key TEXT NOT NULL, ticket_key TEXT NOT NULL, served_at TEXT NOT NULL,
            scanner_id TEXT NOT NULL, master_id TEXT NOT NULL, received_at TEXT NOT NULL,
            status TEXT NOT NULL, payload_hash TEXT NOT NULL, delegate_name TEXT NOT NULL,
            delegate_area TEXT NOT NULL, delegate_church TEXT NOT NULL, meal_name TEXT NOT NULL,
            service_date TEXT NOT NULL);
        CREATE UNIQUE INDEX IF NOT EXISTS event_one_accepted_meal
            ON event_meal_claims(event_id,meal_key,ticket_key) WHERE status='accepted';
        CREATE INDEX IF NOT EXISTS event_meal_ticket_history ON event_meal_claims(event_id,ticket_key,served_at);
        CREATE TABLE IF NOT EXISTS event_meal_audit (
            key TEXT PRIMARY KEY, event_id INTEGER NOT NULL REFERENCES registration_events(id),
            claim_key TEXT NOT NULL, action TEXT NOT NULL, reason TEXT NOT NULL,
            occurred_at TEXT NOT NULL, received_at TEXT NOT NULL, master_id TEXT NOT NULL,
            payload_hash TEXT NOT NULL, previous_status TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS event_meal_batches (
            event_id INTEGER NOT NULL, kind TEXT NOT NULL, key TEXT NOT NULL, master_id TEXT NOT NULL,
            payload_hash TEXT NOT NULL, response_json TEXT NOT NULL, received_at TEXT NOT NULL,
            PRIMARY KEY(event_id,kind,key));
    ''')


def event_meal_hash(value):
    import hashlib
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()


def event_meal_text(value, label, limit=96, identifier=False):
    if not isinstance(value,str) or not value.strip() or len(value)>limit:
        raise ValueError('Invalid '+label+'.')
    if identifier and not re.fullmatch(r'[A-Za-z0-9_-]+',value):
        raise ValueError('Invalid '+label+'.')
    return value.strip()


def event_meal_time(value):
    value = event_meal_text(value,'timestamp',64)
    parsed = datetime.fromisoformat(value.replace('Z','+00:00'))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError('Timestamps must include a UTC offset.')
    return parsed.astimezone(timezone.utc).isoformat()


def event_master_api(view):
    @wraps(view)
    def wrapped(*args,**kwargs):
        from werkzeug.exceptions import HTTPException
        token = os.getenv('EVENT_MEAL_MASTER_TOKEN','').strip()
        if len(token)<32:
            return jsonify(ok=False,code='NOT_CONFIGURED',error='Master meal synchronization is not configured.'),503
        supplied = request.headers.get('Authorization','')
        if not supplied.startswith('Bearer ') or not hmac.compare_digest(supplied[7:].encode(),token.encode()):
            return jsonify(ok=False,code='UNAUTHORIZED',error='Master authorization required.'),401
        if request.content_length and request.content_length>512*1024:
            return jsonify(ok=False,code='TOO_LARGE',error='Sync requests must be at most 512 KiB.'),413
        g.event_master_id = os.getenv('EVENT_MEAL_MASTER_ID','master').strip() or 'master'
        try:
            event_meal_text(g.event_master_id,'configured master ID',identifier=True)
            event_tables()
            event_meal_tables()
            return view(*args,**kwargs)
        except (ValueError,HTTPException,sqlite3.IntegrityError) as exc:
            get_db().rollback()
            status = exc.code if isinstance(exc,HTTPException) else 409 if isinstance(exc,sqlite3.IntegrityError) else 400
            message = exc.description if isinstance(exc,HTTPException) else 'A conflicting meal record already exists.' if isinstance(exc,sqlite3.IntegrityError) else str(exc)
            return jsonify(ok=False,code='CONFLICT' if status==409 else 'INVALID_REQUEST',error=message),status
        except sqlite3.OperationalError:
            get_db().rollback()
            return jsonify(ok=False,code='RETRY',error='Synchronization is busy. Retry the same batch ID and payload.'),503
    return wrapped


def event_meal_event(key):
    if not re.fullmatch(r'[a-f0-9]{32}',key):
        abort(404,'Event not found.')
    row = get_db().execute("SELECT * FROM registration_events WHERE key=? AND deleted_at=''",(key,)).fetchone()
    if not row:
        abort(404,'Event not found.')
    return dict(row)


def event_meal_roster(event):
    rows = get_db().execute("""SELECT key,number,name,area,church,paid,printed,released,updated_at,revision
        FROM event_attendees WHERE event_id=? AND complete=1 AND deleted_at='' ORDER BY key""",(event['id'],))
    delegates = []
    for row in rows:
        item = dict(row)
        item['ticket_id'] = item.pop('key')
        item['meal_eligible'] = bool(item['paid'])
        item['processed'] = bool(item['paid'] and item['printed'] and item['released'])
        for field in ('paid','printed','released'):
            item[field] = bool(item[field])
        item['ticket_url'] = CCM_BASE_URL.rsplit('/ccm',1)[0]+'/event-registration/ticket/'+item['ticket_id']
        delegates.append(item)
    return dict(event_key=event['key'],snapshot=event_meal_hash(dict(event_key=event['key'],delegates=delegates)),mode='replace_event_snapshot',delegates=delegates)


@app.get('/event-registration/api/v1/events')
@event_master_api
def event_meal_api_events():
    rows = get_db().execute("SELECT key,name,start_date,end_date,active FROM registration_events WHERE deleted_at='' ORDER BY id DESC")
    return jsonify(ok=True,api_version=1,master_id=g.event_master_id,events=[dict(row) for row in rows])


@app.get('/event-registration/api/v1/events/<event_key>/delegates')
@app.get('/event-registration/api/v1/events/<event_key>/changes')
@event_master_api
def event_meal_api_delegates(event_key):
    result = event_meal_roster(event_meal_event(event_key))
    changed = request.args.get('cursor') != result['snapshot']
    if not changed:
        result['delegates'] = []
    return jsonify(ok=True,changed=changed,**result)


def event_meal_batch(event, kind, handler):
    payload = request.get_json(silent=True)
    if not isinstance(payload,dict):
        raise ValueError('Send a JSON object.')
    batch_id = event_meal_text(payload.get('batch_id'),'batch ID',identifier=True)
    items = payload.get(kind)
    if not isinstance(items,list) or not 1<=len(items)<=100 or any(not isinstance(item,dict) for item in items):
        raise ValueError('Send between 1 and 100 '+kind+'.')
    digest = event_meal_hash(payload)
    db = get_db()
    db.execute('BEGIN IMMEDIATE')
    # Recheck deletion inside the write transaction, after any competing edits.
    event_meal_event(event['key'])
    previous = db.execute('SELECT * FROM event_meal_batches WHERE event_id=? AND kind=? AND key=?',
                          (event['id'],kind,batch_id)).fetchone()
    if previous:
        if previous['payload_hash']!=digest or previous['master_id']!=g.event_master_id:
            abort(409,'Batch ID was already used with different content or a different master.')
        db.rollback()
        return jsonify(json.loads(previous['response_json']))
    result = dict(ok=True,batch_id=batch_id,results=handler(items),received_at=utc_now_iso())
    db.execute('INSERT INTO event_meal_batches VALUES (?,?,?,?,?,?,?)',
               (event['id'],kind,batch_id,g.event_master_id,digest,json.dumps(result),result['received_at']))
    db.commit()
    return jsonify(result)


@app.post('/event-registration/api/v1/events/<event_key>/meal-sets')
@event_master_api
def event_meal_api_sets(event_key):
    event = event_meal_event(event_key)
    def save(items):
        results = []
        for item in items:
            key = event_meal_text(item.get('set_id'),'meal set ID',identifier=True)
            name = event_meal_text(item.get('name'),'meal set name',150)
            revision = item.get('revision')
            sessions = item.get('sessions')
            if type(revision) is not int or revision<1 or not isinstance(sessions,list) or len(sessions)>1100:
                raise ValueError('Meal sets need a positive revision and at most 1100 sessions.')
            digest = event_meal_hash(item)
            old = get_db().execute('SELECT * FROM event_meal_sets WHERE event_id=? AND key=?',(event['id'],key)).fetchone()
            if old and (old['master_id']!=g.event_master_id or revision<old['revision'] or revision==old['revision'] and digest!=old['definition_hash']):
                abort(409,'Meal set revision is stale or was reused with different content.')
            if old and digest==old['definition_hash']:
                results.append(dict(set_id=key,revision=revision,status='unchanged'))
                continue
            now = utc_now_iso()
            get_db().execute('''INSERT INTO event_meal_sets VALUES (?,?,?,?,?,?,?) ON CONFLICT(event_id,key) DO UPDATE SET
                name=excluded.name,revision=excluded.revision,definition_hash=excluded.definition_hash,
                master_id=excluded.master_id,updated_at=excluded.updated_at''',
                             (event['id'],key,name,revision,digest,g.event_master_id,now))
            get_db().execute('UPDATE event_meal_sessions SET enabled=0,updated_at=? WHERE event_id=? AND set_key=?',(now,event['id'],key))
            seen = set()
            for meal in sessions:
                if not isinstance(meal,dict):
                    raise ValueError('Invalid meal session.')
                meal_key = event_meal_text(meal.get('session_id'),'meal session ID',identifier=True)
                meal_name = event_meal_text(meal.get('name'),'meal name',150)
                day = date.fromisoformat(event_meal_text(meal.get('date'),'meal date',10)).isoformat()
                if type(meal.get('enabled',True)) is not bool or meal_key in seen:
                    raise ValueError('Meal session IDs must be unique and enabled must be boolean.')
                seen.add(meal_key)
                other = get_db().execute('SELECT set_key,service_date FROM event_meal_sessions WHERE event_id=? AND key=?',(event['id'],meal_key)).fetchone()
                if other and other['set_key']!=key:
                    abort(409,'Meal session ID already belongs to another set.')
                if other and other['service_date']!=day and get_db().execute('SELECT 1 FROM event_meal_claims WHERE event_id=? AND meal_key=? LIMIT 1',(event['id'],meal_key)).fetchone():
                    abort(409,'Use a new session ID when changing the date of a meal with scan history.')
                get_db().execute('''INSERT INTO event_meal_sessions VALUES (?,?,?,?,?,?,?) ON CONFLICT(event_id,key) DO UPDATE SET
                    name=excluded.name,service_date=excluded.service_date,enabled=excluded.enabled,updated_at=excluded.updated_at''',
                                 (event['id'],meal_key,key,meal_name,day,int(meal.get('enabled',True)),now))
            results.append(dict(set_id=key,revision=revision,status='saved'))
        return results
    return event_meal_batch(event,'sets',save)


@app.post('/event-registration/api/v1/events/<event_key>/claims')
@event_master_api
def event_meal_api_claims(event_key):
    event = event_meal_event(event_key)
    def save(items):
        results = []
        for item in items:
            key = event_meal_text(item.get('claim_id'),'claim ID',identifier=True)
            ticket = event_meal_text(item.get('ticket_id'),'ticket ID',32)
            if not re.fullmatch(r'[a-f0-9]{32}',ticket):
                raise ValueError('ticket_id must be 32 lowercase hexadecimal characters.')
            meal_key = event_meal_text(item.get('session_id'),'meal session ID',identifier=True)
            scanner = event_meal_text(item.get('scanner_id'),'scanner ID',identifier=True)
            served = event_meal_time(item.get('served_at'))
            digest = event_meal_hash(item)
            old = get_db().execute('SELECT * FROM event_meal_claims WHERE key=?',(key,)).fetchone()
            if old:
                if old['payload_hash']!=digest or old['event_id']!=event['id'] or old['master_id']!=g.event_master_id:
                    abort(409,'Claim ID was reused with different content, event or master.')
                results.append(dict(claim_id=key,status=old['status']))
                continue
            meal = get_db().execute('SELECT * FROM event_meal_sessions WHERE event_id=? AND key=?',(event['id'],meal_key)).fetchone()
            person = get_db().execute("SELECT * FROM event_attendees WHERE event_id=? AND key=? AND complete=1 AND deleted_at=''",(event['id'],ticket)).fetchone()
            duplicate = get_db().execute("SELECT key FROM event_meal_claims WHERE event_id=? AND meal_key=? AND ticket_key=? AND status='accepted'",(event['id'],meal_key,ticket)).fetchone()
            # Enrolled scanner uploads describe meals already served offline;
            # disabling a meal afterward must not invalidate its scan history.
            status = 'invalid_session' if not meal or (not meal['enabled'] and not getattr(g, 'scanner_historical_upload', False)) else 'invalid_ticket' if not person else 'unpaid' if not person['paid'] else 'duplicate' if duplicate else 'accepted'
            get_db().execute('INSERT INTO event_meal_claims VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                (key,event['id'],meal_key,ticket,served,scanner,g.event_master_id,utc_now_iso(),status,digest,
                 person['name'] if person else '',person['area'] if person else '',person['church'] if person else '',
                 meal['name'] if meal else '',meal['service_date'] if meal else ''))
            results.append(dict(claim_id=key,status=status))
        return results
    return event_meal_batch(event,'claims',save)


@app.post('/event-registration/api/v1/events/<event_key>/audit')
@event_master_api
def event_meal_api_audit(event_key):
    event = event_meal_event(event_key)
    def save(items):
        results = []
        for item in items:
            key = event_meal_text(item.get('audit_id'),'audit ID',identifier=True)
            claim_key = event_meal_text(item.get('claim_id'),'claim ID',identifier=True)
            action = item.get('action')
            if action not in ('note','dispute','void_claim'):
                raise ValueError('Audit action must be note, dispute or void_claim.')
            reason = event_meal_text(item.get('reason'),'reason',500)
            occurred = event_meal_time(item.get('occurred_at'))
            digest = event_meal_hash(item)
            old = get_db().execute('SELECT * FROM event_meal_audit WHERE key=?',(key,)).fetchone()
            if old:
                if old['payload_hash']!=digest or old['event_id']!=event['id'] or old['master_id']!=g.event_master_id:
                    abort(409,'Audit ID was reused with different content, event or master.')
                results.append(dict(audit_id=key,status='recorded'))
                continue
            claim = get_db().execute('SELECT * FROM event_meal_claims WHERE key=? AND event_id=?',(claim_key,event['id'])).fetchone()
            if not claim:
                raise ValueError('Audit claim does not exist in this event.')
            get_db().execute('INSERT INTO event_meal_audit VALUES (?,?,?,?,?,?,?,?,?,?)',
                             (key,event['id'],claim_key,action,reason,occurred,utc_now_iso(),g.event_master_id,digest,claim['status']))
            if action=='void_claim':
                get_db().execute("UPDATE event_meal_claims SET status='voided' WHERE key=?",(claim_key,))
            results.append(dict(audit_id=key,status='recorded'))
        return results
    return event_meal_batch(event,'audit',save)


@app.get('/event-registration/api/v1/events/<event_key>/sync-status')
@event_master_api
def event_meal_api_status(event_key):
    event = event_meal_event(event_key)
    sets = [dict(row) for row in get_db().execute('SELECT key AS set_id,name,revision,updated_at FROM event_meal_sets WHERE event_id=?',(event['id'],))]
    sessions = [dict(row) for row in get_db().execute('SELECT key AS session_id,set_key AS set_id,name,service_date AS date,enabled FROM event_meal_sessions WHERE event_id=?',(event['id'],))]
    counts = {row['status']:row['n'] for row in get_db().execute('SELECT status,COUNT(*) AS n FROM event_meal_claims WHERE event_id=? GROUP BY status',(event['id'],))}
    last = get_db().execute('SELECT MAX(received_at) FROM event_meal_batches WHERE event_id=?',(event['id'],)).fetchone()[0]
    return jsonify(ok=True,event_key=event_key,master_id=g.event_master_id,sets=sets,sessions=sessions,claim_counts=counts,last_sync=last)


@app.get('/event-registration/admin/delegate/<key>')
def event_delegate_details(key):
    if not event_admin():
        abort(403)
    event_tables()
    event_meal_tables()
    person = event_attendee(key,admin=True)
    fields = ('key','number','name','birthday','contact','area','church','address','pastor','created_at','paid','printed','physical_printed','released')
    meals = [dict(row) for row in get_db().execute('''SELECT key AS claim_id,meal_name,service_date,served_at,scanner_id,status,received_at
        FROM event_meal_claims WHERE event_id=? AND ticket_key=? ORDER BY served_at DESC''',(person['event_id'],key))]
    audits = [dict(row) for row in get_db().execute('''SELECT a.key AS audit_id,a.claim_key AS claim_id,a.action,a.reason,a.occurred_at
        FROM event_meal_audit a JOIN event_meal_claims c ON c.key=a.claim_key
        WHERE a.event_id=? AND c.ticket_key=? ORDER BY a.received_at DESC''',(person['event_id'],key))]
    return jsonify(ok=True,delegate={field:person[field] for field in fields},meals=meals,audit=audits)


@app.post('/event-registration/admin/meals/backup')
def event_backup_meals():
    """Explicit RAW batch export; importing master scans never contacts Google."""
    from sheet_cache import _locked_sync
    from gspread.utils import rowcol_to_a1
    event_guard(admin=True)
    event_tables()
    event_meal_tables()
    event = get_db().execute('SELECT * FROM registration_events WHERE id=?',(request.form.get('event_id',type=int),)).fetchone()
    if not event:
        abort(404)
    claims = [dict(row) for row in get_db().execute('SELECT * FROM event_meal_claims WHERE event_id=? ORDER BY received_at,key',(event['id'],))]
    audits = [dict(row) for row in get_db().execute('SELECT * FROM event_meal_audit WHERE event_id=? ORDER BY received_at,key',(event['id'],))]
    if not claims and not audits:
        return jsonify(ok=True,claims=0,audit=0)
    specs = [
        ('Event Meals',['Event','Event Key','Claim ID','Ticket ID','Name','Area','Church','Meal Session ID','Meal','Service Date','Served At','Scanner','Master','Received At','Status'],
         [[event['kind']+' '+event['start_date'][:4],event['key'],r['key'],r['ticket_key'],r['delegate_name'],r['delegate_area'],r['delegate_church'],r['meal_key'],r['meal_name'],r['service_date'],r['served_at'],r['scanner_id'],r['master_id'],r['received_at'],r['status']] for r in claims],'Claim ID'),
        ('Event Meal Audit',['Event Key','Audit ID','Claim ID','Action','Reason','Occurred At','Received At','Master','Previous Status'],
         [[event['key'],r['key'],r['claim_key'],r['action'],r['reason'],r['occurred_at'],r['received_at'],r['master_id'],r['previous_status']] for r in audits],'Audit ID')]
    try:
        with _locked_sync(data_path('event-google-meal-backup')):
            book = get_gs_client().open('District4 Data')
            for title,headers,rows,identity in specs:
                if not rows:
                    continue
                try:
                    sheet = book.worksheet(title)
                except gspread.WorksheetNotFound:
                    sheet = book.add_worksheet(title=title,rows=max(1000,len(rows)+1),cols=len(headers))
                values = sheet.get_all_values()
                if not values:
                    if sheet.col_count<len(headers):
                        sheet.resize(cols=len(headers))
                    sheet.update(range_name='A1:'+rowcol_to_a1(1,len(headers)),values=[headers],value_input_option='RAW')
                    values = [headers]
                if values[0]!=headers:
                    raise CCMStorageError(title+' headers differ from the meal backup schema. Local records were kept.')
                column = headers.index(identity)
                indexed = {}
                for number,row in enumerate(values[1:],2):
                    if len(row)>column and row[column]:
                        if row[column] in indexed:
                            raise CCMStorageError('Duplicate meal record IDs found in '+title+'. Resolve them before retrying.')
                        indexed[row[column]] = number
                updates,appends = [],[]
                for row in rows:
                    number = indexed.get(row[column])
                    if number:
                        updates.append(dict(range='A'+str(number)+':'+rowcol_to_a1(number,len(headers)),values=[row]))
                    else:
                        appends.append(row)
                if updates:
                    sheet.batch_update(updates,value_input_option='RAW')
                if appends:
                    sheet.append_rows(appends,value_input_option='RAW')
                confirmed = {row[column]:row+['']*max(0,len(headers)-len(row)) for row in sheet.get_all_values()[1:] if len(row)>column and row[column]}
                if any(confirmed.get(row[column])!=row for row in rows):
                    raise CCMStorageError(title+' backup was not fully confirmed. Retry Backup Data; local records are safe.')
        return jsonify(ok=True,claims=len(claims),audit=len(audits))
    except Exception as exc:
        message = str(exc) if isinstance(exc,CCMStorageError) else 'Meal backup was not confirmed. Local meal and audit records are safe; retry Backup Data.'
        return jsonify(ok=False,error=message),503


def event_csrf():
    if not session.get('event_csrf'):
        session['event_csrf'] = secrets.token_urlsafe(32)
    return session['event_csrf']


def event_guard(admin=False, developer=False):
    if admin and not event_admin():
        abort(403)
    if developer and not is_cache_developer():
        abort(403)
    supplied = request.headers.get('X-CSRF-Token') or request.form.get('csrf_token', '')
    if not supplied or not hmac.compare_digest(supplied.encode(), session.get('event_csrf', '').encode()):
        abort(400, 'Reload Event Registration before trying again.')
    if request.content_length is not None and request.content_length > 13*1024*1024:
        abort(413, 'The picture is too large. Choose a picture smaller than 12 MB.')


def event_limit(label, maximum, seconds):
    import hashlib
    key = label + ':' + hashlib.sha256((request.remote_addr or '').encode()).hexdigest()
    now = time.time()
    db = get_db()
    db.execute('''INSERT INTO event_request_limits VALUES (?,?,1) ON CONFLICT(key) DO UPDATE SET
        count=CASE WHEN started<? THEN 1 ELSE count+1 END,
        started=CASE WHEN started<? THEN excluded.started ELSE started END''', (key,now,now-seconds,now-seconds))
    db.commit()
    if db.execute('SELECT count FROM event_request_limits WHERE key=?', (key,)).fetchone()[0] > maximum:
        abort(429, 'Too many attempts. Please try again later.')


def event_asset_path(filename):
    if not re.fullmatch(r'[a-f0-9]{64}\.png', filename or ''):
        abort(404)
    root = Path(data_path('event_assets'))
    root.mkdir(parents=True, exist_ok=True)
    return root / filename


def event_asset_write(body):
    import hashlib
    path = event_asset_path(hashlib.sha256(body).hexdigest() + '.png')
    if not path.exists():
        temporary = path.with_suffix('.' + uuid.uuid4().hex + '.tmp')
        try:
            with temporary.open('wb') as handle:
                handle.write(body)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)
    return path.name


def event_current():
    return get_db().execute("SELECT * FROM registration_events WHERE active=1 AND deleted_at='' ").fetchone()


def event_registration_opening(view):
    """Enforce the public launch on the server, including saved draft links."""
    @wraps(view)
    def wrapped(*args, **kwargs):
        if request.endpoint == 'event_registration' and request.args.get('tab') in ('admin','attendees'):
            return view(*args, **kwargs)
        if request.endpoint == 'event_image' and kwargs.get('side') != 'review':
            return view(*args, **kwargs)
        now = utc_now()
        if now >= EVENT_REGISTRATION_OPENS_AT or event_admin() or is_cache_developer():
            return view(*args, **kwargs)
        event_tables()
        active = event_current()
        if not active:
            return view(*args, **kwargs)
        if request.method not in ('GET','HEAD'):
            event_guard()
            return jsonify(ok=False,code='REGISTRATION_NOT_OPEN',
                error='Registration opens November 15, 2026 at 6:00 PM Philippine time.',
                opens_at=EVENT_REGISTRATION_OPENS_AT.isoformat(),redirect=url_for('event_registration')),423
        if request.endpoint == 'event_image':
            return jsonify(ok=False,code='REGISTRATION_NOT_OPEN',
                error='Registration has not opened yet.',opens_at=EVENT_REGISTRATION_OPENS_AT.isoformat()),423
        days, remainder = divmod(max(0,int((EVENT_REGISTRATION_OPENS_AT-now).total_seconds())),86400)
        hours, remainder = divmod(remainder,3600)
        minutes, seconds = divmod(remainder,60)
        return render_template('event_registration.html',view='opening',tab='registration',
            event=dict(active),csrf=event_csrf(),admin=False,developer=False,
            opening_at=EVENT_REGISTRATION_OPENS_AT.isoformat(),opening_server_ms=int(now.timestamp()*1000),
            opening_poster=EVENT_REGISTRATION_POSTER,
            opening_countdown=dict(days=days,hours=hours,minutes=minutes,seconds=seconds))
    return wrapped


@app.get('/event-registration/opening-status')
def event_opening_status():
    now = utc_now()
    return jsonify(opens_at=EVENT_REGISTRATION_OPENS_AT.isoformat(),server_now_ms=int(now.timestamp()*1000),
                   is_open=now>=EVENT_REGISTRATION_OPENS_AT,registration_url=url_for('event_registration'))


def event_background_path(event):
    """Use the saved artwork, with a recoverable fallback for the bundled design."""
    path = event_asset_path(event['background']) if event.get('background') else None
    if path and path.is_file():
        return path
    if event.get('id_design') == 'harvest_2026':
        bundled = Path(app.static_folder) / EVENT_HARVEST_TEMPLATE
        if bundled.is_file():
            return bundled
    raise CCMStorageError('The ID artwork for this event is missing. Open Admin > Event & backup > '
                          'Edit event / ID design, then upload the finished ID template picture and save. '
                          'Registration details and camera capture can still be completed.')


def event_attendee(key, admin=False, include_deleted=False):
    if not re.fullmatch(r'[a-f0-9]{32}', key or ''):
        abort(404)
    if not event_admin() and (admin or key != session.get('event_registration_key')):
        abort(403)
    row = get_db().execute('SELECT * FROM event_attendees WHERE key=?', (key,)).fetchone()
    if not row or (row['deleted_at'] and not include_deleted):
        abort(404)
    return dict(row)


def event_render_id(event, attendee=None, photo=None, background=None, temporary=False, review=False):
    """Fill a ready-made template or render the original event layout."""
    from PIL import Image, ImageDraw, ImageFont, ImageOps, ImageFilter
    from reportlab.graphics.barcode.qrencoder import QRCode, QRErrorCorrectLevel
    width, height = 1000, 1500
    ready_made = event.get('id_design', 'classic') in ('harvest_2026', 'ready_made')
    if background is None and (event.get('background') or ready_made):
        background = event_background_path(event).read_bytes()
    if background:
        with Image.open(io.BytesIO(background)) as source:
            card = ImageOps.fit(source.convert('RGB'), (width,height)).convert('RGBA')
    else:
        card = Image.new('RGBA', (width,height), '#16284e' if review and ready_made else '#f1e8d3')
    font_path = str(Path(app.static_folder) / 'ccm' / 'Tinos-Bold.ttf')

    def layout(value, box, initial, minimum, limit, font_file=font_path):
        available = box[2]-box[0]-20
        for size in range(initial, minimum-1, -1):
            font = ImageFont.truetype(font_file,size)
            lines = []
            for paragraph in str(value).splitlines() or ['']:
                line = ''
                for word in paragraph.split():
                    pieces = [word]
                    if font.getlength(word)>available:
                        pieces, piece = [], ''
                        for char in word:
                            if piece and font.getlength(piece+char)>available:
                                pieces.append(piece)
                                piece = ''
                            piece += char
                        pieces.append(piece)
                    for piece in pieces:
                        candidate = (line+' '+piece).strip()
                        if line and font.getlength(candidate) > available:
                            lines.append(line)
                            line = piece
                        else:
                            line = candidate
                if line:
                    lines.append(line)
            bounds = [font.getbbox(line,stroke_width=2) for line in lines]
            heights = [b[3]-b[1] for b in bounds]
            gap = max(4,round(size*.12))
            total = sum(heights)+max(0,len(lines)-1)*gap
            if lines and len(lines) <= limit and total <= box[3]-box[1]-16:
                return font,lines,bounds,heights,gap,total
        raise ValueError('Text is too long to print readably: '+str(value)[:65]+'. Shorten it or add suitable line breaks.')

    def text(value, box, initial, minimum=20, limit=2, color='#071e48', theme=False, glow=True, font_file=None):
        nonlocal card
        font,lines,bounds,heights,gap,total = layout(value,box,initial,minimum,limit,font_file or font_path)
        if glow:
            backing = Image.new('L',card.size,0)
            ImageDraw.Draw(backing).rounded_rectangle(box,radius=18,fill=145)
            backing = backing.filter(ImageFilter.GaussianBlur(13))
            white = Image.new('RGBA',card.size,'white')
            white.putalpha(backing)
            card = Image.alpha_composite(card,white)
        positions, y = [],box[1]+(box[3]-box[1]-total)/2
        mask = Image.new('L',card.size,0)
        for line,bound,line_height in zip(lines,bounds,heights):
            position = ((box[0]+box[2]-(bound[2]-bound[0]))/2-bound[0],y-bound[1])
            ImageDraw.Draw(mask).text(position,line,font=font,fill=245,stroke_width=4,stroke_fill=245)
            positions.append(position)
            y += line_height+gap
        if glow:
            white = Image.new('RGBA',card.size,'white')
            white.putalpha(mask.filter(ImageFilter.GaussianBlur(5)))
            card = Image.alpha_composite(card,white)
        draw = ImageDraw.Draw(card)
        for index,(line,position) in enumerate(zip(lines,positions)):
            fill = '#740b12' if theme and index == len(lines)-1 else color
            draw.text(position,line,font=font,fill=fill,stroke_width=2 if theme else 0,stroke_fill='#d5a43c')

    if not ready_made:
        # A lightly frosted, translucent panel keeps the uploaded background visible.
        panel_box = (38,28,962,638)
        frosted = card.crop(panel_box).filter(ImageFilter.GaussianBlur(7))
        frosted = Image.alpha_composite(frosted,Image.new('RGBA',frosted.size,(255,255,255,100)))
        panel_mask = Image.new('L',frosted.size,0)
        ImageDraw.Draw(panel_mask).rounded_rectangle((0,0,frosted.width-1,frosted.height-1),radius=28,fill=255)
        card.paste(frosted,panel_box[:2],panel_mask)
        border = Image.new('RGBA',card.size)
        ImageDraw.Draw(border).rounded_rectangle((38,28,961,637),radius=28,outline=(255,255,255,175),width=2)
        card = Image.alpha_composite(card,border)
        organization = 'INTERNATIONAL ONE WAY\nOUTREACH · DISTRICT 4'
        organization_layout = layout(organization,(0,0,752,118),52,40,2)
        organization_height = organization_layout[-1]
        organization_width = max(bound[2]-bound[0] for bound in organization_layout[2])
        with Image.open(Path(app.static_folder)/'img'/'logo.png') as original:
            logo = original.convert('RGBA')
            logo = logo.crop(logo.getbbox())
            logo.thumbnail((organization_height,organization_height),Image.Resampling.LANCZOS)
            # Center the actual logo and lettering together, keeping a 14px visual gap.
            logo_x = round((width-logo.width-14-organization_width)/2)
            organization_x = logo_x+logo.width+14
            organization_box = (organization_x-10,48,organization_x+organization_width+10,166)
            card.alpha_composite(logo,(logo_x,round((organization_box[1]+organization_box[3]-logo.height)/2)))
        text(organization,organization_box,52,40,2,glow=False)
        year = date.fromisoformat(event['start_date']).year
        title = ('General Convention' if event['kind']=='District Convention' else 'Youth Convention')+' '+str(year)
        text(title,(90,166,910,254),72,36,1,color='#740b12',glow=False,
             font_file=str(Path(app.static_folder)/'fonts'/'Kalam-Regular.ttf'))
        text('THEME',(365,254,635,294),28,20,1,color='#740b12',glow=False)
        text(event['theme'],(60,294,940,532),96,30,5,theme=True,glow=False)
        text(event['scripture'],(80,538,920,630),42,22,3,glow=False)
    draw = ImageDraw.Draw(card)
    portrait_box = (329,624,671,921) if ready_made else (306,654,694,1037)
    portrait_size = (portrait_box[2]-portrait_box[0],portrait_box[3]-portrait_box[1])
    if not ready_made:
        draw.rounded_rectangle((300,648,700,1043),radius=24,fill='#fffdf8',outline='#c99b2c',width=5)
    if photo:
        with Image.open(io.BytesIO(photo)) as source:
            portrait = ImageOps.fit(ImageOps.exif_transpose(source).convert('RGB'),portrait_size,
                                    centering=(.5,1) if ready_made else (.5,.4))
        mask = Image.new('L',portrait.size,0)
        ImageDraw.Draw(mask).rounded_rectangle((0,0,portrait.width-1,portrait.height-1),radius=34 if ready_made else 18,fill=255)
        card.paste(portrait,portrait_box[:2],mask)
    elif ready_made:
        draw.ellipse((443,675,557,789),fill='#d8c4bb')
        draw.rounded_rectangle((386,801,614,893),radius=45,fill='#d8c4bb')
    else:
        draw.ellipse((420,726,580,886),fill='#b9c3ce')
        draw.rounded_rectangle((354,899,646,1020),radius=55,fill='#b9c3ce')
    attendee = attendee or {'name':'SAMPLE ATTENDEE','area':'4','church':'Sample Church','key':'0'*32}
    if ready_made:
        text(attendee['name'],(130,940,870,1018),62,25,2,color='white',glow=False)
        text('AREA '+attendee['area'],(380,1025,620,1067),37,22,1,color='#740b12',glow=False)
        text(attendee['church'],(125,1069,875,1121),42,20,2,color='white',glow=False)
    else:
        text(attendee['name'],(45,1055,955,1140),65,25,2)
        text('Area '+attendee['area'],(180,1152,820,1197),37,22,1)
        text(attendee['church'],(80,1204,920,1270),39,22,2)
    if not review:
        qr = QRCode(None,QRErrorCorrectLevel.M)
        qr.addData(CCM_BASE_URL.rsplit('/ccm',1)[0]+'/event-registration/ticket/'+attendee['key'])
        qr.make()
        qr_size = 324 if ready_made else 200
        scale = max(1,qr_size//(qr.moduleCount+8))
        qr_image = Image.new('RGB',((qr.moduleCount+8)*scale,)*2,'white')
        qdraw = ImageDraw.Draw(qr_image)
        for y,row in enumerate(qr.modules):
            for x,dark in enumerate(row):
                if dark:
                    qdraw.rectangle(((x+4)*scale,(y+4)*scale,(x+5)*scale-1,(y+5)*scale-1),fill='black')
        if ready_made:
            # Fill the template's QR panel, including its four-module quiet zone.
            # Nearest-neighbor keeps the enlarged modules pure black and white.
            qr_image = qr_image.resize((qr_size,qr_size),Image.Resampling.NEAREST)
        qr_y = 1140+(324-qr_image.height)//2 if ready_made else 1290+(200-qr_image.height)//2
        card.paste(qr_image,((width-qr_image.width)//2,qr_y))
    if temporary:
        # Keep the portrait, name and QR unobstructed. Printable originals never use this banner.
        draw = ImageDraw.Draw(card)
        draw.rounded_rectangle((40,760,280,900),radius=12,fill='#fff0c4',outline='#9a5700',width=3)
        draw.multiline_text((160,830),'TEMPORARY\nID',font=ImageFont.truetype(font_path,34),
                            anchor='mm',align='center',fill='#773c00')
    if review:
        # Baked into the pixels, so hiding webpage elements cannot remove the
        # watermark or reveal a QR. Attendee previews never generate a QR code.
        qr_box = (345,1180,655,1415) if ready_made else (390,1300,610,1480)
        text('PREVIEW ONLY\nNOT VALID\nFOR ENTRY',qr_box,32,22,3,color='#742334',glow=False)
        watermark = Image.new('RGBA',card.size)
        watermark_draw = ImageDraw.Draw(watermark)
        watermark_font = ImageFont.truetype(str(Path(app.static_folder)/'fonts'/'NotoSans-Bold.ttf'),64)
        for y in range(-200,height+200,240):
            for x in range(-300,width+300,650):
                watermark_draw.text((x,y),'PREVIEW ONLY',font=watermark_font,
                                    fill=(255,255,255,115),stroke_width=2,stroke_fill=(8,24,50,65))
        card = Image.alpha_composite(card,watermark.rotate(32,resample=Image.Resampling.BICUBIC))
        banner = ImageDraw.Draw(card)
        banner.rectangle((0,0,width,54),fill='#10254e')
        banner.text((width//2,27),'PREVIEW ONLY - NOT VALID FOR ENTRY',
                    font=ImageFont.truetype(font_path,27),fill='white',anchor='mm')
        card = card.resize((600,900),Image.Resampling.LANCZOS)
    buffer = io.BytesIO()
    card.convert('RGB').save(buffer,'PNG',dpi=(96,96) if review else
                            (width*25.4/EVENT_PRINT_WIDTH_MM,height*25.4/EVENT_PRINT_HEIGHT_MM),compress_level=3)
    return buffer.getvalue()


def event_form_config():
    event_id = request.form.get('event_id','')
    old = get_db().execute("SELECT * FROM registration_events WHERE id=? AND deleted_at=''",(event_id,)).fetchone() if event_id else None
    if event_id and not old:
        abort(404)
    config = dict(old) if old else {}
    upload = request.files.get('background')
    has_upload = bool(upload and upload.filename)
    # Cached original forms retain their renderer. The current form explicitly
    # uses a finished template; its theme/reference fields are event metadata.
    legacy_form = 'theme' in request.form or 'scripture' in request.form
    design = request.form.get('id_design',config.get('id_design','classic') if legacy_form else 'ready_made')
    if design not in ('classic','harvest_2026','ready_made'):
        raise ValueError('Choose an available ID design.')
    if design == 'ready_made' and not has_upload and config.get('id_design') == 'harvest_2026':
        design = 'harvest_2026'
    kind = request.form.get('kind','')
    if kind not in EVENT_TYPES:
        raise ValueError('Choose District Convention or Youth Convention.')
    try:
        start = date.fromisoformat(request.form.get('start_date',''))
        end = date.fromisoformat(request.form.get('end_date',''))
        if end < start:
            raise ValueError
    except ValueError:
        raise ValueError('Choose valid event dates; the end must be on or after the start.') from None
    if design == 'harvest_2026':
        defaults = {'theme':EVENT_HARVEST_THEME,'scripture':EVENT_HARVEST_SCRIPTURE}
    else:
        defaults = config if not has_upload else {}
    theme,scripture = (request.form.get(field,defaults.get(field,'')).strip() for field in ('theme','scripture'))
    if not theme or not scripture or len(theme)>600 or len(scripture)>500:
        raise ValueError('Enter a theme (up to 600 characters) and Bible verse/reference (up to 500 characters).')
    if request.form.get('id_design') == 'harvest_2026' and (kind != 'District Convention' or start.year != 2026 or
                                    theme != EVENT_HARVEST_THEME or scripture != EVENT_HARVEST_SCRIPTURE):
        raise ValueError('The Harvest template is for General Convention 2026, “The Urgent Call For The Great Harvest”, Matt. 9:35–38. Upload an updated template to use different event text.')
    config.update(kind=kind,name=kind+' '+str(start.year),start_date=start.isoformat(),end_date=end.isoformat(),
                  theme=theme,scripture=scripture,id_design=design)
    if old and old['name']!=config['name'] and get_db().execute('SELECT 1 FROM event_attendees WHERE event_id=?',(old['id'],)).fetchone():
        raise ValueError('An event with registrations must keep its original event type and year.')
    if design == 'harvest_2026':
        background = (Path(app.static_folder) / EVENT_HARVEST_TEMPLATE).read_bytes()
    else:
        background = ccm_photo(upload) if has_upload else None
        # Different layouts cannot reuse artwork with incompatible field positions.
        if not background and old and old['id_design'] != design:
            raise ValueError('Upload a finished ID template picture with blank attendee spaces.')
    if design == 'classic':
        # The downloadable template already has all event lettering and frames.
        # Reject it as an original-layout background, including a saved upload.
        artwork = background
        if artwork is None and config.get('background'):
            saved = event_asset_path(config['background'])
            if saved.is_file():
                artwork = saved.read_bytes()
        if artwork:
            from PIL import Image, ImageOps
            with Image.open(io.BytesIO(artwork)) as uploaded, \
                 Image.open(Path(app.static_folder) / EVENT_HARVEST_TEMPLATE) as bundled:
                expected = ImageOps.exif_transpose(bundled).convert('RGB')
                expected.thumbnail(uploaded.size)
                if expected.size == uploaded.size and expected.tobytes() == uploaded.convert('RGB').tobytes():
                    raise ValueError('This picture is the ready-made Harvest template. Reload the event editor '
                                     'and upload it as the ID template picture to align the attendee fields '
                                     'without duplicating event text.')
    if not background and not config.get('background'):
        raise ValueError('Upload the finished ID template picture for this event.')
    if design == 'ready_made' and background:
        from PIL import Image
        with Image.open(io.BytesIO(background)) as artwork:
            if abs(artwork.width / artwork.height - 2/3) > .025:
                raise ValueError('Use a portrait template with a 2:3 width-to-height ratio and the same field positions as the Harvest template.')
    return config,background


def event_directory():
    # A missing church name must use New church; never print an address as its name.
    areas = [r[0] for r in get_db().execute("SELECT DISTINCT TRIM(age) FROM sheet_accounts_cache WHERE TRIM(COALESCE(age,''))<>''")]
    areas = sorted((area for area in areas if not (area.isdigit() and int(area)==0)),
                   key=lambda v:(not v.isdigit(),int(v) if v.isdigit() else v.casefold()))
    overseers = {}
    for row in get_db().execute("""SELECT name,TRIM(age) area,church_address FROM sheet_accounts_cache
            WHERE LOWER(TRIM(position))='area overseer' ORDER BY sheet_row"""):
        overseers.setdefault(row['area'],row)
    churches = []
    for area in areas:
        leader = overseers.get(area)
        churches.append(dict(key='area-overseer:'+area,area=area,church_id='Area Overseer',role='area_overseer',
                             address=(leader['church_address'] or '') if leader else '',pastor=(leader['name'] or '') if leader else ''))
    churches.extend(church for church in ccm_churches() if church['church_id'].strip() and church['area'] in areas)
    return churches, areas


def event_registration_name(name, area, church, minister=False):
    """Use one Ptr. title for declared ministers and matching pastor/overseer Accounts."""
    def without_title(value):
        return re.sub(r'^(?:(?:ptr\.|rev\.)\s*|(?:ptr|pastor|rev|reverend)\s+)+','', ' '.join(value.split()),flags=re.I).strip()

    name = ' '.join(name.split())
    base = without_title(name)
    if not base:
        raise ValueError('Enter your full name after the title.')
    has_title = base != name
    known_minister = any(without_title(row['name'] or '').casefold()==base.casefold()
                         and (row['sex'] or '').strip().casefold()==church.casefold()
        for row in get_db().execute("""SELECT name,sex FROM sheet_accounts_cache WHERE TRIM(age)=?
            AND LOWER(TRIM(position)) IN ('pastor','area overseer')""",(area,)))
    result = 'Ptr. '+base if minister or has_title or known_minister else name
    if len(result)>150:
        raise ValueError('Your full name, including Ptr., must be at most 150 characters.')
    return result


@app.get('/event-registration')
@event_registration_opening
def event_registration():
    event_tables()
    tab = request.args.get('tab','registration')
    if tab not in ('registration','attendees','admin'):
        tab = 'registration'
    developer, admin = is_cache_developer(), event_admin()
    active = event_current()
    events = [dict(r) for r in get_db().execute("SELECT * FROM registration_events WHERE deleted_at='' ORDER BY id DESC")] if admin or developer else []
    chosen = active
    if tab=='admin' and admin and not chosen and events:
        chosen = events[0]
    if tab == 'admin' and admin and request.args.get('event', type=int):
        chosen = next((e for e in events if e['id'] == request.args.get('event', type=int)), None)
    attendees = [dict(r) for r in get_db().execute("SELECT * FROM event_attendees WHERE event_id=? AND complete=1 AND deleted_at='' ORDER BY number",
                 (chosen['id'],))] if chosen and (tab=='attendees' or tab=='admin' and admin) else []
    registered_count = get_db().execute("SELECT COUNT(*) FROM event_attendees WHERE event_id=? AND complete=1 AND deleted_at=''",
                                      (chosen['id'],)).fetchone()[0] if chosen else 0
    meal_sessions, meal_totals = [], []
    if tab=='admin' and admin:
        event_meal_tables()
        if chosen:
            meal_sessions = [dict(row) for row in get_db().execute('''SELECT s.key,s.name,s.service_date,s.enabled,
                SUM(CASE WHEN c.status='accepted' THEN 1 ELSE 0 END) AS served
                FROM event_meal_sessions s LEFT JOIN event_meal_claims c ON c.event_id=s.event_id AND c.meal_key=s.key
                WHERE s.event_id=? GROUP BY s.key ORDER BY s.service_date,s.name''',(chosen['id'],))]
            meal_totals = [dict(row) for row in get_db().execute('SELECT status,COUNT(*) AS count FROM event_meal_claims WHERE event_id=? GROUP BY status',(chosen['id'],))]
    churches, areas = event_directory() if tab == 'registration' and active else ([], [])
    if active and session.get('event_form_event_key')!=active['key']:
        legacy_same_event = not session.get('event_form_event_key') and session.get('event_form_event_id')==active['id']
        if not legacy_same_event:
            session.pop('event_form_key',None)
            session.pop('event_registration_key',None)
        session['event_form_event_id'] = active['id']
        session['event_form_event_key'] = active['key']
    if request.args.get('new') == '1':
        session.pop('event_registration_key', None)
        session.pop('event_form_key', None)
    if not session.get('event_form_key'):
        session['event_form_key'] = uuid.uuid4().hex
    draft = None
    if session.get('event_registration_key') and active:
        row = get_db().execute("SELECT * FROM event_attendees WHERE key=? AND event_id=? AND deleted_at=''",
                              (session['event_registration_key'], active['id'])).fetchone()
        draft = dict(row) if row else None
        if not row:
            session.pop('event_registration_key',None)
            session['event_form_key'] = uuid.uuid4().hex
    return render_template('event_registration.html', view='main', tab=tab, active=dict(active) if active else None,
        event=dict(chosen) if chosen else None, events=events, attendees=attendees, admin=admin, developer=developer,
        csrf=event_csrf(), churches=churches, areas=areas, draft=draft, form_key=session['event_form_key'],
        today=utc_now().astimezone(PH_TZ or timezone(timedelta(hours=8))).date().isoformat(), event_types=EVENT_TYPES,
        meal_sessions=meal_sessions,meal_totals=meal_totals,
        registered_count=registered_count,
        completed_count=sum(bool(p['paid'] and p['physical_printed'] and p['released']) for p in attendees),
        pending_deletions=event_pending_deletions() if admin else 0,
        registration_preview=bool(active and tab=='registration' and utc_now()<EVENT_REGISTRATION_OPENS_AT
                                  and (admin or developer)),
        opening_at=EVENT_REGISTRATION_OPENS_AT.isoformat())


@app.get('/event-registration/progress')
def event_progress():
    """O(1) version check; roster data is returned only after a change."""
    admin = request.args.get('admin') == '1'
    if admin and not event_admin():
        return jsonify(ok=False,error='Unlock Admin again to resume updates.'),403
    db = get_db()
    # One SQLite read snapshot ties the version to the exact returned roster.
    db.execute('BEGIN')
    try:
        event = db.execute("SELECT id,key FROM registration_events WHERE id=? AND deleted_at=''",
                           (request.args.get('event',type=int),)).fetchone() if admin else event_current()
        event_id = event['id'] if event else None
        version = db.execute('SELECT version FROM event_roster_versions WHERE event_id=?',(event_id,)).fetchone()
        cursor = (event['key'] if event else 'none')+':'+str(version[0] if version else 0)
        changed = request.args.get('cursor') != cursor
        result = dict(ok=True,changed=changed,cursor=cursor,event_id=event_id)
        if changed:
            fields = 'key,number,name,area,church,address,photo,paid,printed,physical_printed,released,revision' if admin else 'number,name,area,church'
            people = [dict(row) for row in db.execute(f'''SELECT {fields} FROM event_attendees
                WHERE event_id=? AND complete=1 AND deleted_at='' ORDER BY number''',(event_id,))]
            result.update(count=len(people))
            if admin:
                for person in people:
                    person.update(status_url=url_for('event_status',key=person['key']),
                        delete_url=url_for('event_delete_attendee',key=person['key']),
                        details_url=url_for('event_delegate_details',key=person['key']),
                        photo_url=url_for('event_image',key=person['key'],side='photo'),
                        print_url=url_for('event_print',key=person['key']),
                        release_url=url_for('event_release',key=person['key']))
                    person.pop('photo')
                result['pending_deletions'] = event_pending_deletions()
            if request.args.get('list') == '1' or admin:
                result['attendees'] = people
        return jsonify(result)
    finally:
        db.rollback()



@app.post('/event-registration/admin/login')
def event_login():
    event_guard()
    event_tables()
    event_limit('login',10,600)
    if event_password_matches(request.form.get('password','')):
        session['event_admin_until'] = time.time()+4*3600
        session['event_csrf'] = secrets.token_urlsafe(32)
    else:
        flash('Incorrect event admin password.','error')
    return redirect(url_for('event_registration',tab='admin'))


@app.post('/event-registration/admin/logout')
def event_logout():
    event_guard(admin=True)
    session.pop('event_admin_until',None)
    session.pop('event_csrf',None)
    return redirect(url_for('event_registration',tab='admin'))


@app.post('/event-registration/admin/event/preview')
def event_preview():
    event_guard(developer=True)
    event_tables()
    try:
        config,background = event_form_config()
        return send_file(io.BytesIO(event_render_id(config,background=background)),mimetype='image/png')
    except (ValueError,CCMStorageError) as exc:
        return jsonify(ok=False,error=str(exc)),400


@app.post('/event-registration/admin/event/save')
def event_save():
    event_guard(developer=True)
    event_tables()
    try:
        config,background = event_form_config()
        event_render_id(config,background=background)
        if background:
            config['background'] = event_asset_write(background)
        now = utc_now_iso()
        db = get_db()
        db.execute('BEGIN IMMEDIATE')
        try:
            if config.get('id'):
                changed = db.execute('''UPDATE registration_events SET name=?,kind=?,start_date=?,end_date=?,theme=?,scripture=?,background=?,id_design=?,updated_at=?
                              WHERE id=? AND deleted_at='' ''',tuple(config[f] for f in ('name','kind','start_date','end_date','theme','scripture','background','id_design'))+(now,config['id']))
                if not changed.rowcount:
                    raise ValueError('This event was deleted. Reload the admin page.')
            else:
                db.execute('UPDATE registration_events SET active=0 WHERE active=1')
                db.execute('''INSERT INTO registration_events
                    (key,name,kind,start_date,end_date,theme,scripture,background,id_design,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)''',
                    (uuid.uuid4().hex,)+tuple(config[f] for f in ('name','kind','start_date','end_date','theme','scripture','background','id_design'))+(now,now))
            db.commit()
        except Exception:
            db.rollback()
            raise
        return jsonify(ok=True,redirect=url_for('event_registration',tab='admin'))
    except sqlite3.IntegrityError:
        return jsonify(ok=False,error='An event with that type and year already exists. Edit that event instead.'),400
    except (ValueError,CCMStorageError) as exc:
        return jsonify(ok=False,error=str(exc)),400


@app.get('/event-registration/background/<int:event_id>.png')
def event_background(event_id):
    event_tables()
    row = get_db().execute('SELECT * FROM registration_events WHERE id=?',(event_id,)).fetchone()
    if not row or row['deleted_at'] or not (row['active'] or event_admin() or is_cache_developer()):
        abort(404)
    try:
        artwork = event_background_path(dict(row))
    except CCMStorageError:
        abort(404)
    return send_file(artwork,mimetype='image/png')


@app.post('/event-registration/details')
@event_registration_opening
def event_details():
    event_guard()
    event_tables()
    event_limit('registration',600,3600)
    active = event_current()
    if not active or str(active['id'])!=request.form.get('event_id') or active['key']!=request.form.get('event_key'):
        return jsonify(ok=False,error='The active event changed. Reload the page before registering.'),409
    key = request.form.get('form_key','')
    if not re.fullmatch(r'[a-f0-9]{32}',key) or key!=session.get('event_form_key'):
        abort(400)
    try:
        def field(name,limit,optional=False):
            value = ' '.join(request.form.get(name,'').split())
            if (not value and not optional) or len(value)>limit:
                raise ValueError('Enter a valid '+name.replace('_',' ')+'.')
            return value
        name,contact = field('name',150),field('contact',60,True)
        try:
            birthday = date.fromisoformat(request.form.get('birthday',''))
            today = utc_now().astimezone(PH_TZ or timezone(timedelta(hours=8))).date()
            if birthday<date(1900,1,1) or birthday>today:
                raise ValueError
        except ValueError:
            raise ValueError('Enter a valid birthday that is not in the future.') from None
        churches,areas = event_directory()
        area = field('area',30)
        if area not in areas:
            raise ValueError('Choose an area from the district records.')
        new = request.form.get('new_church')=='1'
        area_overseer = False
        if new:
            church,address,pastor = field('church',200),field('address',300),field('pastor',150)
        else:
            selected = next((c for c in churches if c['key']==request.form.get('church_key') and c['area']==area),None)
            if not selected:
                raise ValueError('Choose a church or check New church.')
            church,address,pastor = selected['church_id'],selected['address'],selected['pastor']
            area_overseer = selected.get('role') == 'area_overseer'
        name = event_registration_name(name,area,church,request.form.get('minister')=='1' or area_overseer)
        # Validate printable text independently of artwork availability. A missing
        # background must not prevent saving details and reaching camera capture.
        event_render_id(dict(active),dict(name=name,area=area,church=church,key=key),background=b'')
        now = utc_now_iso()
        db = get_db()
        db.execute('BEGIN IMMEDIATE')
        try:
            current_event = db.execute('SELECT id FROM registration_events WHERE active=1').fetchone()
            if not current_event or current_event['id']!=active['id']:
                raise ValueError('The active event changed. Reload the registration page.')
            old = db.execute('SELECT * FROM event_attendees WHERE key=?',(key,)).fetchone()
            if old and (old['event_id']!=active['id'] or old['deleted_at']):
                raise ValueError('Reload the registration form for the current event.')
            if not old:
                db.execute('''INSERT INTO event_attendees
                    (key,event_id,name,birthday,contact,area,church,address,pastor,new_church,created_at,updated_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)''',
                    (key,active['id'],name,birthday.isoformat(),contact,area,church,address,pastor,int(new),now,now))
            elif not old['complete']:
                db.execute('''UPDATE event_attendees SET name=?,birthday=?,contact=?,area=?,church=?,address=?,pastor=?,new_church=?,updated_at=?,revision=revision+1
                              WHERE key=?''',(name,birthday.isoformat(),contact,area,church,address,pastor,int(new),now,key))
            db.commit()
        except Exception:
            db.rollback()
            raise
        session['event_registration_key'] = key
        saved = db.execute('SELECT photo,complete FROM event_attendees WHERE key=?',(key,)).fetchone()
        return jsonify(ok=True,redirect=url_for('event_review_page' if saved['photo'] and not saved['complete'] else 'event_photo_page',key=key))
    except (ValueError,CCMStorageError) as exc:
        return jsonify(ok=False,error=str(exc)),400


@app.get('/event-registration/photo/<key>')
@event_registration_opening
def event_photo_page(key):
    event_tables()
    attendee = event_attendee(key)
    event = dict(get_db().execute('SELECT * FROM registration_events WHERE id=?',(attendee['event_id'],)).fetchone())
    return render_template('event_registration.html',view='complete' if attendee['complete'] else 'photo',
                           attendee=attendee,event=event,csrf=event_csrf(),tab='registration',admin=event_admin(),developer=False)


@app.post('/event-registration/photo/<key>')
@event_registration_opening
def event_photo_save(key):
    event_guard()
    event_tables()
    attendee = event_attendee(key)
    if attendee['complete']:
        return jsonify(ok=True,redirect=url_for('event_photo_page',key=key))
    if request.form.get('camera_capture')!='1':
        return jsonify(ok=False,error='Take your picture using the registration camera.'),400
    try:
        photo = ccm_photo(request.files.get('photo'))
        filename = event_asset_write(photo)
        db = get_db()
        db.execute('BEGIN IMMEDIATE')
        try:
            active = db.execute('SELECT * FROM registration_events WHERE active=1').fetchone()
            if not active or active['id']!=attendee['event_id']:
                raise ValueError('Registration for this event is no longer active. Your details are still saved locally.')
            current = db.execute('SELECT complete,deleted_at FROM event_attendees WHERE key=?',(key,)).fetchone()
            if not current or current['deleted_at']:
                raise ValueError('This registration was deleted. Open a new registration form.')
            if not current['complete']:
                db.execute('UPDATE event_attendees SET photo=?,updated_at=?,revision=revision+1 WHERE key=?',
                           (filename,utc_now_iso(),key))
            db.commit()
        except Exception:
            db.rollback()
            raise
        return jsonify(ok=True,redirect=url_for('event_review_page',key=key))
    except (ValueError,CCMStorageError) as exc:
        return jsonify(ok=False,error=str(exc)),400


@app.get('/event-registration/review/<key>')
@event_registration_opening
def event_review_page(key):
    event_tables()
    attendee = event_attendee(key)
    if attendee['complete'] or not attendee['photo']:
        return redirect(url_for('event_photo_page',key=key))
    event = dict(get_db().execute('SELECT * FROM registration_events WHERE id=?',(attendee['event_id'],)).fetchone())
    return render_template('event_registration.html',view='review',attendee=attendee,event=event,
                           csrf=event_csrf(),tab='registration',admin=event_admin(),developer=False)


@app.post('/event-registration/review/<key>')
@event_registration_opening
def event_confirm_registration(key):
    event_guard()
    event_tables()
    if key != session.get('event_registration_key'):
        abort(403)
    attendee = event_attendee(key)
    if attendee['complete']:
        return jsonify(ok=True,redirect=url_for('event_photo_page',key=key))
    db = get_db()
    try:
        db.execute('BEGIN IMMEDIATE')
        active = event_current()
        current = db.execute('SELECT * FROM event_attendees WHERE key=?',(key,)).fetchone()
        if not current or current['deleted_at']:
            raise ValueError('This registration was deleted. Open a new registration form.')
        if current['complete']:
            db.commit()
            return jsonify(ok=True,redirect=url_for('event_photo_page',key=key))
        if not active or active['id'] != current['event_id']:
            raise ValueError('Registration for this event is no longer active. Your details and photo are still saved locally.')
        if not current['photo']:
            raise ValueError('Take your ID photo before confirming registration.')
        if current['revision'] != request.form.get('revision',type=int):
            raise ValueError('Your details or photo changed. Reload this review before confirming registration.')
        if not current['complete']:
            db.execute('UPDATE registration_events SET next_number=next_number+1 WHERE id=?',(active['id'],))
            db.execute('UPDATE event_attendees SET number=?,complete=1,updated_at=?,revision=revision+1 WHERE key=?',
                       (active['next_number'],utc_now_iso(),key))
        db.commit()
        return jsonify(ok=True,redirect=url_for('event_photo_page',key=key))
    except ValueError as exc:
        db.rollback()
        return jsonify(ok=False,error=str(exc)),400
    except Exception:
        db.rollback()
        raise


@app.post('/event-registration/photo/<key>/backup')
def event_photo_backup(key):
    event_guard()
    event_tables()
    event_attendee(key)
    # Old confirmation pages may still try this URL. Only the admin backup
    # action may initiate cloud uploads, including for already-completed forms.
    return jsonify(ok=False,error='Registration is saved locally. An administrator can back up the details and printable ID using Backup Data.'),409


@app.post('/event-registration/cancel/<key>')
def event_cancel_registration(key):
    event_guard()
    event_tables()
    if key != session.get('event_registration_key'):
        abort(403)
    event_attendee(key)
    if request.form.get('confirm') != 'yes':
        return jsonify(ok=False,error='Confirm before cancelling this registration.'),400
    changed = get_db().execute("DELETE FROM event_attendees WHERE key=? AND complete=0 AND deleted_at=''",(key,))
    get_db().commit()
    if not changed.rowcount:
        return jsonify(ok=False,error='This registration is already complete. Ask an administrator if it needs to be deleted.'),409
    for field in ('event_registration_key','event_form_key','event_form_event_id','event_form_event_key'):
        session.pop(field,None)
    return jsonify(ok=True,redirect=url_for('event_registration',new=1))


def event_upload_id(attendee, event):
    """Back up the actual printable ID, reusing a confirmed unchanged rendering."""
    import hashlib
    photo = event_asset_path(attendee['photo']).read_bytes()
    body = event_render_id(event,attendee,photo)
    digest = hashlib.sha256(body).hexdigest()
    if attendee['id_drive_id'] and attendee['id_backup_hash'] == digest:
        return attendee['id_drive_id']
    safe_name = re.sub(r'[<>/\\\x00-\x1f]','_',attendee['name'])
    name = 'Event>'+safe_name+'>'+str(attendee['number'])+'-ID.png'
    file_id = ccm_upload(attendee['key'],'id',body,filename=name,event_key=event['key'])
    ccm_verify_upload(file_id,body)
    get_db().execute("UPDATE event_attendees SET id_drive_id=?,id_backup_hash=? WHERE key=?",
                     (file_id,digest,attendee['key']))
    get_db().commit()
    return file_id


@app.get('/event-registration/image/<key>/<side>.png')
@event_registration_opening
def event_image(key,side):
    event_tables()
    attendee = event_attendee(key,admin=side in ('id','shared'))
    if not attendee['complete'] and (side not in ('photo','review') or not attendee['photo']):
        abort(404)
    photo = event_asset_path(attendee['photo']).read_bytes()
    if side=='photo':
        body = photo
    elif side in ('id','shared','review'):
        event = dict(get_db().execute('SELECT * FROM registration_events WHERE id=?',(attendee['event_id'],)).fetchone())
        try:
            if side == 'review':
                try:
                    background = event_background_path(event).read_bytes()
                except CCMStorageError:
                    background = b''  # A missing template must not block attendee review.
                body = event_render_id(event,attendee,photo,background=background,review=True)
            else:
                body = event_render_id(event,attendee,photo,temporary=side=='shared' and not attendee['physical_printed'])
        except CCMStorageError as exc:
            return jsonify(ok=False,error=str(exc)),503
    else:
        abort(404)
    return send_file(io.BytesIO(body),mimetype='image/png',as_attachment=request.args.get('download')=='1',
                     download_name='Event-'+str(attendee['number'])+'-'+side+'.png')


@app.post('/event-registration/admin/status/<key>')
def event_status(key):
    event_guard(admin=True)
    event_tables()
    field = request.form.get('field')
    if field not in ('paid','released','physical_printed') or request.form.get('value') not in ('0','1'):
        abort(400)
    db = get_db()
    db.execute('BEGIN IMMEDIATE')
    attendee = event_attendee(key,admin=True)
    if not attendee['complete']:
        abort(400)
    value = int(request.form['value'])
    if attendee[field] and not value and request.form.get('confirm')!='yes':
        return jsonify(ok=False,error='Confirm before changing this status back.'),400
    if field=='released' and value and not (attendee['paid'] and attendee['physical_printed']):
        return jsonify(ok=False,error='Confirm payment and physical ID printing before releasing this ID.'),400
    if field=='released' and value and request.form.get('confirm')!='yes':
        return jsonify(ok=False,error='Confirm that you handed the physical ID to the attendee.'),400
    if field=='physical_printed' and value and request.form.get('physical_confirmed')!='yes':
        return jsonify(ok=False,error='Confirm that the physical ID was printed. Downloading alone does not qualify.'),400
    if field in ('paid','physical_printed') and not value and attendee['released']:
        return jsonify(ok=False,error='Undo the physical ID release before clearing payment or printing.'),400
    revision = request.form.get('revision',type=int)
    extra = ',printed='+str(value) if field=='physical_printed' else ''
    cursor = db.execute(f"UPDATE event_attendees SET {field}=?{extra},revision=revision+1,updated_at=? WHERE key=? AND revision=? AND deleted_at=''",
                             (value,utc_now_iso(),key,revision))
    db.commit()
    if not cursor.rowcount:
        return jsonify(ok=False,error='This attendee was updated in another window. Reload before changing their status.'),409
    return jsonify(ok=True,revision=revision+1,**{field:value})


@app.post('/event-registration/admin/delete/<key>')
def event_delete_attendee(key):
    event_guard(admin=True)
    event_tables()
    if not re.fullmatch(r'[a-f0-9]{32}',key):
        abort(404)
    if request.form.get('confirm')!='yes':
        return jsonify(ok=False,error='Confirm before deleting this registration.'),400
    from sheet_cache import _locked_sync
    # Wait for any upload of this attendee before capturing its cloud identifiers.
    with _locked_sync(data_path('event-record-backup-'+key)):
        db = get_db()
        db.execute('BEGIN IMMEDIATE')
        if not db.execute('SELECT 1 FROM event_deletion_queue WHERE key=?',(key,)).fetchone():
            attendee = event_attendee(key,admin=True,include_deleted=True)
            if attendee['revision']!=request.form.get('revision',type=int):
                db.rollback()
                return jsonify(ok=False,error='This registration changed in another window. Refresh before deleting it.'),409
            event_queue_deletion(attendee)
        db.commit()
    return jsonify(ok=True,pending_deletions=event_pending_deletions())


@app.get('/event-registration/admin/deletions')
def event_list_deletions():
    if not event_admin():
        abort(403)
    event_tables()
    return jsonify(ok=True,records=event_deleted_records(),pending_deletions=event_pending_deletions())


@app.post('/event-registration/admin/deletions/<key>/forget')
def event_forget_deletion(key):
    event_guard(admin=True)
    event_tables()
    if not re.fullmatch(r'[a-f0-9]{32}',key):
        abort(404)
    if request.form.get('confirm') != 'yes':
        return jsonify(ok=False,error='Confirm removal of this local cleanup record.'),400
    from sheet_cache import _locked_sync
    with _locked_sync(data_path('event-record-backup-'+key)):
        db = get_db()
        db.execute('BEGIN IMMEDIATE')
        try:
            row = db.execute('SELECT * FROM event_attendees WHERE key=?',(key,)).fetchone()
            queued = db.execute('SELECT payload FROM event_deletion_queue WHERE key=?',(key,)).fetchone()
            if row and not row['deleted_at']:
                db.rollback()
                return jsonify(ok=False,error='This is an active registration. Use its normal Delete action.'),409
            record = json.loads(queued['payload']) if queued else dict(row) if row else None
            if record and record['revision'] != request.form.get('revision',type=int):
                db.rollback()
                return jsonify(ok=False,error='This cleanup record changed. Reload the list before removing it.'),409
            db.execute("DELETE FROM event_attendees WHERE key=? AND deleted_at<>''",(key,))
            db.execute('DELETE FROM event_deletion_queue WHERE key=?',(key,))
            db.commit()
        except Exception:
            db.rollback()
            raise
    return jsonify(ok=True,pending_deletions=event_pending_deletions())


@app.post('/event-registration/admin/event/<int:event_id>/delete')
def event_delete_event(event_id):
    event_guard(developer=True)
    event_tables()
    event_meal_tables()
    if request.form.get('confirm')!='yes':
        return jsonify(ok=False,error='Confirm before deleting this event and its registrations.'),400
    if not event_password_matches(request.form.get('password','')):
        return jsonify(ok=False,error='Enter the correct event admin password to delete this event. The password is required for every deletion.'),403
    db = get_db()
    db.execute('BEGIN IMMEDIATE')
    try:
        event = db.execute('SELECT * FROM registration_events WHERE id=?',(event_id,)).fetchone()
        if not event:
            abort(404)
        if not event['deleted_at']:
            if request.form.get('updated_at')!=event['updated_at']:
                db.rollback()
                return jsonify(ok=False,error='The event changed in another window. Reload before deleting it.'),409
            now = utc_now_iso()
            # Release the visible name so a corrected event with that type/year can be created.
            db.execute('UPDATE registration_events SET active=0,deleted_at=?,updated_at=?,name=? WHERE id=?',
                       (now,now,'[deleted:'+event['key']+'] '+event['name'],event_id))
            db.execute("""UPDATE event_attendees SET deleted_at=?,updated_at=?,revision=revision+1
                          WHERE event_id=? AND deleted_at=''""",(now,now,event_id))
            db.execute('''DELETE FROM registration_events WHERE id=?
                AND NOT EXISTS (SELECT 1 FROM event_attendees WHERE event_id=?)
                AND NOT EXISTS (SELECT 1 FROM event_deletion_queue WHERE event_id=?)
                AND NOT EXISTS (SELECT 1 FROM event_meal_sets WHERE event_id=?)
                AND NOT EXISTS (SELECT 1 FROM event_meal_claims WHERE event_id=?)''',(event_id,)*5)
        db.commit()
    except Exception:
        db.rollback()
        raise
    return jsonify(ok=True,redirect=url_for('event_registration',tab='admin'))


@app.post('/event-registration/admin/print/prepare')
def event_print_prepare():
    event_guard(admin=True)
    event_tables()
    keys = request.form.getlist('keys')
    event_id = request.form.get('event_id',type=int)
    if not keys or len(keys)>100 or any(not re.fullmatch(r'[a-f0-9]{32}',key) for key in keys):
        return jsonify(ok=False,error='Select between 1 and 100 IDs to print.'),400
    keys = list(dict.fromkeys(keys))
    placeholders = ','.join('?' for _ in keys)
    rows = {row['key']:dict(row) for row in get_db().execute(f'''SELECT a.key,a.number,a.name,a.area,a.church,a.revision,a.physical_printed
        FROM event_attendees a JOIN registration_events e ON e.id=a.event_id
        WHERE a.event_id=? AND e.deleted_at='' AND a.complete=1 AND a.deleted_at='' AND a.key IN ({placeholders})''',(event_id,*keys))}
    if len(rows) != len(keys):
        return jsonify(ok=False,error='Some selected registrations changed or are unavailable. Refresh the attendee list before printing.'),409
    items = [dict(**rows[key],image_url=url_for('event_image',key=key,side='id'),
                  status_url=url_for('event_status',key=key)) for key in keys]
    document = render_template_string('''<!doctype html><html><head><meta charset="utf-8"><title>Event IDs</title>
        <style>@page{size:{{ width }}mm {{ height }}mm;margin:0}
        html,body{margin:0;padding:0;width:{{ width }}mm;line-height:0}
        img{display:block;width:{{ width }}mm;height:{{ height }}mm;object-fit:fill;break-inside:avoid;break-after:page;page-break-after:always;print-color-adjust:exact;-webkit-print-color-adjust:exact}
        img:last-child{break-after:auto;page-break-after:auto}</style></head><body>
        {% for item in items %}<img src="{{ item.image_url }}" alt="ID for {{ item.name }}">{% endfor %}</body></html>''',
        items=items,width=EVENT_PRINT_WIDTH_MM,height=EVENT_PRINT_HEIGHT_MM)
    return jsonify(ok=True,items=items,html=document,width_mm=EVENT_PRINT_WIDTH_MM,height_mm=EVENT_PRINT_HEIGHT_MM)


@app.get('/event-registration/print/<key>')
def event_print(key):
    if not event_admin():
        return redirect(url_for('event_registration',tab='admin'))
    event_tables()
    attendee = event_attendee(key,admin=True)
    if not attendee['complete']:
        abort(404)
    event = dict(get_db().execute('SELECT * FROM registration_events WHERE id=?',(attendee['event_id'],)).fetchone())
    return render_template('event_registration.html',view='print',attendee=attendee,event=event,
                           admin=True,developer=False,csrf=event_csrf(),tab='admin')


@app.get('/event-registration/release/<key>')
def event_release(key):
    if not event_admin():
        return redirect(url_for('event_registration',tab='admin'))
    event_tables()
    attendee = event_attendee(key,admin=True)
    if not attendee['complete']:
        abort(404)
    event = dict(get_db().execute('SELECT * FROM registration_events WHERE id=?',(attendee['event_id'],)).fetchone())
    return render_template('event_registration.html',view='release',attendee=attendee,event=event,
                           admin=True,developer=False,csrf=event_csrf(),tab='admin')


@app.get('/event-registration/thumbnail/<int:event_id>/<int:number>.jpg')
def event_thumbnail(event_id,number):
    """Small public photo for the active roster, without exposing ticket keys."""
    row = get_db().execute("""SELECT a.photo FROM event_attendees a JOIN registration_events e ON e.id=a.event_id
        WHERE e.id=? AND e.active=1 AND e.deleted_at='' AND a.number=? AND a.complete=1 AND a.deleted_at=''""",
        (event_id,number)).fetchone()
    if not row:
        abort(404)
    from PIL import Image, ImageDraw, ImageOps, UnidentifiedImageError
    portrait = None
    if re.fullmatch(r'[a-f0-9]{64}\.png',row['photo'] or ''):
        try:
            with Image.open(event_asset_path(row['photo'])) as source:
                portrait = ImageOps.fit(ImageOps.exif_transpose(source).convert('RGB'),(80,96))
        except (OSError,UnidentifiedImageError):
            pass
    if portrait is None:
        portrait = Image.new('RGB',(80,96),'#e8eef8')
        drawing = ImageDraw.Draw(portrait)
        drawing.ellipse((25,15,55,45),fill='#8495b0')
        drawing.rounded_rectangle((12,50,68,88),radius=22,fill='#8495b0')
    body = io.BytesIO()
    portrait.save(body,'JPEG',quality=82,optimize=True)
    return send_file(io.BytesIO(body.getvalue()),mimetype='image/jpeg')


@app.get('/event-registration/ticket/<key>')
def event_ticket(key):
    event_tables()
    if not re.fullmatch(r'[a-f0-9]{32}',key):
        abort(404)
    row = get_db().execute("SELECT name,area,church,event_id FROM event_attendees WHERE key=? AND complete=1 AND deleted_at=''",(key,)).fetchone()
    if not row:
        abort(404)
    event = get_db().execute('SELECT name FROM registration_events WHERE id=?',(row['event_id'],)).fetchone()
    return render_template('event_registration.html',view='ticket',attendee=dict(row),event=dict(event),
                           admin=False,developer=False,tab='registration',csrf=event_csrf())


def event_sheet():
    book = get_gs_client().open('District4 Data')
    try:
        sheet = book.worksheet('Event Registration')
    except gspread.WorksheetNotFound:
        sheet = book.add_worksheet(title='Event Registration',rows=1000,cols=len(EVENT_HEADERS))
    headers = sheet.row_values(1)
    if not headers:
        if sheet.col_count<len(EVENT_HEADERS):
            sheet.resize(cols=len(EVENT_HEADERS))
        sheet.update(range_name='A1:L1',values=[EVENT_HEADERS],value_input_option='RAW')
        sheet.freeze(rows=1)
        headers = list(EVENT_HEADERS)
    if len(headers)!=len(EVENT_HEADERS) or set(headers)!=set(EVENT_HEADERS):
        raise CCMStorageError('The Event Registration worksheet must contain these columns: '+', '.join(EVENT_HEADERS))
    return sheet,headers


def event_remove_drive_registration(attendee, event):
    """Owner-executed cleanup also finds older rendered versions and orphaned retries."""
    script_url = os.getenv('EVENT_DRIVE_SCRIPT_URL','').strip()
    token = os.getenv('EVENT_DRIVE_API_TOKEN','').strip()
    if not script_url and not any(attendee.get(field) for field in ('id_drive_id','photo_drive_id','cloud_snapshot','sheet_row')):
        return  # Never backed up and no configured endpoint could have uploaded it.
    if not re.fullmatch(r'https://script\.google\.com/macros/s/[A-Za-z0-9_-]+/exec',script_url) or len(token)<32:
        raise CCMStorageError('Configure EVENT_DRIVE_SCRIPT_URL and EVENT_DRIVE_API_TOKEN to finish deleted ID cleanup.')
    try:
        response = requests.post(script_url,json=dict(token=token,purpose='event',action='delete',
            event_key=event['key'],record_key=attendee['key']),timeout=(10,60))
        response.raise_for_status()
        result = response.json()
        if not result.get('ok'):
            messages = {
                'AUTH':'The Event Drive connection rejected its token. Check EVENT_DRIVE_API_TOKEN against the Event Apps Script project properties; the deletion remains queued.',
                'EVENT_ONLY':'The configured Drive endpoint is not accepting Event operations. Check EVENT_DRIVE_SCRIPT_URL; the deletion remains queued.',
                'INPUT':'The Event Drive script rejected the cleanup request. Update the existing Apps Script deployment with deployment/event_drive.gs, then retry Backup Data.',
                'DRIVE':'Google Drive could not confirm deleted ID cleanup. Check the Event Apps Script Executions log and folder access, and deploy the latest deployment/event_drive.gs. The cleanup remains queued.',
            }
            raise CCMStorageError(messages.get(result.get('code'),
                'Drive deletion was not confirmed. Update the Event Apps Script deployment with deployment/event_drive.gs, then retry Backup Data. The cleanup remains queued.'))
        if not (result.get('ok') and result.get('deleted') is True and result.get('purpose')=='event'
                and result.get('record_key')==attendee['key'] and result.get('event_key')==event['key']
                and result.get('folder_id')==EVENT_FOLDER_ID):
            raise CCMStorageError('Drive deletion was not confirmed. Deploy the updated deployment/event_drive.gs web app, then retry Backup Data. The cleanup remains queued.')
    except (requests.RequestException,ValueError,AttributeError):
        raise CCMStorageError('Drive deletion was not confirmed. The cleanup remains queued for the next Backup Data retry.') from None


def event_remove_sheet_registration(attendee):
    """Confirm both cloud removals before clearing the durable cleanup record."""
    from sheet_cache import _locked_sync
    event_meal_tables()
    event = get_db().execute('SELECT * FROM registration_events WHERE id=?',(attendee['event_id'],)).fetchone()
    if not event:
        raise CCMStorageError('The deleted registration event is missing. Cleanup remains queued for review.')
    event_remove_drive_registration(attendee,event)
    event_name = event['kind']+' '+str(date.fromisoformat(event['start_date']).year)

    def matching_rows(values, headers):
        indexed = [(i,dict(zip(headers,row+['']*len(headers)))) for i,row in enumerate(values[1:],2) if any(row)]
        matches = [(i,row) for i,row in indexed if row['Date created']==attendee['created_at'] and row['Event']==event_name]
        if not matches:
            matches = [(i,row) for i,row in indexed if row['Date created']==attendee['created_at']]
        if not matches and attendee['cloud_snapshot']:
            previous = json.loads(attendee['cloud_snapshot'])
            matches = [(i,row) for i,row in indexed if all(row[h]==previous[h] for h in ('Name','Bday','Event'))]
        if len(matches)>1:
            raise CCMStorageError('Multiple Google rows match this deleted registration. Resolve the duplicate rows before retrying removal.')
        return matches

    with _locked_sync(data_path('event-google-backup')):
        sheet,headers = event_sheet()
        values = sheet.get_all_values()
        matches = matching_rows(values,headers)
        if not matches and attendee['sheet_row'] and attendee['sheet_row']<=len(values):
            candidate = dict(zip(headers,values[attendee['sheet_row']-1]+['']*len(headers)))
            known_other = get_db().execute('SELECT key FROM event_attendees WHERE created_at=? AND key<>?',
                                          (candidate.get('Date created',''),attendee['key'])).fetchone()
            if any(candidate.values()) and not known_other:
                raise CCMStorageError('The saved Google row changed and cannot be identified safely. The local deletion is kept; review that row before retrying.')
        if matches:
            number = matches[0][0]
            sheet.delete_rows(number)
            if matching_rows(sheet.get_all_values(),headers):
                raise CCMStorageError('Google has not confirmed registration removal. The local deletion remains queued.')
            get_db().execute('UPDATE event_attendees SET sheet_row=sheet_row-1 WHERE sheet_row>?',(number,))
            # A deleted registration may have been queued before this row shifted.
            for queued in get_db().execute('SELECT key,payload FROM event_deletion_queue').fetchall():
                payload = json.loads(queued['payload'])
                if payload.get('sheet_row') and payload['sheet_row']>number:
                    payload['sheet_row'] -= 1
                    get_db().execute('UPDATE event_deletion_queue SET payload=? WHERE key=?',
                                     (json.dumps(payload),queued['key']))
        # The per-record backup lock ensures an older upload cannot recreate a purged record.
        get_db().execute("DELETE FROM event_attendees WHERE key=? AND deleted_at<>'' AND revision=?",
                         (attendee['key'],attendee['revision']))
        get_db().execute('DELETE FROM event_deletion_queue WHERE key=?',(attendee['key'],))
        get_db().execute("""DELETE FROM registration_events WHERE id=? AND deleted_at<>''
            AND NOT EXISTS (SELECT 1 FROM event_attendees WHERE event_id=?)
            AND NOT EXISTS (SELECT 1 FROM event_deletion_queue WHERE event_id=?)
            AND NOT EXISTS (SELECT 1 FROM event_meal_sets WHERE event_id=?)
            AND NOT EXISTS (SELECT 1 FROM event_meal_claims WHERE event_id=?)""",(attendee['event_id'],)*5)
        get_db().commit()
    return {'clean':True,'removed':True}


def event_backup_attendee(key):
    from sheet_cache import _locked_sync
    # Serialize the entire per-record snapshot, including upload, across retrying admins.
    with _locked_sync(data_path('event-record-backup-'+key)):
        queued = get_db().execute('SELECT payload FROM event_deletion_queue WHERE key=?',(key,)).fetchone()
        if queued:
            return event_remove_sheet_registration(json.loads(queued['payload']))
        attendee = dict(get_db().execute("SELECT * FROM event_attendees WHERE key=? AND (complete=1 OR deleted_at<>'')",(key,)).fetchone() or {})
        if not attendee:
            return {'clean':True,'removed':True}  # Safe retry after an acknowledged deletion.
        if attendee['deleted_at']:
            return event_remove_sheet_registration(attendee)
        event = dict(get_db().execute('SELECT * FROM registration_events WHERE id=?',(attendee['event_id'],)).fetchone())
        event_upload_id(attendee,event)
        cloud = dict(zip(EVENT_HEADERS,[attendee['name'],attendee['birthday'],attendee['contact'],attendee['area'],attendee['church'],
            attendee['address'],attendee['pastor'],attendee['created_at'],'Paid' if attendee['paid'] else '',
            'Printed' if attendee['physical_printed'] else '','Released' if attendee['released'] else '',event['name']]))
        with _locked_sync(data_path('event-google-backup')):
            sheet,headers = event_sheet()
            values = sheet.get_all_values()
            indexed = [(i,dict(zip(headers,row+['']*max(0,len(headers)-len(row))))) for i,row in enumerate(values[1:],2) if any(row)]
            matches = [(i,r) for i,r in indexed if r['Date created']==attendee['created_at'] and r['Event']==event['name']]
            if not matches and attendee['cloud_snapshot']:
                previous = json.loads(attendee['cloud_snapshot'])
                matches = [(i,r) for i,r in indexed if all(r[h]==previous[h] for h in ('Name','Bday','Event'))]
            if len(matches)>1:
                raise CCMStorageError('Duplicate matching rows were found in Google. Local records were kept; resolve duplicates before retrying.')
            number = matches[0][0] if matches else attendee['sheet_row']
            if number and number<=len(values):
                remote = dict(zip(headers,values[number-1]+['']*len(headers)))
                other = get_db().execute('SELECT key FROM event_attendees WHERE created_at=? AND key<>?',
                                         (remote.get('Date created',''),key)).fetchone()
                if not matches and other:
                    raise CCMStorageError('Google rows were moved and this saved row belongs to another attendee. Local records were kept for review.')
                sheet.update(range_name=f'A{number}:L{number}',values=[[cloud[h] for h in headers]],value_input_option='RAW')
            else:
                sheet.append_row([cloud[h] for h in headers],value_input_option='RAW')
            confirmed = sheet.get_all_values()
            saved = [(i,r) for i,r in enumerate(confirmed[1:],2)
                     if (r+['']*max(0,len(headers)-len(r)))[:len(headers)]==[cloud[h] for h in headers]]
            if len(saved)!=1:
                raise CCMStorageError('Google has not confirmed the full record. The local registration and photo were kept for retry.')
            get_db().execute('''UPDATE event_attendees SET sheet_row=?,cloud_snapshot=?,backed_up_revision=MAX(backed_up_revision,?),backup_error=''
                                WHERE key=?''',(saved[0][0],json.dumps(cloud),attendee['revision'],key))
            get_db().commit()
        current_event = get_db().execute('SELECT * FROM registration_events WHERE id=?',(attendee['event_id'],)).fetchone()
        fields = ('name','kind','start_date','end_date','theme','scripture','background','id_design','updated_at','deleted_at')
        design_unchanged = current_event and all(current_event[field] == event[field] for field in fields)
        current = get_db().execute("SELECT revision=backed_up_revision AND deleted_at='' FROM event_attendees WHERE key=?",(key,)).fetchone()
        return {'clean':bool(current and current[0] and design_unchanged)}


@app.post('/event-registration/admin/backup')
def event_backup_queue():
    event_guard(admin=True)
    event_tables()
    event_id = request.form.get('event_id',type=int)
    # Explicit submission rechecks all local rows so remote changes never become authoritative.
    keys = [r[0] for r in get_db().execute("""SELECT key FROM event_attendees WHERE
        (event_id=? AND complete=1 AND deleted_at='') OR deleted_at<>'' ORDER BY (deleted_at=''),event_id,number""",(event_id,))]
    keys = [r[0] for r in get_db().execute('SELECT key FROM event_deletion_queue ORDER BY event_id,key')] + keys
    return jsonify(ok=True,keys=keys)


@app.post('/event-registration/admin/backup/<key>')
def event_backup_one(key):
    event_guard(admin=True)
    event_tables()
    if not re.fullmatch(r'[a-f0-9]{32}',key):
        abort(404)
    try:
        result = event_backup_attendee(key)
        return jsonify(ok=True,**result,pending_deletions=event_pending_deletions())
    except Exception as exc:
        app.logger.warning('Event backup not confirmed: %s',type(exc).__name__)
        message = str(exc) if isinstance(exc,(CCMStorageError,CacheSyncError)) else 'Google backup could not be confirmed. Local details and photo are safe; retry backup.'
        get_db().execute('UPDATE event_attendees SET backup_error=? WHERE key=?',(message[:500],key))
        get_db().execute('UPDATE event_deletion_queue SET backup_error=? WHERE key=?',(message[:500],key))
        get_db().commit()
        return jsonify(ok=False,error=message),503


@app.after_request
def event_private_responses(response):
    if (request.endpoint or '').startswith('event_'):
        response.headers['Cache-Control'] = 'private, no-store'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['Referrer-Policy'] = 'same-origin'
    return response


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
        raise RuntimeError("The account is missing from the cache.")

    sheet_row = int(cached["sheet_row"])

    client = get_gs_client()
    sh = client.open("District4 Data")
    ws = sh.worksheet("Accounts")
    headers = _ensure_accounts_headers(ws)
    verify_sheet_row(ws, "Accounts", sheet_row, headers=headers)
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
        raise RuntimeError("The account is missing from the cache.")

    sheet_row = int(cached["sheet_row"])
    if sheet_row <= 1:
        raise ValueError("Invalid account row.")

    client = get_gs_client()
    sh = client.open("District4 Data")
    ws = sh.worksheet("Accounts")

    verify_sheet_row(ws, "Accounts", sheet_row)
    ws.delete_rows(sheet_row)
    return True

from meal_scanner_readonly import register_meal_scanner_api
register_meal_scanner_api(app, get_db, event_tables,
                          event_password_matches, event_limit,
                          meal_tables=event_meal_tables,
                          meal_views={
                              'meal-sets': event_meal_api_sets.__wrapped__,
                              'claims': event_meal_api_claims.__wrapped__,
                              'audit': event_meal_api_audit.__wrapped__,
                              'sync-status': event_meal_api_status.__wrapped__,
                          })

# Gunicorn imports app:app and never enters __main__. Complete the additive
# application schema setup here, before accepting any requests, on both servers.
with app.app_context():
    init_db()
    event_tables()

if __name__ == "__main__":
    print("Database initialized")
    print("Timezone:", PH_TZ)

    app.run(debug=True)
