import os
import sqlite3
from datetime import datetime, date
import calendar
import urllib.parse
import uuid

from zoneinfo import ZoneInfo

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
PH_TZ = ZoneInfo("Asia/Manila")

# ========================
# DB helpers
# ========================


def get_db():
    db = getattr(g, "_database", None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE, check_same_thread=False)
        db.row_factory = sqlite3.Row
    return db



    migrate_monthly_reports_scope_to_pastor()
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

def migrate_monthly_reports_scope_to_pastor():
    """
    Rebuild monthly_reports table to scope data by pastor_username.
    Safe because Google Sheets is the source of truth.
    """
    db = get_db()
    cur = db.cursor()

    table = cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='monthly_reports'"
    ).fetchone()
    if not table:
        return

    cols = [r["name"] for r in cur.execute("PRAGMA table_info(monthly_reports)").fetchall()]
    if "pastor_username" in cols:
        return  # already migrated

    cur.execute("ALTER TABLE monthly_reports RENAME TO monthly_reports_old")

    cur.execute("""
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
    """)

    cur.execute("""
        INSERT INTO monthly_reports (
            id, year, month, pastor_username, submitted, approved, submitted_at, approved_at
        )
        SELECT
            id, year, month, '__legacy__', submitted, approved, submitted_at, approved_at
        FROM monthly_reports_old
    """)

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
            sheet_row INTEGER
        )
        """
    )

    # Ensure newer columns exist (safe on existing DBs)
    try:
        cursor.execute("ALTER TABLE sheet_accounts_cache ADD COLUMN position TEXT")
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
            status TEXT
        )
        """
    )

    # -----------------------
    # ✅ AOPT CACHE (AO Personal Tithes)
    # -----------------------
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS sheet_aopt_cache (
            month TEXT PRIMARY KEY,   -- e.g. "January 2025"
            amount REAL,
            sheet_row INTEGER
        )
        """
    )

    # -----------------------
    # ✅ PRAYER REQUEST CACHE (PrayerRequest sheet → local cache)
    # Sheet tab: "PrayerRequest"
    # Columns:
    # Church Name | Submitted By | Request ID | Prayer Request Title | Prayer Request Date
    # Prayer Request | Status | Pastor's Praying | Answered Date
    # -----------------------
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
    now_str = datetime.utcnow().isoformat()
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
            return datetime.fromisoformat(row["last_sync"])
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
        (datetime.utcnow().isoformat(),),
    )
    get_db().commit()


def sync_from_sheets_if_needed(force=False):
    """
    Reads Google Sheets ONLY once per interval, stores into cache tables.
    AO pages read ONLY from cache tables (no quota spam).
    """
    last = _last_sync_time_utc()
    if not force and last and (datetime.utcnow() - last).total_seconds() < SYNC_INTERVAL_SECONDS:
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

        def cell(row, idx):
            if idx is None:
                return ""
            if idx < len(row):
                return row[idx]
            return ""

        for r in range(1, len(acc_values)):
            row = acc_values[r]
            username = str(cell(row, i_user)).strip()
            if not username:
                continue
            cur.execute(
                """
                INSERT OR REPLACE INTO sheet_accounts_cache
                (username, name, church_address, password, age, sex, contact, birthday, position, sheet_row)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    username,
                    str(cell(row, i_name)).strip(),
                    str(cell(row, i_addr)).strip(),
                    str(cell(row, i_pass)).strip(),
                    str(cell(row, i_age)).strip(),
                    str(cell(row, i_sex)).strip(),
                    str(cell(row, i_contact)).strip(),
                    str(cell(row, i_bday)).strip(),
                    str(cell(row, i_pos)).strip(),
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
        i_status = _find_col(headers, "status")

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
            try:
                d = date.fromisoformat(activity)
            except Exception:
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
                    amount_to_send, status
                ) VALUES (
                    ?, ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?
                )
                """,
                (
                    r + 1,
                    d.year,
                    d.month,
                    activity,
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
            cur.execute(
                """
                INSERT OR REPLACE INTO sheet_aopt_cache (month, amount, sheet_row)
                VALUES (?, ?, ?)
                """,
                (month_label, amount_val, r + 1),
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
        i_status = _find_col(headers, "Status")
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
    _update_sync_time()
    print("✅ Sheets cache sync done.")


# ========================
# Cache-based helpers (NO Sheets calls)
# ========================

def get_aopt_amount_from_cache(month_label: str):
    row = get_db().execute(
        "SELECT amount FROM sheet_aopt_cache WHERE month = ?",
        (month_label,),
    ).fetchone()
    if not row:
        return None
    return row["amount"]


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
        return (session.get("pastor_church_address") or "").strip()
    return ""


# ========================
# ✅ IMPORTANT FIX:
# Cache → Local upsert for Pastor Tool display
# ========================


def sync_local_month_from_cache_for_pastor(year: int, month: int):
    refresh_pastor_from_cache()
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
    pastor_username = (session.get("pastor_username") or "").strip()
    if not pastor_username:
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
        try:
            d = date.fromisoformat(activity_date)
        except Exception:
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


def get_all_churches_from_cache(area_number: str | None = None):
    area_number = (area_number or "").strip()
    if area_number:
        rows = get_db().execute(
            """
            SELECT DISTINCT TRIM(sex) AS c
            FROM sheet_accounts_cache
            WHERE TRIM(sex) != ''
              AND TRIM(age) = ?
            ORDER BY c
            """,
            (area_number,),
        ).fetchall()
    else:
        rows = get_db().execute(
            """
            SELECT DISTINCT TRIM(sex) AS c
            FROM sheet_accounts_cache
            WHERE TRIM(sex) != ''
            ORDER BY c
            """
        ).fetchall()

    return [r["c"] for r in rows]


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

    # IMPORTANT: USER_ENTERED makes Sheets treat dates as real dates
    worksheet.append_row(row, value_input_option="USER_ENTERED")


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

    for row in sunday_rows:
        d = datetime.fromisoformat(row["date"]).date()
        activity_date = f"{d.month}/{d.day}/{d.year}"  # 1/25/2026

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
            "received_jesus": cp_row["received_christ"] or 0,
            "existing_bible_study": cp_row["bible_existing"] or 0,
            "new_bible_study": cp_row["bible_new"] or 0,
            "water_baptized": cp_row["baptized_water"] or 0,
            "holy_spirit_baptized": cp_row["baptized_holy_spirit"] or 0,
            "childrens_dedication": cp_row["child_dedication"] or 0,
            "healed": cp_row["healed"] or 0,
            "activity_date": activity_date,
            "amount_to_send": amount_to_send,
            "status": status_label,
        }

        try:
            append_report_to_sheet(report_data)
        except Exception as e:
            print("Error exporting report row:", e)


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
    return db.execute(
        """
        SELECT *
        FROM sheet_prayer_request_cache
        WHERE TRIM(status) = 'Pending'
        ORDER BY request_date DESC, sheet_row DESC
        """
    ).fetchall()


# ========================
# Bible verse of the day
# ========================

VERSE_REFERENCES = [
    "Romans 12:1",
    "1 Peter 5:2-3",
    "Jeremiah 1:5",
    "Isaiah 6:8",
    "2 Timothy 1:9",
    "Psalm 37:23",
    "Proverbs 16:3",
    "Colossians 3:17",
    "Galatians 2:20",
    "Matthew 6:33",
    "Psalm 119:105",
    "Joshua 1:9",
    "Psalm 90:17",
    "Hebrews 13:7",
    "1 Corinthians 4:2",
    "James 1:22",
    "Micah 6:8",
    "Psalm 51:10",
    "1 Thessalonians 5:23",
    "Romans 6:22",
    "2 Corinthians 5:17",
    "Psalm 24:3-4",
    "Hebrews 12:14",
    "John 15:16",
    "Psalm 119:11",
    "Proverbs 4:23",
    "Romans 8:29",
    "2 Timothy 2:21",
    "Matthew 5:8",
    "Psalm 19:14",
    "Ephesians 4:1",
    "1 Peter 1:15-16",
    "Romans 12:2",
    "Galatians 5:16",
    "Colossians 1:10",
    "Psalm 1:1-3",
    "Titus 2:11-12",
    "2 Corinthians 7:1",
    "Ephesians 5:8",
    "John 17:17",
    "Psalm 15:1-2",
    "Proverbs 20:7",
    "Philippians 1:27",
    "1 John 2:6",
    "Romans 13:14",
    "James 4:8",
    "Psalm 101:2",
    "Proverbs 11:3",
    "Matthew 5:16",
    "Colossians 3:5",
    "Galatians 6:8",
    "1 Timothy 4:12",
    "Hebrews 10:22",
    "Psalm 26:8",
    "Ephesians 5:1",
    "2 Timothy 3:16-17",
    "Romans 8:13",
    "Psalm 34:14",
    "1 John 1:7",
    "Hebrews 11:1",
    "Romans 10:17",
    "Proverbs 3:5-6",
    "Mark 11:22",
    "2 Corinthians 5:7",
    "Psalm 56:3",
    "Isaiah 26:3",
    "Matthew 17:20",
    "Hebrews 11:6",
    "Psalm 112:7",
    "Romans 4:20",
    "James 1:6",
    "Isaiah 41:10",
    "Psalm 37:5",
    "Luke 18:27",
    "John 11:40",
    "Habakkuk 2:4",
    "1 Peter 1:7",
    "Psalm 20:7",
    "Matthew 21:22",
    "Hebrews 10:23",
    "Romans 15:13",
    "Psalm 9:10",
    "Isaiah 30:15",
    "2 Timothy 1:7",
    "1 Thessalonians 5:17",
    "Jeremiah 33:3",
    "Matthew 6:6",
    "James 5:16",
    "Luke 11:9",
    "Philippians 4:6-7",
    "Psalm 145:18",
    "Romans 8:26",
    "Colossians 4:2",
    "Psalm 63:1",
    "Hebrews 4:16",
    "Mark 1:35",
    "Ephesians 6:18",
    "Psalm 66:18",
    "Luke 18:1",
    "1 John 5:14",
    "Matthew 7:7",
    "Psalm 141:2",
    "Daniel 6:10",
    "John 15:7",
    "Psalm 55:17",
    "Acts 1:14",
    "Isaiah 58:9",
    "2 Chronicles 7:14",
    "Matthew 28:19-20",
    "Romans 1:16",
    "Daniel 12:3",
    "Mark 16:15",
    "Luke 19:10",
    "Acts 1:8",
    "1 Corinthians 9:22",
    "Matthew 9:37-38",
    "Proverbs 11:30",
    "2 Corinthians 5:20",
    "Romans 10:14",
    "John 4:35",
    "Psalm 126:5-6",
    "Acts 20:24",
    "1 Peter 3:15",
    "Colossians 4:5-6",
    "Matthew 5:14",
    "Isaiah 52:7",
    "Romans 15:20",
    "Luke 15:7",
    "1 Peter 5:4",
    "Hebrews 13:17",
    "Acts 20:28",
    "1 Timothy 3:1-2",
    "Titus 1:7",
    "John 21:15-17",
    "Matthew 20:26",
    "2 Timothy 2:2",
    "Proverbs 27:23",
    "Psalm 78:72",
    "1 Corinthians 11:1",
    "Ezekiel 34:2",
    "James 3:1",
    "Luke 12:42",
    "1 Timothy 4:16",
    "Proverbs 29:18",
    "Romans 15:1",
    "Philippians 2:3",
    "Matthew 24:45",
    "Colossians 1:28",
    "Isaiah 40:31",
    "2 Corinthians 12:9",
    "Psalm 23:1",
    "Philippians 4:19",
    "Matthew 11:28",
    "Galatians 6:9",
    "Hebrews 6:10",
    "Psalm 46:1",
    "Lamentations 3:22-23",
    "Romans 8:28",
    "John 10:10",
    "Psalm 121:1-2",
    "Deuteronomy 31:6",
    "Nahum 1:7",
    "Psalm 34:19",
    "Revelation 2:10",
    "Hebrews 12:11",
    "Isaiah 43:2",
    "Psalm 66:10",
    "2 Timothy 4:7-8"
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
    # Accept either the explicit flag or presence of pastor_username
    return (session.get("pastor_logged_in") is True) or bool((session.get("pastor_username") or "").strip())


def any_user_logged_in():
    return pastor_logged_in() or ao_logged_in()


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


@app.before_request
def before_request():
    init_db()
    migrate_monthly_reports_scope_to_pastor()
    sync_from_sheets_if_needed()


@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, "_database", None)
    if db is not None:
        db.close()


@app.route("/", methods=["GET", "POST"])
def splash():
    logged_in = pastor_logged_in() or ao_logged_in()

    error = None
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
                session["pastor_username"] = username
                session["pastor_name"] = row["name"] or ""
                session["pastor_church_address"] = row["church_address"] or ""
                session["pastor_church_id"] = (row["sex"] or "").strip()
                session.permanent = True
                return redirect(url_for("pastor_tool"))

            error = "Invalid username or password."

    return render_template("splash.html", logged_in=logged_in, error=error)

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("splash"))


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


# ========================
# Pastor login (uses CACHE)
# ========================

@app.route("/pastor-login", methods=["GET", "POST"])
def pastor_login():
    if pastor_logged_in() or ao_logged_in():
        return redirect(url_for("pastor_tool"))

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
                session["pastor_church_id"] = (row["sex"] or "").strip()
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

@app.route("/pastor-tool", methods=["GET", "POST"])
def pastor_tool():
    # Pastor OR AO may access Pastor Tool
    if not (pastor_logged_in() or ao_logged_in()):
        return redirect(url_for("pastor_login", next=request.path))

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

    cursor.execute(
        "SELECT year, month, submitted FROM monthly_reports WHERE year = ? AND pastor_username = ?",
        (year, pastor_username),
    )
    submitted_map = {
        (r["year"], r["month"]): bool(r["submitted"])
        for r in cursor.fetchall()
    }

    if request.method == "POST":
        if can_submit:
            set_month_submitted(year, month, pastor_username)
            try:
                export_month_to_sheet(year, month, "Pending AO approval")
                sync_from_sheets_if_needed(force=True)
                sync_local_month_from_cache_for_pastor(year, month)
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


@app.route("/pastor-tool/<int:year>/<int:month>/<int:day>", methods=["GET", "POST"], endpoint="sunday_detail")
def sunday_detail(year, month, day):
    if not pastor_logged_in():
        return redirect(url_for("pastor_login", next=request.path))

    refresh_pastor_from_cache()

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

    refresh_pastor_from_cache()

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


# ========================
# AO login & tools
# ========================

@app.route("/ao-login", methods=["GET", "POST"])
def ao_login():
    if ao_logged_in():
        return redirect(url_for("ao_tool"))

    error = None
    next_url = request.args.get("next") or url_for("ao_tool")

    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = (request.form.get("password") or "").strip()

        if not username or not password:
            error = "Username and password are required."
        else:
            row = get_db().execute(
                """
                SELECT username, password, name, age, position
                FROM sheet_accounts_cache
                WHERE username = ?
                """,
                (username,),
            ).fetchone()

            if row:
                db_password = str(row["password"] or "").strip()
                position = str(row["position"] or "").strip()

                if db_password == password and position.lower() == "area overseer":
                    session["ao_logged_in"] = True
                    session["ao_username"] = row["username"]
                    session["ao_name"] = row["name"] or ""
                    session["ao_area_number"] = str(row["age"] or "").strip()
                    session.permanent = True
                    return redirect(request.form.get("next") or next_url)

            error = "Invalid AO credentials."

    return render_template("ao_login.html", error=error, next_url=next_url)


@app.route("/ao-tool")
def ao_tool():
    if not ao_logged_in():
        return redirect(url_for("ao_login", next=request.path))

    last_sync_ph = get_last_sync_display_ph()
    return render_template("ao_tool.html", last_sync_ph=last_sync_ph)


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
                            sync_from_sheets_if_needed(force=True)
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


# ========================
# AO Church Status (DB-cache only)
# ========================

@app.route("/ao-tool/church-status")
def ao_church_status():
    if not ao_logged_in():
        return redirect(url_for("ao_login", next=request.path))

    today = date.today()
    year = request.args.get("year", type=int) or today.year

    year_options = list(range(2025, 2036))

    month_names = [
        ("January", 1), ("February", 2), ("March", 3), ("April", 4),
        ("May", 5), ("June", 6), ("July", 7), ("August", 8),
        ("September", 9), ("October", 10), ("November", 11), ("December", 12),
    ]

    ao_area = (session.get("ao_area_number") or "").strip()
    all_churches = get_all_churches_from_cache(area_number=ao_area)

    months = []
    prev_avg_attendance_by_church = {}

    for name, m in month_names:
        church_items = []
        all_reported = True

        month_label = f"{name} {year}"
        aopt_amount = get_aopt_amount_from_cache(month_label)

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

        months.append(
            {
                "name": name,
                "month": m,
                "year": year,
                "label": month_label,
                "aopt_amount": aopt_amount,
                "all_reported": all_reported,
                "churches": church_items,
            }
        )

    open_month = request.args.get("open_month", type=int)

    return render_template(
        "ao_church_status.html",
        year=year,
        year_options=year_options,
        months=months,
        open_month=open_month,
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

    try:
        client = get_gs_client()
        sh = client.open("District4 Data")
        ws = sh.worksheet("AOPT")

        values = ws.get_all_values()
        if not values:
            ws.append_row(["Month", "Amount"])
            values = ws.get_all_values()

        headers = values[0]
        idx_amount = _find_col(headers, "Amount")

        if idx_amount is None:
            print("❌ AOPT sheet missing required header Amount")
        else:
            db = get_db()
            cached = db.execute(
                "SELECT sheet_row FROM sheet_aopt_cache WHERE month = ?",
                (month_label,),
            ).fetchone()

            if cached and cached["sheet_row"]:
                sheet_row = int(cached["sheet_row"])
                col_letter = chr(ord("A") + idx_amount)
                ws.update(f"{col_letter}{sheet_row}", [[amount_val]])
            else:
                ws.append_row([month_label, amount_val])

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

    if year and month and church:
        try:
            sheet_batch_update_status_for_church_month(year, month, church, "Approved")
            cache_update_status_for_church_month(year, month, church, "Approved")
        except Exception as e:
            print("Error approving church month:", e)

    return redirect(url_for("ao_church_status", year=year))


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
            if not req_id:
                continue
            _update_prayer_request_cells_in_sheet(req_id, {"status": "Approved"})

        sync_from_sheets_if_needed(force=True)
    except Exception as e:
        print("❌ Error approving all prayer requests:", e)

    return redirect(url_for("ao_prayer_requests"))



# ========================
# Other pages
# ========================

@app.route("/event-registration")
def event_registration():
    return render_template("event_registration.html")


@app.route("/schedules")
def schedules():
    return render_template("schedules.html")


if __name__ == "__main__":
    app.run(debug=True)
