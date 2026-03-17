import os
import sqlite3
from datetime import datetime, date
import calendar
import urllib.parse
import uuid
import traceback

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
    flash,
)

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

    # Ensure new columns exist (safe migrations)
    cursor.execute("PRAGMA table_info(sheet_accounts_cache)")
    _acc_cols=[row[1] for row in cursor.fetchall()]
    if "position" not in _acc_cols:
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
            sheet_row INTEGER
        )
        """
    )
    cursor.execute("PRAGMA table_info(sheet_district_schedule_cache)")
    _ds_cols = [row[1] for row in cursor.fetchall()]
    if "joining" not in _ds_cols:
        try:
            cursor.execute("ALTER TABLE sheet_district_schedule_cache ADD COLUMN joining TEXT")
        except Exception:
            pass

    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_district_schedule_start ON sheet_district_schedule_cache(activity_date_start)"
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
    
def ensure_schedule_cache_loaded():
    """
    For the Schedule page only:
    - if local schedule cache is empty (ex: Render restarted), load once from Sheets
    - otherwise use local DB only
    """
    db = get_db()
    row = db.execute(
        "SELECT COUNT(*) AS cnt FROM sheet_district_schedule_cache"
    ).fetchone()

    count = int(row["cnt"] or 0) if row else 0
    if count <= 0:
        sync_from_sheets_if_needed(force=True)


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
                    sheet_row
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    rnum,
                ),
            )
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
    """
    Returns the list of Church IDs from the Accounts cache.
    - Church ID is stored in sheet_accounts_cache.sex
    - Area Number is stored in sheet_accounts_cache.age

    If an AO is logged in and has an area number, only churches in that same area are returned.
    Area Overseer accounts themselves are excluded from the church list.
    """
    db = get_db()
    ao_area = (session.get("ao_area_number") or "").strip() if ao_logged_in() else ""

    if ao_area:
        rows = db.execute(
            """
            SELECT DISTINCT TRIM(sex) AS c
            FROM sheet_accounts_cache
            WHERE TRIM(sex) != ''
              AND TRIM(age) = TRIM(?)
              AND LOWER(TRIM(COALESCE(position, ''))) != 'area overseer'
            ORDER BY c
            """,
            (ao_area,),
        ).fetchall()
    else:
        rows = db.execute(
            """
            SELECT DISTINCT TRIM(sex) AS c
            FROM sheet_accounts_cache
            WHERE TRIM(sex) != ''
              AND LOWER(TRIM(COALESCE(position, ''))) != 'area overseer'
            ORDER BY c
            """
        ).fetchall()

    return [r["c"] for r in rows]

def get_accounts_for_area_cache(area_number: str):
    db = get_db()
    rows = db.execute(
        """
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
            sheet_row
        FROM sheet_accounts_cache
        WHERE TRIM(age) = TRIM(?)
          AND TRIM(username) != ''
          AND LOWER(TRIM(COALESCE(position, ''))) != 'area overseer'
        ORDER BY church_id, church_address, name
        """,
        (area_number,),
    ).fetchall()
    return rows

def get_area_summary_for_month_cache(year: int, month: int, church_items, aopt_amount: float):
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
        "ao_personal_tithes": float(aopt_amount or 0.0),
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

        # Attendance + activity = add each church's MONTHLY AVERAGE once
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

        # Financial = still use totals
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
    summary["total_money_no"] = (
        summary["church_tithes"]
        + summary["offering"]
        + summary["no_mission"]
        + (summary["pastor_personal_tithes"] * 0.10)
        + 300
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
    worksheet.append_row(row, value_input_option="USER_ENTERED")

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

@app.template_filter("phpeso")
def phpeso_filter(value):
    return format_php_currency(value)


@app.before_request
def before_request():
    init_db()


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
                SELECT username, password, name, church_address, sex, age, position
                FROM sheet_accounts_cache
                WHERE username = ?
                """,
                (username,),
            ).fetchone()

            stored_role = str((row["position"] if row and "position" in row.keys() else "") or "").strip().lower()

            if row and str(row["password"] or "").strip() == password:
                session.clear()
                session["selected_position"] = selected_role
                session.permanent = True

                if selected_role == "area overseer":
                    if stored_role != "area overseer":
                        error = "This account is not registered as Area Overseer."
                    else:
                        session["ao_logged_in"] = True
                        session["ao_username"] = username
                        session["ao_name"] = row["name"] or ""
                        session["ao_area_number"] = (row["age"] or "").strip()
                        session["ao_church_id"] = (row["sex"] or "").strip()
                        return redirect(url_for("ao_tool"))
                else:
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

    if not all_reported:
        return posts

    month_label = today.strftime("%B %Y")

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
              AND LOWER(COALESCE(position,'')) != 'area overseer'
              AND TRIM(username) != ''
            ORDER BY TRIM(sex), TRIM(username)
            """,
            (ao_area,),
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

    if request.method == "POST":
        if can_submit:
            set_month_submitted(year, month, pastor_username)
            try:
                export_month_to_sheet(year, month, "Pending AO approval")
                clear_month_dirty(year, month)
                sync_from_sheets_if_needed(force=True)
                sync_local_month_from_cache_for_pastor(year, month)
            except Exception as e:
                print("Error exporting month to sheet on submit:", e)

        # ✅ keep AO selection after submit
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
        can_submit=can_submit,
        status_key=status_key,
        submitted_map=submitted_map,
        cp_complete=cp_complete,
        sundays_ok=sundays_ok,
        monthly_total=monthly_total,
        pastor_name=session.get("pastor_name", ""),
        # AO dropdown context
        ao_mode=ao_mode,
        ao_church_choices=ao_church_choices,
        ao_church_labels=ao_church_labels,
        selected_church=selected_church,
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
            SELECT username, password, name, age, sex, position
            FROM sheet_accounts_cache
            WHERE username = ?
            """,
            (username,),
        ).fetchone()

        if row and (str(row["password"] or "").strip() == password):
            pos = str((row["position"] if "position" in row.keys() else "") or "").strip().lower()
            if pos == "area overseer":
                session["ao_logged_in"] = True
                session["ao_username"] = username
                session["ao_name"] = row["name"] or ""
                session["ao_area_number"] = (row["age"] or "").strip()
                session["ao_church_id"] = (row["sex"] or "").strip()
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
    area_accounts = get_accounts_for_area_cache(area_number)

    selected_username = (request.args.get("edit_username") or "").strip()
    selected_account = None
    if selected_username:
        for row in area_accounts:
            if str(row["username"] or "").strip() == selected_username:
                selected_account = row
                break

    return render_template(
        "ao_tool.html",
        last_sync_ph=last_sync_ph,
        area_accounts=area_accounts,
        selected_account=selected_account,
        ao_area_number=area_number,
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
    if not row or str(row["age"] or "").strip() != area_number:
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
    if not row or str(row["age"] or "").strip() != area_number:
        abort(403)

    try:
        _delete_account_row_in_sheet(username)
        sync_from_sheets_if_needed(force=True)
        flash("Account deleted successfully.", "success")
    except Exception as e:
        print("❌ Delete account failed:", e)
        flash("Failed to delete account.", "error")

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

    today = date.today()
    year = request.args.get("year", type=int) or today.year

    year_options = list(range(2025, 2036))

    month_names = [
        ("January", 1), ("February", 2), ("March", 3), ("April", 4),
        ("May", 5), ("June", 6), ("July", 7), ("August", 8),
        ("September", 9), ("October", 10), ("November", 11), ("December", 12),
    ]

    all_churches = get_all_churches_from_cache()

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

        area_summary = get_area_summary_for_month_cache(year, m, church_items, aopt_amount or 0.0)

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

# Changes starts here 11/03/2026 2:02 pm until @app.route("/schedules")


DISTRICT_SCHEDULE_SHEET_NAME = "DistrictSchedule"
DISTRICT_SECRETARY_CODE = "District_Secretary_444"


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
    ]
    if not values:
        ws.append_row(headers, value_input_option="USER_ENTERED")
    return ws


def _append_district_schedule_row(payload: dict):
    ws = _ensure_district_schedule_headers()
    ws.append_row(
        [
            payload.get("church_name", ""),
            payload.get("church_address", ""),
            payload.get("pastor_name", ""),
            payload.get("contact_number", ""),
            payload.get("activity_date_start", ""),
            payload.get("activity_date_end", ""),
            payload.get("activity_type", ""),
            payload.get("note", ""),
            payload.get("joining", ""),
        ],
        value_input_option="USER_ENTERED",
    )


def _update_district_schedule_row(sheet_row: int, payload: dict):
    ws = _ensure_district_schedule_headers()
    ws.update(
        f"A{sheet_row}:I{sheet_row}",
        [[
            payload.get("church_name", ""),
            payload.get("church_address", ""),
            payload.get("pastor_name", ""),
            payload.get("contact_number", ""),
            payload.get("activity_date_start", ""),
            payload.get("activity_date_end", ""),
            payload.get("activity_type", ""),
            payload.get("note", ""),
            payload.get("joining", ""),
        ]],
        value_input_option="USER_ENTERED",
    )


def _delete_district_schedule_row(sheet_row: int):
    ws = _ensure_district_schedule_headers()
    if int(sheet_row) > 1:
        ws.delete_rows(int(sheet_row))


def _get_schedule_row_from_cache(sheet_row: int):
    db = get_db()
    return db.execute(
        """
        SELECT *
        FROM sheet_district_schedule_cache
        WHERE sheet_row = ?
        """,
        (sheet_row,),
    ).fetchone()
def _join_schedule(sheet_row: int, join_name: str):
    join_name = str(join_name or "").strip()
    if not join_name:
        return False

    row = _get_schedule_row_from_cache(sheet_row)
    if not row:
        return False

    existing = _unique_joining_names(_parse_joining_names(row["joining"]))
    existing_keys = {x.lower() for x in existing}

    if join_name.lower() not in existing_keys:
        existing.append(join_name)

    payload = {
        "church_name": row["church_name"] or "",
        "church_address": row["church_address"] or "",
        "pastor_name": row["pastor_name"] or "",
        "contact_number": row["contact_number"] or "",
        "activity_date_start": row["activity_date_start"] or "",
        "activity_date_end": row["activity_date_end"] or "",
        "activity_type": row["activity_type"] or "",
        "note": row["note"] or "",
        "joining": ", ".join(existing),
    }

    _update_district_schedule_row(sheet_row, payload)
    return True

def _get_schedule_rows_for_month(year: int, month: int):
    db = get_db()
    rows = db.execute(
        """
        SELECT *
        FROM sheet_district_schedule_cache
        ORDER BY activity_date_start ASC, church_name ASC
        """
    ).fetchall()

    filtered = []
    for r in rows:
        start_dt, end_dt = _safe_schedule_end_date(r["activity_date_start"], r["activity_date_end"])
        if not start_dt:
            continue
        if start_dt.year == year and start_dt.month == month:
            filtered.append(r)
            continue
        if end_dt and end_dt.year == year and end_dt.month == month:
            filtered.append(r)

    return filtered


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

def build_schedule_month(year: int, month: int):
    ensure_schedule_cache_loaded()

    db = get_db()
    rows = db.execute(
        """
        SELECT *
        FROM sheet_district_schedule_cache
        ORDER BY activity_date_start ASC, church_name ASC
        """
    ).fetchall()

    cal = calendar.Calendar(firstweekday=6)
    month_days = cal.monthdatescalendar(year, month)

    event_lookup = {}
    day_map = {}

    for idx, r in enumerate(rows, start=1):
        start_dt, end_dt = _safe_schedule_end_date(r["activity_date_start"], r["activity_date_end"])
        if not start_dt:
            continue

        event_id = f"evt_{idx}"
        event_obj = {
            "id": event_id,
            "sheet_row": r["sheet_row"],
            "church_name": str(r["church_name"] or "").strip(),
            "church_address": str(r["church_address"] or "").strip(),
            "pastor_name": str(r["pastor_name"] or "").strip(),
            "contact_number": str(r["contact_number"] or "").strip(),
            "activity_date_start": str(r["activity_date_start"] or "").strip(),
            "activity_date_end": str(r["activity_date_end"] or "").strip(),
            "activity_type": str(r["activity_type"] or "").strip() or "Others",
            "note": str(r["note"] or "").strip(),
            "joining": str(r["joining"] or "").strip(),
            "joining_count": len(_unique_joining_names(_parse_joining_names(r["joining"]))),
            "type_class": _schedule_type_class(r["activity_type"]),
        }
        event_lookup[event_id] = event_obj

        current = start_dt
        while current <= end_dt:
            key = current.isoformat()
            day_map.setdefault(key, []).append(event_obj)
            current = current.fromordinal(current.toordinal() + 1)

    weeks = []
    for week in month_days:
        week_cells = []
        for d in week:
            key = d.isoformat()
            items = day_map.get(key, [])
            week_cells.append(
                {
                    "date": d,
                    "iso": key,
                    "in_month": d.month == month,
                    "events": items,
                    "visible_events": items[:2],
                    "extra_count": max(0, len(items) - 2),
                }
            )
        weeks.append(week_cells)

    month_title = datetime(year, month, 1).strftime("%B %Y")
    prev_year, prev_month = (year - 1, 12) if month == 1 else (year, month - 1)
    next_year, next_month = (year + 1, 1) if month == 12 else (year, month + 1)

    return {
        "month_title": month_title,
        "year": year,
        "month": month,
        "weeks": weeks,
        "event_lookup": event_lookup,
        "prev_year": prev_year,
        "prev_month": prev_month,
        "next_year": next_year,
        "next_month": next_month,
    }


@app.route("/schedules", methods=["GET", "POST"])
def schedules():
    if request.method == "POST":
        action = (request.form.get("action") or "").strip()

        if action == "district_secretary_access":
            access_code = (request.form.get("access_code") or "").strip()
            year = (request.form.get("year") or "").strip()
            month = (request.form.get("month") or "").strip()

            if access_code == DISTRICT_SECRETARY_CODE:
                session["district_secretary_ok"] = True
                flash("District Secretary access granted.", "success")
            else:
                flash("Invalid access code.", "error")

            return redirect(url_for("schedules", year=year, month=month))

        if action == "create_schedule":
            if not session.get("district_secretary_ok"):
                flash("District Secretary access is required.", "error")
                return redirect(url_for("schedules"))

            church_name = (request.form.get("church_name") or "").strip()
            church_address = (request.form.get("church_address") or "").strip()
            pastor_name = (request.form.get("pastor_name") or "").strip()
            contact_number = (request.form.get("contact_number") or "").strip()
            activity_date_start = (request.form.get("activity_date_start") or "").strip()
            activity_date_end = (request.form.get("activity_date_end") or "").strip()
            activity_type = (request.form.get("activity_type") or "").strip()
            note = (request.form.get("note") or "").strip()
            year = (request.form.get("year") or "").strip()
            month = (request.form.get("month") or "").strip()

            if not church_name or not activity_date_start or not activity_type:
                flash("Please complete all required fields marked with *.", "error")
                return redirect(url_for("schedules", year=year, month=month))

            start_dt, end_dt = _safe_schedule_end_date(activity_date_start, activity_date_end)
            if not start_dt:
                flash("Activity Date Start is invalid.", "error")
                return redirect(url_for("schedules", year=year, month=month))

            payload = {
                "church_name": church_name,
                "church_address": church_address,
                "pastor_name": pastor_name,
                "contact_number": contact_number,
                "activity_date_start": activity_date_start,
                "activity_date_end": activity_date_end if end_dt and end_dt != start_dt else "",
                "activity_type": activity_type,
                "note": note,
                "joining": "",
            }

            try:
                _append_district_schedule_row(payload)
                sync_from_sheets_if_needed(force=True)
                flash("Schedule created successfully.", "success")
            except Exception as e:
                print("❌ Create schedule failed:", e)
                flash("Failed to create schedule.", "error")

            return redirect(url_for("schedules", year=year, month=month))

        if action == "edit_schedule":
            if not session.get("district_secretary_ok"):
                flash("District Secretary access is required.", "error")
                return redirect(url_for("schedules"))

            sheet_row = int(request.form.get("sheet_row") or 0)
            church_name = (request.form.get("church_name") or "").strip()
            church_address = (request.form.get("church_address") or "").strip()
            pastor_name = (request.form.get("pastor_name") or "").strip()
            contact_number = (request.form.get("contact_number") or "").strip()
            activity_date_start = (request.form.get("activity_date_start") or "").strip()
            activity_date_end = (request.form.get("activity_date_end") or "").strip()
            activity_type = (request.form.get("activity_type") or "").strip()
            note = (request.form.get("note") or "").strip()
            year = (request.form.get("year") or "").strip()
            month = (request.form.get("month") or "").strip()

            if not sheet_row or not church_name or not activity_date_start or not activity_type:
                flash("Please complete all required fields marked with *.", "error")
                return redirect(url_for("schedules", year=year, month=month))

            start_dt, end_dt = _safe_schedule_end_date(activity_date_start, activity_date_end)
            if not start_dt:
                flash("Activity Date Start is invalid.", "error")
                return redirect(url_for("schedules", year=year, month=month))

            payload = {
                "church_name": church_name,
                "church_address": church_address,
                "pastor_name": pastor_name,
                "contact_number": contact_number,
                "activity_date_start": activity_date_start,
                "activity_date_end": activity_date_end if end_dt and end_dt != start_dt else "",
                "activity_type": activity_type,
                "note": note,
                "joining": "",
            }

            try:
                _update_district_schedule_row(sheet_row, payload)
                sync_from_sheets_if_needed(force=True)
                flash("Schedule updated successfully.", "success")
            except Exception as e:
                print("❌ Edit schedule failed:", e)
                flash("Failed to update schedule.", "error")

            return redirect(url_for("schedules", year=year, month=month))

        if action == "delete_schedule":
            if not session.get("district_secretary_ok"):
                flash("District Secretary access is required.", "error")
                return redirect(url_for("schedules"))

            sheet_row = int(request.form.get("sheet_row") or 0)
            year = (request.form.get("year") or "").strip()
            month = (request.form.get("month") or "").strip()

            if not sheet_row:
                flash("Invalid schedule selected.", "error")
                return redirect(url_for("schedules", year=year, month=month))

            try:
                _delete_district_schedule_row(sheet_row)
                sync_from_sheets_if_needed(force=True)
                flash("Schedule deleted successfully.", "success")
            except Exception as e:
                print("❌ Delete schedule failed:", e)
                flash("Failed to delete schedule.", "error")

            return redirect(url_for("schedules", year=year, month=month))

        if action == "join_schedule":
            sheet_row = int(request.form.get("sheet_row") or 0)
            join_name = (request.form.get("join_name") or "").strip()
            year = (request.form.get("year") or "").strip()
            month = (request.form.get("month") or "").strip()

            remembered_name = (session.get("schedule_join_name") or "").strip()
            effective_name = join_name or remembered_name

            if not sheet_row:
                flash("Invalid schedule selected.", "error")
                return redirect(url_for("schedules", year=year, month=month))

            if not effective_name:
                flash("Name is required to join this activity.", "error")
                return redirect(url_for("schedules", year=year, month=month))

            try:
                _join_schedule(sheet_row, effective_name)
                session["schedule_join_name"] = effective_name
                sync_from_sheets_if_needed(force=True)
                flash("You have been added as joining.", "success")
            except Exception as e:
                print("❌ Join schedule failed:", e)
                flash("Failed to join this activity.", "error")

            return redirect(url_for("schedules", year=year, month=month))

    today = date.today()
    try:
        year = int(request.args.get("year", today.year))
    except Exception:
        year = today.year
    try:
        month = int(request.args.get("month", today.month))
    except Exception:
        month = today.month

    if month < 1 or month > 12:
        month = today.month

    ensure_schedule_cache_loaded()

    calendar_data = build_schedule_month(year, month)
    month_rows = _get_schedule_rows_for_month(year, month)

    return render_template(
        "schedules.html",
        month_title=calendar_data["month_title"],
        year=calendar_data["year"],
        month=calendar_data["month"],
        weeks=calendar_data["weeks"],
        event_lookup=calendar_data["event_lookup"],
        prev_year=calendar_data["prev_year"],
        prev_month=calendar_data["prev_month"],
        next_year=calendar_data["next_year"],
        next_month=calendar_data["next_month"],
        district_secretary_ok=bool(session.get("district_secretary_ok")),
        user_logged_in=any_user_logged_in(),
        month_rows=month_rows,
        activity_types=[
            "Thanksgiving",
            "Convention",
            "Area Activities",
            "District Prayer & Fasting",
            "Others",
        ],
    )
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

    row = [[
        payload.get("full_name", ""),
        payload.get("age", ""),
        payload.get("sex", ""),
        payload.get("church_address", ""),
        payload.get("contact_number", ""),
        payload.get("birthday", ""),
        payload.get("username", ""),
        payload.get("password", ""),
        payload.get("position", "Pastor"),
    ]]

    ws.update(f"A{sheet_row}:I{sheet_row}", row, value_input_option="USER_ENTERED")
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
