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
# SQLite helpers (secondary)
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


def church_progress_complete(monthly_report_id: int) -> bool:
    db = get_db()
    cursor = db.cursor()
    cursor.execute(
        "SELECT is_complete FROM church_progress WHERE monthly_report_id = ?",
        (monthly_report_id,),
    )
    row = cursor.fetchone()
    if not row:
        return False
    return bool(row["is_complete"])


# ------------------------
# Google Sheets integration (PRIMARY)
# ------------------------
GOOGLE_SHEETS_CREDENTIALS_FILE = "service_account.json"
GOOGLE_SHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
SHEET_NAME = "District4 Data"
TAB_ACCOUNTS = "Accounts"
TAB_REPORT = "Report"


def get_gs_client():
    creds = Credentials.from_service_account_file(
        GOOGLE_SHEETS_CREDENTIALS_FILE,
        scopes=GOOGLE_SHEETS_SCOPES,
    )
    return gspread.authorize(creds)


def _norm(s: str) -> str:
    return (s or "").strip().lower()


def _get_any(rec: dict, *possible_keys, default=""):
    """
    Case-insensitive getter for gspread records.
    """
    if not rec:
        return default
    lowered = {str(k).strip().lower(): v for k, v in rec.items()}
    for k in possible_keys:
        v = lowered.get(str(k).strip().lower(), None)
        if v is not None:
            return v
    return default


def parse_float(value):
    try:
        s = str(value).strip()
        if s == "":
            return 0.0
        s = s.replace(",", "")
        return float(s)
    except Exception:
        return 0.0


def refresh_pastor_from_sheets():
    username = session.get("pastor_username")
    if not username:
        return False

    try:
        client = get_gs_client()
        sh = client.open(SHEET_NAME)
        ws = sh.worksheet(TAB_ACCOUNTS)
        records = ws.get_all_records()
    except Exception as e:
        print("Error refreshing pastor data from Sheets:", e)
        return False

    for rec in records:
        sheet_username = str(_get_any(rec, "UserName", "username")).strip()
        if sheet_username == username:
            session["pastor_name"] = _get_any(rec, "Name", "full_name", default="")
            session["pastor_church_address"] = _get_any(rec, "Church Address", "church", "address", default="")
            return True

    return False


def append_account_to_sheet(pastor_data: dict):
    client = get_gs_client()
    sh = client.open(SHEET_NAME)

    try:
        worksheet = sh.worksheet(TAB_ACCOUNTS)
    except gspread.WorksheetNotFound:
        worksheet = sh.add_worksheet(title=TAB_ACCOUNTS, rows=200, cols=20)

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
    sh = client.open(SHEET_NAME)

    try:
        worksheet = sh.worksheet(TAB_REPORT)
    except gspread.WorksheetNotFound:
        worksheet = sh.add_worksheet(title=TAB_REPORT, rows=2000, cols=30)

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


def export_month_to_sheet(year: int, month: int, status_label: str):
    """
    Sends DB data -> Google Sheets ONLY when Submit pressed.
    """
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
            print("Error sending row to Google Sheets:", e)


def get_all_churches_from_accounts():
    """
    Used by AO Church Status page to show ALL churches, even if no report yet.
    """
    churches = []
    try:
        client = get_gs_client()
        sh = client.open(SHEET_NAME)
        ws = sh.worksheet(TAB_ACCOUNTS)
        records = ws.get_all_records()
        for rec in records:
            church = str(_get_any(rec, "Church Address", "church", "address")).strip()
            if church:
                churches.append(church)
    except Exception as e:
        print("Error reading Accounts for church list:", e)

    # unique, keep order
    uniq = []
    seen = set()
    for c in churches:
        k = c.lower()
        if k not in seen:
            seen.add(k)
            uniq.append(c)
    return uniq


def read_report_records():
    try:
        client = get_gs_client()
        sh = client.open(SHEET_NAME)
        ws = sh.worksheet(TAB_REPORT)
        return ws.get_all_records(), ws
    except Exception as e:
        print("Error reading Report sheet:", e)
        return [], None


def month_prefix(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}-"


def compute_church_month_summary(records, year: int, month: int, church: str):
    """
    Returns summary for ONE church in ONE month.
    Averages: attendance + spiritual
    Totals: financial + amount_to_send
    Also returns derived status: not_submitted / pending / approved.
    """
    pref = month_prefix(year, month)
    church_key = _norm(church)

    rows = []
    statuses = set()

    for rec in records:
        activity_date = str(_get_any(rec, "activity_date", default="")).strip()
        if not activity_date.startswith(pref):
            continue

        rec_church = str(_get_any(rec, "church", default="")).strip()
        if _norm(rec_church) != church_key:
            continue

        rows.append(rec)
        st = str(_get_any(rec, "status", default="")).strip()
        if st:
            statuses.add(st.lower())

    if not rows:
        return {
            "church": church,
            "has_data": False,
            "status_key": "not_submitted",
            "sheet_status": "",
            "rows": 0,
            "avg_attendance_total": 0.0,
            "attendance_change": None,
            "avg": {},
            "totals": {
                "tithes": 0.0,
                "offering": 0.0,
                "personal_tithes": 0.0,
                "mission_offering": 0.0,
                "amount_to_send": 0.0,
            },
        }

    # sums for avg fields
    sum_avg = {
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
    }

    totals = {
        "tithes": 0.0,
        "offering": 0.0,
        "personal_tithes": 0.0,
        "mission_offering": 0.0,
        "amount_to_send": 0.0,
    }

    for rec in rows:
        sum_avg["adult"] += parse_float(_get_any(rec, "adult", default=0))
        sum_avg["youth"] += parse_float(_get_any(rec, "youth", default=0))
        sum_avg["children"] += parse_float(_get_any(rec, "children", default=0))

        sum_avg["received_jesus"] += parse_float(_get_any(rec, "received jesus", "received_jesus", default=0))
        sum_avg["existing_bible_study"] += parse_float(_get_any(rec, "existing bible study", "existing_bible_study", default=0))
        sum_avg["new_bible_study"] += parse_float(_get_any(rec, "new bible study", "new_bible_study", default=0))
        sum_avg["water_baptized"] += parse_float(_get_any(rec, "water baptized", "water_baptized", default=0))
        sum_avg["holy_spirit_baptized"] += parse_float(_get_any(rec, "holy spirit baptized", "holy_spirit_baptized", default=0))
        sum_avg["childrens_dedication"] += parse_float(_get_any(rec, "childrens dedication", "childrens_dedication", default=0))
        sum_avg["healed"] += parse_float(_get_any(rec, "healed", default=0))

        totals["tithes"] += parse_float(_get_any(rec, "tithes", default=0))
        totals["offering"] += parse_float(_get_any(rec, "offering", default=0))
        totals["personal_tithes"] += parse_float(_get_any(rec, "personal tithes", "personal_tithes", default=0))
        totals["mission_offering"] += parse_float(_get_any(rec, "mission offering", "mission_offering", default=0))
        totals["amount_to_send"] += parse_float(_get_any(rec, "amount to send", "amount_to_send", default=0))

    n = len(rows)
    avg = {k: (sum_avg[k] / n) for k in sum_avg.keys()}
    avg_att_total = avg["adult"] + avg["youth"] + avg["children"]

    # status
    if len(statuses) == 1 and "approved" in list(statuses)[0]:
        status_key = "approved"
        sheet_status = "Approved"
    elif any("pending" in s for s in statuses):
        status_key = "pending"
        sheet_status = "Pending AO approval"
    elif any("approved" in s for s in statuses):
        status_key = "approved"
        sheet_status = "Approved"
    else:
        status_key = "pending"
        sheet_status = "Pending AO approval"

    return {
        "church": church,
        "has_data": True,
        "status_key": status_key,
        "sheet_status": sheet_status,
        "rows": n,
        "avg_attendance_total": avg_att_total,
        "attendance_change": None,  # computed later vs previous month
        "avg": avg,
        "totals": totals,
    }


def update_sheet_status_for_church_month(year: int, month: int, church: str, new_status: str):
    """
    Update ONLY rows in Report that match:
      - activity_date prefix
      - church exact match (case-insensitive)
    Then set status column to new_status.
    """
    records, ws = read_report_records()
    if not ws:
        return

    values = ws.get_all_values()
    if not values:
        return

    header = values[0]
    header_l = [_norm(h) for h in header]

    # Find column indexes
    try:
        idx_activity = header_l.index("activity_date")
    except ValueError:
        # fallback if header differs
        idx_activity = header_l.index("activity date") if "activity date" in header_l else None

    # status column
    idx_status = None
    for cand in ["status"]:
        if cand in header_l:
            idx_status = header_l.index(cand)
            break

    # church column
    idx_church = None
    for cand in ["church"]:
        if cand in header_l:
            idx_church = header_l.index(cand)
            break

    if idx_activity is None or idx_status is None or idx_church is None:
        print("Cannot find required columns in Report header.")
        return

    pref = month_prefix(year, month)
    church_key = _norm(church)

    # data starts at row 2
    for r in range(2, len(values) + 1):
        row = values[r - 1]
        if len(row) <= max(idx_activity, idx_status, idx_church):
            continue

        activity_date = str(row[idx_activity]).strip()
        if not activity_date.startswith(pref):
            continue

        row_church = str(row[idx_church]).strip()
        if _norm(row_church) != church_key:
            continue

        # update status cell
        try:
            ws.update_cell(r, idx_status + 1, new_status)
        except Exception as e:
            print("Error updating status:", e)


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
# Pastor login (Sheets)
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
                sh = client.open(SHEET_NAME)
                ws = sh.worksheet(TAB_ACCOUNTS)
                records = ws.get_all_records()
                matched = None
                for rec in records:
                    sheet_username = str(_get_any(rec, "UserName", "username")).strip()
                    sheet_password = str(_get_any(rec, "Password", "password")).strip()
                    if username == sheet_username and password == sheet_password:
                        matched = rec
                        break

                if matched:
                    session["pastor_logged_in"] = True
                    session["pastor_username"] = username
                    session["pastor_name"] = _get_any(matched, "Name", "full_name", default="")
                    session["pastor_church_address"] = _get_any(matched, "Church Address", "church", "address", default="")
                    session.permanent = True
                    form_next = request.form.get("next")
                    return redirect(form_next or next_url)
                else:
                    error = "Invalid username or password."
            except Exception as e:
                error = f"Error accessing Google Sheets: {e}"

    return render_template("pastor_login.html", error=error, next_url=next_url)


# ------------------------
# Pastor Tool (DB working; Sheets primary for status)
# ------------------------
@app.route("/pastor-tool", methods=["GET", "POST"])
def pastor_tool():
    if not pastor_logged_in():
        return redirect(url_for("pastor_login", next=request.path))

    refresh_pastor_from_sheets()

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
        ("January", 1),
        ("February", 2),
        ("March", 3),
        ("April", 4),
        ("May", 5),
        ("June", 6),
        ("July", 7),
        ("August", 8),
        ("September", 9),
        ("October", 10),
        ("November", 11),
        ("December", 12),
    ]

    sundays_ok = all_sundays_complete(monthly_report["id"])
    can_submit = sundays_ok and cp_complete

    if request.method == "POST":
        if can_submit:
            # Submit -> write to Sheets
            try:
                export_month_to_sheet(year, month, "Pending AO approval")
            except Exception as e:
                print("Error exporting month:", e)
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
                error = "Please enter numbers only."
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
                error = "All fields are required."
                break
            try:
                numeric_values[field] = int(raw)
            except ValueError:
                error = "Please enter whole numbers only."
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
# AO login
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


# ------------------------
# AO Tool MAIN MENU (restored)
# ------------------------
@app.route("/ao-tool")
def ao_tool():
    if not ao_logged_in():
        return redirect(url_for("ao_login", next=request.path))

    return render_template("ao_tool.html")


# ------------------------
# AO Church Status PAGE (your drawing)
# ------------------------
@app.route("/ao-tool/church-status")
def ao_church_status():
    if not ao_logged_in():
        return redirect(url_for("ao_login", next=request.path))

    today = date.today()
    year = request.args.get("year", type=int) or today.year
    year_options = list(range(today.year - 1, today.year + 4))

    # All churches from Accounts tab
    churches = get_all_churches_from_accounts()

    # Read all report rows once
    report_records, _ = read_report_records()

    # months list
    month_names = [
        ("January", 1),
        ("February", 2),
        ("March", 3),
        ("April", 4),
        ("May", 5),
        ("June", 6),
        ("July", 7),
        ("August", 8),
        ("September", 9),
        ("October", 10),
        ("November", 11),
        ("December", 12),
    ]

    months = []
    for name, m in month_names:
        # compute per church summaries
        church_items = []
        prev_month_avg_by_church = {}  # for % change inside same year (simple)

        for church in churches:
            summary = compute_church_month_summary(report_records, year, m, church)

            # attendance change vs previous month for THIS church
            if m > 1:
                prev = compute_church_month_summary(report_records, year, m - 1, church)
                if prev["has_data"] and prev["avg_attendance_total"] > 0 and summary["has_data"]:
                    summary["attendance_change"] = (
                        (summary["avg_attendance_total"] - prev["avg_attendance_total"])
                        / prev["avg_attendance_total"]
                    ) * 100.0
                else:
                    summary["attendance_change"] = None

            church_items.append(summary)

        # month overall state (for coloring the month bar)
        any_submitted = any(ci["has_data"] for ci in church_items)
        all_missing = all(not ci["has_data"] for ci in church_items)

        if all_missing:
            month_status = "not_submitted"  # red
        elif any_submitted:
            month_status = "pending"  # green-ish (submitted exists)
        else:
            month_status = "not_submitted"

        months.append(
            {
                "name": name,
                "month": m,
                "year": year,
                "status_key": month_status,
                "church_items": church_items,
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

    year = int(request.form.get("year", "0") or 0)
    month = int(request.form.get("month", "0") or 0)
    church = (request.form.get("church") or "").strip()

    if year <= 0 or month <= 0 or not church:
        return redirect(url_for("ao_church_status"))

    try:
        update_sheet_status_for_church_month(year, month, church, "Approved")
    except Exception as e:
        print("Approve error:", e)

    return redirect(url_for("ao_church_status", year=year))


# ------------------------
# Create account (still works)
# ------------------------
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

        if not full_name or not age_raw or not sex or not church_address or not contact_number or not birthday_raw:
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
# Other pages (unchanged)
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
