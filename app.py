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


def get_db():
    db = getattr(g, "_database", None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE, check_same_thread=False)
        db.row_factory = sqlite3.Row
    return db


def init_db():
    db = get_db()
    cursor = db.cursor()

    # Monthly reports: one row per year+month
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

    # Sunday reports: weekly attendance + financials
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

    # Church progress: monthly totals for spiritual metrics
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

    # Verse of the day cache
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

    # Pastor accounts (with birthday)
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

    # Make sure "birthday" exists for older DBs
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
    """Return list of date objects for all Sundays in given month."""
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
# Google Sheets integration
# ------------------------

GOOGLE_SHEETS_CREDENTIALS_FILE = "service_account.json"

GOOGLE_SHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def get_gs_client():
    """
    Returns an authorized gspread client using the service account JSON file.
    """
    creds = Credentials.from_service_account_file(
        GOOGLE_SHEETS_CREDENTIALS_FILE,
        scopes=GOOGLE_SHEETS_SCOPES,
    )
    return gspread.authorize(creds)


def refresh_pastor_from_sheets():
    """
    Refresh pastor_name and pastor_church_address in session
    based on pastor_username from the Accounts sheet.
    """
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
        sheet_username = str(rec.get("UserName", "")).strip()
        if sheet_username == username:
            session["pastor_name"] = rec.get("Name", "")
            session["pastor_church_address"] = rec.get("Church Address", "")
            return True

    return False


def append_account_to_sheet(pastor_data: dict):
    """
    Append a pastor account row to the 'Accounts' tab in 'District4 Data' sheet.
    Expects keys:
      full_name, age, sex, church_address, contact_number, birthday, username, password
    """
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
    """
    Append a raw report row to the 'Report' tab in 'District4 Data' sheet.

    Columns (in order):
      church,
      pastor,
      address,
      adult,
      youth,
      children,
      tithes,
      offering,
      personal tithes,
      mission offering,
      received jesus,
      existing bible study,
      new bible study,
      water baptized,
      holy spirit baptized,
      childrens dedication,
      healed,
      activity_date,
      amount to send,
      status
    """
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
    """
    Update the 'status' column in 'Report' for all rows whose activity_date is in the given year+month.
    """
    try:
        client = get_gs_client()
        sh = client.open("District4 Data")
        ws = sh.worksheet("Report")
    except Exception as e:
        print("Error accessing Report sheet for status update:", e)
        return

    try:
        values = ws.get_all_values()
    except Exception as e:
        print("Error reading Report sheet:", e)
        return

    if not values:
        return

    # Header is row 1; data starts from row_index = 2
    target_prefix = f"{year:04d}-{month:02d}-"
    for i in range(1, len(values)):
        row = values[i]
        # Need at least up to activity_date column (index 17)
        if len(row) < 18:
            continue
        activity_date = row[17]  # 18th column (0-based index 17)
        if activity_date.startswith(target_prefix):
            sheet_row = i + 1  # 1-based row index in sheet
            try:
                # Status is 20th column (1-based index 20, 0-based index 19)
                ws.update_cell(sheet_row, 20, status_label)
            except Exception as e:
                print(f"Error updating status for row {sheet_row}:", e)


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
    """
    Read Report sheet and compute:
      - averages for attendance & spiritual metrics
      - totals for financial metrics + total amount_to_send
    for all rows in the given year+month.
    """
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

    sum_fields = {
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

    statuses = set()
    count = 0

    for rec in records:
        activity_date = str(rec.get("activity_date", "")).strip()
        if not activity_date.startswith(prefix):
            continue

        if count == 0:
            stats["church"] = rec.get("church", "")
            stats["pastor"] = rec.get("pastor", "")
            stats["address"] = rec.get("address", "")

        sum_fields["adult"] += parse_float(rec.get("adult", 0))
        sum_fields["youth"] += parse_float(rec.get("youth", 0))
        sum_fields["children"] += parse_float(rec.get("children", 0))
        sum_fields["received_jesus"] += parse_float(rec.get("received jesus", 0))
        sum_fields["existing_bible_study"] += parse_float(
            rec.get("existing bible study", 0)
        )
        sum_fields["new_bible_study"] += parse_float(rec.get("new bible study", 0))
        sum_fields["water_baptized"] += parse_float(rec.get("water baptized", 0))
        sum_fields["holy_spirit_baptized"] += parse_float(
            rec.get("holy spirit baptized", 0)
        )
        sum_fields["childrens_dedication"] += parse_float(
            rec.get("childrens dedication", 0)
        )
        sum_fields["healed"] += parse_float(rec.get("healed", 0))

        totals["tithes"] += parse_float(rec.get("tithes", 0))
        totals["offering"] += parse_float(rec.get("offering", 0))
        totals["personal_tithes"] += parse_float(rec.get("personal tithes", 0))
        totals["mission_offering"] += parse_float(rec.get("mission offering", 0))
        totals["amount_to_send"] += parse_float(rec.get("amount to send", 0))

        status_val = str(rec.get("status", "")).strip()
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
    """
    Export all Sundays for the given month as rows in the Report sheet.
    Each Sunday row will include the same Church Progress values (for that month).
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

    # Fetch all Sundays for this month
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

    # Ensure pastor info is fresh from Sheets
    refresh_pastor_from_sheets()
    pastor_name = session.get("pastor_name", "")
    church_address = session.get("pastor_church_address", "")

    # Church Progress values (will be copied into every Sunday row)
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
            "church": church_address,  # can change later if you add a separate church name
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
            print("Error sending monthly Sunday row to Google Sheets:", e)


def sync_from_sheets_for_pastor():
    """
    For the currently logged-in pastor, read all rows from the Report sheet
    that belong to this pastor/church, and rebuild the local SQLite data:
      - monthly_reports
      - sunday_reports
      - church_progress
      - submitted / approved flags
    """
    username = session.get("pastor_username")
    if not username:
        return

    # Refresh name & church address from Accounts sheet
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

    cp_cache = {}           # (year, month) -> church progress values
    statuses_by_month = {}  # (year, month) -> set(status strings)

    for rec in records:
        rec_pastor = str(rec.get("pastor", "")).strip()
        rec_address = str(rec.get("address", "")).strip()

        # Match by church address if available
        if church_address and rec_address != church_address:
            continue

        # Optional extra check by pastor name
        if pastor_name and rec_pastor and rec_pastor != pastor_name:
            continue

        activity_date_str = str(rec.get("activity_date", "")).strip()
        if not activity_date_str:
            continue

        try:
            d = date.fromisoformat(activity_date_str)
        except ValueError:
            continue

        year = d.year
        month = d.month
        day_iso = d.isoformat()

        monthly_report = get_or_create_monthly_report(year, month)
        mrid = monthly_report["id"]

        # Ensure sunday_report row exists for that date
        cursor.execute(
            """
            SELECT * FROM sunday_reports
            WHERE monthly_report_id = ? AND date = ?
            """,
            (mrid, day_iso),
        )
        srow = cursor.fetchone()

        adult = parse_float(rec.get("adult", 0))
        youth = parse_float(rec.get("youth", 0))
        children = parse_float(rec.get("children", 0))
        tithes_church = parse_float(rec.get("tithes", 0))
        offering = parse_float(rec.get("offering", 0))
        mission = parse_float(rec.get("mission offering", 0))
        tithes_personal = parse_float(rec.get("personal tithes", 0))

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

        key = (year, month)
        if key not in cp_cache:
            cp_cache[key] = {
                "bible_new": int(parse_float(rec.get("new bible study", 0))),
                "bible_existing": int(parse_float(rec.get("existing bible study", 0))),
                "received_christ": int(parse_float(rec.get("received jesus", 0))),
                "baptized_water": int(parse_float(rec.get("water baptized", 0))),
                "baptized_holy_spirit": int(
                    parse_float(rec.get("holy spirit baptized", 0))
                ),
                "healed": int(parse_float(rec.get("healed", 0))),
                "child_dedication": int(
                    parse_float(rec.get("childrens dedication", 0))
                ),
            }

        status_val = str(rec.get("status", "")).strip()
        if status_val:
            statuses_by_month.setdefault(key, set()).add(status_val)

    # Apply church progress + submitted/approved flags per month
    for (year, month), cpvals in cp_cache.items():
        monthly_report = get_or_create_monthly_report(year, month)
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

        statuses = statuses_by_month.get((year, month), set())
        submitted = 1 if statuses else 0
        approved = 0
        for s in statuses:
            if "approved" in s.lower():
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


# Bible verse of the day logic
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
    """Fetch verse of the day from DB or bible-api.com (once per day)."""
    today_str = date.today().isoformat()
    db = get_db()
    cursor = db.cursor()

    # Check cache
    cursor.execute("SELECT * FROM verses WHERE date = ?", (today_str,))
    row = cursor.fetchone()
    if row:
        return row["reference"], row["text"]

    # Pick a reference deterministic by date
    idx = date.today().toordinal() % len(VERSE_REFERENCES)
    reference = VERSE_REFERENCES[idx]

    # Call bible-api.com (public, free KJV API)
    verse_text = reference
    try:
        encoded_ref = urllib.parse.quote(reference)
        resp = requests.get(f"https://bible-api.com/{encoded_ref}", timeout=5)
        resp.raise_for_status()
        data = resp.json()
        if "text" in data and data["text"].strip():
            verse_text = data["text"].strip()
    except Exception:
        # Fallback: just show reference
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


def ao_logged_in():
    return session.get("ao_logged_in") is True


def pastor_logged_in():
    return session.get("pastor_logged_in") is True


def generate_pastor_credentials(full_name: str, age: int):
    """
    Generate a simple username & password:
    - username: first name in lowercase, with a number suffix if needed
    - password: FirstName + age (e.g., Juan42)
    """
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
        cursor.execute(
            "SELECT 1 FROM pastors WHERE username = ?",
            (username,),
        )
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


app = Flask(__name__)
app.secret_key = "change-this-secret-key-123"


@app.before_request
def before_request():
    # Ensure DB is initialized before each request
    init_db()


@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, "_database", None)
    if db is not None:
        db.close()


@app.route("/")
def splash():
    # Splash screen → then redirect via JS in template to /bulletin
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
# Pastor login (using Google Sheets Accounts tab)
# ------------------------
@app.route("/pastor-login", methods=["GET", "POST"])
def pastor_login():
    error = None    # noqa: F841
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
                try:
                    ws = sh.worksheet("Accounts")
                except gspread.WorksheetNotFound:
                    error = "Accounts sheet not found in District4 Data."
                else:
                    records = ws.get_all_records()
                    matched = None
                    for rec in records:
                        sheet_username = str(rec.get("UserName", "")).strip()
                        sheet_password = str(rec.get("Password", "")).strip()
                        if username == sheet_username and password == sheet_password:
                            matched = rec
                            break

                    if matched:
                        session["pastor_logged_in"] = True
                        session["pastor_username"] = username
                        session["pastor_name"] = matched.get("Name", "")
                        session["pastor_church_address"] = matched.get(
                            "Church Address", ""
                        )
                        session.permanent = True

                        # After successful login, rebuild local DB from Sheets
                        try:
                            sync_from_sheets_for_pastor()
                        except Exception as e:
                            print("Error syncing from Sheets after login:", e)

                        form_next = request.form.get("next")
                        if form_next:
                            next_url_final = form_next
                        else:
                            next_url_final = next_url
                        return redirect(next_url_final)
                    else:
                        error = "Invalid username or password."
            except Exception as e:
                error = f"Error accessing Google Sheets: {e}"

    return render_template("pastor_login.html", error=error, next_url=next_url)


# ------------------------
# Pastor Tool (requires login)
# ------------------------
@app.route("/pastor-tool", methods=["GET", "POST"])
def pastor_tool():
    if not pastor_logged_in():
        return redirect(url_for("pastor_login", next=request.path))

    # Refresh pastor info + sync local DB from Sheets
    refresh_pastor_from_sheets()
    try:
        sync_from_sheets_for_pastor()
    except Exception as e:
        print("Error syncing from Sheets on pastor_tool:", e)

    # Determine selected month/year (default: current)
    today = date.today()
    year = request.args.get("year", type=int) or today.year
    month = request.args.get("month", type=int) or today.month

    # Fetch or create monthly report and Sunday reports
    monthly_report = get_or_create_monthly_report(year, month)
    ensure_sunday_reports(monthly_report["id"], year, month)
    sunday_rows = get_sunday_reports(monthly_report["id"])

    # Ensure church progress row exists
    cp_row = ensure_church_progress(monthly_report["id"])
    cp_complete = bool(cp_row["is_complete"])

    # Build a list of Sundays with display-friendly values
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

    # Compute monthly total (for display only)
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

    # List of years for dropdown (current year +/- 3 years)
    year_options = list(range(today.year - 1, today.year + 4))

    # Month options (1–12)
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

    # Determine if submit/resubmit is enabled
    sundays_ok = all_sundays_complete(monthly_report["id"])
    can_submit = sundays_ok and cp_complete
    status_key = get_month_status(monthly_report)

    # For dropdown checkmarks: which months are submitted in this year
    cursor.execute(
        "SELECT year, month, submitted FROM monthly_reports WHERE year = ?",
        (year,),
    )
    submitted_map = {
        (row["year"], row["month"]): bool(row["submitted"])
        for row in cursor.fetchall()
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
            if form_next:
                next_url_final = form_next
            else:
                next_url_final = next_url
            return redirect(next_url_final)
        else:
            error = "Invalid username or password."

    return render_template("ao_login.html", error=error, next_url=next_url)


@app.route("/ao-tool")
def ao_tool():
    if not ao_logged_in():
        return redirect(url_for("ao_login", next=request.path))

    today = date.today()
    year = request.args.get("year", type=int) or today.year

    # Year choices for dropdown
    year_options = list(range(today.year - 1, today.year + 4))

    # Month names
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
    prev_avg_attendance = None  # for % change vs previous month

    for name, value in month_names:
        stats = get_report_stats_for_month(year, value)
        has_data = stats["rows"] > 0
        total_amount = stats["totals"]["amount_to_send"]
        sheet_status = stats["sheet_status"] or ""

        # Decide status_key for colors
        if not has_data:
            status_key = "not_submitted"
        else:
            ss_lower = sheet_status.lower()
            if "approved" in ss_lower:
                status_key = "approved"
            elif "pending" in ss_lower:
                status_key = "pending"
            else:
                status_key = "pending"

        # Simple average attendance = adult + youth + children
        avg_att = (
            stats["avg"]["adult"]
            + stats["avg"]["youth"]
            + stats["avg"]["children"]
        )

        # Compute % change vs previous month (if we have data)
        if prev_avg_attendance is None or prev_avg_attendance == 0 or not has_data:
            attendance_change = None
        else:
            attendance_change = (
                (avg_att - prev_avg_attendance) / prev_avg_attendance
            ) * 100.0

        if has_data:
            prev_avg_attendance = avg_att

        months.append(
            {
                "name": name,
                "month": value,
                "year": year,
                "has_data": has_data,
                "status_key": status_key,
                "total_amount": total_amount,
                "church": stats["church"],
                "sheet_status": sheet_status,
                "attendance_change": attendance_change,
            }
        )

    return render_template(
        "ao_tool.html",
        year=year,
        year_options=year_options,
        months=months,
    )


@app.route("/ao-tool/<int:year>/<int:month>", methods=["GET", "POST"])
def ao_month_detail(year, month):
    if not ao_logged_in():
        return redirect(url_for("ao_login", next=request.path))

    db = get_db()
    cursor = db.cursor()

    # Ensure monthly_report row exists even if DB was reset
    monthly_report = get_or_create_monthly_report(year, month)

    if request.method == "POST":
        action = (request.form.get("action") or "").strip().lower()
        if action == "approve":
            set_month_approved(year, month)
            try:
                update_sheet_status_for_month(year, month, "Approved")
            except Exception as e:
                print("Error updating status to Approved:", e)
        elif action == "pending":
            set_month_pending(year, month)
            try:
                update_sheet_status_for_month(year, month, "Pending AO approval")
            except Exception as e:
                print("Error updating status to Pending:", e)
        return redirect(url_for("ao_month_detail", year=year, month=month))

    status_key = get_month_status(monthly_report)

    month_label = date(year, month, 1).strftime("%B %Y")
    submitted_at = monthly_report["submitted_at"]
    approved_at = monthly_report["approved_at"]

    sheet_stats = get_report_stats_for_month(year, month)

    can_approve = status_key != "not_submitted"

    return render_template(
        "ao_month_detail.html",
        year=year,
        month=month,
        month_label=month_label,
        status_key=status_key,
        submitted_at=submitted_at,
        approved_at=approved_at,
        sheet_stats=sheet_stats,
        can_approve=can_approve,
    )


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
