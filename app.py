# app.py - District 4 Tool (stabilized build)
# Requires: Flask, gspread, google-auth, requests
# Templates expected in ./templates

import os, sqlite3, calendar, uuid, urllib.parse, traceback
from datetime import datetime, date
from zoneinfo import ZoneInfo

import requests
import gspread
from google.oauth2.service_account import Credentials
from flask import Flask, g, render_template, request, redirect, url_for, abort, session

DATABASE = "app_v2.db"
PH_TZ = ZoneInfo("Asia/Manila")

GOOGLE_SHEETS_CREDENTIALS_FILE = "service_account.json"
GOOGLE_SHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

def get_db():
    db = getattr(g, "_db", None)
    if db is None:
        db = g._db = sqlite3.connect(DATABASE, check_same_thread=False)
        db.row_factory = sqlite3.Row
    return db

def init_db():
    db = get_db()
    cur = db.cursor()

    cur.execute("""
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
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sunday_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            monthly_report_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            is_complete INTEGER DEFAULT 0,
            attendance_adult REAL,
            attendance_youth REAL,
            attendance_children REAL,
            tithes_church REAL,
            offering REAL,
            mission REAL,
            tithes_personal REAL,
            FOREIGN KEY (monthly_report_id) REFERENCES monthly_reports(id),
            UNIQUE(monthly_report_id, date)
        )
    """)

    cur.execute("""
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
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS verses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT UNIQUE NOT NULL,
            reference TEXT NOT NULL,
            text TEXT NOT NULL
        )
    """)

    # cache tables for sheets
    cur.execute("""CREATE TABLE IF NOT EXISTS sync_state (id INTEGER PRIMARY KEY CHECK(id=1), last_sync TEXT)""")
    cur.execute("INSERT OR IGNORE INTO sync_state (id, last_sync) VALUES (1, NULL)")

    cur.execute("""
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
    """)

    cur.execute("""
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
    """)

    cur.execute("""
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
    """)

    db.commit()

def get_gs_client():
    creds = Credentials.from_service_account_file(
        GOOGLE_SHEETS_CREDENTIALS_FILE, scopes=GOOGLE_SHEETS_SCOPES
    )
    return gspread.authorize(creds)

def parse_float(v):
    try:
        s = str(v).strip().replace(",", "")
        return float(s) if s else 0.0
    except Exception:
        return 0.0

def parse_sheet_date(v):
    s = str(v or "").strip()
    if not s:
        return None
    try:
        return date.fromisoformat(s)
    except Exception:
        pass
    try:
        m, d, y = s.split("/")
        return date(int(y), int(m), int(d))
    except Exception:
        return None

def _lower(s): return str(s or "").strip().lower()

def _find_col(headers, wanted):
    w = _lower(wanted)
    for i, h in enumerate(headers):
        if _lower(h) == w:
            return i
    return None

SYNC_INTERVAL_SECONDS = 120

def _last_sync_time_utc():
    row = get_db().execute("SELECT last_sync FROM sync_state WHERE id=1").fetchone()
    if row and row["last_sync"]:
        try:
            return datetime.fromisoformat(row["last_sync"])
        except Exception:
            return None
    return None

def sync_from_sheets_if_needed(force=False):
    last = _last_sync_time_utc()
    if not force and last and (datetime.utcnow() - last).total_seconds() < SYNC_INTERVAL_SECONDS:
        return

    try:
        sh = get_gs_client().open("District4 Data")
    except Exception as e:
        print("❌ open sheet failed:", e)
        return

    db = get_db()
    cur = db.cursor()

    # Accounts
    try:
        ws = sh.worksheet("Accounts")
        values = ws.get_all_values()
    except Exception as e:
        print("❌ Accounts sync failed:", e)
        values = []

    cur.execute("DELETE FROM sheet_accounts_cache")
    if values and len(values) >= 2:
        headers = values[0]
        i_name = _find_col(headers, "Name")
        i_user = _find_col(headers, "UserName")
        i_pass = _find_col(headers, "Password")
        i_addr = _find_col(headers, "Church Address")
        i_age = _find_col(headers, "Area Number") or _find_col(headers, "Age")
        i_sex = _find_col(headers, "Church ID") or _find_col(headers, "Sex")
        i_contact = _find_col(headers, "Contact #")
        i_bday = _find_col(headers, "Birth Day")
        i_pos = _find_col(headers, "Position")

        def cell(row, idx):
            if idx is None: return ""
            return row[idx] if idx < len(row) else ""

        for r in range(1, len(values)):
            row = values[r]
            username = str(cell(row, i_user)).strip()
            if not username:
                continue
            cur.execute("""
                INSERT OR REPLACE INTO sheet_accounts_cache
                (username, name, church_address, password, age, sex, contact, birthday, position, sheet_row)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                username,
                str(cell(row, i_name)).strip(),
                str(cell(row, i_addr)).strip(),
                str(cell(row, i_pass)).strip(),
                str(cell(row, i_age)).strip(),
                str(cell(row, i_sex)).strip(),
                str(cell(row, i_contact)).strip(),
                str(cell(row, i_bday)).strip(),
                str(cell(row, i_pos)).strip(),
                r + 1
            ))

    # PrayerRequest
    try:
        ws = sh.worksheet("PrayerRequest")
        values = ws.get_all_values()
    except Exception as e:
        print("❌ PrayerRequest sync failed:", e)
        values = []

    cur.execute("DELETE FROM sheet_prayer_request_cache")
    if values and len(values) >= 2:
        headers = values[0]
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
            if idx is None: return ""
            return row[idx] if idx < len(row) else ""

        for r in range(1, len(values)):
            row = values[r]
            req_id = str(cell(row, i_request_id)).strip()
            if not req_id:
                continue
            cur.execute("""
                INSERT OR REPLACE INTO sheet_prayer_request_cache
                (request_id, church_name, submitted_by, title, request_date, request_text, status, pastors_praying, answered_date, sheet_row)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                req_id,
                str(cell(row, i_church)).strip(),
                str(cell(row, i_submitted_by)).strip(),
                str(cell(row, i_title)).strip(),
                str(cell(row, i_request_date)).strip(),
                str(cell(row, i_request_text)).strip(),
                str(cell(row, i_status)).strip(),
                str(cell(row, i_praying)).strip(),
                str(cell(row, i_answered)).strip(),
                r + 1
            ))

    db.execute("UPDATE sync_state SET last_sync=? WHERE id=1", (datetime.utcnow().isoformat(),))
    db.commit()
    print("✅ Sheets cache sync done.")

def pastor_logged_in(): return session.get("pastor_logged_in") is True
def ao_logged_in(): return session.get("ao_logged_in") is True
def any_user_logged_in(): return pastor_logged_in() or ao_logged_in()

def refresh_pastor_from_cache():
    u = (session.get("pastor_username") or "").strip()
    if not u: return False
    row = get_db().execute(
        "SELECT name, church_address, sex FROM sheet_accounts_cache WHERE username=?",
        (u,)
    ).fetchone()
    if not row: return False
    session["pastor_name"] = row["name"] or ""
    session["pastor_church_address"] = row["church_address"] or ""
    session["pastor_church_id"] = (row["sex"] or "").strip()
    return True

def get_or_create_monthly_report(year, month, pastor_username):
    db = get_db()
    row = db.execute(
        "SELECT * FROM monthly_reports WHERE year=? AND month=? AND pastor_username=?",
        (year, month, pastor_username)
    ).fetchone()
    if row: return row
    db.execute(
        "INSERT INTO monthly_reports (year, month, pastor_username, submitted, approved) VALUES (?, ?, ?, 0, 0)",
        (year, month, pastor_username)
    )
    db.commit()
    return db.execute(
        "SELECT * FROM monthly_reports WHERE year=? AND month=? AND pastor_username=?",
        (year, month, pastor_username)
    ).fetchone()

def generate_sundays_for_month(year, month):
    sundays = []
    cal = calendar.Calendar(firstweekday=calendar.SUNDAY)
    for week in cal.monthdatescalendar(year, month):
        for d in week:
            if d.month == month and d.weekday() == calendar.SUNDAY:
                sundays.append(d)
    return sundays

def ensure_sunday_reports(mrid, year, month):
    db = get_db()
    for d in generate_sundays_for_month(year, month):
        db.execute(
            "INSERT OR IGNORE INTO sunday_reports (monthly_report_id, date) VALUES (?, ?)",
            (mrid, d.isoformat())
        )
    db.commit()

def get_sunday_reports(mrid):
    return get_db().execute(
        "SELECT * FROM sunday_reports WHERE monthly_report_id=? ORDER BY date",
        (mrid,)
    ).fetchall()

def ensure_church_progress(mrid):
    db = get_db()
    row = db.execute("SELECT * FROM church_progress WHERE monthly_report_id=?", (mrid,)).fetchone()
    if row: return row
    db.execute("INSERT INTO church_progress (monthly_report_id, is_complete) VALUES (?, 0)", (mrid,))
    db.commit()
    return db.execute("SELECT * FROM church_progress WHERE monthly_report_id=?", (mrid,)).fetchone()

VERSE_REFERENCES = [
    "Psalm 23:1", "Proverbs 3:5-6", "Matthew 6:33", "John 3:16", "Romans 8:28",
    "Philippians 4:13", "Hebrews 11:1", "James 1:5", "1 Peter 5:7", "Revelation 21:4"
]

def get_verse_of_the_day():
    today_str = date.today().isoformat()
    db = get_db()
    row = db.execute("SELECT reference, text FROM verses WHERE date=?", (today_str,)).fetchone()
    if row:
        return row["reference"], row["text"]

    ref = VERSE_REFERENCES[date.today().toordinal() % len(VERSE_REFERENCES)]
    verse_text = ref
    try:
        resp = requests.get(f"https://bible-api.com/{urllib.parse.quote(ref)}", timeout=5)
        resp.raise_for_status()
        data = resp.json()
        if data.get("text", "").strip():
            verse_text = data["text"].strip()
    except Exception:
        pass

    db.execute("INSERT OR REPLACE INTO verses (date, reference, text) VALUES (?, ?, ?)",
               (today_str, ref, verse_text))
    db.commit()
    return ref, verse_text

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "change-this-secret-key-123")

@app.before_request
def _before():
    init_db()
    sync_from_sheets_if_needed()

@app.teardown_appcontext
def _close(_exc):
    db = getattr(g, "_db", None)
    if db is not None:
        db.close()

@app.route("/", methods=["GET", "POST"])
def splash():
    logged_in = any_user_logged_in()
    error = None
    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = (request.form.get("password") or "").strip()
        if not username or not password:
            error = "Username and password are required."
        else:
            row = get_db().execute(
                "SELECT username, password, name, church_address, sex FROM sheet_accounts_cache WHERE username=?",
                (username,)
            ).fetchone()
            if row and (str(row["password"] or "").strip() == password):
                session.clear()
                session["pastor_logged_in"] = True
                session["pastor_username"] = username
                session["pastor_name"] = row["name"] or ""
                session["pastor_church_address"] = row["church_address"] or ""
                session["pastor_church_id"] = (row["sex"] or "").strip()
                return redirect(url_for("pastor_tool"))
            error = "Invalid username or password."
    return render_template("splash.html", logged_in=logged_in, error=error)

@app.route("/event-registration")
def event_registration():
    return render_template("event_registration.html")

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("splash"))

@app.route("/bulletin")
def bulletin():
    ref, text = get_verse_of_the_day()
    today_str = date.today().strftime("%B %d, %Y")
    return render_template("bulletin.html", verse_reference=ref, verse_text=text, today_str=today_str)

@app.route("/pastor-login", methods=["GET", "POST"])
def pastor_login():
    error = None
    next_url = request.args.get("next") or url_for("pastor_tool")
    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = (request.form.get("password") or "").strip()
        row = get_db().execute(
            "SELECT username, password, name, church_address, sex FROM sheet_accounts_cache WHERE username=?",
            (username,)
        ).fetchone()
        if row and (str(row["password"] or "").strip() == password):
            session.clear()
            session["pastor_logged_in"] = True
            session["pastor_username"] = username
            session["pastor_name"] = row["name"] or ""
            session["pastor_church_address"] = row["church_address"] or ""
            session["pastor_church_id"] = (row["sex"] or "").strip()
            return redirect(request.form.get("next") or next_url)
        error = "Invalid username or password."
    return render_template("pastor_login.html", error=error, next_url=next_url)

@app.route("/ao-login", methods=["GET", "POST"])
def ao_login():
    error = None
    next_url = request.args.get("next") or url_for("ao_tool")
    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = (request.form.get("password") or "").strip()
        sync_from_sheets_if_needed(force=True)
        row = get_db().execute(
            "SELECT username, password, name, age, sex, position FROM sheet_accounts_cache WHERE username=?",
            (username,)
        ).fetchone()
        if row and (str(row["password"] or "").strip() == password):
            pos = str(row["position"] or "").strip().lower()
            if pos == "area overseer":
                session.clear()
                session["ao_logged_in"] = True
                session["ao_username"] = username
                session["ao_name"] = row["name"] or ""
                session["ao_area_number"] = (row["age"] or "").strip()
                session["ao_church_id"] = (row["sex"] or "").strip()
                return redirect(request.form.get("next") or next_url)
        error = "Invalid username or password."
    return render_template("ao_login.html", error=error, next_url=next_url)

@app.route("/ao-tool")
def ao_tool():
    if not ao_logged_in():
        return redirect(url_for("ao_login", next=request.path))
    return render_template("ao_tool.html")

@app.route("/pastor-tool")
def pastor_tool():
    if not any_user_logged_in():
        return redirect(url_for("pastor_login", next=request.path))
    if ao_logged_in() and not pastor_logged_in():
        session["pastor_logged_in"] = True  # allow AO to view

    refresh_pastor_from_cache()
    pastor_username = (session.get("pastor_username") or "").strip()
    if not pastor_username:
        abort(403)

    today = date.today()
    year = request.args.get("year", type=int) or today.year
    month = request.args.get("month", type=int) or today.month

    mr = get_or_create_monthly_report(year, month, pastor_username)
    ensure_sunday_reports(mr["id"], year, month)
    sundays = get_sunday_reports(mr["id"])
    cp = ensure_church_progress(mr["id"])

    sunday_list = []
    for r in sundays:
        d = date.fromisoformat(r["date"])
        sunday_list.append({
            "id": r["id"],
            "date": r["date"],
            "display": d.strftime("%B %d"),
            "year": d.year, "month": d.month, "day": d.day,
            "is_complete": bool(r["is_complete"])
        })

    year_options = list(range(today.year - 10, today.year + 4))
    month_names = [(calendar.month_name[i], i) for i in range(1, 13)]


    return render_template(
        "pastor_tool.html",
        year=year, month=month,
        year_options=year_options, month_names=month_names,
        monthly_report=mr,
        sunday_list=sunday_list,
        cp_complete=bool(cp["is_complete"]),
        pastor_name=session.get("pastor_name",""),
        ao_mode=ao_logged_in(),
    )

@app.route("/pastor-tool/<int:year>/<int:month>/<int:day>", methods=["GET", "POST"], endpoint="sunday_detail")
def sunday_detail(year, month, day):
    if not any_user_logged_in():
        return redirect(url_for("pastor_login", next=request.path))
    refresh_pastor_from_cache()
    pastor_username = (session.get("pastor_username") or "").strip()
    if not pastor_username:
        abort(403)

    try:
        d = date(year, month, day)
    except ValueError:
        abort(404)

    mr = get_or_create_monthly_report(year, month, pastor_username)
    ensure_sunday_reports(mr["id"], year, month)
    db = get_db()
    sunday = db.execute(
        "SELECT * FROM sunday_reports WHERE monthly_report_id=? AND date=?",
        (mr["id"], d.isoformat())
    ).fetchone()
    if not sunday:
        abort(404)

    error = None
    values = {}

    if request.method == "POST":
        fields = ["attendance_adult","attendance_youth","attendance_children","tithes_church","offering","mission","tithes_personal"]
        numeric = {}
        for f in fields:
            raw = (request.form.get(f) or "").strip()
            values[f] = raw
            if raw == "":
                error = "All fields are required."
                break
            try:
                numeric[f] = float(raw)
            except ValueError:
                error = "Please enter numbers only in all fields."
                break

        if not error:
            db.execute("DELETE FROM sunday_reports WHERE monthly_report_id=? AND date=?",
                       (mr["id"], d.isoformat()))
            db.execute("""
                INSERT INTO sunday_reports
                (monthly_report_id, date, is_complete, attendance_adult, attendance_youth, attendance_children,
                 tithes_church, offering, mission, tithes_personal)
                VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?, ?)
            """, (
                mr["id"], d.isoformat(),
                numeric["attendance_adult"], numeric["attendance_youth"], numeric["attendance_children"],
                numeric["tithes_church"], numeric["offering"], numeric["mission"], numeric["tithes_personal"]
            ))
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

    return render_template(
        "sunday_detail.html",
        year=year, month=month, day=day,
        date_str=d.strftime("%B %d, %Y"),
        values=values,
        error=error,
    )

@app.route("/pastor-tool/<int:year>/<int:month>/progress", methods=["GET","POST"])
def church_progress_view(year, month):
    if not any_user_logged_in():
        return redirect(url_for("pastor_login", next=request.path))
    refresh_pastor_from_cache()
    pastor_username = (session.get("pastor_username") or "").strip()
    if not pastor_username:
        abort(403)

    mr = get_or_create_monthly_report(year, month, pastor_username)
    cp = ensure_church_progress(mr["id"])
    db = get_db()
    error = None
    values = {}

    if request.method == "POST":
        fields = ["bible_new","bible_existing","received_christ","baptized_water","baptized_holy_spirit","healed","child_dedication"]
        numeric = {}
        for f in fields:
            raw = (request.form.get(f) or "").strip()
            values[f] = raw
            if raw == "":
                error = "All fields are required for Church Progress."
                break
            try:
                numeric[f] = int(raw)
            except ValueError:
                error = "Please enter whole numbers only."
                break
        if not error:
            db.execute("""
                UPDATE church_progress
                SET bible_new=?, bible_existing=?, received_christ=?, baptized_water=?, baptized_holy_spirit=?,
                    healed=?, child_dedication=?, is_complete=1
                WHERE id=?
            """, (
                numeric["bible_new"], numeric["bible_existing"], numeric["received_christ"],
                numeric["baptized_water"], numeric["baptized_holy_spirit"], numeric["healed"],
                numeric["child_dedication"], cp["id"]
            ))
            db.commit()
            return redirect(url_for("pastor_tool", year=year, month=month))

    if not values:
        values = {k: (cp[k] or "") for k in ["bible_new","bible_existing","received_christ","baptized_water","baptized_holy_spirit","healed","child_dedication"]}

    return render_template(
        "church_progress.html",
        year=year, month=month,
        date_label=date(year,month,1).strftime("%B %Y"),
        values=values,
        error=error,
    )

# Prayer request basics (write + status)
@app.route("/prayer-request")
def prayer_request():
    if not any_user_logged_in():
        return redirect(url_for("pastor_login", next=request.path))
    return render_template("prayer_request.html")

def _current_user_key():
    if pastor_logged_in():
        return (session.get("pastor_username") or "").strip() or "pastor"
    if ao_logged_in():
        return (session.get("ao_username") or "").strip() or "ao"
    return "unknown"

def _current_user_church_name():
    if pastor_logged_in():
        refresh_pastor_from_cache()
        return (session.get("pastor_church_address") or "").strip()
    return ""

def _append_prayer_request_to_sheet(church_name, submitted_by, request_id, title, request_date, request_text):
    sh = get_gs_client().open("District4 Data")
    ws = sh.worksheet("PrayerRequest")
    ws.append_row([church_name, submitted_by, request_id, title, request_date, request_text, "Pending", "", ""])

@app.route("/prayer-request/write", methods=["GET","POST"])
def prayer_request_write():
    if not any_user_logged_in():
        return redirect(url_for("pastor_login", next=request.path))
    if request.method == "POST":
        title = (request.form.get("title") or "").strip()
        body = (request.form.get("prayer_request") or request.form.get("request_text") or "").strip()
        if not title or not body:
            return render_template("prayer_write.html", error="Title and Prayer Request are required.",
                                   values={"title": title, "prayer_request": body})
        req_id = str(uuid.uuid4())
        _append_prayer_request_to_sheet(_current_user_church_name(), _current_user_key(), req_id, title, date.today().isoformat(), body)
        sync_from_sheets_if_needed(force=True)
        return redirect(url_for("prayer_request_status"))
    return render_template("prayer_write.html", error=None, values={"title":"","prayer_request":""})

@app.route("/prayer-request/status")
def prayer_request_status():
    if not any_user_logged_in():
        return redirect(url_for("pastor_login", next=request.path))
    sync_from_sheets_if_needed(force=True)
    submitted_by = _current_user_key()
    rows = get_db().execute(
        "SELECT * FROM sheet_prayer_request_cache WHERE TRIM(submitted_by)=TRIM(?) ORDER BY request_date DESC, sheet_row DESC",
        (submitted_by,)
    ).fetchall()
    items = [{
        "request_id": r["request_id"],
        "church_name": r["church_name"] or "",
        "submitted_by": r["submitted_by"] or "",
        "title": r["title"] or "",
        "request_date": r["request_date"] or "",
        "request_text": r["request_text"] or "",
        "status": r["status"] or "Pending",
        "pastors_praying": r["pastors_praying"] or "",
        "answered_date": r["answered_date"] or "",
    } for r in rows]
    return render_template("prayer_status.html", items=items, user_display=submitted_by)

if __name__ == "__main__":
    app.run(debug=True)
