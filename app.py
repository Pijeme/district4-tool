import sqlite3
from datetime import datetime, date
import calendar
import urllib.parse

import requests
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

    # Pastor accounts
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS pastors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            full_name TEXT NOT NULL,
            age INTEGER,
            sex TEXT,
            church_address TEXT,
            contact_number TEXT,
            username TEXT UNIQUE,
            password TEXT
        )
        """
    )

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
        # Keep letters only for username base
        base = "".join(ch for ch in first if ch.isalpha()).lower() or "pastor"
        first_name_clean = first.title()

    # Ensure username unique
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

    # Simple, easy-to-remember password
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


@app.route("/pastor-tool", methods=["GET", "POST"])
def pastor_tool():
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

    # Compute monthly total: sum of all money for all Sundays in this month
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

    # Determine if submit/resubmit is enabled:
    # all Sundays complete AND church progress complete
    sundays_ok = all_sundays_complete(monthly_report["id"])
    can_submit = sundays_ok and cp_complete
    status_key = get_month_status(monthly_report)

    # For dropdown checkmarks: which months are submitted in this year
    cursor.execute(
        "SELECT year, month, submitted FROM monthly_reports WHERE year = ?",
        (year,),
    )
    submitted_map = {(row["year"], row["month"]): bool(row["submitted"]) for row in cursor.fetchall()}

    if request.method == "POST":
        if can_submit:
            set_month_submitted(year, month)
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
    )


@app.route("/pastor-tool/<int:year>/<int:month>/<int:day>", methods=["GET", "POST"])
def sunday_detail(year, month, day):
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

    error = None    # to show form error
    values = {}     # to keep what user typed

    if request.method == "POST":
        # Required fields (attendance_total removed)
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

    # For GET or if error, use existing DB values as defaults
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


@app.route("/ao-login", methods=["GET", "POST"])
def ao_login():
    error = None
    next_url = request.args.get("next") or url_for("ao_tool")

    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = (request.form.get("password") or "").strip()

        # Case-sensitive check
        if username == "Pijeme" and password == "Area 7":
            session["ao_logged_in"] = True
            session["ao_username"] = username
            session.permanent = True  # keep session longer on this device
            # respect hidden next parameter if present
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

    db = get_db()
    cursor = db.cursor()

    # Get all monthly reports for selected year
    cursor.execute(
        "SELECT * FROM monthly_reports WHERE year = ? ORDER BY month",
        (year,),
    )
    rows = cursor.fetchall()
    row_map = {row["month"]: row for row in rows}

    # Year dropdown options
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
    for name, value in month_names:
        row = row_map.get(value)
        if row:
            status_key = get_month_status(row)
            months.append(
                {
                    "name": name,
                    "month": value,
                    "year": row["year"],
                    "has_report": True,
                    "status_key": status_key,
                }
            )
        else:
            months.append(
                {
                    "name": name,
                    "month": value,
                    "year": year,
                    "has_report": False,
                    "status_key": "no_data",
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

    # Load monthly report (do NOT auto-create here)
    cursor.execute(
        "SELECT * FROM monthly_reports WHERE year = ? AND month = ?",
        (year, month),
    )
    monthly_report = cursor.fetchone()
    if not monthly_report:
        abort(404)

    if request.method == "POST":
        action = (request.form.get("action") or "").strip().lower()
        if action == "approve":
            set_month_approved(year, month)
        elif action == "pending":
            set_month_pending(year, month)
        return redirect(url_for("ao_month_detail", year=year, month=month))

    # Reload after possible change
    cursor.execute(
        "SELECT * FROM monthly_reports WHERE year = ? AND month = ?",
        (year, month),
    )
    monthly_report = cursor.fetchone()
    status_key = get_month_status(monthly_report)

    # Sundays (ensure they exist)
    ensure_sunday_reports(monthly_report["id"], year, month)
    cursor.execute(
        """
        SELECT * FROM sunday_reports
        WHERE monthly_report_id = ?
        ORDER BY date
        """,
        (monthly_report["id"],),
    )
    sunday_rows = cursor.fetchall()

    sundays = []
    for row in sunday_rows:
        d = datetime.fromisoformat(row["date"]).date()
        tithes_church = row["tithes_church"] or 0
        offering = row["offering"] or 0
        mission = row["mission"] or 0
        tithes_personal = row["tithes_personal"] or 0
        total_amount = tithes_church + offering + mission + tithes_personal

        sundays.append(
            {
                "date_str": d.strftime("%B %d, %Y"),
                "attendance_adult": row["attendance_adult"],
                "attendance_youth": row["attendance_youth"],
                "attendance_children": row["attendance_children"],
                "tithes_church": row["tithes_church"],
                "offering": row["offering"],
                "mission": row["mission"],
                "tithes_personal": row["tithes_personal"],
                "total_amount": total_amount,
                "is_complete": bool(row["is_complete"]),
            }
        )

    # Church progress (ensure exists)
    cp_row = ensure_church_progress(monthly_report["id"])
    church_progress = {
        "bible_new": cp_row["bible_new"],
        "bible_existing": cp_row["bible_existing"],
        "received_christ": cp_row["received_christ"],
        "baptized_water": cp_row["baptized_water"],
        "baptized_holy_spirit": cp_row["baptized_holy_spirit"],
        "healed": cp_row["healed"],
        "child_dedication": cp_row["child_dedication"],
    }

    month_label = date(year, month, 1).strftime("%B %Y")
    submitted_at = monthly_report["submitted_at"]
    approved_at = monthly_report["approved_at"]

    can_approve = status_key != "not_submitted"

    return render_template(
        "ao_month_detail.html",
        year=year,
        month=month,
        month_label=month_label,
        status_key=status_key,
        submitted_at=submitted_at,
        approved_at=approved_at,
        sundays=sundays,
        church_progress=church_progress,
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
    }

    if request.method == "POST":
        full_name = (request.form.get("full_name") or "").strip()
        age_raw = (request.form.get("age") or "").strip()
        sex = (request.form.get("sex") or "").strip()
        church_address = (request.form.get("church_address") or "").strip()
        contact_number = (request.form.get("contact_number") or "").strip()

        values.update(
            {
                "full_name": full_name,
                "age": age_raw,
                "sex": sex,
                "church_address": church_address,
                "contact_number": contact_number,
            }
        )

        if not full_name or not age_raw or not sex or not church_address or not contact_number:
            error = "All fields are required."
        else:
            try:
                age_int = int(age_raw)
            except ValueError:
                error = "Age must be a number."
            else:
                db = get_db()
                cursor = db.cursor()

                username, password = generate_pastor_credentials(full_name, age_int)

                try:
                    cursor.execute(
                        """
                        INSERT INTO pastors
                        (full_name, age, sex, church_address, contact_number, username, password)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            full_name,
                            age_int,
                            sex,
                            church_address,
                            contact_number,
                            username,
                            password,
                        ),
                    )
                    db.commit()
                    success = "Account created successfully."
                    generated_username = username
                    generated_password = password
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


@app.route("/test-sheets")
def test_sheets():
    try:
        append_test_row()
        return "✅ Google Sheets test row added. Check your sheet!"
    except Exception as e:
        return f"❌ Error connecting to Google Sheets: {e}", 500


if __name__ == "__main__":
    app.run(debug=True)
