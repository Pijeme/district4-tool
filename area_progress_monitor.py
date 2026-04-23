from __future__ import annotations

import calendar
import os
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

import gspread
from google.oauth2.service_account import Credentials
from flask import Blueprint, abort, current_app, jsonify, redirect, render_template_string, request, session, url_for

bp = Blueprint("area_progress_monitor", __name__)

PH_TZ = timezone(timedelta(hours=8))
LOGO_SRC = "/static/img/logo.png"
GOOGLE_SHEETS_CREDENTIALS_FILE = "service_account.json"
GOOGLE_SHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
SYNC_INTERVAL_SECONDS = 120

PALETTE = [
    "#2563eb", "#16a34a", "#dc2626", "#f59e0b", "#7c3aed",
    "#0891b2", "#ea580c", "#65a30d", "#be185d", "#0f766e",
    "#4f46e5", "#84cc16", "#ef4444", "#14b8a6", "#9333ea",
    "#0284c7", "#22c55e", "#f97316", "#e11d48", "#334155",
]

FINANCE_FIELDS = {
    "tithes": "Tithes",
    "offering": "Offering",
    "personal_tithes": "Personal Tithes",
    "mission_offering": "Mission Offering",
    "amount_to_send": "Total Money Sent",
}

MINISTRY_FIELDS = {
    "received_jesus": "Received Jesus",
    "existing_bible_study": "Existing Bible Study",
    "new_bible_study": "New Bible Study",
    "water_baptized": "Water Baptized",
    "holy_spirit_baptized": "Holy Spirit Baptized",
    "childrens_dedication": "Children's Dedication",
    "healed": "Healed",
}

ATTENDANCE_FIELDS = {
    "adult": "Adult",
    "youth": "Young People",
    "children": "Children",
    "all": "All",
}

REMARK_CATEGORIES = ["general", "finances", "attendance", "ministry", "reporting"]


@dataclass
class Scope:
    area: str
    sub_area: str
    role: str

    @property
    def is_sub_area(self) -> bool:
        return self.role == "sub area overseer"

    @property
    def scope_key(self) -> str:
        return f"area:{self.area}|sub:{self.sub_area or '-'}"


def _database_path() -> str:
    cfg = current_app.config.get("DATABASE")
    if cfg:
        return cfg
    return os.path.join(current_app.root_path, "app_v2.db")


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(_database_path(), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def _ao_logged_in() -> bool:
    return session.get("ao_logged_in") is True


def _current_scope() -> Scope:
    return Scope(
        area=str(session.get("ao_area_number") or "").strip(),
        sub_area=str(session.get("ao_sub_area") or "").strip(),
        role=str(session.get("ao_role") or "").strip().lower(),
    )


def _require_ao() -> Scope:
    if not _ao_logged_in():
        abort(401)
    scope = _current_scope()
    if not scope.area:
        abort(401)
    return scope


def _now_ph() -> datetime:
    return datetime.now(PH_TZ)


def _safe_float(value: Any) -> float:
    try:
        if value is None or str(value).strip() == "":
            return 0.0
        return float(str(value).replace(",", ""))
    except Exception:
        return 0.0


def _safe_int(value: Any) -> int:
    try:
        if value is None or str(value).strip() == "":
            return 0
        return int(float(value))
    except Exception:
        return 0


def _normalize_church_key(value: Any) -> str:
    s = str(value or "").strip().lower()
    for token in ["church", "ctr", "center"]:
        s = s.replace(token, " ")
    cleaned = []
    for ch in s:
        cleaned.append(ch if ch.isalnum() else " ")
    return " ".join("".join(cleaned).split())


def _church_color(church_name: str) -> str:
    import hashlib
    key = _normalize_church_key(church_name)
    if not key:
        return PALETTE[0]
    digest = hashlib.md5(key.encode("utf-8")).hexdigest()
    idx = int(digest[:8], 16) % len(PALETTE)
    return PALETTE[idx]


def _peso(value: float) -> str:
    return f"₱{float(value or 0):,.2f}"


def _month_label(year: int, month: int) -> str:
    return f"{calendar.month_abbr[month]} {year}"


def _month_floor(dt: date) -> date:
    return dt.replace(day=1)


def _subtract_months(dt: date, months: int) -> date:
    y = dt.year
    m = dt.month - months
    while m <= 0:
        y -= 1
        m += 12
    return date(y, m, 1)


def _latest_completed_month(today: date | None = None) -> date:
    today = today or _now_ph().date()
    return _subtract_months(_month_floor(today), 1)


def _month_window(period: str, today: date | None = None) -> list[tuple[int, int]]:
    latest_completed = _latest_completed_month(today)
    count = {"this_month": 1, "3_months": 3, "6_months": 6, "1_year": 12}.get(period, 3)
    start = _subtract_months(latest_completed, count - 1)
    months = []
    cursor = start
    while cursor <= latest_completed:
        months.append((cursor.year, cursor.month))
        if cursor.month == 12:
            cursor = date(cursor.year + 1, 1, 1)
        else:
            cursor = date(cursor.year, cursor.month + 1, 1)
    return months


def _last_sunday(year: int, month: int) -> date:
    last_day = calendar.monthrange(year, month)[1]
    d = date(year, month, last_day)
    while d.weekday() != 6:
        d -= timedelta(days=1)
    return d


def _report_deadline(year: int, month: int) -> date:
    return _last_sunday(year, month) + timedelta(days=7)


def _number_of_sundays(year: int, month: int) -> int:
    total_days = calendar.monthrange(year, month)[1]
    sundays = sum(1 for day_num in range(1, total_days + 1) if date(year, month, day_num).weekday() == 6)
    return max(sundays, 1)


def _average(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _pct_change(current: float, previous: float) -> float:
    if previous == 0:
        return 100.0 if current > 0 else 0.0
    return ((current - previous) / previous) * 100.0


def _streak_declining(values: list[float]) -> int:
    streak = 0
    for i in range(1, len(values)):
        if values[i] < values[i - 1]:
            streak += 1
        else:
            streak = 0
    return streak


def ensure_area_progress_monitor_tables() -> None:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS area_monitor_chart_ceiling (
            scope_key TEXT NOT NULL,
            metric_key TEXT NOT NULL,
            ceiling_value REAL NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (scope_key, metric_key)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS area_monitor_remarks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scope_key TEXT NOT NULL,
            church_key TEXT NOT NULL,
            category TEXT NOT NULL,
            remark_text TEXT NOT NULL DEFAULT '',
            created_by TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(scope_key, church_key, category)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sync_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            last_sync TEXT
        )
        """
    )
    cur.execute("INSERT OR IGNORE INTO sync_state (id, last_sync) VALUES (1, NULL)")
    cur.execute(
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
            sheet_row INTEGER
        )
        """
    )
    cur.execute(
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
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sheet_aopt_cache (
            month TEXT NOT NULL,
            area_number TEXT NOT NULL DEFAULT '',
            sub_area TEXT NOT NULL DEFAULT '',
            amount REAL,
            sheet_row INTEGER,
            PRIMARY KEY (month, area_number, sub_area)
        )
        """
    )
    cur.execute(
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
    cur.execute("CREATE INDEX IF NOT EXISTS idx_area_monitor_remarks_scope_church ON area_monitor_remarks(scope_key, church_key)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_report_ym_addr ON sheet_report_cache(year, month, address)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_report_ym_church ON sheet_report_cache(year, month, church)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_report_ym ON sheet_report_cache(year, month)")
    conn.commit()
    conn.close()


def _lower(s):
    return str(s or "").strip().lower()


def _find_col(headers, wanted):
    wanted = _lower(wanted)
    for i, h in enumerate(headers):
        if _lower(h) == wanted:
            return i
    return None


def parse_sheet_date(value):
    s = str(value or "").strip()
    if not s:
        return None
    try:
        return date.fromisoformat(s)
    except Exception:
        pass
    try:
        parts = s.split("/")
        if len(parts) == 3:
            return date(int(parts[2]), int(parts[0]), int(parts[1]))
    except Exception:
        return None
    return None


def _last_sync_time_utc() -> datetime | None:
    conn = _connect()
    try:
        row = conn.execute("SELECT last_sync FROM sync_state WHERE id = 1").fetchone()
    finally:
        conn.close()
    if row and row["last_sync"]:
        try:
            dt = datetime.fromisoformat(row["last_sync"])
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            return None
    return None


def _update_sync_time() -> None:
    conn = _connect()
    conn.execute("UPDATE sync_state SET last_sync = ? WHERE id = 1", (datetime.now(timezone.utc).isoformat(),))
    conn.commit()
    conn.close()


def get_last_sync_display_ph() -> str:
    dt_utc = _last_sync_time_utc()
    if not dt_utc:
        return "Never"
    try:
        dt_ph = dt_utc.astimezone(PH_TZ)
        return dt_ph.strftime("%b %d, %Y %I:%M %p")
    except Exception:
        return "Unknown"


def get_gs_client():
    creds = Credentials.from_service_account_file(
        GOOGLE_SHEETS_CREDENTIALS_FILE,
        scopes=GOOGLE_SHEETS_SCOPES,
    )
    return gspread.authorize(creds)


def sync_from_sheets_if_needed(force: bool = False):
    last = _last_sync_time_utc()
    if not force and last and (datetime.now(timezone.utc) - last).total_seconds() < SYNC_INTERVAL_SECONDS:
        return

    client = get_gs_client()
    sh = client.open("District4 Data")
    conn = _connect()
    cur = conn.cursor()

    # Accounts
    try:
        ws_accounts = sh.worksheet("Accounts")
        values = ws_accounts.get_all_values()
    except Exception:
        values = []
    cur.execute("DELETE FROM sheet_accounts_cache")
    if values and len(values) >= 2:
        headers = values[0]
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

        for r in range(1, len(values)):
            row = values[r]
            def cell(idx):
                return row[idx] if idx is not None and idx < len(row) else ""
            username = str(cell(i_user)).strip()
            full_name = str(cell(i_name)).strip()
            church_address = str(cell(i_addr)).strip()
            area_number = str(cell(i_age)).strip()
            church_id = str(cell(i_sex)).strip()
            if not area_number and not church_id and not full_name and not church_address:
                continue
            cur.execute(
                """
                INSERT OR REPLACE INTO sheet_accounts_cache
                (username, name, church_address, password, age, sex, contact, birthday, position, sub_area, sheet_row)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    username,
                    full_name,
                    church_address,
                    str(cell(i_pass)).strip(),
                    area_number,
                    church_id,
                    str(cell(i_contact)).strip(),
                    str(cell(i_bday)).strip(),
                    str(cell(i_pos)).strip(),
                    str(cell(i_sub)).strip(),
                    r + 1,
                ),
            )

    # Report
    try:
        ws_report = sh.worksheet("Report")
        values = ws_report.get_all_values()
    except Exception:
        values = []
    cur.execute("DELETE FROM sheet_report_cache")
    if values and len(values) >= 2:
        headers = values[0]
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
        for r in range(1, len(values)):
            row = values[r]
            def cell(idx):
                return row[idx] if idx is not None and idx < len(row) else ""
            d = parse_sheet_date(cell(i_activity))
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
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    r + 1, d.year, d.month, d.isoformat(),
                    str(cell(i_church)).strip(),
                    str(cell(i_pastor)).strip(),
                    str(cell(i_address)).strip(),
                    _safe_float(cell(i_adult)),
                    _safe_float(cell(i_youth)),
                    _safe_float(cell(i_children)),
                    _safe_float(cell(i_tithes)),
                    _safe_float(cell(i_offering)),
                    _safe_float(cell(i_personal)),
                    _safe_float(cell(i_mission)),
                    _safe_float(cell(i_recv)),
                    _safe_float(cell(i_exist)),
                    _safe_float(cell(i_new)),
                    _safe_float(cell(i_water)),
                    _safe_float(cell(i_holy)),
                    _safe_float(cell(i_ded)),
                    _safe_float(cell(i_healed)),
                    _safe_float(cell(i_send)),
                    str(cell(i_status)).strip(),
                ),
            )

    # AOPT
    try:
        ws_aopt = sh.worksheet("AOPT")
        values = ws_aopt.get_all_values()
    except Exception:
        values = []
    cur.execute("DELETE FROM sheet_aopt_cache")
    if values and len(values) >= 2:
        headers = values[0]
        i_month = _find_col(headers, "Month")
        i_amount = _find_col(headers, "Amount")
        i_area = _find_col(headers, "Area Number")
        if i_area is None:
            i_area = _find_col(headers, "Area")
        i_sub = _find_col(headers, "Sub Area")
        if i_sub is None:
            i_sub = _find_col(headers, "SubArea")
        for r in range(1, len(values)):
            row = values[r]
            def cell(idx):
                return row[idx] if idx is not None and idx < len(row) else ""
            month_label = str(cell(i_month)).strip()
            if not month_label:
                continue
            cur.execute(
                "INSERT OR REPLACE INTO sheet_aopt_cache (month, area_number, sub_area, amount, sheet_row) VALUES (?, ?, ?, ?, ?)",
                (month_label, str(cell(i_area)).strip(), str(cell(i_sub)).strip(), _safe_float(cell(i_amount)), r + 1),
            )

    conn.commit()
    conn.close()
    _update_sync_time()


def get_aopt_amount_from_cache(month_label: str, area_number: str = "", sub_area: str = ""):
    area_number = str(area_number or "").strip()
    sub_area = str(sub_area or "").strip()
    conn = _connect()
    try:
        if sub_area:
            row = conn.execute(
                """
                SELECT amount FROM sheet_aopt_cache
                WHERE month = ? AND TRIM(area_number)=TRIM(?) AND TRIM(COALESCE(sub_area,''))=TRIM(?)
                ORDER BY sheet_row DESC LIMIT 1
                """,
                (month_label, area_number, sub_area),
            ).fetchone()
            if row:
                return row["amount"]
        row = conn.execute(
            """
            SELECT amount FROM sheet_aopt_cache
            WHERE month = ? AND TRIM(area_number)=TRIM(?) AND TRIM(COALESCE(sub_area,''))=''
            ORDER BY sheet_row DESC LIMIT 1
            """,
            (month_label, area_number),
        ).fetchone()
        return row["amount"] if row else None
    finally:
        conn.close()


def _fetch_scope_churches(scope: Scope) -> dict[str, dict[str, Any]]:
    conn = _connect()
    params: list[Any] = [scope.area]
    extra = ""
    if scope.is_sub_area:
        extra = " AND TRIM(COALESCE(sub_area,'')) = TRIM(?)"
        params.append(scope.sub_area)

    rows = conn.execute(
        f"""
        SELECT TRIM(username) AS username,
               TRIM(name) AS pastor_name,
               TRIM(sex) AS church_name,
               TRIM(church_address) AS church_address,
               TRIM(age) AS area_number,
               TRIM(COALESCE(sub_area,'')) AS sub_area
        FROM sheet_accounts_cache
        WHERE TRIM(age)=TRIM(?)
          AND LOWER(TRIM(COALESCE(position,'')))='pastor'
          AND TRIM(COALESCE(sex,'')) <> ''
          {extra}
        ORDER BY church_name
        """,
        tuple(params),
    ).fetchall()
    conn.close()

    churches: dict[str, dict[str, Any]] = {}
    for row in rows:
        church_name = str(row["church_name"] or "").strip()
        church_key = _normalize_church_key(church_name)
        if not church_key:
            continue
        churches[church_key] = {
            "church_key": church_key,
            "church_name": church_name,
            "church_address": str(row["church_address"] or "").strip(),
            "pastor_name": str(row["pastor_name"] or "").strip(),
            "username": str(row["username"] or "").strip(),
            "area_number": str(row["area_number"] or "").strip(),
            "sub_area": str(row["sub_area"] or "").strip(),
            "color": _church_color(church_name),
        }
    return churches


def _resolve_church_key(churches: dict[str, dict[str, Any]], raw_name: str, raw_address: str = "") -> str | None:
    for candidate in [raw_name, raw_address]:
        norm = _normalize_church_key(candidate)
        if norm in churches:
            return norm
    raw_norm = _normalize_church_key(raw_name)
    for key in churches:
        if raw_norm and (raw_norm == key or raw_norm in key or key in raw_norm):
            return key
    addr_norm = _normalize_church_key(raw_address)
    for key in churches:
        if addr_norm and (addr_norm == key or addr_norm in key or key in addr_norm):
            return key
    return None


def _fetch_report_rows(scope: Scope, months: list[tuple[int, int]], churches: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    if not months:
        return []
    conn = _connect()
    clause = " OR ".join(["(CAST(year AS INTEGER)=? AND CAST(month AS INTEGER)=?)" for _ in months])
    params: list[Any] = []
    for year, month in months:
        params.extend([year, month])
    rows = conn.execute(
        f"SELECT * FROM sheet_report_cache WHERE ({clause}) ORDER BY CAST(year AS INTEGER), CAST(month AS INTEGER), activity_date",
        tuple(params),
    ).fetchall()
    conn.close()
    results = []
    for row in rows:
        church_key = _resolve_church_key(churches, str(row["church"] or "").strip(), str(row["address"] or "").strip())
        if not church_key:
            continue
        results.append({
            "church_key": church_key,
            "year": _safe_int(row["year"]),
            "month": _safe_int(row["month"]),
            "adult": _safe_float(row["adult"]),
            "youth": _safe_float(row["youth"]),
            "children": _safe_float(row["children"]),
            "tithes": _safe_float(row["tithes"]),
            "offering": _safe_float(row["offering"]),
            "personal_tithes": _safe_float(row["personal_tithes"]),
            "mission_offering": _safe_float(row["mission_offering"]),
            "received_jesus": _safe_float(row["received_jesus"]),
            "existing_bible_study": _safe_float(row["existing_bible_study"]),
            "new_bible_study": _safe_float(row["new_bible_study"]),
            "water_baptized": _safe_float(row["water_baptized"]),
            "holy_spirit_baptized": _safe_float(row["holy_spirit_baptized"]),
            "childrens_dedication": _safe_float(row["childrens_dedication"]),
            "healed": _safe_float(row["healed"]),
            "amount_to_send": _safe_float(row["amount_to_send"]),
            "status": str(row["status"] or "").strip(),
        })
    return results


def _rollup_monthly(report_rows: list[dict[str, Any]], churches: dict[str, dict[str, Any]]) -> dict[str, dict[tuple[int, int], dict[str, float]]]:
    monthly: dict[str, dict[tuple[int, int], dict[str, float]]] = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    for church_key in churches.keys():
        monthly[church_key] = defaultdict(lambda: defaultdict(float))
    for row in report_rows:
        ym = (row["year"], row["month"])
        bucket = monthly[row["church_key"]][ym]
        bucket["adult"] += row["adult"]
        bucket["youth"] += row["youth"]
        bucket["children"] += row["children"]
        bucket["attendance_total"] += row["adult"] + row["youth"] + row["children"]
        for field in list(FINANCE_FIELDS.keys()) + list(MINISTRY_FIELDS.keys()):
            bucket[field] += row[field]
        bucket["rows"] += 1.0
        sheet_status = str(row.get("status") or "").strip().lower()
        if sheet_status:
            bucket["approved_rows"] += 1.0 if "approved" in sheet_status else 0.0
            bucket["status_rows"] += 1.0
    return monthly


def _attendance_average_series(monthly, months, attendance_category):
    out = {}
    attendance_category = attendance_category if attendance_category in ATTENDANCE_FIELDS else "all"
    for church_key, month_map in monthly.items():
        series = []
        for year, month in months:
            bucket = month_map.get((year, month), {})
            sundays = _number_of_sundays(year, month)
            if attendance_category == "adult":
                total = bucket.get("adult", 0.0)
            elif attendance_category == "youth":
                total = bucket.get("youth", 0.0)
            elif attendance_category == "children":
                total = bucket.get("children", 0.0)
            else:
                total = bucket.get("attendance_total", 0.0)
            series.append(total / sundays)
        out[church_key] = series
    return out


def _get_manual_remarks(scope: Scope, church_key: str) -> dict[str, str]:
    conn = _connect()
    rows = conn.execute(
        "SELECT category, remark_text FROM area_monitor_remarks WHERE scope_key = ? AND church_key = ?",
        (scope.scope_key, church_key),
    ).fetchall()
    conn.close()
    remarks = {category: "" for category in REMARK_CATEGORIES}
    for row in rows:
        category = str(row["category"] or "").strip().lower()
        if category in remarks:
            remarks[category] = str(row["remark_text"] or "")
    return remarks


def _save_manual_remarks(scope: Scope, church_key: str, payload: dict[str, Any]) -> None:
    actor = str(session.get("ao_username") or session.get("ao_name") or "AO").strip()
    now_iso = _now_ph().isoformat()
    conn = _connect()
    for category in REMARK_CATEGORIES:
        if category not in payload:
            continue
        text = str(payload.get(category) or "").strip()
        conn.execute(
            """
            INSERT INTO area_monitor_remarks (scope_key, church_key, category, remark_text, created_by, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(scope_key, church_key, category)
            DO UPDATE SET remark_text = excluded.remark_text, created_by = excluded.created_by, updated_at = excluded.updated_at
            """,
            (scope.scope_key, church_key, category, text, actor, now_iso, now_iso),
        )
    conn.commit()
    conn.close()


def _upsert_ceiling(scope: Scope, metric_key: str, current_max: float) -> float:
    conn = _connect()
    row = conn.execute(
        "SELECT ceiling_value FROM area_monitor_chart_ceiling WHERE scope_key = ? AND metric_key = ?",
        (scope.scope_key, metric_key),
    ).fetchone()
    existing = float(row["ceiling_value"] or 0.0) if row else 0.0
    new_value = max(existing, float(current_max or 0.0))
    if new_value <= 0:
        new_value = 10.0
    if new_value != existing or not row:
        conn.execute(
            """
            INSERT INTO area_monitor_chart_ceiling (scope_key, metric_key, ceiling_value, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(scope_key, metric_key)
            DO UPDATE SET ceiling_value = excluded.ceiling_value, updated_at = excluded.updated_at
            """,
            (scope.scope_key, metric_key, new_value, _now_ph().isoformat()),
        )
        conn.commit()
    conn.close()
    return float(new_value)


def _choose_report_timestamp(row: sqlite3.Row) -> datetime | None:
    for col in ["submitted_at", "updated_at", "created_at", "approved_at", "timestamp", "submitted_on"]:
        if col in row.keys():
            s = str(row[col] or "").strip()
            if not s:
                continue
            for parser in (
                lambda x: datetime.fromisoformat(x.replace("Z", "+00:00")),
                lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S"),
                lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M"),
                lambda x: datetime.strptime(x, "%m/%d/%Y %H:%M:%S"),
                lambda x: datetime.strptime(x, "%m/%d/%Y %H:%M"),
                lambda x: datetime.strptime(x, "%Y-%m-%d"),
            ):
                try:
                    dt = parser(s)
                    if dt.tzinfo is None:
                        return dt.replace(tzinfo=PH_TZ)
                    return dt.astimezone(PH_TZ)
                except Exception:
                    continue
    return None


def _submission_rows_for_month(scope: Scope, target_year: int, target_month: int, churches: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    conn = _connect()
    params = [target_year, target_month, scope.area]
    extra = ""
    if scope.is_sub_area:
        extra = " AND TRIM(COALESCE(a.sub_area,'')) = TRIM(?)"
        params.append(scope.sub_area)
    rows = conn.execute(
        f"""
        SELECT mr.*, TRIM(a.sex) AS church_name, TRIM(a.church_address) AS church_address
        FROM monthly_reports mr
        JOIN sheet_accounts_cache a ON TRIM(a.username) = TRIM(mr.pastor_username)
        WHERE CAST(mr.year AS INTEGER)=?
          AND CAST(mr.month AS INTEGER)=?
          AND TRIM(a.age)=TRIM(?)
          {extra}
        """,
        tuple(params),
    ).fetchall()
    conn.close()
    by_church = {}
    for row in rows:
        church_key = _resolve_church_key(churches, str(row["church_name"] or ""), str(row["church_address"] or ""))
        if not church_key:
            continue
        by_church[church_key] = {
            "submitted": bool(row["submitted"]) if "submitted" in row.keys() else True,
            "approved": bool(row["approved"]) if "approved" in row.keys() else False,
            "submitted_at": _choose_report_timestamp(row),
        }
    return by_church


def _compute_reporting_status(scope: Scope, churches: dict[str, dict[str, Any]], target_year: int, target_month: int) -> dict[str, Any]:
    today = _now_ph().date()
    last_sunday = _last_sunday(target_year, target_month)
    deadline = _report_deadline(target_year, target_month)
    submission_map = _submission_rows_for_month(scope, target_year, target_month, churches)

    # Also treat existing sheet report rows as "reported" even if timestamp metadata did not exist yet.
    report_rows = _fetch_report_rows(scope, [(target_year, target_month)], churches)
    reported_in_sheet = {row["church_key"] for row in report_rows}

    items = []
    counts = {"Early": 0, "On Time": 0, "Late": 0, "Reported": 0, "No Report": 0}

    for church_key, church in churches.items():
        entry = submission_map.get(church_key)
        submitted_at = entry.get("submitted_at") if entry else None
        has_submission = bool(entry and (entry.get("submitted") or entry.get("approved") or submitted_at))
        has_sheet_report = church_key in reported_in_sheet

        if has_submission and submitted_at:
            submitted_date = submitted_at.date()
            if submitted_date <= last_sunday:
                status = "Early"
            elif submitted_date <= deadline:
                status = "On Time"
            else:
                status = "Late"
        elif has_submission or has_sheet_report:
            status = "Reported"
        else:
            status = "No Report" if today > deadline else "On Time"

        counts[status] = counts.get(status, 0) + 1
        items.append({
            "church_key": church_key,
            "church_name": church["church_name"],
            "status": status,
            "date_submitted": submitted_at.strftime("%Y-%m-%d") if submitted_at else (f"{target_year:04d}-{target_month:02d}" if has_sheet_report else ""),
            "time_submitted": submitted_at.strftime("%I:%M %p") if submitted_at else ("Reported" if has_sheet_report else ""),
            "color": church["color"],
        })

    status_order = {"Early": 0, "On Time": 1, "Late": 2, "Reported": 3, "No Report": 4}
    items.sort(key=lambda x: (status_order.get(x["status"], 9), x["church_name"].lower()))
    return {
        "month_label": f"{calendar.month_name[target_month]} {target_year}",
        "deadline_label": deadline.strftime("%B %d, %Y"),
        "counts": counts,
        "rows": items,
    }


def _build_area_insights(monthly, months, churches, reporting):
    attendance_series = _attendance_average_series(monthly, months, "all")
    growth_candidates = []
    declining_count = 0
    no_ministry_count = 0
    for church_key, series in attendance_series.items():
        if len(series) >= 2:
            growth_candidates.append((church_key, _pct_change(series[-1], series[-2])))
        if len(series) >= 3 and _streak_declining(series[-3:]) >= 2:
            declining_count += 1
        ministry_total = 0.0
        for field in MINISTRY_FIELDS.keys():
            ministry_total += sum(monthly[church_key].get(ym, {}).get(field, 0.0) for ym in months)
        if ministry_total <= 0:
            no_ministry_count += 1
    insights = []
    if growth_candidates:
        best_key, best_pct = sorted(growth_candidates, key=lambda x: x[1], reverse=True)[0]
        insights.append(f"Top growing attendance: {churches[best_key]['church_name']} ({best_pct:+.0f}%).")
    if declining_count:
        insights.append(f"{declining_count} church(es) are declining in attendance for 3 straight checkpoints.")
    if no_ministry_count:
        insights.append(f"{no_ministry_count} church(es) have no recorded ministry activity in the selected range.")
    if reporting["counts"].get("Late", 0):
        insights.append(f"{reporting['counts']['Late']} church(es) are late in reporting for {reporting['month_label']}.")
    if reporting["counts"].get("No Report", 0):
        insights.append(f"{reporting['counts']['No Report']} church(es) have no report for {reporting['month_label']}.")
    return insights[:5]

def _top_ranked(
    labels_map: dict[str, str],
    colors_map: dict[str, str],
    metric_values: dict[str, float],
    top_n: int = 10,
) -> dict[str, Any]:
    ranked = sorted(
        metric_values.items(),
        key=lambda item: (-float(item[1] or 0), labels_map.get(item[0], item[0]).lower())
    )

    full_items = [
        {
            "church_key": church_key,
            "church_name": labels_map.get(church_key, church_key),
            "value": round(float(value or 0), 2),
            "color": colors_map.get(church_key, PALETTE[0]),
        }
        for church_key, value in ranked
    ]

    return {
        "top": full_items[:top_n],
        "all": full_items,
    }

def _dashboard_payload(scope: Scope, period: str, church_filter: str, finance_metric: str, attendance_category: str, ministry_metric: str) -> dict[str, Any]:
    churches = _fetch_scope_churches(scope)
    if not churches:
        return {"scope": {"area": scope.area, "sub_area": scope.sub_area, "role": scope.role}, "empty": True, "message": "No churches were found in this AO scope."}
    finance_metric = finance_metric if finance_metric in FINANCE_FIELDS else "amount_to_send"
    attendance_category = attendance_category if attendance_category in ATTENDANCE_FIELDS else "all"
    ministry_metric = ministry_metric if ministry_metric in MINISTRY_FIELDS else "received_jesus"
    months = _month_window(period)
    month_labels = [_month_label(y, m) for y, m in months]
    report_rows = _fetch_report_rows(scope, months, churches)
    monthly = _rollup_monthly(report_rows, churches)
    labels_map = {k: v["church_name"] for k, v in churches.items()}
    colors_map = {k: v["color"] for k, v in churches.items()}

    legend = [{"church_key": k, "church_name": v["church_name"], "color": v["color"]} for k, v in sorted(churches.items(), key=lambda item: item[1]["church_name"].lower())]

    if church_filter and church_filter != "all":
        church_key = _normalize_church_key(church_filter)
        if church_key not in churches:
            abort(404)
        return {
            "scope": {"area": scope.area, "sub_area": scope.sub_area, "role": scope.role},
            "empty": False,
            "detail_view": True,
            "churches": [{"church_key": k, "church_name": v["church_name"]} for k, v in churches.items()],
            "legend": legend,
            "last_sync_display": get_last_sync_display_ph(),
            "church_detail": _church_detail_payload(scope, church_key, period, finance_metric, attendance_category, ministry_metric),
            "server_time": _now_ph().isoformat(),
        }

    finance_series = {church_key: [float(monthly[church_key].get(ym, {}).get(finance_metric, 0.0)) for ym in months] for church_key in churches.keys()}
    finance_totals = {church_key: sum(vals) for church_key, vals in finance_series.items()}
    finance_ranked = _top_ranked(labels_map, colors_map, finance_totals)

    attendance_series = _attendance_average_series(monthly, months, attendance_category)
    attendance_values = {church_key: _average(vals) for church_key, vals in attendance_series.items()}
    attendance_ranked = _top_ranked(labels_map, colors_map, attendance_values)

    # Ministry now uses per-Sunday/per-week average, similar to attendance.
    ministry_series = {}
    for church_key in churches.keys():
        vals = []
        for year, month in months:
            sundays = _number_of_sundays(year, month)
            raw_val = float(monthly[church_key].get((year, month), {}).get(ministry_metric, 0.0))
            vals.append(raw_val / sundays)
        ministry_series[church_key] = vals

    ministry_totals = {church_key: _average(vals) for church_key, vals in ministry_series.items()}
    ministry_ranked = _top_ranked(labels_map, colors_map, ministry_totals)

    target_year, target_month = months[-1]
    reporting = _compute_reporting_status(scope, churches, target_year, target_month)
    insights = _build_area_insights(monthly, months, churches, reporting)

    finance_ceiling = _upsert_ceiling(scope, f"finances:{finance_metric}", max([x["value"] for x in finance_ranked["all"]] or [0]))
    attendance_ceiling = _upsert_ceiling(scope, f"attendance:{attendance_category}", max([x["value"] for x in attendance_ranked["all"]] or [0]))
    ministry_ceiling = _upsert_ceiling(scope, f"ministry:{ministry_metric}", max([x["value"] for x in ministry_ranked["all"]] or [0]))

    return {
        "scope": {"area": scope.area, "sub_area": scope.sub_area, "role": scope.role},
        "empty": False,
        "detail_view": False,
        "server_time": _now_ph().isoformat(),
        "last_sync_display": get_last_sync_display_ph(),
        "period": period,
        "churches": [{"church_key": k, "church_name": v["church_name"]} for k, v in churches.items()],
        "legend": legend,
        "month_labels": month_labels,
        "insights": insights,
        "filters": {"finance_metric": finance_metric, "attendance_category": attendance_category, "ministry_metric": ministry_metric},
        "finances": {"title": FINANCE_FIELDS[finance_metric], "metric_key": finance_metric, "currency": True, "y_max": finance_ceiling, **finance_ranked},
        "attendance": {"title": ATTENDANCE_FIELDS[attendance_category], "category": attendance_category, "currency": False, "y_max": attendance_ceiling, **attendance_ranked},
        "ministry": {"title": MINISTRY_FIELDS[ministry_metric], "metric_key": ministry_metric, "currency": False, "y_max": ministry_ceiling, **ministry_ranked},
        "reporting": reporting,
    }


def _church_detail_payload(scope: Scope, church_key: str, period: str = "3_months", finance_metric: str = "amount_to_send", attendance_category: str = "all", ministry_metric: str = "received_jesus") -> dict[str, Any]:
    churches = _fetch_scope_churches(scope)
    if church_key not in churches:
        abort(404)
    months = _month_window(period)
    report_rows = _fetch_report_rows(scope, months, churches)
    monthly = _rollup_monthly(report_rows, churches)
    church = churches[church_key]
    month_labels = [_month_label(y, m) for y, m in months]

    finance_series = {field: [float(monthly[church_key].get(ym, {}).get(field, 0.0)) for ym in months] for field in FINANCE_FIELDS}
    attendance_series = {field: _attendance_average_series(monthly, months, field)[church_key] for field in ATTENDANCE_FIELDS}

    # Ministry per month divided by number of Sundays/weeks in that month.
    ministry_series = {}
    for field in MINISTRY_FIELDS:
        vals = []
        for year, month in months:
            sundays = _number_of_sundays(year, month)
            raw_val = float(monthly[church_key].get((year, month), {}).get(field, 0.0))
            vals.append(raw_val / sundays)
        ministry_series[field] = vals

    target_year, target_month = months[-1]
    reporting = _compute_reporting_status(scope, churches, target_year, target_month)
    reporting_row = next((r for r in reporting["rows"] if r["church_key"] == church_key), None)
    selected_attendance = attendance_series[attendance_category]
    insights, flags = [], []

    if len(selected_attendance) >= 2:
        pct = _pct_change(selected_attendance[-1], selected_attendance[-2])
        insights.append(f"{pct:+.0f}% {ATTENDANCE_FIELDS[attendance_category].lower()} attendance vs previous checkpoint")
        if pct < 0:
            flags.append(f"{ATTENDANCE_FIELDS[attendance_category]} attendance is down {abs(pct):.0f}% from the previous checkpoint")
    if len(selected_attendance) >= 3 and _streak_declining(selected_attendance[-3:]) >= 2:
        flags.append(f"Declining {ATTENDANCE_FIELDS[attendance_category].lower()} attendance for 3 checkpoints")

    ministry_totals = {field: _average(values) for field, values in ministry_series.items()}
    if any(v > 0 for v in ministry_totals.values()):
        best_ministry_key = sorted(ministry_totals.items(), key=lambda x: x[1], reverse=True)[0][0]
        insights.append(f"Strongest ministry metric: {MINISTRY_FIELDS[best_ministry_key]} ({ministry_totals[best_ministry_key]:.2f} avg/week)")
    else:
        best_ministry_key = ministry_metric
        flags.append("No recorded ministry activity in the selected range")

    total_finance = sum(finance_series["amount_to_send"])
    insights.append(f"Total money sent in range: {_peso(total_finance)}")
    if reporting_row:
        if reporting_row["status"] == "Late":
            flags.append("Reporting is late this cycle")
        elif reporting_row["status"] == "No Report":
            flags.append("No report submitted for the active cycle")
        else:
            insights.append(f"Reporting status: {reporting_row['status']}")

    manual_remarks = _get_manual_remarks(scope, church_key)
    auto_remarks = {
        "general": "; ".join(insights[:2]),
        "finances": f"Selected-range total sent: {_peso(total_finance)}",
        "attendance": insights[0] if insights else "",
        "ministry": f"Top ministry metric: {MINISTRY_FIELDS[best_ministry_key]} ({ministry_totals[best_ministry_key]:.2f} avg/week)" if any(v > 0 for v in ministry_totals.values()) else "No ministry records yet",
        "reporting": reporting_row["status"] if reporting_row else "",
    }
    return {
        "church": church,
        "period": period,
        "month_labels": month_labels,
        "finance_metric": finance_metric,
        "attendance_category": attendance_category,
        "ministry_metric": ministry_metric,
        "finance_series": {k: [round(v, 2) for v in vals] for k, vals in finance_series.items()},
        "attendance_series": {k: [round(v, 2) for v in vals] for k, vals in attendance_series.items()},
        "ministry_series": {k: [round(v, 2) for v in vals] for k, vals in ministry_series.items()},
        "insights": insights[:6],
        "flags": flags[:6],
        "manual_remarks": manual_remarks,
        "automatic_remarks": auto_remarks,
        "reporting_row": reporting_row,
    }


PAGE_HTML = r'''<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Area Progress Monitor</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    :root { --bg:#f3f6fb; --card:#fff; --line:#dbe3ee; --text:#142033; --muted:#5b6880; --brand:#2563eb; --brand2:#1d4ed8; --shadow:0 10px 24px rgba(15,23,42,.08); --radius:18px; }
    *{box-sizing:border-box} html{scroll-behavior:smooth} body{margin:0;font-family:Arial,Helvetica,sans-serif;background:var(--bg);color:var(--text)}
    .topbar{position:sticky;top:0;z-index:40;background:linear-gradient(90deg,#01091f 0%,#04112f 100%);border-bottom:none;box-shadow:0 4px 18px rgba(15,23,42,.15)}
    .topbar-inner{max-width:100%;margin:0;padding:0 12px;min-height:57px;display:flex;align-items:center;justify-content:space-between;gap:10px}
    .brand{display:flex;align-items:center;gap:10px}.brand img{width:48px;height:48px;object-fit:contain}.brand-title{font-size:18px;font-weight:700;color:#fff;line-height:1.1;letter-spacing:.2px}.brand-sub{font-size:12px;color:#cfd8ea;font-weight:400}
    .burger{border:none;background:transparent;border-radius:0;width:40px;height:40px;display:inline-flex;align-items:center;justify-content:center;cursor:pointer;font-size:24px;color:#fff;box-shadow:none;padding:0}
    .menu{position:fixed;inset:0;background:rgba(2,8,23,.35);display:none;z-index:60}.menu.open{display:block}.menu-panel{width:min(292px,86vw);height:100%;background:#000a2b;padding:0;box-shadow:8px 0 30px rgba(0,0,0,.25)}
    .menu-header{display:flex;align-items:center;justify-content:space-between;color:#fff;padding:12px 16px;border-bottom:1px solid rgba(255,255,255,.08);font-size:13px;letter-spacing:.12em}
    .menu-close{background:transparent;border:none;color:#fff;font-size:28px;cursor:pointer;padding:0;line-height:1}
    .menu a{display:block;width:100%;text-align:left;text-decoration:none;color:#fff;padding:14px 16px;border:none;border-radius:0;margin:0;font-weight:500;background:transparent;cursor:pointer}
    .menu a:hover{background:rgba(255,255,255,.06)}
    .menu a.active{background:rgba(255,255,255,.08)}
    .wrap{max-width:1180px;margin:0 auto;padding:16px 12px 42px}.card{background:var(--card);border:1px solid var(--line);border-radius:var(--radius);box-shadow:var(--shadow)}.section{padding:14px;margin-bottom:14px}
    .hero{display:flex;align-items:flex-start;justify-content:space-between;gap:12px;margin-bottom:14px;flex-wrap:wrap}.hero h1{margin:0;font-size:26px}.subtitle{color:var(--muted);font-size:13px}
    .back-row{display:flex;justify-content:space-between;align-items:center;gap:12px;margin-bottom:12px;flex-wrap:wrap}.toolbar{display:grid;grid-template-columns:1fr;gap:10px;margin-bottom:12px}
    .filters{display:flex;gap:8px;flex-wrap:wrap;align-items:center}.section-head{display:flex;align-items:flex-start;justify-content:space-between;gap:8px;margin-bottom:10px;flex-wrap:wrap}.section-title{font-size:20px;font-weight:700}
    .pill,select,button,textarea{border-radius:12px;border:1px solid var(--line);background:#fff;color:var(--text);font-size:14px}.pill,button{padding:10px 14px;cursor:pointer}.pill.active{background:var(--brand);color:#fff;border-color:var(--brand)} select{padding:10px 12px;width:100%}
    .muted-chip{display:inline-flex;padding:6px 10px;border:1px solid var(--line);border-radius:999px;background:#f8fafc;font-size:12px;color:var(--muted)}
    .insights{display:grid;grid-template-columns:1fr;gap:8px;margin-bottom:12px}.insight{padding:10px 12px;border-radius:14px;background:#eef4ff;color:#173c7a;font-size:14px}
    .grid-2,.grid-4{display:grid;gap:14px}.grid-2{grid-template-columns:1fr}.grid-4{grid-template-columns:repeat(2,1fr)}.legend{display:flex;gap:8px;flex-wrap:wrap;margin-top:10px}.legend-item{display:inline-flex;align-items:center;gap:8px;padding:6px 10px;background:#f8fafc;border-radius:999px;border:1px solid var(--line);font-size:12px}.swatch{width:12px;height:12px;border-radius:999px;display:inline-block}
    .stat-grid{display:grid;grid-template-columns:repeat(2,1fr);gap:10px}.stat{padding:12px;border:1px solid var(--line);border-radius:16px;background:#fff}.stat-label{font-size:12px;color:var(--muted);margin-bottom:6px}.stat-value{font-size:24px;font-weight:700}.chart-box{min-height:280px}.inline-actions{display:flex;gap:8px;flex-wrap:wrap;align-items:center}.ghost{background:transparent}.table-wrap{overflow-x:auto}
    table{width:100%;border-collapse:collapse;font-size:14px}th,td{padding:10px 8px;border-bottom:1px solid var(--line);text-align:left;vertical-align:top}th{color:var(--muted);font-weight:600}.status-pill{display:inline-flex;align-items:center;padding:6px 10px;border-radius:999px;font-size:12px;font-weight:700}.status-Early{background:#e7f8ec;color:#146c31}.status-On-Time{background:#e8f1ff;color:#1f4f9c}.status-Late{background:#fff4e5;color:#a5520f}.status-Reported{background:#f3f4f6;color:#374151}.status-No-Report{background:#fde8e8;color:#991b1b}
    .remarks-grid{display:grid;grid-template-columns:1fr;gap:12px}textarea{min-height:88px;width:100%;padding:10px 12px;resize:vertical}.loader{padding:32px 0;text-align:center;color:var(--muted)}.hidden{display:none!important}
    .modal{position:fixed;inset:0;background:rgba(15,23,42,.58);display:none;align-items:stretch;justify-content:stretch;z-index:50}.modal.open{display:flex}.modal-panel{background:var(--bg);width:100%;height:100%;overflow:auto;padding:14px}
    .loading-overlay{position:fixed;inset:0;background:rgba(255,255,255,.90);z-index:9999;display:flex;align-items:center;justify-content:center;flex-direction:column;gap:14px}.loading-box{width:min(420px,88vw);background:#fff;border:1px solid var(--line);border-radius:18px;box-shadow:var(--shadow);padding:22px;text-align:center}.loading-title{font-weight:800;margin-bottom:8px;font-size:18px}.progress-track{width:100%;height:10px;background:#e5edf8;border-radius:999px;overflow:hidden;margin-top:12px}.progress-bar{width:0%;height:100%;background:linear-gradient(90deg,var(--brand),var(--brand2));border-radius:999px;transition:width .25s ease}
    @media (min-width:900px){.toolbar{grid-template-columns:1fr auto}.grid-2{grid-template-columns:1fr 1fr}.grid-4{grid-template-columns:repeat(4,1fr)}.insights{grid-template-columns:1fr 1fr}.remarks-grid{grid-template-columns:1fr 1fr}}
  </style>
</head>
<body>
<div class="loading-overlay" id="loadingOverlay"><div class="loading-box"><div class="loading-title">Loading Area Progress Monitor</div><div class="subtitle">Please wait while the latest data is being prepared...</div><div class="progress-track"><div class="progress-bar" id="progressBar"></div></div></div></div>
<div class="topbar"><div class="topbar-inner"><button class="burger" id="burgerBtn">☰</button><div class="brand"><img src="{{ logo_src }}" alt="Logo" onerror="this.style.display='none'"><div><div class="brand-title">District 4 Tool</div><div class="brand-sub">International One Way Outreach Foundation, Inc.</div></div></div><div style="width:40px;"></div></div></div>
<div class="menu" id="sideMenu"><div class="menu-panel"><div class="menu-header"><span>MENU</span><button type="button" class="menu-close" id="closeMenuBtn">×</button></div><a href="{{ bulletin_board_url }}">Bulletin Board</a><a href="{{ pastor_tool_url }}">Pastor's Tool</a><a href="{{ ao_tool_url }}">AO Tool</a><a href="{{ prayer_request_url }}" class="active">Prayer Request</a><a href="{{ event_registration_url }}">Event Registration (Pending)</a><a href="{{ schedules_url }}">Schedules</a><a href="{{ logout_url }}">Log Out</a></div></div>
<div class="wrap"><div class="back-row"><a href="{{ ao_tool_url }}" class="pill" style="text-decoration:none;">← Back to AO Tool</a><div class="muted-chip" id="lastSyncChip">Last sync: loading...</div></div><div class="hero"><div><h1>Area Progress Monitor</h1><div class="subtitle" id="scopeLabel">Loading scope...</div></div></div><div class="card section"><div class="toolbar"><select id="churchFilter"></select><div class="filters" id="timePills"></div></div><div class="legend" id="globalLegend"></div></div><div id="loader" class="loader card section">Loading Area Progress Monitor...</div><div id="content" class="hidden"></div></div>
<div class="modal" id="chartModal"><div class="modal-panel"><div class="section card"><div class="section-head"><div><div class="section-title" id="modalTitle">All Churches</div><div class="subtitle" id="modalSubtitle"></div></div><button class="pill ghost" id="closeChartModal">Close</button></div><div class="inline-actions" id="modalFilterBar" style="margin-bottom:12px;"></div><div class="chart-box"><canvas id="modalChart"></canvas></div><div class="legend" id="modalLegend"></div></div></div></div>
<div class="modal" id="churchModal"><div class="modal-panel" id="churchModalBody"></div></div>
<script>
const state={period:'3_months',churchFilter:'all',financeMetric:'amount_to_send',attendanceCategory:'all',ministryMetric:'received_jesus',payload:null,charts:{},modalChart:null,currentAnchor:null};
const labels={periods:{this_month:'This Month','3_months':'3 Months','6_months':'6 Months','1_year':'1 Year'},finance:{{ finance_options|tojson }},ministry:{{ ministry_options|tojson }},attendance:{{ attendance_options|tojson }}};
function esc(text){return String(text??'').replace(/[&<>\"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]))}
function peso(value){return new Intl.NumberFormat('en-PH',{style:'currency',currency:'PHP',maximumFractionDigits:2}).format(Number(value||0))}
function setLoader(on){document.getElementById('loader').classList.toggle('hidden',!on);document.getElementById('content').classList.toggle('hidden',on)}
function setOverlayProgress(pct){document.getElementById('progressBar').style.width=`${pct}%`} function hideOverlay(){const overlay=document.getElementById('loadingOverlay');if(overlay)overlay.style.display='none'}
function rememberAnchor(id){state.currentAnchor=id||null} function restoreAnchor(){if(!state.currentAnchor)return;const el=document.getElementById(state.currentAnchor);if(el)setTimeout(()=>el.scrollIntoView({behavior:'smooth',block:'start'}),30)}
function populateChurchFilter(payload){const el=document.getElementById('churchFilter');let html='<option value="all">All Churches</option>';(payload.churches||[]).slice().sort((a,b)=>a.church_name.localeCompare(b.church_name)).forEach(ch=>{html+=`<option value="${esc(ch.church_key)}" ${ch.church_key===state.churchFilter?'selected':''}>${esc(ch.church_name)}</option>`});el.innerHTML=html}
function populateGlobalFilters(){const pillWrap=document.getElementById('timePills');pillWrap.innerHTML=Object.entries(labels.periods).map(([value,label])=>`<button class="pill ${value===state.period?'active':''}" data-period="${value}">${label}</button>`).join('')}
function destroyCharts(){Object.values(state.charts).forEach(chart=>{if(chart&&chart.destroy)chart.destroy()});state.charts={}}
function makeChart(canvasId,type,labelsArr,valuesArr,colorsArr,yMax,currency=false){const ctx=document.getElementById(canvasId);if(!ctx)return null;return new Chart(ctx,{type,data:{labels:labelsArr,datasets:[{data:valuesArr,borderColor:colorsArr[0]||'#2563eb',backgroundColor:type==='line'?colorsArr[0]||'#2563eb':colorsArr,pointBackgroundColor:colorsArr,fill:false,tension:.28}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false},tooltip:{callbacks:{label:(ctx)=>currency?peso(ctx.parsed.y??ctx.parsed):Number(ctx.parsed.y??ctx.parsed).toLocaleString('en-PH')}}},scales:{y:{beginAtZero:true,suggestedMax:Math.ceil(Number(yMax||10)),ticks:{callback:(value)=>currency?peso(value):Number(value).toLocaleString('en-PH')}}}}})}
function legendHtml(items){return(items||[]).map(item=>`<span class="legend-item"><span class="swatch" style="background:${item.color}"></span>${esc(item.church_name)}</span>`).join('')}
function filterButtonsHtml(kind,isDetail=false){if(kind==='finances'){return Object.entries(labels.finance).map(([key,label])=>`<button class="pill ${state.financeMetric===key?'active':''}" data-finance="${key}" ${isDetail?'data-detail="1"':''}>${esc(label)}</button>`).join('')} if(kind==='attendance'){return Object.entries(labels.attendance).map(([key,label])=>`<button class="pill ${state.attendanceCategory===key?'active':''}" data-attendance="${key}" ${isDetail?'data-detail="1"':''}>${esc(label)}</button>`).join('')} if(kind==='ministry'){return Object.entries(labels.ministry).map(([key,label])=>`<button class="pill ${state.ministryMetric===key?'active':''}" data-ministry="${key}" ${isDetail?'data-detail="1"':''}>${esc(label)}</button>`).join('')} if(kind==='period'){return Object.entries(labels.periods).map(([key,label])=>`<button class="pill ${state.period===key?'active':''}" data-detail-period="${key}">${esc(label)}</button>`).join('')} return ''}
function renderDashboard(payload){const content=document.getElementById('content');if(payload.empty){content.innerHTML=`<div class="card section">${esc(payload.message||'No data.')}</div>`;document.getElementById('scopeLabel').textContent=`Area ${payload.scope?.area||''}`;document.getElementById('globalLegend').innerHTML='';return} document.getElementById('scopeLabel').textContent=payload.scope.sub_area?`Area ${payload.scope.area} • ${payload.scope.sub_area}`:`Area ${payload.scope.area}`;document.getElementById('globalLegend').innerHTML=legendHtml(payload.legend||[]);document.getElementById('lastSyncChip').textContent=`Last sync: ${payload.last_sync_display||'Unknown'}`;if(payload.detail_view){renderChurchDetailView(payload.church_detail,false);return} const financeTop=payload.finances.top||[]; const attendanceTop=payload.attendance.top||[]; const ministryTop=payload.ministry.top||[]; content.innerHTML=`<div class="insights">${(payload.insights||[]).map(text=>`<div class="insight">${esc(text)}</div>`).join('')}</div><div class="card section" id="financeSection"><div class="section-head"><div><div class="section-title">Financial Overview</div><div class="subtitle">Top 10 churches — ${esc(payload.finances.title)}</div></div><div class="inline-actions"><button class="pill" data-expand="finances">View All</button></div></div><div class="filters" style="margin-bottom:12px;">${filterButtonsHtml('finances')}</div><div class="chart-box"><canvas id="financeChart"></canvas></div></div><div class="card section" id="attendanceSection"><div class="section-head"><div><div class="section-title">Attendance Overview</div><div class="subtitle">Top 10 churches — ${esc(payload.attendance.title)} average</div></div><div class="inline-actions"><button class="pill" data-expand="attendance">View All</button></div></div><div class="filters" style="margin-bottom:12px;">${filterButtonsHtml('attendance')}</div><div class="chart-box"><canvas id="attendanceChart"></canvas></div></div><div class="grid-2"><div class="card section" id="ministrySection"><div class="section-head"><div><div class="section-title">Ministry Activity</div><div class="subtitle">Top 10 churches — ${esc(payload.ministry.title)}</div></div><div class="inline-actions"><button class="pill" data-expand="ministry">View All</button></div></div><div class="filters" style="margin-bottom:12px;">${filterButtonsHtml('ministry')}</div><div class="chart-box"><canvas id="ministryChart"></canvas></div></div><div class="card section" id="reportingSection"><div class="section-head"><div><div class="section-title">Reporting Status</div><div class="subtitle">${esc(payload.reporting.month_label)} • Deadline ${esc(payload.reporting.deadline_label)}</div></div></div><div class="stat-grid"><div class="stat"><div class="stat-label">Early</div><div class="stat-value">${payload.reporting.counts['Early']||0}</div></div><div class="stat"><div class="stat-label">On Time</div><div class="stat-value">${payload.reporting.counts['On Time']||0}</div></div><div class="stat"><div class="stat-label">Late</div><div class="stat-value">${payload.reporting.counts['Late']||0}</div></div><div class="stat"><div class="stat-label">Reported</div><div class="stat-value">${payload.reporting.counts['Reported']||0}</div></div><div class="stat"><div class="stat-label">No Report</div><div class="stat-value">${payload.reporting.counts['No Report']||0}</div></div></div><div class="table-wrap" style="margin-top:12px;"><table><thead><tr><th>Church</th><th>Status</th><th>Date Submitted</th><th>Time Submitted</th></tr></thead><tbody>${(payload.reporting.rows||[]).map(row=>`<tr><td><button class="pill ghost" data-church="${esc(row.church_key)}">${esc(row.church_name)}</button></td><td><span class="status-pill ${'status-'+String(row.status).replace(/\s+/g,'-')}">${esc(row.status)}</span></td><td>${esc(row.date_submitted||'--')}</td><td>${esc(row.time_submitted||'--')}</td></tr>`).join('')}</tbody></table></div></div></div>`; destroyCharts(); state.charts.finance=makeChart('financeChart','bar',financeTop.map(x=>x.church_name),financeTop.map(x=>x.value),financeTop.map(x=>x.color),payload.finances.y_max,true); state.charts.attendance=makeChart('attendanceChart','line',attendanceTop.map(x=>x.church_name),attendanceTop.map(x=>x.value),attendanceTop.map(x=>x.color),payload.attendance.y_max,false); state.charts.ministry=makeChart('ministryChart','bar',ministryTop.map(x=>x.church_name),ministryTop.map(x=>x.value),ministryTop.map(x=>x.color),payload.ministry.y_max,false)}
function renderChurchDetailView(detail,modal=false){const target=modal?document.getElementById('churchModalBody'):document.getElementById('content');target.innerHTML=`<div class="card section"><div class="section-head"><div><div class="section-title">${esc(detail.church.church_name)}</div><div class="subtitle">${esc(detail.church.pastor_name||'')} • ${esc(detail.church.church_address||'')}</div></div><div class="inline-actions"><button class="pill ghost" ${modal?'id="closeChurchModal"':'id="closeDetailView"'}>Close</button></div></div><div class="filters" style="margin-bottom:12px;">${filterButtonsHtml('period',true)}</div><div class="grid-2" style="margin-top:14px;"><div><div class="section-title" style="font-size:18px;margin-bottom:8px;">Automatic Insights</div><div class="insights">${(detail.insights||[]).map(text=>`<div class="insight">${esc(text)}</div>`).join('')||'<div class="insight">No insights yet.</div>'}</div></div><div><div class="section-title" style="font-size:18px;margin-bottom:8px;">Flags</div><div class="insights">${(detail.flags||[]).map(text=>`<div class="insight" style="background:#fff4e5;color:#8a4a03;">${esc(text)}</div>`).join('')||'<div class="insight">No flags in the current selection.</div>'}</div></div></div><div class="grid-2" style="margin-top:14px;"><div class="card section"><div class="section-head"><div class="section-title" style="font-size:18px;">Attendance Trend</div></div><div class="filters" style="margin-bottom:12px;">${filterButtonsHtml('attendance',true)}</div><div class="chart-box"><canvas id="detailAttendanceChart"></canvas></div></div><div class="card section"><div class="section-head"><div class="section-title" style="font-size:18px;">Finance Trend</div></div><div class="filters" style="margin-bottom:12px;">${filterButtonsHtml('finances',true)}</div><div class="chart-box"><canvas id="detailFinanceChart"></canvas></div></div></div><div class="card section" style="margin-top:14px;"><div class="section-head"><div class="section-title" style="font-size:18px;">Ministry Activity Trend</div></div><div class="filters" style="margin-bottom:12px;">${filterButtonsHtml('ministry',true)}</div><div class="chart-box"><canvas id="detailMinistryChart"></canvas></div></div><div class="card section" style="margin-top:14px;"><div class="section-title" style="font-size:18px;margin-bottom:8px;">Manual Remarks</div><div class="remarks-grid">${['general','finances','attendance','ministry','reporting'].map(category=>`<div><div class="subtitle" style="margin-bottom:6px;text-transform:capitalize;">${esc(category)}</div><textarea data-remark="${category}" placeholder="Write ${category} remark...">${esc((detail.manual_remarks&&detail.manual_remarks[category])||'')}</textarea><div class="subtitle" style="margin-top:6px;"><strong>Automatic note:</strong> ${esc((detail.automatic_remarks&&detail.automatic_remarks[category])||'--')}</div></div>`).join('')}</div><div class="inline-actions" style="margin-top:12px;"><button class="pill" id="saveRemarksBtn" data-church="${esc(detail.church.church_key)}">Save Remarks</button></div></div></div>`; if(modal){document.getElementById('churchModal').classList.add('open');document.getElementById('closeChurchModal')?.addEventListener('click',()=>document.getElementById('churchModal').classList.remove('open'))} else {document.getElementById('closeDetailView')?.addEventListener('click',()=>{state.churchFilter='all';rememberAnchor(null);refreshData(true)})} setTimeout(()=>{if(state.charts.detailAttendance)state.charts.detailAttendance.destroy();if(state.charts.detailFinance)state.charts.detailFinance.destroy();if(state.charts.detailMinistry)state.charts.detailMinistry.destroy();state.charts.detailAttendance=new Chart(document.getElementById('detailAttendanceChart'),{type:'line',data:{labels:detail.month_labels,datasets:[{data:detail.attendance_series[state.attendanceCategory]||[],borderColor:detail.church.color,backgroundColor:detail.church.color,tension:.28}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}}}});state.charts.detailFinance=new Chart(document.getElementById('detailFinanceChart'),{type:'bar',data:{labels:detail.month_labels,datasets:[{data:detail.finance_series[state.financeMetric]||[],backgroundColor:detail.church.color}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}}}});state.charts.detailMinistry=new Chart(document.getElementById('detailMinistryChart'),{type:'bar',data:{labels:detail.month_labels,datasets:[{data:detail.ministry_series[state.ministryMetric]||[],backgroundColor:detail.church.color}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}}}})},10)}
async function bootSync(){const res=await fetch(`{{ api_base }}/boot`,{method:'POST',credentials:'same-origin'}); if(!res.ok) throw new Error('Failed to prepare latest data'); return await res.json()}
async function fetchPayload(){const params=new URLSearchParams({period:state.period,church:state.churchFilter,finance_metric:state.financeMetric,attendance_category:state.attendanceCategory,ministry_metric:state.ministryMetric});const res=await fetch(`{{ api_base }}/snapshot?${params.toString()}`,{credentials:'same-origin'});if(!res.ok)throw new Error('Failed to load dashboard');return await res.json()}
async function refreshData(showLoader=false){if(showLoader)setLoader(true);try{const payload=await fetchPayload();state.payload=payload;populateChurchFilter(payload);populateGlobalFilters();renderDashboard(payload);restoreAnchor()}catch(err){document.getElementById('content').innerHTML=`<div class="card section">${esc(err.message||'Something went wrong.')}</div>`}finally{setLoader(false)}}
function openExpandModal(kind){if(!state.payload||!state.payload[kind])return;const block=state.payload[kind];const items=block.all||[];document.getElementById('chartModal').classList.add('open');document.getElementById('modalTitle').textContent=`${kind.charAt(0).toUpperCase()+kind.slice(1)} — All Churches`;document.getElementById('modalSubtitle').textContent=block.title||'';document.getElementById('modalLegend').innerHTML=legendHtml(items);const modalFilterBar=document.getElementById('modalFilterBar');if(kind==='finances'){modalFilterBar.innerHTML=filterButtonsHtml('finances')}else if(kind==='attendance'){modalFilterBar.innerHTML=filterButtonsHtml('attendance')}else if(kind==='ministry'){modalFilterBar.innerHTML=filterButtonsHtml('ministry')}else{modalFilterBar.innerHTML=''} if(state.modalChart)state.modalChart.destroy(); state.modalChart=new Chart(document.getElementById('modalChart'),{type:kind==='attendance'?'line':'bar',data:{labels:items.map(x=>x.church_name),datasets:[{data:items.map(x=>x.value),backgroundColor:items.map(x=>x.color),borderColor:items[0]?.color||'#2563eb',tension:.28}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}}}})}
async function openChurchModal(churchKey){const params=new URLSearchParams({period:state.period,finance_metric:state.financeMetric,attendance_category:state.attendanceCategory,ministry_metric:state.ministryMetric});const res=await fetch(`{{ api_base }}/church/${encodeURIComponent(churchKey)}?${params.toString()}`,{credentials:'same-origin'});if(!res.ok)return;const detail=await res.json();renderChurchDetailView(detail,true)}
async function saveRemarks(churchKey){const body={};document.querySelectorAll('[data-remark]').forEach(el=>{body[el.getAttribute('data-remark')]=el.value});const res=await fetch(`{{ api_base }}/church/${encodeURIComponent(churchKey)}/remarks`,{method:'POST',credentials:'same-origin',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});if(!res.ok)return alert('Failed to save remarks.');alert('Remarks saved.')}
function wireEvents(){document.getElementById('burgerBtn').addEventListener('click',()=>document.getElementById('sideMenu').classList.add('open'));document.getElementById('closeMenuBtn').addEventListener('click',()=>document.getElementById('sideMenu').classList.remove('open'));document.getElementById('sideMenu').addEventListener('click',e=>{if(e.target.id==='sideMenu')e.currentTarget.classList.remove('open')});document.getElementById('timePills').addEventListener('click',e=>{const value=e.target.getAttribute('data-period');if(!value)return;state.period=value;rememberAnchor(null);refreshData(true)});document.getElementById('churchFilter').addEventListener('change',e=>{state.churchFilter=e.target.value;rememberAnchor(null);refreshData(true)});document.getElementById('content').addEventListener('click',e=>{const expand=e.target.getAttribute('data-expand');if(expand)return openExpandModal(expand);const church=e.target.getAttribute('data-church');if(church)return openChurchModal(church);if(e.target.id==='saveRemarksBtn')return saveRemarks(e.target.getAttribute('data-church'));const finance=e.target.getAttribute('data-finance');if(finance&&!e.target.getAttribute('data-detail')){state.financeMetric=finance;rememberAnchor('financeSection');refreshData(false);return}const attendance=e.target.getAttribute('data-attendance');if(attendance&&!e.target.getAttribute('data-detail')){state.attendanceCategory=attendance;rememberAnchor('attendanceSection');refreshData(false);return}const ministry=e.target.getAttribute('data-ministry');if(ministry&&!e.target.getAttribute('data-detail')){state.ministryMetric=ministry;rememberAnchor('ministrySection');refreshData(false);return}if(finance&&e.target.getAttribute('data-detail')){state.financeMetric=finance;if(state.churchFilter&&state.churchFilter!=='all'){refreshData(false)}else{const openChurch=document.querySelector('#saveRemarksBtn')?.getAttribute('data-church');if(openChurch)openChurchModal(openChurch)}return}if(attendance&&e.target.getAttribute('data-detail')){state.attendanceCategory=attendance;if(state.churchFilter&&state.churchFilter!=='all'){refreshData(false)}else{const openChurch=document.querySelector('#saveRemarksBtn')?.getAttribute('data-church');if(openChurch)openChurchModal(openChurch)}return}if(ministry&&e.target.getAttribute('data-detail')){state.ministryMetric=ministry;if(state.churchFilter&&state.churchFilter!=='all'){refreshData(false)}else{const openChurch=document.querySelector('#saveRemarksBtn')?.getAttribute('data-church');if(openChurch)openChurchModal(openChurch)}return}const detailPeriod=e.target.getAttribute('data-detail-period');if(detailPeriod){state.period=detailPeriod;if(state.churchFilter&&state.churchFilter!=='all'){refreshData(false)}else{const openChurch=document.querySelector('#saveRemarksBtn')?.getAttribute('data-church');if(openChurch)openChurchModal(openChurch)}return}});document.getElementById('chartModal').addEventListener('click',e=>{if(e.target.id==='chartModal')e.currentTarget.classList.remove('open')});document.getElementById('closeChartModal').addEventListener('click',()=>document.getElementById('chartModal').classList.remove('open'));document.getElementById('churchModal').addEventListener('click',e=>{if(e.target.id==='churchModal')e.currentTarget.classList.remove('open')});document.getElementById('modalFilterBar').addEventListener('click',e=>{const finance=e.target.getAttribute('data-finance');const attendance=e.target.getAttribute('data-attendance');const ministry=e.target.getAttribute('data-ministry');if(finance){state.financeMetric=finance;refreshData(false).then(()=>openExpandModal('finances'));return}if(attendance){state.attendanceCategory=attendance;refreshData(false).then(()=>openExpandModal('attendance'));return}if(ministry){state.ministryMetric=ministry;refreshData(false).then(()=>openExpandModal('ministry'));return}})}
(async function init(){wireEvents();populateGlobalFilters();setOverlayProgress(10);try{setOverlayProgress(35);await bootSync();setOverlayProgress(70);await refreshData(true);setOverlayProgress(100)}catch(e){console.error(e);await refreshData(true);setOverlayProgress(100)}setTimeout(()=>hideOverlay(),250)})();
</script>
</body></html>'''


@bp.before_app_request
def _init_monitor_tables_once() -> None:
    ensure_area_progress_monitor_tables()


@bp.route("/ao-tool/area-progress-monitor")
def area_progress_monitor_page():
    try:
        _require_ao()
    except Exception:
        return redirect(url_for("ao_login", next=request.path))
    return render_template_string(
        PAGE_HTML,
        api_base=url_for("area_progress_monitor.area_progress_monitor_snapshot").rsplit("/", 1)[0],
        ao_tool_url=url_for("ao_tool"),
        church_status_url=url_for("ao_church_status"),
        bulletin_board_url=url_for("bulletin_board") if "bulletin_board" in current_app.view_functions else "#",
        pastor_tool_url=url_for("pastor_tool") if "pastor_tool" in current_app.view_functions else "#",
        prayer_request_url=url_for("prayer_requests") if "prayer_requests" in current_app.view_functions else "#",
        event_registration_url=url_for("event_registration_pending") if "event_registration_pending" in current_app.view_functions else "#",
        schedules_url=url_for("schedules") if "schedules" in current_app.view_functions else "#",
        logout_url=url_for("logout") if "logout" in current_app.view_functions else "#",
        finance_options={k: v for k, v in FINANCE_FIELDS.items()},
        ministry_options={k: v for k, v in MINISTRY_FIELDS.items()},
        attendance_options={k: v for k, v in ATTENDANCE_FIELDS.items()},
        logo_src=LOGO_SRC,
    )


@bp.route("/api/ao-tool/area-progress-monitor/boot", methods=["POST"])
def area_progress_monitor_boot():
    _require_ao()
    sync_from_sheets_if_needed(force=True)
    return jsonify({"ok": True, "last_sync_display": get_last_sync_display_ph()})


@bp.route("/api/ao-tool/area-progress-monitor/snapshot")
def area_progress_monitor_snapshot():
    scope = _require_ao()
    payload = _dashboard_payload(
        scope=scope,
        period=str(request.args.get("period") or "3_months"),
        church_filter=str(request.args.get("church") or "all"),
        finance_metric=str(request.args.get("finance_metric") or "amount_to_send"),
        attendance_category=str(request.args.get("attendance_category") or "all"),
        ministry_metric=str(request.args.get("ministry_metric") or "received_jesus"),
    )
    return jsonify(payload)


@bp.route("/api/ao-tool/area-progress-monitor/church/<church_key>")
def area_progress_monitor_church_detail(church_key: str):
    scope = _require_ao()
    detail = _church_detail_payload(
        scope,
        _normalize_church_key(church_key),
        period=str(request.args.get("period") or "3_months"),
        finance_metric=str(request.args.get("finance_metric") or "amount_to_send"),
        attendance_category=str(request.args.get("attendance_category") or "all"),
        ministry_metric=str(request.args.get("ministry_metric") or "received_jesus"),
    )
    return jsonify(detail)


@bp.route("/api/ao-tool/area-progress-monitor/church/<church_key>/remarks", methods=["POST"])
def area_progress_monitor_save_remarks(church_key: str):
    scope = _require_ao()
    payload = request.get_json(silent=True) or {}
    _save_manual_remarks(scope, _normalize_church_key(church_key), payload)
    return jsonify({"ok": True, "saved_at": _now_ph().isoformat()})


@bp.route("/api/ao-tool/area-progress-monitor/church/<church_key>/remarks", methods=["GET"])
def area_progress_monitor_get_remarks(church_key: str):
    scope = _require_ao()
    return jsonify(_get_manual_remarks(scope, _normalize_church_key(church_key)))


def register_area_progress_monitor(app) -> None:
    app.register_blueprint(bp)
