from __future__ import annotations

import calendar
import os
import sqlite3
from collections import defaultdict
from contextlib import closing
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

import gspread
from runtime_config import data_path, google_credentials
from flask import Blueprint, abort, current_app, jsonify, redirect, render_template_string, request, session, url_for
from login_events import ensure_login_event_schema

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
    return data_path("app_v2.db")


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


def _parse_selected_month(selected_month: str | None, today: date | None = None) -> date:
    """Default to the last completed month; allow current-month progress explicitly."""
    today = today or _now_ph().date()
    latest = _month_floor(today)
    fallback = max(date(2026, 1, 1), _latest_completed_month(today))
    try:
        chosen = date.fromisoformat(str(selected_month or '').strip() + '-01')
        return max(date(2026, 1, 1), min(chosen, latest))
    except ValueError:
        return fallback


def _month_options(today: date | None = None) -> list[dict[str, str]]:
    latest_completed = _month_floor(today or _now_ph().date())
    start = date(2026, 1, 1)
    options: list[dict[str, str]] = []

    cursor = start
    while cursor <= latest_completed:
        options.append({
            "value": f"{cursor.year:04d}-{cursor.month:02d}",
            "label": f"{calendar.month_name[cursor.month]} {cursor.year}",
        })
        if cursor.month == 12:
            cursor = date(cursor.year + 1, 1, 1)
        else:
            cursor = date(cursor.year, cursor.month + 1, 1)

    return list(reversed(options))


def _month_window(period: str, today: date | None = None, selected_month: str | None = None) -> list[tuple[int, int]]:
    latest_completed = _latest_completed_month(today)

    if period not in ("6_months", "1_year"):
        chosen = _parse_selected_month(selected_month, today)
        return [(chosen.year, chosen.month)]

    count = {"6_months": 6, "1_year": 12}[period]
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


def ensure_area_progress_monitor_tables(database=None) -> None:
    """Initialize at application startup, sharing the caller's transaction if supplied."""
    conn = database if database is not None else _connect()
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
        CREATE TABLE IF NOT EXISTS area_monitor_seen_notifications (
            user_key TEXT NOT NULL,
            scope_key TEXT NOT NULL,
            notification_id TEXT NOT NULL,
            seen_at TEXT NOT NULL,
            PRIMARY KEY (user_key, scope_key, notification_id)
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_apm_seen_notifications_user_scope ON area_monitor_seen_notifications(user_key, scope_key)")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS area_monitor_member_snapshot (
            scope_key TEXT NOT NULL,
            member_key TEXT NOT NULL,
            name TEXT,
            birthday TEXT,
            church_name TEXT,
            username TEXT,
            pastor TEXT,
            signature TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (scope_key, member_key)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS area_monitor_member_events (
            event_id TEXT PRIMARY KEY,
            scope_key TEXT NOT NULL,
            event_type TEXT NOT NULL,
            title TEXT NOT NULL,
            message TEXT NOT NULL,
            church_name TEXT,
            created_at TEXT NOT NULL
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_apm_member_events_scope ON area_monitor_member_events(scope_key, created_at)")
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
    cur.execute("CREATE INDEX IF NOT EXISTS idx_apm_prayer_church ON sheet_prayer_request_cache(church_name)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_apm_prayer_date ON sheet_prayer_request_cache(request_date)")

    ensure_login_event_schema(conn)

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
    if database is None:
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
    import app as appmod
    return appmod.get_last_sync_display_ph()


def get_gs_client():
    creds = google_credentials(
        GOOGLE_SHEETS_CREDENTIALS_FILE,
        scopes=GOOGLE_SHEETS_SCOPES,
    )
    return gspread.authorize(creds)


def sync_from_sheets_if_needed(force: bool = False):
    import app as appmod
    return appmod.sync_from_sheets_if_needed(force=force)


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
            "date_submitted": submitted_at.strftime("%Y-%m-%d") if submitted_at else "",
            "time_submitted": submitted_at.strftime("%I:%M %p") if submitted_at else "",
            "color": church["color"],
        })

    status_order = {"Early": 0, "On Time": 1, "Late": 2, "Reported": 3, "No Report": 4}
    items.sort(key=lambda x: (status_order.get(x["status"], 9), x["church_name"].lower()))
    return {
        "month": f"{target_year:04d}-{target_month:02d}",
        "month_label": f"{calendar.month_name[target_month]} {target_year}",
        "deadline_label": deadline.strftime("%B %d, %Y"),
        "counts": counts,
        "rows": items,
    }


def _area_insight_details(scope, churches, selected_month, reporting):
    """Explain area messages using only this AO's churches and actual report rows."""
    selected = _parse_selected_month(selected_month)
    comparison = [_subtract_months(selected, offset) for offset in (2, 1, 0)]
    months = [(month.year, month.month) for month in comparison]
    monthly = _rollup_monthly(_fetch_report_rows(scope, months, churches), churches)
    averages = _attendance_average_series(monthly, months, 'all')
    labels = [_month_label(*month) for month in months]
    selected_label = f'{calendar.month_name[selected.month]} {selected.year}'
    details, growing, declining, no_ministry = [], [], [], []
    complete = selected < _month_floor(_now_ph().date())
    for key, church in churches.items():
        values = averages[key]
        present = [monthly[key].get(month, {}).get('rows', 0) > 0 for month in months]
        base = dict(church_key=key, church_name=church['church_name'], month=selected.strftime('%Y-%m'))
        if complete and all(present[-2:]) and values[-2] > 0 and values[-1] > values[-2]:
            percentage = _pct_change(values[-1], values[-2])
            growing.append(dict(base, percentage=percentage, values={labels[-2]:round(values[-2],2),
                labels[-1]:round(values[-1],2), 'Change':f'{percentage:+.1f}%'}))
        if complete and all(present) and _streak_declining(values) >= 2:
            declining.append(dict(base, values={label:round(value,2) for label,value in zip(labels,values)}))
        bucket = monthly[key].get(months[-1], {})
        total = sum(bucket.get(field, 0) for field in MINISTRY_FIELDS)
        if total <= 0:
            no_ministry.append(dict(base, values={'Report available':'Yes' if present[-1] else 'No',
                'Recorded ministry activities':0 if present[-1] else 'Not reported'}))
    if growing:
        growing.sort(key=lambda row:(-row['percentage'],row['church_name']))
        top = growing[0]
        details.append(dict(id='attendance-growth', category='Attendance', title='Top growing attendance',
            message=f"{top['church_name']} has the strongest recorded attendance growth ({top['percentage']:+.1f}%).",
            period=f'{labels[-2]} → {labels[-1]}',
            explanation='All-age attendance totals are divided by the Sundays in each month. Growth is the percentage change from the previous month. Churches need reports in both months and a nonzero previous average; missing reports are not treated as zero attendance.',rows=growing))
    if declining:
        details.append(dict(id='attendance-decline',category='Attendance',title='Declining attendance',
            message=f'{len(declining)} church(es) have lower average attendance at three consecutive monthly checkpoints.',
            period=' → '.join(labels),explanation='Each listed church has actual reports in all three comparison months and two consecutive decreases in average weekly attendance. The comparison ends with the selected progress month.',rows=declining))
    if not growing and not declining:
        details.append(dict(id='attendance-comparison',category='Attendance',title='Attendance comparison',
            message='The selected month is still in progress.' if not complete else 'No positive growth or three-checkpoint decline was confirmed.',
            period=selected_label,explanation='Growth and decline comparisons require completed months with actual reports. A missing report or a zero previous average cannot establish a percentage change.',rows=[]))
    if no_ministry:
        details.append(dict(id='ministry-records',category='Ministry',title='No recorded ministry activity',
            message=f'{len(no_ministry)} church(es) have no recorded ministry activity for {selected_label}.',
            period=selected_label,explanation='This checks all ministry fields in the selected month. A church without a report is shown as Not reported; missing data does not prove that no ministry took place.',rows=no_ministry))
    for status,identifier,title in (('Late','late-reports','Late reporting'),('No Report','missing-reports','Missing reports')):
        rows=[dict(church_key=row['church_key'],church_name=row['church_name'],month=reporting['month'],
                   values={'Status':row['status'],'Submitted':row['date_submitted'] or ('Not submitted' if status=='No Report' else 'Not recorded'),
                           'Time':row['time_submitted'] or '—'}) for row in reporting['rows'] if row['status']==status]
        if rows:
            details.append(dict(id=identifier,category='Church Reporting',title=title,
                message=f"{len(rows)} church(es) {'submitted late' if status=='Late' else 'have no report'} for {reporting['month_label']}.",
                period=reporting['month_label'],explanation='Reporting is checked against the Church Reporting month selector. The deadline is '+reporting['deadline_label']+'. Existing Sheets reports without a submission timestamp are marked Reported, not assigned an invented time.',rows=rows))
    return details


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


def _parse_any_date(value: Any) -> date | None:
    s = str(value or "").strip()
    if not s:
        return None
    for parser in (
        lambda x: date.fromisoformat(x[:10]),
        lambda x: datetime.strptime(x, "%Y-%m-%d").date(),
        lambda x: datetime.strptime(x, "%m/%d/%Y").date(),
        lambda x: datetime.strptime(x, "%m/%d/%y").date(),
        lambda x: datetime.strptime(x, "%B %d, %Y").date(),
        lambda x: datetime.strptime(x, "%b %d, %Y").date(),
    ):
        try:
            return parser(s)
        except Exception:
            continue
    return None


def _birthday_days_until(bday: Any, today: date | None = None) -> int | None:
    parsed = _parse_any_date(bday)
    if not parsed:
        return None
    today = today or _now_ph().date()
    this_year = date(today.year, parsed.month, parsed.day)
    if this_year < today:
        this_year = date(today.year + 1, parsed.month, parsed.day)
    return (this_year - today).days


def _notification_user_key() -> str:
    return str(
        session.get("ao_username")
        or session.get("ao_name")
        or session.get("username")
        or "ao"
    ).strip().lower() or "ao"


def _notification_item(kind: str, title: str, message: str, church_name: str = "", priority: int = 5) -> dict[str, Any]:
    import hashlib
    raw_id = f"{kind}|{title}|{message}|{church_name}"
    notification_id = hashlib.sha1(raw_id.encode("utf-8")).hexdigest()[:24]
    return {
        "id": notification_id,
        "kind": kind,
        "title": title,
        "message": message,
        "church_name": church_name,
        "priority": int(priority),
    }


def _seen_notification_ids(scope: Scope) -> set[str]:
    conn = _connect()
    try:
        rows = conn.execute(
            """
            SELECT notification_id
            FROM area_monitor_seen_notifications
            WHERE user_key = ? AND scope_key = ?
            """,
            (_notification_user_key(), scope.scope_key),
        ).fetchall()
    except Exception:
        rows = []
    finally:
        conn.close()
    return {str(row["notification_id"] or "") for row in rows if str(row["notification_id"] or "")}


def _mark_notifications_seen(scope: Scope, notification_ids: list[str]) -> int:
    clean_ids = []
    for item in notification_ids or []:
        item = str(item or "").strip()
        if item and item not in clean_ids:
            clean_ids.append(item)
    if not clean_ids:
        return 0

    conn = _connect()
    now_iso = _now_ph().isoformat()
    try:
        for notification_id in clean_ids:
            conn.execute(
                """
                INSERT OR IGNORE INTO area_monitor_seen_notifications
                (user_key, scope_key, notification_id, seen_at)
                VALUES (?, ?, ?, ?)
                """,
                (_notification_user_key(), scope.scope_key, notification_id, now_iso),
            )
        conn.commit()
    finally:
        conn.close()
    return len(clean_ids)


def _members_for_scope_from_cache(scope: Scope, churches: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    with closing(_connect()) as conn:
        rows = conn.execute("SELECT * FROM sheet_members_account_cache ORDER BY sheet_row").fetchall()
    members = []
    for member in rows:
        church_name = member["church_id"] or ""
        church_address = member["church_address"] or ""
        church_key = _resolve_church_key(churches, church_name, church_address)
        if not church_key:
            continue
        if str(member["area_number"] or "").strip() and str(member["area_number"] or "").strip() != str(scope.area).strip():
            continue
        members.append({
            "name": str(member["name"] or "").strip(),
            "birthday": str(member["bday"] or "").strip(),
            "church_key": church_key,
            "church_name": churches[church_key]["church_name"],
            "pastor": str(member["pastor"] or "").strip(),
            "username": str(member["username"] or "").strip(),
            "password": str(member["password"] or "").strip(),
            "sheet_row": member["sheet_row"],
        })
    return members


def _member_event_item(event_id: str, kind: str, title: str, message: str, church_name: str = "", priority: int = 5) -> dict[str, Any]:
    return {
        "id": str(event_id or ""),
        "kind": kind,
        "title": title,
        "message": message,
        "church_name": church_name,
        "priority": int(priority),
    }


def _member_account_key(member: dict[str, Any]) -> str:
    username = _normalize_church_key(member.get("username"))
    name = _normalize_church_key(member.get("name"))
    church_key = str(member.get("church_key") or "").strip()
    if username:
        return f"{church_key}|user:{username}"
    return f"{church_key}|name:{name}|row:{member.get('sheet_row', '')}"


def _member_account_signature(member: dict[str, Any]) -> str:
    parts = [
        str(member.get("name") or "").strip(),
        str(member.get("birthday") or "").strip(),
        str(member.get("church_name") or "").strip(),
        str(member.get("pastor") or "").strip(),
        str(member.get("username") or "").strip(),
        str(member.get("password") or "").strip(),
    ]
    return "|".join(parts)


def _member_event_id(scope: Scope, event_type: str, member_key: str, signature: str) -> str:
    import hashlib
    raw = f"{scope.scope_key}|{event_type}|{member_key}|{signature}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:24]


def _record_member_event(scope: Scope, event_type: str, member_key: str, signature: str, title: str, message: str, church_name: str) -> None:
    event_id = _member_event_id(scope, event_type, member_key, signature)
    conn = _connect()
    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO area_monitor_member_events
            (event_id, scope_key, event_type, title, message, church_name, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (event_id, scope.scope_key, event_type, title, message, church_name, _now_ph().isoformat()),
        )
        conn.commit()
    finally:
        conn.close()


def _detect_member_account_changes(scope: Scope, members: list[dict[str, Any]]) -> None:
    """Detect create/edit/delete events for Members Account without breaking old data.

    First run only creates a snapshot so existing accounts do not flood the bell.
    Later changes become notification events.
    """
    now_iso = _now_ph().isoformat()
    current: dict[str, dict[str, Any]] = {}
    for member in members:
        key = _member_account_key(member)
        if key.strip():
            current[key] = member

    conn = _connect()
    try:
        rows = conn.execute(
            """
            SELECT member_key, name, birthday, church_name, username, pastor, signature
            FROM area_monitor_member_snapshot
            WHERE scope_key = ?
            """,
            (scope.scope_key,),
        ).fetchall()
        previous = {str(row["member_key"] or ""): row for row in rows}

        # First run: seed snapshot only. No notifications for old existing accounts.
        if not previous:
            for key, member in current.items():
                conn.execute(
                    """
                    INSERT OR REPLACE INTO area_monitor_member_snapshot
                    (scope_key, member_key, name, birthday, church_name, username, pastor, signature, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        scope.scope_key,
                        key,
                        str(member.get("name") or "").strip(),
                        str(member.get("birthday") or "").strip(),
                        str(member.get("church_name") or "").strip(),
                        str(member.get("username") or "").strip(),
                        str(member.get("pastor") or "").strip(),
                        _member_account_signature(member),
                        now_iso,
                    ),
                )
            conn.commit()
            return

        for key, member in current.items():
            signature = _member_account_signature(member)
            old = previous.get(key)
            church_name = str(member.get("church_name") or "").strip()
            member_name = str(member.get("name") or member.get("username") or "Member").strip()
            if not old:
                title = "Member account created"
                message = f"A member account was created for {member_name} under {church_name}."
                conn.execute(
                    """
                    INSERT OR IGNORE INTO area_monitor_member_events
                    (event_id, scope_key, event_type, title, message, church_name, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (_member_event_id(scope, "member_created", key, signature), scope.scope_key, "member_created", title, message, church_name, now_iso),
                )
            elif str(old["signature"] or "") != signature:
                title = "Member account edited"
                message = f"The member account for {member_name} under {church_name} was edited."
                conn.execute(
                    """
                    INSERT OR IGNORE INTO area_monitor_member_events
                    (event_id, scope_key, event_type, title, message, church_name, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (_member_event_id(scope, "member_edited", key, signature), scope.scope_key, "member_edited", title, message, church_name, now_iso),
                )

            conn.execute(
                """
                INSERT OR REPLACE INTO area_monitor_member_snapshot
                (scope_key, member_key, name, birthday, church_name, username, pastor, signature, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    scope.scope_key,
                    key,
                    str(member.get("name") or "").strip(),
                    str(member.get("birthday") or "").strip(),
                    church_name,
                    str(member.get("username") or "").strip(),
                    str(member.get("pastor") or "").strip(),
                    signature,
                    now_iso,
                ),
            )

        current_keys = set(current.keys())
        for key, old in previous.items():
            if key in current_keys:
                continue
            old_name = str(old["name"] or old["username"] or "Member").strip()
            old_church = str(old["church_name"] or "").strip()
            old_signature = str(old["signature"] or "")
            title = "Member account deleted"
            message = f"The member account for {old_name} under {old_church} was deleted."
            conn.execute(
                """
                INSERT OR IGNORE INTO area_monitor_member_events
                (event_id, scope_key, event_type, title, message, church_name, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (_member_event_id(scope, "member_deleted", key, old_signature), scope.scope_key, "member_deleted", title, message, old_church, now_iso),
            )
            conn.execute(
                "DELETE FROM area_monitor_member_snapshot WHERE scope_key = ? AND member_key = ?",
                (scope.scope_key, key),
            )

        conn.commit()
    finally:
        conn.close()


def _member_account_event_notifications(scope: Scope) -> list[dict[str, Any]]:
    conn = _connect()
    try:
        rows = conn.execute(
            """
            SELECT event_id, event_type, title, message, church_name, created_at
            FROM area_monitor_member_events
            WHERE scope_key = ?
            ORDER BY created_at DESC
            LIMIT 100
            """,
            (scope.scope_key,),
        ).fetchall()
    except Exception:
        rows = []
    finally:
        conn.close()

    items: list[dict[str, Any]] = []
    for row in rows:
        event_type = str(row["event_type"] or "member").strip()
        priority = 2 if event_type in {"member_created", "member_edited", "member_deleted"} else 5
        items.append(_member_event_item(
            str(row["event_id"] or ""),
            "member",
            str(row["title"] or "Member account"),
            str(row["message"] or "Member account activity."),
            str(row["church_name"] or ""),
            priority,
        ))
    return items


def _parse_login_datetime(value: Any) -> datetime | None:
    s = str(value or "").strip()
    if not s:
        return None

    for parser in (
        lambda x: datetime.fromisoformat(x.replace("Z", "+00:00")),
        lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S"),
        lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M"),
        lambda x: datetime.strptime(x, "%m/%d/%Y %H:%M:%S"),
        lambda x: datetime.strptime(x, "%m/%d/%Y %H:%M"),
    ):
        try:
            dt = parser(s)
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc).astimezone(PH_TZ)
            return dt.astimezone(PH_TZ)
        except Exception:
            continue

    return None


def _format_login_time_ph(value: Any) -> str:
    dt = _parse_login_datetime(value)
    if not dt:
        return str(value or "").strip() or "recently"
    today = _now_ph().date()
    if dt.date() == today:
        return dt.strftime("today at %I:%M %p")
    return dt.strftime("%b %d, %Y at %I:%M %p")


def _pastor_login_notifications(scope: Scope, churches: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    """Pastor login notifications from app.py's dedicated user_login_events table.

    This reads only real login events, not page visit logs, so AO selecting a
    pastor in Pastor Tool will not be counted as pastor login.
    """
    church_by_username = {
        str(church.get("username") or "").strip().lower(): church
        for church in churches.values()
        if str(church.get("username") or "").strip()
    }

    if not church_by_username:
        return []

    conn = _connect()
    params: list[Any] = [scope.area]
    sub_sql = ""
    if scope.is_sub_area:
        sub_sql = " AND TRIM(COALESCE(sub_area,'')) = TRIM(?)"
        params.append(scope.sub_area)

    try:
        rows = conn.execute(
            f"""
            SELECT id, username, full_name, role, church_id, church_address,
                   area_number, sub_area, created_at
            FROM user_login_events
            WHERE LOWER(TRIM(COALESCE(role,''))) = 'pastor'
              AND TRIM(COALESCE(area_number,'')) = TRIM(?)
              {sub_sql}
            ORDER BY datetime(created_at) DESC, id DESC
            LIMIT 40
            """,
            tuple(params),
        ).fetchall()
    except Exception:
        rows = []
    finally:
        conn.close()

    items: list[dict[str, Any]] = []
    seen_usernames: set[str] = set()

    for row in rows:
        username = str(row["username"] or "").strip().lower()
        if not username:
            continue

        # Avoid flooding: show latest login per pastor only.
        if username in seen_usernames:
            continue
        seen_usernames.add(username)

        church = church_by_username.get(username)
        if not church:
            continue

        pastor_name = str(row["full_name"] or church.get("pastor_name") or username).strip()
        church_name = str(church.get("church_name") or row["church_id"] or "").strip()
        login_time = _format_login_time_ph(row["created_at"])

        # Stable per real login row; once seen it disappears.
        notification_id = f"pastor-login-{row['id']}"

        items.append(_member_event_item(
            notification_id,
            "login",
            "Pastor logged in",
            f"Pastor {pastor_name} of {church_name} logged in {login_time}.",
            church_name,
            2,
        ))

    return items


def _notification_payload(scope: Scope, churches: dict[str, dict[str, Any]]) -> dict[str, Any]:
    today = _now_ph().date()
    items: list[dict[str, Any]] = []

    # Real pastor login notifications from app.py user_login_events.
    items.extend(_pastor_login_notifications(scope, churches))

    # Reporting notifications for the latest completed month.
    latest = _latest_completed_month(today)
    reporting = _compute_reporting_status(scope, churches, latest.year, latest.month)
    for row in reporting.get("rows", []):
        status = row.get("status")
        church_name = row.get("church_name", "")
        if status == "Late":
            items.append(_notification_item("reporting", "Late report", f"{church_name} submitted late for {reporting['month_label']}.", church_name, 1))
        elif status in {"Early", "On Time", "Reported"}:
            items.append(_notification_item("reporting", "Report submitted", f"{church_name} has a report status of {status} for {reporting['month_label']}.", church_name, 4))

    # Pastor birthdays from Accounts cache.
    for church in churches.values():
        days = None
        bday = ""
        conn = _connect()
        try:
            acc = conn.execute(
                """
                SELECT birthday FROM sheet_accounts_cache
                WHERE TRIM(username)=TRIM(?)
                LIMIT 1
                """,
                (church.get("username", ""),),
            ).fetchone()
            bday = str(acc["birthday"] or "").strip() if acc else ""
        finally:
            conn.close()
        days = _birthday_days_until(bday, today)
        if days is not None and days <= 7:
            label = "today" if days == 0 else f"in {days} day(s)"
            items.append(_notification_item("birthday", "Pastor birthday", f"Pastor {church['pastor_name']} of {church['church_name']} has a birthday {label}.", church["church_name"], 2))

    # Member birthdays plus created/edited/deleted account notifications from Members Account sheet.
    members = _members_for_scope_from_cache(scope, churches)
    _detect_member_account_changes(scope, members)
    items.extend(_member_account_event_notifications(scope))
    for member in members:
        days = _birthday_days_until(member.get("birthday"), today)
        if days is not None and days <= 7:
            label = "today" if days == 0 else f"in {days} day(s)"
            items.append(_notification_item("birthday", "Member birthday", f"{member.get('name')} of {member.get('church_name')} has a birthday {label}.", member.get("church_name", ""), 3))

    # Prayer request notifications from local cache.
    conn = _connect()
    try:
        prayer_rows = conn.execute(
            """
            SELECT * FROM sheet_prayer_request_cache
            ORDER BY sheet_row DESC
            LIMIT 200
            """
        ).fetchall()
    except Exception:
        prayer_rows = []
    finally:
        conn.close()

    for row in prayer_rows:
        church_name = str(row["church_name"] or "").strip()
        church_key = _resolve_church_key(churches, church_name, "")
        if not church_key:
            continue
        status = str(row["status"] or "").strip().lower()
        if status in {"answered", "done", "closed"}:
            continue
        title = str(row["title"] or "Prayer Request").strip() or "Prayer Request"
        submitted_by = str(row["submitted_by"] or "Someone").strip()
        items.append(_notification_item("prayer", "Prayer request", f"{submitted_by} submitted: {title}", churches[church_key]["church_name"], 2))

    seen_ids = _seen_notification_ids(scope)
    items = [item for item in items if str(item.get("id") or "") not in seen_ids]
    items.sort(key=lambda x: (x.get("priority", 9), x.get("kind", ""), x.get("church_name", "")))
    visible = items[:30]
    return {
        "count": len(items),
        "items": visible,
        "has_more": len(items) > len(visible),
    }

def _dashboard_payload(scope: Scope, period: str, church_filter: str, finance_metric: str, attendance_category: str, ministry_metric: str, selected_month: str | None = None, reporting_month: str | None = None) -> dict[str, Any]:
    period = period if period in ("this_month", "6_months", "1_year") else "this_month"
    churches = _fetch_scope_churches(scope)
    if not churches:
        return {"scope": {"area": scope.area, "sub_area": scope.sub_area, "role": scope.role}, "empty": True, "message": "No churches were found in this AO scope.", "selected_month": _parse_selected_month(selected_month).strftime("%Y-%m"), "month_options": _month_options(), "reporting_month": _parse_selected_month(reporting_month or selected_month).strftime("%Y-%m"), "insight_details": []}
    finance_metric = finance_metric if finance_metric in FINANCE_FIELDS else "amount_to_send"
    attendance_category = attendance_category if attendance_category in ATTENDANCE_FIELDS else "all"
    ministry_metric = ministry_metric if ministry_metric in MINISTRY_FIELDS else "received_jesus"
    months = _month_window(period, selected_month=selected_month)
    selected_month_value = f"{months[-1][0]:04d}-{months[-1][1]:02d}" if months else _parse_selected_month(selected_month).strftime("%Y-%m")
    month_labels = [_month_label(y, m) for y, m in months]
    report_rows = _fetch_report_rows(scope, months, churches)
    monthly = _rollup_monthly(report_rows, churches)
    labels_map = {k: v["church_name"] for k, v in churches.items()}
    colors_map = {k: v["color"] for k, v in churches.items()}

    legend = [{"church_key": k, "church_name": v["church_name"], "color": v["color"]} for k, v in sorted(churches.items(), key=lambda item: item[1]["church_name"].lower())]
    report_month = _parse_selected_month(reporting_month or selected_month_value)
    reporting = _compute_reporting_status(scope, churches, report_month.year, report_month.month)
    insight_details = _area_insight_details(scope, churches, selected_month_value, reporting)

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
            "selected_month": selected_month_value,
            "month_options": _month_options(),
            "reporting_month": report_month.strftime("%Y-%m"),
            "insight_details": insight_details,
            "church_detail": _church_detail_payload(scope, church_key, period, finance_metric, attendance_category, ministry_metric, selected_month),
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

    insights = [item['message'] for item in insight_details]

    # Use this selection only; Chart.js rounds the scale up to readable tick values.
    finance_ceiling = max(1.0, max((item["value"] for item in finance_ranked["all"]), default=0.0))
    attendance_ceiling = _upsert_ceiling(scope, f"attendance:{attendance_category}", max([x["value"] for x in attendance_ranked["all"]] or [0]))
    ministry_ceiling = _upsert_ceiling(scope, f"ministry:{ministry_metric}", max([x["value"] for x in ministry_ranked["all"]] or [0]))

    return {
        "scope": {"area": scope.area, "sub_area": scope.sub_area, "role": scope.role},
        "empty": False,
        "detail_view": False,
        "server_time": _now_ph().isoformat(),
        "last_sync_display": get_last_sync_display_ph(),
        "period": period,
        "selected_month": selected_month_value,
        "month_options": _month_options(),
        "reporting_month": report_month.strftime("%Y-%m"),
        "insight_details": insight_details,
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


def _church_detail_payload(scope: Scope, church_key: str, period: str = "this_month", finance_metric: str = "amount_to_send", attendance_category: str = "all", ministry_metric: str = "received_jesus", selected_month: str | None = None) -> dict[str, Any]:
    churches = _fetch_scope_churches(scope)
    if church_key not in churches:
        abort(404)
    months = _month_window(period, selected_month=selected_month)
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
        "selected_month": f"{months[-1][0]:04d}-{months[-1][1]:02d}" if months else _parse_selected_month(selected_month).strftime("%Y-%m"),
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


PAGE_HTML = r'''
{% extends "base.html" %}

{% block title %}Area Progress Monitor{% endblock %}

{% block content %}
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
#areaProgressMonitor{--card:#fff;--line:#dce7f1;--text:#17324d;--muted:#60758a;--brand:#1674bc;--shadow:0 8px 28px #17324d0a;color:var(--text);min-width:0;font-family:system-ui,-apple-system,"Segoe UI",sans-serif}
#areaProgressMonitor *{box-sizing:border-box}#areaProgressMonitor .wrap{width:100%;max-width:1200px;margin:0 auto;padding:8px 0 32px}
#areaProgressMonitor .card{background:var(--card);border:1px solid var(--line);border-radius:20px;box-shadow:var(--shadow);min-width:0}#areaProgressMonitor .section{padding:18px;margin-bottom:16px}
#areaProgressMonitor .back-row{display:flex;justify-content:space-between;gap:12px;align-items:center;flex-wrap:wrap;margin-bottom:16px}#areaProgressMonitor .muted-chip{font-size:12px;color:var(--muted);overflow-wrap:anywhere}
#areaProgressMonitor .apm-hero-card{background:linear-gradient(120deg,#123856,#176a9f);border-radius:22px;padding:24px;color:#fff;margin-bottom:18px;overflow:hidden;position:relative}
#areaProgressMonitor .apm-hero-content{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:14px;align-items:start}#areaProgressMonitor h1{color:#fff;font-size:clamp(23px,4vw,32px);line-height:1.15;margin:0 0 10px;letter-spacing:-.035em;overflow-wrap:anywhere}
#areaProgressMonitor .subtitle{color:var(--muted);font-size:13px;line-height:1.5;overflow-wrap:anywhere}#areaProgressMonitor .apm-hero-card .subtitle{color:#d6edf9}
#areaProgressMonitor button,#areaProgressMonitor select,#areaProgressMonitor textarea,#areaProgressMonitor .pill{font:inherit;color:var(--text);border:1px solid var(--line);border-radius:12px;background:#fff;min-height:44px;max-width:100%}
#areaProgressMonitor button,#areaProgressMonitor .pill{padding:10px 14px;cursor:pointer;text-decoration:none}#areaProgressMonitor button:hover,#areaProgressMonitor .pill:hover{border-color:#6a9cc0;background:#f0f7fc}
#areaProgressMonitor button:focus-visible,#areaProgressMonitor select:focus-visible,#areaProgressMonitor a:focus-visible{outline:3px solid #e6b44a;outline-offset:3px}#areaProgressMonitor .pill.active{background:#176a9f;color:#fff;border-color:#176a9f}
#areaProgressMonitor select,#areaProgressMonitor textarea{width:100%;padding:10px 12px;font-size:16px}#areaProgressMonitor textarea{min-height:110px;resize:vertical}
#areaProgressMonitor .insights-trigger{display:flex;gap:8px;align-items:center;font-size:14px;font-weight:700;white-space:nowrap;box-shadow:0 4px 12px #102d4920}
#areaProgressMonitor .toolbar{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:16px;align-items:start}#areaProgressMonitor .control{display:grid;gap:7px;min-width:0;font-size:13px;font-weight:650}
#areaProgressMonitor #timePills{min-width:0}#areaProgressMonitor .trend-options{margin-top:10px;color:var(--muted);font-size:13px}#areaProgressMonitor summary{cursor:pointer;padding:8px 0}#areaProgressMonitor .trend-options .filters{margin-top:8px}
#areaProgressMonitor .filters{display:flex;gap:8px;flex-wrap:nowrap;overflow-x:auto;max-width:100%;padding-bottom:6px;align-items:center}#areaProgressMonitor .filters button{flex-shrink:0;font-size:13px}
#areaProgressMonitor .legend{display:flex;gap:8px;flex-wrap:wrap}#areaProgressMonitor .legend-item{display:inline-flex;gap:7px;align-items:center;font-size:12px;color:var(--muted);max-width:100%;overflow-wrap:anywhere}#areaProgressMonitor .swatch{width:10px;height:10px;border-radius:50%;flex-shrink:0}
#areaProgressMonitor .section-head{display:flex;justify-content:space-between;align-items:start;gap:12px;flex-wrap:wrap;margin-bottom:14px}#areaProgressMonitor .section-title{font-size:19px;font-weight:750;line-height:1.35;overflow-wrap:anywhere}
#areaProgressMonitor .grid-2{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:16px}#areaProgressMonitor .grid-2>.section{margin-bottom:0}#areaProgressMonitor .inline-actions{display:flex;flex-wrap:wrap;gap:8px;align-items:center}
#areaProgressMonitor .chart-box{position:relative;width:100%;height:340px;min-width:0}#areaProgressMonitor canvas{max-width:100%}#areaProgressMonitor .stat-grid{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:10px;margin:16px 0}
#areaProgressMonitor .stat{padding:12px;border:1px solid var(--line);border-radius:14px;background:#f8fbfe;min-width:0}#areaProgressMonitor .stat-label{color:var(--muted);font-size:12px;margin-bottom:6px}#areaProgressMonitor .stat-value{font-size:26px;font-weight:750}
#areaProgressMonitor .table-wrap{max-width:100%;overflow-x:auto}#areaProgressMonitor table{width:100%;border-collapse:collapse;font-size:14px}#areaProgressMonitor th,#areaProgressMonitor td{padding:12px 10px;border-bottom:1px solid var(--line);text-align:left;vertical-align:middle;overflow-wrap:anywhere}#areaProgressMonitor th{font-size:12px;color:var(--muted);font-weight:650}
#areaProgressMonitor .church-link{border:0;background:transparent;padding:4px 0;text-align:left;font-weight:650;white-space:normal;overflow-wrap:anywhere}#areaProgressMonitor .status-pill{display:inline-flex;padding:6px 10px;border-radius:30px;font-size:12px;font-weight:700;white-space:nowrap}
#areaProgressMonitor .status-Early{background:#e8f7ed;color:#17683c}#areaProgressMonitor .status-On-Time{background:#e6f0fb;color:#235b91}#areaProgressMonitor .status-Late{background:#fff0dc;color:#8c4a00}#areaProgressMonitor .status-Reported{background:#edf0f4;color:#43576a}#areaProgressMonitor .status-No-Report{background:#fdebed;color:#a43141}
#areaProgressMonitor .remarks-grid,#areaProgressMonitor .insights{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:12px}#areaProgressMonitor .insight{padding:14px;border-radius:14px;background:#edf6fc;font-size:14px;line-height:1.6}
#areaProgressMonitor .modal{position:fixed;inset:0;background:#102b4a88;display:none;z-index:2000;padding:16px;align-items:center;justify-content:center}#areaProgressMonitor .modal.open{display:flex}#areaProgressMonitor .modal-panel{width:100%;max-width:1100px;max-height:calc(100dvh - 32px);overflow:auto;overscroll-behavior:contain;background:#f3f7fb;border-radius:20px;padding:16px;min-width:0}
#areaProgressMonitor dialog{width:min(760px,calc(100vw - 24px));max-height:calc(100dvh - 32px);padding:22px;border:1px solid var(--line);border-radius:22px;background:#fff;color:var(--text);overflow:auto;overscroll-behavior:contain;box-shadow:0 22px 70px #102b4a33}#areaProgressMonitor dialog::backdrop{background:#102b4a99}
#areaProgressMonitor .dialog-head{display:flex;justify-content:space-between;align-items:start;gap:14px;margin-bottom:16px}#areaProgressMonitor h2{font-size:23px;line-height:1.25;margin:0 0 6px}#areaProgressMonitor .close-dialog{font-size:22px;line-height:1;padding:8px;min-width:44px;flex-shrink:0}
#areaProgressMonitor .insight-list{display:grid;gap:12px}#areaProgressMonitor .insight-message{display:grid;gap:8px;width:100%;padding:16px;text-align:left;line-height:1.5;background:#f7fbfe}#areaProgressMonitor .insight-category{font-size:11px;font-weight:750;letter-spacing:.08em;text-transform:uppercase;color:#176a9f}#areaProgressMonitor .insight-message strong{font-size:16px}#areaProgressMonitor .insight-message span:last-child{font-size:13px;color:var(--muted)}
#areaProgressMonitor .insight-explanation{font-size:14px;line-height:1.7;color:var(--muted);margin:12px 0 20px}#areaProgressMonitor .insight-detail-rows{display:grid;gap:12px}#areaProgressMonitor .insight-detail-card{border:1px solid var(--line);border-radius:16px;padding:16px;min-width:0}#areaProgressMonitor .insight-facts{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px;margin:12px 0 0}#areaProgressMonitor .insight-facts dt{font-size:12px;color:var(--muted)}#areaProgressMonitor .insight-facts dd{margin:4px 0 0;font-size:16px;font-weight:650;overflow-wrap:anywhere}
#areaProgressMonitor .loading-overlay{position:fixed;inset:0;display:grid;place-items:center;background:#f4f8fcf5;z-index:5000}#areaProgressMonitor .loading-box{width:min(420px,88vw);padding:24px;background:#fff;border-radius:20px;border:1px solid var(--line);text-align:center}#areaProgressMonitor .loading-title{font-size:18px;font-weight:700;margin-bottom:10px}#areaProgressMonitor .progress-track{height:8px;background:#e5edf5;border-radius:8px;overflow:hidden;margin-top:16px}#areaProgressMonitor .progress-bar{height:100%;width:0;background:#1674bc;transition:width .2s}#areaProgressMonitor .loader{padding:24px;text-align:center;color:var(--muted)}#areaProgressMonitor .hidden{display:none!important}
@media(max-width:760px){ #areaProgressMonitor .toolbar,#areaProgressMonitor .grid-2,#areaProgressMonitor .remarks-grid,#areaProgressMonitor .insights{grid-template-columns:minmax(0,1fr)}#areaProgressMonitor .section{padding:14px}#areaProgressMonitor .apm-hero-card{padding:18px}#areaProgressMonitor .insights-trigger{padding:10px;font-size:13px}#areaProgressMonitor .insights-prefix{display:none}#areaProgressMonitor .stat-grid{grid-template-columns:repeat(2,minmax(0,1fr))}#areaProgressMonitor .stat:last-child{grid-column:1/-1}#areaProgressMonitor .modal{padding:8px}#areaProgressMonitor .modal-panel{padding:10px;max-height:calc(100dvh - 16px)}#areaProgressMonitor dialog{padding:16px}#areaProgressMonitor .section-head>.control{width:100%}
#areaProgressMonitor .reporting-table thead{display:none}#areaProgressMonitor .reporting-table,#areaProgressMonitor .reporting-table tbody{display:block}#areaProgressMonitor .reporting-table tbody{display:grid;gap:12px}#areaProgressMonitor .reporting-table tr{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:10px;border:1px solid var(--line);border-radius:16px;padding:14px}#areaProgressMonitor .reporting-table td{padding:0;border:0;display:block}#areaProgressMonitor .reporting-table td:first-child,#areaProgressMonitor .reporting-table td:nth-child(2){grid-column:1/-1}#areaProgressMonitor .reporting-table td:nth-child(2){display:flex;align-items:center;justify-content:space-between}#areaProgressMonitor .reporting-table td:not(:first-child)::before{content:attr(data-label);display:block;font-size:11px;color:var(--muted);margin-bottom:5px}#areaProgressMonitor .reporting-table td:first-child button{font-size:16px}#areaProgressMonitor .reporting-table td:nth-child(2)::before{margin:0}}
  </style>
<section id="areaProgressMonitor">
<div class="loading-overlay" id="loadingOverlay"><div class="loading-box"><div class="loading-title">Loading Area Progress Monitor</div><div class="subtitle">Preparing the selected month's progress…</div><div class="progress-track"><div class="progress-bar" id="progressBar"></div></div></div></div>
<div class="wrap">
  <div class="back-row"><a href="{{ ao_tool_url }}" class="pill">← AO Tool</a><div class="muted-chip" id="lastSyncChip">Last sync: loading…</div></div>
  <div class="apm-hero-card"><div class="apm-hero-content"><div><h1>Area Progress Monitor</h1><div class="subtitle" id="scopeLabel">Loading scope…</div></div>
    <button type="button" class="insights-trigger" id="insightsBtn" aria-haspopup="dialog" aria-controls="insightsDialog"><span aria-hidden="true">↗</span><span><span class="insights-prefix">Area </span>Insights</span></button>
  </div></div>
  <div class="card section"><div class="toolbar"><label class="control" for="churchFilter">Church<select id="churchFilter" aria-label="Church"></select></label><div id="timePills"></div></div>
    <details class="trend-options"><summary>Church colour key</summary><div class="legend" id="globalLegend"></div></details>
  </div>
  <div id="loader" class="loader card section" role="status">Loading monthly progress…</div><div id="content" class="hidden"></div>
</div>
<div class="modal" id="chartModal" role="dialog" aria-modal="true" aria-labelledby="modalTitle"><div class="modal-panel"><div class="section card"><div class="section-head"><div><div class="section-title" id="modalTitle">All Churches</div><div class="subtitle" id="modalSubtitle"></div></div><button class="pill" id="closeChartModal">Close</button></div><div class="filters" id="modalFilterBar"></div><div class="chart-box"><canvas id="modalChart"></canvas></div><div class="legend" id="modalLegend"></div></div></div></div>
<div class="modal" id="churchModal" role="dialog" aria-modal="true" aria-label="Church progress"><div class="modal-panel" id="churchModalBody"></div></div>
<dialog id="insightsDialog" aria-labelledby="insightsTitle"><div class="dialog-head"><div><h2 id="insightsTitle">Area Insights</h2><div class="subtitle" id="insightsPeriod"></div></div><button type="button" class="close-dialog" data-close-dialog="insightsDialog" aria-label="Close Area Insights">×</button></div><p class="subtitle">Choose a message to see the churches, figures, and explanation behind it.</p><div class="insight-list" id="insightsList"></div></dialog>
<dialog id="insightDetailDialog" aria-labelledby="insightDetailTitle"><div class="dialog-head"><div><h2 id="insightDetailTitle"></h2><div class="subtitle" id="insightDetailPeriod"></div></div><button type="button" class="close-dialog" data-close-dialog="insightDetailDialog" aria-label="Close insight details">×</button></div><div id="insightDetailBody"></div></dialog>
</section>
<script>
const state={period:'this_month',selectedMonth:'',reportingMonth:'',reportingPinned:false,detailMonth:'',requestVersion:0,churchFilter:'all',financeMetric:'amount_to_send',attendanceCategory:'all',ministryMetric:'received_jesus',payload:null,charts:{},modalChart:null,currentAnchor:null};
const labels={periods:{'6_months':'6 Months','1_year':'1 Year'},finance:{{ finance_options|tojson }},ministry:{{ ministry_options|tojson }},attendance:{{ attendance_options|tojson }}};
function monthOptions(selected){return ((state.payload&&state.payload.month_options)||[]).map(opt=>`<option value="${esc(opt.value)}" ${opt.value===selected?'selected':''}>${esc(opt.label)}</option>`).join('')}
function renderInsights(payload){const details=payload.insight_details||[];document.getElementById('insightsPeriod').textContent=(payload.month_labels||payload.church_detail?.month_labels||[]).join(' · ');document.getElementById('insightsList').innerHTML=details.map((item,index)=>`<button type="button" class="insight-message" data-insight="${index}"><span class="insight-category">${esc(item.category)}</span><strong>${esc(item.title)}</strong><span>${esc(item.message)}<br>View details →</span></button>`).join('')||'<p class="subtitle">No area insights are available for this selection.</p>'}
function showInsight(index){const item=state.payload?.insight_details?.[Number(index)];if(!item)return;document.getElementById('insightDetailTitle').textContent=item.title;document.getElementById('insightDetailPeriod').textContent=item.period;document.getElementById('insightDetailBody').innerHTML=`<p>${esc(item.message)}</p><p class="insight-explanation">${esc(item.explanation)}</p><div class="insight-detail-rows">${(item.rows||[]).map(row=>`<article class="insight-detail-card"><button type="button" class="church-link" data-church="${esc(row.church_key)}" data-month="${esc(row.month)}">${esc(row.church_name)} →</button><dl class="insight-facts">${Object.entries(row.values||{}).map(([label,value])=>`<div><dt>${esc(label)}</dt><dd>${esc(typeof value==='number'?value.toLocaleString('en-PH',{maximumFractionDigits:2}):value)}</dd></div>`).join('')}</dl></article>`).join('')}</div>`;document.getElementById('insightDetailDialog').showModal()}
function detailMonthFilterHtml(selected,modal){return `<label class="control">Progress month<select data-detail-month="1" data-modal="${modal?'1':'0'}">${monthOptions(selected)}</select></label>`}
function reportingPanelHtml(reporting){return `<section class="card section" id="reportingSection"><div class="section-head"><div><div class="section-title">Church Reporting</div><div class="subtitle">${esc(reporting.month_label)} · Deadline ${esc(reporting.deadline_label)}</div></div><label class="control" for="reportingMonthSelect">Reporting month<select id="reportingMonthSelect">${monthOptions(state.reportingMonth)}</select></label></div><div class="stat-grid">${['Early','On Time','Late','Reported','No Report'].map(status=>`<div class="stat"><div class="stat-label">${status}</div><div class="stat-value">${reporting.counts[status]||0}</div></div>`).join('')}</div><div class="table-wrap"><table class="reporting-table"><thead><tr><th>Church</th><th>Status</th><th>Date submitted</th><th>Time submitted</th></tr></thead><tbody>${(reporting.rows||[]).map(row=>`<tr><td data-label="Church"><button type="button" class="church-link" data-church="${esc(row.church_key)}" data-reporting="1">${esc(row.church_name)}</button></td><td data-label="Status"><span class="status-pill status-${esc(String(row.status).replace(/\s+/g,'-'))}">${esc(row.status)}</span></td><td data-label="Date submitted">${esc(row.date_submitted||'Not recorded')}</td><td data-label="Time submitted">${esc(row.time_submitted||'Not recorded')}</td></tr>`).join('')}</tbody></table></div><a class="pill" style="display:inline-block;margin-top:14px" href="{{ church_status_url }}?year=${encodeURIComponent(state.reportingMonth.split('-')[0])}&month=${encodeURIComponent(Number(state.reportingMonth.split('-')[1]))}">Open Church Status →</a></section>`}
function handleMetricClick(event){const target=event.target.closest('button');if(!target)return;const expanded=target.getAttribute('data-expand');if(expanded)return openExpandModal(expanded);if(target.id==='saveRemarksBtn')return saveRemarks(target.getAttribute('data-church'));const church=target.getAttribute('data-church');if(church){for(const id of ['insightDetailDialog','insightsDialog'])document.getElementById(id).close();return openChurchModal(church,target.getAttribute('data-month')||(target.hasAttribute('data-reporting')?state.reportingMonth:state.selectedMonth))}for(const [attribute,key,anchor] of [['finance','financeMetric','financeSection'],['attendance','attendanceCategory','attendanceSection'],['ministry','ministryMetric','ministrySection']]){const value=target.getAttribute('data-'+attribute);if(!value)continue;state[key]=value;if(target.hasAttribute('data-detail')){const current=document.querySelector('#churchModal.open #saveRemarksBtn')?.getAttribute('data-church');if(current)return openChurchModal(current);return refreshData(false)}if(target.closest('#modalFilterBar'))return refreshData(false).then(()=>openExpandModal(attribute==='finance'?'finances':attribute));rememberAnchor(anchor);return refreshData(false)}}
function esc(text){return String(text??'').replace(/[&<>\"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]))}
function peso(value){return new Intl.NumberFormat('en-PH',{style:'currency',currency:'PHP',maximumFractionDigits:2}).format(Number(value||0))}
function setLoader(on){document.getElementById('loader').classList.toggle('hidden',!on);document.getElementById('content').classList.toggle('hidden',on)}
function setOverlayProgress(pct){document.getElementById('progressBar').style.width=`${pct}%`} function hideOverlay(){const overlay=document.getElementById('loadingOverlay');if(overlay)overlay.style.display='none'}
function rememberAnchor(id){state.currentAnchor=id||null} function restoreAnchor(){if(!state.currentAnchor)return;const el=document.getElementById(state.currentAnchor);if(el)setTimeout(()=>el.scrollIntoView({behavior:'smooth',block:'start'}),30)}
function populateChurchFilter(payload){const el=document.getElementById('churchFilter');let html='<option value="all">All Churches</option>';(payload.churches||[]).slice().sort((a,b)=>a.church_name.localeCompare(b.church_name)).forEach(ch=>{html+=`<option value="${esc(ch.church_key)}" ${ch.church_key===state.churchFilter?'selected':''}>${esc(ch.church_name)}</option>`});el.innerHTML=html}
function populateGlobalFilters(){document.getElementById('timePills').innerHTML=`<label class="control" for="singleMonthSelect">Progress month<select id="singleMonthSelect">${monthOptions(state.selectedMonth)}</select></label><details class="trend-options"><summary>Longer trends</summary><div class="filters">${Object.entries(labels.periods).map(([key,label])=>`<button class="pill ${state.period===key?'active':''}" data-period="${key}">${esc(label)}</button>`).join('')}</div></details>`}
function destroyCharts(){Object.values(state.charts).forEach(chart=>{if(chart&&chart.destroy)chart.destroy()});state.charts={}}
function makeChart(canvasId,type,labelsArr,valuesArr,colorsArr,yMax,currency=false){const ctx=document.getElementById(canvasId);if(!ctx||typeof Chart!=='function')return null;ctx.parentElement.style.height=`${Math.max(260,labelsArr.length*38)}px`;return new Chart(ctx,{type:'bar',data:{labels:labelsArr,datasets:[{data:valuesArr,backgroundColor:colorsArr,borderRadius:5,maxBarThickness:22}]},options:{indexAxis:'y',responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false},tooltip:{callbacks:{label:context=>currency?peso(context.parsed.x):Number(context.parsed.x).toLocaleString('en-PH',{maximumFractionDigits:2})}}},scales:{x:{beginAtZero:true,suggestedMax:yMax,grid:{color:'#edf2f7'},ticks:{maxTicksLimit:5,callback:value=>currency?'₱'+Intl.NumberFormat('en-PH',{notation:'compact'}).format(value):value}},y:{grid:{display:false},ticks:{font:{size:11},callback:function(value){const text=this.getLabelForValue(value);return text.length>23?text.slice(0,22)+'…':text}}}}}})}
function legendHtml(items){return(items||[]).map(item=>`<span class="legend-item"><span class="swatch" style="background:${item.color}"></span>${esc(item.church_name)}</span>`).join('')}
function filterButtonsHtml(kind,isDetail=false){if(kind==='period')return detailMonthFilterHtml(state.selectedMonth,false);const lookup=kind==='finances'?labels.finance:kind==='attendance'?labels.attendance:labels.ministry;const attr=kind==='finances'?'finance':kind;const active=kind==='finances'?state.financeMetric:kind==='attendance'?state.attendanceCategory:state.ministryMetric;return Object.entries(lookup).map(([key,label])=>`<button type="button" class="pill ${active===key?'active':''}" data-${attr}="${key}" ${isDetail?'data-detail="1"':''}>${esc(label)}</button>`).join('')}
function renderDashboard(payload){const content=document.getElementById('content');document.getElementById('scopeLabel').textContent=`Area ${payload.scope?.area||''}${payload.scope?.sub_area?' · '+payload.scope.sub_area:''}`;document.getElementById('globalLegend').innerHTML=legendHtml(payload.legend||[]);document.getElementById('lastSyncChip').textContent=`Last sync: ${payload.last_sync_display||'Unknown'}`;destroyCharts();if(payload.empty){content.innerHTML=`<div class="card section">${esc(payload.message||'No churches found.')}</div>`;return}if(payload.detail_view){renderChurchDetailView(payload.church_detail,false);return}const periodLabel=(payload.month_labels||[]).join(' · ');const cards=[['finances','financeSection','Financial Overview','financeChart'],['attendance','attendanceSection','Attendance Overview','attendanceChart'],['ministry','ministrySection','Ministry Activity','ministryChart']];content.innerHTML=cards.map(([kind,id,title,canvas])=>`<section class="card section" id="${id}"><div class="section-head"><div><div class="section-title">${title}</div><div class="subtitle">${esc(periodLabel)} · ${esc(payload[kind].title)}${kind==='finances'?'':' · average per Sunday'}</div></div><button class="pill" data-expand="${kind}">View all</button></div><div class="filters">${filterButtonsHtml(kind)}</div><div class="chart-box"><canvas id="${canvas}" role="img" aria-label="${title}: top churches for ${esc(periodLabel)}"></canvas></div></section>`).join('')+reportingPanelHtml(payload.reporting);for(const [kind,,,canvas] of cards){const rows=payload[kind].top||[];state.charts[kind]=makeChart(canvas,'bar',rows.map(row=>row.church_name),rows.map(row=>row.value),rows.map(row=>row.color),payload[kind].y_max,kind==='finances')}}
function renderChurchDetailView(detail,modal=false){const target=modal?document.getElementById('churchModalBody'):document.getElementById('content');target.innerHTML=`<div class="card section"><div class="section-head"><div><div class="section-title">${esc(detail.church.church_name)}</div><div class="subtitle">${esc(detail.church.pastor_name||'')} • ${esc(detail.church.church_address||'')}</div></div><div class="inline-actions"><button class="pill ghost" ${modal?'id="closeChurchModal"':'id="closeDetailView"'}>Close</button></div></div><div class="filters" style="margin-bottom:12px;">${detailMonthFilterHtml(detail.selected_month,modal)}</div><div class="grid-2" style="margin-top:14px;"><div><div class="section-title" style="font-size:18px;margin-bottom:8px;">Automatic Insights</div><div class="insights">${(detail.insights||[]).map(text=>`<div class="insight">${esc(text)}</div>`).join('')||'<div class="insight">No insights yet.</div>'}</div></div><div><div class="section-title" style="font-size:18px;margin-bottom:8px;">Flags</div><div class="insights">${(detail.flags||[]).map(text=>`<div class="insight" style="background:#fff4e5;color:#8a4a03;">${esc(text)}</div>`).join('')||'<div class="insight">No flags in the current selection.</div>'}</div></div></div><div class="grid-2" style="margin-top:14px;"><div class="card section"><div class="section-head"><div class="section-title" style="font-size:18px;">Attendance Trend</div></div><div class="filters" style="margin-bottom:12px;">${filterButtonsHtml('attendance',true)}</div><div class="chart-box"><canvas id="detailAttendanceChart"></canvas></div></div><div class="card section"><div class="section-head"><div class="section-title" style="font-size:18px;">Finance Trend</div></div><div class="filters" style="margin-bottom:12px;">${filterButtonsHtml('finances',true)}</div><div class="chart-box"><canvas id="detailFinanceChart"></canvas></div></div></div><div class="card section" style="margin-top:14px;"><div class="section-head"><div class="section-title" style="font-size:18px;">Ministry Activity Trend</div></div><div class="filters" style="margin-bottom:12px;">${filterButtonsHtml('ministry',true)}</div><div class="chart-box"><canvas id="detailMinistryChart"></canvas></div></div><div class="card section" style="margin-top:14px;"><div class="section-title" style="font-size:18px;margin-bottom:8px;">Manual Remarks</div><div class="remarks-grid">${['general','finances','attendance','ministry','reporting'].map(category=>`<div><div class="subtitle" style="margin-bottom:6px;text-transform:capitalize;">${esc(category)}</div><textarea data-remark="${category}" placeholder="Write ${category} remark...">${esc((detail.manual_remarks&&detail.manual_remarks[category])||'')}</textarea><div class="subtitle" style="margin-top:6px;"><strong>Automatic note:</strong> ${esc((detail.automatic_remarks&&detail.automatic_remarks[category])||'--')}</div></div>`).join('')}</div><div class="inline-actions" style="margin-top:12px;"><button class="pill" id="saveRemarksBtn" data-church="${esc(detail.church.church_key)}">Save Remarks</button></div></div></div>`; if(modal){document.getElementById('churchModal').classList.add('open');document.getElementById('closeChurchModal')?.addEventListener('click',()=>document.getElementById('churchModal').classList.remove('open'))} else {document.getElementById('closeDetailView')?.addEventListener('click',()=>{state.churchFilter='all';rememberAnchor(null);refreshData(true)})} setTimeout(()=>{if(typeof Chart!=='function')return;if(state.charts.detailAttendance)state.charts.detailAttendance.destroy();if(state.charts.detailFinance)state.charts.detailFinance.destroy();if(state.charts.detailMinistry)state.charts.detailMinistry.destroy();state.charts.detailAttendance=new Chart(document.getElementById('detailAttendanceChart'),{type:'line',data:{labels:detail.month_labels,datasets:[{data:detail.attendance_series[state.attendanceCategory]||[],borderColor:detail.church.color,backgroundColor:detail.church.color,tension:.28}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}}}});state.charts.detailFinance=new Chart(document.getElementById('detailFinanceChart'),{type:'bar',data:{labels:detail.month_labels,datasets:[{data:detail.finance_series[state.financeMetric]||[],backgroundColor:detail.church.color}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}}}});state.charts.detailMinistry=new Chart(document.getElementById('detailMinistryChart'),{type:'bar',data:{labels:detail.month_labels,datasets:[{data:detail.ministry_series[state.ministryMetric]||[],backgroundColor:detail.church.color}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}}}})},10)}
async function bootSync(){const res=await fetch(`{{ api_base }}/boot`,{method:'POST',credentials:'same-origin'}); if(!res.ok) throw new Error('Failed to prepare latest data'); return await res.json()}
async function fetchPayload(){const params=new URLSearchParams({period:state.period,selected_month:state.selectedMonth,reporting_month:state.reportingMonth,church:state.churchFilter,finance_metric:state.financeMetric,attendance_category:state.attendanceCategory,ministry_metric:state.ministryMetric});const response=await fetch(`{{ api_base }}/snapshot?${params}`,{credentials:'same-origin',cache:'no-store'});if(!response.ok)throw Error('Unable to load progress. Please retry.');return response.json()}
async function refreshData(showLoader=false){const version=++state.requestVersion;if(showLoader)setLoader(true);try{const payload=await fetchPayload();if(version!==state.requestVersion)return;state.payload=payload;if(payload.selected_month)state.selectedMonth=payload.selected_month;if(payload.reporting_month)state.reportingMonth=payload.reporting_month;populateChurchFilter(payload);populateGlobalFilters();renderInsights(payload);renderDashboard(payload);restoreAnchor()}catch(error){if(version===state.requestVersion)document.getElementById('content').innerHTML=`<div class="card section">${esc(error.message)}</div>`}finally{if(version===state.requestVersion)setLoader(false)}}
function openExpandModal(kind){const block=state.payload?.[kind];if(!block)return;document.getElementById('chartModal').classList.add('open');document.getElementById('modalTitle').textContent=kind.charAt(0).toUpperCase()+kind.slice(1)+' — All Churches';document.getElementById('modalSubtitle').textContent=block.title;document.getElementById('modalLegend').innerHTML=legendHtml(block.all||[]);document.getElementById('modalFilterBar').innerHTML=filterButtonsHtml(kind);if(state.modalChart)state.modalChart.destroy();const rows=block.all||[];state.modalChart=makeChart('modalChart','bar',rows.map(row=>row.church_name),rows.map(row=>row.value),rows.map(row=>row.color),block.y_max,kind==='finances')}
async function openChurchModal(churchKey,month=state.detailMonth||state.selectedMonth){state.detailMonth=month;const params=new URLSearchParams({period:'this_month',selected_month:month,finance_metric:state.financeMetric,attendance_category:state.attendanceCategory,ministry_metric:state.ministryMetric});const response=await fetch(`{{ api_base }}/church/${encodeURIComponent(churchKey)}?${params}`,{credentials:'same-origin',cache:'no-store'});if(!response.ok)return alert('Unable to load church progress.');renderChurchDetailView(await response.json(),true)}
async function saveRemarks(churchKey){const body={};document.querySelectorAll('[data-remark]').forEach(el=>{body[el.getAttribute('data-remark')]=el.value});const res=await fetch(`{{ api_base }}/church/${encodeURIComponent(churchKey)}/remarks`,{method:'POST',credentials:'same-origin',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});if(!res.ok)return alert('Failed to save remarks.');alert('Remarks saved.')}
function wireEvents(){document.getElementById('insightsBtn').addEventListener('click',()=>document.getElementById('insightsDialog').showModal());document.getElementById('insightsList').addEventListener('click',event=>{const button=event.target.closest('[data-insight]');if(button)showInsight(button.dataset.insight)});for(const button of document.querySelectorAll('[data-close-dialog]'))button.addEventListener('click',()=>document.getElementById(button.dataset.closeDialog).close());document.getElementById('timePills').addEventListener('click',event=>{const value=event.target.closest('[data-period]')?.dataset.period;if(!value)return;state.period=value;rememberAnchor(null);refreshData(true)});document.getElementById('timePills').addEventListener('change',event=>{if(event.target.id!=='singleMonthSelect')return;state.selectedMonth=event.target.value;state.period='this_month';if(!state.reportingPinned)state.reportingMonth=state.selectedMonth;rememberAnchor(null);refreshData(true)});document.getElementById('churchFilter').addEventListener('change',event=>{state.churchFilter=event.target.value;state.detailMonth='';rememberAnchor(null);refreshData(true)});for(const id of ['content','churchModal','modalFilterBar','insightDetailBody'])document.getElementById(id).addEventListener('click',handleMetricClick);for(const id of ['content','churchModal'])document.getElementById(id).addEventListener('change',event=>{if(event.target.id==='reportingMonthSelect'){state.reportingMonth=event.target.value;state.reportingPinned=true;rememberAnchor('reportingSection');return refreshData(false)}if(event.target.hasAttribute('data-detail-month')){const current=document.querySelector('#churchModal.open #saveRemarksBtn')?.getAttribute('data-church');if(current)return openChurchModal(current,event.target.value);state.selectedMonth=event.target.value;state.period='this_month';if(!state.reportingPinned)state.reportingMonth=state.selectedMonth;return refreshData(false)}});document.getElementById('closeChartModal').addEventListener('click',()=>document.getElementById('chartModal').classList.remove('open'));for(const id of ['chartModal','churchModal'])document.getElementById(id).addEventListener('click',event=>{if(event.target.id===id)event.currentTarget.classList.remove('open')});document.addEventListener('keydown',event=>{if(event.key==='Escape'&&!document.querySelector('#areaProgressMonitor dialog[open]'))for(const id of ['chartModal','churchModal'])document.getElementById(id).classList.remove('open')})}
(async function init(){wireEvents();populateGlobalFilters();setOverlayProgress(10);try{setOverlayProgress(35);await bootSync();setOverlayProgress(70);await refreshData(true);setOverlayProgress(100)}catch(e){console.error(e);await refreshData(true);setOverlayProgress(100)}setTimeout(()=>hideOverlay(),250)})();
</script>
{% endblock %}
'''


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
    sync_from_sheets_if_needed()
    return jsonify({"ok": True, "last_sync_display": get_last_sync_display_ph()})


@bp.route("/api/ao-tool/area-progress-monitor/snapshot")
def area_progress_monitor_snapshot():
    scope = _require_ao()
    payload = _dashboard_payload(
        scope=scope,
        period=str(request.args.get("period") or "this_month"),
        selected_month=str(request.args.get("selected_month") or ""),
        reporting_month=str(request.args.get("reporting_month") or ""),
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
        period=str(request.args.get("period") or "this_month"),
        selected_month=str(request.args.get("selected_month") or ""),
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


@bp.route("/api/ao-tool/area-progress-monitor/notifications/seen", methods=["POST"])
def area_progress_monitor_notifications_seen():
    scope = _require_ao()
    payload = request.get_json(silent=True) or {}
    ids = payload.get("ids") or []
    if isinstance(ids, str):
        ids = [ids]
    saved = _mark_notifications_seen(scope, ids)
    return jsonify({"ok": True, "saved": saved})


def register_area_progress_monitor(app) -> None:
    app.register_blueprint(bp)
