from __future__ import annotations

import calendar
import json
import os
import re
import secrets
import sqlite3
import string
from datetime import date, datetime, timedelta, timezone
from typing import Any

import gspread
from google.oauth2.service_account import Credentials

from flask import (
    Blueprint,
    abort,
    current_app,
    jsonify,
    render_template_string,
    request,
    session,
)

bp = Blueprint("church_progress", __name__)

PH_TZ = timezone(timedelta(hours=8))
START_YEAR = 2026
YEAR_CHOICES_COUNT = 20

GOOGLE_SHEETS_CREDENTIALS_FILE = "service_account.json"
GOOGLE_SHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
GOOGLE_SHEET_NAME = "District4 Data"
MEMBERS_SHEET_NAME = "Members Account"

MEMBER_HEADERS = [
    "Name",
    "BDay",
    "Church ID",
    "Church Address",
    "Area Number",
    "Pastor",
    "UserName",
    "Password",
]

FINANCE_FIELDS = {
    "tithes": "Tithes",
    "offering": "Offering",
    "personal_tithes": "Personal Tithes",
    "mission_offering": "Mission Offering",
    "amount_to_send": "Total Money Sent",
}

ATTENDANCE_FIELDS = {
    "adult": "Adult",
    "youth": "Young People",
    "children": "Children",
    "all": "All",
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


def _database_path() -> str:
    cfg = current_app.config.get("DATABASE")
    if cfg:
        return cfg
    return os.path.join(current_app.root_path, "app_v2.db")


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(_database_path(), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn
def _get_gs_client():
    creds = Credentials.from_service_account_file(
        GOOGLE_SHEETS_CREDENTIALS_FILE,
        scopes=GOOGLE_SHEETS_SCOPES,
    )
    return gspread.authorize(creds)


def _get_members_ws():
    client = _get_gs_client()
    sh = client.open(GOOGLE_SHEET_NAME)

    try:
        ws = sh.worksheet(MEMBERS_SHEET_NAME)
    except Exception:
        ws = sh.add_worksheet(title=MEMBERS_SHEET_NAME, rows=1000, cols=len(MEMBER_HEADERS))

    _ensure_members_headers(ws)
    return ws


def _ensure_members_headers(ws):
    values = ws.get_all_values()

    if not values:
        ws.update("A1:H1", [MEMBER_HEADERS], value_input_option="USER_ENTERED")
        return MEMBER_HEADERS

    current = list(values[0])
    changed = False

    for idx, header in enumerate(MEMBER_HEADERS):
        if len(current) <= idx:
            current.append(header)
            changed = True
        elif str(current[idx] or "").strip() != header:
            current[idx] = header
            changed = True

    if changed:
        ws.update("A1:H1", [current[: len(MEMBER_HEADERS)]], value_input_option="USER_ENTERED")

    return current[: len(MEMBER_HEADERS)]


def _members_from_sheet(church: dict[str, Any]) -> list[dict[str, Any]]:
    try:
        ws = _get_members_ws()
        values = ws.get_all_values()
    except Exception:
        return []

    if len(values) < 2:
        return []

    church_id = str(church.get("church_name") or "").strip()
    church_address = str(church.get("church_address") or "").strip()

    members = []

    for row_num, row in enumerate(values[1:], start=2):
        padded = row + [""] * (len(MEMBER_HEADERS) - len(row))

        member = {
            "sheet_row": row_num,
            "name": str(padded[0] or "").strip(),
            "bday": str(padded[1] or "").strip(),
            "church_id": str(padded[2] or "").strip(),
            "church_address": str(padded[3] or "").strip(),
            "area_number": str(padded[4] or "").strip(),
            "pastor": str(padded[5] or "").strip(),
            "username": str(padded[6] or "").strip(),
            "password": str(padded[7] or "").strip(),
        }

        if not member["name"] and not member["username"]:
            continue

        if (
            _normalize(member["church_id"]) == _normalize(church_id)
            or _normalize(member["church_address"]) == _normalize(church_address)
        ):
            members.append(member)

    members.sort(key=lambda x: _normalize(x["name"]))
    return members

def _now_ph() -> datetime:
    return datetime.now(PH_TZ)


def _safe_float(value: Any) -> float:
    try:
        if value is None or str(value).strip() == "":
            return 0.0
        return float(str(value).replace(",", ""))
    except Exception:
        return 0.0


def _parse_dt_guess(value: Any) -> datetime | None:
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
                return dt.replace(tzinfo=PH_TZ)
            return dt.astimezone(PH_TZ)
        except Exception:
            continue

    return None


def _normalize(value: Any) -> str:
    s = str(value or "").strip().lower()
    for token in ["church", "ctr", "center"]:
        s = s.replace(token, " ")
    cleaned = []
    for ch in s:
        cleaned.append(ch if ch.isalnum() else " ")
    return " ".join("".join(cleaned).split())


def _month_label(year: int, month: int) -> str:
    return f"{calendar.month_abbr[month]} {year}"


def _long_month_label(year: int, month: int) -> str:
    return f"{calendar.month_name[month]} {year}"


def _months_for_selected_year(year: int) -> list[tuple[int, int]]:
    today = _now_ph().date()
    if int(year) == today.year:
        last_month = today.month
    else:
        last_month = 12

    return [(int(year), month) for month in range(1, last_month + 1)]


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
    count = 0
    for day_num in range(1, total_days + 1):
        if date(year, month, day_num).weekday() == 6:
            count += 1
    return max(count, 1)


def _is_logged_in() -> bool:
    return bool(
        session.get("pastor_logged_in")
        or session.get("ao_logged_in")
    )


def _church_info(church_id: str) -> dict[str, Any] | None:
    conn = _connect()

    church_id_raw = str(church_id or "").strip()
    church_id_norm = _normalize(church_id_raw)

    rows = conn.execute(
        """
        SELECT
            TRIM(COALESCE(sex,'')) AS church_name,
            TRIM(COALESCE(name,'')) AS pastor_name,
            TRIM(COALESCE(church_address,'')) AS church_address,
            TRIM(COALESCE(age,'')) AS area_number,
            TRIM(COALESCE(sub_area,'')) AS sub_area,
            TRIM(COALESCE(username,'')) AS username,
            TRIM(COALESCE(position,'')) AS position
        FROM sheet_accounts_cache
        WHERE LOWER(TRIM(COALESCE(position,''))) = 'pastor'
        ORDER BY church_name
        """
    ).fetchall()

    conn.close()

    for row in rows:
        candidates = [
            row["username"],
            row["church_name"],
            row["church_address"],
            row["pastor_name"],
        ]

        if church_id_raw.lower() in [str(c or "").strip().lower() for c in candidates]:
            return {
                "church_name": str(row["church_name"] or "").strip(),
                "pastor_name": str(row["pastor_name"] or "").strip(),
                "church_address": str(row["church_address"] or "").strip(),
                "area_number": str(row["area_number"] or "").strip(),
                "sub_area": str(row["sub_area"] or "").strip(),
                "username": str(row["username"] or "").strip(),
            }

        if church_id_norm and church_id_norm in [_normalize(c) for c in candidates]:
            return {
                "church_name": str(row["church_name"] or "").strip(),
                "pastor_name": str(row["pastor_name"] or "").strip(),
                "church_address": str(row["church_address"] or "").strip(),
                "area_number": str(row["area_number"] or "").strip(),
                "sub_area": str(row["sub_area"] or "").strip(),
                "username": str(row["username"] or "").strip(),
            }

    return None


def _enforce_access(church: dict[str, Any]) -> None:
    if not _is_logged_in():
        abort(401)

    if session.get("pastor_logged_in"):
        session_username = str(session.get("pastor_username") or "").strip().lower()
        church_username = str(church.get("username") or "").strip().lower()

        if session_username and church_username and session_username != church_username:
            abort(403)


def _can_manage_members(church: dict[str, Any]) -> bool:
    if not session.get("pastor_logged_in"):
        return False

    session_username = str(session.get("pastor_username") or "").strip().lower()
    church_username = str(church.get("username") or "").strip().lower()

    return bool(session_username and church_username and session_username == church_username)


def _choose_report_timestamp(row: sqlite3.Row | None) -> datetime | None:
    if not row:
        return None

    preferred = [
        "submitted_at",
        "updated_at",
        "created_at",
        "approved_at",
        "timestamp",
        "submitted_on",
    ]

    for col in preferred:
        if col in row.keys():
            dt = _parse_dt_guess(row[col])
            if dt:
                return dt

    return None


def _sheet_report_rows_for_month(
    conn: sqlite3.Connection,
    church: dict[str, Any],
    year: int,
    month: int,
) -> list[sqlite3.Row]:
    church_name = str(church.get("church_name") or "").strip()
    church_address = str(church.get("church_address") or "").strip()
    pastor_name = str(church.get("pastor_name") or "").strip()

    rows = conn.execute(
        """
        SELECT DISTINCT sheet_row, *
        FROM sheet_report_cache
        WHERE CAST(year AS INTEGER) = ?
          AND CAST(month AS INTEGER) = ?
          AND (
                TRIM(COALESCE(church,'')) = TRIM(?)
             OR TRIM(COALESCE(church,'')) = TRIM(?)
             OR TRIM(COALESCE(address,'')) = TRIM(?)
             OR TRIM(COALESCE(address,'')) = TRIM(?)
             OR TRIM(COALESCE(pastor,'')) = TRIM(?)
          )
        ORDER BY activity_date
        """,
        (
            int(year),
            int(month),
            church_name,
            church_address,
            church_name,
            church_address,
            pastor_name,
        ),
    ).fetchall()

    if rows:
        return rows

    all_rows = conn.execute(
        """
        SELECT DISTINCT sheet_row, *
        FROM sheet_report_cache
        WHERE CAST(year AS INTEGER) = ?
          AND CAST(month AS INTEGER) = ?
        ORDER BY activity_date
        """,
        (int(year), int(month)),
    ).fetchall()

    target_keys = {
        _normalize(church_name),
        _normalize(church_address),
        _normalize(pastor_name),
    }
    target_keys = {x for x in target_keys if x}

    matched = []
    seen_sheet_rows = set()

    for row in all_rows:
        row_keys = {
            _normalize(row["church"] if "church" in row.keys() else ""),
            _normalize(row["address"] if "address" in row.keys() else ""),
            _normalize(row["pastor"] if "pastor" in row.keys() else ""),
        }
        row_keys = {x for x in row_keys if x}

        sheet_row = row["sheet_row"] if "sheet_row" in row.keys() else None
        if sheet_row in seen_sheet_rows:
            continue

        if target_keys.intersection(row_keys):
            seen_sheet_rows.add(sheet_row)
            matched.append(row)

    return matched


def _monthly_report_row(
    conn: sqlite3.Connection,
    church: dict[str, Any],
    year: int,
    month: int,
) -> sqlite3.Row | None:
    username = str(church.get("username") or "").strip()
    if not username:
        return None

    return conn.execute(
        """
        SELECT *
        FROM monthly_reports
        WHERE CAST(year AS INTEGER) = ?
          AND CAST(month AS INTEGER) = ?
          AND LOWER(TRIM(COALESCE(pastor_username,''))) = LOWER(TRIM(?))
          AND submitted = 1
        ORDER BY submitted_at DESC
        LIMIT 1
        """,
        (int(year), int(month), username),
    ).fetchone()


def _year_choices() -> list[int]:
    return [START_YEAR + i for i in range(YEAR_CHOICES_COUNT)]


def _report_status(
    submitted_at: datetime | None,
    has_monthly_row: bool,
    has_sheet_rows: bool,
    year: int,
    month: int,
) -> str:
    if submitted_at:
        last_sunday = _last_sunday(year, month)
        days_after = (submitted_at.date() - last_sunday).days

        if days_after <= 3:
            return "Early"
        if days_after <= 7:
            return "On Time"
        return "Late"

    if has_sheet_rows:
        return "Reported"

    if has_monthly_row:
        return "Reported"

    return "No Report"


def _status_color(status: str) -> str:
    return {
        "Early": "#16a34a",
        "On Time": "#2563eb",
        "Late": "#ea580c",
        "Reported": "#64748b",
        "No Report": "#dc2626",
    }.get(status, "#64748b")


def _generate_message(status: str) -> str:
    if status == "Early":
        return "Your report was submitted early. Thank you for your faithfulness and diligence in ministry reporting."

    if status == "On Time":
        return "Your report was submitted on time. Thank you for your consistency in reporting."

    if status == "Late":
        return "Your report was submitted late this month. Please try to report earlier next time."

    if status == "Reported":
        return "A report was found in the Google Sheets records, but no submission timestamp was found in the local database."

    return "No report was received for this month. Faithfulness in reporting strengthens ministry accountability."


def _overall_insight(report_rows: list[dict[str, Any]]) -> str:
    early = sum(1 for r in report_rows if r["status"] == "Early")
    on_time = sum(1 for r in report_rows if r["status"] == "On Time")
    late = sum(1 for r in report_rows if r["status"] == "Late")
    reported = sum(1 for r in report_rows if r["status"] == "Reported")
    no_report = sum(1 for r in report_rows if r["status"] == "No Report")

    total = max(len(report_rows), 1)

    if early / total >= 0.70:
        return "Excellent reporting consistency observed this year. Keep being faithful in ministry stewardship."

    if (early + on_time + reported) / total >= 0.75:
        return "Thank you for your consistent participation in ministry reporting. Keep building this faithful rhythm."

    if no_report >= 3:
        return "Several months have missing reports. Regular reporting helps strengthen ministry accountability."

    if late >= 3:
        return "Frequent late submissions were detected. Please improve consistency and timeliness in reporting."

    return "Thank you for your continued participation in ministry reporting."


def _build_progress_payload(church: dict[str, Any], selected_year: int) -> dict[str, Any]:
    months = _months_for_selected_year(selected_year)
    month_labels = [_month_label(y, m) for y, m in months]

    conn = _connect()

    report_faithfulness: list[dict[str, Any]] = []

    finance_series = {key: [] for key in FINANCE_FIELDS}
    attendance_series = {key: [] for key in ATTENDANCE_FIELDS}
    ministry_series = {key: [] for key in MINISTRY_FIELDS}

    for year, month in months:
        sheet_rows = _sheet_report_rows_for_month(conn, church, year, month)
        monthly_row = _monthly_report_row(conn, church, year, month)

        submitted_at = _choose_report_timestamp(monthly_row)
        has_monthly_row = bool(monthly_row)
        has_sheet_rows = bool(sheet_rows)

        status = _report_status(
            submitted_at=submitted_at,
            has_monthly_row=has_monthly_row,
            has_sheet_rows=has_sheet_rows,
            year=year,
            month=month,
        )

        report_faithfulness.append(
            {
                "month_label": _long_month_label(year, month),
                "status": status,
                "submitted_date": submitted_at.strftime("%B %d, %Y") if submitted_at else "--",
                "submitted_time": submitted_at.strftime("%I:%M %p") if submitted_at else "--",
                "deadline": _report_deadline(year, month).strftime("%B %d, %Y"),
                "message": _generate_message(status),
                "color": _status_color(status),
            }
        )

        totals = {
            "adult": 0.0,
            "youth": 0.0,
            "children": 0.0,
            "tithes": 0.0,
            "offering": 0.0,
            "personal_tithes": 0.0,
            "mission_offering": 0.0,
            "amount_to_send": 0.0,
            "received_jesus": 0.0,
            "existing_bible_study": 0.0,
            "new_bible_study": 0.0,
            "water_baptized": 0.0,
            "holy_spirit_baptized": 0.0,
            "childrens_dedication": 0.0,
            "healed": 0.0,
        }

        seen_sheet_rows = set()
        for row in sheet_rows:
            sheet_row = row["sheet_row"] if "sheet_row" in row.keys() else None
            if sheet_row and sheet_row in seen_sheet_rows:
                continue
            if sheet_row:
                seen_sheet_rows.add(sheet_row)

            for key in totals.keys():
                if key in row.keys():
                    totals[key] += _safe_float(row[key])

        sundays = _number_of_sundays(year, month)

        finance_series["tithes"].append(round(totals["tithes"], 2))
        finance_series["offering"].append(round(totals["offering"], 2))
        finance_series["personal_tithes"].append(round(totals["personal_tithes"], 2))
        finance_series["mission_offering"].append(round(totals["mission_offering"], 2))
        finance_series["amount_to_send"].append(round(totals["amount_to_send"], 2))

        attendance_series["adult"].append(round(totals["adult"] / sundays, 2))
        attendance_series["youth"].append(round(totals["youth"] / sundays, 2))
        attendance_series["children"].append(round(totals["children"] / sundays, 2))
        attendance_series["all"].append(round((totals["adult"] + totals["youth"] + totals["children"]) / sundays, 2))

        for key in MINISTRY_FIELDS:
            ministry_series[key].append(round(totals[key] / sundays, 2))

    conn.close()

    report_faithfulness_recent_first = list(reversed(report_faithfulness))
    insight = _overall_insight(report_faithfulness)

    counts = {
        "Early": sum(1 for r in report_faithfulness if r["status"] == "Early"),
        "On Time": sum(1 for r in report_faithfulness if r["status"] == "On Time"),
        "Late": sum(1 for r in report_faithfulness if r["status"] == "Late"),
        "Reported": sum(1 for r in report_faithfulness if r["status"] == "Reported"),
        "No Report": sum(1 for r in report_faithfulness if r["status"] == "No Report"),
    }

    members = _members_from_sheet(church)

    return {
        "church": church,
        "selected_year": selected_year,
        "month_labels": month_labels,
        "report_faithfulness": report_faithfulness_recent_first,
        "report_counts": counts,
        "insight": insight,
        "finance_series": finance_series,
        "attendance_series": attendance_series,
        "ministry_series": ministry_series,
        "finance_options": FINANCE_FIELDS,
        "attendance_options": ATTENDANCE_FIELDS,
        "ministry_options": MINISTRY_FIELDS,
        "members": members,
    }


PAGE_HTML = r"""
{% extends "base.html" %}

{% block title %}Church Progress{% endblock %}

{% block content %}

<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>

<style>
  .cp-wrap {
    max-width: 1180px;
    margin: 0 auto;
    padding: 18px 12px 42px;
    color: #142033;
  }

  .cp-display-font {
    font-family: "Trebuchet MS", "Segoe UI", Arial, sans-serif;
    letter-spacing: -0.04em;
  }

  .cp-hero {
    background: linear-gradient(180deg, #ff2bd6 0%, #c4007a 55%, #7a003f 100%);
    color: #fff;
    border-radius: 24px;
    padding: 24px;
    box-shadow: 0 18px 38px rgba(196, 0, 122, .24);
    margin-bottom: 16px;
    position: relative;
    overflow: hidden;
  }

  .cp-hero::before {
    content: "";
    position: absolute;
    inset: -80px -80px auto auto;
    width: 220px;
    height: 220px;
    background: rgba(255,255,255,.18);
    border-radius: 999px;
  }

  .cp-hero h1 {
    position: relative;
    margin: 0;
    font-size: 36px;
    line-height: 1.05;
    font-weight: 950;
    text-shadow: 0 3px 14px rgba(0,0,0,.18);
  }

  .cp-hero-sub {
    position: relative;
    margin-top: 10px;
    opacity: .96;
    line-height: 1.5;
    font-size: 14px;
  }

  .cp-year-row {
    position: relative;
    margin-top: 16px;
    display: flex;
    gap: 10px;
    align-items: center;
    flex-wrap: wrap;
  }

  .cp-year-label {
    font-weight: 900;
    font-size: 13px;
    opacity: .95;
  }

  .cp-year-select {
    min-width: 160px;
    border: 1px solid rgba(255,255,255,.45);
    background: rgba(255,255,255,.95);
    color: #7a003f;
    border-radius: 14px;
    padding: 11px 13px;
    font-weight: 900;
    font-size: 15px;
    outline: none;
  }

  .cp-card {
    background: #fff;
    border: 1px solid #dbe3ee;
    border-radius: 18px;
    box-shadow: 0 10px 24px rgba(15, 23, 42, .08);
    margin-bottom: 16px;
    overflow: hidden;
  }

  .cp-section {
    padding: 16px;
  }

  .cp-section-title {
    margin: 0;
    font-size: 24px;
    font-weight: 950;
    color: #2b1630;
    line-height: 1.12;
  }

  .cp-section-title span {
    color: #c4007a;
  }

  .cp-muted {
    color: #64748b;
    font-size: 13px;
    line-height: 1.45;
  }

  .cp-filters {
    display: flex;
    gap: 8px;
    flex-wrap: wrap;
    margin-top: 12px;
  }

  .cp-pill {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    border-radius: 999px;
    border: 1px solid #f0b5dc;
    background: #fff5fb;
    color: #7a003f;
    padding: 9px 13px;
    font-size: 13px;
    font-weight: 900;
    text-decoration: none;
    cursor: pointer;
  }

  .cp-pill.active {
    background: #c4007a;
    border-color: #c4007a;
    color: #fff;
  }

  .cp-insight {
    background: linear-gradient(135deg, #fff0fa, #f8fbff);
    color: #7a003f;
    border: 1px solid #f0b5dc;
    padding: 13px 14px;
    border-radius: 15px;
    line-height: 1.6;
    font-size: 14px;
    margin-bottom: 12px;
  }

  .cp-stat-grid {
    display: grid;
    grid-template-columns: repeat(2, 1fr);
    gap: 9px;
    margin-bottom: 12px;
  }

  .cp-stat {
    border: 1px solid #dbe3ee;
    background: #fff;
    border-radius: 15px;
    padding: 12px;
  }

  .cp-stat-label {
    color: #64748b;
    font-size: 12px;
    margin-bottom: 5px;
  }

  .cp-stat-value {
    font-size: 24px;
    font-weight: 950;
    color: #2b1630;
  }

  .cp-chart-box {
    height: 310px;
    position: relative;
  }

  .cp-table-wrap {
    overflow-x: auto;
  }

  .cp-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 14px;
  }

  .cp-table th,
  .cp-table td {
    padding: 12px 10px;
    border-bottom: 1px solid #dbe3ee;
    text-align: left;
    vertical-align: top;
  }

  .cp-table th {
    color: #64748b;
    background: #f8fafc;
    font-weight: 900;
  }

  .cp-status {
    display: inline-flex;
    padding: 7px 10px;
    border-radius: 999px;
    color: white;
    font-size: 12px;
    font-weight: 900;
    white-space: nowrap;
  }

  .cp-create-box {
    background: linear-gradient(135deg, #fff0fa, #ffffff);
    border: 1px dashed #f0b5dc;
    border-radius: 16px;
    padding: 15px;
    color: #334155;
    line-height: 1.6;
  }

  .cp-disabled-btn {
    display: inline-block;
    margin-top: 10px;
    background: #9f3b76;
    color: white;
    border-radius: 12px;
    padding: 10px 14px;
    font-weight: 900;
    text-decoration: none;
    cursor: not-allowed;
  }

  .cp-deadline {
    background: #f8fafc;
    color: #334155;
    border-top: 1px solid #dbe3ee;
    padding: 14px 16px;
    font-size: 13px;
    line-height: 1.6;
  }


  .cp-verse {
    margin-top: 8px;
    padding: 10px 12px;
    border-left: 4px solid #c4007a;
    background: #fff5fb;
    color: #7a003f;
    border-radius: 12px;
    font-size: 13px;
    line-height: 1.55;
  }

  .cp-verse em {
    font-style: italic;
  }

  .cp-action-btn {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    border-radius: 999px;
    border: 1px solid #f0b5dc;
    background: #fff5fb;
    color: #7a003f;
    padding: 9px 13px;
    font-size: 13px;
    font-weight: 900;
    text-decoration: none;
    cursor: pointer;
  }

  .cp-action-btn.primary {
    background: #c4007a;
    border-color: #c4007a;
    color: #fff;
  }

  .cp-action-btn.warn {
    background: #f59e0b;
    border-color: #f59e0b;
    color: #111827;
  }

  .cp-action-btn.danger {
    background: #dc2626;
    border-color: #dc2626;
    color: #fff;
  }

  .cp-action-btn:disabled {
    opacity: .65;
    cursor: not-allowed;
  }

  .cp-member-toolbar {
    display: flex;
    flex-wrap: wrap;
    gap: 10px;
    margin: 14px 0;
  }

  .cp-member-empty {
    padding: 14px;
    background: #f8fafc;
    border: 1px solid #dbe3ee;
    border-radius: 14px;
    color: #64748b;
  }

  .cp-modal {
    display: none;
    position: fixed;
    inset: 0;
    background: rgba(15,23,42,.62);
    z-index: 9999;
    padding: 20px;
    overflow: auto;
  }

  .cp-modal.open {
    display: block;
  }

  .cp-modal-card {
    width: min(720px, 96vw);
    margin: 40px auto;
    background: #fff;
    border-radius: 22px;
    padding: 20px;
    box-shadow: 0 24px 60px rgba(0,0,0,.25);
  }

  .cp-modal-head {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 14px;
    margin-bottom: 14px;
  }

  .cp-modal-head h3 {
    margin: 0;
    font-size: 22px;
    color: #2b1630;
  }

  .cp-close {
    border: none;
    border-radius: 999px;
    background: #f1f5f9;
    color: #334155;
    padding: 8px 12px;
    font-weight: 900;
    cursor: pointer;
  }

  .cp-form-grid {
    display: grid;
    gap: 12px;
  }

  .cp-field label {
    display: block;
    font-size: 13px;
    font-weight: 900;
    color: #334155;
    margin-bottom: 6px;
  }

  .cp-field input,
  .cp-field select {
    width: 100%;
    padding: 11px 12px;
    border: 1px solid #cbd5e1;
    border-radius: 12px;
    font: inherit;
  }

  .cp-fixed-box {
    background: #f8fafc;
    border: 1px solid #dbe3ee;
    border-radius: 14px;
    padding: 12px;
    color: #475569;
    font-size: 13px;
    line-height: 1.5;
  }

  .cp-creds-box {
    background: #f8fafc;
    border: 1px solid #dbe3ee;
    border-radius: 14px;
    padding: 14px;
    font-family: monospace;
    margin: 12px 0;
    color: #111827;
  }

  .cp-error {
    background: #fee2e2;
    color: #991b1b;
    border: 1px solid #fecaca;
    border-radius: 12px;
    padding: 10px 12px;
    margin-bottom: 10px;
    display: none;
  }

  
  @media (min-width: 900px) {
    .cp-stat-grid {
      grid-template-columns: repeat(5, 1fr);
    }

    .cp-form-grid.two {
      grid-template-columns: 1fr 1fr;
    }
  }

  @media (max-width: 700px) {
    .cp-hero h1 {
      font-size: 28px;
    }

    .cp-section-title {
      font-size: 21px;
    }

    .cp-table {
      min-width: 950px;
    }

    .cp-chart-box {
      height: 280px;
    }

    .cp-year-select {
      width: 100%;
    }
  }
</style>

<div class="cp-wrap">

  <div class="cp-hero">
    <h1 class="cp-display-font">Church Progress</h1>
    <div class="cp-hero-sub">
      {{ church.church_name }} |
      Pastor {{ church.pastor_name or "-" }} |
      Area {{ church.area_number or "-" }}
      {% if church.sub_area %} | {{ church.sub_area }}{% endif %}
      <br>
      {{ church.church_address or "" }}
    </div>

    <form class="cp-year-row" method="get">
      <label class="cp-year-label" for="yearSelect">Select Report Year</label>
      <select class="cp-year-select" id="yearSelect" name="year" onchange="this.form.submit()">
        {% for y in year_choices %}
          <option value="{{ y }}" {% if y == selected_year %}selected{% endif %}>{{ y }}</option>
        {% endfor %}
      </select>
    </form>
  </div>

  <div class="cp-card">
    <div class="cp-section">
      <h2 class="cp-section-title cp-display-font"><span>1.</span> Report Submission Faithfulness</h2>
      <div class="cp-muted">Shows report month, submission status, date and time submitted, deadline, and pastoral message.</div>
      <div class="cp-verse"><em>“Now it is required that those who have been given a trust must prove faithful.”</em> — 1 Corinthians 4:2, NIV</div>

      <br>
      <div class="cp-insight">{{ insight }}</div>

      <div class="cp-stat-grid">
        <div class="cp-stat"><div class="cp-stat-label">Early</div><div class="cp-stat-value">{{ report_counts["Early"] }}</div></div>
        <div class="cp-stat"><div class="cp-stat-label">On Time</div><div class="cp-stat-value">{{ report_counts["On Time"] }}</div></div>
        <div class="cp-stat"><div class="cp-stat-label">Late</div><div class="cp-stat-value">{{ report_counts["Late"] }}</div></div>
        <div class="cp-stat"><div class="cp-stat-label">Reported</div><div class="cp-stat-value">{{ report_counts["Reported"] }}</div></div>
        <div class="cp-stat"><div class="cp-stat-label">No Report</div><div class="cp-stat-value">{{ report_counts["No Report"] }}</div></div>
      </div>

      <div class="cp-table-wrap">
        <table class="cp-table">
          <thead>
            <tr>
              <th>Report Month</th>
              <th>Status</th>
              <th>Date Submitted</th>
              <th>Time Submitted</th>
              <th>Deadline</th>
              <th>Message</th>
            </tr>
          </thead>
          <tbody>
            {% for row in report_faithfulness %}
            <tr>
              <td>{{ row.month_label }}</td>
              <td><span class="cp-status" style="background:{{ row.color }}">{{ row.status }}</span></td>
              <td>{{ row.submitted_date }}</td>
              <td>{{ row.submitted_time }}</td>
              <td>{{ row.deadline }}</td>
              <td>{{ row.message }}</td>
            </tr>
            {% endfor %}
          </tbody>
        </table>
      </div>
    </div>

    <div class="cp-deadline">
      <strong>Reporting Deadline Rule:</strong><br>
      Reports submitted within 3 days after the last Sunday of the month are marked as <strong>Early</strong>.
      Reports submitted within 7 days after the last Sunday are marked as <strong>On Time</strong>.
      Reports submitted beyond 7 days are marked as <strong>Late</strong>.
      If a report exists in Google Sheets but no local timestamp exists, it is marked as <strong>Reported</strong>.
    </div>
  </div>

  <div class="cp-card">
    <div class="cp-section">
      <h2 class="cp-section-title cp-display-font"><span>2.</span> Financial Overview</h2>
      <div class="cp-muted">Monthly financial progress for this church only for {{ selected_year }}.</div>
      <div class="cp-verse"><em>“God is able to bless you abundantly...”</em> — 2 Corinthians 9:8, NIV</div>
      <div class="cp-filters" id="financeFilters"></div>
      <div class="cp-chart-box"><canvas id="financeChart"></canvas></div>
    </div>
  </div>

  <div class="cp-card">
    <div class="cp-section">
      <h2 class="cp-section-title cp-display-font"><span>3.</span> Attendance Overview</h2>
      <div class="cp-muted">Average attendance per Sunday for {{ selected_year }}.</div>
      <div class="cp-verse"><em>“...not giving up meeting together...”</em> — Hebrews 10:25, NIV</div>
      <div class="cp-filters" id="attendanceFilters"></div>
      <div class="cp-chart-box"><canvas id="attendanceChart"></canvas></div>
    </div>
  </div>

  <div class="cp-card">
    <div class="cp-section">
      <h2 class="cp-section-title cp-display-font"><span>4.</span> Ministry Activity</h2>
      <div class="cp-muted">Average ministry activity per Sunday for {{ selected_year }}.</div>
      <div class="cp-verse"><em>“Always give yourselves fully to the work of the Lord...”</em> — 1 Corinthians 15:58, NIV</div>
      <div class="cp-filters" id="ministryFilters"></div>
      <div class="cp-chart-box"><canvas id="ministryChart"></canvas></div>
    </div>
  </div>

  <div class="cp-card">
    <div class="cp-section">
      <h2 class="cp-section-title cp-display-font"><span>5.</span> Create Member Account</h2>
      <div class="cp-muted">Create, edit, or delete member login accounts under this church.</div>
      <div class="cp-verse"><em>“...entrust to reliable people who will also be qualified to teach others.”</em> — 2 Timothy 2:2, NIV</div>

      {% if can_manage_members %}
        <div class="cp-member-toolbar">
          <button type="button" class="cp-action-btn primary" onclick="openCreateMemberModal()">Create Account</button>
          <button type="button" class="cp-action-btn warn" onclick="openEditMemberModal()">Edit Account</button>
          <button type="button" class="cp-action-btn danger" onclick="openDeleteMemberModal()">Delete</button>
        </div>
      {% else %}
        <div class="cp-member-empty">Only the assigned pastor of this church can create, edit, or delete member accounts.</div>
      {% endif %}

      {% if members %}
        <div class="cp-table-wrap">
          <table class="cp-table">
            <thead>
              <tr>
                <th>Name</th>
                <th>BDay</th>
                <th>UserName</th>
                <th>Church ID</th>
              </tr>
            </thead>
            <tbody>
              {% for m in members %}
              <tr>
                <td>{{ m.name }}</td>
                <td>{{ m.bday or "-" }}</td>
                <td>{{ m.username }}</td>
                <td>{{ m.church_id }}</td>
              </tr>
              {% endfor %}
            </tbody>
          </table>
        </div>
      {% else %}
        <div class="cp-member-empty">No member accounts registered yet for this church.</div>
      {% endif %}
    </div>
  </div>

</div>

<div id="createMemberModal" class="cp-modal">
  <div class="cp-modal-card">
    <div class="cp-modal-head">
      <h3 class="cp-display-font">Create Member Account</h3>
      <button type="button" class="cp-close" onclick="closeModal('createMemberModal')">Close</button>
    </div>

    <div id="createError" class="cp-error"></div>

    <form id="createMemberForm" class="cp-form-grid two">
      <div class="cp-field">
        <label>Name</label>
        <input type="text" name="name" required placeholder="Enter member full name">
      </div>

      <div class="cp-field">
        <label>BDay</label>
        <input type="date" name="bday" required>
      </div>

      <div class="cp-fixed-box" style="grid-column:1/-1;">
        <strong>Fixed Church Information</strong><br>
        Church ID: {{ church.church_name }}<br>
        Church Address: {{ church.church_address }}<br>
        Area Number: {{ church.area_number }}<br>
        Pastor: {{ church.pastor_name }}
      </div>

      <div style="grid-column:1/-1; display:flex; gap:10px; flex-wrap:wrap;">
        <button type="button" class="cp-action-btn primary" onclick="submitCreateMemberForm(this)">Save Member Account</button>
        <button type="button" class="cp-action-btn" onclick="closeModal('createMemberModal')">Cancel</button>
      </div>
    </form>
  </div>
</div>

<div id="editMemberModal" class="cp-modal">
  <div class="cp-modal-card">
    <div class="cp-modal-head">
      <h3 class="cp-display-font">Edit Member Account</h3>
      <button type="button" class="cp-close" onclick="closeModal('editMemberModal')">Close</button>
    </div>

    <div id="editError" class="cp-error"></div>

    <form id="editMemberForm" class="cp-form-grid two">
      <div class="cp-field" style="grid-column:1/-1;">
        <label>Select Member</label>
        <select id="editSheetRow" name="sheet_row" required onchange="fillEditMember()">
          <option value="">Choose member</option>
          {% for m in members %}
            <option value="{{ m.sheet_row }}">{{ m.name }} — {{ m.username }}</option>
          {% endfor %}
        </select>
      </div>

      <div class="cp-field">
        <label>Name</label>
        <input type="text" id="editName" name="name" required>
      </div>

      <div class="cp-field">
        <label>BDay</label>
        <input type="date" id="editBday" name="bday" required>
      </div>

      <div class="cp-field">
        <label>UserName</label>
        <input type="text" id="editUsername" name="username" required>
      </div>

      <div class="cp-field">
        <label>Password</label>
        <input type="text" id="editPassword" name="password" required>
      </div>

      <div class="cp-fixed-box" style="grid-column:1/-1;">
        Church ID, Church Address, Area Number, and Pastor remain fixed to this church.
      </div>

      <div style="grid-column:1/-1; display:flex; gap:10px; flex-wrap:wrap;">
        <button type="button" class="cp-action-btn primary" onclick="submitEditMemberForm(this)">Save Changes</button>
        <button type="button" class="cp-action-btn" onclick="closeModal('editMemberModal')">Cancel</button>
      </div>
    </form>
  </div>
</div>

<div id="deleteMemberModal" class="cp-modal">
  <div class="cp-modal-card">
    <div class="cp-modal-head">
      <h3 class="cp-display-font">Delete Member Account</h3>
      <button type="button" class="cp-close" onclick="closeModal('deleteMemberModal')">Close</button>
    </div>

    <div id="deleteError" class="cp-error"></div>

    <form id="deleteMemberForm" class="cp-form-grid">
      <div class="cp-field">
        <label>Select Member</label>
        <select name="sheet_row" required>
          <option value="">Choose member</option>
          {% for m in members %}
            <option value="{{ m.sheet_row }}">{{ m.name }} — {{ m.username }}</option>
          {% endfor %}
        </select>
      </div>

      <div class="cp-fixed-box">
        This will delete the selected member account from the Members Account sheet.
      </div>

      <div style="display:flex; gap:10px; flex-wrap:wrap;">
        <button type="button" class="cp-action-btn danger" onclick="submitDeleteMemberForm(this)">Delete Account</button>
        <button type="button" class="cp-action-btn" onclick="closeModal('deleteMemberModal')">Cancel</button>
      </div>
    </form>
  </div>
</div>

<div id="successModal" class="cp-modal">
  <div class="cp-modal-card">
    <div class="cp-modal-head">
      <h3 id="successTitle" class="cp-display-font">Account Saved</h3>
      <button type="button" class="cp-close" onclick="reloadPage()">Close</button>
    </div>

    <p id="successMessage">The account has been saved.</p>

    <div id="successCreds" class="cp-creds-box" style="display:none;">
      <p><strong>Username:</strong> <span id="successUsername"></span></p>
      <p><strong>Password:</strong> <span id="successPassword"></span></p>
    </div>

    <div style="display:flex; gap:10px; flex-wrap:wrap;">
      <button id="copyCredsBtn" type="button" class="cp-action-btn primary" onclick="copyCredentials()">Copy Credentials</button>
      <button type="button" class="cp-action-btn" onclick="reloadPage()">Done</button>
    </div>
  </div>
</div>

<script>
  const monthLabels = {{ month_labels_json|safe }};
  const financeSeries = {{ finance_series_json|safe }};
  const attendanceSeries = {{ attendance_series_json|safe }};
  const ministrySeries = {{ ministry_series_json|safe }};
  const financeOptions = {{ finance_options_json|safe }};
  const attendanceOptions = {{ attendance_options_json|safe }};
  const ministryOptions = {{ ministry_options_json|safe }};
  const memberData = {{ members_json|safe }};

  const createUrl = "{{ url_for('church_progress.member_create', church_id=church.username) }}";
  const editUrl = "{{ url_for('church_progress.member_update', church_id=church.username) }}";
  const deleteUrl = "{{ url_for('church_progress.member_delete', church_id=church.username) }}";

  let financeMetric = "amount_to_send";
  let attendanceMetric = "all";
  let ministryMetric = "received_jesus";

  let financeChart = null;
  let attendanceChart = null;
  let ministryChart = null;

  function peso(value) {
    return new Intl.NumberFormat("en-PH", {
      style: "currency",
      currency: "PHP",
      maximumFractionDigits: 2
    }).format(Number(value || 0));
  }

  function makeButtons(targetId, options, activeKey, attrName) {
    const target = document.getElementById(targetId);
    target.innerHTML = Object.entries(options).map(([key, label]) => {
      const active = key === activeKey ? "active" : "";
      return `<button type="button" class="cp-pill ${active}" data-${attrName}="${key}">${label}</button>`;
    }).join("");
  }

  function chartOptions(currency=false) {
    return {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: function(ctx) {
              const val = ctx.parsed.y ?? ctx.parsed;
              return currency ? peso(val) : Number(val || 0).toLocaleString("en-PH");
            }
          }
        }
      },
      scales: {
        y: {
          beginAtZero: true,
          ticks: {
            callback: function(value) {
              return currency ? peso(value) : Number(value || 0).toLocaleString("en-PH");
            }
          }
        }
      }
    };
  }

  function makeLineChart(canvasId, label, data, currency=false) {
    const ctx = document.getElementById(canvasId);
    return new Chart(ctx, {
      type: "line",
      data: {
        labels: monthLabels,
        datasets: [{
          label: label,
          data: data || [],
          borderWidth: 3,
          tension: 0.3,
          pointRadius: 4,
          fill: false
        }]
      },
      options: chartOptions(currency)
    });
  }

  function renderFinanceChart() {
    if (financeChart) financeChart.destroy();
    financeChart = makeLineChart("financeChart", financeOptions[financeMetric] || "Finance", financeSeries[financeMetric] || [], true);
    makeButtons("financeFilters", financeOptions, financeMetric, "finance");
  }

  function renderAttendanceChart() {
    if (attendanceChart) attendanceChart.destroy();
    attendanceChart = makeLineChart("attendanceChart", attendanceOptions[attendanceMetric] || "Attendance", attendanceSeries[attendanceMetric] || [], false);
    makeButtons("attendanceFilters", attendanceOptions, attendanceMetric, "attendance");
  }

  function renderMinistryChart() {
    if (ministryChart) ministryChart.destroy();
    ministryChart = makeLineChart("ministryChart", ministryOptions[ministryMetric] || "Ministry", ministrySeries[ministryMetric] || [], false);
    makeButtons("ministryFilters", ministryOptions, ministryMetric, "ministry");
  }

  document.addEventListener("click", function(e) {
    const finance = e.target.getAttribute("data-finance");
    if (finance) {
      financeMetric = finance;
      renderFinanceChart();
      return;
    }

    const attendance = e.target.getAttribute("data-attendance");
    if (attendance) {
      attendanceMetric = attendance;
      renderAttendanceChart();
      return;
    }

    const ministry = e.target.getAttribute("data-ministry");
    if (ministry) {
      ministryMetric = ministry;
      renderMinistryChart();
      return;
    }
  });


  let latestUsername = "";
  let latestPassword = "";

  function openModal(id) {
    const el = document.getElementById(id);
    if (el) el.classList.add("open");
  }

  function closeModal(id) {
    const el = document.getElementById(id);
    if (el) el.classList.remove("open");
  }

  function openCreateMemberModal() {
    openModal("createMemberModal");
  }

  function openEditMemberModal() {
    openModal("editMemberModal");
  }

  function openDeleteMemberModal() {
    openModal("deleteMemberModal");
  }

  function stopMemberAjaxSubmitEvent(e) {
    if (!e) return;
    e.preventDefault();
    e.stopPropagation();
    if (typeof e.stopImmediatePropagation === "function") {
      e.stopImmediatePropagation();
    }
  }

  function stopGlobalLoadingBar() {
    const candidates = [
      "loadingOverlay",
      "globalLoadingOverlay",
      "global-loading-overlay",
      "loadingModal",
      "loadingScreen",
      "pageLoadingOverlay"
    ];

    candidates.forEach(function(id) {
      const el = document.getElementById(id);
      if (el) {
        el.style.display = "none";
        el.classList.add("hidden");
        el.classList.remove("show", "active", "open");
      }
    });

    document.querySelectorAll(".loading-overlay, .global-loading, .loading-screen, .loading-modal").forEach(function(el) {
      el.style.display = "none";
      el.classList.add("hidden");
      el.classList.remove("show", "active", "open");
    });

    document.body.classList.remove("loading", "is-loading", "global-loading-active");

    if (window.hideLoadingOverlay && typeof window.hideLoadingOverlay === "function") {
      try { window.hideLoadingOverlay(); } catch (err) {}
    }

    if (window.hideLoading && typeof window.hideLoading === "function") {
      try { window.hideLoading(); } catch (err) {}
    }

    if (window.stopLoading && typeof window.stopLoading === "function") {
      try { window.stopLoading(); } catch (err) {}
    }
  }

  function showError(id, message) {
    const el = document.getElementById(id);
    if (!el) return;
    el.style.display = "block";
    el.textContent = message || "Something went wrong.";
  }

  function clearError(id) {
    const el = document.getElementById(id);
    if (!el) return;
    el.style.display = "none";
    el.textContent = "";
  }

  function formToObject(form) {
    const data = new FormData(form);
    const out = {};
    for (const [key, value] of data.entries()) {
      out[key] = value;
    }
    return out;
  }

  async function postJson(url, payload) {
    const response = await fetch(url, {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify(payload || {})
    });

    const data = await response.json().catch(() => ({}));

    if (!response.ok || !data.ok) {
      throw new Error(data.error || "Request failed.");
    }

    return data;
  }

  function showSuccess(message, username, password, title) {
    latestUsername = username || "";
    latestPassword = password || "";

    document.getElementById("successTitle").textContent = title || "Account Saved";
    document.getElementById("successMessage").textContent = message || "The account has been saved.";

    const box = document.getElementById("successCreds");
    const copyBtn = document.getElementById("copyCredsBtn");

    if (latestUsername || latestPassword) {
      box.style.display = "block";
      copyBtn.style.display = "inline-flex";
      document.getElementById("successUsername").textContent = latestUsername;
      document.getElementById("successPassword").textContent = latestPassword;
    } else {
      box.style.display = "none";
      copyBtn.style.display = "none";
    }

    openModal("successModal");
  }

  function copyCredentials() {
    const text = "Username: " + latestUsername + "\\nPassword: " + latestPassword;

    if (!latestUsername && !latestPassword) {
      reloadPage();
      return;
    }

    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(text).then(reloadPage).catch(reloadPage);
    } else {
      const temp = document.createElement("textarea");
      temp.value = text;
      document.body.appendChild(temp);
      temp.select();
      try { document.execCommand("copy"); } catch (e) {}
      document.body.removeChild(temp);
      reloadPage();
    }
  }

  function reloadPage() {
    stopGlobalLoadingBar();
    window.location.reload();
  }

  function fillEditMember() {
    const row = String(document.getElementById("editSheetRow").value || "");
    const member = memberData.find(m => String(m.sheet_row) === row);

    document.getElementById("editName").value = member ? (member.name || "") : "";
    document.getElementById("editBday").value = member ? (member.bday || "") : "";
    document.getElementById("editUsername").value = member ? (member.username || "") : "";
    document.getElementById("editPassword").value = member ? (member.password || "") : "";
  }

  async function submitCreateMemberForm(button) {
    stopGlobalLoadingBar();
    const form = document.getElementById("createMemberForm");
    if (!form) return;

    clearError("createError");

    if (!form.reportValidity()) {
      stopGlobalLoadingBar();
      return;
    }

    const oldText = button ? button.textContent : "";

    try {
      if (button) {
        button.disabled = true;
        button.textContent = "Creating...";
      }

      const data = await postJson(createUrl, formToObject(form));
      closeModal("createMemberModal");
      showSuccess("Member account created successfully.", data.username, data.password, "Account Created");
    } catch (err) {
      showError("createError", err.message);
    } finally {
      if (button) {
        button.disabled = false;
        button.textContent = oldText;
      }
      stopGlobalLoadingBar();
    }
  }

  async function submitEditMemberForm(button) {
    stopGlobalLoadingBar();
    const form = document.getElementById("editMemberForm");
    if (!form) return;

    clearError("editError");

    if (!form.reportValidity()) {
      stopGlobalLoadingBar();
      return;
    }

    const oldText = button ? button.textContent : "";

    try {
      if (button) {
        button.disabled = true;
        button.textContent = "Updating...";
      }

      const data = await postJson(editUrl, formToObject(form));
      closeModal("editMemberModal");
      showSuccess("Member account updated successfully.", data.username, data.password, "Account Updated");
    } catch (err) {
      showError("editError", err.message);
    } finally {
      if (button) {
        button.disabled = false;
        button.textContent = oldText;
      }
      stopGlobalLoadingBar();
    }
  }

  async function submitDeleteMemberForm(button) {
    stopGlobalLoadingBar();
    const form = document.getElementById("deleteMemberForm");
    if (!form) return;

    clearError("deleteError");

    if (!form.reportValidity()) {
      stopGlobalLoadingBar();
      return;
    }

    if (!confirm("Delete this member account?")) {
      stopGlobalLoadingBar();
      return;
    }

    const oldText = button ? button.textContent : "";

    try {
      if (button) {
        button.disabled = true;
        button.textContent = "Deleting...";
      }

      await postJson(deleteUrl, formToObject(form));
      closeModal("deleteMemberModal");
      showSuccess("Member account deleted successfully.", "", "", "Account Deleted");
    } catch (err) {
      showError("deleteError", err.message);
    } finally {
      if (button) {
        button.disabled = false;
        button.textContent = oldText;
      }
      stopGlobalLoadingBar();
    }
  }

  stopGlobalLoadingBar();

  renderFinanceChart();
  renderAttendanceChart();
  renderMinistryChart();
</script>

{% endblock %}
"""


@bp.route("/church-progress/<church_id>")
def church_progress(church_id: str):
    church = _church_info(church_id)

    if not church:
        abort(404)

    _enforce_access(church)

    year_choices = _year_choices()
    requested_year_raw = str(request.args.get("year") or "").strip()

    if requested_year_raw.isdigit():
        requested_year = int(requested_year_raw)
    else:
        requested_year = _now_ph().year

    if requested_year not in year_choices:
        requested_year = START_YEAR

    selected_year = requested_year

    payload = _build_progress_payload(church, selected_year)
    members = payload["members"]
    can_manage_members = _can_manage_members(church)

    return render_template_string(
        PAGE_HTML,
        church=payload["church"],
        selected_year=payload["selected_year"],
        year_choices=year_choices,
        month_labels_json=json.dumps(payload["month_labels"]),
        report_faithfulness=payload["report_faithfulness"],
        report_counts=payload["report_counts"],
        insight=payload["insight"],
        finance_series_json=json.dumps(payload["finance_series"]),
        attendance_series_json=json.dumps(payload["attendance_series"]),
        ministry_series_json=json.dumps(payload["ministry_series"]),
        finance_options_json=json.dumps(payload["finance_options"]),
        attendance_options_json=json.dumps(payload["attendance_options"]),
        ministry_options_json=json.dumps(payload["ministry_options"]),
        members=members,
        members_json=json.dumps(members),
        can_manage_members=can_manage_members,
    )

def _all_member_usernames() -> set[str]:
    try:
        ws = _get_members_ws()
        values = ws.get_all_values()
    except Exception:
        return set()

    usernames = set()

    for row in values[1:]:
        if len(row) >= 7:
            u = str(row[6] or "").strip().lower()
            if u:
                usernames.add(u)

    return usernames


def _generate_username(name: str) -> str:
    base = re.sub(r"[^a-zA-Z0-9]+", "", str(name or "").strip().lower())

    if not base:
        base = "member"

    existing = _all_member_usernames()

    username = base
    counter = 1

    while username.lower() in existing:
        counter += 1
        username = f"{base}{counter}"

    return username


def _generate_password(length: int = 8) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))

@bp.route("/church-progress/<church_id>/member/create", methods=["POST"])
def member_create(church_id: str):
    church = _church_info(church_id)

    if not church:
        return jsonify({"ok": False, "error": "Church not found."}), 404

    _enforce_access(church)

    if not _can_manage_members(church):
        return jsonify({"ok": False, "error": "Only the assigned pastor can create member accounts."}), 403

    data = request.get_json(silent=True) or {}

    name = str(data.get("name") or "").strip()
    bday = str(data.get("bday") or "").strip()

    if not name:
        return jsonify({"ok": False, "error": "Member name is required."}), 400

    if not bday:
        return jsonify({"ok": False, "error": "Birthday is required."}), 400

    username = _generate_username(name)
    password = _generate_password()

    row = [
        name,
        bday,
        str(church.get("church_name") or "").strip(),
        str(church.get("church_address") or "").strip(),
        str(church.get("area_number") or "").strip(),
        str(church.get("pastor_name") or "").strip(),
        username,
        password,
    ]

    try:
        ws = _get_members_ws()
        ws.append_row(row, value_input_option="USER_ENTERED")
    except Exception as e:
        return jsonify({"ok": False, "error": f"Failed to save account: {e}"}), 500

    return jsonify({
        "ok": True,
        "username": username,
        "password": password,
    })



def _safe_sheet_row(value: Any) -> int:
    try:
        row = int(value)
        return row if row >= 2 else 0
    except Exception:
        return 0


def _member_row_belongs_to_church(sheet_row: int, church: dict[str, Any]) -> bool:
    try:
        ws = _get_members_ws()
        values = ws.row_values(sheet_row)
    except Exception:
        return False

    padded = values + [""] * (len(MEMBER_HEADERS) - len(values))

    row_church_id = str(padded[2] or "").strip()
    row_church_address = str(padded[3] or "").strip()

    return (
        _normalize(row_church_id) == _normalize(church.get("church_name"))
        or _normalize(row_church_address) == _normalize(church.get("church_address"))
    )


@bp.route("/church-progress/<church_id>/member/update", methods=["POST"])
def member_update(church_id: str):
    church = _church_info(church_id)

    if not church:
        return jsonify({"ok": False, "error": "Church not found."}), 404

    _enforce_access(church)

    if not _can_manage_members(church):
        return jsonify({"ok": False, "error": "Only the assigned pastor can edit member accounts."}), 403

    data = request.get_json(silent=True) or {}

    sheet_row = _safe_sheet_row(data.get("sheet_row"))
    name = str(data.get("name") or "").strip()
    bday = str(data.get("bday") or "").strip()
    username = str(data.get("username") or "").strip()
    password = str(data.get("password") or "").strip()

    if not sheet_row:
        return jsonify({"ok": False, "error": "Please select a member account."}), 400

    if not _member_row_belongs_to_church(sheet_row, church):
        return jsonify({"ok": False, "error": "This member account does not belong to this church."}), 403

    if not name or not bday or not username or not password:
        return jsonify({"ok": False, "error": "Name, birthday, username, and password are required."}), 400

    row = [
        name,
        bday,
        str(church.get("church_name") or "").strip(),
        str(church.get("church_address") or "").strip(),
        str(church.get("area_number") or "").strip(),
        str(church.get("pastor_name") or "").strip(),
        username,
        password,
    ]

    try:
        ws = _get_members_ws()
        ws.update(f"A{sheet_row}:H{sheet_row}", [row], value_input_option="USER_ENTERED")
    except Exception as e:
        return jsonify({"ok": False, "error": f"Failed to update account: {e}"}), 500

    return jsonify({
        "ok": True,
        "username": username,
        "password": password,
    })


@bp.route("/church-progress/<church_id>/member/delete", methods=["POST"])
def member_delete(church_id: str):
    church = _church_info(church_id)

    if not church:
        return jsonify({"ok": False, "error": "Church not found."}), 404

    _enforce_access(church)

    if not _can_manage_members(church):
        return jsonify({"ok": False, "error": "Only the assigned pastor can delete member accounts."}), 403

    data = request.get_json(silent=True) or {}
    sheet_row = _safe_sheet_row(data.get("sheet_row"))

    if not sheet_row:
        return jsonify({"ok": False, "error": "Please select a member account."}), 400

    if not _member_row_belongs_to_church(sheet_row, church):
        return jsonify({"ok": False, "error": "This member account does not belong to this church."}), 403

    try:
        ws = _get_members_ws()
        try:
            ws.delete_rows(sheet_row)
        except AttributeError:
            ws.delete_row(sheet_row)
    except Exception as e:
        return jsonify({"ok": False, "error": f"Failed to delete account: {e}"}), 500

    return jsonify({"ok": True})


def register_church_progress(app):
    app.register_blueprint(bp)