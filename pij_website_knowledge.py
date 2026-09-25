"""
Pij context provider for District 4 Tool.

EXPERIMENTAL ARCHITECTURE:
- Gemini interprets and answers ALL Pij questions.
- Flask does not catch attendance/schedule/birthday/progress/how-to questions.
- Flask supplies a sanitized, role-authorized database snapshot plus website manual.
- Passwords, login/security logs, API secrets and service-account credentials are NEVER sent.
- Pastor's Resources ebook passages are supplied separately by the Flask-controlled library retrieval layer.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from flask import session

PH_TZ = timezone(timedelta(hours=8))

WEBSITE_MANUAL = r"""
DISTRICT 4 TOOL — OPERATIONAL WEBSITE KNOWLEDGE

GENERAL
- Pij may know and explain how the full District 4 Tool works. This manual describes real routes, controls and workflows from the current project.
- Website knowledge is broader than data permission: Pij may explain a feature even when the current user cannot use it, but must clearly respect the logged-in role and never claim a restricted action is available to that user.
- The authenticated login account determines identity and permission. Temporary working selections never replace the true login identity.
- Pij may combine the steps below into natural guidance and may reason about which feature fits the user's goal.
- Pij does not directly click buttons or submit forms. It guides the user unless backend context explicitly confirms an action occurred.

SIDE MENU / MAIN PAGES
- Bulletin Board: shared landing/information page after login.
- Pastor's Tool: available to Pastor and AO accounts; monthly/Sunday church reporting workspace.
- Church Progress: available to a pastor for the pastor's assigned church; authorized overseers may reach church progress through permitted working contexts.
- AO Tool: available to Area Overseer/Sub-Area Overseer accounts.
- DO Tool: currently marked Pending.
- Prayer Request, Schedules, Church Finder and Pastor's Resources are available from the main menu to logged-in users.
- Event Registration and About Developer are currently marked Pending in the menu/project.

BULLETIN BOARD
- Shows the current bulletin feed, announcements and other district information.
- It can also surface approved prayer-related posts and report-recognition information according to the existing bulletin logic.
- The Bulletin Board is a viewing/landing page; AO announcement management itself is under AO Tool.

PASTOR'S TOOL — MONTHLY REPORT WORKFLOW
- A pastor works on the church attached to the authenticated pastor account.
- An authorized AO/Sub-AO can open Pastor's Tool and select a permitted pastor/church as a temporary working context while remaining an overseer.
- Choose Month and Year, then press Go.
- The month displays every Sunday as a card. A Sunday card is Complete (green) or Missing Data/incomplete.
- Open each Sunday card to enter/edit that Sunday's report.
- Sunday report fields include attendance (Adult, Young People/Youth, Children) and financial/report fields such as Church Tithes, Offering, Personal Tithes, Mission Offering and calculated/sent amount where applicable.
- The month also has a Church Progress card. Open it to complete ministry data such as Received Jesus/Christ, Existing Bible Study, New Bible Study, Water Baptized, Holy Spirit Baptized, Children's Dedication and Healed.
- Submit/Resubmit becomes enabled only when ALL Sunday reports for the selected month are complete AND Church Progress is complete.
- Statuses shown on Pastor's Tool include Not submitted, Pending AO approval, and Approved by AO.
- Once a month is approved by AO, Pastor's Tool does not allow that month to be submitted/resubmitted again.
- When a report is available, Download Report prepares the monthly PDF and allows the user to download it.

AO TOOL — MAIN MENU
- AO Tool is the management hub for Area Overseer/Sub-Area Overseer workflows.
- Main controls are: Church Status, Area Progress Monitor, Create Account, Edit Account, Prayer Request Approval, and Announcements.
- The AO/Sub-AO sees/manages only accounts/churches within the backend-authorized area/sub-area scope.

AO TOOL → CHURCH STATUS — REPORT APPROVAL
- THIS IS THE CORRECT MONTHLY REPORT APPROVAL PATH FOR AN AREA OVERSEER:
  1. Open AO Tool.
  2. Open Church Status.
  3. Choose the Year if needed.
  4. Open/select the Month.
  5. In the Church List, select the church whose submitted report will be reviewed.
  6. Review the church's monthly totals/averages and status in the church detail modal.
  7. If the report is submitted and not yet approved, press Approve.
  8. The church becomes Approved after the backend approval succeeds.
- In short: AO Tool → Church Status → Month → Church → Approve.
- Church status colors: Approved = green, Submitted/Pending = yellow, No Submission = red.
- A month heading turns green only when all churches have submitted for that month.
- Church detail can show total Amount to Send, Tithes, Offering, Personal Tithes, Mission Offering, average attendance, and average ministry figures.
- Church Status also includes AO Personal Tithes / Sub AO Personal Tithes entry (AOPT), depending on role.
- Print options include Main Print, Late Print and Refresh Print, followed by the report generation/download workflow when available.

AO TOOL → AREA PROGRESS MONITOR
- The Area Progress Monitor is a dashboard for an AO/Sub-AO's authorized churches.
- It supports time/period filters, a single-month selection and an All/individual church filter.
- It displays financial, attendance, ministry and reporting-status information, charts, insights and church details.
- Finance/attendance/ministry metrics can be changed from the dashboard controls.
- Church detail views can be opened from the dashboard.
- Manual remarks can be saved for an authorized church.
- The dashboard has notification/seen-state behavior for its generated notifications.

AO TOOL → CREATE / EDIT ACCOUNT
- Create Account creates a pastor account in the AO's authorized area/sub-area using the account form.
- Edit Account: open AO Tool → Edit Account → choose a Pastor/Church → edit the displayed account fields → Save Changes.
- Edit Account also has Delete Account for an authorized account after confirmation.
- Password values exist in the account-management UI, but Pij is never given actual passwords and must never guess or expose them.

AO TOOL → PRAYER REQUEST APPROVAL
- Open AO Tool → Prayer Request Approval.
- The page lists pending prayer requests within the AO's management scope.
- An AO can Approve an individual request, Reject an individual request, or Approve All pending in-scope requests.
- Reject currently removes the prayer-request row.

AO TOOL → ANNOUNCEMENTS
- Open AO Tool → Announcements.
- AO can Submit Announcement and can edit/delete announcements within the authorized area/sub-area scope.
- New announcements require a title and announcement body and are saved with area/sub-area and author information.

CHURCH PROGRESS DASHBOARD
- Church Progress is a year-based church dashboard.
- Sections include Report Submission/Faithfulness, Financial, Attendance, Ministry and member-account management where authorized.
- Attendance covers Adult, Youth/Young People and Children.
- Ministry covers Received Jesus, Existing/New Bible Study, Water/Holy Spirit Baptized, Children's Dedication and Healed.
- The assigned pastor can create, edit and delete member accounts for that church. Backend checks ensure a member belongs to that church before update/delete.
- Member account creation requires name and birthday; the system generates a username and password.

PRAYER REQUEST — USER WORKFLOW
- Prayer Request landing page provides Write Prayer Request, Prayer Request Status and Answered Prayer Request.
- Write Prayer Request requires a title and request text. A new request is Pending until AO approval.
- Prayer Request Status shows the logged-in user's non-answered requests and their status.
- The request owner (or AO where permitted) can edit/delete a request.
- A normal owner can mark a request Answered only after it is Approved; AO has broader management authority according to backend checks.
- Answered Prayer Request shows answered items for the logged-in user.

SCHEDULES
- Schedules has two viewing modes: District schedule and Chain Prayer schedule.
- Users can navigate the calendar by month/year and open a day/event to see schedule details.
- Schedule search can filter by Area, Church and Pastor and can return matching District and Chain Prayer records.
- District schedule records can include church, address, pastor, contact, start/end date, activity type, note, joining names, theme and text.
- Relative date questions (today, tomorrow/ugma/bukas, this Saturday/Sunday, this week, next week, this month) should be interpreted using the supplied CURRENT PHILIPPINE DATE/TIME.
- Logged-in users can use “I'm Joining” on a district activity and provide/remember a joining name.
- Public schedule detail editing permits Theme/Text/Note editing with editor-name audit where the interface exposes it.
- District Secretary management requires the configured secretary access flow. With authorization, the secretary can Create Schedule, Edit Schedule and Delete Schedule.
- Create Schedule supports Thanksgiving mode and Other schedule mode.
- Thanksgiving creation selects Area + Church, fills account-related church/pastor/contact details, requires an activity date and a valid Google Maps link, and can include theme/text/note.
- Other schedule creation requires at least church name, start date and activity type; it can include end date, pastor/contact/address/note and a valid Google Maps link.

CHURCH FINDER
- Church Finder is the church directory/map built from pastor/church accounts.
- It can show area number, church ID/name, church address, pastor, contact and map coordinates/pin when available.
- It supports map/directory use and a map export route.
- Do not invent a missing pin, coordinate, address or contact detail.

PASTOR'S RESOURCES — PUBLIC DIGITAL LIBRARY
- Pastor's Resources is the logged-in digital ebook library backed by the synchronized Google Drive catalog/database.
- Users can search/browse books and filter by supported modes/options such as author/category.
- It supports Continue Reading and My Library.
- The built-in reader supports PDF and EPUB. Reader state/progress is stored per logged-in user.
- Users can favorite books, mark completion, use bookmarks, highlights/annotations/notes, and reading-session/progress features supported by the reader.
- Pij can deep-link to a retrieved PDF page or EPUB section using only approved links supplied by the retrieval layer.
- Library content/index synchronization is an administrator operation. Sync Books refreshes the visible Drive catalog and then starts/queues an incremental Pij AI knowledge refresh.
- Administrator database/details/edit/hide/restore tools exist but remain protected by backend authorization.

PRIVATE SERMON EBOOKS
- The separate Sermon eBooks area under Pastor's Resources is a PRIVATE created-sermon collection restricted by backend authorization to the configured sermon admin account.
- It supports private PDF sync, list/search, reader state, highlights/annotations, bookmarks, reader search and download.
- Pij may retrieve this private collection only when Flask explicitly authorizes the logged-in account. Other users must never be told its private contents.
- Publicly published sermon ebooks that live in normal Pastor's Resources are NOT private and may be searched like other public library books.

TEMPORARY EDIT
- Temporary Edit is a token-protected workflow rather than a normal side-menu workflow.
- The user form can propose edits to church/account details such as church address, name, contact, birthday and Google pin; submission requires editor name, actual changes and a selfie.
- Requests are stored Pending.
- The token-protected admin page can mark requests Approved or Rejected. Approved changes are applied to Google Sheets; rejected items are discarded.
- Pij may explain this workflow but must not reveal or invent private access tokens.

EVENT REGISTRATION / DO TOOL / ABOUT DEVELOPER
- Event Registration currently exists as a page but is marked Pending in the menu/project.
- DO Tool currently returns a Pending page.
- About Developer is currently Pending.
- Do not invent controls for pending pages.

APPROVED INTERNAL WEBSITE LINKS
- Bulletin Board: /bulletin
- Pastor's Tool: /pastor-tool
- Church Progress: /church-progress
- AO Tool: /ao-tool
- AO Church Status: /ao-tool/church-status
- Area Progress Monitor: /ao-tool/area-progress-monitor
- AO Create Account: /ao-tool/create-account
- AO Prayer Request Approval: /ao-tool/prayer-requests
- Prayer Request: /prayer-request
- Prayer Request — Write: /prayer-request/write
- Prayer Request — Status: /prayer-request/status
- Prayer Request — Answered: /prayer-request/answered
- Schedules: /schedules
- Church Finder: /church-finder
- Pastor's Resources: /pastor-resources
- Pastor's Resources — My Library: /pastor-resources/my-library
- Private Sermon eBooks: /pastor-resources/sermon-ebooks (only when the current account is authorized by Flask)
- Temporary Edit: /temp-edit (token protection still applies; never invent a token)
- Event Registration: /event-registration
- DO Tool: /do-tool
- These are approved same-site destinations Pij may turn into clickable Markdown links.
- Use only the exact route supplied here or an exact book/page link supplied by the library retrieval layer. Never invent IDs, tokens or query parameters.

HOW PIJ SHOULD GUIDE USERS
- Use the real workflow above and give the shortest correct path first.
- If the user asks how to approve a monthly church report as AO, answer AO Tool → Church Status → Month → Church → Approve; do not redirect that approval workflow to Pastor's Tool.
- If the user is already on the correct page, continue from that page instead of sending them back through the menu unnecessarily.
- Keep actual labels exactly when useful: Pastor's Tool, Church Progress, Church Status, Area Progress Monitor, Prayer Request Approval, Schedules, Church Finder, Pastor's Resources.
- Pij may reason about which known feature best solves the user's goal, but must not invent a control that is absent from this manual.

LANGUAGE
- The current-message language lock supplied by ai_assistant.py controls the response language for each turn.
- English current question -> English answer.
- Cebuano/Bisaya current question -> Cebuano/Bisaya answer.
- Tagalog/Filipino current question -> Tagalog/Filipino answer.
- A truly mixed current question may receive a natural matching mix.
"""


def _app():
    # app.py imports ai_assistant during startup, so keep this lazy.
    import app as appmod
    return appmod


def current_identity() -> dict[str, str]:
    """Original authenticated identity always wins over Pastor's Tool working context."""
    role = str(session.get("ao_role") or session.get("role") or "").strip()
    role_low = role.lower()

    if session.get("ao_logged_in") or role_low in {
        "area overseer", "ao", "sub area overseer", "subarea overseer",
        "district overseer", "do"
    }:
        kind = "do" if role_low in {"district overseer", "do"} else (
            "sub_ao" if "sub area" in role_low or "subarea" in role_low else "ao"
        )
        return {
            "kind": kind,
            "role": role or ("District Overseer" if kind == "do" else "Area Overseer"),
            "username": str(session.get("ao_username") or session.get("username") or "").strip(),
            "name": str(session.get("ao_name") or "").strip(),
            "church": str(session.get("ao_church_id") or "").strip(),
            "church_address": "",
            "area": str(session.get("ao_area_number") or "").strip(),
            "sub_area": str(session.get("ao_sub_area") or "").strip(),
        }

    if session.get("pastor_logged_in"):
        return {
            "kind": "pastor",
            "role": "Pastor",
            "username": str(session.get("pastor_username") or session.get("username") or "").strip(),
            "name": str(session.get("pastor_name") or "").strip(),
            "church": str(session.get("pastor_church_id") or session.get("pastor_church_address") or "").strip(),
            "church_address": str(session.get("pastor_church_address") or "").strip(),
            "area": str(session.get("pastor_area_number") or "").strip(),
            "sub_area": "",
        }

    return {
        "kind": "member" if session.get("member_logged_in") else "unknown",
        "role": role or "Member",
        "username": str(session.get("username") or "").strip(),
        "name": "",
        "church": "",
        "church_address": "",
        "area": "",
        "sub_area": "",
    }


def page_name(path: str) -> str:
    p = (path or "").split("?", 1)[0].rstrip("/") or "/"
    mapping = [
        ("/ao-tool/area-progress-monitor", "Area Progress Monitor"),
        ("/ao-tool/church-status", "AO Church Status"),
        ("/ao-tool/prayer-requests", "AO Prayer Request Approval"),
        ("/ao-tool/create-account", "AO Create Account"),
        ("/pastor-resources/sermon-ebooks", "Private Sermon eBooks"),
        ("/pastor-resources/my-library", "Pastor's Resources — My Library"),
        ("/pastor-resources/read", "Pastor's Resources Reader"),
        ("/church-progress", "Church Progress"),
        ("/pastor-tool", "Pastor's Tool"),
        ("/church-finder", "Church Finder"),
        ("/prayer-request/write", "Write Prayer Request"),
        ("/prayer-request/status", "Prayer Request Status"),
        ("/prayer-request/answered", "Answered Prayer Request"),
        ("/prayer-request", "Prayer Request"),
        ("/event-registration", "Event Registration"),
        ("/temp-edit", "Temporary Edit"),
        ("/schedules", "Schedules"),
        ("/bulletin", "Bulletin Board"),
        ("/pastor-resources", "Pastor's Resources"),
        ("/ao-tool", "AO Tool"),
        ("/do-tool", "DO Tool"),
    ]
    for prefix, label in mapping:
        if p == prefix or p.startswith(prefix + "/"):
            return label
    return "District 4 Tool"


def _rowdict(row, fields):
    return {f: row[f] for f in fields if f in row.keys()}


def _authorized_pastor_rows(db, ident):
    rows = db.execute("""
        SELECT username, name, church_address, age, sex, birthday, position, sub_area,
               google_pin_location, latitude, longitude
        FROM sheet_accounts_cache
        WHERE LOWER(TRIM(COALESCE(position,'')))='pastor'
        ORDER BY CAST(COALESCE(age,'0') AS INTEGER), sex, church_address
    """).fetchall()

    if ident["kind"] == "do":
        return rows

    if ident["kind"] in {"ao", "sub_ao"}:
        area = str(ident["area"] or "").strip()
        sub = str(ident["sub_area"] or "").strip().lower()
        scoped = [r for r in rows if str(r["age"] or "").strip() == area]
        if ident["kind"] == "sub_ao":
            scoped = [r for r in scoped if str(r["sub_area"] or "").strip().lower() == sub]
        return scoped

    if ident["kind"] == "pastor":
        username = ident["username"].lower()
        church = ident["church"].lower()
        address = ident["church_address"].lower()
        return [
            r for r in rows
            if str(r["username"] or "").strip().lower() == username
            or str(r["sex"] or "").strip().lower() == church
            or (address and str(r["church_address"] or "").strip().lower() == address)
        ]

    return []


def _authorized_account_rows(db, ident):
    """
    Accounts snapshot for directory/birthday reasoning.
    SECURITY: password and contact are intentionally excluded.
    """
    rows = db.execute("""
        SELECT username, name, church_address, age, sex, birthday, position, sub_area,
               google_pin_location, latitude, longitude
        FROM sheet_accounts_cache
        ORDER BY CAST(COALESCE(age,'0') AS INTEGER), position, name
    """).fetchall()

    if ident["kind"] == "do":
        return rows
    if ident["kind"] in {"ao", "sub_ao"}:
        area = str(ident["area"] or "").strip()
        sub = str(ident["sub_area"] or "").strip().lower()
        scoped = [r for r in rows if str(r["age"] or "").strip() == area]
        if ident["kind"] == "sub_ao":
            scoped = [r for r in scoped if str(r["sub_area"] or "").strip().lower() == sub]
        return scoped
    if ident["kind"] == "pastor":
        # Keep broad personal data narrow for pastors: only the pastor's own account row.
        username = ident["username"].lower()
        return [r for r in rows if str(r["username"] or "").strip().lower() == username]
    return []


def _authorized_report_rows(db, ident, pastors):
    church_keys = set()
    for r in pastors:
        for key in (r["sex"], r["church_address"]):
            val = str(key or "").strip().lower()
            if val:
                church_keys.add(val)

    if not church_keys:
        return []

    # Newest first; cap protects Gemini quota while still providing a substantial live snapshot.
    rows = db.execute("""
        SELECT year, month, activity_date, church, pastor, address,
               adult, youth, children,
               tithes, offering, personal_tithes, mission_offering,
               received_jesus, existing_bible_study, new_bible_study,
               water_baptized, holy_spirit_baptized, childrens_dedication, healed,
               amount_to_send, status, report_status
        FROM sheet_report_cache
        ORDER BY year DESC, month DESC, activity_date DESC, sheet_row DESC
        LIMIT 1800
    """).fetchall()

    return [
        r for r in rows
        if str(r["church"] or "").strip().lower() in church_keys
        or str(r["address"] or "").strip().lower() in church_keys
    ]


def _scoped_announcements(db, ident):
    rows = db.execute("""
        SELECT title, announcement, announcement_date, area, sub_area, author_name
        FROM sheet_announcement_cache
        ORDER BY sheet_row DESC
        LIMIT 100
    """).fetchall()
    if ident["kind"] == "do":
        return rows
    if ident["kind"] in {"ao", "sub_ao", "pastor"}:
        area = str(ident["area"] or "").strip()
        sub = str(ident["sub_area"] or "").strip().lower()
        out = []
        for r in rows:
            ra = str(r["area"] or "").strip()
            rs = str(r["sub_area"] or "").strip().lower()
            if ra and area and ra != area:
                continue
            if ident["kind"] == "sub_ao" and rs and sub and rs != sub:
                continue
            out.append(r)
        return out
    return []


def authorized_database_snapshot() -> dict:
    """
    Return sanitized database information Gemini may reason over.
    Flask decides the scope before Gemini sees any row.
    """
    appmod = _app()
    try:
        appmod.ensure_sheet_cache_loaded()
    except Exception:
        pass
    try:
        appmod.ensure_schedule_cache_loaded()
    except Exception:
        pass

    db = appmod.get_db()
    ident = current_identity()
    pastors = _authorized_pastor_rows(db, ident)
    accounts = _authorized_account_rows(db, ident)
    reports = _authorized_report_rows(db, ident, pastors)

    schedules = db.execute("""
        SELECT church_name, church_address, pastor_name, contact_number,
               activity_date_start, activity_date_end, activity_type, note,
               joining, theme, text
        FROM sheet_district_schedule_cache
        ORDER BY activity_date_start ASC
        LIMIT 500
    """).fetchall()

    chain = db.execute("""
        SELECT church_name_assigned, pastor_name, prayer_date
        FROM sheet_chain_prayer_schedule_cache
        ORDER BY prayer_date ASC
        LIMIT 500
    """).fetchall()

    return {
        "scope_note": (
            "This is a SANITIZED, AUTHORIZED snapshot prepared by Flask. "
            "Do not assume records outside this snapshot are accessible."
        ),
        "accounts": [
            _rowdict(r, [
                "username", "name", "church_address", "age", "sex", "birthday",
                "position", "sub_area", "google_pin_location", "latitude", "longitude"
            ]) for r in accounts
        ],
        "pastor_church_directory_in_scope": [
            _rowdict(r, [
                "username", "name", "church_address", "age", "sex", "birthday",
                "position", "sub_area", "google_pin_location", "latitude", "longitude"
            ]) for r in pastors
        ],
        "report_rows_in_scope": [
            _rowdict(r, [
                "year", "month", "activity_date", "church", "pastor", "address",
                "adult", "youth", "children", "tithes", "offering", "personal_tithes",
                "mission_offering", "received_jesus", "existing_bible_study",
                "new_bible_study", "water_baptized", "holy_spirit_baptized",
                "childrens_dedication", "healed", "amount_to_send", "status", "report_status"
            ]) for r in reports
        ],
        "district_schedules": [
            _rowdict(r, [
                "church_name", "church_address", "pastor_name", "contact_number",
                "activity_date_start", "activity_date_end", "activity_type", "note",
                "joining", "theme", "text"
            ]) for r in schedules
        ],
        "chain_prayer_schedule": [
            _rowdict(r, ["church_name_assigned", "pastor_name", "prayer_date"]) for r in chain
        ],
        "announcements_in_scope": [
            _rowdict(r, ["title", "announcement", "announcement_date", "area", "sub_area", "author_name"])
            for r in _scoped_announcements(db, ident)
        ],
        "not_in_broad_snapshot": [
            "passwords",
            "API keys/service-account credentials",
            "login/security logs",
            "private prayer-request text",
            "Pastor's Resources ebooks",
        ],
    }


def safe_context_for_gemini(message: str, current_path: str = "", page_title: str = "") -> str:
    ident = current_identity()
    now = datetime.now(PH_TZ)
    snapshot = authorized_database_snapshot()

    # Temporary Pastor's Tool selection is useful context, but never identity.
    working_pastor = str(session.get("pastor_username") or "").strip() if ident["kind"] in {"ao", "sub_ao", "do"} else ""

    return (
        "CURRENT DISTRICT 4 CONTEXT\n"
        f"- Philippine date/time now: {now.isoformat()}\n"
        f"- Authenticated role: {ident['role']}\n"
        f"- Authenticated username: {ident['username'] or 'not supplied'}\n"
        f"- Authenticated display name: {ident['name'] or 'not supplied'}\n"
        f"- Authorized own church/church id: {ident['church'] or 'not supplied'}\n"
        f"- Authorized area: {ident['area'] or 'not supplied'}\n"
        f"- Authorized sub-area: {ident['sub_area'] or 'not supplied'}\n"
        f"- Current page: {page_title or page_name(current_path)}\n"
        f"- Current path: {current_path or 'unknown'}\n"
        f"- Pastor's Tool temporary working pastor (NOT login identity): {working_pastor or 'none'}\n\n"
        + WEBSITE_MANUAL
        + "\n\nAUTHORIZED SANITIZED DATABASE SNAPSHOT\n"
        + json.dumps(snapshot, ensure_ascii=False, default=str, separators=(",", ":"))
    )
