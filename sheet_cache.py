"""Google Sheets is master; this module only rebuilds validated SQLite mirrors."""

import os
import threading
import time
from contextlib import contextmanager


DATASETS = {
    "Accounts": "sheet_accounts_cache",
    "Report": "sheet_report_cache",
    "AOPT": "sheet_aopt_cache",
    "PrayerRequest": "sheet_prayer_request_cache",
    "DistrictSchedule": "sheet_district_schedule_cache",
    "ChainPrayerSchedules": "sheet_chain_prayer_schedule_cache",
    "Anouncement": "sheet_announcement_cache",
    "Members Account": "sheet_members_account_cache",
}

REQUIRED_HEADERS = {
    "Accounts": ["Name", "UserName", "Password", "Church Address", "Position",
                 ("Area Number", "Age"), ("Church ID", "Sex")],
    "Report": ["activity_date", "church", "pastor", "address", "status",
               "adult", "youth", "children", "tithes", "offering", "personal tithes",
               "mission offering", "received jesus", "existing bible study",
               "new bible study", "water baptized", "holy spirit baptized",
               "childrens dedication", "healed", "amount to send"],
    "AOPT": ["Month", "Amount"],
    "PrayerRequest": ["Request ID", "Church Name", "Submitted By", "Prayer Request Title",
                      "Prayer Request Date", "Prayer Request", "Status"],
    "DistrictSchedule": ["Church Name", "Church Address", "Pastor's Name", "Contact Number",
                         "Activity Date Start", "Activity Date End", "Activity Type", "Note"],
    "ChainPrayerSchedules": ["ChurchNameAssigned", "Date"],
    "Anouncement": ["Title", "Announcement", "Date", "Area"],
    "Members Account": ["Name", ("BDay", "Birthday"), "Church ID", "Church Address",
                        "Area Number", "Pastor", "UserName", "Password"],
}


class CacheSyncError(RuntimeError):
    pass


def init_cache_tables(db):
    db.execute("""CREATE TABLE IF NOT EXISTS sheet_cache_sync_state (
        dataset TEXT PRIMARY KEY, last_sync TEXT NOT NULL
    )""")
    db.execute("""CREATE TABLE IF NOT EXISTS sheet_cache_pending_refresh (
        dataset TEXT PRIMARY KEY, requested_at TEXT NOT NULL
    )""")
    db.execute("""CREATE TABLE IF NOT EXISTS sheet_members_account_cache (
        sheet_row INTEGER PRIMARY KEY, name TEXT, bday TEXT, church_id TEXT,
        church_address TEXT, area_number TEXT, pastor TEXT, username TEXT, password TEXT
    )""")
    db.execute("CREATE INDEX IF NOT EXISTS idx_members_cache_church ON sheet_members_account_cache(church_id)")
    # Existing installations already have seven mirrors. Preserve those rather than
    # forcing a full Google read on deployment. Members is initialized separately.
    old_sync = db.execute("SELECT last_sync FROM sync_state WHERE id = 1").fetchone()
    if old_sync and old_sync[0]:
        db.executemany(
            "INSERT OR IGNORE INTO sheet_cache_sync_state(dataset,last_sync) VALUES (?,?)",
            [(name, old_sync[0]) for name in DATASETS if name != "Members Account"],
        )


def _find_col(headers, wanted):
    wanted = str(wanted).strip().lower()
    return next((i for i, header in enumerate(headers)
                 if str(header).strip().lower() == wanted), None)


def validate_values(name, values, parse_sheet_date):
    if not isinstance(values, list) or not values or not isinstance(values[0], list):
        raise CacheSyncError(f"{name}: missing header row; existing cache was kept.")
    headers = [str(header).strip().lower() for header in values[0]]
    named = [header for header in headers if header]
    if len(named) != len(set(named)):
        raise CacheSyncError(f"{name}: duplicate headers; existing cache was kept.")
    for required in REQUIRED_HEADERS[name]:
        choices = required if isinstance(required, tuple) else (required,)
        if not any(header.lower() in headers for header in choices):
            raise CacheSyncError(f"{name}: required header {' / '.join(choices)} is missing.")
    key = {"Accounts": "UserName", "PrayerRequest": "Request ID"}.get(name)
    seen = set()
    for row_num, row in enumerate(values[1:], start=2):
        if not isinstance(row, list):
            raise CacheSyncError(f"{name}: invalid row {row_num}.")
        if not any(str(cell).strip() for cell in row):
            continue
        if key:
            index = _find_col(values[0], key)
            value = str(row[index]).strip() if index < len(row) else ""
            if value and value in seen:
                raise CacheSyncError(f"{name}: duplicate {key} at row {row_num}.")
            if name == "PrayerRequest" and not value:
                raise CacheSyncError(f"{name}: missing Request ID at row {row_num}.")
            seen.add(value)
        if name == "Report":
            index = _find_col(values[0], "activity_date")
            value = row[index] if index < len(row) else ""
            if not parse_sheet_date(value):
                raise CacheSyncError(f"Report: invalid activity_date at row {row_num}.")


_sync_lock = threading.RLock()


@contextmanager
def _locked_sync(database_path):
    """Serialize fetch + commit across threads and Gunicorn workers, on either OS."""
    with _sync_lock:
        with open(os.fspath(database_path) + ".sheet-cache.lock", "a+b") as lock_file:
            lock_file.seek(0, os.SEEK_END)
            if lock_file.tell() == 0:
                lock_file.write(b"0")
                lock_file.flush()
            deadline = time.monotonic() + 30
            while True:
                try:
                    lock_file.seek(0)
                    if os.name == "nt":
                        import msvcrt
                        msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
                    else:
                        import fcntl
                        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except OSError as exc:
                    if time.monotonic() >= deadline:
                        raise CacheSyncError("Another cache sync is running. Please try again.") from exc
                    time.sleep(0.05)
            try:
                yield
            finally:
                lock_file.seek(0)
                if os.name == "nt":
                    msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def sync_cache(db, database_path, client_factory, parse_float, parse_sheet_date, now,
               datasets=None, only_missing=False):
    names = tuple(DATASETS if datasets is None else dict.fromkeys(datasets))
    if not names or any(name not in DATASETS for name in names):
        raise ValueError("Choose one or more known Google Sheets datasets.")
    with _locked_sync(database_path):
        if only_missing:
            initialized = {row[0] for row in db.execute("SELECT dataset FROM sheet_cache_sync_state")}
            names = tuple(name for name in names if name not in initialized)
            if not names:
                return {}
        pending_at_fetch = dict(db.execute("SELECT dataset, requested_at FROM sheet_cache_pending_refresh"))
        prepared = {}
        try:
            spreadsheet = client_factory().open("District4 Data")
            for name in names:
                values = spreadsheet.worksheet(name).get_all_values()
                validate_values(name, values, parse_sheet_date)
                prepared[name] = PARSERS[name](values, parse_float, parse_sheet_date)
        except CacheSyncError:
            raise
        except Exception as exc:
            raise CacheSyncError("Unable to fetch Google Sheets; existing cache was kept.") from exc

        # Every fetch, header check and conversion finished before touching caches.
        # A savepoint also preserves any caller transaction if an INSERT fails.
        db.execute("SAVEPOINT refresh_sheet_cache")
        counts = {}
        try:
            timestamp = now()
            for name, statements in prepared.items():
                table = DATASETS[name]  # Fixed allowlist, never user-provided SQL.
                db.execute(f"DELETE FROM {table}")
                for sql, parameters in statements:
                    db.execute(sql, parameters)
                counts[name] = db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                db.execute("INSERT OR REPLACE INTO sheet_cache_sync_state VALUES (?,?)", (name, timestamp))
                # A write can finish while a full sync is fetching older values.
                # Keep newer recovery requests rather than clearing them with
                # that earlier snapshot, even if the writer's worker exits.
                if name in pending_at_fetch:
                    db.execute("DELETE FROM sheet_cache_pending_refresh WHERE dataset = ? AND requested_at = ?",
                               (name, pending_at_fetch[name]))
            if set(names) == set(DATASETS):
                db.execute("UPDATE sync_state SET last_sync = ? WHERE id = 1", (timestamp,))
            db.execute("RELEASE SAVEPOINT refresh_sheet_cache")
        except Exception as exc:
            db.execute("ROLLBACK TO SAVEPOINT refresh_sheet_cache")
            db.execute("RELEASE SAVEPOINT refresh_sheet_cache")
            raise CacheSyncError("Cache update failed; existing cache was kept.") from exc
        return counts


def _prepare_accounts(values, parse_float, parse_sheet_date):
    statements = []
    def add(sql, parameters):
        statements.append((sql, parameters))
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
        i_pin = _find_col(headers, "GooglePinLocation")
        i_lat = _find_col(headers, "Latitude")
        i_lng = _find_col(headers, "Longitude")

        def cell(row, idx):
            if idx is None:
                return ""
            if idx < len(row):
                return row[idx]
            return ""

        for r in range(1, len(values)):
            row = values[r]

            username = str(cell(row, i_user)).strip()
            password = str(cell(row, i_pass)).strip()
            full_name = str(cell(row, i_name)).strip()
            church_address = str(cell(row, i_addr)).strip()
            area_number = str(cell(row, i_age)).strip()
            church_id = str(cell(row, i_sex)).strip()
            contact = str(cell(row, i_contact)).strip()
            birthday = str(cell(row, i_bday)).strip()
            position = str(cell(row, i_pos)).strip()
            sub_area = str(cell(row, i_sub)).strip()
            google_pin_location = str(cell(row, i_pin)).strip()
            latitude = str(cell(row, i_lat)).strip()
            longitude = str(cell(row, i_lng)).strip()

            # keep rows that have the search essentials even if username/password are blank
            if not area_number and not church_id and not full_name and not church_address:
                continue

            add(
                """
                INSERT OR REPLACE INTO sheet_accounts_cache
                (username, name, church_address, password, age, sex, contact, birthday, position, sub_area, google_pin_location, latitude, longitude, sheet_row)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    username,
                    full_name,
                    church_address,
                    password,
                    area_number,
                    church_id,
                    contact,
                    birthday,
                    position,
                    sub_area,
                    google_pin_location,
                    latitude,
                    longitude,
                    r + 1,
                ),
            )
    return statements


def _prepare_report(values, parse_float, parse_sheet_date):
    statements = []
    def add(sql, parameters):
        statements.append((sql, parameters))
    if values and len(values) >= 2:
        headers = values[0]

        i_activity = _find_col(headers, "activity_date")

        # Church approval status (Approved / Pending) for Church Status colors
        i_status = _find_col(headers, "Status")
        if i_status is None:
            i_status = _find_col(headers, "status")

        # Print workflow status (MainPrint / LatePrint / Received)
        i_report_status = _find_col(headers, "ReportStatus")

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

        for r in range(1, len(values)):
            row = values[r]
            activity = str(cell(row, i_activity)).strip()
            if not activity:
                continue
            d = parse_sheet_date(activity)
            if not d:
                continue

            add(
                """
                INSERT INTO sheet_report_cache (
                    sheet_row, year, month, activity_date,
                    church, pastor, address,
                    adult, youth, children,
                    tithes, offering, personal_tithes, mission_offering,
                    received_jesus, existing_bible_study, new_bible_study,
                    water_baptized, holy_spirit_baptized, childrens_dedication, healed,
                    amount_to_send, status, report_status
                ) VALUES (
                    ?, ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?
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
                    str(cell(row, i_report_status)).strip(),
                ),
            )
    return statements


def _prepare_aopt(values, parse_float, parse_sheet_date):
    statements = []
    def add(sql, parameters):
        statements.append((sql, parameters))
    if values and len(values) >= 2:
        headers = values[0]
        i_month = _find_col(headers, "Month")
        i_amount = _find_col(headers, "Amount")
        i_area = _find_col(headers, "Area Number")
        if i_area is None:
            i_area = _find_col(headers, "Area")
        i_sub_area = _find_col(headers, "Sub Area")
        if i_sub_area is None:
            i_sub_area = _find_col(headers, "SubArea")

        def cell(row, idx):
            if idx is None:
                return ""
            if idx < len(row):
                return row[idx]
            return ""

        for r in range(1, len(values)):
            row = values[r]
            month_label = str(cell(row, i_month)).strip()
            if not month_label:
                continue
            amount_val = parse_float(cell(row, i_amount))
            area_number = str(cell(row, i_area)).strip()
            sub_area = str(cell(row, i_sub_area)).strip()
            add(
                """
                INSERT OR REPLACE INTO sheet_aopt_cache (month, area_number, sub_area, amount, sheet_row)
                VALUES (?, ?, ?, ?, ?)
                """,
                (month_label, area_number, sub_area, amount_val, r + 1),
            )
    return statements


def _prepare_prayer_requests(values, parse_float, parse_sheet_date):
    statements = []
    def add(sql, parameters):
        statements.append((sql, parameters))
    if values and len(values) >= 2:
        headers = values[0]

        i_church = _find_col(headers, "Church Name")
        i_submitted_by = _find_col(headers, "Submitted By")
        i_request_id = _find_col(headers, "Request ID")
        i_title = _find_col(headers, "Prayer Request Title")
        i_request_date = _find_col(headers, "Prayer Request Date")
        i_request_text = _find_col(headers, "Prayer Request")
        i_status = _find_col(headers, "status")
        if i_status is None:
            i_status = _find_col(headers, "status")
        i_praying = _find_col(headers, "Pastor's Praying")
        i_answered = _find_col(headers, "Answered Date")

        def cell(row, idx):
            if idx is None:
                return ""
            if idx < len(row):
                return row[idx]
            return ""

        for r in range(1, len(values)):
            row = values[r]
            req_id = str(cell(row, i_request_id)).strip()
            if not req_id:
                continue

            add(
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
    return statements


def _prepare_district_schedule(values, parse_float, parse_sheet_date):
    statements = []
    def add(sql, parameters):
        statements.append((sql, parameters))
    if values and len(values) >= 2:
        headers = values[0]

        i_church_name = _find_col(headers, "Church Name")
        i_church_address = _find_col(headers, "Church Address")
        i_pastor_name = _find_col(headers, "Pastor's Name")
        i_contact_number = _find_col(headers, "Contact Number")
        i_activity_start = _find_col(headers, "Activity Date Start")
        i_activity_end = _find_col(headers, "Activity Date End")
        i_activity_type = _find_col(headers, "Activity Type")
        i_note = _find_col(headers, "Note")
        i_joining = _find_col(headers, "Joining")
        i_theme = _find_col(headers, "Theme")
        i_text = _find_col(headers, "Text")

        def ds_cell(row, idx):
            if idx is None:
                return ""
            return row[idx].strip() if idx < len(row) else ""

        for rnum, row in enumerate(values[1:], start=2):
            church_name = ds_cell(row, i_church_name)
            activity_start = ds_cell(row, i_activity_start)

            if not church_name or not activity_start:
                continue

            add(
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
                    theme,
                    text,
                    sheet_row
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    ds_cell(row, i_theme),
                    ds_cell(row, i_text),
                    rnum,
                ),
            )
    return statements


def _prepare_chain_prayer_schedule(values, parse_float, parse_sheet_date):
    statements = []
    def add(sql, parameters):
        statements.append((sql, parameters))
    if values and len(values) >= 2:
        headers = values[0]

        i_church_name_assigned = _find_col(headers, "ChurchNameAssigned")
        i_pastor_name = _find_col(headers, "Pastor")
        i_prayer_date = _find_col(headers, "Date")

        def cp_cell(row, idx):
            if idx is None:
                return ""
            return row[idx].strip() if idx < len(row) else ""

        for rnum, row in enumerate(values[1:], start=2):
            church_name_assigned = cp_cell(row, i_church_name_assigned)
            pastor_name = cp_cell(row, i_pastor_name)
            prayer_date = cp_cell(row, i_prayer_date)

            if not church_name_assigned or not prayer_date:
                continue

            add(
                """
                INSERT INTO sheet_chain_prayer_schedule_cache (
                    church_name_assigned,
                    pastor_name,
                    prayer_date,
                    sheet_row
                )
                VALUES (?, ?, ?, ?)
                """,
                (
                    church_name_assigned,
                    pastor_name,
                    prayer_date,
                    rnum,
                ),
            )
    return statements


def _prepare_announcements(values, parse_float, parse_sheet_date):
    statements = []
    def add(sql, parameters):
        statements.append((sql, parameters))
    if values and len(values) >= 2:
        headers = values[0]
        i_title = _find_col(headers, "Title")
        i_announcement = _find_col(headers, "Announcement")
        i_date = _find_col(headers, "Date")
        i_area = _find_col(headers, "Area")
        i_sub = _find_col(headers, "SubArea")
        if i_sub is None:
            i_sub = _find_col(headers, "Sub Area")
        i_author_u = _find_col(headers, "Author Username")
        i_author_n = _find_col(headers, "Author Name")

        def ann_cell(row, idx):
            if idx is None:
                return ""
            return row[idx].strip() if idx < len(row) else ""

        for rnum, row in enumerate(values[1:], start=2):
            title = ann_cell(row, i_title)
            body = ann_cell(row, i_announcement)
            if not title and not body:
                continue
            add(
                """
                INSERT INTO sheet_announcement_cache (
                    title, announcement, announcement_date, area, sub_area,
                    author_username, author_name, sheet_row
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    title,
                    body,
                    ann_cell(row, i_date),
                    ann_cell(row, i_area),
                    ann_cell(row, i_sub),
                    ann_cell(row, i_author_u),
                    ann_cell(row, i_author_n),
                    rnum,
                ),
            )
    return statements


def _prepare_members(values, parse_float, parse_sheet_date):
    headers = values[0]
    columns = [_find_col(headers, name) for name in
               ("Name", "BDay", "Church ID", "Church Address", "Area Number", "Pastor", "UserName", "Password")]
    if columns[1] is None:
        columns[1] = _find_col(headers, "Birthday")
    statements = []
    for row_num, row in enumerate(values[1:], start=2):
        cells = [str(row[index]).strip() if index is not None and index < len(row) else ""
                 for index in columns]
        if cells[0] or cells[6]:
            statements.append(("""INSERT INTO sheet_members_account_cache
                (sheet_row,name,bday,church_id,church_address,area_number,pastor,username,password)
                VALUES (?,?,?,?,?,?,?,?,?)""", (row_num, *cells)))
    return statements


PARSERS = {
    'Accounts': _prepare_accounts,
    'Report': _prepare_report,
    'AOPT': _prepare_aopt,
    'PrayerRequest': _prepare_prayer_requests,
    'DistrictSchedule': _prepare_district_schedule,
    'ChainPrayerSchedules': _prepare_chain_prayer_schedule,
    'Anouncement': _prepare_announcements,
    "Members Account": _prepare_members,
}
