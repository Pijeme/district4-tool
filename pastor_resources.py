import io
import json
import math
import mimetypes
import os
import re
import sqlite3
import sys
import threading
import zipfile
from datetime import datetime, timezone
from xml.etree import ElementTree as ET

from flask import (
    Response,
    jsonify,
    redirect,
    render_template_string,
    request,
    send_file,
    session,
    url_for,
)

from google.auth.transport.requests import AuthorizedSession
from google.oauth2.service_account import Credentials


# =========================================================
# CONFIGURATION
# =========================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

GOOGLE_SERVICE_ACCOUNT_FILE = os.path.join(
    BASE_DIR,
    "service_account.json",
)

# Main Google Drive folder containing Pastor's Resources
PASTOR_RESOURCES_DRIVE_FOLDER_ID = (
    "1tXOmzl_IzFuSWjeNpmdTrVE4EIDp46mE"
)

GOOGLE_DRIVE_SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly"
]

GOOGLE_DRIVE_FOLDER_MIME = (
    "application/vnd.google-apps.folder"
)

THUMBNAIL_CACHE_DIR = os.path.join(
    BASE_DIR,
    "book_thumbnail_cache",
)

# Keep live sync progress in a tiny separate SQLite file. The main library
# sync intentionally uses one long transaction in app_v2.db; storing live
# progress separately avoids write-lock contention with that transaction.
RESOURCE_SYNC_STATE_DB = os.path.join(
    BASE_DIR,
    "pastor_resource_sync_state.db",
)

SYNC_LOCK = threading.Lock()

# Live synchronization status for the Pastor's Resources page.
# A memory copy is kept for speed, while every update is also persisted
# to SQLite. This makes the progress dialog reliable across page refreshes
# and across separate web workers on hosted deployments such as Render.
RESOURCE_SYNC_STATE_LOCK = threading.Lock()
RESOURCE_SYNC_STATE = {
    "running": False,
    "stage": "idle",
    "message": "",
    "total": 0,
    "processed": 0,
    "new_files": 0,
    "changed_files": 0,
    "unchanged_files": 0,
    "duplicates": 0,
    "current_file": "",
    "last_error": "",
    "started_at": "",
    "finished_at": "",
    "stats": {},
}


def _ensure_resource_sync_runtime_table(db):
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS pastor_library_sync_runtime (
            id INTEGER PRIMARY KEY CHECK(id = 1),
            running INTEGER NOT NULL DEFAULT 0,
            stage TEXT NOT NULL DEFAULT 'idle',
            message TEXT NOT NULL DEFAULT '',
            total INTEGER NOT NULL DEFAULT 0,
            processed INTEGER NOT NULL DEFAULT 0,
            new_files INTEGER NOT NULL DEFAULT 0,
            changed_files INTEGER NOT NULL DEFAULT 0,
            unchanged_files INTEGER NOT NULL DEFAULT 0,
            duplicates INTEGER NOT NULL DEFAULT 0,
            current_file TEXT NOT NULL DEFAULT '',
            last_error TEXT NOT NULL DEFAULT '',
            started_at TEXT NOT NULL DEFAULT '',
            finished_at TEXT NOT NULL DEFAULT '',
            stats_json TEXT NOT NULL DEFAULT '{}',
            updated_at TEXT NOT NULL DEFAULT ''
        )
        """
    )

    db.execute(
        """
        INSERT OR IGNORE INTO pastor_library_sync_runtime (id)
        VALUES (1)
        """
    )


def _open_resource_sync_state_db():
    db = sqlite3.connect(
        RESOURCE_SYNC_STATE_DB,
        timeout=10,
        check_same_thread=False,
    )
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout = 10000")
    return db


def _persist_resource_sync_state(state):
    db = _open_resource_sync_state_db()

    try:
        _ensure_resource_sync_runtime_table(db)

        db.execute(
            """
            UPDATE pastor_library_sync_runtime
            SET running = ?,
                stage = ?,
                message = ?,
                total = ?,
                processed = ?,
                new_files = ?,
                changed_files = ?,
                unchanged_files = ?,
                duplicates = ?,
                current_file = ?,
                last_error = ?,
                started_at = ?,
                finished_at = ?,
                stats_json = ?,
                updated_at = ?
            WHERE id = 1
            """,
            (
                1 if state.get("running") else 0,
                str(state.get("stage") or "idle"),
                str(state.get("message") or ""),
                int(state.get("total") or 0),
                int(state.get("processed") or 0),
                int(state.get("new_files") or 0),
                int(state.get("changed_files") or 0),
                int(state.get("unchanged_files") or 0),
                int(state.get("duplicates") or 0),
                str(state.get("current_file") or ""),
                str(state.get("last_error") or ""),
                str(state.get("started_at") or ""),
                str(state.get("finished_at") or ""),
                json.dumps(state.get("stats") or {}),
                utc_now_iso(),
            ),
        )

        db.commit()

    finally:
        db.close()


def _read_persisted_resource_sync_state():
    db = _open_resource_sync_state_db()

    try:
        _ensure_resource_sync_runtime_table(db)
        db.commit()

        row = db.execute(
            """
            SELECT *
            FROM pastor_library_sync_runtime
            WHERE id = 1
            """
        ).fetchone()

        if not row:
            return None

        try:
            stats = json.loads(str(row["stats_json"] or "{}"))
        except Exception:
            stats = {}

        return {
            "running": bool(row["running"]),
            "stage": str(row["stage"] or "idle"),
            "message": str(row["message"] or ""),
            "total": int(row["total"] or 0),
            "processed": int(row["processed"] or 0),
            "new_files": int(row["new_files"] or 0),
            "changed_files": int(row["changed_files"] or 0),
            "unchanged_files": int(row["unchanged_files"] or 0),
            "duplicates": int(row["duplicates"] or 0),
            "current_file": str(row["current_file"] or ""),
            "last_error": str(row["last_error"] or ""),
            "started_at": str(row["started_at"] or ""),
            "finished_at": str(row["finished_at"] or ""),
            "stats": stats if isinstance(stats, dict) else {},
        }

    finally:
        db.close()


def update_resource_sync_state(**changes):
    with RESOURCE_SYNC_STATE_LOCK:
        RESOURCE_SYNC_STATE.update(changes)
        state = dict(RESOURCE_SYNC_STATE)
        state["stats"] = dict(
            RESOURCE_SYNC_STATE.get("stats") or {}
        )

    try:
        _persist_resource_sync_state(state)
    except Exception as error:
        # Do not stop a working Drive synchronization merely because the
        # status mirror could not be written for one update. The next
        # progress update will try again.
        print(
            "[Pastor Resources Sync State WARNING] "
            + str(error),
            flush=True,
        )

    return state


def get_resource_sync_state():
    try:
        persisted = _read_persisted_resource_sync_state()
    except Exception:
        persisted = None

    with RESOURCE_SYNC_STATE_LOCK:
        if persisted:
            RESOURCE_SYNC_STATE.update(persisted)

        state = dict(RESOURCE_SYNC_STATE)
        state["stats"] = dict(
            RESOURCE_SYNC_STATE.get("stats") or {}
        )
        return state


THUMBNAIL_LOCKS = {}
THUMBNAIL_LOCKS_GUARD = threading.Lock()


# =========================================================
# APP HELPERS
# =========================================================

def _appmod():
    mod = (
        sys.modules.get("app")
        or sys.modules.get("__main__")
    )

    if mod is None:
        raise RuntimeError(
            "App module is not loaded yet."
        )

    return mod


def any_user_logged_in():
    return _appmod().any_user_logged_in()


def drive_library_configured():
    return bool(
        PASTOR_RESOURCES_DRIVE_FOLDER_ID
    )


def utc_now_iso():
    return datetime.now(
        timezone.utc
    ).isoformat()


# =========================================================
# SPECIAL PASTOR'S RESOURCES ADMIN
# =========================================================

def is_resource_admin():
    """
    Private Pastor's Resources administrator.

    Only:
        Username: Pijeme
        Position: Area Overseer

    The password is intentionally NOT checked here because
    authentication already happened during login.
    """

    if not session.get("ao_logged_in"):
        return False

    username = str(
        session.get("ao_username") or ""
    ).strip().lower()

    role = str(
        session.get("ao_role") or ""
    ).strip().lower()

    return (
        username == "pijeme"
        and role == "area overseer"
    )


# =========================================================
# DATABASE CONNECTION
# =========================================================

def get_resource_db():
    """
    Pastor's Resources uses the same app_v2.db configured
    by app.py, but opens its own short-lived connection.
    """

    database_path = _appmod().DATABASE

    db = sqlite3.connect(
        database_path,
        timeout=30,
        check_same_thread=False,
    )

    db.row_factory = sqlite3.Row

    db.execute(
        "PRAGMA foreign_keys = ON"
    )

    db.execute(
        "PRAGMA busy_timeout = 30000"
    )

    return db


# =========================================================
# DATABASE TABLES
# =========================================================

def ensure_resource_tables():
    db = get_resource_db()

    try:
        # -------------------------------------------------
        # One row = one logical book.
        #
        # PDF + EPUB versions of the same title can point
        # to the same book.
        # -------------------------------------------------

        db.execute(
            """
            CREATE TABLE IF NOT EXISTS pastor_library_books (
                id INTEGER PRIMARY KEY AUTOINCREMENT,

                book_key TEXT NOT NULL UNIQUE,

                title TEXT NOT NULL,

                author TEXT,

                category TEXT,

                folder_path TEXT,

                created_time TEXT,

                modified_time TEXT,

                is_active INTEGER NOT NULL DEFAULT 1,

                first_seen_at TEXT NOT NULL,

                last_seen_at TEXT NOT NULL
            )
            """
        )

        db.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_pastor_library_books_title
            ON pastor_library_books(title)
            """
        )

        db.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_pastor_library_books_author
            ON pastor_library_books(author)
            """
        )

        db.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_pastor_library_books_category
            ON pastor_library_books(category)
            """
        )

        db.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_pastor_library_books_active
            ON pastor_library_books(is_active)
            """
        )

        # -------------------------------------------------
        # One row = one actual file in Google Drive.
        # -------------------------------------------------

        db.execute(
            """
            CREATE TABLE IF NOT EXISTS pastor_library_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,

                drive_file_id TEXT NOT NULL UNIQUE,

                book_id INTEGER,

                name TEXT NOT NULL,

                format TEXT,

                mime_type TEXT,

                size INTEGER DEFAULT 0,

                folder_path TEXT,

                created_time TEXT,

                modified_time TEXT,

                md5_checksum TEXT,

                sha1_checksum TEXT,

                sha256_checksum TEXT,

                is_active INTEGER NOT NULL DEFAULT 1,

                is_duplicate INTEGER NOT NULL DEFAULT 0,

                duplicate_of_drive_file_id TEXT,

                first_seen_at TEXT NOT NULL,

                last_seen_at TEXT NOT NULL,

                FOREIGN KEY(book_id)
                    REFERENCES pastor_library_books(id)
            )
            """
        )

        db.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_pastor_library_files_book
            ON pastor_library_files(book_id)
            """
        )

        db.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_pastor_library_files_active
            ON pastor_library_files(is_active)
            """
        )

        db.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_pastor_library_files_sha256
            ON pastor_library_files(sha256_checksum)
            """
        )

        db.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_pastor_library_files_md5
            ON pastor_library_files(md5_checksum)
            """
        )

        # -------------------------------------------------
        # Last synchronization summary.
        # -------------------------------------------------

        db.execute(
            """
            CREATE TABLE IF NOT EXISTS pastor_library_sync (
                id INTEGER PRIMARY KEY CHECK(id = 1),

                last_sync_at TEXT,

                folders_scanned INTEGER DEFAULT 0,

                total_files_seen INTEGER DEFAULT 0,

                supported_files INTEGER DEFAULT 0,

                pdf_count INTEGER DEFAULT 0,

                epub_count INTEGER DEFAULT 0,

                unsupported_files INTEGER DEFAULT 0,

                exact_duplicates INTEGER DEFAULT 0,

                unique_books INTEGER DEFAULT 0,

                new_books INTEGER DEFAULT 0,

                removed_files INTEGER DEFAULT 0
            )
            """
        )

        db.execute(
            """
            INSERT OR IGNORE INTO pastor_library_sync (id)
            VALUES (1)
            """
        )

        db.commit()

    finally:
        db.close()


# =========================================================
# GOOGLE DRIVE CONNECTION
# =========================================================

def get_drive_session():
    if not os.path.exists(
        GOOGLE_SERVICE_ACCOUNT_FILE
    ):
        raise RuntimeError(
            "service_account.json was not found."
        )

    credentials = (
        Credentials.from_service_account_file(
            GOOGLE_SERVICE_ACCOUNT_FILE,
            scopes=GOOGLE_DRIVE_SCOPES,
        )
    )

    return AuthorizedSession(
        credentials
    )


# =========================================================
# GOOGLE DRIVE FOLDER LISTING
# =========================================================

def list_drive_folder(
    drive_session,
    folder_id,
):
    files = []
    page_token = None

    while True:
        params = {
            "q": (
                f"'{folder_id}' in parents "
                f"and trashed = false"
            ),
            "fields": (
                "nextPageToken,"
                "files("
                "id,"
                "name,"
                "mimeType,"
                "size,"
                "createdTime,"
                "modifiedTime,"
                "md5Checksum,"
                "sha1Checksum,"
                "sha256Checksum,"
                "parents"
                ")"
            ),
            "pageSize": 1000,
            "supportsAllDrives": "true",
            "includeItemsFromAllDrives": "true",
        }

        if page_token:
            params["pageToken"] = page_token

        response = drive_session.get(
            "https://www.googleapis.com/drive/v3/files",
            params=params,
            timeout=60,
        )

        response.raise_for_status()

        data = response.json()

        files.extend(
            data.get("files", [])
        )

        page_token = data.get(
            "nextPageToken"
        )

        if not page_token:
            break

    return files


# =========================================================
# BOOK TEXT HELPERS
# =========================================================

EBOOK_EXTENSIONS = (
    ".pdf",
    ".epub",
    ".mobi",
    ".azw3",
    ".azw",
)


def remove_book_extension(filename):
    """
    Remove ebook extensions repeatedly.

    This fixes names such as:
        Book Title.epub.pdf
        Book Title.pdf.epub
    """

    name = str(
        filename or ""
    ).strip()

    while True:
        lower_name = name.lower()
        matched = False

        for extension in EBOOK_EXTENSIONS:
            if lower_name.endswith(extension):
                name = name[
                    : -len(extension)
                ].rstrip(
                    " .-_"
                )
                matched = True
                break

        if not matched:
            break

    return name


def clean_text(value):
    value = str(
        value or ""
    )

    value = value.replace(
        "_",
        " "
    )

    value = re.sub(
        r"\s+",
        " ",
        value,
    )

    return value.strip()


def remove_catalog_source_noise(value):
    """
    Remove common source-site decorations that can cause
    the same ebook to be cataloged as two titles.
    """

    value = clean_text(
        value
    )

    value = re.sub(
        r"\s*[\(\[]\s*"
        r"(?:z-?lib(?:\.org)?|booksee(?:\.org)?|"
        r"oceanofpdf(?:\.com)?|libgen(?:\.is|\.rs)?)"
        r"\s*[\)\]]\s*",
        " ",
        value,
        flags=re.IGNORECASE,
    )

    value = re.sub(
        r"\s*[-–—]\s*"
        r"(?:z-?lib(?:\.org)?|booksee(?:\.org)?|"
        r"oceanofpdf(?:\.com)?|libgen(?:\.is|\.rs)?)"
        r"\s*$",
        "",
        value,
        flags=re.IGNORECASE,
    )

    # Remove long source/library IDs such as "(8771)".
    value = re.sub(
        r"\s*\(\s*\d{4,}\s*\)\s*$",
        "",
        value,
    )

    # Remove leftover ebook extension text.
    value = re.sub(
        r"(?:\.(?:pdf|epub|mobi|azw3?|txt))+\s*$",
        "",
        value,
        flags=re.IGNORECASE,
    )

    return clean_text(
        value
    )


def normalize_text(value):
    value = str(
        value or ""
    ).lower()

    value = value.replace(
        "_",
        " "
    )

    value = re.sub(
        r"[^a-z0-9]+",
        " ",
        value,
    )

    value = re.sub(
        r"\s+",
        " ",
        value,
    )

    return value.strip()


def clean_author_name(value):
    value = remove_book_extension(
        value
    )

    value = remove_catalog_source_noise(
        value
    )

    # Example:
    # A.W. Tozer [Tozer, A. W.]
    # Keep the cleaner first form.
    value = re.sub(
        r"\s*\[[^\]]+\]\s*$",
        "",
        value,
    )

    return clean_text(
        value
    )


def normalize_author_key(value):
    """
    Normalize simple author variants so:
        A. W. Tozer
        Tozer, A. W.
    result in the same author key.
    """

    author = clean_author_name(
        value
    )

    if (
        "," in author
        and author.count(",") == 1
    ):
        left, right = [
            part.strip()
            for part
            in author.split(
                ",",
                1,
            )
        ]

        if (
            left
            and right
            and len(
                left.split()
            ) <= 3
        ):
            author = (
                right
                + " "
                + left
            )

    return normalize_text(
        author
    )


def canonical_title_key(value):
    title = remove_book_extension(
        value
    )

    title = remove_catalog_source_noise(
        title
    )

    return normalize_text(
        title
    )


def create_book_key(
    title,
    author,
):
    """
    Logical duplicate key.

    Folder path is deliberately excluded. Therefore copies
    of the same title by the same author in different Drive
    folders, and PDF/EPUB versions, become one book.
    """

    title_key = canonical_title_key(
        title
    )

    author_key = normalize_author_key(
        author
    )

    if not author_key:
        author_key = "unknown author"

    return (
        title_key
        + "|"
        + author_key
    )


def get_title_and_author(
    filename,
    folder_path,
):
    """
    Conservative filename/folder parser.

    This does not attempt to perfectly catalog every
    publisher naming convention. We can improve metadata
    separately later without changing the Drive/database
    architecture.
    """

    raw_title = clean_text(
        remove_book_extension(
            filename
        )
    )

    title = raw_title
    author = ""

    # -----------------------------------------------------
    # Common pattern:
    # Book Title - Author Name
    # -----------------------------------------------------

    if " - " in raw_title:
        left, right = raw_title.rsplit(
            " - ",
            1,
        )

        left = left.strip()
        right = right.strip()

        if left and right:
            title = left

            # Example:
            # "2nd Edition by Dag Heward-Mills"
            by_match = re.search(
                r"\bby\s+(.+)$",
                right,
                re.IGNORECASE,
            )

            if by_match:
                author = (
                    by_match.group(1)
                    .strip()
                )
            else:
                author = right

    # -----------------------------------------------------
    # Common pattern:
    # Book Title by Author Name
    # -----------------------------------------------------

    if not author:
        by_match = re.search(
            r"\s+by\s+(.+)$",
            raw_title,
            re.IGNORECASE,
        )

        if by_match:
            possible_title = (
                raw_title[
                    :by_match.start()
                ].strip()
            )

            possible_author = (
                by_match.group(1)
                .strip()
            )

            if (
                possible_title
                and possible_author
            ):
                title = possible_title
                author = possible_author

    # -----------------------------------------------------
    # Folder fallback.
    #
    # In many author-organized collections, the folder
    # immediately above a book folder is the author.
    # -----------------------------------------------------

    if not author:
        parts = [
            clean_text(part)
            for part
            in str(
                folder_path or ""
            ).split("/")
            if clean_text(part)
        ]

        if len(parts) >= 2:
            author = parts[-2]

        elif len(parts) == 1:
            author = parts[0]

    title = remove_catalog_source_noise(
        title
    )

    title = remove_book_extension(
        title
    )

    title = clean_text(
        title
    )

    author = clean_author_name(
        author
    )

    if not title:
        title = "Untitled Book"

    if not author:
        author = "Unknown Author"

    return title, author


def get_category(folder_path):
    """
    Temporary automatic category derived from the existing
    Drive folder structure.

    No reorganization of Google Drive is required.
    """

    parts = [
        clean_text(part)
        for part
        in str(
            folder_path or ""
        ).split("/")
        if clean_text(part)
    ]

    if not parts:
        return "General"

    # Prefer folder labels that clearly look like categories.
    category_words = (
        "ebook",
        "books",
        "leadership",
        "worship",
        "children",
        "youth",
        "marriage",
        "family",
        "ministry",
        "theology",
        "prayer",
        "pastor",
        "preaching",
        "sermon",
        "devotional",
    )

    for part in reversed(
        parts[:-1]
        if len(parts) > 1
        else parts
    ):
        lowered = part.lower()

        if any(
            word in lowered
            for word in category_words
        ):
            return part

    if len(parts) >= 3:
        return parts[-3]

    return parts[0]


# =========================================================
# RECURSIVE DRIVE SCANNER
# =========================================================

def scan_drive_library(progress_callback=None):
    """
    Recursively discover supported PDF/EPUB files in Google Drive.

    When progress_callback is supplied, discovery progress is reported as
    each supported ebook is found. At this stage the final number of ebooks
    is not known yet, so total remains 0 and processed means "ebooks found
    so far". The caller switches to a real processed/total percentage after
    discovery is complete.
    """

    def progress(**values):
        if progress_callback:
            progress_callback(**values)

    if not drive_library_configured():
        raise RuntimeError(
            "Pastor's Resources Google Drive "
            "folder is not configured."
        )

    drive_session = (
        get_drive_session()
    )

    files = []

    visited_folders = set()

    folders_scanned = 0
    total_files_seen = 0
    unsupported_files = 0
    pdf_count = 0
    epub_count = 0

    def scan_folder(
        folder_id,
        folder_path="",
    ):
        nonlocal folders_scanned
        nonlocal total_files_seen
        nonlocal unsupported_files
        nonlocal pdf_count
        nonlocal epub_count

        if folder_id in visited_folders:
            return

        visited_folders.add(
            folder_id
        )

        folders_scanned += 1

        progress(
            stage="scanning",
            message=(
                "Scanning Google Drive folder "
                + str(folders_scanned)
                + (": " + folder_path if folder_path else "...")
            ),
            total=0,
            processed=len(files),
            current_file="",
        )

        items = list_drive_folder(
            drive_session,
            folder_id,
        )

        for item in items:
            name = str(
                item.get("name") or ""
            ).strip()

            mime_type = str(
                item.get("mimeType") or ""
            ).strip()

            file_id = str(
                item.get("id") or ""
            ).strip()

            # ---------------------------------------------
            # Folder
            # ---------------------------------------------

            if (
                mime_type
                == GOOGLE_DRIVE_FOLDER_MIME
            ):
                new_path = (
                    f"{folder_path}/{name}"
                    if folder_path
                    else name
                )

                scan_folder(
                    file_id,
                    new_path,
                )

                continue

            # ---------------------------------------------
            # Normal file
            # ---------------------------------------------

            total_files_seen += 1

            lower_name = name.lower()

            is_pdf = (
                mime_type
                == "application/pdf"
                or lower_name.endswith(
                    ".pdf"
                )
            )

            is_epub = (
                mime_type
                == "application/epub+zip"
                or lower_name.endswith(
                    ".epub"
                )
            )

            # MP3, JPG, MOBI, AZW, ZIP, etc. are not part
            # of the current Pastor's Resources catalog.
            if (
                not is_pdf
                and not is_epub
            ):
                unsupported_files += 1
                continue

            if is_pdf:
                file_format = "PDF"
                pdf_count += 1

            else:
                file_format = "EPUB"
                epub_count += 1

            title, author = (
                get_title_and_author(
                    name,
                    folder_path,
                )
            )

            files.append(
                {
                    "drive_file_id":
                        file_id,

                    "name":
                        name,

                    "title":
                        title,

                    "author":
                        author,

                    "category":
                        get_category(
                            folder_path
                        ),

                    "book_key":
                        create_book_key(
                            title,
                            author,
                        ),

                    "format":
                        file_format,

                    "mime_type":
                        mime_type,

                    "size":
                        int(
                            item.get("size")
                            or 0
                        ),

                    "folder_path":
                        folder_path,

                    "created_time":
                        item.get(
                            "createdTime"
                        )
                        or "",

                    "modified_time":
                        item.get(
                            "modifiedTime"
                        )
                        or "",

                    "md5_checksum":
                        item.get(
                            "md5Checksum"
                        )
                        or "",

                    "sha1_checksum":
                        item.get(
                            "sha1Checksum"
                        )
                        or "",

                    "sha256_checksum":
                        item.get(
                            "sha256Checksum"
                        )
                        or "",
                }
            )

            # During recursive discovery there is no truthful final
            # denominator yet. Report the real filename and the number of
            # supported ebooks discovered so far; the determinate percentage
            # begins immediately after the scan completes.
            progress(
                stage="scanning",
                message=(
                    str(len(files))
                    + " supported ebook"
                    + ("" if len(files) == 1 else "s")
                    + " found so far."
                ),
                total=0,
                processed=len(files),
                current_file=name,
            )

    scan_folder(
        PASTOR_RESOURCES_DRIVE_FOLDER_ID
    )

    return {
        "files":
            files,

        "supported_files":
            len(files),

        "folders_scanned":
            folders_scanned,

        "total_files_seen":
            total_files_seen,

        "unsupported_files":
            unsupported_files,

        "pdf_count":
            pdf_count,

        "epub_count":
            epub_count,
    }


# =========================================================
# EXACT DUPLICATE HELPER
# =========================================================

def get_file_checksum_key(item):
    sha256 = str(
        item.get("sha256_checksum")
        or ""
    ).strip()

    md5 = str(
        item.get("md5_checksum")
        or ""
    ).strip()

    if sha256:
        return (
            "sha256:"
            + sha256
        )

    if md5:
        return (
            "md5:"
            + md5
        )

    return ""


# =========================================================
# THUMBNAIL CACHE HELPERS
# =========================================================

def ensure_thumbnail_cache_dir():
    os.makedirs(
        THUMBNAIL_CACHE_DIR,
        exist_ok=True,
    )


def get_thumbnail_lock(book_id):
    key = int(book_id)

    with THUMBNAIL_LOCKS_GUARD:
        if key not in THUMBNAIL_LOCKS:
            THUMBNAIL_LOCKS[key] = (
                threading.Lock()
            )

        return THUMBNAIL_LOCKS[key]


def thumbnail_cache_candidates(book_id):
    ensure_thumbnail_cache_dir()

    base = os.path.join(
        THUMBNAIL_CACHE_DIR,
        str(int(book_id)),
    )

    return [
        (
            base + ".jpg",
            "image/jpeg",
        ),
        (
            base + ".png",
            "image/png",
        ),
        (
            base + ".webp",
            "image/webp",
        ),
        (
            base + ".gif",
            "image/gif",
        ),
    ]


def get_cached_thumbnail(book_id):
    for path, mime_type in (
        thumbnail_cache_candidates(
            book_id
        )
    ):
        if (
            os.path.exists(path)
            and os.path.getsize(path) > 0
        ):
            return path, mime_type

    return None, None


def invalidate_thumbnail_cache(book_id):
    if not book_id:
        return

    for path, _mime_type in (
        thumbnail_cache_candidates(
            book_id
        )
    ):
        try:
            if os.path.exists(path):
                os.remove(path)
        except Exception:
            pass


def guess_image_extension(
    content_type,
    data,
):
    content_type = str(
        content_type or ""
    ).lower()

    if "png" in content_type:
        return ".png", "image/png"

    if "webp" in content_type:
        return ".webp", "image/webp"

    if "gif" in content_type:
        return ".gif", "image/gif"

    if (
        "jpeg" in content_type
        or "jpg" in content_type
    ):
        return ".jpg", "image/jpeg"

    # Signature fallback
    if data.startswith(b"\x89PNG"):
        return ".png", "image/png"

    if data.startswith(
        (
            b"GIF87a",
            b"GIF89a",
        )
    ):
        return ".gif", "image/gif"

    if (
        len(data) >= 12
        and data[:4] == b"RIFF"
        and data[8:12] == b"WEBP"
    ):
        return ".webp", "image/webp"

    return ".jpg", "image/jpeg"


def save_thumbnail_bytes(
    book_id,
    data,
    content_type="",
):
    if not data:
        return None, None

    ensure_thumbnail_cache_dir()

    extension, mime_type = (
        guess_image_extension(
            content_type,
            data,
        )
    )

    target = os.path.join(
        THUMBNAIL_CACHE_DIR,
        str(int(book_id))
        + extension,
    )

    temporary = (
        target
        + ".tmp"
    )

    with open(
        temporary,
        "wb",
    ) as handle:
        handle.write(
            data
        )

    os.replace(
        temporary,
        target,
    )

    return target, mime_type


# =========================================================
# SYNC GOOGLE DRIVE → DATABASE
# =========================================================

def sync_library_to_database(progress_callback=None):
    ensure_resource_tables()

    # -----------------------------------------------------
    # This is the intentionally slow operation.
    # It is run ONLY from the private Sync Books button.
    #
    # progress_callback is optional. The normal database logic
    # remains the same, but the live Pastor's Resources sync UI
    # can now see which file is being processed.
    # -----------------------------------------------------

    def progress(**values):
        if progress_callback:
            progress_callback(**values)

    progress(
        stage="scanning",
        message="Scanning Google Drive folders for PDF and EPUB books...",
        total=0,
        processed=0,
        new_files=0,
        changed_files=0,
        unchanged_files=0,
        duplicates=0,
        current_file="",
        last_error="",
    )

    scan_result = (
        scan_drive_library(
            progress_callback=progress
        )
    )

    total_supported = int(
        scan_result.get(
            "supported_files"
        )
        or len(
            scan_result.get(
                "files",
                [],
            )
        )
    )

    progress(
        stage="syncing",
        message=(
            "Google Drive scan complete. "
            + str(total_supported)
            + " supported ebook file"
            + ("" if total_supported == 1 else "s")
            + " found."
        ),
        total=total_supported,
        processed=0,
        current_file="",
    )

    now_iso = (
        utc_now_iso()
    )

    db = get_resource_db()

    try:
        existing_book_keys = {
            str(row["book_key"])
            for row
            in db.execute(
                """
                SELECT book_key
                FROM pastor_library_books
                """
            ).fetchall()
        }

        old_active_files = {
            str(row["drive_file_id"])
            for row
            in db.execute(
                """
                SELECT drive_file_id
                FROM pastor_library_files
                WHERE is_active = 1
                """
            ).fetchall()
        }

        # Everything becomes inactive temporarily.
        # Anything found during this scan is reactivated.
        # This remains one transaction exactly as in the
        # previously working Pastor's Resources sync.
        db.execute(
            """
            UPDATE pastor_library_books
            SET is_active = 0
            """
        )

        db.execute(
            """
            UPDATE pastor_library_files
            SET is_active = 0,
                is_duplicate = 0,
                duplicate_of_drive_file_id = NULL
            """
        )

        checksum_owners = {}

        scanned_drive_ids = set()

        exact_duplicates = 0
        new_books = 0

        # Live-only counters. They do not alter the existing
        # pastor_library_sync schema.
        new_files = 0
        changed_files = 0
        unchanged_files = 0

        for position, item in enumerate(
            scan_result["files"],
            start=1,
        ):
            drive_file_id = str(
                item["drive_file_id"]
            )

            current_name = str(
                item.get("name")
                or "Unnamed ebook"
            )

            progress(
                stage="syncing",
                message=(
                    "Checking "
                    + str(position)
                    + " of "
                    + str(total_supported)
                ),
                total=total_supported,
                processed=position - 1,
                new_files=new_files,
                changed_files=changed_files,
                unchanged_files=unchanged_files,
                duplicates=exact_duplicates,
                current_file=current_name,
            )

            scanned_drive_ids.add(
                drive_file_id
            )

            old_file = db.execute(
                """
                SELECT
                    book_id,
                    modified_time
                FROM pastor_library_files
                WHERE drive_file_id = ?
                """,
                (
                    drive_file_id,
                ),
            ).fetchone()

            if old_file is None:
                new_files += 1
            elif str(
                old_file["modified_time"]
                or ""
            ) != str(
                item["modified_time"]
                or ""
            ):
                changed_files += 1
            else:
                unchanged_files += 1

            checksum_key = (
                get_file_checksum_key(
                    item
                )
            )

            duplicate = False
            duplicate_of = None
            book_id = None

            # ---------------------------------------------
            # Exact duplicate file.
            # ---------------------------------------------

            if (
                checksum_key
                and checksum_key
                in checksum_owners
            ):
                duplicate = True

                exact_duplicates += 1

                owner = (
                    checksum_owners[
                        checksum_key
                    ]
                )

                duplicate_of = (
                    owner[
                        "drive_file_id"
                    ]
                )

                book_id = (
                    owner[
                        "book_id"
                    ]
                )

            # ---------------------------------------------
            # Normal PDF / EPUB.
            # ---------------------------------------------

            else:
                book_key = str(
                    item["book_key"]
                )

                if (
                    book_key
                    not in existing_book_keys
                ):
                    new_books += 1

                    existing_book_keys.add(
                        book_key
                    )

                db.execute(
                    """
                    INSERT INTO pastor_library_books (
                        book_key,
                        title,
                        author,
                        category,
                        folder_path,
                        created_time,
                        modified_time,
                        is_active,
                        first_seen_at,
                        last_seen_at
                    )
                    VALUES (
                        ?, ?, ?, ?, ?, ?, ?,
                        1, ?, ?
                    )

                    ON CONFLICT(book_key)
                    DO UPDATE SET
                        title = excluded.title,
                        author = excluded.author,
                        category = excluded.category,
                        folder_path = excluded.folder_path,
                        created_time = excluded.created_time,
                        modified_time = excluded.modified_time,
                        is_active = 1,
                        last_seen_at = excluded.last_seen_at
                    """,
                    (
                        book_key,
                        item["title"],
                        item["author"],
                        item["category"],
                        item["folder_path"],
                        item["created_time"],
                        item["modified_time"],
                        now_iso,
                        now_iso,
                    ),
                )

                book_row = db.execute(
                    """
                    SELECT id
                    FROM pastor_library_books
                    WHERE book_key = ?
                    """,
                    (
                        book_key,
                    ),
                ).fetchone()

                book_id = int(
                    book_row["id"]
                )

                if checksum_key:
                    checksum_owners[
                        checksum_key
                    ] = {
                        "drive_file_id":
                            drive_file_id,

                        "book_id":
                            book_id,
                    }

            # ---------------------------------------------
            # Insert/update actual Drive file.
            # ---------------------------------------------

            db.execute(
                """
                INSERT INTO pastor_library_files (
                    drive_file_id,
                    book_id,
                    name,
                    format,
                    mime_type,
                    size,
                    folder_path,
                    created_time,
                    modified_time,
                    md5_checksum,
                    sha1_checksum,
                    sha256_checksum,
                    is_active,
                    is_duplicate,
                    duplicate_of_drive_file_id,
                    first_seen_at,
                    last_seen_at
                )
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    1, ?, ?, ?, ?
                )

                ON CONFLICT(drive_file_id)
                DO UPDATE SET
                    book_id = excluded.book_id,
                    name = excluded.name,
                    format = excluded.format,
                    mime_type = excluded.mime_type,
                    size = excluded.size,
                    folder_path = excluded.folder_path,
                    created_time = excluded.created_time,
                    modified_time = excluded.modified_time,
                    md5_checksum = excluded.md5_checksum,
                    sha1_checksum = excluded.sha1_checksum,
                    sha256_checksum = excluded.sha256_checksum,
                    is_active = 1,
                    is_duplicate = excluded.is_duplicate,
                    duplicate_of_drive_file_id =
                        excluded.duplicate_of_drive_file_id,
                    last_seen_at = excluded.last_seen_at
                """,
                (
                    drive_file_id,
                    book_id,
                    item["name"],
                    item["format"],
                    item["mime_type"],
                    item["size"],
                    item["folder_path"],
                    item["created_time"],
                    item["modified_time"],
                    item["md5_checksum"],
                    item["sha1_checksum"],
                    item["sha256_checksum"],
                    1 if duplicate else 0,
                    duplicate_of,
                    now_iso,
                    now_iso,
                ),
            )

            # If this file is new or changed, remove only
            # this book's cached thumbnail so it can be
            # recreated on the next visible page.
            if (
                book_id
                and (
                    old_file is None
                    or str(
                        old_file[
                            "modified_time"
                        ]
                        or ""
                    )
                    != str(
                        item[
                            "modified_time"
                        ]
                        or ""
                    )
                )
            ):
                invalidate_thumbnail_cache(
                    book_id
                )

            progress(
                stage="syncing",
                message=(
                    "Checked "
                    + str(position)
                    + " of "
                    + str(total_supported)
                ),
                total=total_supported,
                processed=position,
                new_files=new_files,
                changed_files=changed_files,
                unchanged_files=unchanged_files,
                duplicates=exact_duplicates,
                current_file=current_name,
            )

        progress(
            stage="finalizing",
            message="Finalizing the Pastor's Resources library...",
            total=total_supported,
            processed=total_supported,
            new_files=new_files,
            changed_files=changed_files,
            unchanged_files=unchanged_files,
            duplicates=exact_duplicates,
            current_file="",
        )

        # ---------------------------------------------
        # Recalculate logical-book active status.
        # ---------------------------------------------

        db.execute(
            """
            UPDATE pastor_library_books
            SET is_active = 0
            """
        )

        db.execute(
            """
            UPDATE pastor_library_books

            SET is_active = 1

            WHERE id IN (
                SELECT DISTINCT book_id

                FROM pastor_library_files

                WHERE is_active = 1
                  AND is_duplicate = 0
                  AND book_id IS NOT NULL
            )
            """
        )

        removed_files = len(
            old_active_files
            - scanned_drive_ids
        )

        unique_row = db.execute(
            """
            SELECT COUNT(*) AS cnt

            FROM pastor_library_books

            WHERE is_active = 1
            """
        ).fetchone()

        unique_books = int(
            unique_row["cnt"]
            or 0
        )

        db.execute(
            """
            UPDATE pastor_library_sync

            SET
                last_sync_at = ?,
                folders_scanned = ?,
                total_files_seen = ?,
                supported_files = ?,
                pdf_count = ?,
                epub_count = ?,
                unsupported_files = ?,
                exact_duplicates = ?,
                unique_books = ?,
                new_books = ?,
                removed_files = ?

            WHERE id = 1
            """,
            (
                now_iso,
                scan_result[
                    "folders_scanned"
                ],
                scan_result[
                    "total_files_seen"
                ],
                scan_result[
                    "supported_files"
                ],
                scan_result[
                    "pdf_count"
                ],
                scan_result[
                    "epub_count"
                ],
                scan_result[
                    "unsupported_files"
                ],
                exact_duplicates,
                unique_books,
                new_books,
                removed_files,
            ),
        )

        db.commit()

        result = {
            "last_sync_at":
                now_iso,

            "folders_scanned":
                scan_result[
                    "folders_scanned"
                ],

            "total_files_seen":
                scan_result[
                    "total_files_seen"
                ],

            "supported_files":
                scan_result[
                    "supported_files"
                ],

            "pdf_count":
                scan_result[
                    "pdf_count"
                ],

            "epub_count":
                scan_result[
                    "epub_count"
                ],

            "unsupported_files":
                scan_result[
                    "unsupported_files"
                ],

            "exact_duplicates":
                exact_duplicates,

            "unique_books":
                unique_books,

            "new_books":
                new_books,

            "removed_files":
                removed_files,

            # Live UI details; safe extras for existing callers.
            "new_files":
                new_files,

            "changed_files":
                changed_files,

            "unchanged_files":
                unchanged_files,
        }

        progress(
            stage="complete",
            message=(
                "Library synchronization completed. "
                + str(unique_books)
                + " unique books cataloged."
            ),
            total=total_supported,
            processed=total_supported,
            new_files=new_files,
            changed_files=changed_files,
            unchanged_files=unchanged_files,
            duplicates=exact_duplicates,
            current_file="",
            stats=result,
        )

        return result

    except Exception as error:
        db.rollback()

        progress(
            stage="error",
            message="Library synchronization stopped because of an error.",
            last_error=str(error),
            current_file="",
        )

        raise

    finally:
        db.close()


# =========================================================
# DATABASE STATUS
# =========================================================

def get_library_database_status():
    ensure_resource_tables()

    db = get_resource_db()

    try:
        row = db.execute(
            """
            SELECT *
            FROM pastor_library_sync
            WHERE id = 1
            """
        ).fetchone()

        if not row:
            return {
                "last_sync_at":
                    None,

                "folders_scanned":
                    0,

                "total_files_seen":
                    0,

                "supported_files":
                    0,

                "pdf_count":
                    0,

                "epub_count":
                    0,

                "unsupported_files":
                    0,

                "exact_duplicates":
                    0,

                "unique_books":
                    0,

                "new_books":
                    0,

                "removed_files":
                    0,
            }

        return dict(
            row
        )

    finally:
        db.close()


# =========================================================
# DATABASE FILTER OPTIONS
# =========================================================

def get_library_filter_options(
    mode
):
    """
    Values for the dropdown beside the search box.

    Author   -> active authors
    Category -> active categories
    Recent   -> useful date windows
    """

    ensure_resource_tables()

    mode = str(
        mode or ""
    ).strip().lower()

    if mode == "recent":
        return [
            {
                "value": "all",
                "label": "Newest first",
            },
            {
                "value": "7",
                "label": "Added in last 7 days",
            },
            {
                "value": "30",
                "label": "Added in last 30 days",
            },
            {
                "value": "90",
                "label": "Added in last 90 days",
            },
        ]

    if mode not in (
        "author",
        "category",
    ):
        return []

    column = (
        "author"
        if mode == "author"
        else "category"
    )

    db = get_resource_db()

    try:
        rows = db.execute(
            f"""
            SELECT
                TRIM(
                    COALESCE(
                        {column},
                        ''
                    )
                ) AS value,

                COUNT(*) AS cnt

            FROM pastor_library_books

            WHERE is_active = 1
              AND TRIM(
                    COALESCE(
                        {column},
                        ''
                    )
                  ) != ''

            GROUP BY
                LOWER(
                    TRIM(
                        {column}
                    )
                )

            ORDER BY
                LOWER(
                    TRIM(
                        {column}
                    )
                )
            """
        ).fetchall()

        options = []

        for row in rows:
            value = str(
                row["value"]
                or ""
            ).strip()

            if not value:
                continue

            count = int(
                row["cnt"]
                or 0
            )

            options.append(
                {
                    "value":
                        value,

                    "label":
                        (
                            value
                            + " ("
                            + str(count)
                            + ")"
                        ),
                }
            )

        return options

    finally:
        db.close()


# =========================================================
# DATABASE BOOK SEARCH
# =========================================================

def search_library_database(
    query="",
    mode="all",
    filter_value="",
    page=1,
    per_page=24,
):
    ensure_resource_tables()

    query = str(
        query or ""
    ).strip()

    mode = str(
        mode or "all"
    ).strip().lower()

    filter_value = str(
        filter_value or ""
    ).strip()

    try:
        page = max(
            1,
            int(page),
        )
    except Exception:
        page = 1

    try:
        per_page = min(
            max(
                12,
                int(per_page),
            ),
            60,
        )
    except Exception:
        per_page = 24

    db = get_resource_db()

    try:
        where_parts = [
            "b.is_active = 1"
        ]

        params = []

        # ---------------------------------------------
        # Dropdown filter
        # ---------------------------------------------

        if (
            mode == "author"
            and filter_value
        ):
            where_parts.append(
                "LOWER(TRIM(b.author)) "
                "= LOWER(TRIM(?))"
            )

            params.append(
                filter_value
            )

        elif (
            mode == "category"
            and filter_value
        ):
            where_parts.append(
                "LOWER(TRIM(b.category)) "
                "= LOWER(TRIM(?))"
            )

            params.append(
                filter_value
            )

        elif (
            mode == "recent"
            and filter_value
            in ("7", "30", "90")
        ):
            where_parts.append(
                "datetime(b.first_seen_at) "
                ">= datetime('now', ?)"
            )

            params.append(
                "-"
                + filter_value
                + " days"
            )

        # ---------------------------------------------
        # Search inside the chosen dropdown filter.
        # ---------------------------------------------

        if query:
            like_value = (
                "%"
                + query
                + "%"
            )

            where_parts.append(
                """
                (
                    b.title LIKE ?
                    OR b.author LIKE ?
                    OR b.category LIKE ?
                    OR b.folder_path LIKE ?
                )
                """
            )

            params.extend(
                [
                    like_value,
                    like_value,
                    like_value,
                    like_value,
                ]
            )

        where_sql = (
            "WHERE "
            + " AND ".join(
                where_parts
            )
        )

        if mode == "author":
            order_sql = (
                "ORDER BY "
                "LOWER(b.author), "
                "LOWER(b.title)"
            )

        elif mode == "category":
            order_sql = (
                "ORDER BY "
                "LOWER(b.category), "
                "LOWER(b.title)"
            )

        elif mode == "recent":
            order_sql = (
                "ORDER BY "
                "datetime(b.first_seen_at) DESC, "
                "LOWER(b.title)"
            )

        else:
            order_sql = (
                "ORDER BY "
                "LOWER(b.title)"
            )

        count_row = db.execute(
            f"""
            SELECT COUNT(*) AS cnt

            FROM pastor_library_books b

            {where_sql}
            """,
            tuple(params),
        ).fetchone()

        total = int(
            count_row["cnt"]
            or 0
        )

        pages = max(
            1,
            math.ceil(
                total
                / per_page
            ),
        )

        if page > pages:
            page = pages

        offset = (
            (page - 1)
            * per_page
        )

        rows = db.execute(
            f"""
            SELECT
                b.id,
                b.title,
                b.author,
                b.category,
                b.folder_path,
                b.created_time,
                b.modified_time,
                b.first_seen_at,

                GROUP_CONCAT(
                    DISTINCT f.format
                ) AS formats

            FROM pastor_library_books b

            LEFT JOIN pastor_library_files f
                ON f.book_id = b.id
                AND f.is_active = 1
                AND f.is_duplicate = 0

            {where_sql}

            GROUP BY b.id

            {order_sql}

            LIMIT ? OFFSET ?
            """,
            tuple(
                params
                + [
                    per_page,
                    offset,
                ]
            ),
        ).fetchall()

        books = []

        # Extra display safety for old rows until the user
        # runs Sync Books with the new title+author key.
        seen_display_keys = set()

        for row in rows:
            title = str(
                row["title"]
                or ""
            )

            author = str(
                row["author"]
                or ""
            )

            display_key = (
                canonical_title_key(
                    title
                )
                + "|"
                + normalize_author_key(
                    author
                )
            )

            if (
                display_key
                in seen_display_keys
            ):
                continue

            seen_display_keys.add(
                display_key
            )

            formats = [
                value.strip()
                for value
                in str(
                    row["formats"]
                    or ""
                ).split(",")
                if value.strip()
            ]

            books.append(
                {
                    "id":
                        int(row["id"]),

                    "title":
                        title,

                    "author":
                        author,

                    "category":
                        str(
                            row["category"]
                            or ""
                        ),

                    "folder_path":
                        str(
                            row["folder_path"]
                            or ""
                        ),

                    "created_time":
                        str(
                            row["created_time"]
                            or ""
                        ),

                    "modified_time":
                        str(
                            row["modified_time"]
                            or ""
                        ),

                    "formats":
                        formats,
                }
            )

        return {
            "books":
                books,

            "page":
                page,

            "pages":
                pages,

            "per_page":
                per_page,

            "total":
                total,
        }

    finally:
        db.close()


# =========================================================
# THUMBNAIL DATABASE LOOKUP
# =========================================================

def get_thumbnail_source_for_book(
    book_id
):
    ensure_resource_tables()

    db = get_resource_db()

    try:
        book = db.execute(
            """
            SELECT
                id,
                title,
                author

            FROM pastor_library_books

            WHERE id = ?
              AND is_active = 1
            """,
            (
                int(book_id),
            ),
        ).fetchone()

        if not book:
            return None

        # Prefer EPUB for covers because its embedded
        # cover is often cleaner than a rendered PDF page.
        # If no EPUB exists, use PDF.
        file_row = db.execute(
            """
            SELECT
                drive_file_id,
                format,
                mime_type,
                name

            FROM pastor_library_files

            WHERE book_id = ?
              AND is_active = 1
              AND is_duplicate = 0

            ORDER BY
                CASE
                    WHEN format = 'EPUB'
                        THEN 0
                    WHEN format = 'PDF'
                        THEN 1
                    ELSE 2
                END,
                id ASC

            LIMIT 1
            """,
            (
                int(book_id),
            ),
        ).fetchone()

        if not file_row:
            return None

        return {
            "book_id":
                int(book["id"]),

            "title":
                str(
                    book["title"]
                    or ""
                ),

            "author":
                str(
                    book["author"]
                    or ""
                ),

            "drive_file_id":
                str(
                    file_row[
                        "drive_file_id"
                    ]
                    or ""
                ),

            "format":
                str(
                    file_row[
                        "format"
                    ]
                    or ""
                ),

            "mime_type":
                str(
                    file_row[
                        "mime_type"
                    ]
                    or ""
                ),

            "name":
                str(
                    file_row[
                        "name"
                    ]
                    or ""
                ),
        }

    finally:
        db.close()


# =========================================================
# GOOGLE DRIVE THUMBNAIL
# =========================================================

def get_drive_generated_thumbnail(
    drive_session,
    drive_file_id,
):
    metadata_url = (
        "https://www.googleapis.com/"
        "drive/v3/files/"
        + str(drive_file_id)
    )

    metadata_response = (
        drive_session.get(
            metadata_url,
            params={
                "fields":
                    "thumbnailLink",
                "supportsAllDrives":
                    "true",
            },
            timeout=30,
        )
    )

    metadata_response.raise_for_status()

    metadata = (
        metadata_response.json()
    )

    thumbnail_link = str(
        metadata.get(
            "thumbnailLink"
        )
        or ""
    ).strip()

    if not thumbnail_link:
        return None, None

    # Ask Google for a larger thumbnail when the returned
    # URL ends in a normal "=s###" size marker.
    thumbnail_link = re.sub(
        r"=s\d+$",
        "=s500",
        thumbnail_link,
    )

    response = drive_session.get(
        thumbnail_link,
        timeout=45,
    )

    response.raise_for_status()

    content_type = str(
        response.headers.get(
            "Content-Type"
        )
        or ""
    )

    if not content_type.lower().startswith(
        "image/"
    ):
        return None, None

    return (
        response.content,
        content_type,
    )


# =========================================================
# EPUB COVER EXTRACTION
# =========================================================

def download_drive_file_bytes(
    drive_session,
    drive_file_id,
):
    url = (
        "https://www.googleapis.com/"
        "drive/v3/files/"
        + str(drive_file_id)
    )

    response = drive_session.get(
        url,
        params={
            "alt": "media",
            "supportsAllDrives": "true",
        },
        timeout=120,
    )

    response.raise_for_status()

    return response.content


def _xml_local_name(tag):
    return str(
        tag
    ).split(
        "}"
    )[-1]


def extract_epub_cover(
    epub_bytes,
):
    if not epub_bytes:
        return None, None

    with zipfile.ZipFile(
        io.BytesIO(
            epub_bytes
        )
    ) as archive:
        names = set(
            archive.namelist()
        )

        # ---------------------------------------------
        # Locate the OPF package file.
        # ---------------------------------------------

        container_path = (
            "META-INF/container.xml"
        )

        opf_path = ""

        if container_path in names:
            container_xml = (
                archive.read(
                    container_path
                )
            )

            root = ET.fromstring(
                container_xml
            )

            for element in root.iter():
                if (
                    _xml_local_name(
                        element.tag
                    )
                    == "rootfile"
                ):
                    opf_path = str(
                        element.attrib.get(
                            "full-path"
                        )
                        or ""
                    ).strip()

                    if opf_path:
                        break

        if (
            not opf_path
            or opf_path not in names
        ):
            # Conservative fallback.
            opf_candidates = [
                name
                for name
                in names
                if name.lower().endswith(
                    ".opf"
                )
            ]

            if opf_candidates:
                opf_path = (
                    opf_candidates[0]
                )

        if (
            not opf_path
            or opf_path not in names
        ):
            return None, None

        opf_xml = archive.read(
            opf_path
        )

        opf_root = ET.fromstring(
            opf_xml
        )

        manifest = {}

        cover_id = ""

        # ---------------------------------------------
        # Read manifest + legacy cover metadata.
        # ---------------------------------------------

        for element in opf_root.iter():
            local_name = (
                _xml_local_name(
                    element.tag
                )
            )

            if local_name == "item":
                item_id = str(
                    element.attrib.get(
                        "id"
                    )
                    or ""
                ).strip()

                href = str(
                    element.attrib.get(
                        "href"
                    )
                    or ""
                ).strip()

                media_type = str(
                    element.attrib.get(
                        "media-type"
                    )
                    or ""
                ).strip()

                properties = str(
                    element.attrib.get(
                        "properties"
                    )
                    or ""
                ).strip()

                if item_id:
                    manifest[
                        item_id
                    ] = {
                        "href":
                            href,

                        "media_type":
                            media_type,

                        "properties":
                            properties,
                    }

            elif local_name == "meta":
                name_attr = str(
                    element.attrib.get(
                        "name"
                    )
                    or ""
                ).strip().lower()

                if (
                    name_attr
                    == "cover"
                ):
                    cover_id = str(
                        element.attrib.get(
                            "content"
                        )
                        or ""
                    ).strip()

        selected = None

        # EPUB 2 legacy cover declaration.
        if (
            cover_id
            and cover_id
            in manifest
        ):
            selected = (
                manifest[
                    cover_id
                ]
            )

        # EPUB 3 cover-image property.
        if selected is None:
            for item in manifest.values():
                properties = (
                    item[
                        "properties"
                    ]
                    .lower()
                    .split()
                )

                if (
                    "cover-image"
                    in properties
                ):
                    selected = item
                    break

        # Filename fallback.
        if selected is None:
            for item in manifest.values():
                href_lower = (
                    item[
                        "href"
                    ].lower()
                )

                media_lower = (
                    item[
                        "media_type"
                    ].lower()
                )

                if (
                    "cover"
                    in href_lower
                    and media_lower.startswith(
                        "image/"
                    )
                ):
                    selected = item
                    break

        if selected is None:
            return None, None

        opf_directory = (
            os.path.dirname(
                opf_path
            )
        )

        cover_path = os.path.normpath(
            os.path.join(
                opf_directory,
                selected[
                    "href"
                ],
            )
        ).replace(
            "\\",
            "/",
        )

        if cover_path not in names:
            return None, None

        cover_bytes = archive.read(
            cover_path
        )

        media_type = str(
            selected[
                "media_type"
            ]
            or ""
        ).strip()

        if not media_type:
            media_type = (
                mimetypes.guess_type(
                    cover_path
                )[0]
                or ""
            )

        return (
            cover_bytes,
            media_type,
        )


# =========================================================
# BUILD / CACHE THUMBNAIL
# =========================================================

def build_thumbnail_for_book(
    book_id
):
    cached_path, cached_mime = (
        get_cached_thumbnail(
            book_id
        )
    )

    if cached_path:
        return (
            cached_path,
            cached_mime,
        )

    lock = get_thumbnail_lock(
        book_id
    )

    with lock:
        # Another request may have created it while this
        # request was waiting for the lock.
        cached_path, cached_mime = (
            get_cached_thumbnail(
                book_id
            )
        )

        if cached_path:
            return (
                cached_path,
                cached_mime,
            )

        source = (
            get_thumbnail_source_for_book(
                book_id
            )
        )

        if not source:
            return None, None

        drive_session = (
            get_drive_session()
        )

        # -------------------------------------------------
        # EPUB first choice:
        # extract the book's actual embedded cover image.
        # This avoids caching a generic Drive EPUB icon.
        # -------------------------------------------------

        if (
            source["format"]
            == "EPUB"
        ):
            try:
                epub_bytes = (
                    download_drive_file_bytes(
                        drive_session,
                        source[
                            "drive_file_id"
                        ],
                    )
                )

                data, content_type = (
                    extract_epub_cover(
                        epub_bytes
                    )
                )

                if data:
                    return save_thumbnail_bytes(
                        book_id,
                        data,
                        content_type,
                    )

            except Exception:
                pass

        # -------------------------------------------------
        # PDF first-page thumbnail / EPUB fallback:
        # ask Google Drive for its generated thumbnail.
        # -------------------------------------------------

        try:
            data, content_type = (
                get_drive_generated_thumbnail(
                    drive_session,
                    source[
                        "drive_file_id"
                    ],
                )
            )

            if data:
                return save_thumbnail_bytes(
                    book_id,
                    data,
                    content_type,
                )

        except Exception:
            pass

        return None, None


# =========================================================
# DEFAULT COVER SVG
# =========================================================

def make_default_cover_svg(
    title,
):
    title = clean_text(
        title
    )

    if len(title) > 28:
        title = (
            title[:25]
            + "..."
        )

    # Minimal XML escaping.
    title = (
        title.replace(
            "&",
            "&amp;",
        )
        .replace(
            "<",
            "&lt;",
        )
        .replace(
            ">",
            "&gt;",
        )
        .replace(
            '"',
            "&quot;",
        )
    )

    svg = f"""
    <svg
        xmlns="http://www.w3.org/2000/svg"
        width="360"
        height="540"
        viewBox="0 0 360 540"
    >
      <defs>
        <linearGradient
            id="g"
            x1="0"
            y1="0"
            x2="1"
            y2="1"
        >
          <stop offset="0%" stop-color="#dba8cb"/>
          <stop offset="55%" stop-color="#a9b4e8"/>
          <stop offset="100%" stop-color="#8fd2e2"/>
        </linearGradient>
      </defs>

      <rect
          width="360"
          height="540"
          rx="28"
          fill="url(#g)"
      />

      <circle
          cx="180"
          cy="170"
          r="54"
          fill="rgba(255,255,255,.27)"
      />

      <text
          x="180"
          y="190"
          text-anchor="middle"
          font-size="62"
      >📖</text>

      <text
          x="180"
          y="330"
          text-anchor="middle"
          font-family="Arial, sans-serif"
          font-size="20"
          font-weight="700"
          fill="#ffffff"
      >{title}</text>

      <text
          x="180"
          y="470"
          text-anchor="middle"
          font-family="Arial, sans-serif"
          font-size="15"
          fill="rgba(255,255,255,.88)"
      >Pastor's Resources</text>
    </svg>
    """

    return svg


# =========================================================
# PAGE HTML
#
# IMPORTANT:
# This extends the app's existing base.html.
# Therefore the header, logo, burger button, drawer,
# backdrop, role-aware menu, and logout behavior are the
# SAME ones used by the other District 4 pages.
# =========================================================

PASTOR_RESOURCES_HTML = r"""
{% extends "base.html" %}

{% block title %}
Pastor's Resources - District 4 Tool
{% endblock %}


{% block content %}

<style>

/* ======================================================
   PASTOR'S RESOURCES
   MOBILE-FIRST DESIGN
   ====================================================== */

.app-main {
    max-width: 1500px;
    padding: 0;
}

.pr-page {
    width: 100%;
    padding: 14px 12px 44px;
    color: #14213b;
}


/* ======================================================
   HERO
   ====================================================== */

.pr-hero {
    display: flex;
    flex-direction: column;
    gap: 16px;

    padding: 20px 18px;
    margin-bottom: 14px;

    border-radius: 20px;

    background:
        linear-gradient(
            135deg,
            #fff8fb 0%,
            #f7f6ff 48%,
            #eef9ff 100%
        );

    border:
        1px solid
        rgba(15, 23, 42, .07);

    box-shadow:
        0 10px 30px
        rgba(15, 23, 42, .06);
}

.pr-kicker {
    display: inline-flex;
    align-items: center;
    gap: 7px;

    margin-bottom: 5px;

    color: #8b5f92;
    font-size: 12px;
    font-weight: 800;

    letter-spacing: .08em;
    text-transform: uppercase;
}

.pr-title {
    margin: 0;

    font-family:
        Georgia,
        "Times New Roman",
        serif;

    font-size: 34px;
    line-height: 1.02;

    color: #14213b;
}

.pr-subtitle {
    max-width: 730px;

    margin: 10px 0 0;

    color: #64748b;
    font-size: 14px;
    line-height: 1.65;
}

.pr-admin-actions {
    display: grid;
    grid-template-columns: 1fr;
    gap: 9px;
}

.pr-admin-btn {
    width: 100%;

    border: 0;
    border-radius: 13px;

    padding: 12px 15px;

    font: inherit;
    font-size: 14px;
    font-weight: 800;

    cursor: pointer;

    text-decoration: none;

    display: inline-flex;
    align-items: center;
    justify-content: center;
    gap: 7px;

    background: #ffffff;
    color: #475569;

    box-shadow:
        0 6px 18px
        rgba(15, 23, 42, .08);
}

.pr-admin-btn.primary {
    color: white;

    background:
        linear-gradient(
            135deg,
            #c889c2,
            #779be4
        );
}

.pr-admin-btn:disabled {
    opacity: .62;
    cursor: not-allowed;
}


/* ======================================================
   SEARCH
   ====================================================== */

.pr-search-card {
    margin-bottom: 14px;
    padding: 14px;

    border-radius: 18px;

    background: #ffffff;

    border:
        1px solid
        rgba(15, 23, 42, .07);

    box-shadow:
        0 8px 26px
        rgba(15, 23, 42, .05);
}

.pr-search-row {
    display: grid;
    grid-template-columns: minmax(0, 1fr);
    gap: 9px;
}

.pr-search-row.has-dropdown {
    grid-template-columns:
        minmax(0, 1fr)
        minmax(135px, 42%);
}

.pr-search-input {
    width: 100%;
    min-width: 0;
    min-height: 48px;

    padding: 12px 14px;

    border:
        1px solid
        #dbe3ee;

    border-radius: 13px;

    outline: none;

    font: inherit;
    font-size: 15px;

    color: #24324a;

    background: #ffffff;
}

.pr-search-input:focus {
    border-color: #8ea8dc;

    box-shadow:
        0 0 0 3px
        rgba(109, 142, 210, .13);
}

.pr-filter-select {
    width: 100%;
    min-width: 0;
    min-height: 48px;

    display: none;

    padding: 10px 34px 10px 12px;

    border:
        1px solid
        #dbe3ee;

    border-radius: 13px;

    outline: none;

    font: inherit;
    font-size: 13px;
    font-weight: 700;

    color: #475569;

    background: #ffffff;

    cursor: pointer;
}

.pr-filter-select.visible {
    display: block;
}

.pr-filter-select:focus {
    border-color: #8ea8dc;

    box-shadow:
        0 0 0 3px
        rgba(109, 142, 210, .13);
}

.pr-filters {
    display: flex;
    gap: 8px;

    margin-top: 11px;

    overflow-x: auto;
    padding-bottom: 2px;

    scrollbar-width: thin;
}

.pr-filter {
    flex: 0 0 auto;

    border: 0;
    border-radius: 999px;

    padding: 9px 14px;

    background: #eef2f8;
    color: #59677f;

    font: inherit;
    font-size: 13px;
    font-weight: 800;

    cursor: pointer;
}

.pr-filter.active {
    color: white;

    background:
        linear-gradient(
            135deg,
            #b47db9,
            #6f96de
        );
}


/* ======================================================
   LIBRARY STATUS
   ====================================================== */

.pr-status {
    display: flex;
    flex-direction: column;
    gap: 7px;

    margin-bottom: 15px;
    padding: 12px 14px;

    border-radius: 16px;

    background:
        rgba(255,255,255,.88);

    border:
        1px solid
        rgba(15,23,42,.06);

    color: #64748b;
    font-size: 13px;
    line-height: 1.5;
}

.pr-status-main {
    display: flex;
    align-items: center;
    gap: 9px;
}

.pr-status-dot {
    width: 9px;
    height: 9px;

    flex: 0 0 9px;

    border-radius: 50%;
    background: #22c55e;
}

.pr-status-dot.waiting {
    background: #f59e0b;
}

.pr-status-dot.error {
    background: #ef4444;
}

.pr-sync-detail {
    color: #94a3b8;
    font-size: 12px;
}


/* ======================================================
   SECTION HEADING
   ====================================================== */

.pr-books-heading {
    display: flex;
    align-items: end;
    justify-content: space-between;
    gap: 12px;

    margin: 18px 2px 12px;
}

.pr-books-heading h2 {
    margin: 0;

    font-family:
        Georgia,
        "Times New Roman",
        serif;

    font-size: 27px;
    color: #14213b;
}

.pr-book-count {
    margin-top: 4px;

    color: #94a3b8;
    font-size: 12px;
}


/* ======================================================
   BOOK GRID
   ====================================================== */

.pr-grid {
    display: grid;
    grid-template-columns: 1fr;
    gap: 11px;
}

.pr-book-card {
    min-width: 0;

    display: grid;
    grid-template-columns: 92px minmax(0, 1fr);
    gap: 13px;

    padding: 12px;

    border-radius: 18px;

    background: #ffffff;

    border:
        1px solid
        rgba(15,23,42,.07);

    box-shadow:
        0 8px 24px
        rgba(15,23,42,.06);
}

.pr-cover-wrap {
    position: relative;

    width: 92px;
    aspect-ratio: 2 / 3;

    overflow: hidden;

    border-radius: 12px;

    background:
        linear-gradient(
            135deg,
            #d8a7ca,
            #a9b3e6,
            #8ccedf
        );

    box-shadow:
        0 4px 12px
        rgba(15,23,42,.10);
}

.pr-cover {
    width: 100%;
    height: 100%;

    display: block;

    object-fit: cover;

    background:
        linear-gradient(
            135deg,
            #d8a7ca,
            #a9b3e6,
            #8ccedf
        );
}

.pr-book-content {
    min-width: 0;

    display: flex;
    flex-direction: column;
}

.pr-category {
    align-self: flex-start;

    max-width: 100%;

    margin-bottom: 6px;
    padding: 4px 8px;

    border-radius: 999px;

    background: #f5eff8;
    color: #875d8e;

    font-size: 10px;
    font-weight: 800;

    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}

.pr-book-title {
    margin: 0;

    font-family:
        Georgia,
        "Times New Roman",
        serif;

    font-size: 16px;
    line-height: 1.28;

    color: #17233c;

    display: -webkit-box;
    -webkit-line-clamp: 3;
    -webkit-box-orient: vertical;

    overflow: hidden;
}

.pr-author {
    margin-top: 7px;

    color: #7c899f;
    font-size: 12px;
    line-height: 1.4;

    display: -webkit-box;
    -webkit-line-clamp: 2;
    -webkit-box-orient: vertical;

    overflow: hidden;
}

.pr-format-row {
    display: flex;
    flex-wrap: wrap;
    gap: 5px;

    margin-top: 8px;
}

.pr-format-chip {
    padding: 3px 7px;

    border-radius: 999px;

    background: #eef3fa;
    color: #70809a;

    font-size: 10px;
    font-weight: 800;
}

.pr-actions {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 7px;

    margin-top: auto;
    padding-top: 12px;
}

.pr-book-btn {
    min-height: 38px;

    border: 0;
    border-radius: 10px;

    font: inherit;
    font-size: 12px;
    font-weight: 800;

    cursor: pointer;
}

.pr-book-btn.read {
    color: white;

    background:
        linear-gradient(
            135deg,
            #d39bc4,
            #8e9fdf
        );
}

.pr-book-btn.download {
    background: #eef2f8;
    color: #576780;
}


/* ======================================================
   EMPTY / ERROR
   ====================================================== */

.pr-message {
    grid-column: 1 / -1;

    padding: 38px 18px;

    border-radius: 18px;

    background: #ffffff;

    text-align: center;

    color: #64748b;

    border:
        1px solid
        rgba(15,23,42,.06);
}

.pr-message strong {
    display: block;

    margin-bottom: 6px;

    color: #25324a;
    font-size: 17px;
}


/* ======================================================
   PAGINATION
   ====================================================== */

.pr-pagination {
    display: none;

    align-items: center;
    justify-content: center;
    gap: 8px;

    margin-top: 22px;
}

.pr-page-btn {
    min-height: 40px;

    border: 0;
    border-radius: 11px;

    padding: 9px 13px;

    background: white;
    color: #576780;

    font: inherit;
    font-size: 12px;
    font-weight: 800;

    cursor: pointer;

    box-shadow:
        0 4px 14px
        rgba(15,23,42,.07);
}

.pr-page-btn:disabled {
    opacity: .4;
    cursor: default;
}

.pr-page-text {
    color: #7c899f;
    font-size: 12px;
}


/* ======================================================
   TOAST
   ====================================================== */

.pr-toast {
    position: fixed;

    left: 12px;
    right: 12px;
    bottom: 14px;

    z-index: 1000;

    display: none;

    padding: 13px 15px;

    border-radius: 13px;

    background: #111827;
    color: white;

    font-size: 13px;
    line-height: 1.4;

    box-shadow:
        0 14px 32px
        rgba(0,0,0,.20);
}


/* ======================================================
   TABLET
   ====================================================== */

@media (min-width: 600px) {

    .pr-page {
        padding: 20px 18px 50px;
    }

    .pr-admin-actions {
        grid-template-columns:
            repeat(2, minmax(0, 1fr));
    }

    .pr-grid {
        grid-template-columns:
            repeat(2, minmax(0, 1fr));
        gap: 14px;
    }

}


/* ======================================================
   DESKTOP
   ====================================================== */

@media (min-width: 900px) {

    .pr-page {
        padding: 28px 24px 60px;
    }

    .pr-hero {
        flex-direction: row;
        align-items: flex-start;
        justify-content: space-between;

        padding: 27px 28px;
        margin-bottom: 18px;
    }

    .pr-title {
        font-size: 46px;
    }

    .pr-subtitle {
        font-size: 15px;
    }

    .pr-admin-actions {
        flex: 0 0 auto;

        display: flex;
        width: auto;

        align-items: center;
        justify-content: flex-end;
    }

    .pr-admin-btn {
        width: auto;
        min-width: 140px;
    }

    .pr-search-card {
        padding: 17px 18px;
    }

    .pr-search-row.has-dropdown {
        grid-template-columns:
            minmax(0, 1fr)
            minmax(220px, 310px);
    }

    .pr-status {
        flex-direction: row;
        justify-content: space-between;
        align-items: center;
    }

    .pr-grid {
        grid-template-columns:
            repeat(
                auto-fill,
                minmax(220px, 1fr)
            );

        gap: 18px;
    }

    .pr-book-card {
        display: flex;
        flex-direction: column;

        min-height: 445px;

        padding: 14px;

        border-radius: 20px;
    }

    .pr-cover-wrap {
        width: 100%;
        aspect-ratio: 2 / 3;

        border-radius: 15px;
    }

    .pr-book-content {
        flex: 1;

        padding-top: 13px;
    }

    .pr-book-title {
        font-size: 17px;
    }

    .pr-toast {
        left: auto;
        right: 20px;
        bottom: 20px;

        width: 360px;
    }

}


/* ======================================================
   LARGE DESKTOP
   ====================================================== */

@media (min-width: 1250px) {

    .pr-grid {
        grid-template-columns:
            repeat(5, minmax(0, 1fr));
    }

}

</style>


<div class="pr-page">

    <!-- =================================================
         HERO
         ================================================= -->

    <section class="pr-hero">

        <div>
            <div class="pr-kicker">
                📚 District 4 Digital Library
            </div>

            <h1 class="pr-title">
                Pastor's Resources
            </h1>

            <p class="pr-subtitle">
                A curated digital library to equip,
                encourage, and inspire you in ministry.
                Search books and authors without waiting
                for Google Drive to rescan the collection.
            </p>
        </div>


        {% if is_admin %}

        <div class="pr-admin-actions">

            <button
                class="pr-admin-btn primary"
                id="syncButton"
                type="button"
                onclick="syncBooks()"
            >
                🔄 Sync Books
            </button>

            <a
                class="pr-admin-btn"
                href="{{ url_for('sermon_ebooks_home') }}"
            >
                📚 Sermon eBooks
            </a>

        </div>

        {% endif %}

    </section>


    <!-- =================================================
         SEARCH
         ================================================= -->

    <section class="pr-search-card">

        <div
            class="pr-search-row"
            id="searchRow"
        >

            <input
                id="searchInput"
                class="pr-search-input"
                type="search"
                placeholder="Search books, authors, topics..."
                autocomplete="off"
            >

            <select
                id="filterSelect"
                class="pr-filter-select"
                aria-label="Library filter"
                onchange="filterSelectionChanged()"
            >
            </select>

        </div>

        <div class="pr-filters">

            <button
                class="pr-filter active"
                type="button"
                onclick="changeMode('all', this)"
            >
                All
            </button>

            <button
                class="pr-filter"
                type="button"
                onclick="changeMode('author', this)"
            >
                Author
            </button>

            <button
                class="pr-filter"
                type="button"
                onclick="changeMode('category', this)"
            >
                Category
            </button>

            <button
                class="pr-filter"
                type="button"
                onclick="changeMode('recent', this)"
            >
                Recently Added
            </button>

        </div>

    </section>


    <!-- =================================================
         DATABASE STATUS
         ================================================= -->

    <section class="pr-status">

        <div class="pr-status-main">
            <span
                class="pr-status-dot waiting"
                id="statusDot"
            ></span>

            <span id="statusText">
                Loading library database...
            </span>
        </div>

        <div
            class="pr-sync-detail"
            id="syncDetail"
        ></div>

    </section>


    <!-- =================================================
         BOOKS
         ================================================= -->

    <div class="pr-books-heading">

        <div>
            <h2>Books</h2>

            <div
                class="pr-book-count"
                id="bookCount"
            >
                Loading...
            </div>
        </div>

    </div>


    <div
        class="pr-grid"
        id="bookGrid"
    >
        <div class="pr-message">
            Loading books from the local database...
        </div>
    </div>


    <!-- =================================================
         PAGINATION
         ================================================= -->

    <div
        class="pr-pagination"
        id="pagination"
    >

        <button
            class="pr-page-btn"
            id="previousButton"
            type="button"
            onclick="previousPage()"
        >
            ← Previous
        </button>

        <span
            class="pr-page-text"
            id="pageText"
        ></span>

        <button
            class="pr-page-btn"
            id="nextButton"
            type="button"
            onclick="nextPage()"
        >
            Next →
        </button>

    </div>

</div>


<div
    id="prToast"
    class="pr-toast"
></div>


<script>

/* =====================================================
   STATE
   ===================================================== */

let currentPage = 1;

let currentMode = "all";

let currentQuery = "";

let currentFilter = "";

let totalPages = 1;

let perPage = 24;

let searchTimer = null;


/* =====================================================
   ESCAPE HTML
   ===================================================== */

function escapeHtml(value) {

    const div =
        document.createElement(
            "div"
        );

    div.textContent =
        value || "";

    return div.innerHTML;
}


/* =====================================================
   FORMAT SYNC TIME
   ===================================================== */

function formatSyncTime(value) {

    if (!value) {
        return "Never synced";
    }

    const date =
        new Date(value);

    if (
        Number.isNaN(
            date.getTime()
        )
    ) {
        return value;
    }

    return date.toLocaleString();
}


/* =====================================================
   LOAD BOOKS FROM SQLITE
   ===================================================== */

async function loadBooks(
    page = 1
) {

    currentPage = page;

    const grid =
        document.getElementById(
            "bookGrid"
        );

    const bookCount =
        document.getElementById(
            "bookCount"
        );

    const statusText =
        document.getElementById(
            "statusText"
        );

    const statusDot =
        document.getElementById(
            "statusDot"
        );

    const syncDetail =
        document.getElementById(
            "syncDetail"
        );

    statusText.textContent =
        "Loading library database...";

    statusDot.className =
        "pr-status-dot waiting";

    try {

        const params =
            new URLSearchParams({
                page:
                    currentPage,

                per_page:
                    perPage,

                mode:
                    currentMode,

                q:
                    currentQuery,

                filter_value:
                    currentFilter
            });

        const response =
            await fetch(
                "/pastor-resources/api/books?"
                + params.toString()
            );

        const data =
            await response.json();

        if (!data.ok) {

            throw new Error(
                data.error
                || "Unable to load library."
            );
        }

        renderBooks(
            data.books
        );

        currentPage =
            data.page || 1;

        totalPages =
            data.pages || 1;

        bookCount.textContent =
            data.total.toLocaleString()
            + (
                data.total === 1
                ? " book"
                : " books"
            );

        const stats =
            data.stats || {};

        if (
            !stats.last_sync_at
        ) {

            statusDot.className =
                "pr-status-dot waiting";

            {% if is_admin %}

            statusText.textContent =
                "Library database is empty. "
                + "Press Sync Books once to build it.";

            {% else %}

            statusText.textContent =
                "The Pastor's Resources library "
                + "is being prepared.";

            {% endif %}

            syncDetail.textContent =
                "Not synced yet";

        }

        else {

            statusDot.className =
                "pr-status-dot";

            statusText.textContent =
                Number(
                    stats.unique_books
                    || 0
                ).toLocaleString()
                + " unique books • "
                + Number(
                    stats.supported_files
                    || 0
                ).toLocaleString()
                + " ebook files";

            syncDetail.textContent =
                "Last sync: "
                + formatSyncTime(
                    stats.last_sync_at
                );
        }

        updatePagination();

    }

    catch (error) {

        statusDot.className =
            "pr-status-dot error";

        statusText.textContent =
            "Unable to load library.";

        syncDetail.textContent =
            "";

        bookCount.textContent =
            "Library error";

        grid.innerHTML =
            '<div class="pr-message">'
            + '<strong>Library error</strong>'
            + escapeHtml(
                error.message
            )
            + '</div>';
    }
}


/* =====================================================
   RENDER BOOKS
   ===================================================== */

function renderBooks(
    books
) {

    const grid =
        document.getElementById(
            "bookGrid"
        );

    if (!books.length) {

        grid.innerHTML =
            '<div class="pr-message">'
            + '<strong>No books found</strong>'
            + (
                currentQuery
                ? 'Try another search.'
                : 'The library database has not been populated yet.'
            )
            + '</div>';

        return;
    }

    let html = "";

    for (
        const book
        of books
    ) {

        const title =
            escapeHtml(
                book.title
            );

        const author =
            escapeHtml(
                book.author
            );

        const category =
            escapeHtml(
                book.category
                || "General"
            );

        const thumbnail =
            escapeHtml(
                book.thumbnail_url
            );

        const formats =
            Array.isArray(
                book.formats
            )
            ? book.formats
            : [];

        let formatHtml = "";

        for (
            const format
            of formats
        ) {

            formatHtml +=
                '<span class="pr-format-chip">'
                + escapeHtml(
                    format
                )
                + '</span>';
        }

        html += `

        <article class="pr-book-card">

            <div class="pr-cover-wrap">

                <img
                    class="pr-cover"
                    src="${thumbnail}"
                    alt="${title}"
                    loading="lazy"
                    decoding="async"
                >

            </div>

            <div class="pr-book-content">

                <div class="pr-category">
                    ${category}
                </div>

                <h3 class="pr-book-title">
                    ${title}
                </h3>

                <div class="pr-author">
                    ${author}
                </div>

                <div class="pr-format-row">
                    ${formatHtml}
                </div>

                <div class="pr-actions">

                    <button
                        class="pr-book-btn read"
                        type="button"
                        onclick="readerComingSoon()"
                    >
                        Read
                    </button>

                    <button
                        class="pr-book-btn download"
                        type="button"
                        onclick="downloadComingSoon()"
                    >
                        Download
                    </button>

                </div>

            </div>

        </article>

        `;
    }

    grid.innerHTML =
        html;
}


/* =====================================================
   SEARCH
   ===================================================== */

document
    .getElementById(
        "searchInput"
    )
    .addEventListener(
        "input",
        function () {

            clearTimeout(
                searchTimer
            );

            searchTimer =
                setTimeout(
                    function () {

                        currentQuery =
                            document
                            .getElementById(
                                "searchInput"
                            )
                            .value
                            .trim();

                        loadBooks(
                            1
                        );

                    },
                    300
                );
        }
    );


/* =====================================================
   FILTER / SORT MODE
   ===================================================== */

async function loadFilterOptions(
    mode
) {

    const select =
        document.getElementById(
            "filterSelect"
        );

    const row =
        document.getElementById(
            "searchRow"
        );

    currentFilter = "";

    if (
        ![
            "author",
            "category",
            "recent"
        ].includes(
            mode
        )
    ) {

        select.classList.remove(
            "visible"
        );

        row.classList.remove(
            "has-dropdown"
        );

        select.innerHTML =
            "";

        return;
    }

    select.classList.add(
        "visible"
    );

    row.classList.add(
        "has-dropdown"
    );

    const heading =
        mode === "author"
        ? "All Authors"
        : (
            mode === "category"
            ? "All Categories"
            : "Newest First"
        );

    select.innerHTML =
        '<option value="">'
        + heading
        + '</option>';

    try {

        const response =
            await fetch(
                "/pastor-resources/api/filter-options?"
                + new URLSearchParams({
                    mode: mode
                }).toString()
            );

        const data =
            await response.json();

        if (!data.ok) {

            throw new Error(
                data.error
                || "Unable to load filters."
            );
        }

        for (
            const option
            of data.options
        ) {

            const element =
                document.createElement(
                    "option"
                );

            element.value =
                option.value;

            element.textContent =
                option.label;

            select.appendChild(
                element
            );
        }

        if (
            mode === "recent"
        ) {

            select.value =
                "all";

            currentFilter =
                "all";
        }

    }

    catch (error) {

        showToast(
            "Unable to load dropdown: "
            + error.message
        );
    }
}


async function changeMode(
    mode,
    button
) {

    document
        .querySelectorAll(
            ".pr-filter"
        )
        .forEach(
            element => {

                element
                    .classList
                    .remove(
                        "active"
                    );

            }
        );

    button
        .classList
        .add(
            "active"
        );

    currentMode =
        mode;

    await loadFilterOptions(
        mode
    );

    loadBooks(
        1
    );
}


function filterSelectionChanged() {

    const select =
        document.getElementById(
            "filterSelect"
        );

    currentFilter =
        select.value || "";

    loadBooks(
        1
    );
}


/* =====================================================
   PAGINATION
   ===================================================== */

function updatePagination() {

    const pagination =
        document.getElementById(
            "pagination"
        );

    const previous =
        document.getElementById(
            "previousButton"
        );

    const next =
        document.getElementById(
            "nextButton"
        );

    const text =
        document.getElementById(
            "pageText"
        );

    pagination.style.display =
        totalPages > 1
        ? "flex"
        : "none";

    previous.disabled =
        currentPage <= 1;

    next.disabled =
        currentPage >= totalPages;

    text.textContent =
        "Page "
        + currentPage
        + " of "
        + totalPages;
}


function previousPage() {

    if (
        currentPage > 1
    ) {

        loadBooks(
            currentPage - 1
        );

        window.scrollTo({
            top: 120,
            behavior: "smooth"
        });
    }
}


function nextPage() {

    if (
        currentPage
        < totalPages
    ) {

        loadBooks(
            currentPage + 1
        );

        window.scrollTo({
            top: 120,
            behavior: "smooth"
        });
    }
}


/* =====================================================
   SYNC BOOKS
   ===================================================== */

async function syncBooks() {

    const button =
        document.getElementById(
            "syncButton"
        );

    if (!button) {
        return;
    }

    const oldText =
        button.textContent;

    button.disabled =
        true;

    button.textContent =
        "⏳ Syncing...";

    const statusText =
        document.getElementById(
            "statusText"
        );

    const statusDot =
        document.getElementById(
            "statusDot"
        );

    const syncDetail =
        document.getElementById(
            "syncDetail"
        );

    statusDot.className =
        "pr-status-dot waiting";

    statusText.textContent =
        "Scanning Google Drive and updating "
        + "the local library database...";

    syncDetail.textContent =
        "This can take several minutes.";

    showToast(
        "Sync started. Keep this page open "
        + "until it finishes."
    );

    try {

        const response =
            await fetch(
                "/pastor-resources/sync-books",
                {
                    method:
                        "POST"
                }
            );

        const data =
            await response.json();

        if (!data.ok) {

            throw new Error(
                data.error
                || "Synchronization failed."
            );
        }

        const stats =
            data.stats || {};

        showToast(
            "Sync complete: "
            + Number(
                stats.unique_books
                || 0
            ).toLocaleString()
            + " unique books • "
            + Number(
                stats.new_books
                || 0
            ).toLocaleString()
            + " new."
        );

        await loadBooks(
            1
        );

    }

    catch (error) {

        statusDot.className =
            "pr-status-dot error";

        statusText.textContent =
            "Sync failed.";

        syncDetail.textContent =
            error.message;

        showToast(
            "Sync failed: "
            + error.message
        );

    }

    finally {

        button.disabled =
            false;

        button.textContent =
            oldText;
    }
}


/* =====================================================
   TEMPORARY READ / DOWNLOAD
   ===================================================== */

function readerComingSoon() {

    showToast(
        "The built-in PDF/EPUB reader "
        + "is the next development step."
    );
}


function downloadComingSoon() {

    showToast(
        "The Download button will be connected "
        + "after the library foundation is verified."
    );
}


/* =====================================================
   TOAST
   ===================================================== */

function showToast(
    message
) {

    const toast =
        document.getElementById(
            "prToast"
        );

    toast.textContent =
        message;

    toast.style.display =
        "block";

    clearTimeout(
        toast.hideTimer
    );

    toast.hideTimer =
        setTimeout(
            function () {

                toast.style.display =
                    "none";

            },
            5000
        );
}


/* =====================================================
   INITIAL DATABASE LOAD
   ===================================================== */

loadBooks(1);

</script>

{% endblock %}
"""


# =========================================================
# ROUTES
# =========================================================

def register_pastor_resources_routes(
    app
):
    # Create the local database tables when the app starts.
    # This does NOT scan Google Drive.
    ensure_resource_tables()

    # -----------------------------------------------------
    # MAIN PAGE
    # -----------------------------------------------------

    @app.route(
        "/pastor-resources"
    )
    def pastor_resources():

        if not any_user_logged_in():
            return redirect(
                url_for(
                    "splash"
                )
            )

        return render_template_string(
            PASTOR_RESOURCES_HTML,
            is_admin=
                is_resource_admin(),
        )

    # -----------------------------------------------------
    # LEGACY MENU / OLD URL
    # -----------------------------------------------------

    @app.route(
        "/download-resources"
    )
    def download_resources():

        return redirect(
            url_for(
                "pastor_resources"
            )
        )

    # -----------------------------------------------------
    # BOOK DATABASE API
    # -----------------------------------------------------

    @app.route(
        "/pastor-resources/api/books"
    )
    def pastor_resources_api_books():

        if not any_user_logged_in():
            return jsonify(
                {
                    "ok":
                        False,

                    "error":
                        "Unauthorized",
                }
            ), 401

        try:
            query = str(
                request.args.get(
                    "q",
                    "",
                )
            ).strip()

            mode = str(
                request.args.get(
                    "mode",
                    "all",
                )
            ).strip().lower()

            filter_value = str(
                request.args.get(
                    "filter_value",
                    "",
                )
            ).strip()

            try:
                page = int(
                    request.args.get(
                        "page",
                        1,
                    )
                )
            except Exception:
                page = 1

            try:
                per_page = int(
                    request.args.get(
                        "per_page",
                        24,
                    )
                )
            except Exception:
                per_page = 24

            result = (
                search_library_database(
                    query=query,
                    mode=mode,
                    filter_value=filter_value,
                    page=page,
                    per_page=per_page,
                )
            )

            stats = (
                get_library_database_status()
            )

            response_books = []

            for book in result[
                "books"
            ]:
                response_book = dict(
                    book
                )

                response_book[
                    "thumbnail_url"
                ] = url_for(
                    "pastor_resources_thumbnail",
                    book_id=
                        book["id"],
                )

                response_books.append(
                    response_book
                )

            return jsonify(
                {
                    "ok":
                        True,

                    "books":
                        response_books,

                    "page":
                        result["page"],

                    "pages":
                        result["pages"],

                    "per_page":
                        result[
                            "per_page"
                        ],

                    "total":
                        result["total"],

                    "stats":
                        stats,
                }
            )

        except Exception as error:

            return jsonify(
                {
                    "ok":
                        False,

                    "error":
                        str(error),
                }
            ), 500

    # -----------------------------------------------------
    # FILTER DROPDOWN OPTIONS
    # -----------------------------------------------------

    @app.route(
        "/pastor-resources/api/filter-options"
    )
    def pastor_resources_filter_options():

        if not any_user_logged_in():
            return jsonify(
                {
                    "ok":
                        False,

                    "error":
                        "Unauthorized",
                }
            ), 401

        try:
            mode = str(
                request.args.get(
                    "mode",
                    "",
                )
            ).strip().lower()

            options = (
                get_library_filter_options(
                    mode
                )
            )

            return jsonify(
                {
                    "ok":
                        True,

                    "mode":
                        mode,

                    "options":
                        options,
                }
            )

        except Exception as error:

            return jsonify(
                {
                    "ok":
                        False,

                    "error":
                        str(error),
                }
            ), 500


    # -----------------------------------------------------
    # PRIVATE SYNC BOOKS
    # -----------------------------------------------------

    @app.route(
        "/pastor-resources/sync-books",
        methods=["POST"],
    )
    def pastor_resources_sync_books():

        if not any_user_logged_in():
            return jsonify(
                {
                    "ok":
                        False,

                    "error":
                        "Unauthorized",
                }
            ), 401

        if not is_resource_admin():
            return jsonify(
                {
                    "ok":
                        False,

                    "error":
                        "Administrator access required.",
                }
            ), 403

        if not SYNC_LOCK.acquire(
            blocking=False
        ):
            return jsonify(
                {
                    "ok":
                        False,

                    "error":
                        "A book synchronization "
                        "is already running.",
                }
            ), 409

        try:
            stats = (
                sync_library_to_database()
            )

            return jsonify(
                {
                    "ok":
                        True,

                    "message":
                        "Library synchronization completed.",

                    "stats":
                        stats,
                }
            )

        except Exception as error:

            return jsonify(
                {
                    "ok":
                        False,

                    "error":
                        str(error),
                }
            ), 500

        finally:
            SYNC_LOCK.release()

    # -----------------------------------------------------
    # BOOK THUMBNAIL
    # -----------------------------------------------------

    @app.route(
        "/pastor-resources/thumbnail/<int:book_id>"
    )
    def pastor_resources_thumbnail(
        book_id
    ):

        if not any_user_logged_in():
            return Response(
                status=401
            )

        cached_path, cached_mime = (
            get_cached_thumbnail(
                book_id
            )
        )

        if cached_path:
            return send_file(
                cached_path,
                mimetype=
                    cached_mime,
                max_age=
                    604800,
                conditional=
                    True,
            )

        source = (
            get_thumbnail_source_for_book(
                book_id
            )
        )

        if not source:
            return Response(
                make_default_cover_svg(
                    "Book"
                ),
                mimetype=
                    "image/svg+xml",
                headers={
                    "Cache-Control":
                        "public, max-age=3600",
                },
            )

        try:
            path, mime_type = (
                build_thumbnail_for_book(
                    book_id
                )
            )

            if path:
                return send_file(
                    path,
                    mimetype=
                        mime_type,
                    max_age=
                        604800,
                    conditional=
                        True,
                )

        except Exception:
            pass

        return Response(
            make_default_cover_svg(
                source["title"]
            ),
            mimetype=
                "image/svg+xml",
            headers={
                "Cache-Control":
                    "public, max-age=3600",
            },
        )

    # -----------------------------------------------------
    # DATABASE STATUS
    # -----------------------------------------------------

    @app.route(
        "/pastor-resources/status"
    )
    def pastor_resources_status():

        if not any_user_logged_in():
            return jsonify(
                {
                    "ok":
                        False,

                    "error":
                        "Unauthorized",
                }
            ), 401

        try:
            stats = (
                get_library_database_status()
            )

            return jsonify(
                {
                    "ok":
                        True,

                    "storage":
                        "SQLite database",

                    "stats":
                        stats,
                }
            )

        except Exception as error:

            return jsonify(
                {
                    "ok":
                        False,

                    "error":
                        str(error),
                }
            ), 500


# ============================================================================
# PASTOR'S RESOURCES V3
# Integrated release:
# - admin edit / safe remove / restore
# - private Drive download proxy
# - PDF.js + EPUB.js built-in reader
# - resume / real active reading time / completion
# - bookmarks / favorites
# - highlights / underline / notes / sermon notes / tags
# - EPUB typography + themes
# - PDF zoom / fit / text search
# - My Library + Reading Progress
# ============================================================================

import uuid
from urllib.parse import quote


# =========================================================
# V3 DATABASE MIGRATION HELPERS
# =========================================================

def _v3_table_columns(db, table_name):
    return {
        str(row["name"])
        for row in db.execute(
            f"PRAGMA table_info({table_name})"
        ).fetchall()
    }


def _v3_ensure_column(db, table_name, column_name, definition):
    columns = _v3_table_columns(
        db,
        table_name,
    )

    if column_name not in columns:
        db.execute(
            f"ALTER TABLE {table_name} "
            f"ADD COLUMN {column_name} {definition}"
        )


def ensure_v3_tables():
    """
    Non-destructive migration on top of the working catalog tables.
    Existing books, thumbnails and sync data are preserved.
    """

    ensure_resource_tables()

    db = get_resource_db()

    try:
        # ---------------------------------------------
        # Manual metadata + safe hidden state.
        # Sync Books never overwrites these columns.
        # ---------------------------------------------

        _v3_ensure_column(
            db,
            "pastor_library_books",
            "manual_title",
            "TEXT",
        )

        _v3_ensure_column(
            db,
            "pastor_library_books",
            "manual_author",
            "TEXT",
        )

        _v3_ensure_column(
            db,
            "pastor_library_books",
            "manual_category",
            "TEXT",
        )

        _v3_ensure_column(
            db,
            "pastor_library_books",
            "is_hidden",
            "INTEGER NOT NULL DEFAULT 0",
        )

        _v3_ensure_column(
            db,
            "pastor_library_books",
            "hidden_at",
            "TEXT",
        )

        _v3_ensure_column(
            db,
            "pastor_library_books",
            "hidden_by",
            "TEXT",
        )

        # ---------------------------------------------
        # Per-user reading state.
        # ---------------------------------------------

        db.execute(
            """
            CREATE TABLE IF NOT EXISTS pastor_reader_state (
                user_key TEXT NOT NULL,
                book_id INTEGER NOT NULL,

                favorite INTEGER NOT NULL DEFAULT 0,

                last_format TEXT,
                pdf_page INTEGER NOT NULL DEFAULT 1,
                pdf_scale REAL NOT NULL DEFAULT 1.15,
                furthest_pdf_page INTEGER NOT NULL DEFAULT 0,
                epub_cfi TEXT,

                progress_percent REAL NOT NULL DEFAULT 0,
                total_active_seconds INTEGER NOT NULL DEFAULT 0,

                last_opened_at TEXT,
                completed_at TEXT,

                theme TEXT NOT NULL DEFAULT 'light',
                epub_font_size INTEGER NOT NULL DEFAULT 100,
                epub_font_family TEXT NOT NULL DEFAULT 'Georgia, serif',
                epub_line_height REAL NOT NULL DEFAULT 1.6,

                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,

                PRIMARY KEY (user_key, book_id)
            )
            """
        )

        db.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_pastor_reader_state_user
            ON pastor_reader_state(user_key)
            """
        )

        db.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_pastor_reader_state_recent
            ON pastor_reader_state(user_key, last_opened_at)
            """
        )

        # ---------------------------------------------
        # Real active reading sessions.
        # ---------------------------------------------

        db.execute(
            """
            CREATE TABLE IF NOT EXISTS pastor_reading_sessions (
                id TEXT PRIMARY KEY,
                user_key TEXT NOT NULL,
                book_id INTEGER NOT NULL,
                format TEXT,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                active_seconds INTEGER NOT NULL DEFAULT 0,
                last_ping_at TEXT
            )
            """
        )

        db.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_pastor_reading_sessions_user
            ON pastor_reading_sessions(user_key, started_at)
            """
        )

        # ---------------------------------------------
        # Highlights, underline, notes, sermon notes.
        # locator:
        #   PDF  -> JSON normalized rectangles
        #   EPUB -> EPUB CFI string
        # ---------------------------------------------

        db.execute(
            """
            CREATE TABLE IF NOT EXISTS pastor_book_annotations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_key TEXT NOT NULL,
                book_id INTEGER NOT NULL,
                format TEXT NOT NULL,
                annotation_type TEXT NOT NULL,
                selected_text TEXT,
                locator TEXT NOT NULL,
                page INTEGER,
                color TEXT,
                note TEXT,
                tags TEXT,
                is_sermon_note INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )

        db.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_pastor_annotations_user_book
            ON pastor_book_annotations(user_key, book_id)
            """
        )

        # ---------------------------------------------
        # Multiple bookmarks per book.
        # ---------------------------------------------

        db.execute(
            """
            CREATE TABLE IF NOT EXISTS pastor_book_bookmarks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_key TEXT NOT NULL,
                book_id INTEGER NOT NULL,
                format TEXT NOT NULL,
                locator TEXT,
                page INTEGER,
                label TEXT,
                excerpt TEXT,
                created_at TEXT NOT NULL
            )
            """
        )

        db.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_pastor_bookmarks_user_book
            ON pastor_book_bookmarks(user_key, book_id)
            """
        )

        db.commit()

    finally:
        db.close()


# =========================================================
# USER IDENTITY / PRIVACY
# =========================================================

def current_reader_user_key():
    """
    Private reading data is scoped to the logged-in account.
    Prefixing the role prevents collisions if two roles ever
    use the same username.
    """

    if session.get("ao_logged_in"):
        username = str(
            session.get("ao_username")
            or session.get("username")
            or ""
        ).strip().lower()

        role = str(
            session.get("ao_role")
            or session.get("role")
            or "ao"
        ).strip().lower()

    elif session.get("pastor_logged_in"):
        username = str(
            session.get("pastor_username")
            or session.get("username")
            or ""
        ).strip().lower()

        role = "pastor"

    else:
        username = str(
            session.get("username")
            or ""
        ).strip().lower()

        role = str(
            session.get("role")
            or "member"
        ).strip().lower()

    if not username:
        return ""

    return (
        role
        + ":"
        + username
    )


def current_reader_display_name():
    return str(
        session.get("ao_name")
        or session.get("pastor_name")
        or session.get("name")
        or session.get("username")
        or "Reader"
    ).strip()


# =========================================================
# EFFECTIVE BOOK METADATA
# =========================================================

def _v3_effective_title_sql(alias="b"):
    return (
        f"COALESCE(NULLIF(TRIM({alias}.manual_title), ''), "
        f"{alias}.title)"
    )


def _v3_effective_author_sql(alias="b"):
    return (
        f"COALESCE(NULLIF(TRIM({alias}.manual_author), ''), "
        f"{alias}.author)"
    )


def _v3_effective_category_sql(alias="b"):
    return (
        f"COALESCE(NULLIF(TRIM({alias}.manual_category), ''), "
        f"{alias}.category)"
    )


def get_effective_book(book_id, include_hidden=False):
    ensure_v3_tables()

    db = get_resource_db()

    try:
        hidden_sql = ""

        if not include_hidden:
            hidden_sql = (
                " AND COALESCE(b.is_hidden, 0) = 0 "
            )

        row = db.execute(
            f"""
            SELECT
                b.id,
                {_v3_effective_title_sql('b')} AS title,
                {_v3_effective_author_sql('b')} AS author,
                {_v3_effective_category_sql('b')} AS category,
                b.folder_path,
                b.is_active,
                COALESCE(b.is_hidden, 0) AS is_hidden,
                b.manual_title,
                b.manual_author,
                b.manual_category

            FROM pastor_library_books b

            WHERE b.id = ?
              AND b.is_active = 1
              {hidden_sql}
            """,
            (
                int(book_id),
            ),
        ).fetchone()

        return dict(row) if row else None

    finally:
        db.close()


# =========================================================
# USER STATE HELPERS
# =========================================================

def ensure_reader_state(user_key, book_id):
    ensure_v3_tables()

    now_iso = utc_now_iso()

    db = get_resource_db()

    try:
        db.execute(
            """
            INSERT OR IGNORE INTO pastor_reader_state (
                user_key,
                book_id,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?)
            """,
            (
                user_key,
                int(book_id),
                now_iso,
                now_iso,
            ),
        )

        db.commit()

        row = db.execute(
            """
            SELECT *
            FROM pastor_reader_state
            WHERE user_key = ?
              AND book_id = ?
            """,
            (
                user_key,
                int(book_id),
            ),
        ).fetchone()

        return dict(row) if row else {}

    finally:
        db.close()


def get_reader_state(user_key, book_id):
    ensure_v3_tables()

    db = get_resource_db()

    try:
        row = db.execute(
            """
            SELECT *
            FROM pastor_reader_state
            WHERE user_key = ?
              AND book_id = ?
            """,
            (
                user_key,
                int(book_id),
            ),
        ).fetchone()

        if row:
            return dict(row)

    finally:
        db.close()

    return ensure_reader_state(
        user_key,
        book_id,
    )


def batch_favorite_map(user_key, book_ids):
    if not book_ids:
        return {}

    placeholders = ",".join(
        "?"
        for _ in book_ids
    )

    db = get_resource_db()

    try:
        rows = db.execute(
            f"""
            SELECT book_id, favorite
            FROM pastor_reader_state
            WHERE user_key = ?
              AND book_id IN ({placeholders})
            """,
            tuple(
                [user_key]
                + [int(v) for v in book_ids]
            ),
        ).fetchall()

        return {
            int(row["book_id"]):
                bool(row["favorite"])
            for row in rows
        }

    finally:
        db.close()


# =========================================================
# V3 LIBRARY FILTER OPTIONS
# =========================================================

def get_library_filter_options_v3(mode, user_key=""):
    ensure_v3_tables()

    mode = str(
        mode or ""
    ).strip().lower()

    if mode == "recent":
        return [
            {
                "value": "all",
                "label": "Newest first",
            },
            {
                "value": "7",
                "label": "Added in last 7 days",
            },
            {
                "value": "30",
                "label": "Added in last 30 days",
            },
            {
                "value": "90",
                "label": "Added in last 90 days",
            },
        ]

    if mode not in (
        "author",
        "category",
    ):
        return []

    effective = (
        _v3_effective_author_sql("b")
        if mode == "author"
        else _v3_effective_category_sql("b")
    )

    db = get_resource_db()

    try:
        rows = db.execute(
            f"""
            SELECT
                TRIM({effective}) AS value,
                COUNT(*) AS cnt

            FROM pastor_library_books b

            WHERE b.is_active = 1
              AND COALESCE(b.is_hidden, 0) = 0
              AND TRIM(COALESCE({effective}, '')) != ''

            GROUP BY LOWER(TRIM({effective}))

            ORDER BY LOWER(TRIM({effective}))
            """
        ).fetchall()

        return [
            {
                "value": str(row["value"] or "").strip(),
                "label": (
                    str(row["value"] or "").strip()
                    + " ("
                    + str(int(row["cnt"] or 0))
                    + ")"
                ),
            }
            for row in rows
            if str(row["value"] or "").strip()
        ]

    finally:
        db.close()


# =========================================================
# V3 LIBRARY SEARCH
# =========================================================

def search_library_database_v3(
    user_key,
    query="",
    mode="all",
    filter_value="",
    page=1,
    per_page=24,
):
    ensure_v3_tables()

    query = str(query or "").strip()
    mode = str(mode or "all").strip().lower()
    filter_value = str(filter_value or "").strip()

    try:
        page = max(1, int(page))
    except Exception:
        page = 1

    try:
        per_page = min(
            max(12, int(per_page)),
            60,
        )
    except Exception:
        per_page = 24

    title_sql = _v3_effective_title_sql("b")
    author_sql = _v3_effective_author_sql("b")
    category_sql = _v3_effective_category_sql("b")

    where_parts = [
        "b.is_active = 1",
        "COALESCE(b.is_hidden, 0) = 0",
    ]

    params = []

    if mode == "favorites":
        where_parts.append(
            "COALESCE(s.favorite, 0) = 1"
        )

    if (
        mode == "author"
        and filter_value
    ):
        where_parts.append(
            f"LOWER(TRIM({author_sql})) = LOWER(TRIM(?))"
        )
        params.append(filter_value)

    elif (
        mode == "category"
        and filter_value
    ):
        where_parts.append(
            f"LOWER(TRIM({category_sql})) = LOWER(TRIM(?))"
        )
        params.append(filter_value)

    elif (
        mode == "recent"
        and filter_value in ("7", "30", "90")
    ):
        where_parts.append(
            "datetime(b.first_seen_at) >= datetime('now', ?)"
        )
        params.append(
            "-" + filter_value + " days"
        )

    if query:
        like_value = "%" + query + "%"

        where_parts.append(
            f"""
            (
                {title_sql} LIKE ?
                OR {author_sql} LIKE ?
                OR {category_sql} LIKE ?
                OR b.folder_path LIKE ?
            )
            """
        )

        params.extend(
            [
                like_value,
                like_value,
                like_value,
                like_value,
            ]
        )

    where_sql = (
        "WHERE "
        + " AND ".join(where_parts)
    )

    if mode == "author":
        order_sql = (
            f"ORDER BY LOWER({author_sql}), LOWER({title_sql})"
        )

    elif mode == "category":
        order_sql = (
            f"ORDER BY LOWER({category_sql}), LOWER({title_sql})"
        )

    elif mode == "recent":
        order_sql = (
            "ORDER BY datetime(b.first_seen_at) DESC, "
            f"LOWER({title_sql})"
        )

    else:
        order_sql = (
            f"ORDER BY LOWER({title_sql})"
        )

    db = get_resource_db()

    try:
        join_sql = """
            LEFT JOIN pastor_reader_state s
              ON s.book_id = b.id
             AND s.user_key = ?
        """

        count_params = [user_key] + params

        count_row = db.execute(
            f"""
            SELECT COUNT(*) AS cnt
            FROM pastor_library_books b
            {join_sql}
            {where_sql}
            """,
            tuple(count_params),
        ).fetchone()

        total = int(
            count_row["cnt"] or 0
        )

        pages = max(
            1,
            math.ceil(
                total / per_page
            ),
        )

        page = min(
            page,
            pages,
        )

        offset = (
            (page - 1)
            * per_page
        )

        rows = db.execute(
            f"""
            SELECT
                b.id,
                {title_sql} AS title,
                {author_sql} AS author,
                {category_sql} AS category,
                b.folder_path,
                b.created_time,
                b.modified_time,
                b.first_seen_at,
                COALESCE(s.favorite, 0) AS favorite,
                COALESCE(s.progress_percent, 0) AS progress_percent,
                s.last_opened_at,
                s.completed_at,

                GROUP_CONCAT(
                    DISTINCT f.format
                ) AS formats

            FROM pastor_library_books b

            {join_sql}

            LEFT JOIN pastor_library_files f
              ON f.book_id = b.id
             AND f.is_active = 1
             AND f.is_duplicate = 0

            {where_sql}

            GROUP BY b.id

            {order_sql}

            LIMIT ? OFFSET ?
            """,
            tuple(
                [user_key]
                + params
                + [per_page, offset]
            ),
        ).fetchall()

        books = []

        for row in rows:
            formats = [
                value.strip()
                for value
                in str(
                    row["formats"]
                    or ""
                ).split(",")
                if value.strip()
            ]

            books.append(
                {
                    "id": int(row["id"]),
                    "title": str(row["title"] or ""),
                    "author": str(row["author"] or ""),
                    "category": str(row["category"] or ""),
                    "folder_path": str(row["folder_path"] or ""),
                    "created_time": str(row["created_time"] or ""),
                    "modified_time": str(row["modified_time"] or ""),
                    "formats": formats,
                    "favorite": bool(row["favorite"]),
                    "progress_percent": float(
                        row["progress_percent"] or 0
                    ),
                    "last_opened_at": str(
                        row["last_opened_at"] or ""
                    ),
                    "completed": bool(
                        row["completed_at"]
                    ),
                }
            )

        return {
            "books": books,
            "page": page,
            "pages": pages,
            "per_page": per_page,
            "total": total,
        }

    finally:
        db.close()


# =========================================================
# CONTINUE READING
# =========================================================

def get_continue_reading(user_key, limit=8):
    ensure_v3_tables()

    title_sql = _v3_effective_title_sql("b")
    author_sql = _v3_effective_author_sql("b")

    db = get_resource_db()

    try:
        rows = db.execute(
            f"""
            SELECT
                b.id,
                {title_sql} AS title,
                {author_sql} AS author,
                s.progress_percent,
                s.last_opened_at,
                s.last_format,
                s.pdf_page

            FROM pastor_reader_state s

            JOIN pastor_library_books b
              ON b.id = s.book_id

            WHERE s.user_key = ?
              AND b.is_active = 1
              AND COALESCE(b.is_hidden, 0) = 0
              AND COALESCE(s.progress_percent, 0) > 0
              AND s.completed_at IS NULL

            ORDER BY datetime(s.last_opened_at) DESC

            LIMIT ?
            """,
            (
                user_key,
                int(limit),
            ),
        ).fetchall()

        return [
            {
                "id": int(row["id"]),
                "title": str(row["title"] or ""),
                "author": str(row["author"] or ""),
                "progress_percent": float(
                    row["progress_percent"] or 0
                ),
                "last_opened_at": str(
                    row["last_opened_at"] or ""
                ),
                "last_format": str(
                    row["last_format"] or ""
                ),
                "pdf_page": int(
                    row["pdf_page"] or 1
                ),
            }
            for row in rows
        ]

    finally:
        db.close()


# =========================================================
# BOOK FILE SELECTION
# =========================================================

def get_book_files(book_id, include_hidden=False):
    ensure_v3_tables()

    hidden_sql = ""

    if not include_hidden:
        hidden_sql = (
            " AND COALESCE(b.is_hidden, 0) = 0 "
        )

    db = get_resource_db()

    try:
        rows = db.execute(
            f"""
            SELECT
                f.id,
                f.drive_file_id,
                f.book_id,
                f.name,
                f.format,
                f.mime_type,
                f.size

            FROM pastor_library_files f

            JOIN pastor_library_books b
              ON b.id = f.book_id

            WHERE f.book_id = ?
              AND f.is_active = 1
              AND f.is_duplicate = 0
              AND b.is_active = 1
              {hidden_sql}

            ORDER BY
                CASE
                    WHEN f.format = 'EPUB' THEN 0
                    WHEN f.format = 'PDF' THEN 1
                    ELSE 2
                END,
                f.id ASC
            """,
            (
                int(book_id),
            ),
        ).fetchall()

        return [
            dict(row)
            for row in rows
        ]

    finally:
        db.close()


def choose_book_file(
    book_id,
    preferred_format="",
    purpose="read",
):
    files = get_book_files(
        book_id,
        include_hidden=is_resource_admin(),
    )

    if not files:
        return None

    preferred_format = str(
        preferred_format or ""
    ).strip().upper()

    if preferred_format:
        for item in files:
            if str(
                item.get("format") or ""
            ).upper() == preferred_format:
                return item

    # Reading prefers EPUB because it supports reflow and
    # richer typography. Downloading prefers PDF when one
    # exists because it is the more universally expected
    # downloadable document format.
    order = (
        ["PDF", "EPUB"]
        if purpose == "download"
        else ["EPUB", "PDF"]
    )

    for wanted in order:
        for item in files:
            if str(
                item.get("format") or ""
            ).upper() == wanted:
                return item

    return files[0]


def get_file_row(file_row_id):
    ensure_v3_tables()

    db = get_resource_db()

    try:
        row = db.execute(
            """
            SELECT
                f.*,
                COALESCE(b.is_hidden, 0) AS book_hidden,
                b.is_active AS book_active

            FROM pastor_library_files f

            JOIN pastor_library_books b
              ON b.id = f.book_id

            WHERE f.id = ?
              AND f.is_active = 1
              AND f.is_duplicate = 0
            """,
            (
                int(file_row_id),
            ),
        ).fetchone()

        return dict(row) if row else None

    finally:
        db.close()


# =========================================================
# PRIVATE DRIVE BYTE STREAM / RANGE PROXY
# =========================================================

def make_drive_stream_response(
    file_row,
    as_attachment=False,
):
    drive_file_id = str(
        file_row.get("drive_file_id")
        or ""
    ).strip()

    if not drive_file_id:
        return Response(
            "Missing Drive file ID",
            status=404,
        )

    mime_type = str(
        file_row.get("mime_type")
        or "application/octet-stream"
    ).strip()

    filename = str(
        file_row.get("name")
        or "ebook"
    ).strip()

    size = int(
        file_row.get("size")
        or 0
    )

    if request.method == "HEAD":
        headers = {
            "Content-Type": mime_type,
            "Accept-Ranges": "bytes",
        }

        if size > 0:
            headers["Content-Length"] = str(size)

        return Response(
            status=200,
            headers=headers,
        )

    drive_session = get_drive_session()

    request_headers = {}

    range_header = str(
        request.headers.get("Range")
        or ""
    ).strip()

    if range_header:
        request_headers[
            "Range"
        ] = range_header

    response = drive_session.get(
        (
            "https://www.googleapis.com/"
            "drive/v3/files/"
            + drive_file_id
        ),
        params={
            "alt": "media",
            "supportsAllDrives": "true",
        },
        headers=request_headers,
        stream=True,
        timeout=120,
    )

    if response.status_code not in (
        200,
        206,
    ):
        response.close()

        return Response(
            "Unable to retrieve ebook from Google Drive.",
            status=response.status_code,
        )

    def generate():
        try:
            for chunk in response.iter_content(
                chunk_size=256 * 1024
            ):
                if chunk:
                    yield chunk
        finally:
            response.close()

    headers = {
        "Content-Type": str(
            response.headers.get(
                "Content-Type"
            )
            or mime_type
        ),
        "Accept-Ranges": "bytes",
        "Cache-Control": "private, max-age=0",
    }

    for header_name in (
        "Content-Length",
        "Content-Range",
        "ETag",
        "Last-Modified",
    ):
        value = response.headers.get(
            header_name
        )

        if value:
            headers[
                header_name
            ] = value

    if as_attachment:
        headers[
            "Content-Disposition"
        ] = (
            "attachment; filename*=UTF-8''"
            + quote(filename)
        )

    return Response(
        generate(),
        status=response.status_code,
        headers=headers,
        direct_passthrough=True,
    )


# =========================================================
# ADMIN EDIT / HIDE / RESTORE
# =========================================================

def admin_edit_book(
    book_id,
    title,
    author,
    category,
):
    ensure_v3_tables()

    title = clean_text(title)
    author = clean_text(author)
    category = clean_text(category)

    if not title:
        raise ValueError(
            "Book title is required."
        )

    db = get_resource_db()

    try:
        row = db.execute(
            """
            SELECT id
            FROM pastor_library_books
            WHERE id = ?
            """,
            (
                int(book_id),
            ),
        ).fetchone()

        if not row:
            raise ValueError(
                "Book was not found."
            )

        db.execute(
            """
            UPDATE pastor_library_books

            SET manual_title = ?,
                manual_author = ?,
                manual_category = ?

            WHERE id = ?
            """,
            (
                title,
                author,
                category,
                int(book_id),
            ),
        )

        db.commit()

        return get_effective_book(
            book_id,
            include_hidden=True,
        )

    finally:
        db.close()


def admin_hide_book(book_id):
    ensure_v3_tables()

    db = get_resource_db()

    try:
        cursor = db.execute(
            """
            UPDATE pastor_library_books

            SET is_hidden = 1,
                hidden_at = ?,
                hidden_by = ?

            WHERE id = ?
              AND is_active = 1
            """,
            (
                utc_now_iso(),
                current_reader_user_key(),
                int(book_id),
            ),
        )

        if cursor.rowcount <= 0:
            raise ValueError("Book was not found or is no longer active.")

        db.commit()

    finally:
        db.close()


def admin_restore_book(book_id):
    ensure_v3_tables()

    db = get_resource_db()

    try:
        db.execute(
            """
            UPDATE pastor_library_books

            SET is_hidden = 0,
                hidden_at = NULL,
                hidden_by = NULL

            WHERE id = ?
            """,
            (
                int(book_id),
            ),
        )

        db.commit()

    finally:
        db.close()


def get_hidden_books():
    ensure_v3_tables()

    db = get_resource_db()

    try:
        rows = db.execute(
            f"""
            SELECT
                b.id,
                {_v3_effective_title_sql('b')} AS title,
                {_v3_effective_author_sql('b')} AS author,
                {_v3_effective_category_sql('b')} AS category,
                b.hidden_at,
                b.hidden_by

            FROM pastor_library_books b

            WHERE b.is_active = 1
              AND COALESCE(b.is_hidden, 0) = 1

            ORDER BY LOWER({_v3_effective_title_sql('b')})
            """
        ).fetchall()

        return [
            dict(row)
            for row in rows
        ]

    finally:
        db.close()


# =========================================================
# STATE SAVE / FAVORITE / COMPLETION
# =========================================================

def save_reader_state(
    user_key,
    book_id,
    payload,
):
    state = ensure_reader_state(
        user_key,
        book_id,
    )

    allowed = {}

    if "last_format" in payload:
        value = str(
            payload.get("last_format")
            or ""
        ).upper()

        if value in (
            "PDF",
            "EPUB",
        ):
            allowed[
                "last_format"
            ] = value

    if "pdf_page" in payload:
        try:
            page = max(
                1,
                int(
                    payload.get("pdf_page")
                    or 1
                ),
            )

            allowed[
                "pdf_page"
            ] = page

            allowed[
                "furthest_pdf_page"
            ] = max(
                int(
                    state.get(
                        "furthest_pdf_page"
                    )
                    or 0
                ),
                page,
            )
        except Exception:
            pass

    if "pdf_scale" in payload:
        try:
            allowed[
                "pdf_scale"
            ] = min(
                max(
                    float(
                        payload.get(
                            "pdf_scale"
                        )
                    ),
                    0.5,
                ),
                4.0,
            )
        except Exception:
            pass

    if "epub_cfi" in payload:
        allowed[
            "epub_cfi"
        ] = str(
            payload.get("epub_cfi")
            or ""
        )[:4000]

    if "progress_percent" in payload:
        try:
            progress = min(
                max(
                    float(
                        payload.get(
                            "progress_percent"
                        )
                        or 0
                    ),
                    0.0,
                ),
                100.0,
            )

            allowed[
                "progress_percent"
            ] = progress

            # Reaching the end automatically marks it done.
            if progress >= 98.5:
                allowed[
                    "completed_at"
                ] = (
                    state.get(
                        "completed_at"
                    )
                    or utc_now_iso()
                )
        except Exception:
            pass

    if "theme" in payload:
        theme = str(
            payload.get("theme")
            or "light"
        ).lower()

        if theme in (
            "light",
            "sepia",
            "dark",
        ):
            allowed[
                "theme"
            ] = theme

    if "epub_font_size" in payload:
        try:
            allowed[
                "epub_font_size"
            ] = min(
                max(
                    int(
                        payload.get(
                            "epub_font_size"
                        )
                        or 100
                    ),
                    70,
                ),
                220,
            )
        except Exception:
            pass

    if "epub_font_family" in payload:
        family = str(
            payload.get(
                "epub_font_family"
            )
            or "Georgia, serif"
        )

        allowed[
            "epub_font_family"
        ] = family[:120]

    if "epub_line_height" in payload:
        try:
            allowed[
                "epub_line_height"
            ] = min(
                max(
                    float(
                        payload.get(
                            "epub_line_height"
                        )
                        or 1.6
                    ),
                    1.1,
                ),
                2.6,
            )
        except Exception:
            pass

    allowed[
        "last_opened_at"
    ] = utc_now_iso()

    allowed[
        "updated_at"
    ] = utc_now_iso()

    if not allowed:
        return state

    columns = list(
        allowed.keys()
    )

    values = [
        allowed[column]
        for column in columns
    ]

    set_sql = ", ".join(
        column + " = ?"
        for column in columns
    )

    db = get_resource_db()

    try:
        db.execute(
            f"""
            UPDATE pastor_reader_state
            SET {set_sql}
            WHERE user_key = ?
              AND book_id = ?
            """,
            tuple(
                values
                + [
                    user_key,
                    int(book_id),
                ]
            ),
        )

        db.commit()

    finally:
        db.close()

    return get_reader_state(
        user_key,
        book_id,
    )


def set_book_favorite(
    user_key,
    book_id,
    favorite,
):
    ensure_reader_state(
        user_key,
        book_id,
    )

    db = get_resource_db()

    try:
        db.execute(
            """
            UPDATE pastor_reader_state

            SET favorite = ?,
                updated_at = ?

            WHERE user_key = ?
              AND book_id = ?
            """,
            (
                1 if favorite else 0,
                utc_now_iso(),
                user_key,
                int(book_id),
            ),
        )

        db.commit()

    finally:
        db.close()


def set_book_completed(
    user_key,
    book_id,
    completed,
):
    ensure_reader_state(
        user_key,
        book_id,
    )

    db = get_resource_db()

    try:
        db.execute(
            """
            UPDATE pastor_reader_state

            SET completed_at = ?,
                progress_percent = CASE
                    WHEN ? = 1 THEN 100
                    ELSE progress_percent
                END,
                updated_at = ?

            WHERE user_key = ?
              AND book_id = ?
            """,
            (
                utc_now_iso()
                if completed
                else None,
                1 if completed else 0,
                utc_now_iso(),
                user_key,
                int(book_id),
            ),
        )

        db.commit()

    finally:
        db.close()


# =========================================================
# ACTIVE READING SESSION TIMER
# =========================================================

def start_reading_session(
    user_key,
    book_id,
    book_format,
):
    ensure_reader_state(
        user_key,
        book_id,
    )

    session_id = str(
        uuid.uuid4()
    )

    now_iso = utc_now_iso()

    db = get_resource_db()

    try:
        db.execute(
            """
            INSERT INTO pastor_reading_sessions (
                id,
                user_key,
                book_id,
                format,
                started_at,
                active_seconds,
                last_ping_at
            )
            VALUES (?, ?, ?, ?, ?, 0, ?)
            """,
            (
                session_id,
                user_key,
                int(book_id),
                str(book_format or ""),
                now_iso,
                now_iso,
            ),
        )

        db.execute(
            """
            UPDATE pastor_reader_state

            SET last_format = ?,
                last_opened_at = ?,
                updated_at = ?

            WHERE user_key = ?
              AND book_id = ?
            """,
            (
                str(book_format or ""),
                now_iso,
                now_iso,
                user_key,
                int(book_id),
            ),
        )

        db.commit()

    finally:
        db.close()

    return session_id


def ping_reading_session(
    user_key,
    session_id,
    active_seconds,
):
    try:
        active_seconds = int(
            active_seconds
        )
    except Exception:
        active_seconds = 0

    # A client ping should never add an unreasonable block.
    active_seconds = min(
        max(
            active_seconds,
            0,
        ),
        60,
    )

    if active_seconds <= 0:
        return

    db = get_resource_db()

    try:
        row = db.execute(
            """
            SELECT book_id
            FROM pastor_reading_sessions
            WHERE id = ?
              AND user_key = ?
              AND ended_at IS NULL
            """,
            (
                session_id,
                user_key,
            ),
        ).fetchone()

        if not row:
            return

        book_id = int(
            row["book_id"]
        )

        now_iso = utc_now_iso()

        db.execute(
            """
            UPDATE pastor_reading_sessions

            SET active_seconds = active_seconds + ?,
                last_ping_at = ?

            WHERE id = ?
              AND user_key = ?
            """,
            (
                active_seconds,
                now_iso,
                session_id,
                user_key,
            ),
        )

        db.execute(
            """
            UPDATE pastor_reader_state

            SET total_active_seconds = total_active_seconds + ?,
                last_opened_at = ?,
                updated_at = ?

            WHERE user_key = ?
              AND book_id = ?
            """,
            (
                active_seconds,
                now_iso,
                now_iso,
                user_key,
                book_id,
            ),
        )

        db.commit()

    finally:
        db.close()


def end_reading_session(
    user_key,
    session_id,
):
    db = get_resource_db()

    try:
        db.execute(
            """
            UPDATE pastor_reading_sessions

            SET ended_at = COALESCE(ended_at, ?)

            WHERE id = ?
              AND user_key = ?
            """,
            (
                utc_now_iso(),
                session_id,
                user_key,
            ),
        )

        db.commit()

    finally:
        db.close()


# =========================================================
# BOOKMARKS
# =========================================================

def get_bookmarks(
    user_key,
    book_id=None,
):
    ensure_v3_tables()

    title_sql = _v3_effective_title_sql("b")
    author_sql = _v3_effective_author_sql("b")

    params = [user_key]
    extra = ""

    if book_id is not None:
        extra = " AND m.book_id = ? "
        params.append(int(book_id))

    db = get_resource_db()

    try:
        rows = db.execute(
            f"""
            SELECT
                m.*,
                {title_sql} AS book_title,
                {author_sql} AS book_author

            FROM pastor_book_bookmarks m

            JOIN pastor_library_books b
              ON b.id = m.book_id

            WHERE m.user_key = ?
              {extra}
              AND b.is_active = 1

            ORDER BY datetime(m.created_at) DESC
            """,
            tuple(params),
        ).fetchall()

        return [
            dict(row)
            for row in rows
        ]

    finally:
        db.close()


def add_bookmark(
    user_key,
    book_id,
    book_format,
    locator,
    page,
    label,
    excerpt,
):
    ensure_v3_tables()

    db = get_resource_db()

    try:
        cur = db.execute(
            """
            INSERT INTO pastor_book_bookmarks (
                user_key,
                book_id,
                format,
                locator,
                page,
                label,
                excerpt,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_key,
                int(book_id),
                str(book_format or ""),
                str(locator or "")[:5000],
                int(page)
                if page
                else None,
                str(label or "")[:200],
                str(excerpt or "")[:1000],
                utc_now_iso(),
            ),
        )

        db.commit()

        return int(
            cur.lastrowid
        )

    finally:
        db.close()


def delete_bookmark(
    user_key,
    bookmark_id,
):
    db = get_resource_db()

    try:
        db.execute(
            """
            DELETE FROM pastor_book_bookmarks
            WHERE id = ?
              AND user_key = ?
            """,
            (
                int(bookmark_id),
                user_key,
            ),
        )

        db.commit()

    finally:
        db.close()


# =========================================================
# ANNOTATIONS
# =========================================================

def get_annotations(
    user_key,
    book_id=None,
):
    ensure_v3_tables()

    title_sql = _v3_effective_title_sql("b")
    author_sql = _v3_effective_author_sql("b")

    params = [user_key]
    extra = ""

    if book_id is not None:
        extra = " AND a.book_id = ? "
        params.append(int(book_id))

    db = get_resource_db()

    try:
        rows = db.execute(
            f"""
            SELECT
                a.*,
                {title_sql} AS book_title,
                {author_sql} AS book_author

            FROM pastor_book_annotations a

            JOIN pastor_library_books b
              ON b.id = a.book_id

            WHERE a.user_key = ?
              {extra}
              AND b.is_active = 1

            ORDER BY datetime(a.created_at) DESC
            """,
            tuple(params),
        ).fetchall()

        return [
            dict(row)
            for row in rows
        ]

    finally:
        db.close()


def add_annotation(
    user_key,
    book_id,
    book_format,
    annotation_type,
    selected_text,
    locator,
    page,
    color,
    note,
    tags,
    is_sermon_note,
):
    ensure_v3_tables()

    annotation_type = str(
        annotation_type or "highlight"
    ).lower()

    if annotation_type not in (
        "highlight",
        "underline",
    ):
        annotation_type = "highlight"

    db = get_resource_db()

    try:
        now_iso = utc_now_iso()

        cur = db.execute(
            """
            INSERT INTO pastor_book_annotations (
                user_key,
                book_id,
                format,
                annotation_type,
                selected_text,
                locator,
                page,
                color,
                note,
                tags,
                is_sermon_note,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_key,
                int(book_id),
                str(book_format or ""),
                annotation_type,
                str(selected_text or "")[:12000],
                str(locator or "")[:24000],
                int(page)
                if page
                else None,
                str(color or "#ffe66d")[:40],
                str(note or "")[:12000],
                str(tags or "")[:1000],
                1 if is_sermon_note else 0,
                now_iso,
                now_iso,
            ),
        )

        db.commit()

        return int(
            cur.lastrowid
        )

    finally:
        db.close()


def update_annotation_note(
    user_key,
    annotation_id,
    note,
    tags,
    is_sermon_note,
):
    db = get_resource_db()

    try:
        db.execute(
            """
            UPDATE pastor_book_annotations

            SET note = ?,
                tags = ?,
                is_sermon_note = ?,
                updated_at = ?

            WHERE id = ?
              AND user_key = ?
            """,
            (
                str(note or "")[:12000],
                str(tags or "")[:1000],
                1 if is_sermon_note else 0,
                utc_now_iso(),
                int(annotation_id),
                user_key,
            ),
        )

        db.commit()

    finally:
        db.close()


def delete_annotation(
    user_key,
    annotation_id,
):
    db = get_resource_db()

    try:
        db.execute(
            """
            DELETE FROM pastor_book_annotations
            WHERE id = ?
              AND user_key = ?
            """,
            (
                int(annotation_id),
                user_key,
            ),
        )

        db.commit()

    finally:
        db.close()


# =========================================================
# MY LIBRARY / READING PROGRESS
# =========================================================

def get_my_library_payload(
    user_key,
    include_hidden=False,
):
    ensure_v3_tables()

    title_sql = _v3_effective_title_sql("b")
    author_sql = _v3_effective_author_sql("b")
    category_sql = _v3_effective_category_sql("b")

    db = get_resource_db()

    try:
        favorite_rows = db.execute(
            f"""
            SELECT
                b.id,
                {title_sql} AS title,
                {author_sql} AS author,
                {category_sql} AS category,
                s.progress_percent,
                s.last_opened_at

            FROM pastor_reader_state s

            JOIN pastor_library_books b
              ON b.id = s.book_id

            WHERE s.user_key = ?
              AND s.favorite = 1
              AND b.is_active = 1
              AND COALESCE(b.is_hidden, 0) = 0

            ORDER BY LOWER({title_sql})
            """,
            (
                user_key,
            ),
        ).fetchall()

        reading_rows = db.execute(
            f"""
            SELECT
                b.id,
                {title_sql} AS title,
                {author_sql} AS author,
                s.progress_percent,
                s.last_opened_at,
                s.total_active_seconds,
                s.last_format,
                s.pdf_page,
                s.furthest_pdf_page

            FROM pastor_reader_state s

            JOIN pastor_library_books b
              ON b.id = s.book_id

            WHERE s.user_key = ?
              AND b.is_active = 1
              AND COALESCE(b.is_hidden, 0) = 0
              AND COALESCE(s.progress_percent, 0) > 0
              AND s.completed_at IS NULL

            ORDER BY datetime(s.last_opened_at) DESC
            """,
            (
                user_key,
            ),
        ).fetchall()

        completed_rows = db.execute(
            f"""
            SELECT
                b.id,
                {title_sql} AS title,
                {author_sql} AS author,
                s.completed_at,
                s.total_active_seconds

            FROM pastor_reader_state s

            JOIN pastor_library_books b
              ON b.id = s.book_id

            WHERE s.user_key = ?
              AND b.is_active = 1
              AND COALESCE(b.is_hidden, 0) = 0
              AND s.completed_at IS NOT NULL

            ORDER BY datetime(s.completed_at) DESC
            """,
            (
                user_key,
            ),
        ).fetchall()

        summary = db.execute(
            """
            SELECT
                COUNT(*) AS state_books,
                SUM(CASE WHEN favorite = 1 THEN 1 ELSE 0 END) AS favorites,
                SUM(CASE WHEN completed_at IS NOT NULL THEN 1 ELSE 0 END) AS completed,
                SUM(
                    CASE
                        WHEN progress_percent > 0
                         AND completed_at IS NULL
                        THEN 1
                        ELSE 0
                    END
                ) AS currently_reading,
                SUM(total_active_seconds) AS total_active_seconds,
                SUM(furthest_pdf_page) AS pdf_pages_reached

            FROM pastor_reader_state

            WHERE user_key = ?
            """,
            (
                user_key,
            ),
        ).fetchone()

        weekly = db.execute(
            """
            SELECT
                SUM(active_seconds) AS total

            FROM pastor_reading_sessions

            WHERE user_key = ?
              AND datetime(started_at) >= datetime('now', '-7 days')
            """,
            (
                user_key,
            ),
        ).fetchone()

        monthly = db.execute(
            """
            SELECT
                SUM(active_seconds) AS total

            FROM pastor_reading_sessions

            WHERE user_key = ?
              AND datetime(started_at) >= datetime('now', '-30 days')
            """,
            (
                user_key,
            ),
        ).fetchone()

        average = db.execute(
            """
            SELECT
                AVG(active_seconds) AS avg_seconds

            FROM pastor_reading_sessions

            WHERE user_key = ?
              AND active_seconds > 0
            """,
            (
                user_key,
            ),
        ).fetchone()

        payload = {
            "favorites": [dict(row) for row in favorite_rows],
            "currently_reading": [dict(row) for row in reading_rows],
            "completed": [dict(row) for row in completed_rows],
            "bookmarks": get_bookmarks(user_key),
            "annotations": get_annotations(user_key),
            "summary": {
                "favorites": int(summary["favorites"] or 0),
                "completed": int(summary["completed"] or 0),
                "currently_reading": int(summary["currently_reading"] or 0),
                "total_active_seconds": int(summary["total_active_seconds"] or 0),
                "pdf_pages_reached": int(summary["pdf_pages_reached"] or 0),
                "weekly_seconds": int(weekly["total"] or 0),
                "monthly_seconds": int(monthly["total"] or 0),
                "average_session_seconds": int(average["avg_seconds"] or 0),
            },
        }

        if include_hidden:
            payload[
                "hidden_books"
            ] = get_hidden_books()

        return payload

    finally:
        db.close()


# =========================================================
# MAIN LIBRARY TEMPLATE
# =========================================================

PASTOR_RESOURCES_V3_HTML = r"""
{% extends "base.html" %}

{% block title %}
Pastor's Resources - District 4 Tool
{% endblock %}

{% block content %}

<style>
@import url('https://fonts.googleapis.com/css2?family=Caveat:wght@600;700&family=Lora:wght@500;600;700&family=Nunito+Sans:wght@400;600;700;800;900&display=swap');

.app-main {
    max-width: 1500px;
    padding: 0;
}

.pr-page {
    width: 100%;
    padding: 14px 12px 50px;
    color: #14213b;
    font-family: "Nunito Sans", Arial, sans-serif;
}

.pr-hero {
    padding: 18px;
    margin-bottom: 14px;
    border-radius: 20px;
    background: linear-gradient(135deg,#fff8fb 0%,#f7f6ff 50%,#eef9ff 100%);
    border: 1px solid rgba(15,23,42,.07);
    box-shadow: 0 10px 30px rgba(15,23,42,.06);
}

.pr-hero-top {
    display: flex;
    flex-direction: column;
    gap: 15px;
}

.pr-kicker {
    color: #8b5f92;
    font-size: 11px;
    font-weight: 900;
    letter-spacing: .08em;
    text-transform: uppercase;
}

.pr-title {
    margin: 2px 0 0;
    font-family: "Caveat", cursive;
    font-size: 52px;
    font-weight: 700;
    line-height: .95;
    letter-spacing: .01em;
    color: #14213b;
}

.pr-subtitle {
    max-width: 760px;
    margin: 9px 0 0;
    color: #64748b;
    font-size: 14px;
    line-height: 1.6;
}

.pr-subtitle strong {
    color: #7a5c8d;
    font-weight: 900;
    white-space: nowrap;
}

.pr-admin-actions,
.pr-resource-nav {
    display: flex;
    gap: 8px;
    flex-wrap: wrap;
}

.pr-admin-btn,
.pr-nav-btn {
    border: 0;
    border-radius: 12px;
    padding: 10px 12px;
    background: #fff;
    color: #475569;
    text-decoration: none;
    font: inherit;
    font-size: 12px;
    font-weight: 850;
    cursor: pointer;
    box-shadow: 0 5px 16px rgba(15,23,42,.07);
}

.pr-admin-btn.primary,
.pr-nav-btn.primary {
    color: white;
    background: linear-gradient(135deg,#c889c2,#779be4);
}

.pr-admin-btn:disabled { opacity: .6; cursor: not-allowed; }

.pr-resource-nav {
    margin-top: 14px;
    padding-top: 13px;
    border-top: 1px solid rgba(15,23,42,.07);
}

.pr-search-card {
    margin-bottom: 14px;
    padding: 13px;
    border-radius: 18px;
    background: #fff;
    border: 1px solid rgba(15,23,42,.07);
    box-shadow: 0 8px 26px rgba(15,23,42,.05);
}

.pr-search-row {
    display: grid;
    grid-template-columns: 1fr;
    gap: 8px;
}

.pr-search-row.has-dropdown {
    grid-template-columns: minmax(0,1fr) minmax(125px,42%);
}

.pr-search-input,
.pr-filter-select {
    width: 100%;
    min-width: 0;
    min-height: 47px;
    border: 1px solid #dbe3ee;
    border-radius: 13px;
    padding: 10px 13px;
    outline: none;
    background: white;
    color: #334155;
    font: inherit;
    font-size: 14px;
}

.pr-search-input:focus,
.pr-filter-select:focus {
    border-color: #8ea8dc;
    box-shadow: 0 0 0 3px rgba(109,142,210,.13);
}

.pr-filter-select { display:none; font-weight:700; }
.pr-filter-select.visible { display:block; }

.pr-filters {
    display: flex;
    gap: 7px;
    margin-top: 10px;
    overflow-x: auto;
    padding-bottom: 2px;
}

.pr-filter {
    flex: 0 0 auto;
    border: 0;
    border-radius: 999px;
    padding: 8px 13px;
    background: #eef2f8;
    color: #59677f;
    font: inherit;
    font-size: 12px;
    font-weight: 850;
    cursor: pointer;
}

.pr-filter.active {
    color: white;
    background: linear-gradient(135deg,#b47db9,#6f96de);
}

.pr-status {
    display: flex;
    flex-direction: column;
    gap: 6px;
    margin-bottom: 16px;
    padding: 11px 13px;
    border-radius: 15px;
    background: rgba(255,255,255,.9);
    border: 1px solid rgba(15,23,42,.06);
    color: #64748b;
    font-size: 12px;
}

.pr-status-main { display:flex; align-items:center; gap:8px; }
.pr-status-dot { width:9px; height:9px; border-radius:50%; background:#22c55e; flex:0 0 9px; }
.pr-status-dot.waiting { background:#f59e0b; }
.pr-status-dot.error { background:#ef4444; }
.pr-sync-detail { color:#94a3b8; }

.pr-section { margin-top: 18px; }
.pr-section-head { display:flex; justify-content:space-between; gap:12px; align-items:end; margin:0 2px 10px; }
.pr-section-head h2 { margin:0; font-family:"Lora",Georgia,serif; font-size:25px; font-weight:700; color:#14213b; }
.pr-muted { color:#94a3b8; font-size:12px; }

.pr-continue {
    display: flex;
    gap: 11px;
    overflow-x: auto;
    padding: 2px 1px 8px;
}

.pr-continue-card {
    flex: 0 0 260px;
    display: grid;
    grid-template-columns: 76px minmax(0,1fr);
    gap: 11px;
    padding: 11px;
    border-radius: 16px;
    background: #fff;
    border: 1px solid rgba(15,23,42,.07);
    box-shadow: 0 6px 18px rgba(15,23,42,.05);
}

.pr-mini-cover {
    width:76px;
    aspect-ratio:2/3;
    border-radius:10px;
    object-fit:cover;
    background:#e9eef7;
}

.pr-continue-title {
    margin:0;
    color:#17233c;
    font:700 14px/1.25 "Lora",Georgia,serif;
    display:-webkit-box;
    -webkit-line-clamp:3;
    -webkit-box-orient:vertical;
    overflow:hidden;
}

.pr-continue-author { margin-top:5px; color:#7c899f; font-size:11px; }
.pr-progress-track { height:6px; border-radius:999px; background:#e8edf5; overflow:hidden; margin-top:9px; }
.pr-progress-fill { height:100%; border-radius:999px; background:linear-gradient(90deg,#ca8fc0,#7299dd); }
.pr-progress-label { margin-top:4px; color:#8a96a9; font-size:10px; }
.pr-continue-btn { margin-top:8px; display:inline-flex; text-decoration:none; border-radius:9px; padding:7px 9px; background:#edf2fb; color:#52627d; font-size:11px; font-weight:850; }

.pr-grid {
    display:grid;
    grid-template-columns:1fr;
    gap:11px;
}

.pr-card {
    position:relative;
    display:grid;
    grid-template-columns:92px minmax(0,1fr);
    gap:12px;
    padding:12px;
    border-radius:18px;
    background:#fff;
    border:1px solid rgba(15,23,42,.07);
    box-shadow:0 8px 24px rgba(15,23,42,.06);
}

.pr-cover {
    width:92px;
    aspect-ratio:2/3;
    object-fit:cover;
    border-radius:12px;
    background:linear-gradient(135deg,#d8a7ca,#a9b3e6,#8ccedf);
    box-shadow:0 4px 12px rgba(15,23,42,.10);
}

.pr-card-body { min-width:0; display:flex; flex-direction:column; }
.pr-card-top { display:flex; align-items:flex-start; gap:7px; }
.pr-category { max-width:calc(100% - 38px); overflow:hidden; text-overflow:ellipsis; white-space:nowrap; padding:4px 8px; border-radius:999px; background:#f5eff8; color:#875d8e; font-size:9px; font-weight:850; }
.pr-fav { margin-left:auto; border:0; background:transparent; font-size:22px; line-height:1; cursor:pointer; color:#cbd5e1; }
.pr-fav.on { color:#e05686; }
.pr-book-title { margin:7px 0 0; color:#17233c; font:700 16px/1.25 "Lora",Georgia,serif; display:-webkit-box; -webkit-line-clamp:3; -webkit-box-orient:vertical; overflow:hidden; }
.pr-author { margin-top:6px; color:#7c899f; font-size:11px; line-height:1.35; display:-webkit-box; -webkit-line-clamp:2; -webkit-box-orient:vertical; overflow:hidden; }

.pr-actions { display:grid; grid-template-columns:1fr 1fr; gap:6px; margin-top:auto; padding-top:11px; }
.pr-btn { min-height:36px; border:0; border-radius:10px; font:800 11px/1 "Nunito Sans",Arial,sans-serif; cursor:pointer; text-decoration:none; display:flex; align-items:center; justify-content:center; }
.pr-btn.read { color:#fff; background:linear-gradient(135deg,#d39bc4,#8e9fdf); }
.pr-btn.download { background:#eef2f8; color:#576780; }
.pr-admin-row { grid-column:1/-1; display:grid; grid-template-columns:1fr 1fr; gap:6px; }
.pr-btn.edit { background:#fff7e8; color:#9a6200; }
.pr-btn.remove { background:#fff0f2; color:#b4233e; }

.pr-message { grid-column:1/-1; padding:34px 16px; border-radius:17px; background:#fff; text-align:center; color:#64748b; border:1px solid rgba(15,23,42,.06); }
.pr-message strong { display:block; color:#25324a; margin-bottom:5px; }

.pr-pagination { display:none; justify-content:center; align-items:center; gap:8px; margin-top:22px; }
.pr-page-btn { border:0; border-radius:10px; min-height:39px; padding:8px 12px; background:#fff; color:#576780; font:800 11px "Nunito Sans",Arial,sans-serif; cursor:pointer; box-shadow:0 4px 14px rgba(15,23,42,.07); }
.pr-page-btn:disabled { opacity:.4; cursor:default; }
.pr-page-text { color:#7c899f; font-size:11px; }

.pr-toast { position:fixed; left:12px; right:12px; bottom:14px; z-index:10000; display:none; padding:13px 15px; border-radius:13px; background:#111827; color:#fff; font-size:12px; line-height:1.4; box-shadow:0 14px 32px rgba(0,0,0,.2); }

.pr-modal-backdrop { position:fixed; inset:0; z-index:9000; display:none; align-items:center; justify-content:center; padding:15px; background:rgba(15,23,42,.55); }
.pr-modal-backdrop.show { display:flex; }
.pr-modal { width:min(520px,100%); max-height:92vh; overflow:auto; border-radius:20px; background:#fff; padding:18px; box-shadow:0 22px 60px rgba(0,0,0,.22); }
.pr-modal h3 { margin:0 0 13px; color:#17233c; font-family:"Lora",Georgia,serif; font-size:23px; }
.pr-field { margin-top:11px; }
.pr-field label { display:block; margin-bottom:5px; color:#52627d; font-size:11px; font-weight:850; }
.pr-field input { width:100%; min-height:44px; border:1px solid #dbe3ee; border-radius:11px; padding:10px 11px; font:inherit; font-size:13px; outline:none; }
.pr-modal-actions { display:flex; justify-content:flex-end; gap:8px; margin-top:16px; }
.pr-modal-actions button { border:0; border-radius:10px; padding:10px 13px; font-weight:850; cursor:pointer; }
.pr-modal-cancel { background:#eef2f8; color:#5b6a81; }
.pr-modal-save { background:#355fbb; color:#fff; }

/* Live Sync Books progress overlay */
.pr-sync-overlay {
    position:fixed;
    inset:0;
    z-index:12000;
    display:none;
    align-items:center;
    justify-content:center;
    padding:18px;
    background:rgba(15,23,42,.48);
    backdrop-filter:blur(2px);
}
.pr-sync-overlay.show { display:flex; }

.pr-sync-card {
    width:min(560px,100%);
    border-radius:22px;
    background:#fff;
    padding:25px 25px 23px;
    box-shadow:0 24px 70px rgba(15,23,42,.28);
    text-align:center;
}

.pr-sync-title {
    margin:0;
    color:#17233c;
    font:700 22px/1.2 "Lora",Georgia,serif;
}

.pr-sync-current {
    margin-top:10px;
    min-height:19px;
    color:#65738a;
    font-size:13px;
    line-height:1.4;
    overflow-wrap:anywhere;
}

.pr-sync-track {
    height:12px;
    margin-top:17px;
    border-radius:999px;
    overflow:hidden;
    background:#e9edf5;
}

.pr-sync-bar {
    width:0%;
    height:100%;
    border-radius:999px;
    background:linear-gradient(90deg,#c47eb7,#7398df);
    transition:width .28s ease;
}

.pr-sync-bar.indeterminate {
    width:32%;
    animation:prSyncIndeterminate 1.25s ease-in-out infinite alternate;
}

@keyframes prSyncIndeterminate {
    from { transform:translateX(-45%); }
    to { transform:translateX(220%); }
}

.pr-sync-stats {
    margin-top:14px;
    color:#34425b;
    font-size:12px;
    font-weight:850;
    line-height:1.45;
}

.pr-sync-stage {
    margin-top:6px;
    color:#8a96a9;
    font-size:11px;
    line-height:1.4;
}

.pr-sync-progress-line {
    display:flex;
    align-items:center;
    justify-content:space-between;
    gap:12px;
    margin-top:15px;
    color:#65738a;
    font-size:11px;
    line-height:1.25;
}

.pr-sync-progress-line strong {
    flex:0 0 auto;
    color:#26344d;
    font-size:12px;
    font-weight:900;
}

/* Main-library real download progress overlay. */
.pr-download-overlay {
    position:fixed;
    inset:0;
    z-index:12500;
    display:none;
    align-items:center;
    justify-content:center;
    padding:18px;
    background:rgba(15,23,42,.48);
    backdrop-filter:blur(2px);
}

.pr-download-overlay.show { display:flex; }

.pr-download-card {
    width:min(520px,100%);
    border-radius:22px;
    background:#fff;
    padding:24px;
    box-shadow:0 24px 70px rgba(15,23,42,.28);
}

.pr-download-title {
    margin:0;
    color:#17233c;
    font:700 21px/1.2 "Lora",Georgia,serif;
}

.pr-download-name {
    margin-top:8px;
    min-height:19px;
    color:#65738a;
    font-size:12px;
    line-height:1.4;
    overflow-wrap:anywhere;
}

.pr-download-track {
    height:12px;
    margin-top:16px;
    border-radius:999px;
    overflow:hidden;
    background:#e9edf5;
}

.pr-download-bar {
    width:0%;
    height:100%;
    border-radius:999px;
    background:linear-gradient(90deg,#c47eb7,#7398df);
    transition:width .18s linear;
}

.pr-download-bar.indeterminate {
    width:32%;
    animation:prSyncIndeterminate 1.25s ease-in-out infinite alternate;
}

.pr-download-meta {
    display:flex;
    align-items:center;
    justify-content:space-between;
    gap:12px;
    margin-top:10px;
    color:#65738a;
    font-size:11px;
    font-weight:800;
}

.pr-download-actions {
    display:flex;
    flex-wrap:wrap;
    justify-content:flex-end;
    gap:8px;
    margin-top:16px;
}

.pr-download-actions button,
.pr-download-actions a {
    border:0;
    border-radius:10px;
    padding:9px 12px;
    background:#eef2f8;
    color:#576780;
    font:800 11px/1 "Nunito Sans",Arial,sans-serif;
    text-decoration:none;
    cursor:pointer;
}

.pr-download-actions a {
    display:none;
    align-items:center;
    justify-content:center;
}

.pr-download-actions a.show { display:inline-flex; }

@media (min-width:600px) {
    .pr-page { padding:20px 18px 55px; }
    .pr-grid { grid-template-columns:repeat(2,minmax(0,1fr)); gap:14px; }
}

@media (min-width:900px) {
    .pr-page { padding:26px 24px 65px; }
    .pr-hero { padding:26px 27px; }
    .pr-hero-top { flex-direction:row; align-items:flex-start; justify-content:space-between; }
    .pr-title { font-size:64px; }
    .pr-admin-actions { justify-content:flex-end; }
    .pr-status { flex-direction:row; align-items:center; justify-content:space-between; }
    .pr-grid { grid-template-columns:repeat(auto-fill,minmax(220px,1fr)); gap:18px; }
    .pr-card { display:flex; flex-direction:column; min-height:440px; padding:14px; border-radius:20px; }
    .pr-cover { width:100%; aspect-ratio:2/3; border-radius:15px; }
    .pr-card-body { flex:1; padding-top:11px; }
    .pr-admin-row { margin-top:6px; }
    .pr-toast { left:auto; right:20px; bottom:20px; width:360px; }
}

@media (min-width:1250px) {
    .pr-grid { grid-template-columns:repeat(5,minmax(0,1fr)); }
}

</style>

<div class="pr-page">

    <section class="pr-hero">
        <div class="pr-hero-top">
            <div>
                <div class="pr-kicker">📚 District 4 Digital Library</div>
                <h1 class="pr-title">Pastor's Resources</h1>
                <p class="pr-subtitle">
                    “Do your best to present yourself to God as one approved, a worker who does not need to be ashamed
                    and who correctly handles the word of truth.”
                    <strong>— 2 Timothy 2:15 (NIV)</strong>
                </p>
            </div>

            {% if is_admin %}
            <div class="pr-admin-actions">
                <button class="pr-admin-btn primary" id="syncButton" type="button" onclick="syncBooks()">🔄 Sync Books</button>
                <a class="pr-admin-btn" href="{{ url_for('sermon_ebooks_home') }}">📚 Sermon eBooks</a>
                <a class="pr-admin-btn" href="{{ url_for('pastor_resources_my_library') }}?tab=hidden">🗃 Hidden Books</a>
                <a class="pr-admin-btn" href="{{ url_for('pastor_resources_database_details') }}">🗄 Database Details</a>
            </div>
            {% endif %}
        </div>

        <div class="pr-resource-nav">
            <a class="pr-nav-btn primary" href="{{ url_for('pastor_resources') }}">Library</a>
            <a class="pr-nav-btn" href="{{ url_for('pastor_resources_my_library') }}?tab=favorites">❤️ My Library</a>
            <a class="pr-nav-btn" href="{{ url_for('pastor_resources_my_library') }}?tab=bookmarks">🔖 Bookmarks</a>
            <a class="pr-nav-btn" href="{{ url_for('pastor_resources_my_library') }}?tab=highlights">🖍 Highlights / Notes</a>
            <a class="pr-nav-btn" href="{{ url_for('pastor_resources_my_library') }}?tab=sermon">📝 Sermon Notes</a>
            <a class="pr-nav-btn" href="{{ url_for('pastor_resources_my_library') }}?tab=progress">📊 Reading Progress</a>
        </div>
    </section>

    <section class="pr-search-card">
        <div class="pr-search-row" id="searchRow">
            <input id="searchInput" class="pr-search-input" type="search" placeholder="Search books, authors, topics..." autocomplete="off">
            <select id="filterSelect" class="pr-filter-select" aria-label="Library filter" onchange="filterSelectionChanged()"></select>
        </div>

        <div class="pr-filters">
            <button class="pr-filter active" data-mode="all" type="button" onclick="changeMode('all',this)">All</button>
            <button class="pr-filter" data-mode="author" type="button" onclick="changeMode('author',this)">Author</button>
            <button class="pr-filter" data-mode="category" type="button" onclick="changeMode('category',this)">Category</button>
            <button class="pr-filter" data-mode="recent" type="button" onclick="changeMode('recent',this)">Recently Added</button>
            <button class="pr-filter" data-mode="favorites" type="button" onclick="changeMode('favorites',this)">❤️ Favorites</button>
        </div>
    </section>

    <section class="pr-status">
        <div class="pr-status-main">
            <span class="pr-status-dot waiting" id="statusDot"></span>
            <span id="statusText">Loading library database...</span>
        </div>
        <div class="pr-sync-detail" id="syncDetail"></div>
    </section>

    <section class="pr-section" id="continueSection" style="display:none;">
        <div class="pr-section-head">
            <div>
                <h2>Continue Reading</h2>
                <div class="pr-muted">Resume exactly where you stopped.</div>
            </div>
        </div>
        <div class="pr-continue" id="continueGrid"></div>
    </section>

    <section class="pr-section">
        <div class="pr-section-head">
            <div>
                <h2>Books</h2>
                <div class="pr-muted" id="bookCount">Loading...</div>
            </div>
        </div>

        <div class="pr-grid" id="bookGrid">
            <div class="pr-message">Loading books from the local database...</div>
        </div>

        <div class="pr-pagination" id="pagination">
            <button class="pr-page-btn" id="previousButton" type="button" onclick="previousPage()">← Previous</button>
            <span class="pr-page-text" id="pageText"></span>
            <button class="pr-page-btn" id="nextButton" type="button" onclick="nextPage()">Next →</button>
        </div>
    </section>

</div>

<div class="pr-toast" id="prToast"></div>

{% if is_admin %}
<div class="pr-sync-overlay" id="resourceSyncOverlay" aria-live="polite" aria-busy="true">
    <div class="pr-sync-card">
        <h3 class="pr-sync-title" id="resourceSyncTitle">
            Syncing Pastor's Resources...
        </h3>

        <div class="pr-sync-current" id="resourceSyncCurrent">
            Preparing synchronization...
        </div>

        <div class="pr-sync-progress-line">
            <span id="resourceSyncCount">Preparing synchronization...</span>
            <strong id="resourceSyncPercent">Starting...</strong>
        </div>

        <div class="pr-sync-track">
            <div class="pr-sync-bar indeterminate" id="resourceSyncBar"></div>
        </div>

        <div class="pr-sync-stats" id="resourceSyncStats">
            Starting...
        </div>

        <div class="pr-sync-stage" id="resourceSyncStage">
            Please keep this page open while the library is being updated.
        </div>
    </div>
</div>
{% endif %}

<div class="pr-download-overlay" id="libraryDownloadOverlay" aria-live="polite" aria-busy="true">
    <div class="pr-download-card">
        <h3 class="pr-download-title" id="libraryDownloadTitle">Downloading ebook…</h3>
        <div class="pr-download-name" id="libraryDownloadName">Preparing download…</div>

        <div class="pr-download-track">
            <div class="pr-download-bar indeterminate" id="libraryDownloadBar"></div>
        </div>

        <div class="pr-download-meta">
            <span id="libraryDownloadBytes">Preparing…</span>
            <span id="libraryDownloadPercent"></span>
        </div>

        <div class="pr-download-actions">
            <a id="libraryDownloadDirect" href="#">Direct download</a>
            <button id="libraryDownloadCancel" type="button" onclick="cancelLibraryDownload()">Cancel</button>
        </div>
    </div>
</div>

{% if is_admin %}
<div class="pr-modal-backdrop" id="editModalBackdrop" onclick="modalBackdropClick(event)">
    <div class="pr-modal">
        <h3>Edit eBook Details</h3>
        <input type="hidden" id="editBookId">

        <div class="pr-field">
            <label>Title</label>
            <input id="editTitle" type="text">
        </div>

        <div class="pr-field">
            <label>Author</label>
            <input id="editAuthor" type="text">
        </div>

        <div class="pr-field">
            <label>Category</label>
            <input id="editCategory" type="text">
        </div>

        <div class="pr-modal-actions">
            <button class="pr-modal-cancel" type="button" onclick="closeEditModal()">Cancel</button>
            <button class="pr-modal-save" type="button" onclick="saveBookEdit()">Save Changes</button>
        </div>
    </div>
</div>
{% endif %}

<script>

const IS_RESOURCE_ADMIN = {{ 'true' if is_admin else 'false' }};

let currentPage = 1;
let currentMode = "all";
let currentQuery = "";
let currentFilter = "";
let totalPages = 1;
let perPage = 24;
let searchTimer = null;
let currentBookMap = new Map();

const LIBRARY_STATE_KEY = "pastorResourcesLibraryStateV1";
const LIBRARY_RESTORE_KEY = "pastorResourcesRestoreRequestedV1";
const READER_RETURN_URL_KEY = "pastorReaderReturnUrlV1";

function escapeHtml(value) {
    const div = document.createElement("div");
    div.textContent = value || "";
    return div.innerHTML;
}

function safeNumber(value) {
    const n = Number(value || 0);
    return Number.isFinite(n) ? n : 0;
}

function formatSyncTime(value) {
    if (!value) return "Never synced";
    const d = new Date(value);
    return Number.isNaN(d.getTime()) ? value : d.toLocaleString();
}

function showToast(message) {
    const toast = document.getElementById("prToast");
    toast.textContent = message;
    toast.style.display = "block";
    clearTimeout(toast.hideTimer);
    toast.hideTimer = setTimeout(() => {
        toast.style.display = "none";
    }, 4800);
}

let activeLibraryDownloadController = null;

function formatLibraryDownloadBytes(value) {
    const bytes = Math.max(0, Number(value || 0));
    if (!bytes) return "0 B";

    const units = ["B","KB","MB","GB"];
    const exponent = Math.min(
        units.length - 1,
        Math.floor(Math.log(bytes) / Math.log(1024))
    );
    const amount = bytes / Math.pow(1024, exponent);

    return (
        amount >= 100 || exponent === 0
            ? Math.round(amount).toLocaleString()
            : amount.toFixed(1)
    ) + " " + units[exponent];
}

function parseLibraryDownloadFilename(headerValue, fallback="ebook") {
    const header = String(headerValue || "");

    const utfMatch = header.match(/filename\*=UTF-8''([^;]+)/i);
    if (utfMatch) {
        try {
            return decodeURIComponent(utfMatch[1].trim().replace(/^"|"$/g,""));
        } catch (_error) {
            return utfMatch[1].trim().replace(/^"|"$/g,"");
        }
    }

    const normalMatch = header.match(/filename="?([^";]+)"?/i);
    if (normalMatch) return normalMatch[1].trim();

    return fallback || "ebook";
}

function updateLibraryDownloadProgress(loaded, total) {
    const bar = document.getElementById("libraryDownloadBar");
    const bytes = document.getElementById("libraryDownloadBytes");
    const percent = document.getElementById("libraryDownloadPercent");

    loaded = Math.max(0, Number(loaded || 0));
    total = Math.max(0, Number(total || 0));

    if (bytes) {
        bytes.textContent = total > 0
            ? formatLibraryDownloadBytes(loaded) + " / " + formatLibraryDownloadBytes(total)
            : formatLibraryDownloadBytes(loaded) + " downloaded";
    }

    if (!bar || !percent) return;

    if (total > 0) {
        const value = Math.min(100, Math.max(0, Math.round((loaded / total) * 100)));
        bar.classList.remove("indeterminate");
        bar.style.width = value + "%";
        percent.textContent = value + "%";
    } else {
        bar.style.width = "";
        bar.classList.add("indeterminate");
        percent.textContent = "Downloading…";
    }
}

function closeLibraryDownloadOverlay() {
    document.getElementById("libraryDownloadOverlay")?.classList.remove("show");
}

function cancelLibraryDownload() {
    if (activeLibraryDownloadController) {
        activeLibraryDownloadController.abort();
        activeLibraryDownloadController = null;
        return;
    }

    closeLibraryDownloadOverlay();
}

async function startLibraryDownload(bookId) {
    if (activeLibraryDownloadController) return;

    const book = currentBookMap.get(Number(bookId));
    if (!book?.download_url) {
        showToast("Download file is no longer available. Please refresh the library.");
        return;
    }

    const overlay = document.getElementById("libraryDownloadOverlay");
    const title = document.getElementById("libraryDownloadTitle");
    const name = document.getElementById("libraryDownloadName");
    const cancel = document.getElementById("libraryDownloadCancel");
    const direct = document.getElementById("libraryDownloadDirect");

    overlay?.classList.add("show");
    if (title) title.textContent = "Downloading ebook…";
    if (name) name.textContent = book.title || "Preparing download…";
    if (cancel) cancel.textContent = "Cancel";
    if (direct) {
        direct.href = book.download_url;
        direct.classList.remove("show");
    }
    updateLibraryDownloadProgress(0, 0);

    const controller = new AbortController();
    activeLibraryDownloadController = controller;

    try {
        const response = await fetch(book.download_url, {
            method:"GET",
            credentials:"same-origin",
            cache:"no-store",
            signal:controller.signal
        });

        if (!response.ok) {
            throw new Error("Download request failed (HTTP " + response.status + ").");
        }

        const fallbackName = (book.title || "ebook").trim() || "ebook";
        const filename = parseLibraryDownloadFilename(
            response.headers.get("Content-Disposition"),
            fallbackName
        );
        const total = Number(response.headers.get("Content-Length") || 0);
        const contentType = response.headers.get("Content-Type") || "application/octet-stream";

        if (name) name.textContent = filename;

        const chunks = [];
        let loaded = 0;

        if (response.body?.getReader) {
            const reader = response.body.getReader();

            while (true) {
                const {done, value} = await reader.read();
                if (done) break;
                if (!value) continue;

                chunks.push(value);
                loaded += value.byteLength;
                updateLibraryDownloadProgress(loaded, total);
            }
        } else {
            const blob = await response.blob();
            chunks.push(blob);
            loaded = blob.size;
            updateLibraryDownloadProgress(loaded, total || loaded);
        }

        const blob = new Blob(chunks, {type:contentType});
        updateLibraryDownloadProgress(blob.size, total || blob.size);

        const objectUrl = URL.createObjectURL(blob);
        const link = document.createElement("a");
        link.href = objectUrl;
        link.download = filename || fallbackName;
        link.style.display = "none";
        document.body.appendChild(link);
        link.click();
        link.remove();

        setTimeout(() => URL.revokeObjectURL(objectUrl), 60000);

        if (title) title.textContent = "Download complete";
        if (cancel) cancel.textContent = "Close";
        activeLibraryDownloadController = null;

    } catch (error) {
        const aborted = error?.name === "AbortError";
        activeLibraryDownloadController = null;

        if (aborted) {
            if (title) title.textContent = "Download cancelled";
            if (name) name.textContent = "The ebook download was cancelled.";
            if (cancel) cancel.textContent = "Close";
            return;
        }

        console.warn(error);
        if (title) title.textContent = "Download interrupted";
        if (name) name.textContent = error.message || "Unable to download this ebook.";
        if (cancel) cancel.textContent = "Close";
        if (direct) direct.classList.add("show");
    }
}

function captureLibraryState() {
    return {
        page: currentPage,
        mode: currentMode,
        query: currentQuery,
        filter: currentFilter,
        scrollY: Math.max(0, window.scrollY || 0)
    };
}

function rememberLibraryState() {
    try {
        sessionStorage.setItem(LIBRARY_STATE_KEY, JSON.stringify(captureLibraryState()));
        sessionStorage.setItem(READER_RETURN_URL_KEY, window.location.pathname + window.location.search);
    } catch (error) {
        console.warn("Unable to save library position", error);
    }
}

function requestedLibraryRestoreState() {
    try {
        if (sessionStorage.getItem(LIBRARY_RESTORE_KEY) !== "1") return null;
        sessionStorage.removeItem(LIBRARY_RESTORE_KEY);
        const raw = sessionStorage.getItem(LIBRARY_STATE_KEY);
        return raw ? JSON.parse(raw) : null;
    } catch (error) {
        return null;
    }
}

document.addEventListener("click", event => {
    const link = event.target.closest('a[data-reader-link="1"]');
    if (link) rememberLibraryState();
});

async function initializeLibraryPage() {
    const saved = requestedLibraryRestoreState();

    if (saved) {
        currentMode = ["all","author","category","recent","favorites"].includes(saved.mode) ? saved.mode : "all";
        currentQuery = String(saved.query || "");
        currentFilter = String(saved.filter || "");
        currentPage = Math.max(1, Number(saved.page || 1));

        const search = document.getElementById("searchInput");
        search.value = currentQuery;

        document.querySelectorAll(".pr-filter").forEach(button => {
            button.classList.toggle("active", button.dataset.mode === currentMode);
        });

        const wantedFilter = currentFilter;
        await loadFilterOptions(currentMode);
        currentFilter = wantedFilter;

        const select = document.getElementById("filterSelect");
        if (select.classList.contains("visible")) {
            select.value = currentFilter;
        }

        await Promise.all([
            loadBooks(currentPage),
            loadContinueReading()
        ]);

        setTimeout(() => {
            window.scrollTo({top: Math.max(0, Number(saved.scrollY || 0)), behavior:"auto"});
        }, 60);
        return;
    }

    await Promise.all([
        loadBooks(1),
        loadContinueReading()
    ]);
}

async function loadContinueReading() {
    try {
        const response = await fetch("/pastor-resources/api/continue-reading");
        const data = await response.json();
        if (!data.ok || !data.books.length) {
            document.getElementById("continueSection").style.display = "none";
            return;
        }

        const section = document.getElementById("continueSection");
        const grid = document.getElementById("continueGrid");
        section.style.display = "block";

        grid.innerHTML = data.books.map(book => {
            const progress = Math.min(100, Math.max(0, safeNumber(book.progress_percent)));
            return `
                <article class="pr-continue-card">
                    <img class="pr-mini-cover" loading="lazy" src="${escapeHtml(book.thumbnail_url)}" alt="${escapeHtml(book.title)}">
                    <div>
                        <h3 class="pr-continue-title">${escapeHtml(book.title)}</h3>
                        <div class="pr-continue-author">${escapeHtml(book.author)}</div>
                        <div class="pr-progress-track"><div class="pr-progress-fill" style="width:${progress}%"></div></div>
                        <div class="pr-progress-label">${Math.round(progress)}% complete</div>
                        <a class="pr-continue-btn" data-reader-link="1" href="${escapeHtml(book.read_url)}">Continue →</a>
                    </div>
                </article>
            `;
        }).join("");

    } catch (error) {
        console.warn(error);
    }
}

async function loadBooks(page = 1) {
    currentPage = page;

    const grid = document.getElementById("bookGrid");
    const statusText = document.getElementById("statusText");
    const statusDot = document.getElementById("statusDot");
    const syncDetail = document.getElementById("syncDetail");

    statusText.textContent = "Loading library database...";
    statusDot.className = "pr-status-dot waiting";

    try {
        const params = new URLSearchParams({
            page: currentPage,
            per_page: perPage,
            mode: currentMode,
            q: currentQuery,
            filter_value: currentFilter
        });

        const response = await fetch("/pastor-resources/api/books?" + params.toString());
        const data = await response.json();
        if (!data.ok) throw new Error(data.error || "Unable to load library.");

        currentPage = data.page || 1;
        totalPages = data.pages || 1;

        renderBooks(data.books || []);
        document.getElementById("bookCount").textContent = safeNumber(data.total).toLocaleString() + (data.total === 1 ? " book" : " books");

        const stats = data.stats || {};
        if (!stats.last_sync_at) {
            statusDot.className = "pr-status-dot waiting";
            statusText.textContent = IS_RESOURCE_ADMIN
                ? "Library database is empty. Press Sync Books once to build it."
                : "The Pastor's Resources library is being prepared.";
            syncDetail.textContent = "Not synced yet";
        } else {
            statusDot.className = "pr-status-dot";
            statusText.textContent = "Pastor's Resources library is ready.";
            syncDetail.textContent = IS_RESOURCE_ADMIN
                ? "Last sync: " + formatSyncTime(stats.last_sync_at)
                : "";
        }

        updatePagination();

    } catch (error) {
        statusDot.className = "pr-status-dot error";
        statusText.textContent = "Unable to load library.";
        syncDetail.textContent = "";
        grid.innerHTML = '<div class="pr-message"><strong>Library error</strong>' + escapeHtml(error.message) + '</div>';
    }
}

function renderBooks(books) {
    const grid = document.getElementById("bookGrid");
    currentBookMap = new Map((books || []).map(book => [Number(book.id), book]));

    if (!books.length) {
        grid.innerHTML = '<div class="pr-message"><strong>No books found</strong>' + (currentQuery ? 'Try another search.' : 'No books matched this filter.') + '</div>';
        return;
    }

    grid.innerHTML = books.map(book => {
        const adminButtons = IS_RESOURCE_ADMIN ? `
            <div class="pr-admin-row">
                <button class="pr-btn edit" type="button" data-admin-action="edit" data-book-id="${Number(book.id)}">✏️ Edit</button>
                <button class="pr-btn remove" type="button" data-admin-action="remove" data-book-id="${Number(book.id)}">🗑 Remove</button>
            </div>
        ` : "";

        return `
            <article class="pr-card" id="book-card-${Number(book.id)}">
                <img class="pr-cover" src="${escapeHtml(book.thumbnail_url)}" alt="${escapeHtml(book.title)}" loading="lazy" decoding="async">
                <div class="pr-card-body">
                    <div class="pr-card-top">
                        <div class="pr-category">${escapeHtml(book.category || "General")}</div>
                        <button class="pr-fav ${book.favorite ? 'on' : ''}" type="button" title="Favorite" onclick="toggleFavorite(${Number(book.id)}, this)">${book.favorite ? '♥' : '♡'}</button>
                    </div>
                    <h3 class="pr-book-title">${escapeHtml(book.title)}</h3>
                    <div class="pr-author">${escapeHtml(book.author || "Unknown Author")}</div>
                    <div class="pr-actions">
                        <a class="pr-btn read" data-reader-link="1" href="${escapeHtml(book.read_url)}">Read</a>
                        <button class="pr-btn download" type="button" onclick="startLibraryDownload(${Number(book.id)})">Download</button>
                        ${adminButtons}
                    </div>
                </div>
            </article>
        `;
    }).join("");
}

document.getElementById("searchInput").addEventListener("input", function () {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(() => {
        currentQuery = document.getElementById("searchInput").value.trim();
        loadBooks(1);
    }, 300);
});

async function loadFilterOptions(mode) {
    const select = document.getElementById("filterSelect");
    const row = document.getElementById("searchRow");
    currentFilter = "";

    if (!["author","category","recent"].includes(mode)) {
        select.classList.remove("visible");
        row.classList.remove("has-dropdown");
        select.innerHTML = "";
        return;
    }

    select.classList.add("visible");
    row.classList.add("has-dropdown");

    const heading = mode === "author" ? "All Authors" : (mode === "category" ? "All Categories" : "Newest First");
    select.innerHTML = '<option value="">' + heading + '</option>';

    try {
        const response = await fetch("/pastor-resources/api/filter-options?" + new URLSearchParams({mode}).toString());
        const data = await response.json();
        if (!data.ok) throw new Error(data.error || "Unable to load filters.");

        for (const option of data.options) {
            const el = document.createElement("option");
            el.value = option.value;
            el.textContent = option.label;
            select.appendChild(el);
        }

        if (mode === "recent") {
            select.value = "all";
            currentFilter = "all";
        }
    } catch (error) {
        showToast("Unable to load dropdown: " + error.message);
    }
}

async function changeMode(mode, button) {
    document.querySelectorAll(".pr-filter").forEach(el => el.classList.remove("active"));
    button.classList.add("active");
    currentMode = mode;
    await loadFilterOptions(mode);
    loadBooks(1);
}

function filterSelectionChanged() {
    currentFilter = document.getElementById("filterSelect").value || "";
    loadBooks(1);
}

function updatePagination() {
    const pagination = document.getElementById("pagination");
    document.getElementById("previousButton").disabled = currentPage <= 1;
    document.getElementById("nextButton").disabled = currentPage >= totalPages;
    document.getElementById("pageText").textContent = "Page " + currentPage + " of " + totalPages;
    pagination.style.display = totalPages > 1 ? "flex" : "none";
}

function previousPage() {
    if (currentPage > 1) {
        loadBooks(currentPage - 1);
        window.scrollTo({top:120,behavior:"smooth"});
    }
}

function nextPage() {
    if (currentPage < totalPages) {
        loadBooks(currentPage + 1);
        window.scrollTo({top:120,behavior:"smooth"});
    }
}

async function toggleFavorite(bookId, button) {
    const newValue = !button.classList.contains("on");

    try {
        const response = await fetch("/pastor-resources/api/favorite/" + bookId, {
            method:"POST",
            headers:{"Content-Type":"application/json"},
            body:JSON.stringify({favorite:newValue})
        });
        const data = await response.json();
        if (!data.ok) throw new Error(data.error || "Unable to update favorite.");
        button.classList.toggle("on", newValue);
        button.textContent = newValue ? "♥" : "♡";
        if (currentMode === "favorites" && !newValue) loadBooks(currentPage);
    } catch (error) {
        showToast(error.message);
    }
}

let resourceSyncPollTimer = null;

function setResourceSyncUiRunning(running) {
    const button = document.getElementById("syncButton");
    const overlay = document.getElementById("resourceSyncOverlay");

    if (button) {
        button.disabled = Boolean(running);
        button.textContent = running
            ? "⏳ Syncing Books..."
            : "🔄 Sync Books";
    }

    if (overlay) {
        overlay.classList.toggle("show", Boolean(running));
    }
}

function renderResourceSyncProgress(state) {
    const overlay = document.getElementById("resourceSyncOverlay");
    const bar = document.getElementById("resourceSyncBar");
    const current = document.getElementById("resourceSyncCurrent");
    const stats = document.getElementById("resourceSyncStats");
    const stage = document.getElementById("resourceSyncStage");
    const count = document.getElementById("resourceSyncCount");
    const percentLabel = document.getElementById("resourceSyncPercent");

    if (!overlay || !bar || !current || !stats || !stage) {
        return;
    }

    const total = Math.max(0, safeNumber(state.total));
    const processed = Math.max(0, safeNumber(state.processed));
    const newFiles = Math.max(0, safeNumber(state.new_files));
    const changedFiles = Math.max(0, safeNumber(state.changed_files));
    const unchangedFiles = Math.max(0, safeNumber(state.unchanged_files));
    const duplicates = Math.max(0, safeNumber(state.duplicates));

    const currentFile = String(state.current_file || "").trim();
    const message = String(state.message || "").trim();
    const stateStage = String(state.stage || "").trim().toLowerCase();

    if (stateStage === "scanning") {
        current.textContent = currentFile
            ? "Discovered: " + currentFile
            : (message || "Scanning Google Drive folders...");

        bar.style.width = "";
        bar.classList.add("indeterminate");

        if (count) {
            count.textContent = processed.toLocaleString()
                + (processed === 1 ? " supported ebook found" : " supported ebooks found")
                + " so far";
        }

        if (percentLabel) percentLabel.textContent = "Discovering…";

        stats.textContent = processed > 0
            ? "Building the complete PDF / EPUB file list before percentage checking begins."
            : "Scanning Google Drive folders for PDF and EPUB files...";

        stage.textContent = message
            || "Finding PDF and EPUB files in Google Drive...";
        return;
    }

    if (total > 0) {
        const percent = Math.min(
            100,
            Math.max(
                0,
                Math.round((processed / total) * 100)
            )
        );

        bar.classList.remove("indeterminate");
        bar.style.width = percent + "%";

        if (count) {
            count.textContent = processed.toLocaleString()
                + " / "
                + total.toLocaleString()
                + " ebooks checked";
        }

        if (percentLabel) percentLabel.textContent = percent + "%";

        stats.textContent =
            newFiles.toLocaleString()
            + " new"
            + " · "
            + changedFiles.toLocaleString()
            + " changed"
            + " · "
            + unchangedFiles.toLocaleString()
            + " unchanged"
            + " · "
            + duplicates.toLocaleString()
            + " duplicates";
    } else {
        bar.style.width = "";
        bar.classList.add("indeterminate");
        if (count) count.textContent = "Preparing ebook list...";
        if (percentLabel) percentLabel.textContent = "Starting…";
        stats.textContent = "Starting synchronization...";
    }

    if (stateStage === "syncing") {
        current.textContent = currentFile
            ? "Checking: " + currentFile
            : (message || "Checking ebook metadata...");

        stage.textContent = message
            || "Comparing Google Drive files with the local library database...";

    } else if (stateStage === "finalizing") {
        current.textContent = "All ebook files checked.";
        stage.textContent = "Finalizing the local database and visible book count...";

    } else if (stateStage === "complete") {
        current.textContent = "Synchronization complete.";
        if (count && total > 0) {
            count.textContent = total.toLocaleString()
                + " / "
                + total.toLocaleString()
                + " ebooks checked";
        }
        if (percentLabel) percentLabel.textContent = "100%";
        bar.classList.remove("indeterminate");
        bar.style.width = "100%";
        stage.textContent = message || "Pastor's Resources is up to date.";

    } else if (stateStage === "error") {
        current.textContent = currentFile || "Synchronization stopped.";
        stage.textContent = message || "Synchronization stopped because of an error.";

    } else {
        current.textContent = currentFile || message || "Working...";
        stage.textContent = message || "Updating Pastor's Resources...";
    }
}

async function fetchResourceSyncStatus() {
    const response = await fetch(
        "/pastor-resources/sync-books-status",
        {
            cache:"no-store"
        }
    );

    const data = await response.json();

    if (!response.ok || !data.ok) {
        throw new Error(
            data.error
            || "Unable to read synchronization status."
        );
    }

    return data.state || {};
}

async function pollResourceSyncProgress() {
    clearTimeout(resourceSyncPollTimer);

    try {
        const state = await fetchResourceSyncStatus();

        renderResourceSyncProgress(state);

        if (state.running) {
            setResourceSyncUiRunning(true);

            resourceSyncPollTimer = setTimeout(
                pollResourceSyncProgress,
                400
            );

            return;
        }

        if (state.stage === "complete") {
            const finalStats = state.stats || {};

            // Keep the completed progress card visible briefly so the
            // user can actually see that the synchronization reached 100%.
            renderResourceSyncProgress({
                ...state,
                running:false,
                total:Math.max(1, safeNumber(state.total)),
                processed:Math.max(
                    safeNumber(state.processed),
                    safeNumber(state.total)
                )
            });

            const bar = document.getElementById("resourceSyncBar");
            if (bar) {
                bar.classList.remove("indeterminate");
                bar.style.width = "100%";
            }

            showToast(
                "Sync complete: "
                + safeNumber(
                    finalStats.unique_books
                ).toLocaleString()
                + " unique books."
            );

            await Promise.all([
                loadBooks(1),
                loadContinueReading()
            ]);

            setTimeout(() => {
                setResourceSyncUiRunning(false);
            }, 1600);

        } else if (state.stage === "error") {
            setResourceSyncUiRunning(true);

            document.getElementById(
                "statusDot"
            ).className = "pr-status-dot error";

            document.getElementById(
                "statusText"
            ).textContent = "Sync failed.";

            document.getElementById(
                "syncDetail"
            ).textContent = (
                state.last_error
                || "Unknown synchronization error."
            );

            showToast(
                "Sync failed: "
                + (
                    state.last_error
                    || "Unknown synchronization error."
                )
            );

            setTimeout(() => {
                setResourceSyncUiRunning(false);
            }, 2400);

        } else {
            setResourceSyncUiRunning(false);
        }

    } catch (error) {
        // A temporary poll failure must not make a running synchronization
        // look as though it disappeared. Keep the progress card open and
        // keep retrying the persistent status endpoint.
        setResourceSyncUiRunning(true);

        document.getElementById(
            "statusDot"
        ).className = "pr-status-dot waiting";

        document.getElementById(
            "statusText"
        ).textContent = "Reconnecting to sync progress...";

        document.getElementById(
            "syncDetail"
        ).textContent =
            "The synchronization may still be running. Retrying status automatically.";

        const stage = document.getElementById("resourceSyncStage");
        if (stage) {
            stage.textContent = "Connection interrupted. Rechecking synchronization status...";
        }

        resourceSyncPollTimer = setTimeout(
            pollResourceSyncProgress,
            1800
        );
    }
}

async function syncBooks() {
    const button = document.getElementById("syncButton");

    if (!button) {
        return;
    }

    if (!confirm(
        "Sync the Pastor's Resources Google Drive folder now?"
    )) {
        return;
    }

    setResourceSyncUiRunning(true);

    renderResourceSyncProgress({
        running:true,
        stage:"starting",
        message:"Starting Pastor's Resources synchronization...",
        total:0,
        processed:0,
        new_files:0,
        changed_files:0,
        unchanged_files:0,
        duplicates:0,
        current_file:""
    });

    document.getElementById(
        "statusDot"
    ).className = "pr-status-dot waiting";

    document.getElementById(
        "statusText"
    ).textContent =
        "Synchronizing Google Drive with Pastor's Resources...";

    document.getElementById(
        "syncDetail"
    ).textContent =
        "Live progress is shown on screen.";

    try {
        const response = await fetch(
            "/pastor-resources/sync-books-live",
            {
                method:"POST"
            }
        );

        const data = await response.json();

        if (!response.ok || !data.ok) {
            throw new Error(
                data.error
                || "Unable to start synchronization."
            );
        }

        renderResourceSyncProgress(
            data.state || {}
        );

        pollResourceSyncProgress();

    } catch (error) {
        setResourceSyncUiRunning(false);

        document.getElementById(
            "statusDot"
        ).className = "pr-status-dot error";

        document.getElementById(
            "statusText"
        ).textContent = "Sync failed to start.";

        document.getElementById(
            "syncDetail"
        ).textContent = error.message;

        showToast(
            "Sync failed: "
            + error.message
        );
    }
}

async function resumeResourceSyncIfRunning() {
    if (!IS_RESOURCE_ADMIN) {
        return;
    }

    try {
        const state = await fetchResourceSyncStatus();

        if (state.running) {
            setResourceSyncUiRunning(true);
            renderResourceSyncProgress(state);
            pollResourceSyncProgress();
        }
    } catch (_error) {
        // Normal page loading should not fail only because
        // live sync status could not be checked.
    }
}

{% if is_admin %}
function openEditModalById(bookId) {
    const book = currentBookMap.get(Number(bookId));
    if (!book) {
        showToast("Book details are no longer on this page. Please refresh.");
        return;
    }

    document.getElementById("editBookId").value = book.id;
    document.getElementById("editTitle").value = book.title || "";
    document.getElementById("editAuthor").value = book.author || "";
    document.getElementById("editCategory").value = book.category || "";
    document.getElementById("editModalBackdrop").classList.add("show");
}

function closeEditModal() {
    document.getElementById("editModalBackdrop").classList.remove("show");
}

function modalBackdropClick(event) {
    if (event.target.id === "editModalBackdrop") closeEditModal();
}

async function saveBookEdit() {
    const bookId = document.getElementById("editBookId").value;
    const payload = {
        title:document.getElementById("editTitle").value.trim(),
        author:document.getElementById("editAuthor").value.trim(),
        category:document.getElementById("editCategory").value.trim()
    };

    if (!payload.title) {
        showToast("Title is required.");
        return;
    }

    try {
        const response = await fetch("/pastor-resources/admin/edit/" + bookId, {
            method:"POST",
            headers:{"Content-Type":"application/json"},
            body:JSON.stringify(payload)
        });
        const data = await response.json();
        if (!data.ok) throw new Error(data.error || "Unable to edit book.");
        closeEditModal();
        showToast("Book details updated. Your manual changes will survive future syncs.");
        await loadBooks(currentPage);
    } catch (error) {
        showToast(error.message);
    }
}

async function removeBook(bookId) {
    const book = currentBookMap.get(Number(bookId));
    const title = book?.title || "this ebook";

    if (!confirm('Remove "' + title + '" from Pastor\'s Resources?\n\nThe Google Drive file will NOT be deleted. You can restore it later from Hidden Books.')) return;

    try {
        const response = await fetch("/pastor-resources/admin/hide/" + Number(bookId), {method:"POST"});
        const data = await response.json();
        if (!response.ok || !data.ok) throw new Error(data.error || "Unable to remove book.");

        const card = document.getElementById("book-card-" + Number(bookId));
        if (card) card.remove();
        currentBookMap.delete(Number(bookId));

        showToast("Book hidden from the library. The Drive file is safe and can be restored.");
        await Promise.all([loadBooks(currentPage), loadContinueReading()]);
    } catch (error) {
        showToast(error.message);
    }
}

document.getElementById("bookGrid").addEventListener("click", event => {
    const button = event.target.closest("[data-admin-action]");
    if (!button) return;

    const bookId = Number(button.dataset.bookId || 0);
    if (!bookId) return;

    if (button.dataset.adminAction === "edit") {
        openEditModalById(bookId);
    } else if (button.dataset.adminAction === "remove") {
        removeBook(bookId);
    }
});
{% endif %}

initializeLibraryPage();
resumeResourceSyncIfRunning();

</script>

{% endblock %}
"""


# =========================================================
# MY LIBRARY TEMPLATE
# =========================================================

PASTOR_MY_LIBRARY_HTML = r"""
{% extends "base.html" %}

{% block title %}
My Library - Pastor's Resources
{% endblock %}

{% block content %}

<style>
@import url('https://fonts.googleapis.com/css2?family=Lora:wght@500;600;700&family=Nunito+Sans:wght@400;600;700;800;900&display=swap');
.app-main { max-width:1500px; padding:0; }
.ml-page { padding:14px 12px 50px; color:#14213b; font-family:"Nunito Sans",Arial,sans-serif; }
.ml-hero { padding:18px; border-radius:20px; background:linear-gradient(135deg,#fff8fb,#f6f7ff,#eef9ff); border:1px solid rgba(15,23,42,.07); box-shadow:0 10px 30px rgba(15,23,42,.06); }
.ml-title { margin:0; font:700 34px/1.05 "Lora",Georgia,serif; color:#14213b; }
.ml-sub { margin:8px 0 0; color:#64748b; font-size:13px; line-height:1.55; }
.ml-back { display:inline-flex; margin-top:12px; text-decoration:none; padding:9px 11px; border-radius:11px; background:#fff; color:#52627d; font-size:11px; font-weight:850; box-shadow:0 4px 14px rgba(15,23,42,.06); }
.ml-tabs { display:flex; gap:7px; overflow-x:auto; margin:14px 0; padding-bottom:2px; }
.ml-tab { flex:0 0 auto; border:0; border-radius:999px; padding:9px 13px; background:#eef2f8; color:#59677f; font:850 11px Arial,sans-serif; cursor:pointer; }
.ml-tab.active { color:#fff; background:linear-gradient(135deg,#b47db9,#6f96de); }
.ml-panel { display:none; }
.ml-panel.active { display:block; }
.ml-section-title { margin:18px 0 10px; font:700 24px/1.1 "Lora",Georgia,serif; }
.ml-grid { display:grid; grid-template-columns:1fr; gap:10px; }
.ml-card { display:grid; grid-template-columns:75px minmax(0,1fr); gap:11px; padding:11px; border-radius:16px; background:#fff; border:1px solid rgba(15,23,42,.07); box-shadow:0 6px 18px rgba(15,23,42,.05); }
.ml-cover { width:75px; aspect-ratio:2/3; border-radius:10px; object-fit:cover; background:#e9eef7; }
.ml-card h3 { margin:0; color:#17233c; font:700 14px/1.25 "Lora",Georgia,serif; }
.ml-meta { margin-top:5px; color:#7c899f; font-size:11px; line-height:1.4; }
.ml-actions { display:flex; flex-wrap:wrap; gap:6px; margin-top:9px; }
.ml-btn { border:0; border-radius:9px; padding:7px 9px; text-decoration:none; background:#eef2f8; color:#56677f; font:800 10px Arial,sans-serif; cursor:pointer; }
.ml-btn.primary { color:white; background:linear-gradient(135deg,#cf96c2,#809ce0); }
.ml-btn.danger { background:#fff0f2; color:#b4233e; }
.ml-btn.restore { background:#ecfdf3; color:#167546; }
.ml-progress { height:6px; border-radius:999px; background:#e8edf5; overflow:hidden; margin-top:8px; }
.ml-progress > div { height:100%; background:linear-gradient(90deg,#ca8fc0,#7299dd); }
.ml-stat-grid { display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:9px; }
.ml-stat { padding:13px; border-radius:15px; background:#fff; border:1px solid rgba(15,23,42,.07); }
.ml-stat-number { color:#17233c; font:800 24px/1 Georgia,"Times New Roman",serif; }
.ml-stat-label { margin-top:5px; color:#7c899f; font-size:10px; font-weight:800; }
.ml-list { display:grid; gap:9px; }
.ml-note { padding:12px; border-radius:15px; background:#fff; border:1px solid rgba(15,23,42,.07); }
.ml-note-top { display:flex; justify-content:space-between; gap:9px; align-items:start; }
.ml-note-book { color:#39475f; font-size:11px; font-weight:850; }
.ml-note-type { flex:0 0 auto; padding:4px 7px; border-radius:999px; background:#f4eff8; color:#865c8c; font-size:9px; font-weight:850; }
.ml-quote { margin-top:8px; padding-left:10px; border-left:3px solid #d6a2c7; color:#39475f; font-size:12px; line-height:1.55; }
.ml-note-text { margin-top:8px; color:#64748b; font-size:11px; line-height:1.5; }
.ml-tags { margin-top:7px; color:#8b5f92; font-size:10px; font-weight:800; }
.ml-empty { padding:30px 15px; border-radius:16px; background:#fff; text-align:center; color:#64748b; border:1px solid rgba(15,23,42,.07); }
.ml-toast { position:fixed; left:12px; right:12px; bottom:14px; z-index:10000; display:none; padding:13px 15px; border-radius:13px; background:#111827; color:#fff; font-size:12px; box-shadow:0 14px 32px rgba(0,0,0,.2); }

@media(min-width:650px) {
  .ml-page { padding:20px 18px 55px; }
  .ml-grid { grid-template-columns:repeat(2,minmax(0,1fr)); }
  .ml-stat-grid { grid-template-columns:repeat(4,minmax(0,1fr)); }
}
@media(min-width:950px) {
  .ml-page { padding:26px 24px 65px; }
  .ml-title { font-size:44px; }
  .ml-grid { grid-template-columns:repeat(3,minmax(0,1fr)); gap:14px; }
  .ml-stat-grid { grid-template-columns:repeat(7,minmax(0,1fr)); }
  .ml-toast { left:auto; right:20px; width:360px; }
}
</style>

<div class="ml-page">
    <section class="ml-hero">
        <h1 class="ml-title">My Library</h1>
        <p class="ml-sub">Your favorites, bookmarks, highlights, sermon notes, completed books and real active reading time are private to your account.</p>
        <a class="ml-back" href="{{ url_for('pastor_resources') }}">← Back to Pastor's Resources</a>
    </section>

    <div class="ml-tabs">
        <button class="ml-tab" data-tab="favorites" onclick="openTab('favorites',this)">❤️ Favorites</button>
        <button class="ml-tab" data-tab="bookmarks" onclick="openTab('bookmarks',this)">🔖 Bookmarks</button>
        <button class="ml-tab" data-tab="highlights" onclick="openTab('highlights',this)">🖍 Highlights & Notes</button>
        <button class="ml-tab" data-tab="sermon" onclick="openTab('sermon',this)">📝 Sermon Notes</button>
        <button class="ml-tab" data-tab="progress" onclick="openTab('progress',this)">📊 Reading Progress</button>
        {% if is_admin %}<button class="ml-tab" data-tab="hidden" onclick="openTab('hidden',this)">🗃 Hidden Books</button>{% endif %}
    </div>

    <section class="ml-panel" id="panel-favorites">
        <h2 class="ml-section-title">Favorite Books</h2>
        <div class="ml-grid" id="favoritesGrid"></div>
    </section>

    <section class="ml-panel" id="panel-bookmarks">
        <h2 class="ml-section-title">Bookmarks</h2>
        <div class="ml-list" id="bookmarksList"></div>
    </section>

    <section class="ml-panel" id="panel-highlights">
        <h2 class="ml-section-title">Highlights, Underlines & Notes</h2>
        <div class="ml-list" id="annotationsList"></div>
    </section>

    <section class="ml-panel" id="panel-sermon">
        <h2 class="ml-section-title">Sermon Notes</h2>
        <div class="ml-list" id="sermonNotesList"></div>
    </section>

    <section class="ml-panel" id="panel-progress">
        <h2 class="ml-section-title">Reading Progress</h2>
        <div class="ml-stat-grid" id="statsGrid"></div>

        <h2 class="ml-section-title">Currently Reading</h2>
        <div class="ml-grid" id="readingGrid"></div>

        <h2 class="ml-section-title">Completed Books</h2>
        <div class="ml-grid" id="completedGrid"></div>
    </section>

    {% if is_admin %}
    <section class="ml-panel" id="panel-hidden">
        <h2 class="ml-section-title">Hidden Books</h2>
        <div class="ml-list" id="hiddenList"></div>
    </section>
    {% endif %}
</div>

<div class="ml-toast" id="mlToast"></div>

<script>
const INITIAL_TAB = {{ initial_tab|tojson }};
const IS_ADMIN = {{ 'true' if is_admin else 'false' }};
let libraryData = null;

function escapeHtml(value) {
    const div = document.createElement("div");
    div.textContent = value || "";
    return div.innerHTML;
}

function showToast(message) {
    const toast = document.getElementById("mlToast");
    toast.textContent = message;
    toast.style.display = "block";
    clearTimeout(toast.hideTimer);
    toast.hideTimer = setTimeout(() => toast.style.display="none", 4300);
}

const READER_RETURN_URL_KEY = "pastorReaderReturnUrlV1";

function rememberReaderOrigin() {
    try {
        sessionStorage.setItem(READER_RETURN_URL_KEY, window.location.pathname + window.location.search);
    } catch (error) {}
}

document.addEventListener("click", event => {
    const link = event.target.closest('a[href*="/pastor-resources/read/"]');
    if (link) rememberReaderOrigin();
});

function formatDuration(seconds) {
    seconds = Math.max(0, Number(seconds || 0));
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    if (hours > 0) return hours + "h " + minutes + "m";
    return minutes + "m";
}

function openTab(name, button) {
    if (name === "hidden" && !IS_ADMIN) name = "favorites";
    document.querySelectorAll(".ml-panel").forEach(el => el.classList.remove("active"));
    document.querySelectorAll(".ml-tab").forEach(el => el.classList.remove("active"));
    const panel = document.getElementById("panel-" + name);
    if (panel) panel.classList.add("active");
    const target = button || document.querySelector('.ml-tab[data-tab="' + name + '"]');
    if (target) target.classList.add("active");
    const url = new URL(window.location.href);
    url.searchParams.set("tab", name);
    history.replaceState({},"",url);
}

function bookCard(book, extra="") {
    const progress = Math.min(100, Math.max(0, Number(book.progress_percent || 0)));
    return `
        <article class="ml-card">
            <img class="ml-cover" src="${escapeHtml(book.thumbnail_url)}" loading="lazy" alt="${escapeHtml(book.title)}">
            <div>
                <h3>${escapeHtml(book.title)}</h3>
                <div class="ml-meta">${escapeHtml(book.author || "Unknown Author")}</div>
                ${progress > 0 ? '<div class="ml-progress"><div style="width:' + progress + '%"></div></div><div class="ml-meta">' + Math.round(progress) + '% complete</div>' : ''}
                ${extra}
                <div class="ml-actions"><a class="ml-btn primary" href="${escapeHtml(book.read_url)}">Read</a></div>
            </div>
        </article>
    `;
}

function renderData(data) {
    const favorites = data.favorites || [];
    document.getElementById("favoritesGrid").innerHTML = favorites.length
        ? favorites.map(book => bookCard(book)).join("")
        : '<div class="ml-empty">No favorite books yet. Tap ♡ on any book to add it here.</div>';

    const bookmarks = data.bookmarks || [];
    document.getElementById("bookmarksList").innerHTML = bookmarks.length
        ? bookmarks.map(item => `
            <article class="ml-note">
                <div class="ml-note-top">
                    <div class="ml-note-book">${escapeHtml(item.book_title)} • ${escapeHtml(item.book_author || "")}</div>
                    <div class="ml-note-type">Bookmark</div>
                </div>
                <div class="ml-note-text">${escapeHtml(item.label || (item.format === "PDF" ? "Page " + (item.page || "") : "Saved location"))}</div>
                <div class="ml-actions">
                    <a class="ml-btn primary" href="${escapeHtml(item.jump_url)}">Open</a>
                    <button class="ml-btn danger" onclick="deleteBookmark(${item.id})">Delete</button>
                </div>
            </article>
        `).join("")
        : '<div class="ml-empty">No bookmarks yet.</div>';

    const annotations = data.annotations || [];
    const normalAnnotations = annotations.filter(item => !item.is_sermon_note);
    const sermonNotes = annotations.filter(item => Boolean(item.is_sermon_note));

    function annotationCard(item, sermonMode=false) {
        return `
            <article class="ml-note">
                <div class="ml-note-top">
                    <div class="ml-note-book">${escapeHtml(item.book_title)} • ${escapeHtml(item.book_author || "")}</div>
                    <div class="ml-note-type">${sermonMode ? 'Sermon Note' : escapeHtml(item.annotation_type || 'Highlight')}</div>
                </div>
                ${item.selected_text ? '<div class="ml-quote">' + escapeHtml(item.selected_text) + '</div>' : ''}
                ${item.note ? '<div class="ml-note-text"><strong>Note:</strong> ' + escapeHtml(item.note) + '</div>' : ''}
                ${item.tags ? '<div class="ml-tags"># ' + escapeHtml(item.tags) + '</div>' : ''}
                <div class="ml-actions">
                    <a class="ml-btn primary" href="${escapeHtml(item.jump_url)}">Open Passage</a>
                    <button class="ml-btn danger" onclick="deleteAnnotation(${item.id})">Delete</button>
                </div>
            </article>
        `;
    }

    document.getElementById("annotationsList").innerHTML = normalAnnotations.length
        ? normalAnnotations.map(item => annotationCard(item, false)).join("")
        : '<div class="ml-empty">No highlights or notes yet. Select text inside the reader to begin.</div>';

    document.getElementById("sermonNotesList").innerHTML = sermonNotes.length
        ? sermonNotes.map(item => annotationCard(item, true)).join("")
        : '<div class="ml-empty">No sermon notes yet. Use “Save for Sermon” while reading to collect passages here.</div>';

    const summary = data.summary || {};
    const stats = [
        [summary.currently_reading || 0, "Currently Reading"],
        [summary.completed || 0, "Completed"],
        [summary.favorites || 0, "Favorites"],
        [formatDuration(summary.total_active_seconds), "Total Reading"],
        [formatDuration(summary.weekly_seconds), "Last 7 Days"],
        [formatDuration(summary.monthly_seconds), "Last 30 Days"],
        [formatDuration(summary.average_session_seconds), "Avg. Session"]
    ];
    document.getElementById("statsGrid").innerHTML = stats.map(item => '<div class="ml-stat"><div class="ml-stat-number">' + escapeHtml(String(item[0])) + '</div><div class="ml-stat-label">' + escapeHtml(item[1]) + '</div></div>').join("");

    const reading = data.currently_reading || [];
    document.getElementById("readingGrid").innerHTML = reading.length
        ? reading.map(book => bookCard(book, '<div class="ml-meta">Active reading: ' + formatDuration(book.total_active_seconds) + '</div>')).join("")
        : '<div class="ml-empty">No books currently in progress.</div>';

    const completed = data.completed || [];
    document.getElementById("completedGrid").innerHTML = completed.length
        ? completed.map(book => bookCard(book, '<div class="ml-meta">Finished: ' + escapeHtml(book.completed_display || '') + '</div>')).join("")
        : '<div class="ml-empty">No completed books yet.</div>';

    if (IS_ADMIN) {
        const hidden = data.hidden_books || [];
        document.getElementById("hiddenList").innerHTML = hidden.length
            ? hidden.map(book => `
                <article class="ml-note">
                    <div class="ml-note-top">
                        <div class="ml-note-book">${escapeHtml(book.title)}</div>
                        <div class="ml-note-type">Hidden</div>
                    </div>
                    <div class="ml-note-text">${escapeHtml(book.author || "Unknown Author")} • ${escapeHtml(book.category || "General")}</div>
                    <div class="ml-actions"><button class="ml-btn restore" onclick="restoreBook(${book.id})">Restore to Library</button></div>
                </article>
            `).join("")
            : '<div class="ml-empty">No hidden books.</div>';
    }
}

async function loadMyLibrary() {
    try {
        const response = await fetch("/pastor-resources/api/my-library");
        const data = await response.json();
        if (!data.ok) throw new Error(data.error || "Unable to load My Library.");
        libraryData = data;
        renderData(data);
    } catch (error) {
        showToast(error.message);
    }
}

async function deleteBookmark(id) {
    if (!confirm("Delete this bookmark?")) return;
    const response = await fetch("/pastor-resources/api/bookmarks/" + id, {method:"DELETE"});
    const data = await response.json();
    if (!data.ok) return showToast(data.error || "Unable to delete bookmark.");
    showToast("Bookmark deleted.");
    loadMyLibrary();
}

async function deleteAnnotation(id) {
    if (!confirm("Delete this highlight/note?")) return;
    const response = await fetch("/pastor-resources/api/annotations/" + id, {method:"DELETE"});
    const data = await response.json();
    if (!data.ok) return showToast(data.error || "Unable to delete annotation.");
    showToast("Annotation deleted.");
    loadMyLibrary();
}

{% if is_admin %}
async function restoreBook(id) {
    const response = await fetch("/pastor-resources/admin/restore/" + id, {method:"POST"});
    const data = await response.json();
    if (!data.ok) return showToast(data.error || "Unable to restore book.");
    showToast("Book restored to Pastor's Resources.");
    loadMyLibrary();
}
{% endif %}

openTab(INITIAL_TAB || "favorites");
loadMyLibrary();
</script>

{% endblock %}
"""


# =========================================================
# BUILT-IN READER TEMPLATE
# =========================================================

PASTOR_READER_HTML = r"""
{% extends "base.html" %}

{% block title %}
{{ book.title }} - Reader
{% endblock %}

{% block content %}

<script src="https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/jszip@3.10.1/dist/jszip.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/epubjs@0.3.93/dist/epub.min.js"></script>

<style>
@import url('https://fonts.googleapis.com/css2?family=Lora:wght@500;600;700&family=Nunito+Sans:wght@400;600;700;800;900&display=swap');

html, body { overflow:hidden !important; }
.app-main { max-width:none; padding:0; }

.reader-root {
    --reader-bg:#f6f1e9;
    --reader-panel:#fffdfa;
    --reader-text:#1f1c18;
    --reader-muted:#80786e;
    --reader-line:rgba(56,47,39,.12);
    position:fixed;
    top:var(--reader-viewport-top,0px);
    left:0;
    right:0;
    bottom:auto;
    z-index:20000;
    width:100%;
    height:var(--reader-viewport-height,100dvh);
    min-height:0;
    display:flex;
    flex-direction:column;
    overflow:hidden;
    box-sizing:border-box;
    padding-top:env(safe-area-inset-top,0px);
    padding-bottom:0;
    background:var(--reader-bg);
    color:var(--reader-text);
    font-family:"Nunito Sans",Arial,sans-serif;
    overscroll-behavior:none;
}

.reader-root.theme-sepia {
    --reader-bg:#eee3d1;
    --reader-panel:#fbf3e4;
    --reader-text:#4b3b29;
    --reader-muted:#7b6a56;
    --reader-line:rgba(75,59,41,.14);
}

.reader-root.theme-dark {
    --reader-bg:#171b24;
    --reader-panel:#242a36;
    --reader-text:#ecf0f7;
    --reader-muted:#a9b2c1;
    --reader-line:rgba(255,255,255,.10);
}

.reader-toolbar {
    position:relative;
    z-index:160;
    flex:0 0 auto;
    background:rgba(255,253,250,.985);
    border-bottom:1px solid var(--reader-line);
    box-shadow:none;
    backdrop-filter:blur(14px);
    -webkit-backdrop-filter:blur(14px);
}

.theme-sepia .reader-toolbar { background:rgba(251,243,228,.985); }
.theme-dark .reader-toolbar { background:rgba(36,42,54,.985); }

.reader-topline {
    min-height:48px;
    display:grid;
    grid-template-columns:minmax(72px,auto) minmax(0,1fr) auto auto;
    align-items:center;
    gap:4px;
    padding:4px 7px;
    min-width:0;
}

.reader-back-btn,
.reader-icon-btn {
    flex:0 0 auto;
    min-height:36px;
    border:0;
    border-radius:9px;
    background:transparent;
    color:var(--reader-text);
    cursor:pointer;
    -webkit-tap-highlight-color:transparent;
}

.reader-back-btn {
    padding:5px 5px 5px 0;
    white-space:nowrap;
    text-align:left;
    font:500 12px Georgia,"Times New Roman",serif;
}

.reader-back-btn .reader-chevron {
    display:inline-block;
    margin-right:2px;
    font:400 20px/1 Arial,sans-serif;
    vertical-align:-2px;
}

.reader-icon-btn {
    width:34px;
    padding:0;
    display:inline-flex;
    align-items:center;
    justify-content:center;
    font:400 20px/1 Arial,sans-serif;
}

.reader-icon-btn:hover,
.reader-back-btn:hover {
    background:rgba(78,69,60,.06);
}

.theme-dark .reader-back-btn,
.theme-dark .reader-icon-btn,
.theme-sepia .reader-back-btn,
.theme-sepia .reader-icon-btn {
    background:transparent;
    color:var(--reader-text);
}

.reader-book-info {
    min-width:0;
    text-align:center;
    padding:0 4px;
}

.reader-book-title {
    overflow:hidden;
    text-overflow:ellipsis;
    white-space:nowrap;
    font:600 12px Georgia,"Times New Roman",serif;
    color:var(--reader-text);
}

.reader-book-author {
    display:none;
}

.reader-search-panel {
    position:absolute;
    z-index:210;
    top:0;
    left:0;
    right:0;
    display:none;
    grid-template-columns:minmax(0,1fr) auto;
    align-items:center;
    gap:8px;
    padding:8px 10px 7px;
    background:rgba(255,255,255,.985);
    border-bottom:1px solid rgba(60,50,40,.08);
    box-shadow:0 7px 22px rgba(40,32,24,.08);
    backdrop-filter:blur(18px);
    -webkit-backdrop-filter:blur(18px);
}

.reader-search-panel.open { display:grid; }

.reader-search-box {
    min-width:0;
    display:flex;
    align-items:center;
    gap:7px;
    min-height:36px;
    padding:0 8px 0 10px;
    border:0;
    border-radius:11px;
    background:#f1f1f4;
    box-shadow:inset 0 0 0 1px rgba(30,30,30,.025);
}

.reader-search-symbol {
    flex:0 0 auto;
    width:15px;
    height:15px;
    color:#7b7b80;
    display:inline-flex;
    align-items:center;
    justify-content:center;
}

.reader-search-symbol svg,
.reader-top-svg {
    width:18px;
    height:18px;
    fill:none;
    stroke:currentColor;
    stroke-width:1.8;
    stroke-linecap:round;
    stroke-linejoin:round;
}

.reader-search {
    width:100%;
    min-width:0;
    border:0;
    outline:0;
    padding:0;
    background:transparent;
    color:#1f1f22;
    font:400 14px -apple-system,BlinkMacSystemFont,"Segoe UI",Arial,sans-serif;
    -webkit-appearance:none;
    appearance:none;
}

.reader-search::-webkit-search-cancel-button {
    -webkit-appearance:none;
    appearance:none;
}

.reader-search::placeholder { color:#8d8d93; }

.reader-search-clear {
    flex:0 0 auto;
    width:18px;
    height:18px;
    display:none;
    align-items:center;
    justify-content:center;
    border:0;
    border-radius:50%;
    padding:0 0 1px;
    background:#9a9aa0;
    color:#fff;
    font:700 12px/1 Arial,sans-serif;
    cursor:pointer;
    -webkit-tap-highlight-color:transparent;
}

.reader-search-clear.show { display:inline-flex; }

.reader-search-action {
    flex:0 0 auto;
    min-height:36px;
    border:0;
    border-radius:8px;
    padding:0 2px;
    background:transparent;
    color:#007aff;
    font:500 14px -apple-system,BlinkMacSystemFont,"Segoe UI",Arial,sans-serif;
    cursor:pointer;
    -webkit-tap-highlight-color:transparent;
}

.reader-search-action.primary { display:none; }

.reader-search-results {
    grid-column:1 / -1;
    display:none;
    align-items:center;
    justify-content:space-between;
    gap:10px;
    min-height:29px;
    padding:3px 2px 0;
    color:#77777d;
    font:500 11px -apple-system,BlinkMacSystemFont,"Segoe UI",Arial,sans-serif;
}

.reader-search-result-nav {
    display:flex;
    align-items:center;
    gap:4px;
}

.reader-search-step {
    width:28px;
    height:26px;
    border:0;
    border-radius:8px;
    background:transparent;
    color:#007aff;
    font:400 20px/1 Arial,sans-serif;
    cursor:pointer;
}

.theme-sepia .reader-search-panel { background:rgba(251,244,229,.985); }
.theme-sepia .reader-search-box { background:#eee7da; }
.theme-sepia .reader-search { color:#4b3b29; }

.theme-dark .reader-search-panel {
    background:rgba(36,42,54,.985);
    border-bottom-color:rgba(255,255,255,.08);
}
.theme-dark .reader-search-box { background:#343b49; }
.theme-dark .reader-search,
.theme-dark .reader-search-symbol { color:#edf1f7; }
.theme-dark .reader-search::placeholder { color:#a5adba; }
.theme-dark .reader-search-action,
.theme-dark .reader-search-step { color:#69a7ff; }
.theme-dark .reader-search-results { color:#b2bac6; }

.reader-progress {
    height:3px;
    background:rgba(148,163,184,.22);
    overflow:hidden;
}

.reader-progress > div {
    height:100%;
    background:linear-gradient(90deg,#cc8fc1,#6f97dd);
}

.reader-main {
    position:relative;
    flex:1 1 auto;
    min-height:0;
    display:flex;
    overflow:hidden;
}

.reader-canvas-area {
    flex:1;
    min-width:0;
    min-height:0;
    overflow:auto;
    -webkit-overflow-scrolling:touch;
    padding:10px 8px 12px;
    display:flex;
    justify-content:center;
    align-items:flex-start;
    touch-action:pan-y pinch-zoom;
    overscroll-behavior-x:contain;
    background:var(--reader-bg);
}

.reader-message {
    margin:30px auto;
    padding:18px;
    border-radius:14px;
    background:var(--reader-panel);
    color:var(--reader-text);
    box-shadow:0 8px 24px rgba(15,23,42,.08);
}

/* PDF */
#pdfStage {
    position:relative;
    flex:0 0 auto;
    background:#fff;
    box-shadow:0 4px 18px rgba(54,43,32,.10);
}

#pdfCanvas { display:block; }

.textLayer {
    position:absolute;
    inset:0;
    overflow:hidden;
    opacity:1;
    line-height:1;
    text-size-adjust:none;
    -webkit-text-size-adjust:none;
    transform-origin:0 0;
    z-index:2;
    -webkit-user-select:text;
    user-select:text;
}

.textLayer span,
.textLayer br {
    color:transparent;
    position:absolute;
    white-space:pre;
    cursor:text;
    transform-origin:0% 0%;
    -webkit-user-select:text;
    user-select:text;
}

.textLayer ::selection { background:rgba(70,115,220,.32); }

.pdf-link-layer {
    position:absolute;
    inset:0;
    z-index:3;
    pointer-events:none;
}

.pdf-link-hit {
    position:absolute;
    pointer-events:auto;
    cursor:pointer;
    border:0;
    padding:0;
    margin:0;
    background:rgba(52,105,190,.035);
    border-bottom:1px solid rgba(52,105,190,.28);
}

.pdf-link-hit:hover { background:rgba(52,105,190,.12); }

.pdf-annotation-layer {
    position:absolute;
    inset:0;
    z-index:1;
    pointer-events:none;
}

.pdf-annotation {
    position:absolute;
    border-radius:2px;
    pointer-events:auto;
    cursor:pointer;
}

.theme-dark #pdfStage { filter:invert(.88) hue-rotate(180deg); }
.theme-sepia #pdfStage { filter:sepia(.25) saturate(.9); }

/* EPUB */
#epubViewer {
    width:min(100%,980px);
    height:100%;
    min-height:0;
    background:var(--reader-panel);
    border-radius:0;
    overflow:hidden;
    box-shadow:0 4px 18px rgba(54,43,32,.08);
}

/* Permanent page navigation for desktop and mobile. */
.reader-bottom-bar {
    position:relative;
    z-index:240;
    flex:0 0 auto;
    min-height:48px;
    display:flex;
    align-items:center;
    justify-content:center;
    padding:5px 10px calc(5px + env(safe-area-inset-bottom,0px));
    border-top:1px solid var(--reader-line);
    background:var(--reader-panel);
    color:#777068;
    box-sizing:border-box;
}

.reader-page-nav {
    display:flex;
    align-items:center;
    justify-content:center;
    gap:9px;
    min-width:0;
}

.reader-page-arrow {
    width:38px;
    height:34px;
    border:0;
    border-radius:10px;
    padding:0;
    background:transparent;
    color:#665e56;
    font:400 23px/1 Arial,sans-serif;
    cursor:pointer;
    -webkit-tap-highlight-color:transparent;
}

.reader-page-arrow:hover {
    background:rgba(80,70,60,.07);
    color:#302a24;
}

.reader-page-input {
    width:76px;
    height:34px;
    box-sizing:border-box;
    border:1px solid rgba(90,78,66,.25);
    border-radius:9px;
    background:#fffdfa;
    color:#2f2924;
    text-align:center;
    font:600 14px Georgia,"Times New Roman",serif;
    outline:none;
    -moz-appearance:textfield;
}

.reader-page-input::-webkit-outer-spin-button,
.reader-page-input::-webkit-inner-spin-button {
    -webkit-appearance:none;
    margin:0;
}

.reader-page-input:focus {
    border-color:#9b7b56;
    box-shadow:0 0 0 2px rgba(155,123,86,.12);
}

.reader-page-input:disabled {
    opacity:.55;
    cursor:wait;
}

.reader-page-total {
    min-width:48px;
    color:#4e4740;
    font:600 11px Georgia,"Times New Roman",serif;
    white-space:nowrap;
}

.reader-page-kind {
    display:none;
    color:var(--reader-muted);
    font-size:9px;
}

.theme-dark .reader-bottom-bar {
    background:var(--reader-panel);
    color:#b7b0a7;
}

.theme-dark .reader-page-arrow,
.theme-dark .reader-page-total {
    color:#e7e1d8;
}

.theme-dark .reader-page-input {
    background:#303747;
    color:#f4f0e9;
    border-color:#465065;
}

/* Reading notes drawer */
.reader-side {
    position:fixed;
    z-index:20600;
    top:0;
    right:0;
    bottom:0;
    width:min(390px,92vw);
    padding-top:env(safe-area-inset-top,0px);
    padding-bottom:env(safe-area-inset-bottom,0px);
    transform:translateX(105%);
    transition:transform .22s ease;
    background:var(--reader-panel);
    color:var(--reader-text);
    box-shadow:-12px 0 36px rgba(15,23,42,.18);
    display:flex;
    flex-direction:column;
}

.reader-side.open { transform:translateX(0); }

.reader-side-head {
    display:flex;
    align-items:center;
    justify-content:space-between;
    gap:10px;
    padding:13px;
    border-bottom:1px solid rgba(100,116,139,.18);
}

.reader-side-head h3 {
    margin:0;
    font:700 19px "Lora",Georgia,serif;
}

.reader-side-tabs {
    display:flex;
    gap:6px;
    padding:10px;
    border-bottom:1px solid rgba(100,116,139,.14);
}

.reader-side-tab {
    flex:1;
    border:0;
    border-radius:9px;
    padding:8px;
    background:#edf2f8;
    color:#53637b;
    font-size:10px;
    font-weight:850;
    cursor:pointer;
}

.reader-side-tab.active {
    color:white;
    background:linear-gradient(135deg,#c98cc0,#789be0);
}

.reader-side-body {
    flex:1;
    overflow:auto;
    -webkit-overflow-scrolling:touch;
    padding:10px;
}

.reader-side-panel { display:none; }
.reader-side-panel.active { display:block; }

.reader-item {
    padding:10px;
    border-radius:12px;
    margin-bottom:8px;
    background:rgba(148,163,184,.10);
    font-size:11px;
    line-height:1.45;
}

.reader-item-quote {
    margin-top:6px;
    padding-left:8px;
    border-left:3px solid #d7a0c8;
}

.reader-item-actions {
    display:flex;
    gap:5px;
    margin-top:8px;
    flex-wrap:wrap;
}

.reader-item-actions button {
    border:0;
    border-radius:8px;
    padding:6px 7px;
    font-size:9px;
    font-weight:850;
    cursor:pointer;
}

.reader-side-backdrop,
.reader-tools-backdrop {
    position:fixed;
    inset:0;
    display:none;
    background:rgba(15,23,42,.38);
}

.reader-side-backdrop { z-index:20500; }
.reader-tools-backdrop { z-index:20300; }

.reader-side-backdrop.show,
.reader-tools-backdrop.show { display:block; }

/* Bottom tools sheet - iOS-style panel from the approved mockup. */
.reader-tools-sheet {
    position:fixed;
    z-index:20400;
    left:0;
    right:0;
    bottom:0;
    max-height:min(78vh,650px);
    max-height:min(78dvh,650px);
    overflow:auto;
    -webkit-overflow-scrolling:touch;
    transform:translateY(105%);
    transition:transform .22s ease;
    padding:7px 14px calc(16px + env(safe-area-inset-bottom,0px));
    border-radius:24px 24px 0 0;
    background:#fffdfa;
    color:#1f1c18;
    box-shadow:0 -18px 48px rgba(54,43,32,.18);
}

.theme-sepia .reader-tools-sheet { background:#fbf3e4; color:#4b3b29; }
.theme-dark .reader-tools-sheet { background:#242a36; color:#ecf0f7; }

.reader-tools-sheet.open { transform:translateY(0); }

.reader-tools-handle {
    width:42px;
    height:4px;
    margin:0 auto 8px;
    border-radius:999px;
    background:#8f8a84;
}

.reader-tools-head {
    display:flex;
    align-items:center;
    justify-content:space-between;
    gap:10px;
    margin:0 0 4px;
}

.reader-tools-head h3 {
    margin:0;
    font:600 15px Georgia,"Times New Roman",serif;
}

.reader-tools-grid {
    display:grid;
    grid-template-columns:repeat(3,minmax(0,1fr));
    gap:10px 5px;
    padding:4px 0 8px;
}

.reader-tool-btn {
    min-height:72px;
    border:0;
    border-radius:12px;
    padding:5px 2px;
    background:transparent;
    color:inherit;
    display:flex;
    flex-direction:column;
    align-items:center;
    justify-content:center;
    gap:5px;
    text-align:center;
    font:600 10px "Nunito Sans",Arial,sans-serif;
    cursor:pointer;
    text-decoration:none;
    -webkit-tap-highlight-color:transparent;
}

.reader-tool-icon-circle {
    width:48px;
    height:48px;
    border-radius:50%;
    display:flex;
    align-items:center;
    justify-content:center;
    background:#f6f1e9;
    color:#211e1a;
    font:500 17px/1 Arial,sans-serif;
}

.reader-tool-icon-circle svg {
    width:22px;
    height:22px;
    stroke:currentColor;
    fill:none;
    stroke-width:1.7;
    stroke-linecap:round;
    stroke-linejoin:round;
}

.reader-tool-btn.highlight-tool .reader-tool-icon-circle {
    background:#fff0c8;
}

.theme-sepia .reader-tool-icon-circle { background:#f2e6d1; color:#4b3b29; }
.theme-dark .reader-tool-icon-circle { background:#303747; color:#edf1f7; }
.theme-dark .reader-tool-btn.highlight-tool .reader-tool-icon-circle { background:#5c5133; }

.reader-tool-btn.favorite.on {
    color:#d94677;
}

.reader-tool-detail {
    display:none;
    margin-top:7px;
    padding:11px 0 2px;
    border-top:1px solid rgba(100,116,139,.16);
}

.reader-tool-detail.open { display:block; }

.reader-more-grid {
    display:grid;
    grid-template-columns:repeat(2,minmax(0,1fr));
    gap:6px;
    margin-top:6px;
    padding-top:8px;
    border-top:1px solid rgba(100,116,139,.14);
}

.reader-tool-section {
    margin-top:12px;
    padding-top:11px;
    border-top:1px solid rgba(100,116,139,.16);
}

.reader-tool-section-title {
    margin:0 0 7px;
    color:var(--reader-muted);
    font-size:9px;
    font-weight:900;
    letter-spacing:.08em;
    text-transform:uppercase;
}

.reader-control-row {
    display:flex;
    flex-wrap:wrap;
    gap:6px;
}

.reader-btn,
.reader-select,
.reader-small-input {
    min-height:38px;
    border:1px solid #d8e0eb;
    border-radius:9px;
    padding:7px 9px;
    background:#fff;
    color:#4e5f78;
    font:800 10px "Nunito Sans",Arial,sans-serif;
}

.reader-btn { cursor:pointer; }

.reader-select {
    flex:1 1 135px;
    min-width:110px;
}

.reader-small-input {
    width:74px;
    font-weight:700;
}

.theme-dark .reader-btn,
.theme-dark .reader-select,
.theme-dark .reader-small-input {
    background:#303747;
    color:#e8edf5;
    border-color:#465065;
}

.theme-sepia .reader-btn,
.theme-sepia .reader-select,
.theme-sepia .reader-small-input {
    background:#fffaf0;
    color:#5f4c37;
    border-color:#ddd0b9;
}

/* Custom annotation popover. iPhone's native selection UI is left untouched. */
.selection-bar {
    position:fixed;
    z-index:20900;
    display:none;
    flex-direction:column;
    align-items:center;
    justify-content:center;
    gap:0;
    left:50%;
    top:calc(env(safe-area-inset-top,0px) + 54px);
    transform:translateX(-50%);
    max-width:calc(100vw - 12px);
    padding:0;
    background:transparent;
    color:white;
    box-shadow:none;
}

.selection-bar.show { display:flex; }

.selection-actions {
    display:flex;
    align-items:center;
    overflow:hidden;
    border-radius:8px;
    background:rgba(26,26,26,.97);
    box-shadow:0 9px 28px rgba(0,0,0,.26);
    backdrop-filter:blur(10px);
    -webkit-backdrop-filter:blur(10px);
}

.selection-btn {
    flex:0 0 auto;
    min-height:34px;
    border:0;
    border-right:1px solid rgba(255,255,255,.14);
    border-radius:0;
    padding:7px 11px;
    font:500 10px Arial,sans-serif;
    cursor:pointer;
    background:transparent;
    color:#fff;
    -webkit-tap-highlight-color:transparent;
}

.selection-actions .selection-btn:last-child {
    border-right:0;
}

.selection-colors {
    display:flex;
    align-items:center;
    justify-content:center;
    gap:9px;
    margin-top:0;
    padding:5px 12px 6px;
    border-radius:0 0 9px 9px;
    background:rgba(255,255,255,.98);
    box-shadow:0 5px 16px rgba(0,0,0,.12);
}

.selection-btn.color-dot {
    width:16px;
    height:16px;
    min-height:16px;
    padding:0;
    border:0;
    border-radius:50%;
    box-shadow:0 0 0 1px rgba(0,0,0,.06);
}

.reader-toast {
    position:fixed;
    z-index:21200;
    left:10px;
    right:10px;
    bottom:calc(58px + env(safe-area-inset-bottom,0px));
    display:none;
    padding:12px 13px;
    border-radius:12px;
    background:#111827;
    color:white;
    font-size:11px;
    box-shadow:0 12px 30px rgba(0,0,0,.24);
}

/* Loading / busy */
.reader-load-overlay {
    position:fixed;
    inset:0;
    z-index:22000;
    display:flex;
    align-items:center;
    justify-content:center;
    padding:18px;
    background:rgba(244,247,252,.94);
    backdrop-filter:blur(6px);
    -webkit-backdrop-filter:blur(6px);
}

.theme-dark .reader-load-overlay { background:rgba(17,22,31,.95); }
.reader-load-overlay.hidden { display:none; }

.reader-load-card {
    width:min(520px,100%);
    padding:22px;
    border-radius:20px;
    background:var(--reader-panel);
    color:var(--reader-text);
    box-shadow:0 24px 70px rgba(15,23,42,.20);
    border:1px solid rgba(100,116,139,.15);
}

.reader-load-title {
    font:700 22px/1.2 "Lora",Georgia,serif;
}

.reader-load-detail {
    margin-top:7px;
    color:#738097;
    font-size:12px;
    line-height:1.5;
}

.theme-dark .reader-load-detail { color:#b3bdcc; }

.reader-load-track {
    height:13px;
    margin-top:15px;
    border-radius:999px;
    overflow:hidden;
    background:rgba(148,163,184,.24);
}

.reader-load-fill {
    width:0%;
    height:100%;
    border-radius:999px;
    background:linear-gradient(90deg,#c98fc2,#789ee3);
    transition:width .18s ease;
}

.reader-load-fill.indeterminate {
    width:34%;
    animation:reader-load-slide 1.15s ease-in-out infinite;
}

@keyframes reader-load-slide {
    0% { transform:translateX(-120%); }
    100% { transform:translateX(310%); }
}

.reader-load-meta {
    margin-top:8px;
    display:flex;
    justify-content:space-between;
    gap:10px;
    color:#8490a3;
    font-size:10px;
}

.reader-load-actions {
    display:none;
    gap:8px;
    flex-wrap:wrap;
    margin-top:15px;
}

.reader-load-actions.show { display:flex; }

.reader-load-actions button,
.reader-load-actions a {
    border:0;
    border-radius:10px;
    padding:9px 11px;
    text-decoration:none;
    background:#eef2f8;
    color:#52627d;
    font:800 10px "Nunito Sans",Arial,sans-serif;
    cursor:pointer;
}

.reader-load-actions .primary {
    color:#fff;
    background:linear-gradient(135deg,#c98cc0,#789be0);
}

.reader-download-overlay {
    position:fixed;
    inset:0;
    z-index:22100;
    display:none;
    align-items:center;
    justify-content:center;
    padding:18px;
    background:rgba(24,22,19,.34);
    backdrop-filter:blur(5px);
    -webkit-backdrop-filter:blur(5px);
}

.reader-download-overlay.show { display:flex; }

.reader-download-card {
    width:min(430px,100%);
    padding:18px;
    border-radius:18px;
    background:var(--reader-panel);
    color:var(--reader-text);
    box-shadow:0 22px 60px rgba(0,0,0,.22);
    border:1px solid var(--reader-line);
}

.reader-download-title {
    font:600 18px Georgia,"Times New Roman",serif;
}

.reader-download-name {
    margin-top:5px;
    overflow:hidden;
    text-overflow:ellipsis;
    white-space:nowrap;
    color:var(--reader-muted);
    font-size:10px;
}

.reader-download-track {
    height:10px;
    margin-top:14px;
    overflow:hidden;
    border-radius:999px;
    background:rgba(120,110,100,.16);
}

.reader-download-bar {
    width:0%;
    height:100%;
    border-radius:999px;
    background:#98785a;
    transition:width .12s linear;
}

.reader-download-bar.indeterminate {
    width:34%;
    animation:reader-load-slide 1.15s ease-in-out infinite;
}

.reader-download-meta {
    display:flex;
    justify-content:space-between;
    gap:8px;
    margin-top:8px;
    color:var(--reader-muted);
    font-size:10px;
}

.reader-download-actions {
    display:flex;
    justify-content:flex-end;
    gap:7px;
    margin-top:13px;
}

.reader-download-actions button,
.reader-download-actions a {
    border:0;
    border-radius:9px;
    padding:8px 10px;
    background:rgba(120,110,100,.10);
    color:inherit;
    text-decoration:none;
    font-size:10px;
    font-weight:800;
    cursor:pointer;
}

.reader-download-direct { display:none; }
.reader-download-direct.show { display:inline-flex; }

.reader-page-busy {
    position:absolute;
    z-index:80;
    top:10px;
    left:50%;
    transform:translateX(-50%);
    display:none;
    padding:7px 11px;
    border-radius:999px;
    background:rgba(17,24,39,.86);
    color:#fff;
    font-size:10px;
    font-weight:800;
    box-shadow:0 6px 18px rgba(0,0,0,.15);
    pointer-events:none;
}

.reader-page-busy.show { display:block; }

@media (min-width:800px) {
    .reader-topline {
        min-height:50px;
        padding:5px 16px;
        grid-template-columns:minmax(100px,auto) minmax(0,1fr) auto auto;
    }
    .reader-back-btn { font-size:13px; }
    .reader-book-title { font-size:13px; }
    .reader-canvas-area { padding:14px 18px 14px; }
    .reader-bottom-bar { min-height:48px; }
    .reader-page-nav { gap:12px; }
    .reader-page-arrow { width:44px; font-size:25px; }
    .reader-page-input { width:88px; font-size:15px; }
    .reader-page-total { min-width:58px; font-size:12px; }
    .reader-page-kind { display:inline; }
    .reader-tools-sheet {
        left:auto;
        right:18px;
        bottom:18px;
        width:min(390px,calc(100vw - 36px));
        border-radius:22px;
        padding-bottom:14px;
    }
    .reader-tools-grid { grid-template-columns:repeat(3,minmax(0,1fr)); }
    .reader-toast {
        left:auto;
        right:20px;
        width:360px;
        bottom:20px;
    }
}

@media (max-width:420px) {
    .reader-topline {
        grid-template-columns:minmax(64px,auto) minmax(0,1fr) 32px 32px;
        gap:2px;
        padding-left:5px;
        padding-right:5px;
    }
    .reader-back-btn { font-size:11px; }
    .reader-book-title { font-size:11px; }
    .reader-icon-btn { width:32px; min-height:34px; font-size:18px; }
    .selection-btn { padding:7px 9px; font-size:9px; }
    .reader-bottom-bar { padding-left:6px; padding-right:6px; }
    .reader-page-nav { gap:5px; }
    .reader-page-arrow { width:34px; }
    .reader-page-input { width:66px; }
    .reader-page-total { min-width:44px; font-size:10px; }
}
</style>

<div class="reader-root theme-{{ state.theme or 'light' }}" id="readerRoot">
    <div class="reader-toolbar">
        <div class="reader-topline">
            <button class="reader-back-btn" type="button" onclick="returnToLibrary()"><span class="reader-chevron">‹</span>Library</button>

            <div class="reader-book-info">
                <div class="reader-book-title">{{ book.title }}</div>
                <div class="reader-book-author">{{ book.author }}</div>
            </div>

            <button class="reader-icon-btn" type="button" title="Search" aria-label="Search in book" onclick="toggleReaderSearch()">
                <svg class="reader-top-svg" viewBox="0 0 24 24" aria-hidden="true"><circle cx="11" cy="11" r="6.5"></circle><path d="m16 16 4.2 4.2"></path></svg>
            </button>
            <button class="reader-icon-btn" type="button" title="Reader tools" aria-label="Reader tools" onclick="toggleToolsPanel()">•••</button>
        </div>

        <div class="reader-search-panel" id="readerSearchPanel" role="search">
            <div class="reader-search-box">
                <span class="reader-search-symbol" aria-hidden="true">
                    <svg viewBox="0 0 24 24"><circle cx="11" cy="11" r="6.5"></circle><path d="m16 16 4.2 4.2"></path></svg>
                </span>
                <input
                    class="reader-search"
                    id="readerSearchInput"
                    type="search"
                    placeholder="Search"
                    autocomplete="off"
                    enterkeyhint="search"
                    oninput="handleReaderSearchInput(this.value)"
                    onkeydown="if(event.key==='Enter'){event.preventDefault();findInBook();}"
                >
                <button class="reader-search-clear" id="readerSearchClear" type="button" aria-label="Clear search" onclick="clearReaderSearchInput()">×</button>
            </div>
            <button class="reader-search-action" type="button" onclick="toggleReaderSearch(false)">Cancel</button>
            <div class="reader-search-results" id="readerSearchResults">
                <span id="readerSearchResultsText">0 results</span>
                <span class="reader-search-result-nav">
                    <button class="reader-search-step" type="button" onclick="stepSearchMatch(-1)" aria-label="Previous search result">‹</button>
                    <span id="readerSearchPosition">0 of 0</span>
                    <button class="reader-search-step" type="button" onclick="stepSearchMatch(1)" aria-label="Next search result">›</button>
                </span>
            </div>
        </div>

        <div class="reader-progress">
            <div id="readerProgressFill" style="width:{{ state.progress_percent or 0 }}%"></div>
        </div>
    </div>

    <main class="reader-main">
        <div class="reader-page-busy" id="readerPageBusy">Loading page…</div>

        <div class="reader-canvas-area" id="readerCanvasArea">
            {% if reader_format == 'PDF' %}
            <div id="pdfStage">
                <canvas id="pdfCanvas"></canvas>
                <div class="pdf-annotation-layer" id="pdfAnnotationLayer"></div>
                <div class="textLayer" id="pdfTextLayer"></div>
                <div class="pdf-link-layer" id="pdfLinkLayer"></div>
            </div>
            {% else %}
            <div id="epubViewer"></div>
            {% endif %}
        </div>
    </main>

    <footer class="reader-bottom-bar">
        <div class="reader-page-nav">
            <button class="reader-page-arrow" id="readerPreviousButton" type="button" onclick="goPrevious()" title="Previous page" aria-label="Previous page">‹</button>
            <span class="reader-page-kind" id="readerPageKind">Page</span>
            <input
                class="reader-page-input"
                id="readerPageInput"
                type="number"
                min="1"
                value="{{ state.pdf_page or 1 }}"
                inputmode="numeric"
                aria-label="Go directly to page"
                title="Type a page or location and press Enter"
                onkeydown="if(event.key==='Enter'){event.preventDefault();this.blur();}"
                onchange="jumpReaderPage()"
            >
            <span class="reader-page-total" id="readerPageTotal">/ …</span>
            <button class="reader-page-arrow" id="readerNextButton" type="button" onclick="goNext()" title="Next page" aria-label="Next page">›</button>
        </div>
    </footer>

    <aside class="reader-side" id="readerSide">
        <div class="reader-side-head">
            <h3>Reading Notes</h3>
            <button class="reader-btn" type="button" onclick="toggleSidePanel(false)">✕</button>
        </div>

        <div class="reader-side-tabs">
            <button class="reader-side-tab active" type="button" onclick="openReaderTab('annotations',this)">Highlights</button>
            <button class="reader-side-tab" type="button" onclick="openReaderTab('bookmarks',this)">Bookmarks</button>
        </div>

        <div class="reader-side-body">
            <div class="reader-side-panel active" id="side-annotations"></div>
            <div class="reader-side-panel" id="side-bookmarks"></div>
        </div>
    </aside>

    <div class="reader-side-backdrop" id="readerSideBackdrop" onclick="toggleSidePanel(false)"></div>

    <div class="reader-tools-sheet" id="readerToolsSheet">
        <div class="reader-tools-handle"></div>

        <div class="reader-tools-head">
            <h3>Tools</h3>
        </div>

        <div class="reader-tools-grid">
            <button class="reader-tool-btn" type="button" onclick="addCurrentBookmark();toggleToolsPanel(false)">
                <span class="reader-tool-icon-circle" aria-hidden="true">
                    <svg viewBox="0 0 24 24"><path d="M6 4.8A1.8 1.8 0 0 1 7.8 3h8.4A1.8 1.8 0 0 1 18 4.8V21l-6-3.7L6 21z"/></svg>
                </span>
                <span>Bookmark</span>
            </button>

            <button class="reader-tool-btn" type="button" onclick="toggleToolDetail('theme')">
                <span class="reader-tool-icon-circle" aria-hidden="true" style="font-family:Georgia,serif;">Aa</span>
                <span>Theme</span>
            </button>

            <button class="reader-tool-btn" type="button" onclick="toggleToolsPanel(false);toggleSidePanel(true)">
                <span class="reader-tool-icon-circle" aria-hidden="true">
                    <svg viewBox="0 0 24 24"><path d="M6 3h12a2 2 0 0 1 2 2v11a2 2 0 0 1-2 2h-7l-5 3v-3a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2z"/><path d="M8 8h8M8 12h6"/></svg>
                </span>
                <span>Notes</span>
            </button>

            <button class="reader-tool-btn highlight-tool" type="button" onclick="openHighlightsFromTools()">
                <span class="reader-tool-icon-circle" aria-hidden="true">
                    <svg viewBox="0 0 24 24"><path d="m5 16 9.8-9.8 3 3L8 19H5z"/><path d="m13.8 7.2 3 3"/><path d="M4 21h7"/></svg>
                </span>
                <span>Highlights</span>
            </button>

            <button class="reader-tool-btn" type="button" onclick="startReaderDownload()">
                <span class="reader-tool-icon-circle" aria-hidden="true">
                    <svg viewBox="0 0 24 24"><path d="M12 3v12"/><path d="m7 10 5 5 5-5"/><path d="M5 21h14"/></svg>
                </span>
                <span>Download</span>
            </button>

            <button class="reader-tool-btn" type="button" onclick="toggleToolDetail('contents')">
                <span class="reader-tool-icon-circle" aria-hidden="true">
                    <svg viewBox="0 0 24 24"><path d="M9 6h10M9 12h10M9 18h10"/><path d="M5 6h.01M5 12h.01M5 18h.01"/></svg>
                </span>
                <span>Contents</span>
            </button>
        </div>

        <div class="reader-tool-detail" id="readerToolDetailTheme">
            <div class="reader-tool-section-title">Theme</div>
            <div class="reader-control-row">
                <select class="reader-select" id="themeSelect" onchange="setReaderTheme(this.value)">
                    <option value="light">Light</option>
                    <option value="sepia">Sepia</option>
                    <option value="dark">Dark</option>
                </select>
            </div>
        </div>

        <div class="reader-tool-detail" id="readerToolDetailContents">
            {% if reader_format == 'PDF' %}
            <div class="reader-tool-section-title">PDF View</div>
            <div class="reader-control-row">
                <button class="reader-btn" type="button" onclick="zoomPdf(-0.15)">Zoom −</button>
                <button class="reader-btn" type="button" onclick="zoomPdf(0.15)">Zoom +</button>
                <button class="reader-btn" type="button" onclick="fitPdfWidth()">Fit Width</button>
                <button class="reader-btn" type="button" onclick="fitPdfPage()">Fit Page</button>
            </div>
            {% else %}
            <div class="reader-tool-section-title">Table of Contents</div>
            <div class="reader-control-row">
                <select class="reader-select" id="tocSelect" onchange="jumpToc(this.value)">
                    <option value="">Table of Contents</option>
                </select>
            </div>

            <div class="reader-tool-section-title" style="margin-top:10px;">Text</div>
            <div class="reader-control-row">
                <button class="reader-btn" type="button" onclick="changeEpubFont(-10)">A−</button>
                <button class="reader-btn" type="button" onclick="changeEpubFont(10)">A+</button>
                <select class="reader-select" id="fontFamilySelect" onchange="setEpubFontFamily(this.value)">
                    <option value="Georgia, serif">Serif</option>
                    <option value="Arial, sans-serif">Sans Serif</option>
                    <option value="Verdana, sans-serif">Verdana</option>
                    <option value="Trebuchet MS, sans-serif">Trebuchet</option>
                </select>
                <select class="reader-select" id="lineHeightSelect" onchange="setEpubLineHeight(this.value)">
                    <option value="1.3">Tight Lines</option>
                    <option value="1.6">Normal Lines</option>
                    <option value="2.0">Wide Lines</option>
                </select>
            </div>
            {% endif %}

            {% if formats|length > 1 %}
            <div class="reader-tool-section-title" style="margin-top:10px;">Format</div>
            <div class="reader-control-row">
                <select class="reader-select" id="formatSelect" onchange="switchFormat(this.value)">
                    {% for fmt in formats %}
                    <option value="{{ fmt }}" {{ 'selected' if fmt == reader_format else '' }}>{{ fmt }}</option>
                    {% endfor %}
                </select>
            </div>
            {% endif %}
        </div>

        <div class="reader-more-grid">
            <button
                class="reader-tool-btn favorite {{ 'on' if state.favorite else '' }}"
                id="readerFavorite"
                type="button"
                onclick="toggleReaderFavorite()"
            >
                <span class="reader-tool-icon-circle" id="readerFavoriteIcon">{{ '♥' if state.favorite else '♡' }}</span>
                <span>Favorite</span>
            </button>

            <button class="reader-tool-btn" id="finishButton" type="button" onclick="toggleFinished()">
                <span class="reader-tool-icon-circle">✓</span>
                <span id="finishButtonLabel">{{ 'Reopen' if state.completed_at else 'Mark Finished' }}</span>
            </button>
        </div>
    </div>

    <div class="reader-tools-backdrop" id="readerToolsBackdrop" onclick="toggleToolsPanel(false)"></div>

    <div class="selection-bar" id="selectionBar" aria-label="Selected text actions">
        <div class="selection-actions">
            <button class="selection-btn" type="button" onclick="copySelectedText()">Copy</button>
            <button class="selection-btn" type="button" onclick="savePendingAnnotation('underline','#e5962d',false,false)">Underline</button>
            <button class="selection-btn" type="button" onclick="savePendingAnnotation('highlight','#ffe66d',true,false)">Note</button>
            <button class="selection-btn" type="button" onclick="savePendingAnnotation('highlight','#ffd2df',true,true)">Sermon</button>
        </div>

        <div class="selection-colors" aria-label="Highlight colors">
            <button class="selection-btn color-dot" type="button" style="background:#ffd45a" onclick="savePendingAnnotation('highlight','#ffe66d',false,false)" title="Yellow Highlight" aria-label="Yellow Highlight"></button>
            <button class="selection-btn color-dot" type="button" style="background:#79d991" onclick="savePendingAnnotation('highlight','#9ee6b8',false,false)" title="Green Highlight" aria-label="Green Highlight"></button>
            <button class="selection-btn color-dot" type="button" style="background:#4e8ff0" onclick="savePendingAnnotation('highlight','#9ed3ff',false,false)" title="Blue Highlight" aria-label="Blue Highlight"></button>
        </div>
    </div>

    <div class="reader-load-overlay" id="readerLoadOverlay">
        <div class="reader-load-card">
            <div class="reader-load-title" id="readerLoadTitle">Opening ebook…</div>
            <div class="reader-load-detail" id="readerLoadDetail">Preparing your reader.</div>
            <div class="reader-load-track">
                <div class="reader-load-fill indeterminate" id="readerLoadFill"></div>
            </div>
            <div class="reader-load-meta">
                <span id="readerLoadBytes">Please wait…</span>
                <span id="readerLoadPercent"></span>
            </div>
            <div class="reader-load-actions" id="readerLoadActions">
                <button class="primary" type="button" onclick="location.reload()">Retry</button>
                <a href="{{ download_url }}">Download Book</a>
                <button type="button" id="alternateFormatButton" onclick="tryAlternateFormat()" style="display:none;">Try Other Format</button>
            </div>
        </div>
    </div>

    <div class="reader-download-overlay" id="readerDownloadOverlay" aria-live="polite">
        <div class="reader-download-card">
            <div class="reader-download-title" id="readerDownloadTitle">Downloading ebook…</div>
            <div class="reader-download-name" id="readerDownloadName">Preparing download…</div>
            <div class="reader-download-track">
                <div class="reader-download-bar indeterminate" id="readerDownloadBar"></div>
            </div>
            <div class="reader-download-meta">
                <span id="readerDownloadBytes">Connecting…</span>
                <span id="readerDownloadPercent"></span>
            </div>
            <div class="reader-download-actions">
                <button type="button" id="readerDownloadCancel" onclick="cancelReaderDownload()">Cancel</button>
                <a class="reader-download-direct" id="readerDownloadDirect" href="{{ download_url }}">Direct download</a>
            </div>
        </div>
    </div>

    <div class="reader-toast" id="readerToast"></div>
</div>

<script>
const BOOK_ID = {{ book.id }};
const READER_FORMAT = {{ reader_format|tojson }};
const MEDIA_URL = {{ media_url|tojson }};
const DOWNLOAD_URL = {{ download_url|tojson }};
const READ_BASE_URL = {{ read_base_url|tojson }};
const STATE = {{ state|tojson }};
const JUMP_ANNOTATION_ID = {{ jump_annotation_id|tojson }};
const JUMP_BOOKMARK_ID = {{ jump_bookmark_id|tojson }};
const AVAILABLE_FORMATS = {{ formats|tojson }};
const PASTOR_RESOURCES_URL = {{ url_for('pastor_resources')|tojson }};
const LIBRARY_RESTORE_KEY = "pastorResourcesRestoreRequestedV1";
const READER_RETURN_URL_KEY = "pastorReaderReturnUrlV1";

let annotations = [];
let bookmarks = [];
let pendingSelection = null;
let currentProgress = Number(STATE.progress_percent || 0);
let readingSessionId = "";
let lastActivityAt = Date.now();
let currentTheme = STATE.theme || "light";
let finished = Boolean(STATE.completed_at);
let selectionCaptureTimer = null;
let selectionToolbarInteracting = false;
let lastEpubContents = null;
let searchMatches = [];
let searchMatchIndex = -1;
let activeDownloadController = null;
let epubLocationTotal = 0;
let epubLocationCurrent = 1;
const EPUB_CONTENT_HANDLERS = new WeakSet();

function showReaderToast(message) {
    const toast = document.getElementById("readerToast");
    toast.textContent = message;
    toast.style.display = "block";
    clearTimeout(toast.hideTimer);
    toast.hideTimer = setTimeout(() => toast.style.display="none", 4200);
}

function formatReaderBytes(bytes) {
    const value = Number(bytes || 0);
    if (!value || value < 0) return "";
    const units = ["B","KB","MB","GB"];
    let size = value;
    let unit = 0;
    while (size >= 1024 && unit < units.length - 1) {
        size /= 1024;
        unit++;
    }
    return (unit === 0 ? Math.round(size) : size.toFixed(size >= 10 ? 1 : 2)) + " " + units[unit];
}

function showReaderLoading(title, detail="", percent=null, loaded=0, total=0) {
    const overlay = document.getElementById("readerLoadOverlay");
    const fill = document.getElementById("readerLoadFill");
    overlay.classList.remove("hidden");
    document.getElementById("readerLoadTitle").textContent = title || "Opening ebook…";
    document.getElementById("readerLoadDetail").textContent = detail || "Preparing your reader.";
    document.getElementById("readerLoadActions").classList.remove("show");

    if (percent === null || !Number.isFinite(Number(percent))) {
        fill.classList.add("indeterminate");
        fill.style.width = "34%";
        document.getElementById("readerLoadPercent").textContent = "";
    } else {
        const safe = Math.max(0, Math.min(100, Number(percent)));
        fill.classList.remove("indeterminate");
        fill.style.transform = "none";
        fill.style.width = safe.toFixed(1) + "%";
        document.getElementById("readerLoadPercent").textContent = Math.round(safe) + "%";
    }

    if (loaded > 0) {
        document.getElementById("readerLoadBytes").textContent = total > 0
            ? formatReaderBytes(loaded) + " of " + formatReaderBytes(total)
            : formatReaderBytes(loaded) + " loaded";
    } else {
        document.getElementById("readerLoadBytes").textContent = "Please wait…";
    }
}

function hideReaderLoading() {
    document.getElementById("readerLoadOverlay").classList.add("hidden");
}

function showReaderLoadError(message) {
    const overlay = document.getElementById("readerLoadOverlay");
    const fill = document.getElementById("readerLoadFill");
    overlay.classList.remove("hidden");
    fill.classList.remove("indeterminate");
    fill.style.transform = "none";
    fill.style.width = "100%";
    fill.style.background = "#ef4444";
    document.getElementById("readerLoadTitle").textContent = "Unable to open this ebook";
    document.getElementById("readerLoadDetail").textContent = message || "The reader could not load this file.";
    document.getElementById("readerLoadBytes").textContent = "You can retry or download the book.";
    document.getElementById("readerLoadPercent").textContent = "";
    document.getElementById("readerLoadActions").classList.add("show");

    const alt = document.getElementById("alternateFormatButton");
    const other = AVAILABLE_FORMATS.find(fmt => String(fmt).toUpperCase() !== READER_FORMAT);
    alt.style.display = other ? "inline-flex" : "none";
}

function parseReaderDownloadFilename(disposition) {
    const value = String(disposition || "");
    let match = value.match(/filename\*=UTF-8''([^;]+)/i);
    if (match && match[1]) {
        try { return decodeURIComponent(match[1].trim()); } catch (error) { return match[1].trim(); }
    }

    match = value.match(/filename="?([^";]+)"?/i);
    return match && match[1] ? match[1].trim() : "ebook";
}

function closeReaderDownloadProgress() {
    document.getElementById("readerDownloadOverlay")?.classList.remove("show");
    document.getElementById("readerDownloadDirect")?.classList.remove("show");
}

function cancelReaderDownload() {
    if (activeDownloadController) {
        activeDownloadController.abort();
    } else {
        closeReaderDownloadProgress();
    }
}

function updateReaderDownloadProgress(loaded, total) {
    const bar = document.getElementById("readerDownloadBar");
    const bytes = document.getElementById("readerDownloadBytes");
    const percent = document.getElementById("readerDownloadPercent");

    if (!bar || !bytes || !percent) return;

    if (total > 0) {
        const pct = Math.max(0, Math.min(100, (loaded / total) * 100));
        bar.classList.remove("indeterminate");
        bar.style.transform = "none";
        bar.style.width = pct.toFixed(1) + "%";
        bytes.textContent = formatReaderBytes(loaded) + " / " + formatReaderBytes(total);
        percent.textContent = Math.round(pct) + "%";
    } else {
        bar.classList.add("indeterminate");
        bar.style.width = "34%";
        bytes.textContent = loaded > 0 ? formatReaderBytes(loaded) + " downloaded" : "Connecting…";
        percent.textContent = "";
    }
}

async function startReaderDownload() {
    if (activeDownloadController) return;

    toggleToolsPanel(false);

    const overlay = document.getElementById("readerDownloadOverlay");
    const title = document.getElementById("readerDownloadTitle");
    const name = document.getElementById("readerDownloadName");
    const direct = document.getElementById("readerDownloadDirect");
    const cancel = document.getElementById("readerDownloadCancel");

    overlay?.classList.add("show");
    direct?.classList.remove("show");
    if (title) title.textContent = "Downloading ebook…";
    if (name) name.textContent = "Preparing download…";
    if (cancel) cancel.textContent = "Cancel";
    updateReaderDownloadProgress(0, 0);

    const controller = new AbortController();
    activeDownloadController = controller;

    try {
        const response = await fetch(DOWNLOAD_URL, {
            method:"GET",
            credentials:"same-origin",
            cache:"no-store",
            signal:controller.signal
        });

        if (!response.ok) {
            throw new Error("Download request failed (HTTP " + response.status + ").");
        }

        const filename = parseReaderDownloadFilename(
            response.headers.get("Content-Disposition")
        );
        const total = Number(response.headers.get("Content-Length") || 0);
        const contentType = response.headers.get("Content-Type") || "application/octet-stream";

        if (name) name.textContent = filename;

        const chunks = [];
        let loaded = 0;

        if (response.body?.getReader) {
            const reader = response.body.getReader();

            while (true) {
                const {done, value} = await reader.read();
                if (done) break;
                if (!value) continue;

                chunks.push(value);
                loaded += value.byteLength;
                updateReaderDownloadProgress(loaded, total);
            }
        } else {
            const blob = await response.blob();
            chunks.push(blob);
            loaded = blob.size;
            updateReaderDownloadProgress(loaded, total || loaded);
        }

        const blob = new Blob(chunks, {type:contentType});
        updateReaderDownloadProgress(blob.size, total || blob.size);

        const objectUrl = URL.createObjectURL(blob);
        const link = document.createElement("a");
        link.href = objectUrl;
        link.download = filename || "ebook";
        link.style.display = "none";
        document.body.appendChild(link);
        link.click();
        link.remove();

        setTimeout(() => URL.revokeObjectURL(objectUrl), 60000);

        if (title) title.textContent = "Download complete";
        if (cancel) cancel.textContent = "Close";
        activeDownloadController = null;
        showReaderToast("Download complete.");

        setTimeout(() => {
            closeReaderDownloadProgress();
        }, 1400);

    } catch (error) {
        const aborted = error?.name === "AbortError";
        activeDownloadController = null;

        if (aborted) {
            closeReaderDownloadProgress();
            showReaderToast("Download cancelled.");
            return;
        }

        if (title) title.textContent = "Download interrupted";
        if (name) name.textContent = error?.message || "Unable to download this ebook.";
        if (cancel) cancel.textContent = "Close";
        direct?.classList.add("show");
        showReaderToast("Download interrupted. You can try the direct download.");
    }
}

function setPageBusy(show, text="Loading page…") {
    const busy = document.getElementById("readerPageBusy");
    if (!busy) return;
    busy.textContent = text;
    busy.classList.toggle("show", Boolean(show));
}

function tryAlternateFormat() {
    const other = AVAILABLE_FORMATS.find(fmt => String(fmt).toUpperCase() !== READER_FORMAT);
    if (other) window.location.href = READ_BASE_URL + "?format=" + encodeURIComponent(other);
}

function returnToLibrary() {
    let returnUrl = "";
    try { returnUrl = sessionStorage.getItem(READER_RETURN_URL_KEY) || ""; } catch (error) {}

    if (returnUrl && returnUrl.startsWith("/pastor-resources/my-library")) {
        window.location.href = returnUrl;
        return;
    }

    try { sessionStorage.setItem(LIBRARY_RESTORE_KEY, "1"); } catch (error) {}
    window.location.href = PASTOR_RESOURCES_URL;
}

function updateReaderPageControls(current, total, kind="Page", enabled=true) {
    const input = document.getElementById("readerPageInput");
    const totalEl = document.getElementById("readerPageTotal");
    const kindEl = document.getElementById("readerPageKind");

    const currentNumber = Math.max(1, Number(current || 1));
    const totalNumber = Math.max(0, Number(total || 0));

    if (input) {
        if (document.activeElement !== input) {
            input.value = String(Math.round(currentNumber));
        }
        input.min = "1";
        if (totalNumber > 0) input.max = String(Math.round(totalNumber));
        else input.removeAttribute("max");
        input.disabled = !enabled;
        input.setAttribute(
            "aria-label",
            String(kind || "Page") + " number. Type a number and press Enter."
        );
    }

    if (totalEl) {
        totalEl.textContent = totalNumber > 0
            ? "/ " + Math.round(totalNumber)
            : "/ …";
    }

    if (kindEl) {
        kindEl.textContent = String(kind || "Page");
    }
}

function syncReaderViewport() {
    const root = document.getElementById("readerRoot");
    if (!root) return;

    const viewport = window.visualViewport;
    const height = Math.max(1, Math.round(viewport?.height || window.innerHeight || document.documentElement.clientHeight || 1));
    const top = Math.max(0, Math.round(viewport?.offsetTop || 0));

    root.style.setProperty("--reader-viewport-height", height + "px");
    root.style.setProperty("--reader-viewport-top", top + "px");
}

function handleReaderSearchInput(value) {
    const clearButton = document.getElementById("readerSearchClear");
    const hasValue = Boolean(String(value || "").trim());
    clearButton?.classList.toggle("show", hasValue);

    if (!hasValue) {
        searchMatches = [];
        searchMatchIndex = -1;
        updateSearchResultUI();
    }
}

function clearReaderSearchInput() {
    const input = document.getElementById("readerSearchInput");
    if (!input) return;
    input.value = "";
    handleReaderSearchInput("");
    try { input.focus({preventScroll:true}); } catch (error) { input.focus?.(); }
}

function toggleReaderSearch(force) {
    const panel = document.getElementById("readerSearchPanel");
    const input = document.getElementById("readerSearchInput");
    if (!panel) return;

    const open = typeof force === "boolean"
        ? force
        : !panel.classList.contains("open");

    panel.classList.toggle("open", open);

    if (open) {
        toggleToolsPanel(false);
        handleReaderSearchInput(input?.value || "");
        setTimeout(() => {
            try {
                input?.focus({preventScroll:true});
                input?.select?.();
            } catch (error) {
                input?.focus?.();
            }
        }, 60);
    } else {
        try { input?.blur?.(); } catch (error) {}
    }

    setTimeout(() => {
        try { rendition?.resize?.(); } catch (error) {}
    }, 90);
}

function toggleToolsPanel(force) {
    const sheet = document.getElementById("readerToolsSheet");
    const backdrop = document.getElementById("readerToolsBackdrop");
    if (!sheet || !backdrop) return;

    const open = typeof force === "boolean"
        ? force
        : !sheet.classList.contains("open");

    sheet.classList.toggle("open", open);
    backdrop.classList.toggle("show", open);

    if (open) {
        toggleReaderSearch(false);
        toggleSidePanel(false);
    } else {
        document.getElementById("readerToolDetailTheme")?.classList.remove("open");
        document.getElementById("readerToolDetailContents")?.classList.remove("open");
    }
}

function openHighlightsFromTools() {
    toggleToolsPanel(false);
    const button = document.querySelector(".reader-side-tab");
    openReaderTab("annotations", button);
    toggleSidePanel(true);
}

function toggleToolDetail(name) {
    const theme = document.getElementById("readerToolDetailTheme");
    const contents = document.getElementById("readerToolDetailContents");
    const target = name === "theme" ? theme : contents;

    if (!target) return;

    const shouldOpen = !target.classList.contains("open");
    theme?.classList.remove("open");
    contents?.classList.remove("open");

    if (shouldOpen) {
        target.classList.add("open");
        setTimeout(() => {
            try { target.scrollIntoView({block:"nearest",behavior:"smooth"}); } catch (error) {}
        }, 30);
    }
}

function selectionViewportRect(range, sourceWindow=window) {
    if (!range) return null;

    const rects = Array.from(range.getClientRects?.() || [])
        .filter(rect => rect.width > 0.5 && rect.height > 0.5);

    if (!rects.length) {
        const single = range.getBoundingClientRect?.();
        if (!single || (!single.width && !single.height)) return null;
        rects.push(single);
    }

    let left = Math.min(...rects.map(rect => rect.left));
    let top = Math.min(...rects.map(rect => rect.top));
    let right = Math.max(...rects.map(rect => rect.right));
    let bottom = Math.max(...rects.map(rect => rect.bottom));

    if (sourceWindow && sourceWindow !== window) {
        try {
            const frame = sourceWindow.frameElement;
            if (frame) {
                const frameRect = frame.getBoundingClientRect();
                left += frameRect.left;
                right += frameRect.left;
                top += frameRect.top;
                bottom += frameRect.top;
            }
        } catch (error) {}
    }

    return {
        left,
        top,
        right,
        bottom,
        width:Math.max(1,right-left),
        height:Math.max(1,bottom-top)
    };
}

function showSelectionBarAtRect(_rect) {
    const bar = document.getElementById("selectionBar");
    if (!bar) return;

    // Keep our actions away from the native iPhone text-selection bubble.
    // They now live at one predictable location directly below the reader
    // toolbar and only appear while a real text selection exists.
    toggleToolsPanel(false);
    const searchPanel = document.getElementById("readerSearchPanel");
    if (searchPanel?.classList.contains("open")) {
        toggleReaderSearch(false);
    }

    bar.classList.add("show");
    bar.style.visibility = "hidden";
    bar.style.left = "50%";
    bar.style.right = "auto";
    bar.style.bottom = "auto";
    bar.style.transform = "translateX(-50%)";

    requestAnimationFrame(() => {
        const toolbar = document.querySelector(".reader-toolbar");
        const toolbarBottom = toolbar
            ? toolbar.getBoundingClientRect().bottom
            : Number(window.visualViewport?.offsetTop || 0) + 52;

        const viewportTop = Number(window.visualViewport?.offsetTop || 0);
        const top = Math.max(
            viewportTop + 4,
            toolbarBottom + 5
        );

        bar.style.top = Math.round(top) + "px";
        bar.style.visibility = "visible";
    });
}

function hideSelectionBarOnly() {
    const bar = document.getElementById("selectionBar");
    if (!bar) return;
    bar.classList.remove("show");
    bar.style.visibility = "";
    bar.style.left = "";
    bar.style.top = "";
    bar.style.right = "";
    bar.style.bottom = "";
    bar.style.transform = "";
}

function schedulePdfSelectionCapture(delay=140) {
    clearTimeout(selectionCaptureTimer);
    selectionCaptureTimer = setTimeout(() => {
        if (selectionToolbarInteracting) return;
        capturePdfSelection();
    }, delay);
}

async function fetchArrayBufferWithProgress(url, label="EPUB") {
    const response = await fetch(url, {credentials:"same-origin"});
    if (!response.ok) throw new Error(label + " request failed (HTTP " + response.status + ").");

    const total = Number(response.headers.get("Content-Length") || 0);

    if (!response.body || !response.body.getReader) {
        showReaderLoading("Loading " + label + "…", "Downloading ebook content…", total ? 0 : null, 0, total);
        return await response.arrayBuffer();
    }

    const reader = response.body.getReader();
    const chunks = [];
    let loaded = 0;

    while (true) {
        const {done, value} = await reader.read();
        if (done) break;
        if (!value) continue;
        chunks.push(value);
        loaded += value.byteLength;
        const pct = total > 0 ? (loaded / total) * 100 : null;
        showReaderLoading("Loading " + label + "…", "Downloading ebook content from the private library…", pct, loaded, total);
    }

    return await new Blob(chunks).arrayBuffer();
}

async function apiJson(url, options={}) {
    const response = await fetch(url, options);
    const data = await response.json();
    if (!response.ok || !data.ok) throw new Error(data.error || "Request failed.");
    return data;
}

function setProgress(percent) {
    currentProgress = Math.max(0, Math.min(100, Number(percent || 0)));
    document.getElementById("readerProgressFill").style.width = currentProgress + "%";
}

async function saveState(payload) {
    try {
        await apiJson("/pastor-resources/api/state/" + BOOK_ID, {
            method:"POST",
            headers:{"Content-Type":"application/json"},
            body:JSON.stringify(payload)
        });
    } catch (error) {
        console.warn("State save failed", error);
    }
}

async function loadNotesData() {
    try {
        const [a,b] = await Promise.all([
            apiJson("/pastor-resources/api/annotations?book_id=" + BOOK_ID),
            apiJson("/pastor-resources/api/bookmarks?book_id=" + BOOK_ID)
        ]);
        annotations = a.annotations || [];
        bookmarks = b.bookmarks || [];
        renderSidePanel();
    } catch (error) {
        console.warn(error);
    }
}

function renderSidePanel() {
    const ann = document.getElementById("side-annotations");
    ann.innerHTML = annotations.length ? annotations.map(item => `
        <article class="reader-item">
            <strong>${item.is_sermon_note ? 'Sermon Note' : (item.annotation_type === 'underline' ? 'Underline' : 'Highlight')}</strong>
            ${item.selected_text ? '<div class="reader-item-quote">' + escapeReaderHtml(item.selected_text) + '</div>' : ''}
            ${item.note ? '<div style="margin-top:6px">' + escapeReaderHtml(item.note) + '</div>' : ''}
            ${item.tags ? '<div style="margin-top:5px;color:#8b5f92"># ' + escapeReaderHtml(item.tags) + '</div>' : ''}
            <div class="reader-item-actions">
                <button onclick="jumpToAnnotation(${item.id})">Go</button>
                <button onclick="editAnnotation(${item.id})">Edit Note</button>
                <button onclick="deleteReaderAnnotation(${item.id})">Delete</button>
            </div>
        </article>
    `).join("") : '<div class="reader-item">Select text in the book to highlight, underline or save a sermon note.</div>';

    const bm = document.getElementById("side-bookmarks");
    bm.innerHTML = bookmarks.length ? bookmarks.map(item => `
        <article class="reader-item">
            <strong>${escapeReaderHtml(item.label || (item.format === 'PDF' ? 'Page ' + (item.page || '') : 'Saved location'))}</strong>
            <div class="reader-item-actions">
                <button onclick="jumpToBookmark(${item.id})">Go</button>
                <button onclick="deleteReaderBookmark(${item.id})">Delete</button>
            </div>
        </article>
    `).join("") : '<div class="reader-item">No bookmarks in this book yet.</div>';
}

function escapeReaderHtml(value) {
    const div = document.createElement("div");
    div.textContent = value || "";
    return div.innerHTML;
}

function toggleSidePanel(force) {
    const side = document.getElementById("readerSide");
    const backdrop = document.getElementById("readerSideBackdrop");
    const open = typeof force === "boolean" ? force : !side.classList.contains("open");
    side.classList.toggle("open",open);
    backdrop.classList.toggle("show",open);
}

function openReaderTab(name, button) {
    document.querySelectorAll(".reader-side-tab").forEach(el => el.classList.remove("active"));
    document.querySelectorAll(".reader-side-panel").forEach(el => el.classList.remove("active"));
    if (button) button.classList.add("active");
    document.getElementById("side-" + name).classList.add("active");
}

async function toggleReaderFavorite() {
    const button = document.getElementById("readerFavorite");
    const icon = document.getElementById("readerFavoriteIcon");
    if (!button) return;

    const favorite = !button.classList.contains("on");

    try {
        await apiJson("/pastor-resources/api/favorite/" + BOOK_ID, {
            method:"POST",
            headers:{"Content-Type":"application/json"},
            body:JSON.stringify({favorite})
        });

        button.classList.toggle("on", favorite);
        if (icon) icon.textContent = favorite ? "♥" : "♡";
        showReaderToast(favorite ? "Added to Favorites." : "Removed from Favorites.");
    } catch (error) {
        showReaderToast(error.message);
    }
}

async function toggleFinished() {
    finished = !finished;

    try {
        await apiJson("/pastor-resources/api/completed/" + BOOK_ID, {
            method:"POST",
            headers:{"Content-Type":"application/json"},
            body:JSON.stringify({completed:finished})
        });

        const label = document.getElementById("finishButtonLabel");
        if (label) label.textContent = finished ? "Reopen" : "Mark Finished";

        if (finished) setProgress(100);
        showReaderToast(finished ? "Book marked as completed." : "Book reopened.");
    } catch (error) {
        finished = !finished;
        showReaderToast(error.message);
    }
}

function switchFormat(format) {
    window.location.href = READ_BASE_URL + "?format=" + encodeURIComponent(format);
}

function setReaderTheme(theme) {
    currentTheme = theme;
    const root = document.getElementById("readerRoot");
    root.classList.remove("theme-light","theme-sepia","theme-dark");
    root.classList.add("theme-" + theme);
    applyEpubTheme();
    if (READER_FORMAT === "EPUB") {
        scheduleEpubLayoutRefresh("Updating theme…");
    }
    saveState({theme});
}

function clearPendingSelection() {
    const sourceWindow = pendingSelection?.sourceWindow || null;
    pendingSelection = null;
    hideSelectionBarOnly();

    try { window.getSelection()?.removeAllRanges(); } catch (error) {}

    if (sourceWindow && sourceWindow !== window) {
        try { sourceWindow.getSelection()?.removeAllRanges(); } catch (error) {}
    }
}

async function copySelectedText() {
    if (!pendingSelection || !pendingSelection.text) return;

    const text = pendingSelection.text;

    try {
        if (navigator.clipboard?.writeText) {
            await navigator.clipboard.writeText(text);
        } else {
            const helper = document.createElement("textarea");
            helper.value = text;
            helper.setAttribute("readonly", "");
            helper.style.position = "fixed";
            helper.style.opacity = "0";
            document.body.appendChild(helper);
            helper.select();
            document.execCommand("copy");
            helper.remove();
        }

        showReaderToast("Quote copied.");
    } catch (error) {
        showReaderToast("Unable to copy quote.");
    }
}

async function savePendingAnnotation(type,color,needsNote,isSermon) {
    if (!pendingSelection) return;

    let note = "";
    let tags = "";

    if (needsNote) {
        note = window.prompt(isSermon ? "Sermon note / how you plan to use this passage:" : "Add your note:", "") || "";
    }

    if (isSermon) {
        tags = window.prompt("Tags (optional, e.g. Leadership, Illustration, Grace):", "") || "";
    }

    try {
        const data = await apiJson("/pastor-resources/api/annotations", {
            method:"POST",
            headers:{"Content-Type":"application/json"},
            body:JSON.stringify({
                book_id:BOOK_ID,
                format:READER_FORMAT,
                annotation_type:type,
                selected_text:pendingSelection.text,
                locator:pendingSelection.locator,
                page:pendingSelection.page || null,
                color,
                note,
                tags,
                is_sermon_note:isSermon
            })
        });

        annotations.unshift(data.annotation);
        renderSidePanel();
        applyAnnotation(data.annotation);
        clearPendingSelection();
        showReaderToast(isSermon ? "Saved to Sermon Notes." : "Annotation saved.");
    } catch (error) {
        showReaderToast(error.message);
    }
}

async function editAnnotation(id) {
    const item = annotations.find(a => Number(a.id) === Number(id));
    if (!item) return;
    const note = window.prompt("Edit note:", item.note || "");
    if (note === null) return;
    const tags = window.prompt("Tags:", item.tags || "");
    if (tags === null) return;

    try {
        await apiJson("/pastor-resources/api/annotations/" + id, {
            method:"PATCH",
            headers:{"Content-Type":"application/json"},
            body:JSON.stringify({note,tags,is_sermon_note:Boolean(item.is_sermon_note)})
        });
        await loadNotesData();
        showReaderToast("Note updated.");
    } catch (error) {
        showReaderToast(error.message);
    }
}

async function deleteReaderAnnotation(id) {
    if (!confirm("Delete this highlight/note?")) return;

    const item = annotations.find(a => Number(a.id) === Number(id)) || null;

    try {
        await apiJson("/pastor-resources/api/annotations/" + id, {method:"DELETE"});

        if (READER_FORMAT === "EPUB" && item) {
            removeEpubAnnotationVisual(item);
        }

        annotations = annotations.filter(a => Number(a.id) !== Number(id));
        renderSidePanel();

        if (READER_FORMAT === "PDF") {
            renderPdfAnnotations();
        } else {
            await rebuildVisibleEpubAnnotations();
            showReaderToast("Annotation deleted.");
        }
    } catch (error) {
        showReaderToast(error.message);
    }
}

async function deleteReaderBookmark(id) {
    if (!confirm("Delete this bookmark?")) return;
    try {
        await apiJson("/pastor-resources/api/bookmarks/" + id, {method:"DELETE"});
        bookmarks = bookmarks.filter(b => Number(b.id) !== Number(id));
        renderSidePanel();
    } catch (error) {
        showReaderToast(error.message);
    }
}

async function addCurrentBookmark() {
    let locator = "";
    let page = null;
    if (READER_FORMAT === "PDF") page = pdfPageNumber;
    else locator = currentEpubCfi || "";

    const defaultLabel = READER_FORMAT === "PDF" ? "Page " + page : "Saved passage";
    const label = window.prompt("Bookmark label:", defaultLabel);
    if (label === null) return;

    try {
        const data = await apiJson("/pastor-resources/api/bookmarks", {
            method:"POST",
            headers:{"Content-Type":"application/json"},
            body:JSON.stringify({book_id:BOOK_ID,format:READER_FORMAT,locator,page,label,excerpt:""})
        });
        bookmarks.unshift(data.bookmark);
        renderSidePanel();
        showReaderToast("Bookmark saved.");
    } catch (error) {
        showReaderToast(error.message);
    }
}

/* =====================================================
   REAL ACTIVE READING SESSION
   ===================================================== */

async function startReadingTimer() {
    try {
        const data = await apiJson("/pastor-resources/api/session/start/" + BOOK_ID, {
            method:"POST",
            headers:{"Content-Type":"application/json"},
            body:JSON.stringify({format:READER_FORMAT})
        });
        readingSessionId = data.session_id || "";
    } catch (error) {
        console.warn(error);
    }
}

["mousemove","mousedown","keydown","touchstart","wheel","scroll","click"].forEach(eventName => {
    window.addEventListener(eventName, () => { lastActivityAt = Date.now(); }, {passive:true});
});

setInterval(async () => {
    if (!readingSessionId) return;
    const active = document.visibilityState === "visible" && (Date.now() - lastActivityAt) < 60000;
    if (!active) return;
    try {
        await apiJson("/pastor-resources/api/session/ping", {
            method:"POST",
            headers:{"Content-Type":"application/json"},
            body:JSON.stringify({session_id:readingSessionId,active_seconds:15})
        });
    } catch (error) {
        console.warn(error);
    }
}, 15000);

window.addEventListener("beforeunload", () => {
    if (!readingSessionId) return;
    try {
        const blob = new Blob([JSON.stringify({session_id:readingSessionId})], {type:"application/json"});
        navigator.sendBeacon("/pastor-resources/api/session/end", blob);
    } catch (error) {}
});

/* =====================================================
   PDF READER
   ===================================================== */

let pdfDoc = null;
let pdfPageNumber = Math.max(1, Number(STATE.pdf_page || 1));
let pdfScale = Math.max(.5, Number(STATE.pdf_scale || 1.15));
let pdfRenderTask = null;

async function initPdfReader() {
    if (!window.pdfjsLib) {
        showReaderLoadError("PDF.js could not be loaded. Check the internet connection used to load the reader library.");
        return;
    }

    pdfjsLib.GlobalWorkerOptions.workerSrc = "https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.worker.min.js";

    try {
        showReaderLoading("Loading PDF…", "Connecting to the private ebook file…", null);

        const loadingTask = pdfjsLib.getDocument({url:MEDIA_URL,rangeChunkSize:65536});
        loadingTask.onProgress = progress => {
            const loaded = Number(progress?.loaded || 0);
            const total = Number(progress?.total || 0);
            const pct = total > 0 ? (loaded / total) * 100 : null;
            showReaderLoading("Loading PDF…", "Downloading the PDF data needed to open the book…", pct, loaded, total);
        };

        pdfDoc = await loadingTask.promise;
        pdfPageNumber = Math.min(pdfPageNumber,pdfDoc.numPages);

        const jumpAnn = annotations.find(a => Number(a.id) === Number(JUMP_ANNOTATION_ID));
        const jumpBm = bookmarks.find(b => Number(b.id) === Number(JUMP_BOOKMARK_ID));
        if (jumpAnn && jumpAnn.page) pdfPageNumber = Number(jumpAnn.page);
        else if (jumpBm && jumpBm.page) pdfPageNumber = Number(jumpBm.page);

        showReaderLoading("Preparing PDF…", "Rendering page " + pdfPageNumber + " of " + pdfDoc.numPages + "…", null);
        await renderPdfPage();
        hideReaderLoading();
    } catch (error) {
        console.error(error);
        showReaderLoadError(error?.message || "Unable to open this PDF.");
    }
}

async function renderPdfPage() {
    if (!pdfDoc) return;

    setPageBusy(true, "Loading page " + pdfPageNumber + "…");

    try {
        pdfPageNumber = Math.max(1,Math.min(pdfDoc.numPages,pdfPageNumber));
        const page = await pdfDoc.getPage(pdfPageNumber);
        const viewport = page.getViewport({scale:pdfScale});
        const canvas = document.getElementById("pdfCanvas");
        const stage = document.getElementById("pdfStage");
        const textLayer = document.getElementById("pdfTextLayer");
        const linkLayer = document.getElementById("pdfLinkLayer");
        const dpr = window.devicePixelRatio || 1;

        stage.style.width = viewport.width + "px";
        stage.style.height = viewport.height + "px";
        canvas.style.width = viewport.width + "px";
        canvas.style.height = viewport.height + "px";
        canvas.width = Math.floor(viewport.width * dpr);
        canvas.height = Math.floor(viewport.height * dpr);

        [textLayer, linkLayer].forEach(layer => {
            if (!layer) return;
            layer.style.width = viewport.width + "px";
            layer.style.height = viewport.height + "px";
        });

        const context = canvas.getContext("2d");
        if (pdfRenderTask) {
            try { pdfRenderTask.cancel(); } catch(e) {}
        }
        pdfRenderTask = page.render({
            canvasContext:context,
            viewport,
            transform:dpr !== 1 ? [dpr,0,0,dpr,0,0] : null
        });
        await pdfRenderTask.promise;

        textLayer.innerHTML = "";
        textLayer.style.setProperty("--scale-factor",pdfScale);
        const textContent = await page.getTextContent();
        const textTask = pdfjsLib.renderTextLayer({textContentSource:textContent,container:textLayer,viewport,textDivs:[]});
        if (textTask && textTask.promise) await textTask.promise;

        await renderPdfLinkLayer(page, viewport);

        updateReaderPageControls(
            pdfPageNumber,
            pdfDoc.numPages,
            "Page",
            true
        );

        const percent = (pdfPageNumber / pdfDoc.numPages) * 100;
        setProgress(percent);
        saveState({last_format:"PDF",pdf_page:pdfPageNumber,pdf_scale:pdfScale,progress_percent:percent});
        renderPdfAnnotations();
    } catch (error) {
        if (String(error?.name || "") !== "RenderingCancelledException") {
            showReaderToast("Unable to render this page: " + (error?.message || error));
        }
    } finally {
        setPageBusy(false);
    }
}

async function renderPdfLinkLayer(page, viewport) {
    const layer = document.getElementById("pdfLinkLayer");
    if (!layer) return;
    layer.innerHTML = "";

    let annotations = [];
    try {
        annotations = await page.getAnnotations({intent:"display"});
    } catch (error) {
        console.warn("PDF links could not be read", error);
        return;
    }

    for (const annotation of annotations) {
        if (annotation.subtype !== "Link" || !annotation.rect) continue;
        if (!annotation.url && !annotation.dest && !annotation.action) continue;

        const rect = viewport.convertToViewportRectangle(annotation.rect);
        const left = Math.min(rect[0], rect[2]);
        const top = Math.min(rect[1], rect[3]);
        const width = Math.abs(rect[0] - rect[2]);
        const height = Math.abs(rect[1] - rect[3]);

        if (width < 2 || height < 2) continue;

        const hit = document.createElement("button");
        hit.type = "button";
        hit.className = "pdf-link-hit";
        hit.style.left = left + "px";
        hit.style.top = top + "px";
        hit.style.width = width + "px";
        hit.style.height = height + "px";
        hit.title = annotation.url ? "Open link" : "Go to linked page";
        hit.setAttribute("aria-label", hit.title);
        hit.addEventListener("click", event => {
            event.preventDefault();
            event.stopPropagation();
            followPdfLink(annotation);
        });
        layer.appendChild(hit);
    }
}

async function followPdfLink(annotation) {
    try {
        if (annotation.url) {
            const url = String(annotation.url || "");
            if (/^(https?:|mailto:)/i.test(url)) {
                window.open(url, "_blank", "noopener,noreferrer");
            }
            return;
        }

        if (annotation.dest) {
            await goToPdfDestination(annotation.dest);
            return;
        }

        const action = String(annotation.action || "");
        if (action === "NextPage") return goNext();
        if (action === "PrevPage") return goPrevious();
        if (action === "FirstPage") {
            pdfPageNumber = 1;
            return renderPdfPage();
        }
        if (action === "LastPage" && pdfDoc) {
            pdfPageNumber = pdfDoc.numPages;
            return renderPdfPage();
        }
    } catch (error) {
        showReaderToast("This PDF link could not be opened.");
        console.warn(error);
    }
}

async function goToPdfDestination(destination) {
    if (!pdfDoc) return;

    let explicit = destination;
    if (typeof destination === "string") {
        explicit = await pdfDoc.getDestination(destination);
    }

    if (!Array.isArray(explicit) || !explicit.length) return;

    const target = explicit[0];
    let pageIndex = null;

    if (typeof target === "number") {
        pageIndex = target;
    } else if (target && typeof target === "object") {
        pageIndex = await pdfDoc.getPageIndex(target);
    }

    if (pageIndex === null || pageIndex === undefined) return;
    pdfPageNumber = Math.max(1, Math.min(pdfDoc.numPages, Number(pageIndex) + 1));
    await renderPdfPage();
}

function capturePdfSelection() {
    const selection = window.getSelection();

    if (!selection || selection.isCollapsed || !selection.rangeCount) {
        if (!selectionToolbarInteracting) {
            pendingSelection = null;
            hideSelectionBarOnly();
        }
        return;
    }

    const text = selection.toString().trim();
    if (!text) {
        if (!selectionToolbarInteracting) {
            pendingSelection = null;
            hideSelectionBarOnly();
        }
        return;
    }

    const stage = document.getElementById("pdfStage");
    const range = selection.getRangeAt(0);

    if (!stage || !stage.contains(range.commonAncestorContainer)) {
        if (!selectionToolbarInteracting) {
            pendingSelection = null;
            hideSelectionBarOnly();
        }
        return;
    }

    const stageRect = stage.getBoundingClientRect();

    const rects = Array.from(range.getClientRects())
        .filter(rect => rect.width > 1 && rect.height > 1)
        .map(rect => ({
            x:(rect.left-stageRect.left)/stageRect.width,
            y:(rect.top-stageRect.top)/stageRect.height,
            w:rect.width/stageRect.width,
            h:rect.height/stageRect.height
        }));

    if (!rects.length) return;

    pendingSelection = {
        text,
        locator:JSON.stringify({rects}),
        page:pdfPageNumber,
        sourceWindow:window
    };

    const selectionRect = selectionViewportRect(range, window);
    if (selectionRect) {
        showSelectionBarAtRect(selectionRect);
    }
}

function renderPdfAnnotations() {
    const layer = document.getElementById("pdfAnnotationLayer");
    if (!layer) return;
    layer.innerHTML = "";

    annotations.filter(a => String(a.format).toUpperCase() === "PDF" && Number(a.page) === Number(pdfPageNumber)).forEach(item => {
        let locator;
        try { locator = JSON.parse(item.locator || "{}"); } catch(e) { locator={}; }
        (locator.rects || []).forEach(rect => {
            const el = document.createElement("div");
            el.className = "pdf-annotation";
            el.style.left = (rect.x*100) + "%";
            el.style.top = (rect.y*100) + "%";
            el.style.width = (rect.w*100) + "%";
            if (item.annotation_type === "underline") {
                el.style.height = "2px";
                el.style.top = ((rect.y+rect.h)*100 - .35) + "%";
                el.style.background = item.color || "#e5962d";
            } else {
                el.style.height = (rect.h*100) + "%";
                el.style.background = item.color || "#ffe66d";
                el.style.opacity = ".42";
            }
            el.title = item.note || item.selected_text || "Annotation";
            el.onclick = () => item.note && showReaderToast(item.note);
            layer.appendChild(el);
        });
    });
}

async function zoomPdf(delta) {
    pdfScale = Math.min(4,Math.max(.5,pdfScale + delta));
    await renderPdfPage();
}

async function fitPdfWidth() {
    if (!pdfDoc) return;
    const page = await pdfDoc.getPage(pdfPageNumber);
    const base = page.getViewport({scale:1});
    const available = Math.max(280,document.getElementById("readerCanvasArea").clientWidth - 24);
    pdfScale = Math.min(4,Math.max(.5,available/base.width));
    await renderPdfPage();
}

async function fitPdfPage() {
    if (!pdfDoc) return;
    const page = await pdfDoc.getPage(pdfPageNumber);
    const base = page.getViewport({scale:1});
    const area = document.getElementById("readerCanvasArea");
    const widthScale = Math.max(.5,(area.clientWidth-24)/base.width);
    const heightScale = Math.max(.5,(area.clientHeight-24)/base.height);
    pdfScale = Math.min(4,widthScale,heightScale);
    await renderPdfPage();
}

function jumpReaderPage() {
    const input = document.getElementById("readerPageInput");
    if (!input) return;

    const requested = Math.max(1, Math.round(Number(input.value || 1)));

    if (READER_FORMAT === "PDF") {
        if (!pdfDoc) return;
        pdfPageNumber = Math.max(1, Math.min(pdfDoc.numPages, requested));
        input.value = String(pdfPageNumber);
        renderPdfPage();
        return;
    }

    if (!epubBook || !rendition || !epubLocationsReady || epubLocationTotal <= 0) {
        showReaderToast("EPUB locations are still being prepared.");
        updateReaderPageControls(epubLocationCurrent, epubLocationTotal, "Location", false);
        return;
    }

    const target = Math.max(1, Math.min(epubLocationTotal, requested));
    const cfi = epubBook.locations.cfiFromLocation(target - 1);

    if (!cfi) {
        showReaderToast("That EPUB location could not be opened.");
        return;
    }

    input.value = String(target);
    epubNavigationQueue = [];
    setPageBusy(true, "Opening location " + target + "…");

    Promise.resolve(rendition.display(cfi))
        .catch(error => {
            console.warn(error);
            showReaderToast("Unable to open that EPUB location.");
        })
        .finally(() => {
            if (!epubLayoutRefreshing) setPageBusy(false);
        });
}

/* =====================================================
   EPUB READER
   ===================================================== */

let epubBook = null;
let rendition = null;
let currentEpubCfi = STATE.epub_cfi || "";
let epubFontSize = Number(STATE.epub_font_size || 100);
let epubLineHeight = Number(STATE.epub_line_height || 1.6);
let epubFontFamily = STATE.epub_font_family || "Georgia, serif";
let epubLocationsReady = false;
let appliedEpubAnnotationIds = new Set();
let epubLayoutRefreshTimer = null;
let epubLayoutRefreshToken = 0;
let epubLayoutRefreshing = false;
let epubNavigationQueue = [];
let epubNavigationRunning = false;

function captureEpubSelection(contents, forcedCfi="") {
    if (!contents?.window || !contents?.document) return;

    const selection = contents.window.getSelection?.();

    if (!selection || selection.isCollapsed || !selection.rangeCount) {
        if (!selectionToolbarInteracting) {
            pendingSelection = null;
            hideSelectionBarOnly();
        }
        return;
    }

    const text = String(selection.toString() || "").trim();
    if (!text) {
        if (!selectionToolbarInteracting) {
            pendingSelection = null;
            hideSelectionBarOnly();
        }
        return;
    }

    const range = selection.getRangeAt(0);

    let locator = String(forcedCfi || "").trim();

    if (!locator) {
        try {
            locator = String(contents.cfiFromRange?.(range) || "").trim();
        } catch (error) {}
    }

    if (!locator) return;

    pendingSelection = {
        text,
        locator,
        page:null,
        sourceWindow:contents.window
    };

    const selectionRect = selectionViewportRect(
        range,
        contents.window
    );

    if (selectionRect) {
        showSelectionBarAtRect(selectionRect);
    }
}

function installEpubContentHandlers(contents) {
    if (!contents?.document || !contents?.window) return;

    lastEpubContents = contents;

    const doc = contents.document;
    const win = contents.window;
    const target = doc.body || doc.documentElement || doc;

    try {
        if (doc.documentElement) {
            doc.documentElement.style.touchAction = "pan-y pinch-zoom";
            doc.documentElement.style.overscrollBehaviorX = "contain";
            doc.documentElement.style.webkitUserSelect = "text";
            doc.documentElement.style.userSelect = "text";
        }

        if (doc.body) {
            doc.body.style.touchAction = "pan-y pinch-zoom";
            doc.body.style.overscrollBehaviorX = "contain";
            doc.body.style.webkitUserSelect = "text";
            doc.body.style.userSelect = "text";
        }
    } catch (error) {}

    installEpubSwipeHandlers(doc, win);

    if (EPUB_CONTENT_HANDLERS.has(doc)) return;
    EPUB_CONTENT_HANDLERS.add(doc);

    let localSelectionTimer = null;

    const scheduleCapture = delay => {
        clearTimeout(localSelectionTimer);
        localSelectionTimer = setTimeout(() => {
            if (selectionToolbarInteracting) return;
            captureEpubSelection(contents);
        }, delay);
    };

    doc.addEventListener(
        "selectionchange",
        () => scheduleCapture(150),
        {passive:true}
    );

    target.addEventListener(
        "touchend",
        () => scheduleCapture(180),
        {passive:true,capture:true}
    );

    target.addEventListener(
        "mouseup",
        () => scheduleCapture(0),
        {passive:true,capture:true}
    );
}

async function initEpubReader() {
    if (!window.ePub) {
        showReaderLoadError("EPUB.js could not be loaded. Check the internet connection used to load the reader library.");
        return;
    }

    try {
        updateReaderPageControls(1, 0, "Location", false);
        const epubData = await fetchArrayBufferWithProgress(MEDIA_URL, "EPUB");

        showReaderLoading("Opening EPUB…", "Reading the table of contents and preparing the pages…", null, epubData.byteLength, epubData.byteLength);

        // EPUB.js is given the actual binary EPUB data instead of the
        // private /media/<id> URL. This avoids the no-.epub-extension
        // URL problem and works reliably with authenticated Drive files.
        epubBook = ePub(epubData);
        rendition = epubBook.renderTo("epubViewer",{width:"100%",height:"100%",spread:"none",flow:"paginated"});

        rendition.themes.register("light",{body:{background:"#ffffff",color:"#17233c"},a:{color:"#3567b5"}});
        rendition.themes.register("sepia",{body:{background:"#fbf4e5",color:"#4b3b29"},a:{color:"#805a31"}});
        rendition.themes.register("dark",{body:{background:"#242a36",color:"#edf1f7"},a:{color:"#9ec4ff"}});
        applyEpubTheme();

        document.getElementById("fontFamilySelect").value = epubFontFamily;
        document.getElementById("lineHeightSelect").value = String(epubLineHeight);

        // Mobile gestures and text selection must be installed inside
        // EPUB.js's iframe. This keeps native iPhone text selection while
        // also giving the EPUB its own reliable left/right swipe handler.
        if (rendition.hooks?.content?.register) {
            rendition.hooks.content.register(contents => {
                try {
                    installEpubContentHandlers(contents);
                } catch (error) {
                    console.warn(error);
                }
            });
        }

        rendition.on("rendered",(section,view) => {
            try {
                if (view?.contents) {
                    installEpubContentHandlers(view.contents);
                } else if (view?.document) {
                    installEpubSwipeHandlers(
                        view.document,
                        view.window || view.document.defaultView || window
                    );
                }
            } catch (error) {
                console.warn(error);
            }
        });

        const navigation = await epubBook.loaded.navigation;
        const toc = document.getElementById("tocSelect");
        toc.innerHTML = '<option value="">Table of Contents</option>';
        (navigation.toc || []).forEach(item => {
            const option = document.createElement("option");
            option.value = item.href;
            option.textContent = item.label || item.href;
            toc.appendChild(option);
        });

        rendition.on("selected",(cfiRange,contents) => {
            try {
                installEpubContentHandlers(contents);
                captureEpubSelection(contents, cfiRange);
            } catch (error) {
                console.warn(error);
            }
        });

        rendition.on("relocated",location => {
            if (!epubNavigationRunning && !epubLayoutRefreshing) {
                setPageBusy(false);
            }
            currentEpubCfi = location.start.cfi;

            let percent = Number(location.start.percentage || 0) * 100;

            if (epubLocationsReady && currentEpubCfi) {
                try {
                    percent = epubBook.locations.percentageFromCfi(currentEpubCfi) * 100;
                } catch (error) {}
            }

            setProgress(percent);

            if (epubLocationsReady && currentEpubCfi && epubLocationTotal > 0) {
                try {
                    const locationIndex = Number(
                        epubBook.locations.locationFromCfi(currentEpubCfi)
                    );
                    if (Number.isFinite(locationIndex) && locationIndex >= 0) {
                        epubLocationCurrent = Math.min(
                            epubLocationTotal,
                            locationIndex + 1
                        );
                    }
                } catch (error) {}

                updateReaderPageControls(
                    epubLocationCurrent,
                    epubLocationTotal,
                    "Location",
                    true
                );
            } else {
                const displayed = location?.start?.displayed || null;
                updateReaderPageControls(
                    Number(displayed?.page || 1),
                    Number(displayed?.total || 0),
                    "Location",
                    false
                );
            }

            saveState({
                last_format:"EPUB",
                epub_cfi:currentEpubCfi,
                progress_percent:percent,
                epub_font_size:epubFontSize,
                epub_font_family:epubFontFamily,
                epub_line_height:epubLineHeight
            });
        });

        let initialLocation = currentEpubCfi || undefined;
        const jumpAnn = annotations.find(a => Number(a.id) === Number(JUMP_ANNOTATION_ID));
        const jumpBm = bookmarks.find(b => Number(b.id) === Number(JUMP_BOOKMARK_ID));
        if (jumpAnn && jumpAnn.locator) initialLocation = jumpAnn.locator;
        else if (jumpBm && jumpBm.locator) initialLocation = jumpBm.locator;

        setPageBusy(true,"Preparing first page…");
        await rendition.display(initialLocation);
        applyAllEpubAnnotations();
        hideReaderLoading();
        setPageBusy(false);

        epubBook.ready.then(async () => {
            try {
                const generatedLocations = await epubBook.locations.generate(1600);
                epubLocationsReady = true;

                let totalLocations = 0;
                if (Array.isArray(generatedLocations)) {
                    totalLocations = generatedLocations.length;
                }
                if (!totalLocations) {
                    try { totalLocations = Number(epubBook.locations.length?.() || 0); } catch (error) {}
                }
                if (!totalLocations) {
                    totalLocations = Number(epubBook.locations.total || 0);
                }

                epubLocationTotal = Math.max(1, Math.round(totalLocations || 1));

                if (currentEpubCfi) {
                    const pct = epubBook.locations.percentageFromCfi(currentEpubCfi) * 100;
                    setProgress(pct);

                    try {
                        const locationIndex = Number(
                            epubBook.locations.locationFromCfi(currentEpubCfi)
                        );
                        if (Number.isFinite(locationIndex) && locationIndex >= 0) {
                            epubLocationCurrent = Math.min(
                                epubLocationTotal,
                                locationIndex + 1
                            );
                        }
                    } catch (error) {}

                    saveState({progress_percent:pct});
                }

                updateReaderPageControls(
                    epubLocationCurrent,
                    epubLocationTotal,
                    "Location",
                    true
                );
            } catch(e) { console.warn(e); }
        });

    } catch (error) {
        console.error(error);
        setPageBusy(false);
        showReaderLoadError(error?.message || "Unable to open this EPUB.");
    }
}

function applyEpubTheme() {
    if (!rendition) return;
    rendition.themes.select(currentTheme || "light");
    rendition.themes.fontSize(epubFontSize + "%");
    rendition.themes.font(epubFontFamily);
    rendition.themes.override("line-height",String(epubLineHeight),true);
}

function changeEpubFont(delta) {
    epubFontSize = Math.min(220,Math.max(70,epubFontSize + delta));
    applyEpubTheme();
    scheduleEpubLayoutRefresh("Updating text size…");
    saveState({epub_font_size:epubFontSize});
    showReaderToast("Font size: " + epubFontSize + "%");
}

function setEpubFontFamily(value) {
    epubFontFamily = value;
    applyEpubTheme();
    scheduleEpubLayoutRefresh("Updating font…");
    saveState({epub_font_family:value});
}

function setEpubLineHeight(value) {
    epubLineHeight = Number(value || 1.6);
    applyEpubTheme();
    scheduleEpubLayoutRefresh("Updating line spacing…");
    saveState({epub_line_height:epubLineHeight});
}

function jumpToc(value) {
    if (rendition && value) {
        epubNavigationQueue = [];
        setPageBusy(true,"Opening section…");
        Promise.resolve(rendition.display(value))
            .catch(error => console.warn(error))
            .finally(() => setPageBusy(false));
    }
}

function epubAnnotationKind(item) {
    return item?.annotation_type === "underline" ? "underline" : "highlight";
}

function removeEpubAnnotationVisual(item) {
    if (!item) return;

    const id = Number(item.id);
    const locator = String(item.locator || "");
    const kind = epubAnnotationKind(item);

    if (rendition && locator) {
        try {
            rendition.annotations.remove(locator, kind);
        } catch (error) {
            console.warn("EPUB annotation remove failed", error);
        }
    }

    // Extra DOM cleanup for WebKit/iPhone. EPUB.js uses the class name
    // supplied when the annotation was created; removing it here prevents
    // a stale SVG mark from remaining on screen after Delete or reflow.
    try {
        const selectors = [".pr-hl-" + id, ".pr-ul-" + id].join(",");
        const contents = rendition?.getContents?.() || [];
        contents.forEach(content => {
            content?.document?.querySelectorAll?.(selectors)?.forEach?.(node => node.remove());
        });
    } catch (error) {}

    appliedEpubAnnotationIds.delete(id);
}

function clearVisibleEpubAnnotationVisuals(items=annotations) {
    (items || [])
        .filter(item => String(item.format || "").toUpperCase() === "EPUB")
        .forEach(removeEpubAnnotationVisual);
    appliedEpubAnnotationIds.clear();
}

function nextAnimationFrame() {
    return new Promise(resolve => requestAnimationFrame(() => resolve()));
}

async function rebuildVisibleEpubAnnotations() {
    if (READER_FORMAT !== "EPUB" || !rendition) return;

    const currentItems = annotations.filter(
        item => String(item.format || "").toUpperCase() === "EPUB"
    );

    clearVisibleEpubAnnotationVisuals(currentItems);
    await nextAnimationFrame();
    applyAllEpubAnnotations();
}

function scheduleEpubLayoutRefresh(message="Updating page…") {
    if (READER_FORMAT !== "EPUB" || !rendition) return;

    clearTimeout(epubLayoutRefreshTimer);
    epubLayoutRefreshTimer = setTimeout(() => {
        refreshEpubLayoutAndAnnotations(message);
    }, 150);
}

async function refreshEpubLayoutAndAnnotations(message="Updating page…") {
    if (READER_FORMAT !== "EPUB" || !rendition) return;

    const token = ++epubLayoutRefreshToken;
    const anchor = currentEpubCfi || "";
    const oldItems = annotations.filter(
        item => String(item.format || "").toUpperCase() === "EPUB"
    );

    epubLayoutRefreshing = true;
    setPageBusy(true,message);

    try {
        clearVisibleEpubAnnotationVisuals(oldItems);

        try { rendition.resize?.(); } catch (error) {}

        // WebKit needs a moment to finish the EPUB reflow before the SVG
        // annotation geometry is rebuilt. Keeping the CFI anchor preserves
        // the reader's logical position even though line breaks changed.
        await new Promise(resolve => setTimeout(resolve,140));
        if (token !== epubLayoutRefreshToken) return;

        if (anchor) {
            try { await Promise.resolve(rendition.display(anchor)); } catch (error) { console.warn(error); }
        }

        await nextAnimationFrame();
        await nextAnimationFrame();
        if (token !== epubLayoutRefreshToken) return;

        appliedEpubAnnotationIds.clear();
        applyAllEpubAnnotations();
    } finally {
        if (token === epubLayoutRefreshToken) {
            epubLayoutRefreshing = false;
            setPageBusy(false);
        }
    }
}

function applyAnnotation(item) {
    if (READER_FORMAT === "PDF") {
        renderPdfAnnotations();
        return;
    }
    if (!rendition || !item.locator || appliedEpubAnnotationIds.has(Number(item.id))) return;

    const callback = () => {
        if (item.note) showReaderToast(item.note);
    };

    try {
        if (item.annotation_type === "underline") {
            rendition.annotations.underline(item.locator,{annotationId:item.id},callback,"pr-ul-"+item.id,{"stroke":item.color || "#e5962d","stroke-opacity":"1"});
        } else {
            rendition.annotations.highlight(item.locator,{annotationId:item.id},callback,"pr-hl-"+item.id,{"fill":item.color || "#ffe66d","fill-opacity":"0.38","mix-blend-mode":"multiply"});
        }
        appliedEpubAnnotationIds.add(Number(item.id));
    } catch (error) {
        console.warn(error);
    }
}

function applyAllEpubAnnotations() {
    annotations.filter(a => String(a.format).toUpperCase() === "EPUB").forEach(applyAnnotation);
}

/* =====================================================
   SWIPE NAVIGATION
   ===================================================== */

const SWIPE_INSTALLED = new WeakSet();

function selectionIsActive(win) {
    try {
        const selection = win?.getSelection?.();
        return Boolean(selection && !selection.isCollapsed && String(selection.toString() || "").trim());
    } catch (error) {
        return false;
    }
}

function installEpubSwipeHandlers(doc, win=window) {
    if (!doc || SWIPE_INSTALLED.has(doc)) return;
    SWIPE_INSTALLED.add(doc);

    let startX = 0;
    let startY = 0;
    let startTime = 0;
    let tracking = false;
    let horizontalGesture = false;

    const reset = () => {
        tracking = false;
        horizontalGesture = false;
    };

    doc.addEventListener("touchstart", event => {
        reset();
        if (!event.touches || event.touches.length !== 1) return;
        if (selectionIsActive(win)) return;

        const touch = event.touches[0];
        const viewportWidth = Number(win?.innerWidth || window.innerWidth || 0);

        // Preserve Safari's system-level edge navigation gesture.
        if (
            viewportWidth > 0
            && (touch.clientX < 24 || touch.clientX > viewportWidth - 24)
        ) {
            return;
        }

        startX = touch.clientX;
        startY = touch.clientY;
        startTime = Date.now();
        tracking = true;
    }, {passive:true,capture:true});

    doc.addEventListener("touchmove", event => {
        if (!tracking || !event.touches || event.touches.length !== 1) return;
        if (selectionIsActive(win)) {
            reset();
            return;
        }

        const touch = event.touches[0];
        const dx = touch.clientX - startX;
        const dy = touch.clientY - startY;

        if (!horizontalGesture) {
            if (Math.abs(dx) < 12 && Math.abs(dy) < 12) return;

            // A vertical gesture belongs to the document/native browser.
            if (Math.abs(dy) >= Math.abs(dx) * .85) {
                reset();
                return;
            }

            if (Math.abs(dx) >= 18) {
                horizontalGesture = true;
            }
        }

        // Once the gesture is clearly horizontal, prevent WebKit's iframe
        // from turning it into a scroll/overscroll gesture before touchend.
        if (horizontalGesture && event.cancelable) {
            event.preventDefault();
        }
    }, {passive:false,capture:true});

    doc.addEventListener("touchcancel", reset, {passive:true,capture:true});

    doc.addEventListener("touchend", event => {
        if (!tracking) return;
        const wasHorizontal = horizontalGesture;
        tracking = false;
        horizontalGesture = false;

        if (!event.changedTouches || event.changedTouches.length !== 1) return;
        if (selectionIsActive(win)) return;

        const touch = event.changedTouches[0];
        const dx = touch.clientX - startX;
        const dy = touch.clientY - startY;
        const elapsed = Date.now() - startTime;

        if (elapsed > 1000) return;
        if (!wasHorizontal && Math.abs(dx) < 48) return;
        if (Math.abs(dx) < 48) return;
        if (Math.abs(dx) < Math.abs(dy) * 1.12) return;

        hideSelectionBarOnly();

        if (dx < 0) queueEpubNavigation(1);
        else queueEpubNavigation(-1);
    }, {passive:true,capture:true});
}

function installSwipeHandlers(target, win=window) {
    if (!target || SWIPE_INSTALLED.has(target)) return;
    SWIPE_INSTALLED.add(target);

    let startX = 0;
    let startY = 0;
    let startTime = 0;
    let tracking = false;

    target.addEventListener("touchstart", event => {
        tracking = false;

        if (!event.touches || event.touches.length !== 1) return;

        const touch = event.touches[0];
        const viewportWidth = Number(win?.innerWidth || window.innerWidth || 0);

        // Leave the extreme screen edges to iOS/Safari so its own
        // browser back/forward gestures are not hijacked.
        if (
            viewportWidth > 0
            && (
                touch.clientX < 24
                || touch.clientX > viewportWidth - 24
            )
        ) {
            return;
        }

        startX = touch.clientX;
        startY = touch.clientY;
        startTime = Date.now();
        tracking = true;
    }, {passive:true,capture:true});

    target.addEventListener("touchcancel", () => {
        tracking = false;
    }, {passive:true,capture:true});

    target.addEventListener("touchend", event => {
        if (!tracking) return;
        tracking = false;

        if (!event.changedTouches || event.changedTouches.length !== 1) return;
        if (selectionIsActive(win)) return;

        const touch = event.changedTouches[0];
        const dx = touch.clientX - startX;
        const dy = touch.clientY - startY;
        const elapsed = Date.now() - startTime;

        if (elapsed > 900) return;
        if (Math.abs(dx) < 56) return;
        if (Math.abs(dx) < Math.abs(dy) * 1.20) return;

        hideSelectionBarOnly();

        if (dx < 0) {
            goNext();
        } else {
            goPrevious();
        }
    }, {passive:true,capture:true});
}

/* =====================================================
   SHARED NAVIGATION / SEARCH
   ===================================================== */

function queueEpubNavigation(direction) {
    if (!rendition) return;

    const normalized = direction < 0 ? -1 : 1;

    // Keep short bursts of taps/swipes instead of silently dropping them,
    // but cap the queue so an accidental rapid gesture cannot race through
    // dozens of pages.
    if (epubNavigationQueue.length < 5) {
        epubNavigationQueue.push(normalized);
    }

    if (epubNavigationRunning) return;
    runEpubNavigationQueue();
}

async function runEpubNavigationQueue() {
    if (epubNavigationRunning || !rendition) return;

    epubNavigationRunning = true;

    try {
        while (epubNavigationQueue.length && rendition) {
            const direction = epubNavigationQueue.shift();
            setPageBusy(true, direction < 0 ? "Loading previous page…" : "Loading next page…");

            try {
                if (direction < 0) {
                    await Promise.resolve(rendition.prev());
                } else {
                    await Promise.resolve(rendition.next());
                }
            } catch (error) {
                console.warn("EPUB page turn failed", error);
                epubNavigationQueue = [];
                break;
            }

            // Give WebKit one frame to settle the paginated iframe before
            // executing another queued page turn.
            await nextAnimationFrame();
        }
    } finally {
        epubNavigationRunning = false;
        if (!epubLayoutRefreshing) setPageBusy(false);
    }
}

function goPrevious() {
    if (pendingSelection) clearPendingSelection();

    if (READER_FORMAT === "PDF") {
        if (pdfDoc && pdfPageNumber > 1) {
            pdfPageNumber--;
            renderPdfPage();
        }
    } else if (rendition) {
        queueEpubNavigation(-1);
    }
}

function goNext() {
    if (pendingSelection) clearPendingSelection();

    if (READER_FORMAT === "PDF") {
        if (pdfDoc && pdfPageNumber < pdfDoc.numPages) {
            pdfPageNumber++;
            renderPdfPage();
        }
    } else if (rendition) {
        queueEpubNavigation(1);
    }
}

function updateSearchResultUI() {
    const row = document.getElementById("readerSearchResults");
    const count = document.getElementById("readerSearchResultsText");
    const position = document.getElementById("readerSearchPosition");

    if (!row || !count || !position) return;

    if (!searchMatches.length) {
        row.style.display = "none";
        count.textContent = "0 results";
        position.textContent = "0 of 0";
        return;
    }

    row.style.display = "flex";
    count.textContent = searchMatches.length + (searchMatches.length === 1 ? " result" : " results");
    position.textContent = (searchMatchIndex + 1) + " of " + searchMatches.length;
}

async function openSearchMatch(index) {
    if (!searchMatches.length) return;

    searchMatchIndex = (index + searchMatches.length) % searchMatches.length;
    const match = searchMatches[searchMatchIndex];

    if (READER_FORMAT === "PDF") {
        pdfPageNumber = Number(match.page || 1);
        await renderPdfPage();
    } else if (rendition && match.cfi) {
        epubNavigationQueue = [];
        setPageBusy(true,"Opening search result…");
        try {
            await Promise.resolve(rendition.display(match.cfi));
        } finally {
            if (!epubLayoutRefreshing) setPageBusy(false);
        }
    }

    updateSearchResultUI();
}

function stepSearchMatch(delta) {
    if (!searchMatches.length) return;
    openSearchMatch(searchMatchIndex + Number(delta || 0));
}

async function findInBook() {
    const query = document.getElementById("readerSearchInput").value.trim();

    searchMatches = [];
    searchMatchIndex = -1;
    updateSearchResultUI();

    if (!query) return;

    showReaderToast("Searching book...");

    if (READER_FORMAT === "PDF") {
        if (!pdfDoc) return;

        const q = query.toLowerCase();

        for (let i=1;i<=pdfDoc.numPages;i++) {
            const page = await pdfDoc.getPage(i);
            const content = await page.getTextContent();
            const text = content.items.map(item => item.str || "").join(" ").toLowerCase();

            if (text.includes(q)) {
                searchMatches.push({page:i});
            }
        }

        if (searchMatches.length) {
            await openSearchMatch(0);
        } else {
            showReaderToast("No match found.");
            updateSearchResultUI();
        }

        return;
    }

    if (!epubBook || !rendition) return;

    try {
        for (const section of epubBook.spine.spineItems) {
            await section.load(epubBook.load.bind(epubBook));
            const found = section.find(query) || [];

            found.forEach(match => {
                searchMatches.push({
                    cfi:match.cfi,
                    excerpt:match.excerpt || ""
                });
            });

            section.unload();
        }

        if (searchMatches.length) {
            await openSearchMatch(0);
        } else {
            showReaderToast("No match found.");
            updateSearchResultUI();
        }
    } catch (error) {
        showReaderToast("Search failed: " + error.message);
    }
}

function jumpToAnnotation(id) {
    const item = annotations.find(a => Number(a.id) === Number(id));
    if (!item) return;
    toggleSidePanel(false);
    if (READER_FORMAT === "PDF" && item.page) {
        pdfPageNumber = Number(item.page);
        renderPdfPage();
    } else if (READER_FORMAT === "EPUB" && item.locator && rendition) {
        rendition.display(item.locator);
    }
}

function jumpToBookmark(id) {
    const item = bookmarks.find(b => Number(b.id) === Number(id));
    if (!item) return;
    toggleSidePanel(false);
    if (READER_FORMAT === "PDF" && item.page) {
        pdfPageNumber = Number(item.page);
        renderPdfPage();
    } else if (READER_FORMAT === "EPUB" && item.locator && rendition) {
        rendition.display(item.locator);
    }
}

/* =====================================================
   INITIALIZE
   ===================================================== */

document.getElementById("themeSelect").value = currentTheme;

(async function initReader() {
    const canvasArea = document.getElementById("readerCanvasArea");
    const selectionBar = document.getElementById("selectionBar");

    syncReaderViewport();

    if (READER_FORMAT === "PDF") {
        installSwipeHandlers(canvasArea, window);
    }

    document.addEventListener("keydown", event => {
        const target = event.target;
        const tag = String(target?.tagName || "").toLowerCase();

        if (
            tag === "input"
            || tag === "textarea"
            || tag === "select"
            || target?.isContentEditable
            || selectionIsActive(window)
        ) {
            return;
        }

        if (event.key === "ArrowLeft") {
            event.preventDefault();
            goPrevious();
        } else if (event.key === "ArrowRight") {
            event.preventDefault();
            goNext();
        }
    });

    if (selectionBar) {
        const holdSelectionTools = () => {
            selectionToolbarInteracting = true;
            setTimeout(() => {
                selectionToolbarInteracting = false;
            }, 550);
        };

        selectionBar.addEventListener("touchstart", holdSelectionTools, {passive:true});
        selectionBar.addEventListener("pointerdown", holdSelectionTools, {passive:true});
        selectionBar.addEventListener("mousedown", holdSelectionTools, {passive:true});
    }

    await loadNotesData();
    await startReadingTimer();

    if (READER_FORMAT === "PDF") {
        const textLayer = document.getElementById("pdfTextLayer");

        textLayer.addEventListener(
            "mouseup",
            () => schedulePdfSelectionCapture(0),
            {passive:true}
        );

        textLayer.addEventListener(
            "touchend",
            () => schedulePdfSelectionCapture(180),
            {passive:true}
        );

        document.addEventListener(
            "selectionchange",
            () => schedulePdfSelectionCapture(150),
            {passive:true}
        );

        await initPdfReader();
    } else {
        await initEpubReader();
    }

    const refreshReaderViewport = () => {
        syncReaderViewport();
        setTimeout(() => {
            try { rendition?.resize?.(); } catch (error) {}
        }, 70);
    };

    syncReaderViewport();

    if (window.visualViewport) {
        window.visualViewport.addEventListener("resize", refreshReaderViewport, {passive:true});
        window.visualViewport.addEventListener("scroll", refreshReaderViewport, {passive:true});
    }

    window.addEventListener("resize", refreshReaderViewport, {passive:true});
    window.addEventListener("orientationchange", () => {
        setTimeout(refreshReaderViewport, 120);
    }, {passive:true});

    if (JUMP_ANNOTATION_ID) {
        setTimeout(() => jumpToAnnotation(JUMP_ANNOTATION_ID),600);
    } else if (JUMP_BOOKMARK_ID) {
        setTimeout(() => jumpToBookmark(JUMP_BOOKMARK_ID),600);
    }
})();

</script>

{% endblock %}
"""


# =========================================================
# JSON / URL DECORATION HELPERS
# =========================================================

def decorate_book_payload(book):
    item = dict(book)

    book_id = int(
        item["id"]
    )

    item[
        "thumbnail_url"
    ] = url_for(
        "pastor_resources_thumbnail",
        book_id=book_id,
    )

    item[
        "read_url"
    ] = url_for(
        "pastor_resources_read_book",
        book_id=book_id,
    )

    item[
        "download_url"
    ] = url_for(
        "pastor_resources_download_book",
        book_id=book_id,
    )

    return item


def decorate_bookmark_payload(item):
    value = dict(item)

    value[
        "jump_url"
    ] = (
        url_for(
            "pastor_resources_read_book",
            book_id=int(
                value["book_id"]
            ),
        )
        + "?bookmark="
        + str(
            value["id"]
        )
    )

    return value


def decorate_annotation_payload(item):
    value = dict(item)

    value[
        "is_sermon_note"
    ] = bool(
        value.get(
            "is_sermon_note"
        )
    )

    value[
        "jump_url"
    ] = (
        url_for(
            "pastor_resources_read_book",
            book_id=int(
                value["book_id"]
            ),
        )
        + "?annotation="
        + str(
            value["id"]
        )
    )

    return value


def sync_library_to_database_v3(progress_callback=None):
    """
    Reuse the tested Drive scanner/sync, then report the
    visible book count after private hidden books are removed.
    """

    result = sync_library_to_database(
        progress_callback=progress_callback
    )

    if progress_callback:
        progress_callback(
            stage="finalizing",
            message="Updating the visible Pastor's Resources count...",
            current_file="",
        )

    db = get_resource_db()

    try:
        row = db.execute(
            """
            SELECT COUNT(*) AS cnt

            FROM pastor_library_books

            WHERE is_active = 1
              AND COALESCE(is_hidden, 0) = 0
            """
        ).fetchone()

        visible_count = int(
            row["cnt"] or 0
        )

        db.execute(
            """
            UPDATE pastor_library_sync
            SET unique_books = ?
            WHERE id = 1
            """,
            (
                visible_count,
            ),
        )

        db.commit()

        result[
            "unique_books"
        ] = visible_count

    finally:
        db.close()

    return result


def start_resource_library_sync(app):
    """
    Start one live Pastor's Resources synchronization in a background
    thread so the browser can poll real progress without waiting for one
    long POST request.
    """

    if not SYNC_LOCK.acquire(
        blocking=False
    ):
        return False, get_resource_sync_state()

    update_resource_sync_state(
        running=True,
        stage="starting",
        message="Starting Pastor's Resources synchronization...",
        total=0,
        processed=0,
        new_files=0,
        changed_files=0,
        unchanged_files=0,
        duplicates=0,
        current_file="",
        last_error="",
        started_at=utc_now_iso(),
        finished_at="",
        stats={},
    )

    def worker():
        try:
            with app.app_context():
                stats = sync_library_to_database_v3(
                    progress_callback=update_resource_sync_state
                )

            update_resource_sync_state(
                running=False,
                stage="complete",
                message=(
                    "Sync complete. "
                    + str(
                        stats.get(
                            "unique_books",
                            0,
                        )
                    )
                    + " unique books cataloged."
                ),
                current_file="",
                finished_at=utc_now_iso(),
                stats=stats,
            )

        except Exception as error:
            error_text = str(error)

            print(
                "[Pastor Resources Sync ERROR] "
                + error_text,
                flush=True,
            )

            update_resource_sync_state(
                running=False,
                stage="error",
                message="Pastor's Resources synchronization stopped because of an error.",
                current_file="",
                last_error=error_text,
                finished_at=utc_now_iso(),
            )

        finally:
            SYNC_LOCK.release()

    thread = threading.Thread(
        target=worker,
        name="pastor-resources-sync",
        daemon=True,
    )

    thread.start()

    return True, get_resource_sync_state()



# =========================================================
# PRIVATE DATABASE DETAILS - ADMIN / DEVELOPER VIEW
# =========================================================

def _database_detail_same_name_key(filename):
    """
    Conservative same-name key for the developer dashboard.

    This is intentionally informational only. It never merges,
    hides or deletes anything automatically.
    """

    value = remove_book_extension(
        filename
    )

    value = remove_catalog_source_noise(
        value
    )

    return canonical_title_key(
        value
    )


def get_database_details_payload(
    query="",
    view="all",
    sort="name",
    page=1,
    per_page=50,
):
    """
    Read the last synchronized Google Drive ebook snapshot
    from SQLite. This page never performs a Drive scan by itself.

    "All Drive eBooks" means active PDF/EPUB files found during
    the most recent Sync Books operation. Inactive/missing records
    remain available in the separate Inactive filter for diagnosis.
    """

    ensure_v3_tables()

    query = str(
        query or ""
    ).strip()

    view = str(
        view or "all"
    ).strip().lower()

    sort = str(
        sort or "name"
    ).strip().lower()

    allowed_views = {
        "all",
        "exact",
        "same_name",
        "hidden",
        "inactive",
    }

    allowed_sorts = {
        "name",
        "uploaded_desc",
        "modified_desc",
        "size_desc",
    }

    if view not in allowed_views:
        view = "all"

    if sort not in allowed_sorts:
        sort = "name"

    try:
        page = max(
            1,
            int(page),
        )
    except Exception:
        page = 1

    try:
        per_page = min(
            max(
                20,
                int(per_page),
            ),
            100,
        )
    except Exception:
        per_page = 50

    db = get_resource_db()

    try:
        sync_row = db.execute(
            """
            SELECT *
            FROM pastor_library_sync
            WHERE id = 1
            """
        ).fetchone()

        sync_status = (
            dict(sync_row)
            if sync_row
            else {}
        )

        rows = db.execute(
            """
            SELECT
                f.id AS database_file_id,
                f.drive_file_id,
                f.book_id,
                f.name AS drive_name,
                f.format,
                f.mime_type,
                f.size,
                f.folder_path AS file_folder_path,
                f.created_time AS drive_created_time,
                f.modified_time AS drive_modified_time,
                f.md5_checksum,
                f.sha1_checksum,
                f.sha256_checksum,
                f.is_active AS file_is_active,
                f.is_duplicate,
                f.duplicate_of_drive_file_id,
                f.first_seen_at AS file_first_seen_at,
                f.last_seen_at AS file_last_seen_at,

                b.book_key,
                b.title AS detected_title,
                b.author AS detected_author,
                b.category AS detected_category,
                b.folder_path AS book_folder_path,
                b.is_active AS book_is_active,
                b.first_seen_at AS book_first_seen_at,
                b.last_seen_at AS book_last_seen_at,

                COALESCE(b.manual_title, '') AS manual_title,
                COALESCE(b.manual_author, '') AS manual_author,
                COALESCE(b.manual_category, '') AS manual_category,
                COALESCE(b.is_hidden, 0) AS is_hidden,
                COALESCE(b.hidden_at, '') AS hidden_at,
                COALESCE(b.hidden_by, '') AS hidden_by

            FROM pastor_library_files f

            LEFT JOIN pastor_library_books b
              ON b.id = f.book_id
            """
        ).fetchall()

        raw_rows = [
            dict(row)
            for row in rows
        ]

        # ---------------------------------------------
        # Same-name groups among files still present in
        # the most recent Google Drive synchronization.
        # ---------------------------------------------

        name_counts = {}

        for item in raw_rows:
            if not int(
                item.get(
                    "file_is_active"
                )
                or 0
            ):
                continue

            key = (
                _database_detail_same_name_key(
                    item.get(
                        "drive_name"
                    )
                )
            )

            if key:
                name_counts[key] = (
                    name_counts.get(
                        key,
                        0,
                    )
                    + 1
                )

        same_name_group_count = sum(
            1
            for count in name_counts.values()
            if count > 1
        )

        book_file_counts = {}

        for item in raw_rows:
            if not int(
                item.get(
                    "file_is_active"
                )
                or 0
            ):
                continue

            book_id = item.get(
                "book_id"
            )

            if book_id is None:
                continue

            book_file_counts[
                int(book_id)
            ] = (
                book_file_counts.get(
                    int(book_id),
                    0,
                )
                + 1
            )

        items = []

        for row in raw_rows:
            item = dict(row)

            manual_title = str(
                item.get(
                    "manual_title"
                )
                or ""
            ).strip()

            manual_author = str(
                item.get(
                    "manual_author"
                )
                or ""
            ).strip()

            manual_category = str(
                item.get(
                    "manual_category"
                )
                or ""
            ).strip()

            item[
                "title"
            ] = (
                manual_title
                or str(
                    item.get(
                        "detected_title"
                    )
                    or ""
                )
            )

            item[
                "author"
            ] = (
                manual_author
                or str(
                    item.get(
                        "detected_author"
                    )
                    or ""
                )
            )

            item[
                "category"
            ] = (
                manual_category
                or str(
                    item.get(
                        "detected_category"
                    )
                    or ""
                )
            )

            item[
                "has_manual_override"
            ] = bool(
                manual_title
                or manual_author
                or manual_category
            )

            same_key = (
                _database_detail_same_name_key(
                    item.get(
                        "drive_name"
                    )
                )
            )

            item[
                "same_name_key"
            ] = same_key

            item[
                "same_name_count"
            ] = int(
                name_counts.get(
                    same_key,
                    0,
                )
            )

            try:
                logical_book_id = int(
                    item.get(
                        "book_id"
                    )
                )
            except Exception:
                logical_book_id = None

            item[
                "logical_group_file_count"
            ] = (
                int(
                    book_file_counts.get(
                        logical_book_id,
                        0,
                    )
                )
                if logical_book_id
                else 0
            )

            item[
                "is_current"
            ] = bool(
                int(
                    item.get(
                        "file_is_active"
                    )
                    or 0
                )
            )

            item[
                "is_exact_duplicate"
            ] = bool(
                int(
                    item.get(
                        "is_duplicate"
                    )
                    or 0
                )
            )

            item[
                "is_hidden"
            ] = bool(
                int(
                    item.get(
                        "is_hidden"
                    )
                    or 0
                )
            )

            item[
                "is_same_name"
            ] = (
                item[
                    "same_name_count"
                ]
                > 1
            )

            # -----------------------------------------
            # Dashboard filter
            # -----------------------------------------

            include = False

            if view == "all":
                include = (
                    item[
                        "is_current"
                    ]
                )

            elif view == "exact":
                include = (
                    item[
                        "is_current"
                    ]
                    and item[
                        "is_exact_duplicate"
                    ]
                )

            elif view == "same_name":
                include = (
                    item[
                        "is_current"
                    ]
                    and item[
                        "is_same_name"
                    ]
                )

            elif view == "hidden":
                include = (
                    item[
                        "is_current"
                    ]
                    and item[
                        "is_hidden"
                    ]
                )

            elif view == "inactive":
                include = (
                    not item[
                        "is_current"
                    ]
                )

            if not include:
                continue

            # -----------------------------------------
            # Search developer fields.
            # -----------------------------------------

            if query:
                haystack = " ".join(
                    [
                        str(
                            item.get(
                                "title"
                            )
                            or ""
                        ),
                        str(
                            item.get(
                                "author"
                            )
                            or ""
                        ),
                        str(
                            item.get(
                                "category"
                            )
                            or ""
                        ),
                        str(
                            item.get(
                                "drive_name"
                            )
                            or ""
                        ),
                        str(
                            item.get(
                                "drive_file_id"
                            )
                            or ""
                        ),
                        str(
                            item.get(
                                "book_id"
                            )
                            or ""
                        ),
                        str(
                            item.get(
                                "file_folder_path"
                            )
                            or ""
                        ),
                        str(
                            item.get(
                                "mime_type"
                            )
                            or ""
                        ),
                        str(
                            item.get(
                                "md5_checksum"
                            )
                            or ""
                        ),
                        str(
                            item.get(
                                "sha256_checksum"
                            )
                            or ""
                        ),
                    ]
                ).lower()

                if (
                    query.lower()
                    not in haystack
                ):
                    continue

            items.append(
                item
            )

        # ---------------------------------------------
        # Sort
        # ---------------------------------------------

        if sort == "uploaded_desc":
            items.sort(
                key=lambda item: str(
                    item.get(
                        "drive_created_time"
                    )
                    or ""
                ),
                reverse=True,
            )

        elif sort == "modified_desc":
            items.sort(
                key=lambda item: str(
                    item.get(
                        "drive_modified_time"
                    )
                    or ""
                ),
                reverse=True,
            )

        elif sort == "size_desc":
            items.sort(
                key=lambda item: int(
                    item.get(
                        "size"
                    )
                    or 0
                ),
                reverse=True,
            )

        else:
            items.sort(
                key=lambda item: (
                    str(
                        item.get(
                            "title"
                        )
                        or item.get(
                            "drive_name"
                        )
                        or ""
                    ).lower(),
                    str(
                        item.get(
                            "format"
                        )
                        or ""
                    ).lower(),
                )
            )

        total = len(
            items
        )

        pages = max(
            1,
            math.ceil(
                total
                / per_page
            ),
        )

        if page > pages:
            page = pages

        start = (
            (page - 1)
            * per_page
        )

        page_items = items[
            start:
            start + per_page
        ]

        current_rows = [
            item
            for item in raw_rows
            if int(
                item.get(
                    "file_is_active"
                )
                or 0
            )
        ]

        current_book_ids = {
            int(
                item[
                    "book_id"
                ]
            )
            for item in current_rows
            if item.get(
                "book_id"
            )
            is not None
        }

        hidden_book_ids = {
            int(
                item[
                    "book_id"
                ]
            )
            for item in current_rows
            if item.get(
                "book_id"
            )
            is not None
            and int(
                item.get(
                    "is_hidden"
                )
                or 0
            )
        }

        summary = {
            "current_ebook_files":
                len(
                    current_rows
                ),

            "logical_books":
                len(
                    current_book_ids
                ),

            "pdf_files":
                sum(
                    1
                    for item in current_rows
                    if str(
                        item.get(
                            "format"
                        )
                        or ""
                    ).upper()
                    == "PDF"
                ),

            "epub_files":
                sum(
                    1
                    for item in current_rows
                    if str(
                        item.get(
                            "format"
                        )
                        or ""
                    ).upper()
                    == "EPUB"
                ),

            "exact_duplicate_files":
                sum(
                    1
                    for item in current_rows
                    if int(
                        item.get(
                            "is_duplicate"
                        )
                        or 0
                    )
                ),

            "same_name_groups":
                same_name_group_count,

            "hidden_books":
                len(
                    hidden_book_ids
                ),

            "inactive_file_records":
                sum(
                    1
                    for item in raw_rows
                    if not int(
                        item.get(
                            "file_is_active"
                        )
                        or 0
                    )
                ),

            "folders_scanned":
                int(
                    sync_status.get(
                        "folders_scanned"
                    )
                    or 0
                ),

            "unsupported_files":
                int(
                    sync_status.get(
                        "unsupported_files"
                    )
                    or 0
                ),

            "last_sync_at":
                sync_status.get(
                    "last_sync_at"
                )
                or "",
        }

        return {
            "summary":
                summary,

            "items":
                page_items,

            "page":
                page,

            "pages":
                pages,

            "per_page":
                per_page,

            "total":
                total,

            "view":
                view,

            "sort":
                sort,

            "query":
                query,
        }

    finally:
        db.close()


PASTOR_DATABASE_DETAILS_HTML = r"""
{% extends "base.html" %}

{% block title %}
Database Details - Pastor's Resources
{% endblock %}

{% block content %}

<style>
@import url('https://fonts.googleapis.com/css2?family=Lora:wght@600;700&family=Nunito+Sans:wght@400;600;700;800;900&display=swap');

.app-main {
    max-width: 1550px;
    padding: 0;
}

.db-page {
    width: 100%;
    padding: 14px 12px 52px;
    color: #17233c;
    font-family: "Nunito Sans", Arial, sans-serif;
}

.db-hero {
    padding: 18px;
    border-radius: 20px;
    background: linear-gradient(
        135deg,
        #f8fbff,
        #f7f5ff 48%,
        #fff7fb
    );
    border: 1px solid rgba(15,23,42,.07);
    box-shadow: 0 10px 30px rgba(15,23,42,.06);
}

.db-kicker {
    color: #8a6590;
    font-size: 10px;
    font-weight: 900;
    letter-spacing: .08em;
    text-transform: uppercase;
}

.db-title {
    margin: 4px 0 0;
    font: 700 31px/1.05 "Lora", Georgia, serif;
    color: #17233c;
}

.db-subtitle {
    max-width: 850px;
    margin: 8px 0 0;
    color: #64748b;
    font-size: 12px;
    line-height: 1.55;
}

.db-actions {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    margin-top: 14px;
}

.db-action {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    min-height: 40px;
    padding: 9px 12px;
    border: 0;
    border-radius: 11px;
    background: #fff;
    color: #52627d;
    text-decoration: none;
    font: 800 11px "Nunito Sans", Arial, sans-serif;
    cursor: pointer;
    box-shadow: 0 4px 14px rgba(15,23,42,.07);
}

.db-action.primary {
    color: #fff;
    background: linear-gradient(135deg,#b77fba,#6f96dd);
}

.db-action:disabled {
    opacity: .6;
    cursor: not-allowed;
}

.db-summary {
    display: grid;
    grid-template-columns: repeat(2,minmax(0,1fr));
    gap: 8px;
    margin-top: 14px;
}

.db-stat {
    min-width: 0;
    padding: 12px;
    border-radius: 15px;
    background: #fff;
    border: 1px solid rgba(15,23,42,.07);
    box-shadow: 0 6px 18px rgba(15,23,42,.045);
}

.db-stat-value {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    color: #17233c;
    font-size: 21px;
    font-weight: 900;
}

.db-stat-label {
    margin-top: 2px;
    color: #8a96a9;
    font-size: 9px;
    font-weight: 800;
    text-transform: uppercase;
    letter-spacing: .04em;
}

.db-note {
    margin-top: 12px;
    padding: 11px 13px;
    border-radius: 13px;
    background: #fff9e9;
    color: #755b1e;
    border: 1px solid #f3e1a4;
    font-size: 11px;
    line-height: 1.5;
}

.db-controls {
    margin-top: 14px;
    padding: 12px;
    border-radius: 17px;
    background: #fff;
    border: 1px solid rgba(15,23,42,.07);
}

.db-search-row {
    display: grid;
    grid-template-columns: minmax(0,1fr);
    gap: 8px;
}

.db-input,
.db-select {
    width: 100%;
    min-height: 44px;
    border: 1px solid #dbe3ee;
    border-radius: 11px;
    padding: 9px 11px;
    outline: none;
    background: #fff;
    color: #334155;
    font: 700 12px "Nunito Sans", Arial, sans-serif;
}

.db-input:focus,
.db-select:focus {
    border-color: #8ea8dc;
    box-shadow: 0 0 0 3px rgba(109,142,210,.12);
}

.db-filters {
    display: flex;
    gap: 7px;
    margin-top: 9px;
    overflow-x: auto;
    padding-bottom: 2px;
}

.db-filter {
    flex: 0 0 auto;
    border: 0;
    border-radius: 999px;
    padding: 8px 11px;
    background: #eef2f8;
    color: #59677f;
    font: 850 10px "Nunito Sans", Arial, sans-serif;
    cursor: pointer;
}

.db-filter.active {
    color: #fff;
    background: linear-gradient(135deg,#a878b0,#6c94dc);
}

.db-results-head {
    display: flex;
    justify-content: space-between;
    gap: 10px;
    align-items: end;
    margin: 17px 2px 9px;
}

.db-results-head h2 {
    margin: 0;
    color: #17233c;
    font: 700 21px "Lora", Georgia, serif;
}

.db-results-count {
    color: #8a96a9;
    font-size: 10px;
    text-align: right;
}

.db-list {
    display: grid;
    gap: 10px;
}

.db-file-card {
    padding: 13px;
    border-radius: 17px;
    background: #fff;
    border: 1px solid rgba(15,23,42,.07);
    box-shadow: 0 6px 18px rgba(15,23,42,.045);
}

.db-file-top {
    display: flex;
    align-items: flex-start;
    gap: 8px;
}

.db-file-main {
    min-width: 0;
    flex: 1;
}

.db-badges {
    display: flex;
    flex-wrap: wrap;
    gap: 5px;
    margin-bottom: 7px;
}

.db-badge {
    display: inline-flex;
    align-items: center;
    min-height: 22px;
    padding: 3px 7px;
    border-radius: 999px;
    background: #eef2f8;
    color: #59677f;
    font-size: 8px;
    font-weight: 900;
    letter-spacing: .025em;
}

.db-badge.pdf { background:#fff0f0; color:#b53b3b; }
.db-badge.epub { background:#eef8f2; color:#28845a; }
.db-badge.duplicate { background:#fff2d8; color:#995e00; }
.db-badge.same { background:#f2ecff; color:#7049a4; }
.db-badge.hidden { background:#ffeef5; color:#a33d6c; }
.db-badge.inactive { background:#edf0f4; color:#6f7887; }
.db-badge.manual { background:#eaf4ff; color:#28669c; }

.db-file-title {
    margin: 0;
    color: #17233c;
    font: 700 16px/1.3 "Lora", Georgia, serif;
    overflow-wrap: anywhere;
}

.db-drive-name {
    margin-top: 4px;
    color: #5f6d84;
    font-size: 10px;
    line-height: 1.4;
    overflow-wrap: anywhere;
}

.db-author {
    margin-top: 5px;
    color: #7b879a;
    font-size: 10px;
}

.db-meta-grid {
    display: grid;
    grid-template-columns: 1fr;
    gap: 7px;
    margin-top: 11px;
}

.db-meta {
    min-width: 0;
    padding: 8px 9px;
    border-radius: 10px;
    background: #f8fafc;
}

.db-meta-label {
    color: #9aa5b5;
    font-size: 8px;
    font-weight: 900;
    text-transform: uppercase;
    letter-spacing: .04em;
}

.db-meta-value {
    margin-top: 2px;
    color: #44526a;
    font-size: 10px;
    line-height: 1.4;
    overflow-wrap: anywhere;
}

.db-copy {
    margin-left: 5px;
    border: 0;
    border-radius: 7px;
    padding: 3px 6px;
    background: #e9eef7;
    color: #586980;
    font: 800 8px "Nunito Sans", Arial, sans-serif;
    cursor: pointer;
}

.db-empty {
    padding: 32px 15px;
    border-radius: 16px;
    background: #fff;
    border: 1px solid rgba(15,23,42,.07);
    text-align: center;
    color: #64748b;
    font-size: 12px;
}

.db-pagination {
    display: flex;
    justify-content: center;
    align-items: center;
    gap: 8px;
    margin-top: 18px;
}

.db-page-btn {
    min-height: 38px;
    border: 0;
    border-radius: 10px;
    padding: 8px 11px;
    background: #fff;
    color: #576780;
    font: 850 10px "Nunito Sans", Arial, sans-serif;
    cursor: pointer;
    box-shadow: 0 4px 14px rgba(15,23,42,.07);
}

.db-page-btn:disabled {
    opacity: .4;
    cursor: default;
}

.db-page-label {
    color: #7b879a;
    font-size: 10px;
}

.db-toast {
    position: fixed;
    left: 12px;
    right: 12px;
    bottom: 14px;
    z-index: 12000;
    display: none;
    padding: 12px 14px;
    border-radius: 12px;
    background: #111827;
    color: #fff;
    font-size: 11px;
    box-shadow: 0 14px 32px rgba(0,0,0,.2);
}

@media(min-width:650px) {
    .db-page {
        padding: 20px 18px 58px;
    }

    .db-summary {
        grid-template-columns:
            repeat(4,minmax(0,1fr));
    }

    .db-search-row {
        grid-template-columns:
            minmax(0,1fr)
            190px;
    }

    .db-meta-grid {
        grid-template-columns:
            repeat(2,minmax(0,1fr));
    }
}

@media(min-width:1000px) {
    .db-page {
        padding: 26px 24px 68px;
    }

    .db-hero {
        padding: 24px 26px;
    }

    .db-title {
        font-size: 38px;
    }

    .db-summary {
        grid-template-columns:
            repeat(5,minmax(0,1fr));
    }

    .db-meta-grid {
        grid-template-columns:
            repeat(4,minmax(0,1fr));
    }

    .db-toast {
        left: auto;
        right: 20px;
        width: 360px;
    }
}
</style>

<div class="db-page">
    <section class="db-hero">
        <div class="db-kicker">
            🔐 Private administrator / developer view
        </div>

        <h1 class="db-title">
            Database Details
        </h1>

        <p class="db-subtitle">
            Inspect the PDF and EPUB files recorded from the most recent Google Drive synchronization,
            including upload dates, file IDs, checksums, hidden records, exact duplicates and same-name groups.
        </p>

        <div class="db-actions">
            <a
                class="db-action"
                href="{{ url_for('pastor_resources') }}"
            >
                ← Back to Pastor's Resources
            </a>

            <button
                class="db-action primary"
                id="dbSyncButton"
                type="button"
                onclick="syncDatabaseBooks()"
            >
                🔄 Sync Books Now
            </button>
        </div>

        <div
            class="db-summary"
            id="dbSummary"
        ></div>

        <div class="db-note">
            This page reads the SQLite catalog snapshot only. It does not scan Google Drive every time you open it.
            Use <strong>Sync Books Now</strong> when you want this developer view to reflect the latest Drive contents.
            “Same name” is an informational warning only; it never removes or merges files automatically.
        </div>
    </section>

    <section class="db-controls">
        <div class="db-search-row">
            <input
                class="db-input"
                id="dbSearch"
                type="search"
                placeholder="Search title, author, Drive filename, folder, file ID, checksum..."
                autocomplete="off"
            >

            <select
                class="db-select"
                id="dbSort"
                onchange="dbSortChanged()"
            >
                <option value="name">Sort: Name</option>
                <option value="uploaded_desc">Uploaded: Newest</option>
                <option value="modified_desc">Modified: Newest</option>
                <option value="size_desc">Largest File</option>
            </select>
        </div>

        <div class="db-filters">
            <button class="db-filter active" data-view="all" onclick="changeDbView('all',this)">All Drive eBooks</button>
            <button class="db-filter" data-view="exact" onclick="changeDbView('exact',this)">Exact Duplicates</button>
            <button class="db-filter" data-view="same_name" onclick="changeDbView('same_name',this)">Same Names</button>
            <button class="db-filter" data-view="hidden" onclick="changeDbView('hidden',this)">Hidden</button>
            <button class="db-filter" data-view="inactive" onclick="changeDbView('inactive',this)">Inactive / Missing</button>
        </div>
    </section>

    <div class="db-results-head">
        <h2 id="dbResultsTitle">
            All Drive eBooks
        </h2>

        <div
            class="db-results-count"
            id="dbResultsCount"
        >
            Loading…
        </div>
    </div>

    <div
        class="db-list"
        id="dbList"
    >
        <div class="db-empty">
            Loading database details…
        </div>
    </div>

    <div
        class="db-pagination"
        id="dbPagination"
    ></div>

    <div
        class="db-toast"
        id="dbToast"
    ></div>
</div>

<script>
let dbCurrentPage = 1;
let dbCurrentView = "all";
let dbCurrentSort = "name";
let dbCurrentQuery = "";
let dbTotalPages = 1;
let dbSearchTimer = null;

const DB_VIEW_TITLES = {
    all: "All Drive eBooks",
    exact: "Exact Duplicate Files",
    same_name: "Same-Name eBooks",
    hidden: "Hidden Books / Files",
    inactive: "Inactive / Missing File Records"
};

function dbEscape(value) {
    const div = document.createElement("div");
    div.textContent = value == null ? "" : String(value);
    return div.innerHTML;
}

function dbToast(message) {
    const toast = document.getElementById("dbToast");
    toast.textContent = message || "";
    toast.style.display = "block";
    clearTimeout(toast.hideTimer);
    toast.hideTimer = setTimeout(
        () => toast.style.display = "none",
        4200
    );
}

function dbNumber(value) {
    return Number(value || 0);
}

function dbBytes(bytes) {
    let value = Number(bytes || 0);

    if (!value) {
        return "0 B";
    }

    const units = [
        "B",
        "KB",
        "MB",
        "GB",
        "TB"
    ];

    let unit = 0;

    while (
        value >= 1024
        && unit < units.length - 1
    ) {
        value /= 1024;
        unit += 1;
    }

    return (
        (
            unit === 0
            ? Math.round(value)
            : value.toFixed(
                value >= 10 ? 1 : 2
            )
        )
        + " "
        + units[unit]
    );
}

function dbDate(value) {
    if (!value) {
        return "—";
    }

    const date = new Date(value);

    if (Number.isNaN(date.getTime())) {
        return String(value);
    }

    return date.toLocaleString();
}

async function dbCopy(text) {
    try {
        await navigator.clipboard.writeText(
            String(text || "")
        );

        dbToast("Copied.");
    } catch (_error) {
        dbToast("Unable to copy automatically.");
    }
}

function renderDbSummary(summary) {
    const values = [
        [
            dbNumber(summary.logical_books)
                .toLocaleString(),
            "Logical Books"
        ],
        [
            dbNumber(summary.current_ebook_files)
                .toLocaleString(),
            "Drive eBook Files"
        ],
        [
            dbNumber(summary.pdf_files)
                .toLocaleString(),
            "PDF Files"
        ],
        [
            dbNumber(summary.epub_files)
                .toLocaleString(),
            "EPUB Files"
        ],
        [
            dbNumber(summary.exact_duplicate_files)
                .toLocaleString(),
            "Exact Duplicates"
        ],
        [
            dbNumber(summary.same_name_groups)
                .toLocaleString(),
            "Same-Name Groups"
        ],
        [
            dbNumber(summary.hidden_books)
                .toLocaleString(),
            "Hidden Books"
        ],
        [
            dbNumber(summary.inactive_file_records)
                .toLocaleString(),
            "Inactive Records"
        ],
        [
            dbNumber(summary.folders_scanned)
                .toLocaleString(),
            "Folders Scanned"
        ],
        [
            dbNumber(summary.unsupported_files)
                .toLocaleString(),
            "Unsupported Files"
        ]
    ];

    document.getElementById(
        "dbSummary"
    ).innerHTML = (
        values.map(
            item => `
                <div class="db-stat">
                    <div class="db-stat-value">
                        ${dbEscape(item[0])}
                    </div>
                    <div class="db-stat-label">
                        ${dbEscape(item[1])}
                    </div>
                </div>
            `
        ).join("")
        + `
            <div class="db-stat">
                <div
                    class="db-stat-value"
                    style="font-size:13px;padding-top:5px;"
                >
                    ${dbEscape(
                        dbDate(
                            summary.last_sync_at
                        )
                    )}
                </div>
                <div class="db-stat-label">
                    Last Sync
                </div>
            </div>
        `
    );
}

function dbBadge(
    label,
    className=""
) {
    return (
        '<span class="db-badge '
        + dbEscape(className)
        + '">'
        + dbEscape(label)
        + '</span>'
    );
}

function renderDbItems(items) {
    const list = document.getElementById(
        "dbList"
    );

    if (!items.length) {
        list.innerHTML = `
            <div class="db-empty">
                No database records matched this view.
            </div>
        `;
        return;
    }

    list.innerHTML = items.map(
        item => {
            const format = String(
                item.format || "FILE"
            ).toUpperCase();

            let badges = "";

            badges += dbBadge(
                format,
                format === "PDF"
                    ? "pdf"
                    : (
                        format === "EPUB"
                        ? "epub"
                        : ""
                    )
            );

            if (item.is_exact_duplicate) {
                badges += dbBadge(
                    "EXACT DUPLICATE",
                    "duplicate"
                );
            }

            if (item.is_same_name) {
                badges += dbBadge(
                    "SAME NAME ×"
                    + dbNumber(
                        item.same_name_count
                    ),
                    "same"
                );
            }

            if (
                dbNumber(
                    item.logical_group_file_count
                )
                > 1
            ) {
                badges += dbBadge(
                    "GROUPED FILES ×"
                    + dbNumber(
                        item.logical_group_file_count
                    ),
                    "same"
                );
            }

            if (item.is_hidden) {
                badges += dbBadge(
                    "HIDDEN",
                    "hidden"
                );
            }

            if (!item.is_current) {
                badges += dbBadge(
                    "INACTIVE / MISSING",
                    "inactive"
                );
            }

            if (
                item.has_manual_override
            ) {
                badges += dbBadge(
                    "MANUAL METADATA",
                    "manual"
                );
            }

            const checksum = (
                item.sha256_checksum
                || item.md5_checksum
                || item.sha1_checksum
                || "—"
            );

            const folder = (
                item.file_folder_path
                || item.book_folder_path
                || "Root folder"
            );

            const duplicateOf = (
                item.duplicate_of_drive_file_id
                || "—"
            );

            return `
                <article class="db-file-card">
                    <div class="db-file-top">
                        <div class="db-file-main">
                            <div class="db-badges">
                                ${badges}
                            </div>

                            <h3 class="db-file-title">
                                ${dbEscape(
                                    item.title
                                    || item.drive_name
                                    || "Untitled"
                                )}
                            </h3>

                            <div class="db-author">
                                ${dbEscape(
                                    item.author
                                    || "Unknown Author"
                                )}
                                •
                                ${dbEscape(
                                    item.category
                                    || "General"
                                )}
                            </div>

                            <div class="db-drive-name">
                                <strong>Drive filename:</strong>
                                ${dbEscape(
                                    item.drive_name
                                    || ""
                                )}
                            </div>
                        </div>
                    </div>

                    <div class="db-meta-grid">
                        <div class="db-meta">
                            <div class="db-meta-label">
                                Drive Uploaded / Created
                            </div>
                            <div class="db-meta-value">
                                ${dbEscape(
                                    dbDate(
                                        item.drive_created_time
                                    )
                                )}
                            </div>
                        </div>

                        <div class="db-meta">
                            <div class="db-meta-label">
                                Drive Modified
                            </div>
                            <div class="db-meta-value">
                                ${dbEscape(
                                    dbDate(
                                        item.drive_modified_time
                                    )
                                )}
                            </div>
                        </div>

                        <div class="db-meta">
                            <div class="db-meta-label">
                                Size / MIME
                            </div>
                            <div class="db-meta-value">
                                ${dbEscape(
                                    dbBytes(
                                        item.size
                                    )
                                )}
                                •
                                ${dbEscape(
                                    item.mime_type
                                    || "—"
                                )}
                            </div>
                        </div>

                        <div class="db-meta">
                            <div class="db-meta-label">
                                Logical Book ID
                            </div>
                            <div class="db-meta-value">
                                ${dbEscape(
                                    item.book_id
                                    ?? "—"
                                )}
                                •
                                ${dbEscape(
                                    item.logical_group_file_count
                                    || 0
                                )}
                                current file(s)
                            </div>
                        </div>

                        <div class="db-meta">
                            <div class="db-meta-label">
                                Google Drive File ID
                            </div>
                            <div class="db-meta-value">
                                ${dbEscape(
                                    item.drive_file_id
                                    || "—"
                                )}
                                <button
                                    class="db-copy"
                                    type="button"
                                    data-copy="${dbEscape(
                                        item.drive_file_id
                                        || ""
                                    )}"
                                >
                                    Copy
                                </button>
                            </div>
                        </div>

                        <div class="db-meta">
                            <div class="db-meta-label">
                                Folder
                            </div>
                            <div class="db-meta-value">
                                ${dbEscape(folder)}
                            </div>
                        </div>

                        <div class="db-meta">
                            <div class="db-meta-label">
                                Checksum
                            </div>
                            <div class="db-meta-value">
                                ${dbEscape(checksum)}
                                <button
                                    class="db-copy"
                                    type="button"
                                    data-copy="${dbEscape(
                                        checksum === "—"
                                        ? ""
                                        : checksum
                                    )}"
                                >
                                    Copy
                                </button>
                            </div>
                        </div>

                        <div class="db-meta">
                            <div class="db-meta-label">
                                Duplicate Of Drive ID
                            </div>
                            <div class="db-meta-value">
                                ${dbEscape(duplicateOf)}
                            </div>
                        </div>

                        <div class="db-meta">
                            <div class="db-meta-label">
                                First Seen in Database
                            </div>
                            <div class="db-meta-value">
                                ${dbEscape(
                                    dbDate(
                                        item.file_first_seen_at
                                    )
                                )}
                            </div>
                        </div>

                        <div class="db-meta">
                            <div class="db-meta-label">
                                Last Seen in Database
                            </div>
                            <div class="db-meta-value">
                                ${dbEscape(
                                    dbDate(
                                        item.file_last_seen_at
                                    )
                                )}
                            </div>
                        </div>

                        <div class="db-meta">
                            <div class="db-meta-label">
                                Book Key
                            </div>
                            <div class="db-meta-value">
                                ${dbEscape(
                                    item.book_key
                                    || "—"
                                )}
                            </div>
                        </div>

                        <div class="db-meta">
                            <div class="db-meta-label">
                                Database File Row ID
                            </div>
                            <div class="db-meta-value">
                                ${dbEscape(
                                    item.database_file_id
                                    ?? "—"
                                )}
                            </div>
                        </div>
                    </div>
                </article>
            `;
        }
    ).join("");

    list
        .querySelectorAll(
            "[data-copy]"
        )
        .forEach(
            button => {
                button.addEventListener(
                    "click",
                    () => dbCopy(
                        button.dataset.copy
                        || ""
                    )
                );
            }
        );
}

function renderDbPagination() {
    const box = document.getElementById(
        "dbPagination"
    );

    if (dbTotalPages <= 1) {
        box.innerHTML = "";
        return;
    }

    box.innerHTML = `
        <button
            class="db-page-btn"
            type="button"
            ${dbCurrentPage <= 1 ? "disabled" : ""}
            onclick="loadDatabaseDetails(${dbCurrentPage - 1})"
        >
            ← Previous
        </button>

        <span class="db-page-label">
            Page ${dbCurrentPage}
            of ${dbTotalPages}
        </span>

        <button
            class="db-page-btn"
            type="button"
            ${dbCurrentPage >= dbTotalPages ? "disabled" : ""}
            onclick="loadDatabaseDetails(${dbCurrentPage + 1})"
        >
            Next →
        </button>
    `;
}

async function loadDatabaseDetails(
    page=1
) {
    dbCurrentPage = Math.max(
        1,
        Number(page || 1)
    );

    document.getElementById(
        "dbResultsTitle"
    ).textContent = (
        DB_VIEW_TITLES[
            dbCurrentView
        ]
        || "Database Details"
    );

    document.getElementById(
        "dbResultsCount"
    ).textContent = "Loading…";

    try {
        const params = new URLSearchParams({
            page: dbCurrentPage,
            per_page: 50,
            view: dbCurrentView,
            sort: dbCurrentSort,
            q: dbCurrentQuery
        });

        const response = await fetch(
            "/pastor-resources/admin/api/database-details?"
            + params.toString()
        );

        const data = await response.json();

        if (
            !response.ok
            || !data.ok
        ) {
            throw new Error(
                data.error
                || "Unable to load database details."
            );
        }

        dbCurrentPage = Number(
            data.page || 1
        );

        dbTotalPages = Number(
            data.pages || 1
        );

        renderDbSummary(
            data.summary || {}
        );

        renderDbItems(
            data.items || []
        );

        document.getElementById(
            "dbResultsCount"
        ).textContent = (
            dbNumber(
                data.total
            ).toLocaleString()
            + (
                dbNumber(
                    data.total
                ) === 1
                ? " file"
                : " files"
            )
        );

        renderDbPagination();

    } catch (error) {
        document.getElementById(
            "dbList"
        ).innerHTML = `
            <div class="db-empty">
                <strong>Database error</strong><br>
                ${dbEscape(error.message)}
            </div>
        `;

        document.getElementById(
            "dbResultsCount"
        ).textContent = "";

        dbToast(
            error.message
        );
    }
}

function changeDbView(
    view,
    button
) {
    dbCurrentView = view;

    document
        .querySelectorAll(
            ".db-filter"
        )
        .forEach(
            item => item.classList.remove(
                "active"
            )
        );

    if (button) {
        button.classList.add(
            "active"
        );
    }

    loadDatabaseDetails(
        1
    );
}

function dbSortChanged() {
    dbCurrentSort = (
        document.getElementById(
            "dbSort"
        ).value
        || "name"
    );

    loadDatabaseDetails(
        1
    );
}

async function syncDatabaseBooks() {
    const button = document.getElementById(
        "dbSyncButton"
    );

    if (!button) {
        return;
    }

    const oldText = button.textContent;

    button.disabled = true;
    button.textContent = "⏳ Syncing…";

    dbToast(
        "Scanning Google Drive. Keep this page open until synchronization finishes."
    );

    try {
        const response = await fetch(
            "/pastor-resources/sync-books",
            {
                method: "POST"
            }
        );

        const data = await response.json();

        if (
            !response.ok
            || !data.ok
        ) {
            throw new Error(
                data.error
                || "Synchronization failed."
            );
        }

        dbToast(
            "Sync complete. Refreshing database details…"
        );

        await loadDatabaseDetails(
            1
        );

    } catch (error) {
        dbToast(
            "Sync failed: "
            + error.message
        );

    } finally {
        button.disabled = false;
        button.textContent = oldText;
    }
}

document
    .getElementById(
        "dbSearch"
    )
    .addEventListener(
        "input",
        event => {
            clearTimeout(
                dbSearchTimer
            );

            dbSearchTimer = setTimeout(
                () => {
                    dbCurrentQuery = (
                        event.target.value
                        || ""
                    ).trim();

                    loadDatabaseDetails(
                        1
                    );
                },
                280
            );
        }
    );

loadDatabaseDetails(
    1
);
</script>

{% endblock %}
"""


# =========================================================
# ROUTE REGISTRATION - V3 OVERRIDES EARLIER VERSION
# =========================================================

def register_pastor_resources_routes(app):
    ensure_v3_tables()

    # -----------------------------------------------------
    # MAIN LIBRARY
    # -----------------------------------------------------

    @app.route(
        "/pastor-resources"
    )
    def pastor_resources():
        if not any_user_logged_in():
            return redirect(
                url_for("splash")
            )

        return render_template_string(
            PASTOR_RESOURCES_V3_HTML,
            is_admin=is_resource_admin(),
        )

    # -----------------------------------------------------
    # LEGACY URL USED BY EXISTING BASE MENU
    # -----------------------------------------------------

    @app.route(
        "/download-resources"
    )
    def download_resources():
        return redirect(
            url_for(
                "pastor_resources"
            )
        )

    # -----------------------------------------------------
    # LIBRARY BOOK API
    # -----------------------------------------------------

    @app.route(
        "/pastor-resources/api/books"
    )
    def pastor_resources_api_books():
        if not any_user_logged_in():
            return jsonify(
                ok=False,
                error="Unauthorized",
            ), 401

        try:
            user_key = (
                current_reader_user_key()
            )

            result = (
                search_library_database_v3(
                    user_key=user_key,
                    query=request.args.get(
                        "q",
                        "",
                    ),
                    mode=request.args.get(
                        "mode",
                        "all",
                    ),
                    filter_value=request.args.get(
                        "filter_value",
                        "",
                    ),
                    page=request.args.get(
                        "page",
                        1,
                    ),
                    per_page=request.args.get(
                        "per_page",
                        24,
                    ),
                )
            )

            books = [
                decorate_book_payload(
                    book
                )
                for book in result[
                    "books"
                ]
            ]

            database_status = (
                get_library_database_status()
            )

            public_status = {
                "last_sync_at":
                    database_status.get(
                        "last_sync_at"
                    )
            }

            return jsonify(
                ok=True,
                books=books,
                page=result["page"],
                pages=result["pages"],
                per_page=result[
                    "per_page"
                ],
                total=result["total"],
                stats=(
                    database_status
                    if is_resource_admin()
                    else public_status
                ),
            )

        except Exception as error:
            return jsonify(
                ok=False,
                error=str(error),
            ), 500

    # -----------------------------------------------------
    # DROPDOWN OPTIONS
    # -----------------------------------------------------

    @app.route(
        "/pastor-resources/api/filter-options"
    )
    def pastor_resources_filter_options():
        if not any_user_logged_in():
            return jsonify(
                ok=False,
                error="Unauthorized",
            ), 401

        try:
            mode = str(
                request.args.get(
                    "mode",
                    "",
                )
            ).strip().lower()

            return jsonify(
                ok=True,
                mode=mode,
                options=(
                    get_library_filter_options_v3(
                        mode,
                        current_reader_user_key(),
                    )
                ),
            )

        except Exception as error:
            return jsonify(
                ok=False,
                error=str(error),
            ), 500

    # -----------------------------------------------------
    # CONTINUE READING
    # -----------------------------------------------------

    @app.route(
        "/pastor-resources/api/continue-reading"
    )
    def pastor_resources_continue_reading():
        if not any_user_logged_in():
            return jsonify(
                ok=False,
                error="Unauthorized",
            ), 401

        user_key = (
            current_reader_user_key()
        )

        books = [
            decorate_book_payload(
                book
            )
            for book in get_continue_reading(
                user_key,
                limit=8,
            )
        ]

        return jsonify(
            ok=True,
            books=books,
        )

    # -----------------------------------------------------
    # PRIVATE SYNC BOOKS
    # -----------------------------------------------------

    @app.route(
        "/pastor-resources/sync-books",
        methods=["POST"],
    )
    def pastor_resources_sync_books():
        if not any_user_logged_in():
            return jsonify(
                ok=False,
                error="Unauthorized",
            ), 401

        if not is_resource_admin():
            return jsonify(
                ok=False,
                error=(
                    "Administrator access required."
                ),
            ), 403

        if not SYNC_LOCK.acquire(
            blocking=False
        ):
            return jsonify(
                ok=False,
                error=(
                    "A book synchronization is already running."
                ),
            ), 409

        try:
            stats = (
                sync_library_to_database_v3()
            )

            return jsonify(
                ok=True,
                message=(
                    "Library synchronization completed."
                ),
                stats=stats,
            )

        except Exception as error:
            return jsonify(
                ok=False,
                error=str(error),
            ), 500

        finally:
            SYNC_LOCK.release()

    # -----------------------------------------------------
    # LIVE BACKGROUND SYNC + STATUS
    #
    # The original /sync-books route above is intentionally
    # preserved for the Database Details page. The main
    # Pastor's Resources page uses these two routes so it can
    # display real-time progress without changing the existing
    # working synchronous route.
    # -----------------------------------------------------

    @app.route(
        "/pastor-resources/sync-books-live",
        methods=["POST"],
    )
    def pastor_resources_sync_books_live():
        if not any_user_logged_in():
            return jsonify(
                ok=False,
                error="Unauthorized",
            ), 401

        if not is_resource_admin():
            return jsonify(
                ok=False,
                error=(
                    "Administrator access required."
                ),
            ), 403

        started, state = (
            start_resource_library_sync(
                app
            )
        )

        if not started:
            if state.get("running"):
                return jsonify(
                    ok=True,
                    started=False,
                    state=state,
                )

            return jsonify(
                ok=False,
                error=(
                    "A book synchronization is already running."
                ),
                state=state,
            ), 409

        return jsonify(
            ok=True,
            started=True,
            state=state,
        )

    @app.route(
        "/pastor-resources/sync-books-status"
    )
    def pastor_resources_sync_books_status():
        if not any_user_logged_in():
            return jsonify(
                ok=False,
                error="Unauthorized",
            ), 401

        if not is_resource_admin():
            return jsonify(
                ok=False,
                error=(
                    "Administrator access required."
                ),
            ), 403

        return jsonify(
            ok=True,
            state=get_resource_sync_state(),
        )

    # -----------------------------------------------------
    # THUMBNAIL
    # -----------------------------------------------------

    @app.route(
        "/pastor-resources/thumbnail/<int:book_id>"
    )
    def pastor_resources_thumbnail(book_id):
        if not any_user_logged_in():
            return Response(
                status=401
            )

        book = get_effective_book(
            book_id,
            include_hidden=(
                is_resource_admin()
            ),
        )

        if not book:
            return Response(
                status=404
            )

        cached_path, cached_mime = (
            get_cached_thumbnail(
                book_id
            )
        )

        if cached_path:
            return send_file(
                cached_path,
                mimetype=cached_mime,
                max_age=604800,
                conditional=True,
            )

        try:
            path, mime_type = (
                build_thumbnail_for_book(
                    book_id
                )
            )

            if path:
                return send_file(
                    path,
                    mimetype=mime_type,
                    max_age=604800,
                    conditional=True,
                )

        except Exception:
            pass

        return Response(
            make_default_cover_svg(
                book["title"]
            ),
            mimetype="image/svg+xml",
            headers={
                "Cache-Control":
                    "private, max-age=3600",
            },
        )

    # -----------------------------------------------------
    # READER
    # -----------------------------------------------------

    @app.route(
        "/pastor-resources/read/<int:book_id>"
    )
    def pastor_resources_read_book(book_id):
        if not any_user_logged_in():
            return redirect(
                url_for("splash")
            )

        user_key = (
            current_reader_user_key()
        )

        book = get_effective_book(
            book_id,
            include_hidden=(
                is_resource_admin()
            ),
        )

        if not book:
            return redirect(
                url_for(
                    "pastor_resources"
                )
            )

        state = get_reader_state(
            user_key,
            book_id,
        )

        requested_format = str(
            request.args.get(
                "format",
                "",
            )
        ).strip().upper()

        preferred_format = (
            requested_format
            or str(
                state.get(
                    "last_format"
                )
                or ""
            )
        )

        file_row = choose_book_file(
            book_id,
            preferred_format=preferred_format,
            purpose="read",
        )

        if not file_row:
            return redirect(
                url_for(
                    "pastor_resources"
                )
            )

        reader_format = str(
            file_row["format"]
            or ""
        ).upper()

        state = save_reader_state(
            user_key,
            book_id,
            {
                "last_format":
                    reader_format,
            },
        )

        formats = []

        for value in get_book_files(
            book_id,
            include_hidden=(
                is_resource_admin()
            ),
        ):
            fmt = str(
                value.get("format")
                or ""
            ).upper()

            if fmt and fmt not in formats:
                formats.append(fmt)

        return render_template_string(
            PASTOR_READER_HTML,
            book=book,
            state=state,
            reader_format=reader_format,
            formats=formats,
            media_url=url_for(
                "pastor_resources_media",
                file_row_id=int(
                    file_row["id"]
                ),
            ),
            download_url=url_for(
                "pastor_resources_download_book",
                book_id=book_id,
            ),
            read_base_url=url_for(
                "pastor_resources_read_book",
                book_id=book_id,
            ),
            jump_annotation_id=(
                request.args.get(
                    "annotation",
                    "",
                )
            ),
            jump_bookmark_id=(
                request.args.get(
                    "bookmark",
                    "",
                )
            ),
        )

    # -----------------------------------------------------
    # PRIVATE BYTE/RANGE MEDIA PROXY
    # -----------------------------------------------------

    @app.route(
        "/pastor-resources/media/<int:file_row_id>",
        methods=["GET", "HEAD"],
    )
    def pastor_resources_media(file_row_id):
        if not any_user_logged_in():
            return Response(
                status=401
            )

        file_row = get_file_row(
            file_row_id
        )

        if not file_row:
            return Response(
                status=404
            )

        if (
            int(
                file_row.get(
                    "book_hidden"
                )
                or 0
            )
            and not is_resource_admin()
        ):
            return Response(
                status=404
            )

        return make_drive_stream_response(
            file_row,
            as_attachment=False,
        )

    # -----------------------------------------------------
    # REAL DOWNLOAD
    # -----------------------------------------------------

    @app.route(
        "/pastor-resources/download/<int:book_id>",
        methods=["GET", "HEAD"],
    )
    def pastor_resources_download_book(book_id):
        if not any_user_logged_in():
            return redirect(
                url_for("splash")
            )

        book = get_effective_book(
            book_id,
            include_hidden=(
                is_resource_admin()
            ),
        )

        if not book:
            return Response(
                "Book not found.",
                status=404,
            )

        file_row = choose_book_file(
            book_id,
            purpose="download",
        )

        if not file_row:
            return Response(
                "Download file not found.",
                status=404,
            )

        return make_drive_stream_response(
            file_row,
            as_attachment=True,
        )

    # -----------------------------------------------------
    # READER STATE
    # -----------------------------------------------------

    @app.route(
        "/pastor-resources/api/state/<int:book_id>",
        methods=["GET", "POST"],
    )
    def pastor_resources_state(book_id):
        if not any_user_logged_in():
            return jsonify(
                ok=False,
                error="Unauthorized",
            ), 401

        user_key = (
            current_reader_user_key()
        )

        if not get_effective_book(
            book_id,
            include_hidden=(
                is_resource_admin()
            ),
        ):
            return jsonify(
                ok=False,
                error="Book not found.",
            ), 404

        if request.method == "GET":
            state = get_reader_state(
                user_key,
                book_id,
            )
        else:
            state = save_reader_state(
                user_key,
                book_id,
                request.get_json(
                    silent=True
                )
                or {},
            )

        return jsonify(
            ok=True,
            state=state,
        )

    # -----------------------------------------------------
    # FAVORITE
    # -----------------------------------------------------

    @app.route(
        "/pastor-resources/api/favorite/<int:book_id>",
        methods=["POST"],
    )
    def pastor_resources_favorite(book_id):
        if not any_user_logged_in():
            return jsonify(
                ok=False,
                error="Unauthorized",
            ), 401

        payload = request.get_json(
            silent=True
        ) or {}

        favorite = bool(
            payload.get("favorite")
        )

        set_book_favorite(
            current_reader_user_key(),
            book_id,
            favorite,
        )

        return jsonify(
            ok=True,
            favorite=favorite,
        )

    # -----------------------------------------------------
    # MARK FINISHED / REOPEN
    # -----------------------------------------------------

    @app.route(
        "/pastor-resources/api/completed/<int:book_id>",
        methods=["POST"],
    )
    def pastor_resources_completed(book_id):
        if not any_user_logged_in():
            return jsonify(
                ok=False,
                error="Unauthorized",
            ), 401

        payload = request.get_json(
            silent=True
        ) or {}

        completed = bool(
            payload.get("completed")
        )

        set_book_completed(
            current_reader_user_key(),
            book_id,
            completed,
        )

        return jsonify(
            ok=True,
            completed=completed,
        )

    # -----------------------------------------------------
    # ACTIVE READING SESSIONS
    # -----------------------------------------------------

    @app.route(
        "/pastor-resources/api/session/start/<int:book_id>",
        methods=["POST"],
    )
    def pastor_resources_session_start(book_id):
        if not any_user_logged_in():
            return jsonify(
                ok=False,
                error="Unauthorized",
            ), 401

        payload = request.get_json(
            silent=True
        ) or {}

        session_id = (
            start_reading_session(
                current_reader_user_key(),
                book_id,
                str(
                    payload.get("format")
                    or ""
                ),
            )
        )

        return jsonify(
            ok=True,
            session_id=session_id,
        )

    @app.route(
        "/pastor-resources/api/session/ping",
        methods=["POST"],
    )
    def pastor_resources_session_ping():
        if not any_user_logged_in():
            return jsonify(
                ok=False,
                error="Unauthorized",
            ), 401

        payload = request.get_json(
            silent=True
        ) or {}

        ping_reading_session(
            current_reader_user_key(),
            str(
                payload.get(
                    "session_id"
                )
                or ""
            ),
            payload.get(
                "active_seconds"
            )
            or 0,
        )

        return jsonify(
            ok=True
        )

    @app.route(
        "/pastor-resources/api/session/end",
        methods=["POST"],
    )
    def pastor_resources_session_end():
        if not any_user_logged_in():
            return jsonify(
                ok=False,
                error="Unauthorized",
            ), 401

        payload = request.get_json(
            silent=True
        ) or {}

        end_reading_session(
            current_reader_user_key(),
            str(
                payload.get(
                    "session_id"
                )
                or ""
            ),
        )

        return jsonify(
            ok=True
        )

    # -----------------------------------------------------
    # BOOKMARKS API
    # -----------------------------------------------------

    @app.route(
        "/pastor-resources/api/bookmarks",
        methods=["GET", "POST"],
    )
    def pastor_resources_bookmarks():
        if not any_user_logged_in():
            return jsonify(
                ok=False,
                error="Unauthorized",
            ), 401

        user_key = (
            current_reader_user_key()
        )

        if request.method == "GET":
            book_id = request.args.get(
                "book_id"
            )

            bookmarks = get_bookmarks(
                user_key,
                int(book_id)
                if book_id
                else None,
            )

            return jsonify(
                ok=True,
                bookmarks=[
                    decorate_bookmark_payload(
                        item
                    )
                    for item in bookmarks
                ],
            )

        payload = request.get_json(
            silent=True
        ) or {}

        book_id = int(
            payload.get("book_id")
        )

        bookmark_id = add_bookmark(
            user_key=user_key,
            book_id=book_id,
            book_format=payload.get(
                "format"
            ),
            locator=payload.get(
                "locator"
            ),
            page=payload.get("page"),
            label=payload.get("label"),
            excerpt=payload.get(
                "excerpt"
            ),
        )

        item = [
            value
            for value in get_bookmarks(
                user_key,
                book_id,
            )
            if int(value["id"])
            == bookmark_id
        ][0]

        return jsonify(
            ok=True,
            bookmark=(
                decorate_bookmark_payload(
                    item
                )
            ),
        )

    @app.route(
        "/pastor-resources/api/bookmarks/<int:bookmark_id>",
        methods=["DELETE"],
    )
    def pastor_resources_delete_bookmark(bookmark_id):
        if not any_user_logged_in():
            return jsonify(
                ok=False,
                error="Unauthorized",
            ), 401

        delete_bookmark(
            current_reader_user_key(),
            bookmark_id,
        )

        return jsonify(
            ok=True
        )

    # -----------------------------------------------------
    # ANNOTATIONS API
    # -----------------------------------------------------

    @app.route(
        "/pastor-resources/api/annotations",
        methods=["GET", "POST"],
    )
    def pastor_resources_annotations():
        if not any_user_logged_in():
            return jsonify(
                ok=False,
                error="Unauthorized",
            ), 401

        user_key = (
            current_reader_user_key()
        )

        if request.method == "GET":
            book_id = request.args.get(
                "book_id"
            )

            items = get_annotations(
                user_key,
                int(book_id)
                if book_id
                else None,
            )

            return jsonify(
                ok=True,
                annotations=[
                    decorate_annotation_payload(
                        item
                    )
                    for item in items
                ],
            )

        payload = request.get_json(
            silent=True
        ) or {}

        book_id = int(
            payload.get("book_id")
        )

        annotation_id = add_annotation(
            user_key=user_key,
            book_id=book_id,
            book_format=payload.get(
                "format"
            ),
            annotation_type=payload.get(
                "annotation_type"
            ),
            selected_text=payload.get(
                "selected_text"
            ),
            locator=payload.get(
                "locator"
            ),
            page=payload.get("page"),
            color=payload.get("color"),
            note=payload.get("note"),
            tags=payload.get("tags"),
            is_sermon_note=payload.get(
                "is_sermon_note"
            ),
        )

        item = [
            value
            for value in get_annotations(
                user_key,
                book_id,
            )
            if int(value["id"])
            == annotation_id
        ][0]

        return jsonify(
            ok=True,
            annotation=(
                decorate_annotation_payload(
                    item
                )
            ),
        )

    @app.route(
        "/pastor-resources/api/annotations/<int:annotation_id>",
        methods=["PATCH", "DELETE"],
    )
    def pastor_resources_annotation_item(annotation_id):
        if not any_user_logged_in():
            return jsonify(
                ok=False,
                error="Unauthorized",
            ), 401

        user_key = (
            current_reader_user_key()
        )

        if request.method == "DELETE":
            delete_annotation(
                user_key,
                annotation_id,
            )

            return jsonify(
                ok=True
            )

        payload = request.get_json(
            silent=True
        ) or {}

        update_annotation_note(
            user_key=user_key,
            annotation_id=annotation_id,
            note=payload.get("note"),
            tags=payload.get("tags"),
            is_sermon_note=payload.get(
                "is_sermon_note"
            ),
        )

        return jsonify(
            ok=True
        )

    # -----------------------------------------------------
    # PRIVATE DATABASE DETAILS
    # -----------------------------------------------------

    @app.route(
        "/pastor-resources/admin/database-details"
    )
    def pastor_resources_database_details():
        if not any_user_logged_in():
            return redirect(
                url_for(
                    "splash"
                )
            )

        if not is_resource_admin():
            return redirect(
                url_for(
                    "pastor_resources"
                )
            )

        return render_template_string(
            PASTOR_DATABASE_DETAILS_HTML
        )

    @app.route(
        "/pastor-resources/admin/api/database-details"
    )
    def pastor_resources_database_details_api():
        if not is_resource_admin():
            return jsonify(
                ok=False,
                error=(
                    "Administrator access required."
                ),
            ), 403

        try:
            result = (
                get_database_details_payload(
                    query=request.args.get(
                        "q",
                        "",
                    ),
                    view=request.args.get(
                        "view",
                        "all",
                    ),
                    sort=request.args.get(
                        "sort",
                        "name",
                    ),
                    page=request.args.get(
                        "page",
                        1,
                    ),
                    per_page=request.args.get(
                        "per_page",
                        50,
                    ),
                )
            )

            return jsonify(
                ok=True,
                **result,
            )

        except Exception as error:
            return jsonify(
                ok=False,
                error=str(error),
            ), 500

    # -----------------------------------------------------
    # MY LIBRARY
    # -----------------------------------------------------

    @app.route(
        "/pastor-resources/my-library"
    )
    def pastor_resources_my_library():
        if not any_user_logged_in():
            return redirect(
                url_for("splash")
            )

        initial_tab = str(
            request.args.get(
                "tab",
                "favorites",
            )
        ).strip().lower()

        allowed = {
            "favorites",
            "bookmarks",
            "highlights",
            "sermon",
            "progress",
        }

        if is_resource_admin():
            allowed.add("hidden")

        if initial_tab not in allowed:
            initial_tab = "favorites"

        return render_template_string(
            PASTOR_MY_LIBRARY_HTML,
            initial_tab=initial_tab,
            is_admin=is_resource_admin(),
        )

    @app.route(
        "/pastor-resources/api/my-library"
    )
    def pastor_resources_my_library_api():
        if not any_user_logged_in():
            return jsonify(
                ok=False,
                error="Unauthorized",
            ), 401

        payload = get_my_library_payload(
            current_reader_user_key(),
            include_hidden=(
                is_resource_admin()
            ),
        )

        for key in (
            "favorites",
            "currently_reading",
            "completed",
        ):
            payload[key] = [
                decorate_book_payload(
                    item
                )
                for item in payload.get(
                    key,
                    [],
                )
            ]

        payload[
            "bookmarks"
        ] = [
            decorate_bookmark_payload(
                item
            )
            for item in payload.get(
                "bookmarks",
                [],
            )
        ]

        payload[
            "annotations"
        ] = [
            decorate_annotation_payload(
                item
            )
            for item in payload.get(
                "annotations",
                [],
            )
        ]

        return jsonify(
            ok=True,
            **payload,
        )

    # -----------------------------------------------------
    # ADMIN EDIT / SAFE REMOVE / RESTORE
    # -----------------------------------------------------

    @app.route(
        "/pastor-resources/admin/edit/<int:book_id>",
        methods=["POST"],
    )
    def pastor_resources_admin_edit(book_id):
        if not is_resource_admin():
            return jsonify(
                ok=False,
                error=(
                    "Administrator access required."
                ),
            ), 403

        payload = request.get_json(
            silent=True
        ) or {}

        try:
            book = admin_edit_book(
                book_id,
                payload.get("title"),
                payload.get("author"),
                payload.get("category"),
            )

            return jsonify(
                ok=True,
                book=book,
            )

        except Exception as error:
            return jsonify(
                ok=False,
                error=str(error),
            ), 400

    @app.route(
        "/pastor-resources/admin/hide/<int:book_id>",
        methods=["POST"],
    )
    def pastor_resources_admin_hide(book_id):
        if not is_resource_admin():
            return jsonify(
                ok=False,
                error=(
                    "Administrator access required."
                ),
            ), 403

        try:
            admin_hide_book(
                book_id
            )
        except Exception as error:
            return jsonify(
                ok=False,
                error=str(error),
            ), 400

        return jsonify(
            ok=True,
            message=(
                "Book hidden. Google Drive file was not deleted."
            ),
        )

    @app.route(
        "/pastor-resources/admin/restore/<int:book_id>",
        methods=["POST"],
    )
    def pastor_resources_admin_restore(book_id):
        if not is_resource_admin():
            return jsonify(
                ok=False,
                error=(
                    "Administrator access required."
                ),
            ), 403

        admin_restore_book(
            book_id
        )

        return jsonify(
            ok=True
        )

    # -----------------------------------------------------
    # STATUS
    # -----------------------------------------------------

    @app.route(
        "/pastor-resources/status"
    )
    def pastor_resources_status():
        if not any_user_logged_in():
            return jsonify(
                ok=False,
                error="Unauthorized",
            ), 401

        database_status = (
            get_library_database_status()
        )

        if not is_resource_admin():
            return jsonify(
                ok=True,
                storage="Library database",
                ready=bool(
                    database_status.get(
                        "last_sync_at"
                    )
                ),
            )

        return jsonify(
            ok=True,
            storage="SQLite database",
            stats=database_status,
        )


