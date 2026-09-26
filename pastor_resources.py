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

        # Page counts already discovered by Pij's ebook index are reused here.
        # This avoids downloading/opening every PDF merely to render the library grid.
        page_count_by_book = {}
        try:
            has_ai_docs = db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='pij_library_documents'"
            ).fetchone()
            if has_ai_docs and rows:
                book_ids = [int(row["id"]) for row in rows]
                placeholders = ",".join("?" for _ in book_ids)
                page_rows = db.execute(
                    f"""
                    SELECT book_id, MAX(COALESCE(page_count,0)) AS page_count
                    FROM pij_library_documents
                    WHERE source_type='public_ebook' AND LOWER(COALESCE(format,''))='pdf' AND book_id IN ({placeholders})
                    GROUP BY book_id
                    """,
                    tuple(book_ids),
                ).fetchall()
                page_count_by_book = {
                    int(item["book_id"]): int(item["page_count"] or 0)
                    for item in page_rows
                }
        except Exception:
            page_count_by_book = {}

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
                    "page_count": int(page_count_by_book.get(int(row["id"]), 0)),
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
                    <div class="pr-pages">${safeNumber(book.page_count) > 0 ? safeNumber(book.page_count).toLocaleString() + " pages" : "Pages unavailable"}</div>
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
let pijIndexPollTimer = null;

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
    const title = document.getElementById("resourceSyncTitle");
    const bar = document.getElementById("resourceSyncBar");
    const current = document.getElementById("resourceSyncCurrent");
    const stats = document.getElementById("resourceSyncStats");
    const stage = document.getElementById("resourceSyncStage");
    const count = document.getElementById("resourceSyncCount");
    const percentLabel = document.getElementById("resourceSyncPercent");

    if (!overlay || !bar || !current || !stats || !stage) {
        return;
    }

    if (title) {
        title.textContent = "Syncing Pastor's Resources...";
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

async function fetchPijIndexStatus() {
    const response = await fetch(
        "/ai/library-index/status",
        {
            cache:"no-store"
        }
    );

    const data = await response.json();

    if (!response.ok || !data.ok) {
        throw new Error(
            data.error
            || "Unable to read Pij AI indexing status."
        );
    }

    return data;
}

function renderPijIndexProgress(state) {
    state = state || {};

    const overlay = document.getElementById("resourceSyncOverlay");
    const title = document.getElementById("resourceSyncTitle");
    const bar = document.getElementById("resourceSyncBar");
    const current = document.getElementById("resourceSyncCurrent");
    const stats = document.getElementById("resourceSyncStats");
    const stage = document.getElementById("resourceSyncStage");
    const count = document.getElementById("resourceSyncCount");
    const percentLabel = document.getElementById("resourceSyncPercent");
    const button = document.getElementById("syncButton");

    if (!overlay || !bar || !current || !stats || !stage) {
        return;
    }

    const total = Math.max(0, safeNumber(state.total));
    const processed = Math.max(0, safeNumber(state.processed));
    const indexed = Math.max(0, safeNumber(state.indexed));
    const skipped = Math.max(0, safeNumber(state.skipped));
    const errors = Math.max(0, safeNumber(state.errors));
    const chunks = Math.max(0, safeNumber(state.chunks));
    const searchableDocuments = Math.max(
        0,
        safeNumber(state.searchable_documents)
    );
    const currentFile = String(state.current_file || "").trim();
    const queued = Boolean(state.queued);

    if (title) {
        title.textContent = "Updating Pij AI Knowledge...";
    }

    if (button) {
        button.disabled = true;
        button.textContent = "🧠 Updating Pij...";
    }

    overlay.classList.add("show");

    current.textContent = currentFile
        ? "AI indexing: " + currentFile
        : (
            state.running
                ? "Preparing the next ebook for Pij..."
                : "Pij AI ebook indexing complete."
        );

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
                + " ebooks checked for AI";
        }

        if (percentLabel) {
            percentLabel.textContent = percent + "%";
        }
    } else {
        bar.style.width = "";
        bar.classList.add("indeterminate");

        if (count) {
            count.textContent = "Preparing AI ebook index...";
        }

        if (percentLabel) {
            percentLabel.textContent = "Starting…";
        }
    }

    stats.textContent =
        indexed.toLocaleString()
        + " newly indexed"
        + " · "
        + skipped.toLocaleString()
        + " already current"
        + " · "
        + errors.toLocaleString()
        + " errors"
        + " · "
        + chunks.toLocaleString()
        + " searchable chunks";

    if (queued) {
        stage.textContent =
            "Pij is still indexing. Another incremental pass is queued so newly synced books are not missed.";
    } else if (state.running) {
        stage.textContent =
            state.message
            || "Extracting ebook text and updating Pij's searchable knowledge...";
    } else if (String(state.stage || "") === "error") {
        stage.textContent =
            state.last_error
            || "Pij AI indexing stopped because of an error.";
    } else {
        stage.textContent =
            searchableDocuments.toLocaleString()
            + " searchable ebooks are ready for Pij.";
    }
}

async function pollPijIndexProgress() {
    clearTimeout(pijIndexPollTimer);

    try {
        const state = await fetchPijIndexStatus();

        setResourceSyncUiRunning(true);
        renderPijIndexProgress(state);

        if (state.running || state.queued) {
            pijIndexPollTimer = setTimeout(
                pollPijIndexProgress,
                650
            );
            return;
        }

        if (String(state.stage || "") === "error") {
            showToast(
                "Library sync finished, but Pij AI indexing stopped: "
                + (
                    state.last_error
                    || "Unknown indexing error."
                )
            );

            setTimeout(() => {
                setResourceSyncUiRunning(false);
            }, 3000);

            return;
        }

        showToast(
            "Pij AI knowledge is ready: "
            + safeNumber(
                state.searchable_documents
            ).toLocaleString()
            + " searchable ebooks."
        );

        document.getElementById(
            "statusDot"
        ).className = "pr-status-dot ready";

        document.getElementById(
            "statusText"
        ).textContent =
            "Pastor's Resources and Pij AI knowledge are up to date.";

        document.getElementById(
            "syncDetail"
        ).textContent =
            safeNumber(state.chunks).toLocaleString()
            + " searchable AI text chunks ready.";

        setTimeout(() => {
            setResourceSyncUiRunning(false);
        }, 1800);

    } catch (error) {
        // The visible Drive catalog is already safe. Keep the overlay up and
        // retry briefly because the AI indexing thread may still be working.
        const stage = document.getElementById("resourceSyncStage");

        if (stage) {
            stage.textContent =
                "Reconnecting to Pij AI indexing progress...";
        }

        pijIndexPollTimer = setTimeout(
            pollPijIndexProgress,
            1800
        );
    }
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
                "Library catalog sync complete: "
                + safeNumber(
                    finalStats.unique_books
                ).toLocaleString()
                + " unique books. Updating Pij AI knowledge now..."
            );

            await Promise.all([
                loadBooks(1),
                loadContinueReading()
            ]);

            // The backend automatically starts or queues the incremental Pij
            // index when Drive synchronization completes. Keep this same
            // progress card open and transition to AI indexing progress.
            await pollPijIndexProgress();
            return;

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
            return;
        }

        // A browser refresh must not make an active Pij indexing pass look
        // as though it disappeared after the Drive catalog sync finished.
        try {
            const aiState = await fetchPijIndexStatus();

            if (aiState.running || aiState.queued) {
                setResourceSyncUiRunning(true);
                renderPijIndexProgress(aiState);
                pollPijIndexProgress();
            }
        } catch (_aiError) {
            // Normal library browsing continues even if AI status cannot be
            // checked for this one page load.
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

/* iOS direct-DOM EPUB reader.  This intentionally avoids EPUB.js/WKWebView
   iframe interaction on iPhone/iPad while leaving Android/Desktop unchanged. */
#iosEpubViewer {
    display:none;
    width:min(100%,980px);
    min-height:100%;
    box-sizing:border-box;
    background:var(--reader-panel);
    color:var(--reader-text);
    border-radius:0;
    box-shadow:0 4px 18px rgba(54,43,32,.08);
    overflow:visible;
    -webkit-user-select:text;
    user-select:text;
    -webkit-touch-callout:default;
    touch-action:pan-y;
}

#iosEpubContent {
    box-sizing:border-box;
    width:100%;
    max-width:820px;
    margin:0 auto;
    padding:22px 20px 44px;
    font-family:Georgia,serif;
    font-size:100%;
    line-height:1.6;
    overflow-wrap:anywhere;
    -webkit-user-select:text;
    user-select:text;
    -webkit-touch-callout:default;
}

#iosEpubContent * {
    max-width:100%;
    box-sizing:border-box;
}
#iosEpubContent img,
#iosEpubContent svg,
#iosEpubContent video {
    max-width:100% !important;
    height:auto !important;
}
#iosEpubContent table {
    width:100%;
    max-width:100%;
    border-collapse:collapse;
}
#iosEpubContent pre {
    white-space:pre-wrap;
    overflow-wrap:anywhere;
}
#iosEpubContent p { margin:.72em 0; }
#iosEpubContent h1,
#iosEpubContent h2,
#iosEpubContent h3,
#iosEpubContent h4 {
    line-height:1.25;
    margin:1.15em 0 .55em;
}
#iosEpubContent blockquote {
    margin:1em 1.1em;
    padding-left:.9em;
    border-left:3px solid var(--reader-line);
}
#iosEpubContent a { color:#3567b5; }
.theme-sepia #iosEpubViewer { background:#fbf4e5; color:#4b3b29; }
.theme-sepia #iosEpubContent a { color:#805a31; }
.theme-dark #iosEpubViewer { background:#242a36; color:#edf1f7; }
.theme-dark #iosEpubContent a { color:#9ec4ff; }
.ios-epub-fallback-highlight {
    border-radius:2px;
}

/* Search results are intentionally much stronger than saved highlights.
   Saved annotations remain soft; search hits use yellow and the active
   result uses orange so it is easy to find on a phone. */
.reader-search-hit {
    background:#ffe66d !important;
    color:#15120f !important;
    border-radius:2px;
    box-shadow:0 0 0 1px rgba(160,118,0,.28);
}
.reader-search-hit-active {
    background:#ff8a00 !important;
    color:#111 !important;
    outline:2px solid rgba(170,72,0,.72);
    outline-offset:1px;
    border-radius:2px;
}
#pdfTextLayer .reader-search-hit,
#pdfTextLayer .reader-search-hit-active {
    padding:0;
    margin:0;
}
::highlight(pastor-ios-search-hit) {
    background-color:#ffe66d;
    color:#111;
}
::highlight(pastor-ios-search-active) {
    background-color:#ff8a00;
    color:#111;
    text-decoration:underline 2px rgba(130,48,0,.85);
}
.reader-toolbar,
.reader-bottom-bar,
.reader-tools-sheet,
.reader-side {
    touch-action:manipulation;
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

/* Keep browser page zoom away from the reader content. Two-finger gestures
   inside the content are handled by the PDF/EPUB reader itself so the fixed
   top and bottom controls stay the same size. */
.format-pdf .reader-canvas-area { touch-action:pan-x pan-y; }
.format-epub #epubViewer { touch-action:pan-y; }
#pdfStage { transform-origin:top center; }

.reader-chapter-button {
    width:38px;
    min-width:38px;
    height:38px;
    border:0;
    border-radius:50%;
    background:transparent;
    color:var(--reader-text);
    display:inline-flex;
    align-items:center;
    justify-content:center;
    cursor:pointer;
    -webkit-tap-highlight-color:transparent;
}
.reader-chapter-button:hover { background:rgba(148,163,184,.12); }
.reader-chapter-button svg {
    width:20px;
    height:20px;
    fill:none;
    stroke:currentColor;
    stroke-width:1.8;
    stroke-linecap:round;
}
.reader-chapter-backdrop {
    position:fixed;
    inset:0;
    z-index:20700;
    display:none;
    background:rgba(15,23,42,.38);
}
.reader-chapter-backdrop.show { display:block; }
.reader-chapter-sheet {
    position:fixed;
    z-index:20800;
    left:0;
    right:0;
    bottom:0;
    max-height:min(72vh,620px);
    max-height:min(72dvh,620px);
    transform:translateY(105%);
    transition:transform .22s ease;
    border-radius:24px 24px 0 0;
    background:#fffdfa;
    color:#1f1c18;
    box-shadow:0 -18px 48px rgba(54,43,32,.18);
    padding:7px 12px calc(14px + env(safe-area-inset-bottom,0px));
    display:flex;
    flex-direction:column;
}
.theme-sepia .reader-chapter-sheet { background:#fbf3e4; color:#4b3b29; }
.theme-dark .reader-chapter-sheet { background:#242a36; color:#ecf0f7; }
.reader-chapter-sheet.open { transform:translateY(0); }
.reader-chapter-head {
    display:flex;
    align-items:center;
    justify-content:space-between;
    gap:10px;
    padding:0 2px 8px;
}
.reader-chapter-head h3 { margin:0; font:600 16px Georgia,"Times New Roman",serif; }
.reader-chapter-list {
    overflow:auto;
    -webkit-overflow-scrolling:touch;
    padding:2px 0 4px;
}
.reader-chapter-item {
    width:100%;
    min-height:42px;
    border:0;
    border-bottom:1px solid rgba(100,116,139,.12);
    background:transparent;
    color:inherit;
    text-align:left;
    padding:9px 10px;
    border-radius:8px;
    font:700 12px/1.35 "Nunito Sans",Arial,sans-serif;
    cursor:pointer;
}
.reader-chapter-item.current {
    background:rgba(87,132,204,.13);
    color:#315f9f;
}
.theme-dark .reader-chapter-item.current { color:#b7d4ff; background:rgba(126,168,234,.16); }
.reader-chapter-empty { padding:18px 10px; color:var(--reader-muted); text-align:center; font-size:11px; }

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

@media (max-width:799px) {
    .reader-root.format-pdf .reader-canvas-area {
        padding-left:0;
        padding-right:0;
        padding-top:4px;
        padding-bottom:8px;
    }

    .reader-root.format-pdf #pdfStage {
        box-shadow:0 2px 10px rgba(54,43,32,.10);
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

<div class="reader-root theme-{{ state.theme or 'light' }} format-{{ reader_format|lower }}" id="readerRoot">
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
            <button class="reader-icon-btn" type="button" title="Reader settings" aria-label="Reader settings" onclick="toggleToolsPanel()">
                <svg class="reader-top-svg" viewBox="0 0 24 24" aria-hidden="true">
                    <circle cx="12" cy="12" r="3"></circle>
                    <path d="M19.4 15a1.7 1.7 0 0 0 .34 1.88l.06.06-2.83 2.83-.06-.06A1.7 1.7 0 0 0 15 19.4a1.7 1.7 0 0 0-1 .6 1.7 1.7 0 0 0-.4 1.1V21h-4v-.09A1.7 1.7 0 0 0 8.6 19.4a1.7 1.7 0 0 0-1.88.34l-.06.06-2.83-2.83.06-.06A1.7 1.7 0 0 0 4.6 15a1.7 1.7 0 0 0-.6-1 1.7 1.7 0 0 0-1.1-.4H3v-4h.09A1.7 1.7 0 0 0 4.6 8.6a1.7 1.7 0 0 0-.34-1.88l-.06-.06 2.83-2.83.06.06A1.7 1.7 0 0 0 9 4.6a1.7 1.7 0 0 0 1-.6 1.7 1.7 0 0 0 .4-1.1V3h4v.09A1.7 1.7 0 0 0 15.4 4.6a1.7 1.7 0 0 0 1.88-.34l.06-.06 2.83 2.83-.06.06A1.7 1.7 0 0 0 19.4 9c.16.38.38.72.68 1 .3.27.68.4 1.09.4H21v4h-.09c-.41 0-.79.13-1.09.4-.3.28-.52.62-.68 1z"></path>
                </svg>
            </button>
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
            <div id="iosEpubViewer" aria-label="EPUB reading content">
                <article id="iosEpubContent"></article>
            </div>
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
            <button class="reader-chapter-button" id="readerChapterButton" type="button" onclick="toggleChapterPanel()" title="Quick chapters" aria-label="Quick chapter navigation">
                <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 7h16"></path><path d="M4 12h16"></path><path d="M4 17h16"></path></svg>
            </button>
        </div>
    </footer>

    <div class="reader-chapter-backdrop" id="readerChapterBackdrop" onclick="toggleChapterPanel(false)"></div>
    <div class="reader-chapter-sheet" id="readerChapterSheet" role="dialog" aria-modal="true" aria-label="Quick chapter navigation">
        <div class="reader-tools-handle"></div>
        <div class="reader-chapter-head">
            <h3 id="readerChapterHeading">Chapters</h3>
            <button class="reader-btn" type="button" onclick="toggleChapterPanel(false)" aria-label="Close chapters">✕</button>
        </div>
        <div class="reader-chapter-list" id="readerChapterList">
            <div class="reader-chapter-empty">Preparing chapter navigation…</div>
        </div>
    </div>

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
const PIJ_JUMP_PAGE = {{ jump_page|tojson }};
const PIJ_JUMP_SECTION = {{ jump_section|tojson }};
const AVAILABLE_FORMATS = {{ formats|tojson }};
const PASTOR_RESOURCES_URL = {{ url_for('pastor_resources')|tojson }};
const LIBRARY_RESTORE_KEY = "pastorResourcesRestoreRequestedV1";
const READER_RETURN_URL_KEY = "pastorReaderReturnUrlV1";
const IS_IOS_READER = (() => {
    const ua = String(navigator.userAgent || "");
    const platform = String(navigator.platform || "");
    return /iPad|iPhone|iPod/i.test(ua)
        || (platform === "MacIntel" && Number(navigator.maxTouchPoints || 0) > 1);
})();

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
let activeSearchQuery = "";
let classicEpubSearchMarkCfi = "";
let iosEpubSearchFallbackMarks = [];
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

function installReaderBrowserZoomLock() {
    if (document.__pastorReaderZoomLockInstalled) return;
    document.__pastorReaderZoomLockInstalled = true;

    // This route is a dedicated reader. Keep the browser shell at 1x and let
    // the PDF/EPUB reader implement its own content zoom instead.
    let viewportMeta = document.querySelector('meta[name="viewport"]');
    if (!viewportMeta) {
        viewportMeta = document.createElement("meta");
        viewportMeta.name = "viewport";
        document.head.appendChild(viewportMeta);
    }
    viewportMeta.setAttribute(
        "content",
        "width=device-width, initial-scale=1, minimum-scale=1, maximum-scale=1, user-scalable=no, viewport-fit=cover"
    );

    const insideReader = event => {
        const root = document.getElementById("readerRoot");
        if (!root) return false;
        const target = event?.target;
        return !target || target === document || target === window || root.contains(target);
    };

    const blockBrowserGesture = event => {
        if (!insideReader(event)) return;
        if (event.cancelable) event.preventDefault();
    };

    // iOS/Safari's non-standard pinch events.
    document.addEventListener("gesturestart", blockBrowserGesture, {
        passive:false,
        capture:true
    });
    document.addEventListener("gesturechange", blockBrowserGesture, {
        passive:false,
        capture:true
    });
    document.addEventListener("gestureend", blockBrowserGesture, {
        passive:false,
        capture:true
    });

    // Standard two-finger touch path. preventDefault stops page-level
    // magnification but does not stop our own PDF/EPUB touch listeners from
    // receiving the same event.
    document.addEventListener("touchmove", event => {
        if (!insideReader(event)) return;
        if (event.touches && event.touches.length > 1 && event.cancelable) {
            event.preventDefault();
        }
    }, {passive:false,capture:true});

    // Avoid accidental double-tap page zoom on the reader chrome/content.
    document.addEventListener("dblclick", event => {
        if (!insideReader(event)) return;
        if (event.cancelable) event.preventDefault();
    }, {passive:false,capture:true});
}

function handleReaderSearchInput(value) {
    const clearButton = document.getElementById("readerSearchClear");
    const normalized = String(value || "").trim();
    const hasValue = Boolean(normalized);
    clearButton?.classList.toggle("show", hasValue);

    if (!hasValue) {
        activeSearchQuery = "";
        searchMatches = [];
        searchMatchIndex = -1;
        clearReaderSearchHighlights();
        updateSearchResultUI();
        return;
    }

    if (activeSearchQuery && normalized !== activeSearchQuery) {
        clearReaderSearchHighlights();
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
    if (READER_FORMAT === "EPUB" && !iosDirectEpubActive) {
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

        clearPendingSelection();
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
    else if (iosDirectEpubActive) locator = iosEpubCurrentPositionLocator();
    else locator = currentEpubCfi || "";

    const defaultLabel = READER_FORMAT === "PDF"
        ? "Page " + page
        : (iosDirectEpubActive ? "Chapter " + (iosEpubSpineIndex + 1) : "Saved passage");
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
    try { iosEpubAssetUrls?.forEach?.(url => URL.revokeObjectURL(url)); } catch (error) {}
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
let pdfPageNumber = Math.max(
    1,
    Number(PIJ_JUMP_PAGE || STATE.pdf_page || 1)
);
let pdfScale = Math.max(.5, Number(STATE.pdf_scale || 1.15));
let pdfRenderTask = null;
let pdfOutlineFlat = [];
let pdfAutoFitWidth = window.matchMedia?.("(max-width: 799px)")?.matches || false;

function readerIsMobileWidth() {
    return Boolean(window.matchMedia?.("(max-width: 799px)")?.matches);
}

function pdfFitWidthScaleForPage(page) {
    if (!page) return pdfScale;

    const area = document.getElementById("readerCanvasArea");
    if (!area) return pdfScale;

    const base = page.getViewport({scale:1});
    const horizontalGutter = readerIsMobileWidth() ? 2 : 24;
    const available = Math.max(120, area.clientWidth - horizontalGutter);

    return Math.min(4, Math.max(.5, available / base.width));
}

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
        preparePdfQuickNavigation().catch(error => {
            console.warn("PDF chapter navigation could not be prepared", error);
            renderQuickChapterList();
        });

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

        // On phones, PDF reading defaults to Fit Width so the document uses
        // the available screen instead of shrinking the whole page into a
        // small centered rectangle. The same mode stays active across pages.
        if (pdfAutoFitWidth) {
            pdfScale = pdfFitWidthScaleForPage(page);
        }

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
        updateCurrentChapterHighlight();

        const percent = (pdfPageNumber / pdfDoc.numPages) * 100;
        setProgress(percent);
        saveState({last_format:"PDF",pdf_page:pdfPageNumber,pdf_scale:pdfScale,progress_percent:percent});
        renderPdfAnnotations();
        if (activeSearchQuery) {
            applyPdfSearchHighlights();
        }
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
    pdfAutoFitWidth = false;
    pdfScale = Math.min(4,Math.max(.5,pdfScale + delta));
    await renderPdfPage();
}

async function fitPdfWidth() {
    if (!pdfDoc) return;
    pdfAutoFitWidth = true;
    const page = await pdfDoc.getPage(pdfPageNumber);
    pdfScale = pdfFitWidthScaleForPage(page);
    await renderPdfPage();
}

async function fitPdfPage() {
    if (!pdfDoc) return;
    pdfAutoFitWidth = false;
    const page = await pdfDoc.getPage(pdfPageNumber);
    const base = page.getViewport({scale:1});
    const area = document.getElementById("readerCanvasArea");
    const widthScale = Math.max(.5,(area.clientWidth-24)/base.width);
    const heightScale = Math.max(.5,(area.clientHeight-24)/base.height);
    pdfScale = Math.min(4,widthScale,heightScale);
    await renderPdfPage();
}


function flattenPdfOutline(items, depth=0, output=[]) {
    (items || []).forEach(item => {
        output.push({
            label:String(item?.title || "Untitled section").trim(),
            dest:item?.dest ?? null,
            depth:Number(depth || 0),
            page:null
        });
        if (Array.isArray(item?.items) && item.items.length) {
            flattenPdfOutline(item.items, depth + 1, output);
        }
    });
    return output;
}

async function pdfOutlinePageFromDestination(destination) {
    if (!pdfDoc || !destination) return null;
    try {
        let explicit = destination;
        if (typeof explicit === "string") {
            explicit = await pdfDoc.getDestination(explicit);
        }
        if (!Array.isArray(explicit) || !explicit.length) return null;
        const target = explicit[0];
        let pageIndex = null;
        if (typeof target === "number") {
            pageIndex = target;
        } else if (target && typeof target === "object") {
            pageIndex = await pdfDoc.getPageIndex(target);
        }
        if (pageIndex === null || pageIndex === undefined) return null;
        return Math.max(1, Math.min(pdfDoc.numPages, Number(pageIndex) + 1));
    } catch (error) {
        return null;
    }
}

async function preparePdfQuickNavigation() {
    if (!pdfDoc) return;
    const outline = await pdfDoc.getOutline();
    const flat = flattenPdfOutline(outline || []);
    for (const item of flat) {
        item.page = await pdfOutlinePageFromDestination(item.dest);
    }
    pdfOutlineFlat = flat.filter(item => Number.isFinite(Number(item.page)) && Number(item.page) >= 1);
    renderQuickChapterList();
    updateCurrentChapterHighlight();
}

async function openPdfOutlineItem(index, closePanel=false) {
    if (!pdfDoc) return;
    const item = pdfOutlineFlat[Number(index)];
    if (!item || !Number.isFinite(Number(item.page))) return;
    hideSelectionBarOnly();
    pdfAutoFitWidth = pdfAutoFitWidth && readerIsMobileWidth();
    pdfPageNumber = Math.max(1, Math.min(pdfDoc.numPages, Number(item.page)));
    await renderPdfPage();
    if (closePanel) toggleChapterPanel(false);
}

function installPdfPinchZoom(target) {
    if (!target || target.__pastorPdfPinchInstalled) return;
    target.__pastorPdfPinchInstalled = true;

    let pinching = false;
    let startDistance = 0;
    let startScale = pdfScale;
    let targetScale = pdfScale;

    const distance = touches => {
        if (!touches || touches.length < 2) return 0;
        return Math.hypot(
            touches[0].clientX - touches[1].clientX,
            touches[0].clientY - touches[1].clientY
        );
    };

    const finish = async () => {
        if (!pinching) return;
        pinching = false;
        const stage = document.getElementById("pdfStage");
        if (stage) stage.style.transform = "";
        pdfAutoFitWidth = false;
        pdfScale = Math.min(4, Math.max(.5, targetScale));
        await renderPdfPage();
    };

    target.addEventListener("touchstart", event => {
        if (!event.touches || event.touches.length !== 2) return;
        startDistance = distance(event.touches);
        if (!startDistance) return;
        startScale = pdfScale;
        targetScale = pdfScale;
        pinching = true;
        if (event.cancelable) event.preventDefault();
    }, {passive:false,capture:true});

    target.addEventListener("touchmove", event => {
        if (!pinching || !event.touches || event.touches.length !== 2) return;
        const nextDistance = distance(event.touches);
        if (!nextDistance || !startDistance) return;
        targetScale = Math.min(4, Math.max(.5, startScale * (nextDistance / startDistance)));
        const stage = document.getElementById("pdfStage");
        if (stage) stage.style.transform = `scale(${targetScale / startScale})`;
        if (event.cancelable) event.preventDefault();
    }, {passive:false,capture:true});

    target.addEventListener("touchend", event => {
        if (pinching && (!event.touches || event.touches.length < 2)) finish();
    }, {passive:true,capture:true});
    target.addEventListener("touchcancel", finish, {passive:true,capture:true});

    // Safari/iOS exposes non-standard gesture events for two-finger pinch.
    // On iPhone these can be dispatched above the PDF element, so listen on
    // document in capture phase and accept only gestures that began in the
    // reader content. Single-finger PDF text selection remains untouched.
    let gestureStartScale = pdfScale;
    let iosGestureInsidePdf = false;
    const gestureTarget = IS_IOS_READER ? document : target;
    const eventInsidePdf = event => {
        try {
            const path = event.composedPath?.() || [];
            if (path.includes(target)) return true;
            if (event.target && target.contains(event.target)) return true;
            const x = Number(event.clientX);
            const y = Number(event.clientY);
            if (Number.isFinite(x) && Number.isFinite(y)) {
                const hit = document.elementFromPoint(x,y);
                if (hit && target.contains(hit)) return true;
            }
            return false;
        } catch (error) {
            return false;
        }
    };
    gestureTarget.addEventListener("gesturestart", event => {
        iosGestureInsidePdf = eventInsidePdf(event);
        if (IS_IOS_READER && !iosGestureInsidePdf) return;
        gestureStartScale = pdfScale;
        startScale = pdfScale;
        targetScale = pdfScale;
        pinching = true;
        if (event.cancelable) event.preventDefault();
    }, {passive:false,capture:true});
    gestureTarget.addEventListener("gesturechange", event => {
        if (!pinching || (IS_IOS_READER && !iosGestureInsidePdf)) return;
        targetScale = Math.min(4, Math.max(.5, gestureStartScale * Number(event.scale || 1)));
        const stage = document.getElementById("pdfStage");
        if (stage) stage.style.transform = `scale(${targetScale / gestureStartScale})`;
        if (event.cancelable) event.preventDefault();
    }, {passive:false,capture:true});
    gestureTarget.addEventListener("gestureend", event => {
        if (IS_IOS_READER && !iosGestureInsidePdf) return;
        iosGestureInsidePdf = false;
        finish();
    }, {passive:true,capture:true});
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

    if (iosDirectEpubActive) {
        const chapter = Math.max(1, Math.min(iosEpubSpine.length, requested));
        input.value = String(chapter);
        iosEpubRenderChapter(chapter - 1, "", 0);
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
let epubLastSwipeAt = 0;
let epubRenditionGestureInstalled = false;
let epubTocFlat = [];
let currentEpubHref = "";
let epubPinchPendingSize = null;

// iPhone/iPad compatibility reader. Android/Desktop continue using EPUB.js.
let iosDirectEpubActive = false;
let iosEpubZip = null;
let iosEpubOpfPath = "";
let iosEpubManifest = new Map();
let iosEpubSpine = [];
let iosEpubSpineIndex = 0;
let iosEpubCurrentFragment = "";
let iosEpubAssetUrls = new Map();
let iosEpubSelectionTimer = null;
let iosEpubSelectionHideTimer = null;
let iosEpubProgressTimer = null;
let iosEpubPinchStartDistance = 0;
let iosEpubPinchStartSize = 100;
let iosEpubPinching = false;
let iosEpubChapterNavigationBusy = false;
const IOS_EPUB_POSITION_KEY = "pastorIosDirectEpubPositionV1:" + BOOK_ID;

function epubRangeFromCfi(contents, cfiRange) {
    if (!contents || !cfiRange) return null;
    try {
        if (typeof contents.range === "function") {
            const range = contents.range(cfiRange);
            if (range) return range;
        }
    } catch (error) {}
    return null;
}

function captureEpubSelection(contents, forcedCfi="") {
    if (!contents?.window || !contents?.document) return false;

    let selection = null;
    let range = null;
    let text = "";
    let locator = String(forcedCfi || "").trim();

    // On iOS, EPUB.js may emit a valid CFI after the native selection object
    // has already collapsed. Rebuild the Range from that CFI first so the
    // selected text toolbar does not depend on Safari's selection timing.
    if (locator) {
        range = epubRangeFromCfi(contents, locator);
        try { text = String(range?.toString?.() || "").trim(); } catch (error) {}
    }

    // Normal live-selection path for desktop/Android and iOS when available.
    if (!text) {
        try { selection = contents.window.getSelection?.(); } catch (error) {}
        if (selection && !selection.isCollapsed && selection.rangeCount) {
            range = selection.getRangeAt(0);
            text = String(selection.toString() || "").trim();
        }
    }

    if (!text || !range) return false;

    if (!locator) {
        try {
            locator = String(contents.cfiFromRange?.(range) || "").trim();
        } catch (error) {}
    }

    if (!locator) return false;

    pendingSelection = {
        text,
        locator,
        page:null,
        sourceWindow:contents.window
    };

    let selectionRect = null;
    try { selectionRect = selectionViewportRect(range, contents.window); } catch (error) {}
    showSelectionBarAtRect(selectionRect);
    return true;
}

function applyEpubInteractionCss(doc) {
    if (!doc) return;
    try {
        const marker = "pastor-epub-ios-interaction-style";
        if (!doc.getElementById(marker)) {
            const style = doc.createElement("style");
            style.id = marker;
            const epubTouchAction = IS_IOS_READER ? "auto" : "pan-y";
            style.textContent = `
                html, body {
                    -webkit-user-select:text !important;
                    user-select:text !important;
                    -webkit-touch-callout:default !important;
                    touch-action:${epubTouchAction} !important;
                    overscroll-behavior-x:contain !important;
                }
                body *:not(input):not(textarea):not(select):not(button) {
                    -webkit-user-select:text !important;
                    user-select:text !important;
                    -webkit-touch-callout:default !important;
                }
            `;
            (doc.head || doc.documentElement)?.appendChild(style);
        }
    } catch (error) {}
}

function epubTouchDistance(touches) {
    if (!touches || touches.length < 2) return 0;
    const dx = Number(touches[0].clientX) - Number(touches[1].clientX);
    const dy = Number(touches[0].clientY) - Number(touches[1].clientY);
    return Math.hypot(dx, dy);
}

function installEpubPinchHandlers(target, contents) {
    if (!target || target.__pastorEpubPinchInstalled) return;
    target.__pastorEpubPinchInstalled = true;

    let pinching = false;
    let startDistance = 0;
    let startSize = epubFontSize;

    const finishPinch = () => {
        if (!pinching) return;
        pinching = false;
        if (Number.isFinite(epubPinchPendingSize)) {
            const nextSize = Math.min(220, Math.max(70, Math.round(epubPinchPendingSize / 5) * 5));
            epubPinchPendingSize = null;
            if (nextSize !== epubFontSize) {
                epubFontSize = nextSize;
                applyEpubTheme();
                scheduleEpubLayoutRefresh("Updating text size…");
                saveState({epub_font_size:epubFontSize});
                showReaderToast("Font size: " + epubFontSize + "%");
            }
        }
    };

    target.addEventListener("touchstart", event => {
        if (!event.touches || event.touches.length !== 2) return;
        startDistance = epubTouchDistance(event.touches);
        if (!startDistance) return;
        startSize = epubFontSize;
        epubPinchPendingSize = startSize;
        pinching = true;
        if (event.cancelable) event.preventDefault();
    }, {passive:false,capture:true});

    target.addEventListener("touchmove", event => {
        if (!pinching || !event.touches || event.touches.length !== 2) return;
        const distance = epubTouchDistance(event.touches);
        if (!distance || !startDistance) return;
        const ratio = distance / startDistance;
        epubPinchPendingSize = Math.min(220, Math.max(70, startSize * ratio));
        if (event.cancelable) event.preventDefault();
    }, {passive:false,capture:true});

    target.addEventListener("touchend", event => {
        if (pinching && (!event.touches || event.touches.length < 2)) finishPinch();
    }, {passive:true,capture:true});
    target.addEventListener("touchcancel", finishPinch, {passive:true,capture:true});

    // Safari/WebKit legacy gesture events are a useful fallback on iPhone.
    let gestureStartSize = epubFontSize;
    target.addEventListener("gesturestart", event => {
        gestureStartSize = epubFontSize;
        epubPinchPendingSize = gestureStartSize;
        pinching = true;
        if (event.cancelable) event.preventDefault();
    }, {passive:false,capture:true});
    target.addEventListener("gesturechange", event => {
        if (!pinching) return;
        const scale = Number(event.scale || 1);
        epubPinchPendingSize = Math.min(220, Math.max(70, gestureStartSize * scale));
        if (event.cancelable) event.preventDefault();
    }, {passive:false,capture:true});
    target.addEventListener("gestureend", finishPinch, {passive:true,capture:true});
}


function installEpubIosTapNavigation(contents) {
    if (!IS_IOS_READER || !contents?.document || !contents?.window) return;
    const doc = contents.document;
    const win = contents.window;
    if (doc.__pastorIosTapNavigationInstalled) return;
    doc.__pastorIosTapNavigationInstalled = true;

    doc.addEventListener("click", event => {
        const target = event.target;
        if (target?.closest?.("a,button,input,textarea,select,label,[role='button']")) return;

        // A long press/selection must never become a page turn.
        if (selectionIsActive(win)) return;

        const width = Number(win.innerWidth || doc.documentElement?.clientWidth || 0);
        const x = Number(event.clientX);
        if (!width || !Number.isFinite(x)) return;

        let direction = 0;
        if (x <= width * 0.23) direction = -1;
        else if (x >= width * 0.77) direction = 1;
        if (!direction) return;

        // iOS can finalize a native selection immediately after click. Give it
        // a short moment, then navigate only if no text selection exists.
        setTimeout(() => {
            if (selectionIsActive(win)) return;
            hideSelectionBarOnly();
            queueEpubNavigation(direction);
        }, 70);
    }, {passive:true,capture:false});
}

function installEpubIosGesturePinch(contents) {
    if (!IS_IOS_READER || !contents?.document || !contents?.window) return;
    const doc = contents.document;
    const win = contents.window;
    if (doc.__pastorIosGesturePinchInstalled) return;
    doc.__pastorIosGesturePinchInstalled = true;

    let pinching = false;
    let startSize = epubFontSize;
    let pendingSize = epubFontSize;

    const start = event => {
        startSize = epubFontSize;
        pendingSize = epubFontSize;
        pinching = true;
        if (event.cancelable) event.preventDefault();
    };
    const change = event => {
        if (!pinching) return;
        const scale = Number(event.scale || 1);
        pendingSize = Math.min(220, Math.max(70, startSize * scale));
        if (event.cancelable) event.preventDefault();
    };
    const end = event => {
        if (!pinching) return;
        pinching = false;
        if (event?.cancelable) event.preventDefault();
        const nextSize = Math.min(220, Math.max(70, Math.round(Number(pendingSize || epubFontSize) / 5) * 5));
        if (nextSize === epubFontSize) return;
        epubFontSize = nextSize;
        applyEpubTheme();
        scheduleEpubLayoutRefresh("Updating text size…");
        saveState({epub_font_size:epubFontSize});
        showReaderToast("Font size: " + epubFontSize + "%");
    };

    [win, doc, doc.body].filter(Boolean).forEach(target => {
        target.addEventListener("gesturestart", start, {passive:false,capture:true});
        target.addEventListener("gesturechange", change, {passive:false,capture:true});
        target.addEventListener("gestureend", end, {passive:false,capture:true});
    });
}

function installEpubContentHandlers(contents) {
    if (!contents?.document || !contents?.window) return;

    lastEpubContents = contents;

    const doc = contents.document;
    const win = contents.window;
    const target = doc.body || doc.documentElement || doc;

    applyEpubInteractionCss(doc);

    try {
        const touchAction = IS_IOS_READER ? "auto" : "pan-y";
        if (doc.documentElement) {
            doc.documentElement.style.touchAction = touchAction;
            doc.documentElement.style.overscrollBehaviorX = "contain";
            doc.documentElement.style.webkitUserSelect = "text";
            doc.documentElement.style.userSelect = "text";
            doc.documentElement.style.webkitTouchCallout = "default";
        }

        if (doc.body) {
            doc.body.style.touchAction = touchAction;
            doc.body.style.overscrollBehaviorX = "contain";
            doc.body.style.webkitUserSelect = "text";
            doc.body.style.userSelect = "text";
            doc.body.style.webkitTouchCallout = "default";
        }
    } catch (error) {}

    if (IS_IOS_READER) {
        // iOS-safe mode: do not attach one-finger swipe recognizers. They can
        // compete with WebKit's long-press text-selection machinery. Navigation
        // uses edge taps + visible Previous/Next, while native selection stays
        // in control. Two-finger pinch uses WebKit gesture events only.
        installEpubIosTapNavigation(contents);
        installEpubIosGesturePinch(contents);
    } else {
        installEpubSwipeHandlers(doc, win);
        installEpubPinchHandlers(win, contents);
        installEpubPinchHandlers(doc, contents);
    }

    if (EPUB_CONTENT_HANDLERS.has(doc)) return;
    EPUB_CONTENT_HANDLERS.add(doc);

    let localSelectionTimers = [];

    const clearSelectionTimers = () => {
        localSelectionTimers.forEach(timer => clearTimeout(timer));
        localSelectionTimers = [];
    };

    const scheduleCaptureBurst = (forcedCfi="") => {
        clearSelectionTimers();
        [0, 80, 180, 350, 650, 1000].forEach(delay => {
            localSelectionTimers.push(setTimeout(() => {
                if (selectionToolbarInteracting) return;
                const captured = captureEpubSelection(contents, forcedCfi);
                if (captured) clearSelectionTimers();
            }, delay));
        });
    };

    doc.addEventListener("selectionchange", () => scheduleCaptureBurst(), {passive:true});
    target.addEventListener("touchend", () => scheduleCaptureBurst(), {passive:true,capture:true});
    target.addEventListener("mouseup", () => scheduleCaptureBurst(), {passive:true,capture:true});
    target.addEventListener("contextmenu", () => scheduleCaptureBurst(), {passive:true,capture:true});

    try {
        contents.__pastorScheduleSelectionCapture = scheduleCaptureBurst;
    } catch (error) {}
}


function iosEpubXmlElements(parent, localName) {
    if (!parent) return [];
    try { return Array.from(parent.getElementsByTagNameNS("*", localName)); }
    catch (error) { return Array.from(parent.getElementsByTagName(localName)); }
}

function iosEpubFirstXmlElement(parent, localName) {
    return iosEpubXmlElements(parent, localName)[0] || null;
}

function iosEpubDecodePath(value) {
    let text = String(value || "").replace(/^\/+/, "");
    try { text = decodeURIComponent(text); } catch (error) {}
    return text;
}

function iosEpubSplitReference(value) {
    const raw = String(value || "").trim();
    const hash = raw.indexOf("#");
    if (hash < 0) return {path:raw, fragment:""};
    return {path:raw.slice(0, hash), fragment:raw.slice(hash + 1)};
}

function iosEpubResolvePath(baseFile, reference) {
    const parts = iosEpubSplitReference(reference);
    const ref = String(parts.path || "").trim();
    if (!ref) return {path:iosEpubDecodePath(baseFile), fragment:parts.fragment};
    if (/^(?:https?:|data:|blob:|mailto:|tel:|javascript:)/i.test(ref)) {
        return {path:ref, fragment:parts.fragment, external:true};
    }
    try {
        const base = "https://epub.invalid/" + String(baseFile || "").replace(/^\/+/, "");
        const resolved = new URL(ref, base);
        return {
            path:iosEpubDecodePath(resolved.pathname.slice(1)),
            fragment:parts.fragment,
            external:false
        };
    } catch (error) {
        return {path:iosEpubDecodePath(ref), fragment:parts.fragment, external:false};
    }
}

function iosEpubMimeForPath(path, declared="") {
    if (declared) return declared;
    const value = String(path || "").toLowerCase();
    if (value.endsWith(".jpg") || value.endsWith(".jpeg")) return "image/jpeg";
    if (value.endsWith(".png")) return "image/png";
    if (value.endsWith(".gif")) return "image/gif";
    if (value.endsWith(".webp")) return "image/webp";
    if (value.endsWith(".svg")) return "image/svg+xml";
    if (value.endsWith(".woff2")) return "font/woff2";
    if (value.endsWith(".woff")) return "font/woff";
    if (value.endsWith(".ttf")) return "font/ttf";
    if (value.endsWith(".otf")) return "font/otf";
    if (value.endsWith(".mp3")) return "audio/mpeg";
    if (value.endsWith(".mp4")) return "video/mp4";
    return "application/octet-stream";
}

async function iosEpubAssetUrl(path, declaredMime="") {
    const normalized = iosEpubDecodePath(path);
    if (!normalized || !iosEpubZip) return "";
    if (iosEpubAssetUrls.has(normalized)) return iosEpubAssetUrls.get(normalized);
    const file = iosEpubZip.file(normalized);
    if (!file) return "";
    try {
        const bytes = await file.async("uint8array");
        const blob = new Blob([bytes], {type:iosEpubMimeForPath(normalized, declaredMime)});
        const url = URL.createObjectURL(blob);
        iosEpubAssetUrls.set(normalized, url);
        return url;
    } catch (error) {
        console.warn("EPUB asset could not be prepared", normalized, error);
        return "";
    }
}

function iosEpubSpineIndexForHref(href) {
    const parts = iosEpubSplitReference(href);
    const direct = iosEpubDecodePath(parts.path).toLowerCase();
    const resolved = iosEpubResolvePath(iosEpubOpfPath, parts.path);
    const relative = String(resolved.path || "").toLowerCase();
    const candidates = [direct, relative].filter(Boolean);
    if (!candidates.length) return -1;

    let index = iosEpubSpine.findIndex(item => {
        const path = String(item.path || "").toLowerCase();
        return candidates.includes(path);
    });
    if (index >= 0) return index;

    index = iosEpubSpine.findIndex(item => {
        const path = String(item.path || "").toLowerCase();
        return candidates.some(wanted =>
            path.endsWith("/" + wanted) || wanted.endsWith("/" + path)
        );
    });
    return index;
}

function iosEpubFlattenNavList(list, baseFile, depth=0, output=[]) {
    if (!list) return output;
    const children = Array.from(list.children || []);
    children.forEach(li => {
        if (String(li.tagName || "").toLowerCase() !== "li") return;
        const anchor = Array.from(li.children || []).find(node => String(node.tagName || "").toLowerCase() === "a")
            || li.querySelector?.("a[href]");
        if (anchor) {
            const rawHref = String(anchor.getAttribute("href") || "").trim();
            const resolved = iosEpubResolvePath(baseFile, rawHref);
            const fullHref = resolved.path + (resolved.fragment ? "#" + resolved.fragment : "");
            output.push({
                label:String(anchor.textContent || rawHref || "Untitled section").replace(/\s+/g," ").trim(),
                href:fullHref,
                depth:Number(depth || 0),
                spineIndex:iosEpubSpineIndexForHref(fullHref)
            });
        }
        const nested = Array.from(li.children || []).find(node => String(node.tagName || "").toLowerCase() === "ol");
        if (nested) iosEpubFlattenNavList(nested, baseFile, depth + 1, output);
    });
    return output;
}

async function iosEpubBuildToc(opfDocument, manifestItems) {
    let items = [];
    const navItem = manifestItems.find(item => String(item.properties || "").split(/\s+/).includes("nav"));
    if (navItem && iosEpubZip?.file(navItem.path)) {
        try {
            const navText = await iosEpubZip.file(navItem.path).async("text");
            const navDoc = new DOMParser().parseFromString(navText, "text/html");
            const navs = Array.from(navDoc.querySelectorAll("nav"));
            const tocNav = navs.find(nav => {
                const type = String(nav.getAttribute("epub:type") || nav.getAttribute("type") || "").toLowerCase();
                const role = String(nav.getAttribute("role") || "").toLowerCase();
                return type.includes("toc") || role.includes("doc-toc");
            }) || navs[0];
            const ol = tocNav?.querySelector?.("ol");
            if (ol) items = iosEpubFlattenNavList(ol, navItem.path, 0, []);
        } catch (error) {
            console.warn("EPUB navigation document could not be read", error);
        }
    }

    if (!items.length) {
        const spine = iosEpubFirstXmlElement(opfDocument, "spine");
        const tocId = String(spine?.getAttribute("toc") || "");
        const ncx = manifestItems.find(item => item.id === tocId || item.mediaType === "application/x-dtbncx+xml");
        if (ncx && iosEpubZip?.file(ncx.path)) {
            try {
                const ncxText = await iosEpubZip.file(ncx.path).async("text");
                const ncxDoc = new DOMParser().parseFromString(ncxText, "application/xml");
                const walk = (node, depth=0) => {
                    Array.from(node.children || []).forEach(child => {
                        if (String(child.localName || child.tagName || "").toLowerCase() !== "navpoint") return;
                        const labelNode = iosEpubFirstXmlElement(child, "text");
                        const contentNode = iosEpubFirstXmlElement(child, "content");
                        const rawHref = String(contentNode?.getAttribute("src") || "");
                        const resolved = iosEpubResolvePath(ncx.path, rawHref);
                        const fullHref = resolved.path + (resolved.fragment ? "#" + resolved.fragment : "");
                        items.push({
                            label:String(labelNode?.textContent || rawHref || "Untitled section").replace(/\s+/g," ").trim(),
                            href:fullHref,
                            depth,
                            spineIndex:iosEpubSpineIndexForHref(fullHref)
                        });
                        walk(child, depth + 1);
                    });
                };
                const navMap = iosEpubFirstXmlElement(ncxDoc, "navMap");
                if (navMap) walk(navMap, 0);
            } catch (error) {
                console.warn("EPUB NCX could not be read", error);
            }
        }
    }

    if (!items.length) {
        items = iosEpubSpine.map((item,index) => ({
            label:"Chapter " + (index + 1),
            href:item.path,
            depth:0,
            spineIndex:index
        }));
    }
    return items;
}

async function iosEpubPrepareBook(epubData) {
    if (!window.JSZip) throw new Error("JSZip could not be loaded for the iPhone/iPad EPUB reader.");
    iosEpubZip = await JSZip.loadAsync(epubData);

    const containerFile = iosEpubZip.file("META-INF/container.xml");
    if (!containerFile) throw new Error("This EPUB does not contain META-INF/container.xml.");
    const containerText = await containerFile.async("text");
    const containerDoc = new DOMParser().parseFromString(containerText, "application/xml");
    const rootfile = iosEpubFirstXmlElement(containerDoc, "rootfile");
    iosEpubOpfPath = iosEpubDecodePath(rootfile?.getAttribute("full-path") || "");
    if (!iosEpubOpfPath || !iosEpubZip.file(iosEpubOpfPath)) throw new Error("The EPUB package document could not be found.");

    const opfText = await iosEpubZip.file(iosEpubOpfPath).async("text");
    const opfDoc = new DOMParser().parseFromString(opfText, "application/xml");
    const manifestItems = iosEpubXmlElements(opfDoc, "item").map(node => {
        const href = String(node.getAttribute("href") || "");
        const resolved = iosEpubResolvePath(iosEpubOpfPath, href);
        return {
            id:String(node.getAttribute("id") || ""),
            href,
            path:resolved.path,
            mediaType:String(node.getAttribute("media-type") || ""),
            properties:String(node.getAttribute("properties") || "")
        };
    });
    iosEpubManifest = new Map(manifestItems.map(item => [item.id,item]));

    iosEpubSpine = iosEpubXmlElements(opfDoc, "itemref").map(node => {
        const idref = String(node.getAttribute("idref") || "");
        const item = iosEpubManifest.get(idref);
        if (!item) return null;
        return {...item, linear:String(node.getAttribute("linear") || "yes")};
    }).filter(Boolean);

    if (!iosEpubSpine.length) throw new Error("This EPUB does not contain a readable spine.");
    epubTocFlat = await iosEpubBuildToc(opfDoc, manifestItems);
    populateEpubTocControls();
}

function iosEpubCleanElementTree(root) {
    if (!root) return;
    root.querySelectorAll("script,iframe,object,embed,form,input,textarea,select,button,base").forEach(node => node.remove());
    root.querySelectorAll("style,link[rel='stylesheet']").forEach(node => node.remove());
    root.querySelectorAll("*").forEach(node => {
        Array.from(node.attributes || []).forEach(attr => {
            const name = String(attr.name || "").toLowerCase();
            const value = String(attr.value || "");
            if (name.startsWith("on") || /javascript:/i.test(value)) node.removeAttribute(attr.name);
        });
        // Publisher fixed-layout inline CSS is a frequent cause of tiny or
        // non-selectable text in mobile WebKit. Preserve semantics, not layout.
        node.removeAttribute("style");
        node.removeAttribute("contenteditable");
        node.removeAttribute("draggable");
    });
}

async function iosEpubRewriteChapterAssets(root, chapterPath) {
    if (!root) return;
    const images = Array.from(root.querySelectorAll("img[src],source[src],video[poster]"));
    for (const node of images) {
        const attribute = node.hasAttribute("src") ? "src" : "poster";
        const raw = String(node.getAttribute(attribute) || "");
        if (!raw || /^(?:data:|blob:|https?:)/i.test(raw)) continue;
        const resolved = iosEpubResolvePath(chapterPath, raw);
        const manifest = Array.from(iosEpubManifest.values()).find(item => item.path === resolved.path);
        const url = await iosEpubAssetUrl(resolved.path, manifest?.mediaType || "");
        if (url) node.setAttribute(attribute, url);
    }

    const svgImages = Array.from(root.querySelectorAll("image"));
    for (const node of svgImages) {
        const attr = node.hasAttribute("href") ? "href" : (node.hasAttribute("xlink:href") ? "xlink:href" : "");
        if (!attr) continue;
        const raw = String(node.getAttribute(attr) || "");
        if (!raw || /^(?:data:|blob:|https?:)/i.test(raw)) continue;
        const resolved = iosEpubResolvePath(chapterPath, raw);
        const url = await iosEpubAssetUrl(resolved.path);
        if (url) node.setAttribute(attr, url);
    }

    root.querySelectorAll("a[href]").forEach(anchor => {
        const raw = String(anchor.getAttribute("href") || "").trim();
        if (!raw) return;
        if (/^(?:https?:|mailto:|tel:)/i.test(raw)) {
            anchor.target = "_blank";
            anchor.rel = "noopener noreferrer";
            return;
        }
        const resolved = iosEpubResolvePath(chapterPath, raw);
        const full = resolved.path + (resolved.fragment ? "#" + resolved.fragment : "");
        anchor.dataset.epubHref = full;
        anchor.setAttribute("href", "#");
    });
}

function iosEpubNodePath(node, root) {
    const path = [];
    let current = node;
    while (current && current !== root) {
        const parent = current.parentNode;
        if (!parent) return null;
        const index = Array.prototype.indexOf.call(parent.childNodes, current);
        if (index < 0) return null;
        path.unshift(index);
        current = parent;
    }
    return current === root ? path : null;
}

function iosEpubNodeFromPath(root, path) {
    let current = root;
    for (const rawIndex of (path || [])) {
        const index = Number(rawIndex);
        if (!current?.childNodes || index < 0 || index >= current.childNodes.length) return null;
        current = current.childNodes[index];
    }
    return current;
}

function iosEpubMakeRangeLocator(range, text) {
    const root = document.getElementById("iosEpubContent");
    if (!root || !range || !root.contains(range.commonAncestorContainer)) return "";
    const payload = {
        v:1,
        s:iosEpubSpineIndex,
        sp:iosEpubNodePath(range.startContainer, root),
        so:Number(range.startOffset || 0),
        ep:iosEpubNodePath(range.endContainer, root),
        eo:Number(range.endOffset || 0),
        q:String(text || "").slice(0,240)
    };
    if (!payload.sp || !payload.ep) return "";
    return "iosdom:" + encodeURIComponent(JSON.stringify(payload));
}

function iosEpubParseRangeLocator(locator) {
    const raw = String(locator || "");
    if (!raw.startsWith("iosdom:")) return null;
    try { return JSON.parse(decodeURIComponent(raw.slice(7))); }
    catch (error) { return null; }
}

function iosEpubRangeFromLocator(locator, selectedText="") {
    const payload = iosEpubParseRangeLocator(locator);
    const root = document.getElementById("iosEpubContent");
    if (!payload || !root || Number(payload.s) !== Number(iosEpubSpineIndex)) return null;
    try {
        const start = iosEpubNodeFromPath(root, payload.sp);
        const end = iosEpubNodeFromPath(root, payload.ep);
        if (start && end) {
            const range = document.createRange();
            range.setStart(start, Math.min(Number(payload.so || 0), start.length ?? start.childNodes?.length ?? 0));
            range.setEnd(end, Math.min(Number(payload.eo || 0), end.length ?? end.childNodes?.length ?? 0));
            if (String(range.toString() || "").trim()) return range;
        }
    } catch (error) {}

    // Quote fallback makes saved annotations survive harmless DOM/path shifts.
    const quote = String(selectedText || payload.q || "").trim();
    if (!quote) return null;
    const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
    let node;
    while ((node = walker.nextNode())) {
        const index = String(node.nodeValue || "").indexOf(quote);
        if (index >= 0) {
            const range = document.createRange();
            range.setStart(node, index);
            range.setEnd(node, index + quote.length);
            return range;
        }
    }
    return null;
}

function iosEpubCurrentSelectionRange() {
    if (!iosDirectEpubActive) return null;
    const root = document.getElementById("iosEpubContent");
    if (!root) return null;

    const selection = window.getSelection?.();
    if (!selection || selection.isCollapsed || !selection.rangeCount) return null;

    const range = selection.getRangeAt(0);
    if (!root.contains(range.commonAncestorContainer)) return null;

    const text = String(selection.toString() || "").trim();
    if (!text) return null;

    return {selection, range, text};
}

function iosEpubCaptureSelection() {
    if (!iosDirectEpubActive || selectionToolbarInteracting) return false;

    const current = iosEpubCurrentSelectionRange();
    if (!current) return false;

    clearTimeout(iosEpubSelectionHideTimer);

    const locator = iosEpubMakeRangeLocator(current.range, current.text);
    if (!locator) return false;

    pendingSelection = {
        text:current.text,
        locator,
        page:null,
        sourceWindow:window
    };

    showSelectionBarAtRect(current.range.getBoundingClientRect?.() || null);
    return true;
}

function iosEpubHideSelectionToolbarIfCollapsed() {
    clearTimeout(iosEpubSelectionHideTimer);

    iosEpubSelectionHideTimer = setTimeout(() => {
        if (!iosDirectEpubActive || selectionToolbarInteracting) return;
        if (iosEpubCurrentSelectionRange()) return;

        pendingSelection = null;
        hideSelectionBarOnly();
    }, 220);
}

function iosEpubScheduleSelectionCapture() {
    clearTimeout(iosEpubSelectionTimer);

    iosEpubSelectionTimer = setTimeout(() => {
        if (selectionToolbarInteracting) return;

        if (!iosEpubCaptureSelection()) {
            iosEpubHideSelectionToolbarIfCollapsed();
        }
    }, 140);
}

function iosEpubAnnotationName(id) {
    return "pr-ios-ann-" + String(id).replace(/[^a-zA-Z0-9_-]/g,"-");
}

function iosEpubApplyAnnotation(item) {
    if (!iosDirectEpubActive || !item?.locator) return;
    const range = iosEpubRangeFromLocator(item.locator, item.selected_text || "");
    if (!range) return;
    const id = Number(item.id);
    const name = iosEpubAnnotationName(id);

    if (window.CSS?.highlights && window.Highlight) {
        try {
            CSS.highlights.set(name, new Highlight(range));
            let style = document.getElementById("iosEpubHighlightRules");
            if (!style) {
                style = document.createElement("style");
                style.id = "iosEpubHighlightRules";
                document.head.appendChild(style);
            }
            const color = String(item.color || (item.annotation_type === "underline" ? "#e5962d" : "#ffe66d"));
            style.sheet?.insertRule?.(
                item.annotation_type === "underline"
                    ? `::highlight(${name}) { text-decoration: underline 2px ${color}; text-underline-offset: 2px; }`
                    : `::highlight(${name}) { background-color: ${color}; }`,
                style.sheet.cssRules.length
            );
            appliedEpubAnnotationIds.add(id);
            return;
        } catch (error) {
            console.warn("CSS Highlight API fallback", error);
        }
    }

    try {
        const span = document.createElement("span");
        span.className = "ios-epub-fallback-highlight";
        span.dataset.annotationId = String(id);
        if (item.annotation_type === "underline") {
            span.style.textDecoration = "underline 2px " + (item.color || "#e5962d");
            span.style.textUnderlineOffset = "2px";
        } else {
            span.style.background = item.color || "#ffe66d";
        }
        const fragment = range.extractContents();
        span.appendChild(fragment);
        range.insertNode(span);
        appliedEpubAnnotationIds.add(id);
    } catch (error) {
        console.warn("Direct EPUB annotation could not be drawn", error);
    }
}

function iosEpubRemoveAnnotationVisual(item) {
    const id = Number(item?.id);
    if (!Number.isFinite(id)) return;
    const name = iosEpubAnnotationName(id);
    try { window.CSS?.highlights?.delete?.(name); } catch (error) {}
    document.querySelectorAll(`#iosEpubContent [data-annotation-id="${id}"]`).forEach(node => {
        const parent = node.parentNode;
        if (!parent) return;
        while (node.firstChild) parent.insertBefore(node.firstChild, node);
        node.remove();
        parent.normalize?.();
    });
    appliedEpubAnnotationIds.delete(id);
}

function iosEpubApplyCurrentAnnotations() {
    annotations
        .filter(item => String(item.format || "").toUpperCase() === "EPUB")
        .filter(item => String(item.locator || "").startsWith("iosdom:"))
        .forEach(iosEpubApplyAnnotation);
}

function iosEpubCurrentPositionLocator() {
    const area = document.getElementById("readerCanvasArea");
    const max = Math.max(1, Number(area?.scrollHeight || 1) - Number(area?.clientHeight || 0));
    const ratio = Math.max(0, Math.min(1, Number(area?.scrollTop || 0) / max));
    return `iosdompos:${iosEpubSpineIndex}:${ratio.toFixed(5)}`;
}

function iosEpubParsePositionLocator(locator) {
    const match = /^iosdompos:(\d+)(?::([0-9.]+))?/.exec(String(locator || ""));
    if (!match) return null;
    return {index:Number(match[1]), ratio:Number(match[2] || 0)};
}

function iosEpubSavePosition() {
    if (!iosDirectEpubActive || !iosEpubSpine.length) return;
    const area = document.getElementById("readerCanvasArea");
    const max = Math.max(1, Number(area?.scrollHeight || 1) - Number(area?.clientHeight || 0));
    const ratio = Math.max(0, Math.min(1, Number(area?.scrollTop || 0) / max));
    const progress = Math.max(0, Math.min(100, ((iosEpubSpineIndex + ratio) / iosEpubSpine.length) * 100));
    setProgress(progress);
    try { localStorage.setItem(IOS_EPUB_POSITION_KEY, JSON.stringify({index:iosEpubSpineIndex, ratio})); } catch (error) {}
    clearTimeout(iosEpubProgressTimer);
    iosEpubProgressTimer = setTimeout(() => {
        saveState({
            last_format:"EPUB",
            progress_percent:progress,
            epub_font_size:epubFontSize,
            epub_font_family:epubFontFamily,
            epub_line_height:epubLineHeight
        });
    }, 260);
}

function iosEpubApplyReaderAppearance() {
    const content = document.getElementById("iosEpubContent");
    if (!content) return;
    content.style.fontSize = epubFontSize + "%";
    content.style.fontFamily = epubFontFamily;
    content.style.lineHeight = String(epubLineHeight);
}

async function iosEpubRenderChapter(index, fragment="", restoreRatio=0) {
    if (!iosDirectEpubActive || !iosEpubSpine.length) return false;
    const targetIndex = Math.max(0, Math.min(iosEpubSpine.length - 1, Number(index || 0)));
    const item = iosEpubSpine[targetIndex];
    const file = iosEpubZip?.file(item.path);
    if (!file) return false;

    setPageBusy(true, "Opening chapter " + (targetIndex + 1) + "…");
    try {
        const xhtml = await file.async("text");
        const parsed = new DOMParser().parseFromString(xhtml, "text/html");
        const body = parsed.body || parsed.documentElement;
        iosEpubCleanElementTree(body);
        await iosEpubRewriteChapterAssets(body, item.path);

        const content = document.getElementById("iosEpubContent");
        content.replaceChildren(...Array.from(body.childNodes).map(node => document.importNode(node, true)));
        iosEpubSpineIndex = targetIndex;
        iosEpubCurrentFragment = String(fragment || "");
        currentEpubHref = item.path + (fragment ? "#" + fragment : "");
        iosEpubApplyReaderAppearance();
        appliedEpubAnnotationIds.clear();
        iosEpubApplyCurrentAnnotations();

        if (activeSearchQuery) {
            const activeMatch = searchMatches[searchMatchIndex] || null;
            const activeOccurrence = Number(activeMatch?.iosSpineIndex) === targetIndex
                ? Number(activeMatch?.iosOccurrence ?? 0)
                : -1;
            applyIosEpubSearchHighlights(
                activeSearchQuery,
                activeOccurrence
            );
        }

        updateReaderPageControls(targetIndex + 1, iosEpubSpine.length, "Chapter", true);
        updateCurrentChapterHighlight();

        const area = document.getElementById("readerCanvasArea");
        await nextAnimationFrame();
        if (fragment) {
            let target = null;
            try { target = content.querySelector("#" + CSS.escape(fragment)); } catch (error) {}
            if (target) target.scrollIntoView({block:"start"});
            else if (area) area.scrollTop = 0;
        } else if (area) {
            const max = Math.max(0, area.scrollHeight - area.clientHeight);
            area.scrollTop = max * Math.max(0, Math.min(1, Number(restoreRatio || 0)));
        }
        iosEpubSavePosition();
        return true;
    } finally {
        setPageBusy(false);
    }
}

async function iosEpubOpenHref(href) {
    const parts = iosEpubSplitReference(href);
    let index = iosEpubSpineIndexForHref(parts.path);
    if (index < 0) {
        const normalized = iosEpubDecodePath(parts.path).toLowerCase();
        index = iosEpubSpine.findIndex(item => {
            const path = String(item.path || "").toLowerCase();
            return path === normalized || path.endsWith("/" + normalized) || normalized.endsWith("/" + path);
        });
    }
    if (index < 0) return false;
    return iosEpubRenderChapter(index, parts.fragment, 0);
}

async function iosEpubGo(direction) {
    if (!iosDirectEpubActive || iosEpubChapterNavigationBusy) return;

    const normalized = direction < 0 ? -1 : 1;
    const next = iosEpubSpineIndex + normalized;

    if (next < 0 || next >= iosEpubSpine.length) {
        showReaderToast(normalized < 0 ? "Beginning of book." : "End of book.");
        return;
    }

    // A page/chapter turn should never leave an old native selection or
    // floating selection toolbar behind. This is used by buttons, edge taps
    // and the iOS swipe handler below.
    if (pendingSelection || selectionIsActive(window)) {
        clearPendingSelection();
    } else {
        hideSelectionBarOnly();
    }

    iosEpubChapterNavigationBusy = true;
    try {
        await iosEpubRenderChapter(next, "", 0);
    } finally {
        iosEpubChapterNavigationBusy = false;
    }
}

function iosEpubInstallInteractions() {
    const viewer = document.getElementById("iosEpubViewer");
    const content = document.getElementById("iosEpubContent");
    const area = document.getElementById("readerCanvasArea");
    if (!viewer || !content) return;

    if (!viewer.__pastorDirectHandlers) {
        viewer.__pastorDirectHandlers = true;

        // -------------------------------------------------
        // iOS direct-DOM chapter swipe
        // -------------------------------------------------
        // Do not preventDefault() on one-finger touches. Safari must keep the
        // complete native touch stream so long-press text selection and the
        // blue selection handles continue to work. We only decide that the
        // gesture was a swipe after touchend/touchcancel.
        let swipeStartX = 0;
        let swipeStartY = 0;
        let swipeLastX = 0;
        let swipeLastY = 0;
        let swipeStartedAt = 0;
        let swipeTracking = false;
        let swipeMultiTouch = false;
        let lastCompletedSwipeAt = 0;

        const resetDirectSwipe = () => {
            swipeTracking = false;
            swipeMultiTouch = false;
        };

        const beginDirectSwipe = event => {
            if (!event?.touches || event.touches.length !== 1) {
                if (event?.touches?.length > 1) swipeMultiTouch = true;
                swipeTracking = false;
                return;
            }

            if (iosEpubPinching || selectionToolbarInteracting || selectionIsActive(window)) {
                swipeTracking = false;
                return;
            }

            if (event.target?.closest?.("a,button,input,textarea,select,label,[role='button']")) {
                swipeTracking = false;
                return;
            }

            const touch = event.touches[0];
            const viewportWidth = Number(window.innerWidth || document.documentElement.clientWidth || 0);

            // Leave the extreme screen edges to Safari's own back/forward
            // navigation gesture. Swiping anywhere else in the reading area
            // can turn the EPUB chapter.
            if (viewportWidth > 0 && (touch.clientX < 24 || touch.clientX > viewportWidth - 24)) {
                swipeTracking = false;
                return;
            }

            swipeStartX = swipeLastX = Number(touch.clientX || 0);
            swipeStartY = swipeLastY = Number(touch.clientY || 0);
            swipeStartedAt = Date.now();
            swipeTracking = true;
            swipeMultiTouch = false;
        };

        const moveDirectSwipe = event => {
            if (!swipeTracking) return;

            if (!event?.touches || event.touches.length !== 1) {
                if (event?.touches?.length > 1) swipeMultiTouch = true;
                swipeTracking = false;
                return;
            }

            const touch = event.touches[0];
            swipeLastX = Number(touch.clientX || swipeLastX);
            swipeLastY = Number(touch.clientY || swipeLastY);

            // Intentionally no preventDefault here. Vertical scrolling and
            // native iOS text selection must remain fully native.
        };

        const finishDirectSwipe = (x, y) => {
            if (!swipeTracking || swipeMultiTouch) {
                resetDirectSwipe();
                return false;
            }

            const dx = Number(x ?? swipeLastX) - swipeStartX;
            const dy = Number(y ?? swipeLastY) - swipeStartY;
            const elapsed = Date.now() - swipeStartedAt;
            resetDirectSwipe();

            // A native selection always wins over chapter navigation. This
            // prevents a long-press or selection-handle drag from turning the
            // chapter.
            if (selectionToolbarInteracting || selectionIsActive(window)) return false;
            if (elapsed > 950) return false;

            // Conservative threshold: a deliberate horizontal gesture of at
            // least 64px that is clearly more horizontal than vertical.
            if (Math.abs(dx) < 64) return false;
            if (Math.abs(dx) < Math.abs(dy) * 1.25) return false;

            lastCompletedSwipeAt = Date.now();
            iosEpubGo(dx < 0 ? 1 : -1);
            return true;
        };

        viewer.addEventListener("touchstart", beginDirectSwipe, {passive:true,capture:true});
        viewer.addEventListener("touchmove", moveDirectSwipe, {passive:true,capture:true});
        viewer.addEventListener("touchend", event => {
            const touch = event?.changedTouches?.[0];
            finishDirectSwipe(touch?.clientX ?? swipeLastX, touch?.clientY ?? swipeLastY);
        }, {passive:true,capture:true});
        viewer.addEventListener("touchcancel", () => {
            // WebKit may convert a finished horizontal gesture to touchcancel.
            // Use the last observed point so that a valid deliberate swipe is
            // not lost, while the same selection safeguards still apply.
            finishDirectSwipe(swipeLastX, swipeLastY);
        }, {passive:true,capture:true});

        viewer.addEventListener("click", event => {
            // Some WebKit builds synthesize a click after a touch gesture.
            // Ignore it briefly so a swipe cannot also trigger the edge-tap
            // chapter navigation and accidentally skip two chapters.
            if (Date.now() - lastCompletedSwipeAt < 450) return;
            const link = event.target?.closest?.("a[data-epub-href]");
            if (link) {
                event.preventDefault();
                iosEpubOpenHref(link.dataset.epubHref || "");
                return;
            }
            if (event.target?.closest?.("a,button,input,textarea,select,label,[role='button']")) return;
            if (selectionIsActive(window)) return;
            const rect = viewer.getBoundingClientRect();
            const x = Number(event.clientX) - rect.left;
            if (!rect.width || !Number.isFinite(x)) return;
            let direction = 0;
            if (x <= rect.width * .18) direction = -1;
            else if (x >= rect.width * .82) direction = 1;
            if (!direction) return;
            setTimeout(() => {
                if (!selectionIsActive(window)) iosEpubGo(direction);
            }, 90);
        });

        viewer.addEventListener("touchstart", event => {
            if (!event.touches || event.touches.length !== 2) return;
            iosEpubPinchStartDistance = epubTouchDistance(event.touches);
            iosEpubPinchStartSize = epubFontSize;
            iosEpubPinching = Boolean(iosEpubPinchStartDistance);
            if (iosEpubPinching && event.cancelable) event.preventDefault();
        }, {passive:false,capture:true});
        viewer.addEventListener("touchmove", event => {
            if (!iosEpubPinching || !event.touches || event.touches.length !== 2) return;
            const distance = epubTouchDistance(event.touches);
            if (!distance) return;
            const nextSize = Math.min(220, Math.max(70, iosEpubPinchStartSize * (distance / iosEpubPinchStartDistance)));
            content.style.fontSize = nextSize + "%";
            epubPinchPendingSize = nextSize;
            if (event.cancelable) event.preventDefault();
        }, {passive:false,capture:true});
        const finishPinch = () => {
            if (!iosEpubPinching) return;
            iosEpubPinching = false;
            if (Number.isFinite(epubPinchPendingSize)) {
                epubFontSize = Math.min(220, Math.max(70, Math.round(epubPinchPendingSize / 5) * 5));
                epubPinchPendingSize = null;
                iosEpubApplyReaderAppearance();
                saveState({epub_font_size:epubFontSize});
                showReaderToast("Font size: " + epubFontSize + "%");
            }
        };
        viewer.addEventListener("touchend", event => {
            iosEpubScheduleSelectionCapture();
            if (!event.touches || event.touches.length < 2) finishPinch();
        }, {passive:true,capture:true});
        viewer.addEventListener("touchcancel", finishPinch, {passive:true,capture:true});
        viewer.addEventListener("mouseup", iosEpubScheduleSelectionCapture, {passive:true});
        viewer.addEventListener("contextmenu", iosEpubScheduleSelectionCapture, {passive:true});
    }

    if (area && !area.__pastorDirectEpubScroll) {
        area.__pastorDirectEpubScroll = true;
        area.addEventListener("scroll", () => iosEpubSavePosition(), {passive:true});
    }
    if (!document.__pastorDirectEpubSelection) {
        document.__pastorDirectEpubSelection = true;
        document.addEventListener("selectionchange", iosEpubScheduleSelectionCapture, {passive:true});
    }
}

async function initIosDirectEpubReader() {
    iosDirectEpubActive = true;
    const classic = document.getElementById("epubViewer");
    const direct = document.getElementById("iosEpubViewer");
    if (classic) classic.style.display = "none";
    if (direct) direct.style.display = "block";

    try {
        updateReaderPageControls(1, 0, "Chapter", false);
        const epubData = await fetchArrayBufferWithProgress(MEDIA_URL, "EPUB");
        showReaderLoading("Opening EPUB…", "Preparing the iPhone/iPad compatibility reader…", null, epubData.byteLength, epubData.byteLength);
        await iosEpubPrepareBook(epubData);
        document.getElementById("fontFamilySelect").value = epubFontFamily;
        document.getElementById("lineHeightSelect").value = String(epubLineHeight);
        iosEpubInstallInteractions();

        let initialIndex = 0;
        let initialRatio = 0;
        try {
            const saved = JSON.parse(localStorage.getItem(IOS_EPUB_POSITION_KEY) || "null");
            if (saved && Number.isFinite(Number(saved.index))) {
                initialIndex = Math.max(0, Math.min(iosEpubSpine.length - 1, Number(saved.index)));
                initialRatio = Math.max(0, Math.min(1, Number(saved.ratio || 0)));
            } else if (Number(STATE.progress_percent || 0) > 0) {
                initialIndex = Math.max(0, Math.min(iosEpubSpine.length - 1, Math.floor((Number(STATE.progress_percent) / 100) * iosEpubSpine.length)));
            }
        } catch (error) {}

        if (Number(PIJ_JUMP_SECTION || 0) > 0) {
            initialIndex = Math.max(
                0,
                Math.min(
                    iosEpubSpine.length - 1,
                    Number(PIJ_JUMP_SECTION) - 1
                )
            );
            initialRatio = 0;
        }

        const jumpAnn = annotations.find(a => Number(a.id) === Number(JUMP_ANNOTATION_ID));
        const jumpBm = bookmarks.find(b => Number(b.id) === Number(JUMP_BOOKMARK_ID));
        const annLoc = iosEpubParseRangeLocator(jumpAnn?.locator || "");
        const bmPos = iosEpubParsePositionLocator(jumpBm?.locator || "");
        if (annLoc) { initialIndex = Math.max(0, Math.min(iosEpubSpine.length - 1, Number(annLoc.s || 0))); initialRatio = 0; }
        else if (bmPos) { initialIndex = Math.max(0, Math.min(iosEpubSpine.length - 1, bmPos.index)); initialRatio = bmPos.ratio; }

        await iosEpubRenderChapter(initialIndex, "", initialRatio);
        hideReaderLoading();
        showReaderToast("iPhone/iPad EPUB mode: swipe left/right or tap the far edge to change chapter; long-press to select text.");

        if (annLoc && jumpAnn?.locator) {
            const range = iosEpubRangeFromLocator(jumpAnn.locator, jumpAnn.selected_text || "");
            range?.startContainer?.parentElement?.scrollIntoView?.({block:"center"});
        }
    } catch (error) {
        console.error(error);
        iosDirectEpubActive = false;
        showReaderLoadError(error?.message || "Unable to open this EPUB in iPhone/iPad compatibility mode.");
    }
}

async function initEpubReader() {
    if (IS_IOS_READER) {
        await initIosDirectEpubReader();
        return;
    }

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
        if (!IS_IOS_READER) installEpubRenditionGestureFallback();

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
                } else if (view?.document && !IS_IOS_READER) {
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
        epubTocFlat = flattenEpubToc(navigation.toc || []);
        populateEpubTocControls();

        rendition.on("selected",(cfiRange,contents) => {
            try {
                installEpubContentHandlers(contents);

                const captured = captureEpubSelection(contents, cfiRange);
                if (!captured) {
                    contents?.__pastorScheduleSelectionCapture?.(cfiRange);
                }
            } catch (error) {
                console.warn(error);
            }
        });

        rendition.on("relocated",location => {
            if (!epubNavigationRunning && !epubLayoutRefreshing) {
                setPageBusy(false);
            }
            currentEpubCfi = location.start.cfi;
            currentEpubHref = String(location?.start?.href || location?.end?.href || "");
            updateCurrentChapterHighlight();

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

        if (Number(PIJ_JUMP_SECTION || 0) > 0) {
            try {
                const spineIndex = Math.max(
                    0,
                    Number(PIJ_JUMP_SECTION) - 1
                );
                const spineItem = epubBook.spine.get(spineIndex);

                if (spineItem && spineItem.href) {
                    initialLocation = spineItem.href;
                }
            } catch (error) {
                console.warn(
                    "Unable to jump directly to the Pij EPUB section",
                    error
                );
            }
        }

        const jumpAnn = annotations.find(a => Number(a.id) === Number(JUMP_ANNOTATION_ID));
        const jumpBm = bookmarks.find(b => Number(b.id) === Number(JUMP_BOOKMARK_ID));
        if (jumpAnn && jumpAnn.locator) initialLocation = jumpAnn.locator;
        else if (jumpBm && jumpBm.locator) initialLocation = jumpBm.locator;

        setPageBusy(true,"Preparing first page…");
        await rendition.display(initialLocation);
        applyAllEpubAnnotations();
        hideReaderLoading();
        setPageBusy(false);

        if (IS_IOS_READER) {
            try {
                const hintKey = "pastorEpubIosTapHintV1";
                if (!sessionStorage.getItem(hintKey)) {
                    sessionStorage.setItem(hintKey, "1");
                    showReaderToast("iPhone/iPad: tap the left or right edge to turn pages. Long-press text to select.", 4200);
                }
            } catch (error) {}
        }

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
    if (iosDirectEpubActive) {
        iosEpubApplyReaderAppearance();
        return;
    }
    if (!rendition) return;
    rendition.themes.select(currentTheme || "light");
    rendition.themes.fontSize(epubFontSize + "%");
    rendition.themes.font(epubFontFamily);
    rendition.themes.override("line-height",String(epubLineHeight),true);
}

function changeEpubFont(delta) {
    epubFontSize = Math.min(220,Math.max(70,epubFontSize + delta));
    applyEpubTheme();
    if (!iosDirectEpubActive) scheduleEpubLayoutRefresh("Updating text size…");
    saveState({epub_font_size:epubFontSize});
    showReaderToast("Font size: " + epubFontSize + "%");
}

function setEpubFontFamily(value) {
    epubFontFamily = value;
    applyEpubTheme();
    if (!iosDirectEpubActive) scheduleEpubLayoutRefresh("Updating font…");
    saveState({epub_font_family:value});
}

function setEpubLineHeight(value) {
    epubLineHeight = Number(value || 1.6);
    applyEpubTheme();
    if (!iosDirectEpubActive) scheduleEpubLayoutRefresh("Updating line spacing…");
    saveState({epub_line_height:epubLineHeight});
}

function normalizeEpubHref(value) {
    let raw = String(value || "").trim();
    try { raw = decodeURIComponent(raw); } catch (error) {}
    raw = raw.replace(/\\/g, "/");
    raw = raw.replace(/^\.\//, "");
    while (raw.startsWith("../")) raw = raw.slice(3);
    return raw;
}

function splitEpubHref(value) {
    const raw = normalizeEpubHref(value);
    const hashIndex = raw.indexOf("#");
    if (hashIndex < 0) return {path:raw, fragment:""};
    return {path:raw.slice(0,hashIndex), fragment:raw.slice(hashIndex + 1)};
}

function flattenEpubToc(items, depth=0, output=[]) {
    (items || []).forEach(item => {
        output.push({
            label:String(item?.label || item?.href || "Untitled section").trim(),
            href:String(item?.href || "").trim(),
            depth:Number(depth || 0)
        });
        if (Array.isArray(item?.subitems) && item.subitems.length) {
            flattenEpubToc(item.subitems, depth + 1, output);
        }
    });
    return output;
}

function populateEpubTocControls() {
    const toc = document.getElementById("tocSelect");
    if (toc) {
        toc.innerHTML = '<option value="">Table of Contents</option>';
        epubTocFlat.forEach((item,index) => {
            const option = document.createElement("option");
            option.value = String(index);
            option.textContent = (item.depth ? "— ".repeat(Math.min(3,item.depth)) : "") + item.label;
            toc.appendChild(option);
        });
    }
    renderQuickChapterList();
}

function renderQuickChapterList() {
    const list = document.getElementById("readerChapterList");
    const heading = document.getElementById("readerChapterHeading");
    if (!list) return;
    list.innerHTML = "";

    if (READER_FORMAT === "PDF") {
        if (heading) heading.textContent = "Chapters";
        if (!pdfOutlineFlat.length) {
            list.innerHTML = '<div class="reader-chapter-empty">This PDF does not provide embedded chapter bookmarks.</div>';
            return;
        }
        pdfOutlineFlat.forEach((item,index) => {
            const button = document.createElement("button");
            button.type = "button";
            button.className = "reader-chapter-item";
            button.dataset.index = String(index);
            button.dataset.page = String(item.page || "");
            button.style.paddingLeft = (10 + Math.min(4,item.depth) * 16) + "px";
            button.textContent = item.label + (item.page ? "  ·  p. " + item.page : "");
            button.onclick = () => openPdfOutlineItem(index, true);
            list.appendChild(button);
        });
        updateCurrentChapterHighlight();
        return;
    }

    if (heading) heading.textContent = "Chapters";
    if (!epubTocFlat.length) {
        list.innerHTML = '<div class="reader-chapter-empty">This EPUB does not provide a table of contents.</div>';
        return;
    }
    epubTocFlat.forEach((item,index) => {
        const button = document.createElement("button");
        button.type = "button";
        button.className = "reader-chapter-item";
        button.dataset.index = String(index);
        button.dataset.href = item.href;
        button.style.paddingLeft = (10 + Math.min(4,item.depth) * 16) + "px";
        button.textContent = item.label;
        button.onclick = () => openEpubTocItem(index, true);
        list.appendChild(button);
    });
    updateCurrentChapterHighlight();
}

function epubHrefLooksSame(a,b) {
    const left = splitEpubHref(a).path.toLowerCase();
    const right = splitEpubHref(b).path.toLowerCase();
    if (!left || !right) return false;
    return left === right || left.endsWith("/" + right) || right.endsWith("/" + left);
}

function updateCurrentChapterHighlight() {
    const buttons = document.querySelectorAll(".reader-chapter-item");
    let best = -1;

    if (READER_FORMAT === "PDF") {
        pdfOutlineFlat.forEach((item,index) => {
            if (Number(item.page || 0) <= Number(pdfPageNumber || 1)) best = index;
        });
    } else {
        epubTocFlat.forEach((item,index) => {
            if (epubHrefLooksSame(item.href, currentEpubHref)) best = index;
        });
    }

    buttons.forEach(button => {
        button.classList.toggle("current", Number(button.dataset.index) === best);
    });
}

function toggleChapterPanel(force) {
    const sheet = document.getElementById("readerChapterSheet");
    const backdrop = document.getElementById("readerChapterBackdrop");
    if (!sheet || !backdrop) return;
    const open = typeof force === "boolean" ? force : !sheet.classList.contains("open");
    if (open) toggleToolsPanel(false);
    sheet.classList.toggle("open", open);
    backdrop.classList.toggle("show", open);
    if (open) {
        renderQuickChapterList();
        updateCurrentChapterHighlight();
    }
}

function epubDisplayCandidates(rawHref) {
    const candidates = [];
    const push = value => {
        if (value === null || value === undefined || value === "") return;
        const key = String(value);
        if (!candidates.some(existing => String(existing) === key)) candidates.push(value);
    };

    const raw = String(rawHref || "").trim();
    const decoded = normalizeEpubHref(raw);
    const parts = splitEpubHref(decoded);
    const hash = parts.fragment ? "#" + parts.fragment : "";

    push(raw);
    push(decoded);

    const spineItems = epubBook?.spine?.spineItems || [];
    let matched = null;

    // First let EPUB.js resolve the target using its own spine lookup.
    for (const candidate of [raw, decoded, parts.path]) {
        try {
            const found = epubBook?.spine?.get?.(candidate);
            if (found) { matched = found; break; }
        } catch (error) {}
    }

    // Some EPUBs put OEBPS/Text/... in the spine while the nav document uses
    // Text/... or ../Text/.... Match by normalized suffix as a fallback.
    if (!matched && parts.path) {
        const wanted = normalizeEpubHref(parts.path).toLowerCase();
        matched = spineItems.find(item => {
            const href = normalizeEpubHref(item?.href || item?.url || "").toLowerCase();
            return href === wanted || href.endsWith("/" + wanted) || wanted.endsWith("/" + href);
        }) || null;
    }

    if (matched) {
        push(String(matched.href || matched.url || "") + hash);
        if (Number.isFinite(Number(matched.index))) push(Number(matched.index));
        try { if (matched.cfiBase) push(matched.cfiBase); } catch (error) {}
    }

    return candidates;
}

async function displayEpubHref(rawHref) {
    if (!rendition || !rawHref) return false;
    const candidates = epubDisplayCandidates(rawHref);
    let lastError = null;
    for (const candidate of candidates) {
        try {
            await Promise.resolve(rendition.display(candidate));
            return true;
        } catch (error) {
            lastError = error;
        }
    }
    if (lastError) console.warn("EPUB TOC navigation failed", lastError);
    return false;
}

async function openEpubTocItem(indexOrHref, closePanel=false) {
    let item = null;
    const index = Number(indexOrHref);
    if (Number.isInteger(index) && index >= 0 && index < epubTocFlat.length) {
        item = epubTocFlat[index];
    } else {
        item = {label:"Section", href:String(indexOrHref || "")};
    }
    if (!item?.href) return;

    if (iosDirectEpubActive) {
        hideSelectionBarOnly();
        const opened = item.spineIndex >= 0
            ? await iosEpubRenderChapter(item.spineIndex, iosEpubSplitReference(item.href).fragment, 0)
            : await iosEpubOpenHref(item.href);
        if (!opened) showReaderToast("That chapter could not be opened in this EPUB.");
        else if (closePanel) toggleChapterPanel(false);
        return;
    }

    if (!rendition) return;
    epubNavigationQueue = [];
    hideSelectionBarOnly();
    setPageBusy(true,"Opening " + (item.label || "section") + "…");
    const opened = await displayEpubHref(item.href);
    setPageBusy(false);

    if (!opened) {
        showReaderToast("That chapter could not be opened in this EPUB.");
    } else if (closePanel) {
        toggleChapterPanel(false);
    }
}

function jumpToc(value) {
    if (value === "" || value === null || value === undefined) return;
    openEpubTocItem(value, false);
}

function epubAnnotationKind(item) {
    return item?.annotation_type === "underline" ? "underline" : "highlight";
}

function removeEpubAnnotationVisual(item) {
    if (!item) return;

    if (iosDirectEpubActive && String(item.locator || "").startsWith("iosdom:")) {
        iosEpubRemoveAnnotationVisual(item);
        return;
    }

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
    if (READER_FORMAT !== "EPUB") return;
    if (iosDirectEpubActive) {
        // Rebuild the visible iOS chapter from the clean EPUB source. This is
        // more reliable than trying to surgically unwrap a stale Safari
        // highlight/span after deletion.
        const area = document.getElementById("readerCanvasArea");
        const max = Math.max(
            1,
            Number(area?.scrollHeight || 1) - Number(area?.clientHeight || 0)
        );
        const ratio = Math.max(
            0,
            Math.min(1, Number(area?.scrollTop || 0) / max)
        );

        await iosEpubRenderChapter(iosEpubSpineIndex, "", ratio);
        return;
    }
    if (!rendition) return;

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
    if (iosDirectEpubActive) {
        if (String(item?.locator || "").startsWith("iosdom:")) iosEpubApplyAnnotation(item);
        return;
    }
    if (!rendition || !item.locator || String(item.locator).startsWith("iosdom:") || appliedEpubAnnotationIds.has(Number(item.id))) return;

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

function queueEpubSwipeFromGesture(direction) {
    if (!rendition) return;
    if (selectionIsActive(lastEpubContents?.window || window)) return;

    // Multiple iOS/EPUB.js event paths may report the same physical swipe.
    // Keep one page turn while still allowing normal repeated swipes.
    const now = Date.now();
    if (now - epubLastSwipeAt < 360) return;
    epubLastSwipeAt = now;

    hideSelectionBarOnly();
    queueEpubNavigation(direction);
}

function installEpubRenditionGestureFallback() {
    if (!rendition || epubRenditionGestureInstalled) return;
    epubRenditionGestureInstalled = true;

    let startX = 0;
    let startY = 0;
    let startTime = 0;
    let tracking = false;

    const pointFromEvent = event => {
        if (event?.touches?.length && event.touches.length !== 1) return null;
        if (event?.changedTouches?.length && event.changedTouches.length !== 1) return null;
        const touch = event?.changedTouches?.[0] || event?.touches?.[0] || event;
        if (!touch) return null;
        const x = Number(touch.clientX);
        const y = Number(touch.clientY);
        return Number.isFinite(x) && Number.isFinite(y) ? {x,y} : null;
    };

    rendition.on("touchstart", event => {
        if (selectionIsActive(lastEpubContents?.window || window)) return;
        const point = pointFromEvent(event);
        if (!point) { tracking = false; return; }
        startX = point.x;
        startY = point.y;
        startTime = Date.now();
        tracking = true;
    });

    rendition.on("touchend", event => {
        if (!tracking) return;
        tracking = false;
        const point = pointFromEvent(event);
        if (!point || selectionIsActive(lastEpubContents?.window || window)) return;
        const dx = point.x - startX;
        const dy = point.y - startY;
        const elapsed = Date.now() - startTime;
        if (elapsed <= 1100 && Math.abs(dx) >= 44 && Math.abs(dx) >= Math.abs(dy) * 1.12) {
            queueEpubSwipeFromGesture(dx < 0 ? 1 : -1);
        }
    });
}

function installEpubSwipeHandlers(doc, win=window) {
    if (!doc || SWIPE_INSTALLED.has(doc)) return;
    SWIPE_INSTALLED.add(doc);

    let startX = 0;
    let startY = 0;
    let lastX = 0;
    let lastY = 0;
    let startTime = 0;
    let tracking = false;
    let multiTouch = false;

    const reset = () => {
        tracking = false;
        multiTouch = false;
    };

    const begin = event => {
        if (!event?.touches) return;
        if (event.touches.length !== 1) {
            multiTouch = event.touches.length > 1;
            tracking = false;
            return;
        }
        if (selectionIsActive(win)) return;
        const touch = event.touches[0];
        const viewportWidth = Number(win?.innerWidth || window.innerWidth || 0);
        if (viewportWidth > 0 && (touch.clientX < 18 || touch.clientX > viewportWidth - 18)) return;
        startX = lastX = Number(touch.clientX || 0);
        startY = lastY = Number(touch.clientY || 0);
        startTime = Date.now();
        tracking = true;
        multiTouch = false;
    };

    const move = event => {
        if (!tracking || !event?.touches || event.touches.length !== 1) return;
        const touch = event.touches[0];
        lastX = Number(touch.clientX || 0);
        lastY = Number(touch.clientY || 0);
        // Do not preventDefault here. iOS long-press selection needs the native
        // touch stream to remain intact. We decide whether it was a swipe only
        // after the finger is released.
    };

    const finishAt = (x,y) => {
        if (!tracking || multiTouch) { reset(); return; }
        const dx = Number(x || lastX) - startX;
        const dy = Number(y || lastY) - startY;
        const elapsed = Date.now() - startTime;
        reset();
        if (selectionIsActive(win)) return;
        if (elapsed > 1100) return;
        if (Math.abs(dx) < 44) return;
        if (Math.abs(dx) < Math.abs(dy) * 1.12) return;
        queueEpubSwipeFromGesture(dx < 0 ? 1 : -1);
    };

    const end = event => {
        if (!tracking) return;
        const touch = event?.changedTouches?.[0];
        finishAt(touch?.clientX ?? lastX, touch?.clientY ?? lastY);
    };

    const cancel = () => {
        // WebKit sometimes converts a completed iframe swipe into touchcancel.
        if (tracking && !multiTouch) finishAt(lastX,lastY);
        else reset();
    };

    [win, doc, doc.body].filter(Boolean).forEach(target => {
        target.addEventListener("touchstart", begin, {passive:true,capture:true});
        target.addEventListener("touchmove", move, {passive:true,capture:true});
        target.addEventListener("touchend", end, {passive:true,capture:true});
        target.addEventListener("touchcancel", cancel, {passive:true,capture:true});
    });

    // Pointer fallback remains useful on Android/desktop touch devices, but it
    // never suppresses the iOS native text-selection path.
    if (window.PointerEvent) {
        let pointerId = null;
        let pointerTracking = false;
        let px = 0, py = 0, pLastX = 0, pLastY = 0, pStart = 0;
        const pDown = event => {
            if (event.pointerType === "mouse" || selectionIsActive(win)) return;
            pointerId = event.pointerId;
            px = pLastX = event.clientX;
            py = pLastY = event.clientY;
            pStart = Date.now();
            pointerTracking = true;
        };
        const pMove = event => {
            if (!pointerTracking || event.pointerId !== pointerId) return;
            pLastX = event.clientX; pLastY = event.clientY;
        };
        const pUp = event => {
            if (!pointerTracking || event.pointerId !== pointerId) return;
            pointerTracking = false;
            const dx = event.clientX - px;
            const dy = event.clientY - py;
            if (Date.now()-pStart <= 1100 && Math.abs(dx)>=44 && Math.abs(dx)>=Math.abs(dy)*1.12 && !selectionIsActive(win)) {
                queueEpubSwipeFromGesture(dx < 0 ? 1 : -1);
            }
        };
        const pCancel = () => { pointerTracking = false; pointerId = null; };
        [win, doc].filter(Boolean).forEach(target => {
            target.addEventListener("pointerdown", pDown, {passive:true,capture:true});
            target.addEventListener("pointermove", pMove, {passive:true,capture:true});
            target.addEventListener("pointerup", pUp, {passive:true,capture:true});
            target.addEventListener("pointercancel", pCancel, {passive:true,capture:true});
        });
    }
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
    } else if (iosDirectEpubActive) {
        iosEpubGo(-1);
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
    } else if (iosDirectEpubActive) {
        iosEpubGo(1);
    } else if (rendition) {
        queueEpubNavigation(1);
    }
}


function countTextOccurrences(value, query) {
    const textValue = String(value || "").toLowerCase();
    const needle = String(query || "").toLowerCase();
    if (!needle) return 0;

    let count = 0;
    let from = 0;
    while (from <= textValue.length - needle.length) {
        const at = textValue.indexOf(needle, from);
        if (at < 0) break;
        count += 1;
        from = at + Math.max(1, needle.length);
    }
    return count;
}

function clearPdfSearchHighlights() {
    const layer = document.getElementById("pdfTextLayer");
    if (!layer) return;

    layer.querySelectorAll("mark.reader-search-hit, mark.reader-search-hit-active").forEach(mark => {
        const parent = mark.parentNode;
        if (!parent) return;
        parent.replaceChild(document.createTextNode(mark.textContent || ""), mark);
        parent.normalize?.();
    });
}

function applyPdfSearchHighlights() {
    const layer = document.getElementById("pdfTextLayer");
    const query = String(activeSearchQuery || "").trim();
    if (!layer || !query) return;

    clearPdfSearchHighlights();

    const activeMatch = searchMatches[searchMatchIndex] || null;
    const activeOccurrence = Number(activeMatch?.page) === Number(pdfPageNumber)
        ? Number(activeMatch?.pdfOccurrence ?? -1)
        : -1;

    const nodes = [];
    const walker = document.createTreeWalker(layer, NodeFilter.SHOW_TEXT);
    let node;
    while ((node = walker.nextNode())) {
        if (String(node.nodeValue || "").toLowerCase().includes(query.toLowerCase())) {
            nodes.push(node);
        }
    }

    let occurrence = 0;
    let activeMark = null;
    const needle = query.toLowerCase();

    nodes.forEach(textNode => {
        const original = String(textNode.nodeValue || "");
        const lower = original.toLowerCase();
        let from = 0;
        let at = lower.indexOf(needle, from);
        if (at < 0) return;

        const fragment = document.createDocumentFragment();

        while (at >= 0) {
            if (at > from) {
                fragment.appendChild(document.createTextNode(original.slice(from, at)));
            }

            const mark = document.createElement("mark");
            const isActive = occurrence === activeOccurrence;
            mark.className = isActive
                ? "reader-search-hit reader-search-hit-active"
                : "reader-search-hit";
            mark.textContent = original.slice(at, at + query.length);
            fragment.appendChild(mark);

            if (isActive) activeMark = mark;

            occurrence += 1;
            from = at + query.length;
            at = lower.indexOf(needle, from);
        }

        if (from < original.length) {
            fragment.appendChild(document.createTextNode(original.slice(from)));
        }

        textNode.parentNode?.replaceChild(fragment, textNode);
    });

    if (activeMark) {
        requestAnimationFrame(() => {
            try {
                activeMark.scrollIntoView({
                    block:"center",
                    inline:"center",
                    behavior:"smooth"
                });
            } catch (error) {}
        });
    }
}

function clearIosEpubSearchHighlights() {
    try { window.CSS?.highlights?.delete?.("pastor-ios-search-hit"); } catch (error) {}
    try { window.CSS?.highlights?.delete?.("pastor-ios-search-active"); } catch (error) {}

    document.querySelectorAll(
        "#iosEpubContent mark.reader-search-hit, #iosEpubContent mark.reader-search-hit-active"
    ).forEach(mark => {
        const parent = mark.parentNode;
        if (!parent) return;
        parent.replaceChild(document.createTextNode(mark.textContent || ""), mark);
        parent.normalize?.();
    });

    iosEpubSearchFallbackMarks = [];
}

function iosEpubCollectSearchRanges(query) {
    const root = document.getElementById("iosEpubContent");
    const needle = String(query || "").trim().toLowerCase();
    if (!root || !needle) return [];

    const ranges = [];
    const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
    let node;

    while ((node = walker.nextNode())) {
        const parent = node.parentElement;
        if (
            parent?.closest?.("script,style,noscript,template")
            || parent?.closest?.("mark.reader-search-hit,mark.reader-search-hit-active")
        ) {
            continue;
        }

        const original = String(node.nodeValue || "");
        const lower = original.toLowerCase();
        let from = 0;

        while (from <= lower.length - needle.length) {
            const at = lower.indexOf(needle, from);
            if (at < 0) break;

            try {
                const range = document.createRange();
                range.setStart(node, at);
                range.setEnd(node, at + query.length);
                ranges.push(range);
            } catch (error) {}

            from = at + Math.max(1, needle.length);
        }
    }

    return ranges;
}

function applyIosEpubSearchHighlights(query, activeOccurrence=-1) {
    if (!iosDirectEpubActive) return;

    clearIosEpubSearchHighlights();

    const ranges = iosEpubCollectSearchRanges(query);
    if (!ranges.length) return;

    const requestedOccurrence = Number(activeOccurrence);
    const activeIndex = Number.isFinite(requestedOccurrence) && requestedOccurrence >= 0
        ? Math.max(0, Math.min(ranges.length - 1, requestedOccurrence))
        : -1;
    const activeRange = activeIndex >= 0 ? ranges[activeIndex] : null;

    if (window.CSS?.highlights && window.Highlight) {
        try {
            CSS.highlights.set(
                "pastor-ios-search-hit",
                new Highlight(...ranges)
            );
            if (activeRange) {
                CSS.highlights.set(
                    "pastor-ios-search-active",
                    new Highlight(activeRange)
                );
            }

            if (activeRange) requestAnimationFrame(() => {
                try {
                    const target = activeRange.startContainer?.parentElement;
                    target?.scrollIntoView?.({
                        block:"center",
                        inline:"nearest",
                        behavior:"smooth"
                    });
                } catch (error) {}
            });
            return;
        } catch (error) {
            console.warn("iOS EPUB search CSS Highlight fallback", error);
        }
    }

    // Fallback for browsers without the CSS Highlight API. Work from the end
    // of the document so wrapping one match cannot invalidate later ranges.
    const ordered = ranges
        .map((range, index) => ({range,index}))
        .reverse();

    ordered.forEach(({range,index}) => {
        try {
            const mark = document.createElement("mark");
            mark.className = index === activeIndex
                ? "reader-search-hit reader-search-hit-active"
                : "reader-search-hit";
            range.surroundContents(mark);
            iosEpubSearchFallbackMarks.push(mark);
        } catch (error) {}
    });

    const activeMark = document.querySelector(
        "#iosEpubContent mark.reader-search-hit-active"
    );
    activeMark?.scrollIntoView?.({
        block:"center",
        inline:"nearest",
        behavior:"smooth"
    });
}

function clearClassicEpubSearchHighlight() {
    if (!classicEpubSearchMarkCfi || !rendition) {
        classicEpubSearchMarkCfi = "";
        return;
    }

    try {
        rendition.annotations.remove(classicEpubSearchMarkCfi, "mark");
    } catch (error) {}

    classicEpubSearchMarkCfi = "";
}

function applyClassicEpubSearchHighlight(cfi) {
    clearClassicEpubSearchHighlight();

    const locator = String(cfi || "");
    if (!locator || !rendition) return;

    try {
        rendition.annotations.mark(
            locator,
            {pastorSearch:true},
            null,
            "pr-search-active",
            {
                "fill":"#ff8a00",
                "fill-opacity":"0.72",
                "mix-blend-mode":"multiply"
            }
        );
        classicEpubSearchMarkCfi = locator;
    } catch (error) {
        console.warn("EPUB search result could not be highlighted", error);
    }
}

function clearReaderSearchHighlights() {
    clearPdfSearchHighlights();
    clearIosEpubSearchHighlights();
    clearClassicEpubSearchHighlight();
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
        applyPdfSearchHighlights();
    } else if (
        iosDirectEpubActive
        && Number.isFinite(Number(match.iosSpineIndex))
    ) {
        await iosEpubRenderChapter(
            Number(match.iosSpineIndex),
            "",
            0
        );
        applyIosEpubSearchHighlights(
            activeSearchQuery,
            Number(match.iosOccurrence ?? 0)
        );
    } else if (rendition && match.cfi) {
        epubNavigationQueue = [];
        setPageBusy(true,"Opening search result…");
        try {
            await Promise.resolve(rendition.display(match.cfi));
            applyClassicEpubSearchHighlight(match.cfi);
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

    clearReaderSearchHighlights();
    activeSearchQuery = query;
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

            let occurrenceOnPage = 0;
            let directMatches = 0;

            for (const item of content.items) {
                const itemText = String(item.str || "");
                const count = countTextOccurrences(itemText, q);

                for (let n=0; n<count; n++) {
                    searchMatches.push({
                        page:i,
                        pdfOccurrence:occurrenceOnPage,
                        query
                    });
                    occurrenceOnPage += 1;
                    directMatches += 1;
                }
            }

            // Fallback for a phrase split across PDF text items. We can still
            // navigate to the correct page even when PDF.js split the phrase
            // into separate positioned spans.
            if (!directMatches) {
                const pageText = content.items
                    .map(item => item.str || "")
                    .join(" ")
                    .toLowerCase();

                if (pageText.includes(q)) {
                    searchMatches.push({
                        page:i,
                        pdfOccurrence:0,
                        query
                    });
                }
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

    if (iosDirectEpubActive) {
        try {
            for (let i=0; i<iosEpubSpine.length; i++) {
                const file = iosEpubZip?.file(iosEpubSpine[i].path);
                if (!file) continue;

                const source = await file.async("text");
                const doc = new DOMParser().parseFromString(source, "text/html");
                const chapterText = String(
                    doc.body?.textContent
                    || doc.documentElement?.textContent
                    || ""
                ).replace(/\s+/g," ");

                const count = countTextOccurrences(chapterText, query);

                for (let occurrence=0; occurrence<count; occurrence++) {
                    searchMatches.push({
                        iosSpineIndex:i,
                        iosOccurrence:occurrence,
                        query,
                        excerpt:chapterText.slice(0,180)
                    });
                }
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
                    excerpt:match.excerpt || "",
                    query
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
    } else if (READER_FORMAT === "EPUB" && item.locator && iosDirectEpubActive) {
        const loc = iosEpubParseRangeLocator(item.locator);
        if (loc) {
            iosEpubRenderChapter(Number(loc.s || 0), "", 0).then(() => {
                const range = iosEpubRangeFromLocator(item.locator, item.selected_text || "");
                range?.startContainer?.parentElement?.scrollIntoView?.({block:"center"});
            });
        }
    } else if (READER_FORMAT === "EPUB" && item.locator && rendition && !String(item.locator).startsWith("iosdom:")) {
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
    } else if (READER_FORMAT === "EPUB" && item.locator && iosDirectEpubActive) {
        const pos = iosEpubParsePositionLocator(item.locator);
        if (pos) iosEpubRenderChapter(pos.index, "", pos.ratio);
        else {
            const loc = iosEpubParseRangeLocator(item.locator);
            if (loc) iosEpubRenderChapter(Number(loc.s || 0), "", 0);
        }
    } else if (READER_FORMAT === "EPUB" && item.locator && rendition && !String(item.locator).startsWith("iosdom")) {
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

    installReaderBrowserZoomLock();
    syncReaderViewport();

    if (READER_FORMAT === "PDF") {
        installSwipeHandlers(canvasArea, window);
        installPdfPinchZoom(canvasArea);
    } else {
        const epubViewer = document.getElementById("epubViewer");
        if (IS_IOS_READER) {
            if (epubViewer) epubViewer.style.display = "none";
        } else {
            installSwipeHandlers(epubViewer, window);
        }
    }

    document.addEventListener("keydown", event => {
        const target = event.target;
        const tag = String(target?.tagName || "").toLowerCase();

        if (event.key === "Escape") {
            toggleChapterPanel(false);
        }

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

    let lastReaderLayoutWidth = Math.round(
        window.visualViewport?.width
        || window.innerWidth
        || document.documentElement.clientWidth
        || 0
    );

    const refreshReaderViewport = () => {
        syncReaderViewport();

        const currentWidth = Math.round(
            window.visualViewport?.width
            || window.innerWidth
            || document.documentElement.clientWidth
            || 0
        );
        const widthChanged = Math.abs(currentWidth - lastReaderLayoutWidth) > 2;
        lastReaderLayoutWidth = currentWidth;

        setTimeout(() => {
            if (READER_FORMAT === "PDF") {
                if (pdfAutoFitWidth && widthChanged) {
                    renderPdfPage();
                }
                return;
            }

            if (iosDirectEpubActive) {
                iosEpubApplyReaderAppearance();
                return;
            }
            try { rendition?.resize?.(); } catch (error) {}
        }, 70);
    };

    const refreshReaderViewportPositionOnly = () => {
        syncReaderViewport();
    };

    syncReaderViewport();

    if (window.visualViewport) {
        window.visualViewport.addEventListener("resize", refreshReaderViewport, {passive:true});
        window.visualViewport.addEventListener("scroll", refreshReaderViewportPositionOnly, {passive:true});
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


def trigger_pij_library_index():
    """
    Start (or queue) the incremental Pij ebook-text index after the visible
    Pastor's Resources catalog has synchronized.

    The import is deliberately lazy because pij_library_knowledge imports this
    module for Drive/file helpers. Importing it here avoids a circular import
    during Flask startup.
    """
    try:
        from pij_library_knowledge import (
            get_index_state,
            start_public_library_index,
        )

        started = start_public_library_index(
            force=False,
            queue_if_running=True,
        )
        state = get_index_state()

        return {
            "ok": True,
            "started": bool(started),
            "queued": bool(state.get("queued")),
            "running": bool(state.get("running")),
            "state": state,
        }

    except Exception as error:
        print(
            "[Pastor Resources -> Pij Index WARNING] "
            + str(error),
            flush=True,
        )
        return {
            "ok": False,
            "started": False,
            "queued": False,
            "running": False,
            "error": str(error),
        }


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

                # Keep Pij's searchable ebook text in step with the visible
                # library. If another AI index is already running, the helper
                # queues one more incremental pass instead of starting a
                # competing writer.
                pij_index = trigger_pij_library_index()
                stats = dict(stats or {})
                stats["pij_index_started"] = bool(
                    pij_index.get("started")
                )
                stats["pij_index_queued"] = bool(
                    pij_index.get("queued")
                )
                stats["pij_index_ok"] = bool(
                    pij_index.get("ok")
                )

            update_resource_sync_state(
                running=False,
                stage="complete",
                message=(
                    "Library catalog sync complete. "
                    + str(
                        stats.get(
                            "unique_books",
                            0,
                        )
                    )
                    + " unique books cataloged. "
                    + (
                        "Pij AI indexing is running."
                        if stats.get("pij_index_ok")
                        else "Pij AI indexing could not be started automatically."
                    )
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
        "logical",
        "pdf",
        "epub",
        "ai_indexed",
        "ai_not_indexed",
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
                COALESCE(b.hidden_by, '') AS hidden_by,

                COALESCE((
                    SELECT MAX(CASE WHEN d.searchable=1 AND d.chunk_count>0 THEN 1 ELSE 0 END)
                    FROM pij_library_documents d
                    WHERE d.source_type='public_ebook' AND d.book_id=b.id
                ),0) AS ai_indexed,

                COALESCE((
                    SELECT MAX(d.page_count)
                    FROM pij_library_documents d
                    WHERE d.source_type='public_ebook' AND d.book_id=b.id
                ),0) AS ai_page_count,

                COALESCE((
                    SELECT SUM(d.chunk_count)
                    FROM pij_library_documents d
                    WHERE d.source_type='public_ebook' AND d.book_id=b.id
                ),0) AS ai_chunk_count,

                COALESCE((
                    SELECT MAX(d.indexed_at)
                    FROM pij_library_documents d
                    WHERE d.source_type='public_ebook' AND d.book_id=b.id
                ),'') AS ai_indexed_at,

                COALESCE((
                    SELECT MAX(d.extract_error)
                    FROM pij_library_documents d
                    WHERE d.source_type='public_ebook' AND d.book_id=b.id
                      AND COALESCE(d.extract_error,'')<>''
                ),'') AS ai_extract_error

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
                include = (not item["is_current"])

            elif view == "logical":
                include = item["is_current"]

            elif view == "pdf":
                include = (
                    item["is_current"]
                    and str(item.get("format") or "").lower() == "pdf"
                )

            elif view == "epub":
                include = (
                    item["is_current"]
                    and str(item.get("format") or "").lower() == "epub"
                )

            elif view == "ai_indexed":
                include = (
                    item["is_current"]
                    and not item["is_hidden"]
                    and bool(int(item.get("ai_indexed") or 0))
                )

            elif view == "ai_not_indexed":
                include = (
                    item["is_current"]
                    and not item["is_hidden"]
                    and not bool(int(item.get("ai_indexed") or 0))
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

        # Book-level views list each logical book once.
        if view in {"logical", "hidden", "ai_indexed", "ai_not_indexed"}:
            grouped = {}
            for item in items:
                group_key = item.get("book_id")
                if group_key is None:
                    group_key = "file:" + str(item.get("database_file_id") or "")
                previous = grouped.get(group_key)
                if (
                    previous is None
                    or (
                        str(item.get("format") or "").lower() == "pdf"
                        and str(previous.get("format") or "").lower() != "pdf"
                    )
                ):
                    grouped[group_key] = item
            items = list(grouped.values())

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

            "ai_indexed_books":
                len({
                    int(item["book_id"])
                    for item in current_rows
                    if item.get("book_id") is not None
                    and not int(item.get("is_hidden") or 0)
                    and int(item.get("ai_indexed") or 0)
                }),

            "ai_not_indexed_books":
                len({
                    int(item["book_id"])
                    for item in current_rows
                    if item.get("book_id") is not None
                    and not int(item.get("is_hidden") or 0)
                    and not int(item.get("ai_indexed") or 0)
                }),
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
{% block title %}Database Details - Pastor's Resources{% endblock %}
{% block content %}
<style>
.app-main{max-width:1550px;padding:0}.db-page{padding:16px 14px 60px;color:#10213d;font-family:Arial,sans-serif}
.db-hero{padding:24px 28px;border-radius:22px;background:linear-gradient(135deg,#f8fbff,#f7f5ff 48%,#fff7fb);border:1px solid rgba(15,23,42,.07);box-shadow:0 10px 30px rgba(15,23,42,.06)}
.db-kicker{color:#8b6591;font-size:11px;font-weight:800;letter-spacing:.07em;text-transform:uppercase}.db-title{margin:6px 0 0;font:700 40px/1.05 Georgia,serif}.db-subtitle{max-width:980px;margin:10px 0 0;color:#607292;font-size:13px;line-height:1.55}
.db-actions{display:flex;flex-wrap:wrap;gap:9px;margin-top:18px}.db-action{display:inline-flex;align-items:center;justify-content:center;min-height:45px;padding:10px 14px;border:0;border-radius:12px;background:#fff;color:#3e5578;text-decoration:none;font-size:12px;font-weight:800;cursor:pointer;box-shadow:0 5px 16px rgba(15,23,42,.07)}.db-action.primary{color:#fff;background:linear-gradient(135deg,#b27db9,#7297de)}.db-action:disabled{opacity:.6}
.db-summary{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px;margin-top:16px}.db-stat{min-width:0;padding:14px;border-radius:16px;background:#fff;border:1px solid rgba(15,23,42,.07);box-shadow:0 6px 18px rgba(15,23,42,.045);text-align:left}.db-stat.click{cursor:pointer}.db-stat.click:hover{transform:translateY(-1px)}.db-stat.active{outline:2px solid rgba(112,148,220,.45)}.db-value{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:24px;font-weight:900}.db-label{margin-top:3px;color:#8796ad;font-size:10px;font-weight:800;text-transform:uppercase;letter-spacing:.04em}
.db-note{margin-top:14px;padding:13px 15px;border-radius:14px;background:#fff9e9;color:#755b1e;border:1px solid #f0d985;font-size:11px;line-height:1.5}
.db-browser{display:none;margin-top:18px}.db-browser.open{display:block}.db-head{display:flex;flex-wrap:wrap;gap:10px;align-items:center;justify-content:space-between;margin-bottom:10px}.db-head h2{margin:0;font:700 22px Georgia,serif}.db-tools{display:flex;flex-wrap:wrap;gap:8px}.db-input,.db-select{min-height:42px;border:1px solid #dbe3ee;border-radius:11px;padding:9px 11px;background:#fff;color:#334155;font-size:12px;font-weight:700}.db-input{width:min(420px,80vw)}
.db-list{display:grid;gap:8px}.db-row{background:#fff;border:1px solid rgba(15,23,42,.08);border-radius:14px;overflow:hidden;box-shadow:0 5px 16px rgba(15,23,42,.04)}.db-rowbtn{width:100%;border:0;background:#fff;padding:13px 14px;text-align:left;cursor:pointer;display:flex;gap:12px;align-items:center}.db-main{min-width:0;flex:1}.db-book{font:700 15px/1.3 Georgia,serif}.db-sub{margin-top:4px;color:#7b879a;font-size:10px;overflow-wrap:anywhere}.db-badges{display:flex;flex-wrap:wrap;gap:5px;margin-top:7px}.badge{padding:4px 7px;border-radius:999px;background:#eef2f8;color:#59677f;font-size:8px;font-weight:900}.pdf{background:#fff0f0;color:#b53b3b}.epub{background:#eef8f2;color:#28845a}.aiyes{background:#e9f8ef;color:#18754a}.aino{background:#fff1e8;color:#a45718}.warn{background:#f2ecff;color:#7049a4}.chev{font-size:18px;color:#9aa7b9}
.db-details{display:none;padding:0 14px 14px}.db-row.open .db-details{display:block}.db-grid{display:grid;grid-template-columns:1fr;gap:7px}.db-detail{padding:9px 10px;border-radius:10px;background:#f8fafc;min-width:0}.db-dlabel{font-size:8px;font-weight:900;text-transform:uppercase;letter-spacing:.04em;color:#9aa5b5}.db-dvalue{margin-top:3px;color:#44526a;font-size:10px;line-height:1.45;overflow-wrap:anywhere}.copy{margin-left:5px;border:0;border-radius:7px;padding:3px 6px;background:#e9eef7;color:#586980;font-size:8px;font-weight:800;cursor:pointer}
.db-empty{padding:30px 15px;border-radius:15px;background:#fff;text-align:center;color:#64748b}.db-pages{display:flex;justify-content:center;align-items:center;gap:8px;margin-top:16px}.db-pbtn{min-height:38px;border:0;border-radius:10px;padding:8px 11px;background:#fff;color:#576780;font-size:10px;font-weight:800;cursor:pointer;box-shadow:0 4px 14px rgba(15,23,42,.07)}.db-pbtn:disabled{opacity:.4}.db-toast{position:fixed;left:12px;right:12px;bottom:14px;z-index:12000;display:none;padding:12px 14px;border-radius:12px;background:#111827;color:#fff;font-size:11px}
@media(min-width:700px){.db-summary{grid-template-columns:repeat(4,minmax(0,1fr))}.db-grid{grid-template-columns:repeat(2,minmax(0,1fr))}}@media(min-width:1050px){.db-summary{grid-template-columns:repeat(5,minmax(0,1fr))}.db-grid{grid-template-columns:repeat(4,minmax(0,1fr))}.db-toast{left:auto;right:20px;width:360px}}
</style>

<div class="db-page">
<section class="db-hero">
<div class="db-kicker">🔐 Private administrator / developer view</div>
<h1 class="db-title">Database Details</h1>
<p class="db-subtitle">Inspect the PDF and EPUB files recorded from the most recent Google Drive synchronization, including upload dates, file IDs, checksums, hidden records, exact duplicates, same-name groups and Pij AI indexing status.</p>
<div class="db-actions"><a class="db-action" href="{{ url_for('pastor_resources') }}">← Back to Pastor's Resources</a><button class="db-action primary" id="syncBtn" onclick="syncBooks()">🔄 Sync Books Now</button></div>
<div class="db-summary" id="summary"></div>
<div class="db-note">This page reads the SQLite catalog snapshot only. It does not scan Google Drive every time you open it. Use <strong>Sync Books Now</strong> when you want this developer view to reflect the latest Drive contents. “Same name” is an informational warning only; it never removes or merges files automatically.</div>
</section>
<section class="db-browser" id="browser">
<div class="db-head"><div><h2 id="resultTitle">Books</h2><div id="resultCount" style="font-size:10px;color:#8a96a9;margin-top:3px"></div></div><div class="db-tools"><input class="db-input" id="search" type="search" placeholder="Search this list..."><select class="db-select" id="sort"><option value="name">Name</option><option value="uploaded_desc">Uploaded newest</option><option value="modified_desc">Modified newest</option><option value="size_desc">Largest file</option></select></div></div>
<div class="db-list" id="list"></div><div class="db-pages" id="pages"></div>
</section></div><div class="db-toast" id="toast"></div>

<script>
let V="",P=1,TP=1,Q="",S="name",timer=null;
const titles={all:"Drive eBook Files",logical:"Logical Books",pdf:"PDF Files",epub:"EPUB Files",exact:"Exact Duplicates",same_name:"Same-Name Groups",hidden:"Hidden Books",inactive:"Inactive Records",ai_indexed:"AI Indexed Books",ai_not_indexed:"Not AI Indexed"};
const esc=v=>{const d=document.createElement("div");d.textContent=String(v??"");return d.innerHTML},num=v=>Number(v||0)||0;
function date(v){if(!v)return"—";const d=new Date(v);return isNaN(d) ? String(v):d.toLocaleString()}
function size(v){let n=num(v),u=["B","KB","MB","GB"],i=0;while(n>=1024&&i<3){n/=1024;i++}return `${n.toFixed(i?1:0)} ${u[i]}`}
function toast(m){const e=document.getElementById("toast");e.textContent=m;e.style.display="block";clearTimeout(toast.t);toast.t=setTimeout(()=>e.style.display="none",2500)}
async function copy(v){if(v)try{await navigator.clipboard.writeText(v);toast("Copied")}catch(e){toast("Copy failed")}}
function card(v,l,view){return `<button class="db-stat click ${V===view?"active":""}" onclick="openView('${view}')"><div class="db-value">${num(v).toLocaleString()}</div><div class="db-label">${l}</div></button>`}
function info(v,l){return `<div class="db-stat"><div class="db-value">${esc(v)}</div><div class="db-label">${l}</div></div>`}
function summary(s){document.getElementById("summary").innerHTML=card(s.logical_books,"Logical Books","logical")+card(s.current_ebook_files,"Drive eBook Files","all")+card(s.pdf_files,"PDF Files","pdf")+card(s.epub_files,"EPUB Files","epub")+card(s.exact_duplicate_files,"Exact Duplicates","exact")+card(s.same_name_groups,"Same-Name Groups","same_name")+card(s.hidden_books,"Hidden Books","hidden")+card(s.inactive_file_records,"Inactive Records","inactive")+info(num(s.folders_scanned).toLocaleString(),"Folders Scanned")+info(num(s.unsupported_files).toLocaleString(),"Unsupported Files")+card(s.ai_indexed_books,"AI Indexed Books","ai_indexed")+card(s.ai_not_indexed_books,"Not AI Indexed","ai_not_indexed")+info(date(s.last_sync_at),"Last Sync")}
function badges(i){let x=[`<span class="badge ${String(i.format||"").toLowerCase()}">${esc(String(i.format||"FILE").toUpperCase())}</span>`,i.ai_indexed?`<span class="badge aiyes">AI INDEXED</span>`:`<span class="badge aino">NOT AI INDEXED</span>`];if(i.is_exact_duplicate)x.push(`<span class="badge warn">EXACT DUPLICATE</span>`);if(i.is_same_name)x.push(`<span class="badge warn">SAME NAME ×${num(i.same_name_count)}</span>`);if(i.is_hidden)x.push(`<span class="badge warn">HIDDEN</span>`);if(!i.is_current)x.push(`<span class="badge warn">INACTIVE</span>`);return x.join("")}
function detail(l,v,c=false){v=(v===null||v===undefined||v==="")?"—":String(v);return `<div class="db-detail"><div class="db-dlabel">${esc(l)}</div><div class="db-dvalue">${esc(v)}${c&&v!=="—"?`<button class="copy" data-copy="${esc(v)}">Copy</button>`:""}</div></div>`}
function render(items){const e=document.getElementById("list");if(!items.length){e.innerHTML=`<div class="db-empty">No records found.</div>`;return}e.innerHTML=items.map((i,n)=>{let ck=i.sha256_checksum||i.sha1_checksum||i.md5_checksum||"—",folder=i.file_folder_path||i.book_folder_path||"—";return `<article class="db-row" id="r${n}"><button class="db-rowbtn" onclick="document.getElementById('r${n}').classList.toggle('open')"><div class="db-main"><div class="db-book">${esc(i.title||i.drive_name||"Untitled")}</div><div class="db-sub">${esc(i.author||"Unknown Author")} • ${esc(i.drive_name||"")}</div><div class="db-badges">${badges(i)}</div></div><div class="chev">›</div></button><div class="db-details"><div class="db-grid">${detail("Drive filename",i.drive_name)}${detail("Drive Uploaded / Created",date(i.drive_created_time))}${detail("Drive Modified",date(i.drive_modified_time))}${detail("Size / MIME",`${size(i.size)} • ${i.mime_type||"—"}`)}${detail("Logical Book ID",`${i.book_id??"—"} • ${num(i.logical_group_file_count)} current file(s)`)}${detail("Google Drive File ID",i.drive_file_id,true)}${detail("Folder",folder)}${detail("Checksum",ck,true)}${detail("Duplicate Of Drive ID",i.duplicate_of_drive_file_id||"—")}${detail("First Seen in Database",date(i.file_first_seen_at))}${detail("Last Seen in Database",date(i.file_last_seen_at))}${detail("Book Key",i.book_key)}${detail("Database File Row ID",i.database_file_id)}${detail("Pij AI Index Status",i.ai_indexed?"Indexed / searchable":"Not indexed / not searchable")}${detail("AI Indexed Pages",i.ai_page_count||0)}${detail("AI Chunks",i.ai_chunk_count||0)}${detail("AI Indexed At",date(i.ai_indexed_at))}${detail("AI Index / Extraction Error",i.ai_extract_error||"None")}</div></div></article>`}).join("");e.querySelectorAll("[data-copy]").forEach(b=>b.onclick=x=>{x.stopPropagation();copy(b.dataset.copy||"")})}
function pager(){const e=document.getElementById("pages");e.innerHTML=TP<=1?"":`<button class="db-pbtn" ${P<=1?"disabled":""} onclick="load(${P-1})">← Previous</button><span style="font-size:10px;color:#7b879a">Page ${P} of ${TP}</span><button class="db-pbtn" ${P>=TP?"disabled":""} onclick="load(${P+1})">Next →</button>`}
async function load(p=1,onlySummary=false){P=Math.max(1,num(p));try{const q=new URLSearchParams({page:P,per_page:50,view:V||"all",sort:S,q:Q}),r=await fetch("/pastor-resources/admin/api/database-details?"+q),d=await r.json();if(!r.ok||!d.ok)throw Error(d.error||"Unable to load database details.");summary(d.summary||{});if(onlySummary&&!V)return;P=num(d.page)||1;TP=num(d.pages)||1;document.getElementById("resultTitle").textContent=titles[V]||"Database Details";document.getElementById("resultCount").textContent=`${num(d.total).toLocaleString()} records`;render(d.items||[]);pager()}catch(e){toast(e.message)}}
function openView(v){V=v;Q="";P=1;document.getElementById("search").value="";document.getElementById("browser").classList.add("open");load(1);setTimeout(()=>document.getElementById("browser").scrollIntoView({behavior:"smooth"}),70)}
document.getElementById("sort").onchange=e=>{S=e.target.value;load(1)};document.getElementById("search").oninput=e=>{clearTimeout(timer);timer=setTimeout(()=>{Q=e.target.value.trim();load(1)},300)};
async function syncBooks(){const b=document.getElementById("syncBtn"),old=b.textContent;b.disabled=true;b.textContent="⏳ Syncing…";try{const r=await fetch("/pastor-resources/sync-books",{method:"POST"}),d=await r.json();if(!r.ok||!d.ok)throw Error(d.error||"Synchronization failed.");toast("Sync complete.");await load(1,!V)}catch(e){toast("Sync failed: "+e.message)}finally{b.disabled=false;b.textContent=old}}
load(1,true);
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

            pij_index = trigger_pij_library_index()
            stats = dict(stats or {})
            stats["pij_index_started"] = bool(
                pij_index.get("started")
            )
            stats["pij_index_queued"] = bool(
                pij_index.get("queued")
            )
            stats["pij_index_ok"] = bool(
                pij_index.get("ok")
            )

            return jsonify(
                ok=True,
                message=(
                    "Library synchronization completed. "
                    "Pij AI knowledge refresh started or queued."
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

        # Pij may link directly to the PDF page / EPUB section that supplied
        # an answer. These query values are navigation hints only; normal
        # authentication and book visibility checks above still apply.
        jump_page = 0
        jump_section = 0

        try:
            jump_page = max(
                0,
                int(
                    request.args.get(
                        "page",
                        0,
                    )
                    or 0
                ),
            )
        except Exception:
            jump_page = 0

        try:
            jump_section = max(
                0,
                int(
                    request.args.get(
                        "section",
                        0,
                    )
                    or 0
                ),
            )
        except Exception:
            jump_section = 0

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
            jump_page=jump_page,
            jump_section=jump_section,
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


