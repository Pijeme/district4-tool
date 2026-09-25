import io
import math
import os
import re
import sqlite3
import sys
import threading
import traceback
from datetime import datetime, timezone
from email.utils import format_datetime

from flask import (
    Response,
    jsonify,
    redirect,
    render_template_string,
    request,
    session,
    url_for,
)
from google.auth.transport.requests import AuthorizedSession
from google.oauth2.service_account import Credentials
from pypdf import PdfReader


# =========================================================
# CONFIGURATION
# =========================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

GOOGLE_SERVICE_ACCOUNT_FILE = os.path.join(
    BASE_DIR,
    "service_account.json",
)

SERMON_EBOOKS_DRIVE_FOLDER_ID = (
    "1Pj7or17jlwevIcnItu2gtM_d3aAii34o"
)

GOOGLE_DRIVE_SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly"
]

GOOGLE_DRIVE_FOLDER_MIME = (
    "application/vnd.google-apps.folder"
)

SERMON_PER_PAGE = 30


# Only one sermon sync may run in this Python process at a time.
SERMON_SYNC_LOCK = threading.Lock()
SERMON_SYNC_STATE_LOCK = threading.Lock()
SERMON_SYNC_STATE = {
    "running": False,
    "stage": "idle",
    "message": "",
    "total": 0,
    "processed": 0,
    "indexed": 0,
    "skipped": 0,
    "errors": 0,
    "current_file": "",
    "last_error": "",
    "started_at": "",
    "finished_at": "",
    "stats": {},
}


def safe_unicode(value):
    """
    Convert arbitrary PDF/Drive text into SQLite/JSON-safe Unicode.

    Some PDFs contain isolated UTF-16 surrogate characters. Python's
    SQLite adapter and UTF-8 JSON output cannot encode those characters.
    Replace them instead of allowing one bad character to abort the sync.
    """
    if value is None:
        return ""

    text = str(value)
    text = text.replace("\x00", " ")
    return text.encode(
        "utf-8",
        errors="replace",
    ).decode(
        "utf-8",
        errors="replace",
    )


def console_safe(value):
    """Make progress/error messages safe for the Windows terminal."""
    text = safe_unicode(value)
    encoding = getattr(sys.stdout, "encoding", None) or "utf-8"
    return text.encode(
        encoding,
        errors="backslashreplace",
    ).decode(
        encoding,
        errors="ignore",
    )


def update_sync_state(**changes):
    with SERMON_SYNC_STATE_LOCK:
        SERMON_SYNC_STATE.update(changes)
        return dict(SERMON_SYNC_STATE)


def get_live_sync_state():
    with SERMON_SYNC_STATE_LOCK:
        state = dict(SERMON_SYNC_STATE)
        state["stats"] = dict(
            SERMON_SYNC_STATE.get("stats") or {}
        )
        return state


# =========================================================
# APP / AUTH HELPERS
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


def is_sermon_admin():
    """
    Keep the same private authorization rule used by
    Pastor's Resources.
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


def utc_now_iso():
    return datetime.now(
        timezone.utc
    ).isoformat()


# =========================================================
# DATABASE
# =========================================================

def get_db():
    db = sqlite3.connect(
        _appmod().DATABASE,
        timeout=30,
        check_same_thread=False,
    )

    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys = ON")
    db.execute("PRAGMA busy_timeout = 30000")

    return db



def ensure_sermon_tables():
    """
    Create only additive sermon-library tables.

    Important:
    - Existing sermon_library_files rows are never deleted or rebuilt here.
    - Safe "Delete" is stored in a separate sermon_hidden_items table,
      so the working Drive sync table is left untouched.
    - Reader state, highlights, notes and bookmarks use separate tables.
    """
    db = get_db()

    try:
        # Keep the original working sermon catalog schema unchanged.
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS sermon_library_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                drive_file_id TEXT NOT NULL UNIQUE,
                filename TEXT NOT NULL,
                folder_path TEXT,
                mime_type TEXT,
                size INTEGER NOT NULL DEFAULT 0,
                md5_checksum TEXT,
                sha256_checksum TEXT,

                drive_created_time TEXT,
                drive_modified_time TEXT,

                pdf_creation_date TEXT,
                pdf_modification_date TEXT,
                effective_created_date TEXT,
                date_source TEXT,

                detected_text TEXT,
                detected_theme TEXT,
                manual_text TEXT,
                manual_theme TEXT,
                manual_created_date TEXT,

                canonical_book_index INTEGER NOT NULL DEFAULT 999,
                canonical_chapter INTEGER NOT NULL DEFAULT 999,
                canonical_verse INTEGER NOT NULL DEFAULT 999,

                page_count INTEGER NOT NULL DEFAULT 0,
                searchable INTEGER NOT NULL DEFAULT 0,
                extract_error TEXT,

                is_active INTEGER NOT NULL DEFAULT 1,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                indexed_at TEXT
            )
            """
        )

        db.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_sermon_library_active
            ON sermon_library_files(is_active)
            """
        )

        db.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_sermon_library_bible_sort
            ON sermon_library_files(
                canonical_book_index,
                canonical_chapter,
                canonical_verse
            )
            """
        )

        db.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_sermon_library_created
            ON sermon_library_files(effective_created_date)
            """
        )

        db.execute(
            """
            CREATE TABLE IF NOT EXISTS sermon_library_pages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sermon_id INTEGER NOT NULL,
                page_number INTEGER NOT NULL,
                page_text TEXT,
                FOREIGN KEY(sermon_id)
                    REFERENCES sermon_library_files(id)
                    ON DELETE CASCADE,
                UNIQUE(sermon_id, page_number)
            )
            """
        )

        db.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_sermon_pages_sermon
            ON sermon_library_pages(sermon_id, page_number)
            """
        )

        db.execute(
            """
            CREATE TABLE IF NOT EXISTS sermon_library_sync (
                id INTEGER PRIMARY KEY CHECK(id = 1),
                last_sync_at TEXT,
                folders_scanned INTEGER NOT NULL DEFAULT 0,
                pdf_files_seen INTEGER NOT NULL DEFAULT 0,
                indexed_pdfs INTEGER NOT NULL DEFAULT 0,
                searchable_pdfs INTEGER NOT NULL DEFAULT 0,
                unreadable_pdfs INTEGER NOT NULL DEFAULT 0,
                new_pdfs INTEGER NOT NULL DEFAULT 0,
                changed_pdfs INTEGER NOT NULL DEFAULT 0,
                removed_pdfs INTEGER NOT NULL DEFAULT 0
            )
            """
        )

        db.execute(
            """
            INSERT OR IGNORE INTO sermon_library_sync (id)
            VALUES (1)
            """
        )

        # Safe local removal. This does NOT touch Google Drive and does
        # not change sermon_library_files, so synchronization stays intact.
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS sermon_hidden_items (
                sermon_id INTEGER PRIMARY KEY,
                hidden_at TEXT NOT NULL,
                hidden_by TEXT,
                FOREIGN KEY(sermon_id)
                    REFERENCES sermon_library_files(id)
                    ON DELETE CASCADE
            )
            """
        )

        # PDF reader state, intentionally separate from Pastor's Resources.
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS sermon_reader_state (
                user_key TEXT NOT NULL,
                sermon_id INTEGER NOT NULL,
                pdf_page INTEGER NOT NULL DEFAULT 1,
                pdf_scale REAL NOT NULL DEFAULT 1.15,
                progress_percent REAL NOT NULL DEFAULT 0,
                theme TEXT NOT NULL DEFAULT 'light',
                updated_at TEXT NOT NULL,
                PRIMARY KEY(user_key, sermon_id),
                FOREIGN KEY(sermon_id)
                    REFERENCES sermon_library_files(id)
                    ON DELETE CASCADE
            )
            """
        )

        db.execute(
            """
            CREATE TABLE IF NOT EXISTS sermon_reader_annotations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_key TEXT NOT NULL,
                sermon_id INTEGER NOT NULL,
                annotation_type TEXT NOT NULL DEFAULT 'highlight',
                selected_text TEXT,
                locator TEXT NOT NULL,
                page INTEGER,
                color TEXT,
                note TEXT,
                tags TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(sermon_id)
                    REFERENCES sermon_library_files(id)
                    ON DELETE CASCADE
            )
            """
        )

        db.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_sermon_reader_annotations_user_sermon
            ON sermon_reader_annotations(user_key, sermon_id)
            """
        )

        db.execute(
            """
            CREATE TABLE IF NOT EXISTS sermon_reader_bookmarks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_key TEXT NOT NULL,
                sermon_id INTEGER NOT NULL,
                page INTEGER NOT NULL,
                label TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(sermon_id)
                    REFERENCES sermon_library_files(id)
                    ON DELETE CASCADE
            )
            """
        )

        db.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_sermon_reader_bookmarks_user_sermon
            ON sermon_reader_bookmarks(user_key, sermon_id)
            """
        )

        # FTS5 is preferred, but the rest of the module works
        # even on an SQLite build without it.
        try:
            db.execute(
                """
                CREATE VIRTUAL TABLE IF NOT EXISTS
                sermon_library_pages_fts
                USING fts5(
                    sermon_id UNINDEXED,
                    page_number UNINDEXED,
                    page_text
                )
                """
            )
        except sqlite3.OperationalError:
            pass

        db.commit()

    finally:
        db.close()


def has_fts5(db):
    row = db.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
          AND name = 'sermon_library_pages_fts'
        """
    ).fetchone()

    return bool(row)


# =========================================================
# GOOGLE DRIVE
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

    return AuthorizedSession(credentials)


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
                "and trashed = false"
            ),
            "fields": (
                "nextPageToken,"
                "files("
                "id,name,mimeType,size,"
                "createdTime,modifiedTime,"
                "md5Checksum,sha256Checksum,parents"
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

        payload = response.json()
        files.extend(payload.get("files", []))

        page_token = payload.get(
            "nextPageToken"
        )

        if not page_token:
            break

    return files


def scan_sermon_drive():
    drive_session = get_drive_session()

    files = []
    folders_scanned = 0
    visited = set()

    def scan(folder_id, folder_path=""):
        nonlocal folders_scanned

        if folder_id in visited:
            return

        visited.add(folder_id)
        folders_scanned += 1

        for item in list_drive_folder(
            drive_session,
            folder_id,
        ):
            name = safe_unicode(
                item.get("name") or ""
            ).strip()

            mime_type = safe_unicode(
                item.get("mimeType") or ""
            ).strip()

            file_id = safe_unicode(
                item.get("id") or ""
            ).strip()

            if (
                mime_type
                == GOOGLE_DRIVE_FOLDER_MIME
            ):
                next_path = (
                    f"{folder_path}/{name}"
                    if folder_path
                    else name
                )
                scan(file_id, next_path)
                continue

            is_pdf = (
                mime_type == "application/pdf"
                or name.lower().endswith(".pdf")
            )

            if not is_pdf:
                continue

            files.append(
                {
                    "drive_file_id": file_id,
                    "filename": name,
                    "folder_path": safe_unicode(folder_path),
                    "mime_type": mime_type
                    or "application/pdf",
                    "size": int(
                        item.get("size") or 0
                    ),
                    "drive_created_time": safe_unicode(
                        item.get("createdTime")
                        or ""
                    ),
                    "drive_modified_time": safe_unicode(
                        item.get("modifiedTime")
                        or ""
                    ),
                    "md5_checksum": safe_unicode(
                        item.get("md5Checksum")
                        or ""
                    ),
                    "sha256_checksum": safe_unicode(
                        item.get("sha256Checksum")
                        or ""
                    ),
                }
            )

    scan(SERMON_EBOOKS_DRIVE_FOLDER_ID)

    return {
        "files": files,
        "folders_scanned": folders_scanned,
        "pdf_files_seen": len(files),
    }


def download_drive_file_bytes(
    drive_session,
    drive_file_id,
):
    response = drive_session.get(
        (
            "https://www.googleapis.com/drive/v3/files/"
            f"{drive_file_id}"
        ),
        params={
            "alt": "media",
            "supportsAllDrives": "true",
        },
        timeout=180,
    )
    response.raise_for_status()
    return response.content


# =========================================================
# PDF METADATA / CONTENT EXTRACTION
# =========================================================

def normalize_pdf_date(value):
    """
    Returns an ISO-like UTC-neutral string YYYY-MM-DDTHH:MM:SS
    when possible. PDF metadata frequently looks like:
        D:20240102143015+08'00'
    """

    if value is None:
        return ""

    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is not None:
            dt = dt.astimezone(
                timezone.utc
            ).replace(tzinfo=None)
        return dt.isoformat(
            timespec="seconds"
        )

    text = safe_unicode(value).strip()

    if not text:
        return ""

    if text.startswith("D:"):
        text = text[2:]

    match = re.match(
        r"^(\d{4})"
        r"(\d{2})?"
        r"(\d{2})?"
        r"(\d{2})?"
        r"(\d{2})?"
        r"(\d{2})?",
        text,
    )

    if match:
        year = int(match.group(1))
        month = int(match.group(2) or 1)
        day = int(match.group(3) or 1)
        hour = int(match.group(4) or 0)
        minute = int(match.group(5) or 0)
        second = int(match.group(6) or 0)

        try:
            return datetime(
                year,
                month,
                day,
                hour,
                minute,
                second,
            ).isoformat(
                timespec="seconds"
            )
        except ValueError:
            return ""

    # ISO / Drive-style fallback.
    try:
        cleaned = text.replace(
            "Z",
            "+00:00",
        )
        dt = datetime.fromisoformat(cleaned)
        if dt.tzinfo is not None:
            dt = dt.astimezone(
                timezone.utc
            ).replace(tzinfo=None)
        return dt.isoformat(
            timespec="seconds"
        )
    except Exception:
        return ""


def clean_detected_value(value):
    text = safe_unicode(value).strip()
    text = re.sub(r"^\*+|\*+$", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip(" .")


def detect_text_and_theme(page_texts):
    # Sermon headers should normally be on the opening pages.
    # The patterns also support Text and Theme appearing on
    # the same extracted PDF line.
    beginning = "\n".join(
        page_texts[:8]
    )

    bible_text = ""
    theme = ""

    text_patterns = [
        r"(?is)(?:^|\n)\s*(?:\*\*)?\s*Text\s*:\s*(.+?)(?=\s+(?:Theme|Title|Topic)\s*:|\r?\n|$)",
        r"(?is)(?:^|\n)\s*(?:\*\*)?\s*Scripture\s*:\s*(.+?)(?=\s+(?:Theme|Title|Topic)\s*:|\r?\n|$)",
    ]

    theme_patterns = [
        r"(?is)(?:^|\n|\s)\s*(?:\*\*)?\s*Theme\s*:\s*(.+?)(?=\s+(?:Text|Scripture|Introduction|Topic)\s*:|\r?\n|$)",
        r"(?is)(?:^|\n|\s)\s*(?:\*\*)?\s*Title\s*:\s*(.+?)(?=\s+(?:Text|Scripture|Introduction|Topic)\s*:|\r?\n|$)",
    ]

    for pattern in text_patterns:
        match = re.search(pattern, beginning)
        if match:
            bible_text = clean_detected_value(
                match.group(1)
            )
            break

    for pattern in theme_patterns:
        match = re.search(pattern, beginning)
        if match:
            theme = clean_detected_value(
                match.group(1)
            )
            break

    return bible_text, theme


# =========================================================
# CANONICAL BIBLE ORDER
# =========================================================

BIBLE_BOOKS = [
    "Genesis", "Exodus", "Leviticus", "Numbers", "Deuteronomy",
    "Joshua", "Judges", "Ruth", "1 Samuel", "2 Samuel",
    "1 Kings", "2 Kings", "1 Chronicles", "2 Chronicles", "Ezra",
    "Nehemiah", "Esther", "Job", "Psalms", "Proverbs",
    "Ecclesiastes", "Song of Solomon", "Isaiah", "Jeremiah", "Lamentations",
    "Ezekiel", "Daniel", "Hosea", "Joel", "Amos",
    "Obadiah", "Jonah", "Micah", "Nahum", "Habakkuk",
    "Zephaniah", "Haggai", "Zechariah", "Malachi", "Matthew",
    "Mark", "Luke", "John", "Acts", "Romans",
    "1 Corinthians", "2 Corinthians", "Galatians", "Ephesians", "Philippians",
    "Colossians", "1 Thessalonians", "2 Thessalonians", "1 Timothy", "2 Timothy",
    "Titus", "Philemon", "Hebrews", "James", "1 Peter",
    "2 Peter", "1 John", "2 John", "3 John", "Jude",
    "Revelation",
]

BIBLE_BOOK_INDEX = {
    name.lower(): index + 1
    for index, name in enumerate(BIBLE_BOOKS)
}

BIBLE_ALIASES = {
    "psalm": "Psalms",
    "psalms": "Psalms",
    "song of songs": "Song of Solomon",
    "song of solomon": "Song of Solomon",
    "canticles": "Song of Solomon",
    "rev": "Revelation",
    "revelations": "Revelation",
    "jn": "John",
    "mt": "Matthew",
    "mk": "Mark",
    "lk": "Luke",
    "rom": "Romans",
    "1 cor": "1 Corinthians",
    "2 cor": "2 Corinthians",
    "1 thess": "1 Thessalonians",
    "2 thess": "2 Thessalonians",
    "1 tim": "1 Timothy",
    "2 tim": "2 Timothy",
    "1 pet": "1 Peter",
    "2 pet": "2 Peter",
    "1 sam": "1 Samuel",
    "2 sam": "2 Samuel",
    "1 kgs": "1 Kings",
    "2 kgs": "2 Kings",
    "1 chr": "1 Chronicles",
    "2 chr": "2 Chronicles",
}

# Longest names first so "1 John" is not confused with "John".
BOOK_MATCH_NAMES = sorted(
    list(BIBLE_BOOKS)
    + list(BIBLE_ALIASES.keys()),
    key=len,
    reverse=True,
)


def canonical_sort_values(bible_text):
    text = safe_unicode(bible_text).strip()

    if not text:
        return 999, 999, 999

    normalized = re.sub(
        r"\s+",
        " ",
        text,
    )

    lower_text = normalized.lower()

    matched_name = None
    canonical_name = None
    match_end = None

    for candidate in BOOK_MATCH_NAMES:
        pattern = (
            r"(?<![a-z])"
            + re.escape(candidate.lower())
            + r"\b"
        )
        match = re.search(pattern, lower_text)
        if match:
            matched_name = candidate
            canonical_name = BIBLE_ALIASES.get(
                candidate.lower(),
                candidate,
            )
            match_end = match.end()
            break

    if not canonical_name:
        return 999, 999, 999

    book_index = BIBLE_BOOK_INDEX.get(
        canonical_name.lower(),
        999,
    )

    remainder = normalized[
        match_end or 0:
    ]

    reference = re.search(
        r"(\d{1,3})"
        r"(?:\s*:\s*(\d{1,3}))?",
        remainder,
    )

    if not reference:
        return book_index, 999, 999

    chapter = int(reference.group(1))
    verse = int(reference.group(2) or 0)

    return book_index, chapter, verse


# =========================================================
# PDF INDEXING
# =========================================================

def extract_pdf_document(pdf_bytes):
    reader = PdfReader(
        io.BytesIO(pdf_bytes)
    )

    if reader.is_encrypted:
        try:
            reader.decrypt("")
        except Exception:
            pass

    metadata = reader.metadata

    creation_date = ""
    modification_date = ""

    if metadata:
        try:
            creation_date = normalize_pdf_date(
                metadata.creation_date
            )
        except Exception:
            creation_date = normalize_pdf_date(
                metadata.get("/CreationDate")
            )

        try:
            modification_date = normalize_pdf_date(
                metadata.modification_date
            )
        except Exception:
            modification_date = normalize_pdf_date(
                metadata.get("/ModDate")
            )

    page_texts = []

    for page in reader.pages:
        try:
            text = safe_unicode(
                page.extract_text() or ""
            )
        except Exception:
            text = ""

        page_texts.append(text)

    bible_text, theme = detect_text_and_theme(
        page_texts
    )

    searchable = any(
        bool(text.strip())
        for text in page_texts
    )

    return {
        "page_count": len(reader.pages),
        "page_texts": page_texts,
        "pdf_creation_date": creation_date,
        "pdf_modification_date": modification_date,
        "detected_text": bible_text,
        "detected_theme": theme,
        "searchable": 1 if searchable else 0,
    }


def choose_effective_created_date(
    manual_date,
    pdf_creation_date,
    pdf_modification_date,
    drive_created_time,
):
    manual = normalize_pdf_date(
        manual_date
    )

    if manual:
        return manual, "Manual"

    if pdf_creation_date:
        return pdf_creation_date, "PDF CreationDate"

    if pdf_modification_date:
        return pdf_modification_date, "PDF ModDate"

    drive_date = normalize_pdf_date(
        drive_created_time
    )

    if drive_date:
        return drive_date, "Google Drive createdTime"

    return "", "Unknown"


def replace_page_index(
    db,
    sermon_id,
    page_texts,
):
    db.execute(
        "DELETE FROM sermon_library_pages WHERE sermon_id = ?",
        (sermon_id,),
    )

    if has_fts5(db):
        db.execute(
            "DELETE FROM sermon_library_pages_fts WHERE sermon_id = ?",
            (sermon_id,),
        )

    for page_number, page_text in enumerate(
        page_texts,
        start=1,
    ):
        page_text = safe_unicode(page_text)

        db.execute(
            """
            INSERT INTO sermon_library_pages (
                sermon_id,
                page_number,
                page_text
            )
            VALUES (?, ?, ?)
            """,
            (
                sermon_id,
                page_number,
                page_text,
            ),
        )

        if has_fts5(db) and page_text.strip():
            db.execute(
                """
                INSERT INTO sermon_library_pages_fts (
                    sermon_id,
                    page_number,
                    page_text
                )
                VALUES (?, ?, ?)
                """,
                (
                    sermon_id,
                    page_number,
                    page_text,
                ),
            )


def _empty_extracted_document():
    return {
        "page_count": 0,
        "page_texts": [],
        "pdf_creation_date": "",
        "pdf_modification_date": "",
        "detected_text": "",
        "detected_theme": "",
        "searchable": 0,
    }


def _safe_item(item):
    return {
        "drive_file_id": safe_unicode(
            item.get("drive_file_id")
        ).strip(),
        "filename": safe_unicode(
            item.get("filename")
        ).strip() or "Untitled sermon.pdf",
        "folder_path": safe_unicode(
            item.get("folder_path")
        ).strip(),
        "mime_type": safe_unicode(
            item.get("mime_type")
        ).strip() or "application/pdf",
        "size": int(item.get("size") or 0),
        "md5_checksum": safe_unicode(
            item.get("md5_checksum")
        ).strip(),
        "sha256_checksum": safe_unicode(
            item.get("sha256_checksum")
        ).strip(),
        "drive_created_time": safe_unicode(
            item.get("drive_created_time")
        ).strip(),
        "drive_modified_time": safe_unicode(
            item.get("drive_modified_time")
        ).strip(),
    }


def _save_failed_item(db, item, existing, error_text, now_iso):
    """Best-effort persistence for a PDF that could not be indexed."""
    error_text = safe_unicode(error_text)[:4000]

    if existing:
        db.execute(
            """
            UPDATE sermon_library_files
            SET filename = ?,
                folder_path = ?,
                mime_type = ?,
                size = ?,
                md5_checksum = ?,
                sha256_checksum = ?,
                drive_created_time = ?,
                drive_modified_time = ?,
                extract_error = ?,
                is_active = 1,
                last_seen_at = ?
            WHERE id = ?
            """,
            (
                item["filename"],
                item["folder_path"],
                item["mime_type"],
                item["size"],
                item["md5_checksum"],
                item["sha256_checksum"],
                item["drive_created_time"],
                item["drive_modified_time"],
                error_text,
                now_iso,
                int(existing["id"]),
            ),
        )
        return int(existing["id"])

    cursor = db.execute(
        """
        INSERT INTO sermon_library_files (
            drive_file_id,
            filename,
            folder_path,
            mime_type,
            size,
            md5_checksum,
            sha256_checksum,
            drive_created_time,
            drive_modified_time,
            effective_created_date,
            date_source,
            detected_text,
            detected_theme,
            canonical_book_index,
            canonical_chapter,
            canonical_verse,
            page_count,
            searchable,
            extract_error,
            is_active,
            first_seen_at,
            last_seen_at,
            indexed_at
        )
        VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', '',
            999, 999, 999, 0, 0, ?, 1, ?, ?, ?
        )
        """,
        (
            item["drive_file_id"],
            item["filename"],
            item["folder_path"],
            item["mime_type"],
            item["size"],
            item["md5_checksum"],
            item["sha256_checksum"],
            item["drive_created_time"],
            item["drive_modified_time"],
            normalize_pdf_date(
                item["drive_created_time"]
            ),
            "Google Drive createdTime",
            error_text,
            now_iso,
            now_iso,
            now_iso,
        ),
    )
    return int(cursor.lastrowid)


def sync_sermon_library(progress_callback=None):
    """
    Scan the sermon Drive folder and index only new/changed PDFs.

    Important safety behavior:
    - commits after every PDF, so completed work survives interruption;
    - one unreadable/bad PDF is recorded and the sync continues;
    - records are marked inactive only after a complete Drive scan/process pass;
    - all PDF text is sanitized before SQLite/JSON UTF-8 encoding.
    """
    ensure_sermon_tables()

    def progress(**values):
        if progress_callback:
            progress_callback(**values)

    progress(
        stage="scanning",
        message="Scanning the sermon Google Drive folder...",
        total=0,
        processed=0,
        indexed=0,
        skipped=0,
        errors=0,
        current_file="",
        last_error="",
    )

    scan_result = scan_sermon_drive()
    drive_session = get_drive_session()
    now_iso = utc_now_iso()

    files = [
        _safe_item(item)
        for item in scan_result["files"]
    ]
    total_files = len(files)

    progress(
        stage="indexing",
        message=(
            f"Found {total_files} sermon PDF"
            + ("" if total_files == 1 else "s")
            + ". Indexing new or changed files..."
        ),
        total=total_files,
    )

    db = get_db()

    try:
        existing_active_ids = {
            safe_unicode(row["drive_file_id"])
            for row in db.execute(
                """
                SELECT drive_file_id
                FROM sermon_library_files
                WHERE is_active = 1
                """
            ).fetchall()
        }

        scanned_ids = set()
        new_pdfs = 0
        changed_pdfs = 0
        indexed_pdfs = 0
        skipped_pdfs = 0
        processing_errors = 0
        last_error = ""

        for position, item in enumerate(
            files,
            start=1,
        ):
            drive_file_id = item["drive_file_id"]
            scanned_ids.add(drive_file_id)

            progress(
                stage="indexing",
                total=total_files,
                processed=position - 1,
                indexed=indexed_pdfs,
                skipped=skipped_pdfs,
                errors=processing_errors,
                current_file=item["filename"],
                message=(
                    f"Processing {position} of {total_files}: "
                    + item["filename"]
                ),
                last_error=last_error,
            )

            print(
                console_safe(
                    f"[Sermon Sync] {position}/{total_files} - "
                    f"{item['filename']}"
                ),
                flush=True,
            )

            existing = db.execute(
                """
                SELECT *
                FROM sermon_library_files
                WHERE drive_file_id = ?
                """,
                (drive_file_id,),
            ).fetchone()

            needs_index = (
                existing is None
                or safe_unicode(
                    existing["drive_modified_time"]
                    or ""
                )
                != item["drive_modified_time"]
                or int(existing["size"] or 0)
                != int(item["size"] or 0)
            )

            if existing is None:
                new_pdfs += 1
            elif needs_index:
                changed_pdfs += 1

            if not needs_index:
                try:
                    db.execute(
                        """
                        UPDATE sermon_library_files
                        SET filename = ?,
                            folder_path = ?,
                            mime_type = ?,
                            size = ?,
                            md5_checksum = ?,
                            sha256_checksum = ?,
                            drive_created_time = ?,
                            drive_modified_time = ?,
                            is_active = 1,
                            last_seen_at = ?
                        WHERE drive_file_id = ?
                        """,
                        (
                            item["filename"],
                            item["folder_path"],
                            item["mime_type"],
                            item["size"],
                            item["md5_checksum"],
                            item["sha256_checksum"],
                            item["drive_created_time"],
                            item["drive_modified_time"],
                            now_iso,
                            drive_file_id,
                        ),
                    )
                    db.commit()
                    skipped_pdfs += 1
                except Exception as error:
                    db.rollback()
                    processing_errors += 1
                    last_error = safe_unicode(
                        f"{item['filename']}: {error}"
                    )
                    print(
                        console_safe(
                            "[Sermon Sync ERROR] "
                            + last_error
                        ),
                        flush=True,
                    )

                progress(
                    processed=position,
                    indexed=indexed_pdfs,
                    skipped=skipped_pdfs,
                    errors=processing_errors,
                    last_error=last_error,
                )
                continue

            try:
                extracted = _empty_extracted_document()
                extract_error = ""

                try:
                    pdf_bytes = download_drive_file_bytes(
                        drive_session,
                        drive_file_id,
                    )
                    extracted = extract_pdf_document(
                        pdf_bytes
                    )
                except Exception as error:
                    extract_error = safe_unicode(error)[:4000]

                # Final sanitation boundary before any SQLite writes.
                extracted["pdf_creation_date"] = safe_unicode(
                    extracted.get("pdf_creation_date")
                )
                extracted["pdf_modification_date"] = safe_unicode(
                    extracted.get("pdf_modification_date")
                )
                extracted["detected_text"] = safe_unicode(
                    extracted.get("detected_text")
                )
                extracted["detected_theme"] = safe_unicode(
                    extracted.get("detected_theme")
                )
                extracted["page_texts"] = [
                    safe_unicode(page_text)
                    for page_text in extracted.get(
                        "page_texts",
                        [],
                    )
                ]

                manual_text = (
                    safe_unicode(existing["manual_text"])
                    if existing
                    else ""
                )
                manual_theme = (
                    safe_unicode(existing["manual_theme"])
                    if existing
                    else ""
                )
                manual_created_date = (
                    safe_unicode(
                        existing["manual_created_date"]
                    )
                    if existing
                    else ""
                )

                display_text = (
                    manual_text.strip()
                    or extracted["detected_text"]
                )

                book_index, chapter, verse = (
                    canonical_sort_values(
                        display_text
                    )
                )

                effective_date, date_source = (
                    choose_effective_created_date(
                        manual_created_date,
                        extracted["pdf_creation_date"],
                        extracted["pdf_modification_date"],
                        item["drive_created_time"],
                    )
                )

                if existing:
                    sermon_id = int(existing["id"])

                    db.execute(
                        """
                        UPDATE sermon_library_files
                        SET filename = ?,
                            folder_path = ?,
                            mime_type = ?,
                            size = ?,
                            md5_checksum = ?,
                            sha256_checksum = ?,
                            drive_created_time = ?,
                            drive_modified_time = ?,
                            pdf_creation_date = ?,
                            pdf_modification_date = ?,
                            effective_created_date = ?,
                            date_source = ?,
                            detected_text = ?,
                            detected_theme = ?,
                            canonical_book_index = ?,
                            canonical_chapter = ?,
                            canonical_verse = ?,
                            page_count = ?,
                            searchable = ?,
                            extract_error = ?,
                            is_active = 1,
                            last_seen_at = ?,
                            indexed_at = ?
                        WHERE id = ?
                        """,
                        (
                            item["filename"],
                            item["folder_path"],
                            item["mime_type"],
                            item["size"],
                            item["md5_checksum"],
                            item["sha256_checksum"],
                            item["drive_created_time"],
                            item["drive_modified_time"],
                            extracted["pdf_creation_date"],
                            extracted["pdf_modification_date"],
                            effective_date,
                            safe_unicode(date_source),
                            extracted["detected_text"],
                            extracted["detected_theme"],
                            book_index,
                            chapter,
                            verse,
                            int(extracted["page_count"] or 0),
                            int(extracted["searchable"] or 0),
                            extract_error,
                            now_iso,
                            now_iso,
                            sermon_id,
                        ),
                    )
                else:
                    cursor = db.execute(
                        """
                        INSERT INTO sermon_library_files (
                            drive_file_id,
                            filename,
                            folder_path,
                            mime_type,
                            size,
                            md5_checksum,
                            sha256_checksum,
                            drive_created_time,
                            drive_modified_time,
                            pdf_creation_date,
                            pdf_modification_date,
                            effective_created_date,
                            date_source,
                            detected_text,
                            detected_theme,
                            canonical_book_index,
                            canonical_chapter,
                            canonical_verse,
                            page_count,
                            searchable,
                            extract_error,
                            is_active,
                            first_seen_at,
                            last_seen_at,
                            indexed_at
                        )
                        VALUES (
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, 1, ?, ?, ?
                        )
                        """,
                        (
                            drive_file_id,
                            item["filename"],
                            item["folder_path"],
                            item["mime_type"],
                            item["size"],
                            item["md5_checksum"],
                            item["sha256_checksum"],
                            item["drive_created_time"],
                            item["drive_modified_time"],
                            extracted["pdf_creation_date"],
                            extracted["pdf_modification_date"],
                            effective_date,
                            safe_unicode(date_source),
                            extracted["detected_text"],
                            extracted["detected_theme"],
                            book_index,
                            chapter,
                            verse,
                            int(extracted["page_count"] or 0),
                            int(extracted["searchable"] or 0),
                            extract_error,
                            now_iso,
                            now_iso,
                            now_iso,
                        ),
                    )
                    sermon_id = int(cursor.lastrowid)

                replace_page_index(
                    db,
                    sermon_id,
                    extracted["page_texts"],
                )

                # Save this PDF now. A later failure cannot erase it.
                db.commit()
                indexed_pdfs += 1

                if extract_error:
                    processing_errors += 1
                    last_error = safe_unicode(
                        f"{item['filename']}: {extract_error}"
                    )

            except Exception as error:
                db.rollback()
                processing_errors += 1
                last_error = safe_unicode(
                    f"{item['filename']}: {error}"
                )

                print(
                    console_safe(
                        "[Sermon Sync ERROR] "
                        + last_error
                    ),
                    flush=True,
                )
                traceback.print_exc()

                # Keep a catalog row for the bad PDF and continue.
                try:
                    _save_failed_item(
                        db,
                        item,
                        existing,
                        last_error,
                        now_iso,
                    )
                    db.commit()
                except Exception:
                    db.rollback()
                    traceback.print_exc()

            progress(
                processed=position,
                indexed=indexed_pdfs,
                skipped=skipped_pdfs,
                errors=processing_errors,
                current_file=item["filename"],
                last_error=last_error,
            )

        # Only now, after a complete pass, deactivate files that
        # disappeared from the Drive folder.
        if scanned_ids:
            placeholders = ",".join(
                "?" for _ in scanned_ids
            )
            db.execute(
                f"""
                UPDATE sermon_library_files
                SET is_active = 0
                WHERE drive_file_id NOT IN ({placeholders})
                """,
                tuple(sorted(scanned_ids)),
            )
        else:
            db.execute(
                "UPDATE sermon_library_files SET is_active = 0"
            )

        removed_pdfs = len(
            existing_active_ids - scanned_ids
        )

        searchable_row = db.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN searchable = 1 THEN 1 ELSE 0 END) AS searchable,
                SUM(CASE WHEN extract_error IS NOT NULL AND TRIM(extract_error) != '' THEN 1 ELSE 0 END) AS unreadable
            FROM sermon_library_files
            WHERE is_active = 1
            """
        ).fetchone()

        active_total = int(
            searchable_row["total"] or 0
        )
        searchable_total = int(
            searchable_row["searchable"] or 0
        )
        unreadable_total = int(
            searchable_row["unreadable"] or 0
        )

        db.execute(
            """
            UPDATE sermon_library_sync
            SET last_sync_at = ?,
                folders_scanned = ?,
                pdf_files_seen = ?,
                indexed_pdfs = ?,
                searchable_pdfs = ?,
                unreadable_pdfs = ?,
                new_pdfs = ?,
                changed_pdfs = ?,
                removed_pdfs = ?
            WHERE id = 1
            """,
            (
                now_iso,
                scan_result["folders_scanned"],
                active_total,
                indexed_pdfs,
                searchable_total,
                unreadable_total,
                new_pdfs,
                changed_pdfs,
                removed_pdfs,
            ),
        )
        db.commit()

        stats = {
            "last_sync_at": now_iso,
            "folders_scanned": scan_result[
                "folders_scanned"
            ],
            "pdf_files_seen": active_total,
            "indexed_pdfs": indexed_pdfs,
            "skipped_pdfs": skipped_pdfs,
            "searchable_pdfs": searchable_total,
            "unreadable_pdfs": unreadable_total,
            "new_pdfs": new_pdfs,
            "changed_pdfs": changed_pdfs,
            "removed_pdfs": removed_pdfs,
            "processing_errors": processing_errors,
        }

        progress(
            stage="complete",
            message=(
                f"Sync complete. {active_total} sermon PDFs cataloged."
            ),
            total=total_files,
            processed=total_files,
            indexed=indexed_pdfs,
            skipped=skipped_pdfs,
            errors=processing_errors,
            current_file="",
            last_error=last_error,
            stats=stats,
        )

        return stats

    except Exception:
        # Previous per-file commits are intentionally preserved.
        db.rollback()
        raise

    finally:
        db.close()


def start_sermon_sync(app):
    """Start one background sync and return immediately to the browser."""
    if not SERMON_SYNC_LOCK.acquire(blocking=False):
        return False, get_live_sync_state()

    update_sync_state(
        running=True,
        stage="starting",
        message="Starting sermon PDF synchronization...",
        total=0,
        processed=0,
        indexed=0,
        skipped=0,
        errors=0,
        current_file="",
        last_error="",
        started_at=utc_now_iso(),
        finished_at="",
        stats={},
    )

    def worker():
        try:
            with app.app_context():
                stats = sync_sermon_library(
                    progress_callback=update_sync_state
                )

            update_sync_state(
                running=False,
                stage="complete",
                message=(
                    "Sync complete. "
                    + str(stats.get("pdf_files_seen", 0))
                    + " sermon PDFs cataloged. "
                    "Pij private sermon knowledge is refreshed."
                ),
                finished_at=utc_now_iso(),
                stats=stats,
            )

        except Exception as error:
            error_text = safe_unicode(error)
            print(
                console_safe(
                    "[Sermon Sync FATAL] "
                    + error_text
                ),
                flush=True,
            )
            traceback.print_exc()

            update_sync_state(
                running=False,
                stage="error",
                message="Sermon sync stopped because of an error.",
                last_error=error_text,
                finished_at=utc_now_iso(),
            )

        finally:
            SERMON_SYNC_LOCK.release()

    thread = threading.Thread(
        target=worker,
        name="sermon-library-sync",
        daemon=True,
    )
    thread.start()

    return True, get_live_sync_state()


# =========================================================
# SEARCH / LISTING
# =========================================================

def build_fts_query(query):
    query = str(query or "").strip()

    if not query:
        return ""

    tokens = re.findall(
        r"\w+",
        query,
        flags=re.UNICODE,
    )

    if not tokens:
        return ""

    if len(tokens) == 1:
        token = tokens[0].replace('"', '')
        return f'"{token}"*'

    return " AND ".join(
        f'"{token.replace(chr(34), "")}"*'
        for token in tokens
    )


def get_content_hits(db, query):
    query = str(query or "").strip()

    if not query:
        return {}

    hits = {}

    if has_fts5(db):
        fts_query = build_fts_query(query)

        if fts_query:
            try:
                rows = db.execute(
                    """
                    SELECT
                        CAST(sermon_id AS INTEGER) AS sermon_id,
                        CAST(page_number AS INTEGER) AS page_number,
                        snippet(
                            sermon_library_pages_fts,
                            2,
                            '<mark>',
                            '</mark>',
                            ' … ',
                            22
                        ) AS snippet_text
                    FROM sermon_library_pages_fts
                    WHERE sermon_library_pages_fts MATCH ?
                    ORDER BY rank
                    LIMIT 3000
                    """,
                    (fts_query,),
                ).fetchall()

                for row in rows:
                    sermon_id = int(
                        row["sermon_id"]
                    )

                    if sermon_id not in hits:
                        hits[sermon_id] = {
                            "page_number": int(
                                row["page_number"]
                            ),
                            "snippet": safe_unicode(
                                row["snippet_text"]
                                or ""
                            ),
                        }

                return hits

            except sqlite3.OperationalError:
                pass

    # Fallback when FTS5 is unavailable or the query cannot
    # be parsed. This is slower but still functional.
    like_value = "%" + query + "%"

    rows = db.execute(
        """
        SELECT sermon_id, page_number, page_text
        FROM sermon_library_pages
        WHERE page_text LIKE ?
        ORDER BY sermon_id, page_number
        LIMIT 3000
        """,
        (like_value,),
    ).fetchall()

    lower_query = query.lower()

    for row in rows:
        sermon_id = int(row["sermon_id"])

        if sermon_id in hits:
            continue

        text = safe_unicode(row["page_text"] or "")
        lower = text.lower()
        index = lower.find(lower_query)

        if index < 0:
            index = 0

        start = max(0, index - 110)
        end = min(len(text), index + len(query) + 160)

        snippet = re.sub(
            r"\s+",
            " ",
            text[start:end],
        ).strip()

        hits[sermon_id] = {
            "page_number": int(
                row["page_number"]
            ),
            "snippet": snippet,
        }

    return hits


def sermon_display_text(row):
    return (
        safe_unicode(row["manual_text"]).strip()
        or safe_unicode(row["detected_text"]).strip()
        or "Text not detected"
    )


def sermon_display_theme(row):
    return (
        safe_unicode(row["manual_theme"]).strip()
        or safe_unicode(row["detected_theme"]).strip()
        or "Theme not detected"
    )


def format_created_date(value):
    value = str(value or "").strip()

    if not value:
        return "Date unavailable"

    try:
        dt = datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )
        return dt.strftime("%B %d, %Y")
    except Exception:
        return value[:10]



def list_sermons(
    query="",
    sort_by="text",
    direction="asc",
    page=1,
    per_page=SERMON_PER_PAGE,
    view="active",
    book_index=0,
):
    ensure_sermon_tables()

    query = str(query or "").strip()
    sort_by = str(
        sort_by or "text"
    ).strip().lower()
    direction = str(
        direction or "asc"
    ).strip().lower()
    view = str(
        view or "active"
    ).strip().lower()

    if sort_by not in {
        "text",
        "theme",
        "date",
    }:
        sort_by = "text"

    if direction not in {
        "asc",
        "desc",
    }:
        direction = "asc"

    if view not in {
        "active",
        "deleted",
    }:
        view = "active"

    try:
        selected_book_index = int(book_index or 0)
    except Exception:
        selected_book_index = 0

    if selected_book_index < 1 or selected_book_index > len(BIBLE_BOOKS):
        selected_book_index = 0

    selected_book_name = (
        BIBLE_BOOKS[selected_book_index - 1]
        if selected_book_index
        else ""
    )

    try:
        page = max(1, int(page))
    except Exception:
        page = 1

    try:
        per_page = min(
            100,
            max(10, int(per_page)),
        )
    except Exception:
        per_page = SERMON_PER_PAGE

    db = get_db()

    try:
        counts = db.execute(
            """
            SELECT
                SUM(
                    CASE
                    WHEN s.is_active = 1
                     AND NOT EXISTS (
                        SELECT 1
                        FROM sermon_hidden_items h
                        WHERE h.sermon_id = s.id
                     )
                    THEN 1 ELSE 0
                    END
                ) AS active_total,
                SUM(
                    CASE
                    WHEN s.is_active = 1
                     AND EXISTS (
                        SELECT 1
                        FROM sermon_hidden_items h
                        WHERE h.sermon_id = s.id
                     )
                    THEN 1 ELSE 0
                    END
                ) AS hidden_total
            FROM sermon_library_files s
            """
        ).fetchone()

        library_total = int(
            counts["active_total"] or 0
        )
        hidden_total = int(
            counts["hidden_total"] or 0
        )

        content_hits = get_content_hits(
            db,
            query,
        )

        where_parts = [
            "s.is_active = 1"
        ]

        if view == "deleted":
            where_parts.append(
                """
                EXISTS (
                    SELECT 1
                    FROM sermon_hidden_items h
                    WHERE h.sermon_id = s.id
                )
                """
            )
        else:
            where_parts.append(
                """
                NOT EXISTS (
                    SELECT 1
                    FROM sermon_hidden_items h
                    WHERE h.sermon_id = s.id
                )
                """
            )

        params = []

        if selected_book_index:
            where_parts.append(
                "s.canonical_book_index = ?"
            )
            params.append(selected_book_index)

        if query:
            like_value = "%" + query + "%"

            metadata_condition = (
                "("
                "COALESCE(NULLIF(TRIM(s.manual_theme), ''), s.detected_theme, '') LIKE ? "
                "OR COALESCE(NULLIF(TRIM(s.manual_text), ''), s.detected_text, '') LIKE ? "
                "OR s.filename LIKE ?"
                ")"
            )

            params.extend(
                [
                    like_value,
                    like_value,
                    like_value,
                ]
            )

            if content_hits:
                ids = sorted(content_hits.keys())
                placeholders = ",".join(
                    "?" for _ in ids
                )
                where_parts.append(
                    "(" + metadata_condition
                    + f" OR s.id IN ({placeholders})"
                    + ")"
                )
                params.extend(ids)
            else:
                where_parts.append(
                    metadata_condition
                )

        where_sql = (
            "WHERE "
            + " AND ".join(where_parts)
        )

        count_row = db.execute(
            f"""
            SELECT COUNT(*) AS cnt
            FROM sermon_library_files s
            {where_sql}
            """,
            tuple(params),
        ).fetchone()

        total = int(
            count_row["cnt"] or 0
        )

        pages = max(
            1,
            math.ceil(total / per_page),
        )

        if page > pages:
            page = pages

        offset = (page - 1) * per_page

        if sort_by == "theme":
            order_sql = (
                "LOWER(COALESCE(NULLIF(TRIM(s.manual_theme), ''), s.detected_theme, '')) "
                + ("DESC" if direction == "desc" else "ASC")
                + ", s.id ASC"
            )

        elif sort_by == "date":
            order_sql = (
                "CASE WHEN s.effective_created_date IS NULL OR TRIM(s.effective_created_date) = '' THEN 1 ELSE 0 END ASC, "
                "datetime(s.effective_created_date) "
                + ("ASC" if direction == "asc" else "DESC")
                + ", s.id ASC"
            )

        else:
            # Canonical Bible order is the default.
            order_sql = (
                "s.canonical_book_index "
                + ("DESC" if direction == "desc" else "ASC")
                + ", s.canonical_chapter "
                + ("DESC" if direction == "desc" else "ASC")
                + ", s.canonical_verse "
                + ("DESC" if direction == "desc" else "ASC")
                + ", LOWER(COALESCE(NULLIF(TRIM(s.manual_theme), ''), s.detected_theme, '')) ASC"
            )

        rows = db.execute(
            f"""
            SELECT s.*
            FROM sermon_library_files s
            {where_sql}
            ORDER BY {order_sql}
            LIMIT ? OFFSET ?
            """,
            tuple(
                params
                + [per_page, offset]
            ),
        ).fetchall()

        items = []

        for row in rows:
            sermon_id = int(row["id"])
            hit = content_hits.get(
                sermon_id,
                {},
            )

            items.append(
                {
                    "id": sermon_id,
                    "theme": sermon_display_theme(row),
                    "text": sermon_display_text(row),
                    "filename": safe_unicode(
                        row["filename"] or ""
                    ),
                    "folder_path": safe_unicode(
                        row["folder_path"] or ""
                    ),
                    "created_date": str(
                        row["effective_created_date"]
                        or ""
                    ),
                    "created_display": format_created_date(
                        row["effective_created_date"]
                    ),
                    "date_source": safe_unicode(
                        row["date_source"] or ""
                    ),
                    "page_count": int(
                        row["page_count"] or 0
                    ),
                    "searchable": bool(
                        row["searchable"]
                    ),
                    "extract_error": safe_unicode(
                        row["extract_error"] or ""
                    ),
                    "hidden": view == "deleted",
                    "match_page": hit.get(
                        "page_number"
                    ),
                    "match_snippet": hit.get(
                        "snippet",
                        "",
                    ),
                }
            )

        return {
            "items": items,
            "page": page,
            "pages": pages,
            "per_page": per_page,
            "total": total,
            "library_total": library_total,
            "hidden_total": hidden_total,
            "query": query,
            "sort_by": sort_by,
            "direction": direction,
            "view": view,
            "book_index": selected_book_index,
            "book_name": selected_book_name,
        }

    finally:
        db.close()


def get_sync_status():
    ensure_sermon_tables()
    db = get_db()

    try:
        row = db.execute(
            """
            SELECT *
            FROM sermon_library_sync
            WHERE id = 1
            """
        ).fetchone()

        return dict(row) if row else {}

    finally:
        db.close()


# =========================================================
# ADMIN EDIT
# =========================================================

def edit_sermon_details(
    sermon_id,
    bible_text,
    theme,
    created_date,
):
    ensure_sermon_tables()
    db = get_db()

    try:
        row = db.execute(
            """
            SELECT *
            FROM sermon_library_files
            WHERE id = ?
            """,
            (sermon_id,),
        ).fetchone()

        if not row:
            raise RuntimeError(
                "Sermon PDF was not found."
            )

        manual_text = safe_unicode(
            bible_text or ""
        ).strip()
        manual_theme = safe_unicode(
            theme or ""
        ).strip()
        manual_created = safe_unicode(
            created_date or ""
        ).strip()

        display_text = (
            manual_text
            or str(row["detected_text"] or "")
        )

        book_index, chapter, verse = (
            canonical_sort_values(
                display_text
            )
        )

        effective_date, date_source = (
            choose_effective_created_date(
                manual_created,
                str(row["pdf_creation_date"] or ""),
                str(row["pdf_modification_date"] or ""),
                str(row["drive_created_time"] or ""),
            )
        )

        db.execute(
            """
            UPDATE sermon_library_files
            SET manual_text = ?,
                manual_theme = ?,
                manual_created_date = ?,
                canonical_book_index = ?,
                canonical_chapter = ?,
                canonical_verse = ?,
                effective_created_date = ?,
                date_source = ?
            WHERE id = ?
            """,
            (
                manual_text,
                manual_theme,
                manual_created,
                book_index,
                chapter,
                verse,
                effective_date,
                date_source,
                sermon_id,
            ),
        )

        db.commit()

    finally:
        db.close()



# =========================================================
# SAFE REMOVE / RESTORE + PDF READER DATA
# =========================================================

def current_sermon_user_key():
    """
    Keep sermon reader data private to the logged-in account.
    Prefixing the account type prevents username collisions.
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

    return role + ":" + username


def set_sermon_hidden(
    sermon_id,
    hidden=True,
    user_key="",
):
    """
    Hide/restore only inside Sermon eBooks.
    The Google Drive PDF and the sync catalog row are never deleted.
    """
    ensure_sermon_tables()
    db = get_db()

    try:
        row = db.execute(
            """
            SELECT id
            FROM sermon_library_files
            WHERE id = ?
              AND is_active = 1
            """,
            (int(sermon_id),),
        ).fetchone()

        if not row:
            raise RuntimeError(
                "Sermon PDF was not found."
            )

        if hidden:
            db.execute(
                """
                INSERT INTO sermon_hidden_items (
                    sermon_id,
                    hidden_at,
                    hidden_by
                )
                VALUES (?, ?, ?)
                ON CONFLICT(sermon_id)
                DO UPDATE SET
                    hidden_at = excluded.hidden_at,
                    hidden_by = excluded.hidden_by
                """,
                (
                    int(sermon_id),
                    utc_now_iso(),
                    safe_unicode(user_key),
                ),
            )
        else:
            db.execute(
                """
                DELETE FROM sermon_hidden_items
                WHERE sermon_id = ?
                """,
                (int(sermon_id),),
            )

        db.commit()

    finally:
        db.close()


def get_sermon_reader_state(
    user_key,
    sermon_id,
):
    ensure_sermon_tables()
    db = get_db()

    try:
        row = db.execute(
            """
            SELECT *
            FROM sermon_reader_state
            WHERE user_key = ?
              AND sermon_id = ?
            """,
            (
                str(user_key or ""),
                int(sermon_id),
            ),
        ).fetchone()

        if not row:
            return {
                "pdf_page": 1,
                "pdf_scale": 1.15,
                "progress_percent": 0.0,
                "theme": "light",
            }

        theme = str(
            row["theme"] or "light"
        ).strip().lower()

        if theme not in {
            "light",
            "sepia",
            "dark",
        }:
            theme = "light"

        return {
            "pdf_page": max(
                1,
                int(row["pdf_page"] or 1),
            ),
            "pdf_scale": min(
                4.0,
                max(
                    0.5,
                    float(row["pdf_scale"] or 1.15),
                ),
            ),
            "progress_percent": min(
                100.0,
                max(
                    0.0,
                    float(row["progress_percent"] or 0),
                ),
            ),
            "theme": theme,
        }

    finally:
        db.close()


def save_sermon_reader_state(
    user_key,
    sermon_id,
    payload,
):
    ensure_sermon_tables()

    current = get_sermon_reader_state(
        user_key,
        sermon_id,
    )

    try:
        pdf_page = max(
            1,
            int(
                payload.get(
                    "pdf_page",
                    current["pdf_page"],
                )
            ),
        )
    except Exception:
        pdf_page = current["pdf_page"]

    try:
        pdf_scale = min(
            4.0,
            max(
                0.5,
                float(
                    payload.get(
                        "pdf_scale",
                        current["pdf_scale"],
                    )
                ),
            ),
        )
    except Exception:
        pdf_scale = current["pdf_scale"]

    try:
        progress_percent = min(
            100.0,
            max(
                0.0,
                float(
                    payload.get(
                        "progress_percent",
                        current["progress_percent"],
                    )
                ),
            ),
        )
    except Exception:
        progress_percent = current[
            "progress_percent"
        ]

    theme = str(
        payload.get(
            "theme",
            current["theme"],
        )
        or "light"
    ).strip().lower()

    if theme not in {
        "light",
        "sepia",
        "dark",
    }:
        theme = "light"

    db = get_db()

    try:
        db.execute(
            """
            INSERT INTO sermon_reader_state (
                user_key,
                sermon_id,
                pdf_page,
                pdf_scale,
                progress_percent,
                theme,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_key, sermon_id)
            DO UPDATE SET
                pdf_page = excluded.pdf_page,
                pdf_scale = excluded.pdf_scale,
                progress_percent = excluded.progress_percent,
                theme = excluded.theme,
                updated_at = excluded.updated_at
            """,
            (
                str(user_key or ""),
                int(sermon_id),
                pdf_page,
                pdf_scale,
                progress_percent,
                theme,
                utc_now_iso(),
            ),
        )
        db.commit()

    finally:
        db.close()

    return {
        "pdf_page": pdf_page,
        "pdf_scale": pdf_scale,
        "progress_percent": progress_percent,
        "theme": theme,
    }


def get_sermon_annotations(
    user_key,
    sermon_id,
):
    ensure_sermon_tables()
    db = get_db()

    try:
        rows = db.execute(
            """
            SELECT *
            FROM sermon_reader_annotations
            WHERE user_key = ?
              AND sermon_id = ?
            ORDER BY id DESC
            """,
            (
                str(user_key or ""),
                int(sermon_id),
            ),
        ).fetchall()

        return [
            {
                "id": int(row["id"]),
                "annotation_type": safe_unicode(
                    row["annotation_type"]
                    or "highlight"
                ),
                "selected_text": safe_unicode(
                    row["selected_text"]
                    or ""
                ),
                "locator": safe_unicode(
                    row["locator"]
                    or ""
                ),
                "page": (
                    int(row["page"])
                    if row["page"] is not None
                    else None
                ),
                "color": safe_unicode(
                    row["color"] or ""
                ),
                "note": safe_unicode(
                    row["note"] or ""
                ),
                "tags": safe_unicode(
                    row["tags"] or ""
                ),
            }
            for row in rows
        ]

    finally:
        db.close()


def add_sermon_annotation(
    user_key,
    sermon_id,
    payload,
):
    ensure_sermon_tables()

    annotation_type = str(
        payload.get(
            "annotation_type",
            "highlight",
        )
        or "highlight"
    ).strip().lower()

    if annotation_type not in {
        "highlight",
        "underline",
    }:
        annotation_type = "highlight"

    selected_text = safe_unicode(
        payload.get(
            "selected_text",
            "",
        )
    )[:12000]

    locator = safe_unicode(
        payload.get(
            "locator",
            "",
        )
    )[:24000]

    if not locator:
        raise RuntimeError(
            "Missing annotation location."
        )

    page = payload.get("page")
    try:
        page = (
            max(1, int(page))
            if page is not None
            else None
        )
    except Exception:
        page = None

    color = safe_unicode(
        payload.get(
            "color",
            "",
        )
    )[:64]

    note = safe_unicode(
        payload.get(
            "note",
            "",
        )
    )[:12000]

    tags = safe_unicode(
        payload.get(
            "tags",
            "",
        )
    )[:1000]

    now_iso = utc_now_iso()
    db = get_db()

    try:
        cur = db.execute(
            """
            INSERT INTO sermon_reader_annotations (
                user_key,
                sermon_id,
                annotation_type,
                selected_text,
                locator,
                page,
                color,
                note,
                tags,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(user_key or ""),
                int(sermon_id),
                annotation_type,
                selected_text,
                locator,
                page,
                color,
                note,
                tags,
                now_iso,
                now_iso,
            ),
        )

        annotation_id = int(
            cur.lastrowid
        )
        db.commit()

        row = db.execute(
            """
            SELECT *
            FROM sermon_reader_annotations
            WHERE id = ?
            """,
            (annotation_id,),
        ).fetchone()

        return {
            "id": annotation_id,
            "annotation_type": safe_unicode(
                row["annotation_type"]
                or "highlight"
            ),
            "selected_text": safe_unicode(
                row["selected_text"]
                or ""
            ),
            "locator": safe_unicode(
                row["locator"]
                or ""
            ),
            "page": (
                int(row["page"])
                if row["page"] is not None
                else None
            ),
            "color": safe_unicode(
                row["color"] or ""
            ),
            "note": safe_unicode(
                row["note"] or ""
            ),
            "tags": safe_unicode(
                row["tags"] or ""
            ),
        }

    finally:
        db.close()


def update_sermon_annotation(
    user_key,
    annotation_id,
    payload,
):
    ensure_sermon_tables()

    note = safe_unicode(
        payload.get(
            "note",
            "",
        )
    )[:12000]

    tags = safe_unicode(
        payload.get(
            "tags",
            "",
        )
    )[:1000]

    db = get_db()

    try:
        cur = db.execute(
            """
            UPDATE sermon_reader_annotations
            SET note = ?,
                tags = ?,
                updated_at = ?
            WHERE id = ?
              AND user_key = ?
            """,
            (
                note,
                tags,
                utc_now_iso(),
                int(annotation_id),
                str(user_key or ""),
            ),
        )

        if cur.rowcount <= 0:
            raise RuntimeError(
                "Annotation was not found."
            )

        db.commit()

    finally:
        db.close()


def delete_sermon_annotation(
    user_key,
    annotation_id,
):
    ensure_sermon_tables()
    db = get_db()

    try:
        db.execute(
            """
            DELETE FROM sermon_reader_annotations
            WHERE id = ?
              AND user_key = ?
            """,
            (
                int(annotation_id),
                str(user_key or ""),
            ),
        )
        db.commit()

    finally:
        db.close()


def get_sermon_bookmarks(
    user_key,
    sermon_id,
):
    ensure_sermon_tables()
    db = get_db()

    try:
        rows = db.execute(
            """
            SELECT *
            FROM sermon_reader_bookmarks
            WHERE user_key = ?
              AND sermon_id = ?
            ORDER BY id DESC
            """,
            (
                str(user_key or ""),
                int(sermon_id),
            ),
        ).fetchall()

        return [
            {
                "id": int(row["id"]),
                "page": int(
                    row["page"] or 1
                ),
                "label": safe_unicode(
                    row["label"]
                    or (
                        "Page "
                        + str(
                            row["page"] or 1
                        )
                    )
                ),
            }
            for row in rows
        ]

    finally:
        db.close()


def add_sermon_bookmark(
    user_key,
    sermon_id,
    payload,
):
    ensure_sermon_tables()

    try:
        page = max(
            1,
            int(
                payload.get(
                    "page",
                    1,
                )
            ),
        )
    except Exception:
        page = 1

    label = safe_unicode(
        payload.get(
            "label",
            "",
        )
    ).strip()[:500]

    if not label:
        label = "Page " + str(page)

    db = get_db()

    try:
        cur = db.execute(
            """
            INSERT INTO sermon_reader_bookmarks (
                user_key,
                sermon_id,
                page,
                label,
                created_at
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                str(user_key or ""),
                int(sermon_id),
                page,
                label,
                utc_now_iso(),
            ),
        )

        bookmark_id = int(
            cur.lastrowid
        )
        db.commit()

        return {
            "id": bookmark_id,
            "page": page,
            "label": label,
        }

    finally:
        db.close()


def delete_sermon_bookmark(
    user_key,
    bookmark_id,
):
    ensure_sermon_tables()
    db = get_db()

    try:
        db.execute(
            """
            DELETE FROM sermon_reader_bookmarks
            WHERE id = ?
              AND user_key = ?
            """,
            (
                int(bookmark_id),
                str(user_key or ""),
            ),
        )
        db.commit()

    finally:
        db.close()


def find_sermon_page_match(
    sermon_id,
    query,
    after_page=0,
):
    """
    Find the next indexed PDF page containing query, wrapping once
    to the beginning. This reuses the page text already built by sync.
    """
    ensure_sermon_tables()

    query = safe_unicode(
        query
    ).strip()

    if not query:
        return None

    try:
        after_page = max(
            0,
            int(after_page),
        )
    except Exception:
        after_page = 0

    db = get_db()

    try:
        like_value = "%" + query + "%"

        row = db.execute(
            """
            SELECT page_number, page_text
            FROM sermon_library_pages
            WHERE sermon_id = ?
              AND page_text LIKE ?
            ORDER BY
                CASE
                    WHEN page_number > ? THEN 0
                    ELSE 1
                END,
                page_number ASC
            LIMIT 1
            """,
            (
                int(sermon_id),
                like_value,
                after_page,
            ),
        ).fetchone()

        if not row:
            return None

        text = safe_unicode(
            row["page_text"] or ""
        )
        lower = text.lower()
        lower_query = query.lower()
        index = lower.find(
            lower_query
        )

        if index < 0:
            index = 0

        start = max(
            0,
            index - 90,
        )
        end = min(
            len(text),
            index + len(query) + 130,
        )

        snippet = re.sub(
            r"\s+",
            " ",
            text[start:end],
        ).strip()

        return {
            "page_number": int(
                row["page_number"]
            ),
            "snippet": snippet,
        }

    finally:
        db.close()


# =========================================================
# FILE LOOKUP / STREAMING
# =========================================================

def get_sermon_row(sermon_id):
    ensure_sermon_tables()
    db = get_db()

    try:
        return db.execute(
            """
            SELECT *
            FROM sermon_library_files
            WHERE id = ?
              AND is_active = 1
            """,
            (sermon_id,),
        ).fetchone()

    finally:
        db.close()


def proxy_pdf_file(row, as_attachment=False):
    drive_session = get_drive_session()

    headers = {}
    incoming_range = request.headers.get(
        "Range"
    )

    if incoming_range:
        headers["Range"] = incoming_range

    response = drive_session.get(
        (
            "https://www.googleapis.com/drive/v3/files/"
            f"{row['drive_file_id']}"
        ),
        params={
            "alt": "media",
            "supportsAllDrives": "true",
        },
        headers=headers,
        stream=True,
        timeout=180,
    )

    response.raise_for_status()

    def generate():
        try:
            for chunk in response.iter_content(
                chunk_size=1024 * 256
            ):
                if chunk:
                    yield chunk
        finally:
            response.close()

    flask_response = Response(
        generate(),
        status=response.status_code,
        mimetype="application/pdf",
    )

    for header_name in (
        "Content-Length",
        "Content-Range",
        "Accept-Ranges",
        "ETag",
        "Last-Modified",
    ):
        value = response.headers.get(
            header_name
        )
        if value:
            flask_response.headers[
                header_name
            ] = value

    filename = safe_unicode(
        row["filename"]
        or "sermon.pdf"
    ).replace('"', "")

    disposition = (
        "attachment"
        if as_attachment
        else "inline"
    )

    flask_response.headers[
        "Content-Disposition"
    ] = (
        f'{disposition}; filename="{filename}"'
    )

    flask_response.headers[
        "Cache-Control"
    ] = "private, max-age=3600"

    return flask_response


# =========================================================
# TEMPLATE
# =========================================================

SERMON_EBOOKS_HTML = r"""
{% extends "base.html" %}

{% block title %}Sermon eBooks - District 4 Tool{% endblock %}

{% block content %}
<style>
@import url('https://fonts.googleapis.com/css2?family=Lora:wght@500;600;700&family=Nunito+Sans:wght@400;600;700;800;900&display=swap');

.app-main {
    max-width: none;
    padding: 0;
}

.se-page {
    min-height: calc(100vh - 70px);
    padding: 14px 12px 50px;
    background:
        radial-gradient(circle at top left, rgba(255,218,232,.48), transparent 34%),
        radial-gradient(circle at top right, rgba(205,224,255,.55), transparent 31%),
        #f7f9fc;
    font-family: "Nunito Sans", Arial, sans-serif;
    color: #17233c;
}

.se-shell {
    width: min(1180px, 100%);
    margin: 0 auto;
}

.se-hero {
    padding: 20px 18px;
    border: 1px solid rgba(15,23,42,.08);
    border-radius: 22px;
    background: rgba(255,255,255,.93);
    box-shadow: 0 13px 36px rgba(15,23,42,.08);
}

.se-kicker {
    color: #855c8c;
    font-size: 11px;
    font-weight: 900;
    letter-spacing: .09em;
    text-transform: uppercase;
}

.se-title {
    margin: 5px 0 5px;
    color: #273653;
    font-family: "Lora", Georgia, serif;
    font-size: clamp(28px, 8vw, 48px);
    line-height: 1.05;
}

.se-subtitle {
    margin: 0;
    color: #69778d;
    font-size: 13px;
    line-height: 1.65;
}

.se-hero-row {
    display: flex;
    flex-direction: column;
    gap: 15px;
}

.se-actions,
.se-view-row {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
}

.se-btn,
.se-link-btn,
.se-view-btn {
    min-height: 40px;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    gap: 6px;
    padding: 9px 13px;
    border: 0;
    border-radius: 11px;
    text-decoration: none;
    font: 850 11px "Nunito Sans", Arial, sans-serif;
    cursor: pointer;
}

.se-btn.primary {
    color: #fff;
    background: linear-gradient(135deg,#c787bb,#718fd2);
}

.se-link-btn,
.se-view-btn {
    color: #475873;
    background: #eef3f8;
}

.se-view-btn.active {
    color: #fff;
    background: linear-gradient(135deg,#8f6ba0,#718fd2);
}

.se-count {
    margin-top: 15px;
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    align-items: center;
}

.se-count-main {
    font-size: 24px;
    font-weight: 950;
    color: #2d3c59;
}

.se-count-note {
    color: #7b8799;
    font-size: 11px;
}

.se-tools {
    margin-top: 13px;
    padding: 12px;
    border: 1px solid rgba(15,23,42,.08);
    border-radius: 18px;
    background: rgba(255,255,255,.95);
    box-shadow: 0 9px 28px rgba(15,23,42,.06);
}

.se-view-row {
    margin-bottom: 9px;
}

.se-search {
    width: 100%;
    min-height: 46px;
    padding: 10px 12px;
    border: 1px solid #dbe3ed;
    border-radius: 12px;
    outline: none;
    font: 700 13px "Nunito Sans", Arial, sans-serif;
}

.se-search:focus,
.se-select:focus {
    border-color: #8ba6dc;
    box-shadow: 0 0 0 3px rgba(116,148,211,.12);
}

.se-sort-row {
    margin-top: 9px;
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 8px;
}

.se-select {
    width: 100%;
    min-height: 42px;
    padding: 8px 10px;
    border: 1px solid #dbe3ed;
    border-radius: 11px;
    background: #fff;
    color: #4a5a73;
    outline: none;
    font: 800 11px "Nunito Sans", Arial, sans-serif;
}

.se-status {
    min-height: 22px;
    margin-top: 9px;
    color: #778399;
    font-size: 11px;
}

.se-list {
    margin-top: 13px;
    display: grid;
    gap: 10px;
}

.se-item {
    padding: 14px;
    border: 1px solid rgba(15,23,42,.075);
    border-radius: 16px;
    background: #fff;
    box-shadow: 0 7px 20px rgba(15,23,42,.045);
}

.se-item-top {
    display: flex;
    flex-direction: column;
    gap: 9px;
}

.se-theme {
    margin: 0;
    color: #263650;
    font-family: "Lora", Georgia, serif;
    font-size: 18px;
    line-height: 1.25;
}

.se-text {
    margin-top: 4px;
    color: #8b5d8e;
    font-size: 12px;
    font-weight: 900;
}

.se-meta {
    margin-top: 8px;
    display: flex;
    flex-wrap: wrap;
    gap: 7px 12px;
    color: #78859a;
    font-size: 10px;
}

.se-match {
    margin-top: 10px;
    padding: 10px 11px;
    border-left: 3px solid #9caedd;
    border-radius: 8px;
    background: #f6f8fc;
    color: #526078;
    font-size: 11px;
    line-height: 1.55;
}

.se-match mark {
    background: #fff09a;
    color: inherit;
    padding: 0 2px;
    border-radius: 3px;
}

.se-item-actions {
    margin-top: 3px;
    display: flex;
    flex-wrap: wrap;
    gap: 7px;
}

.se-read,
.se-download,
.se-edit,
.se-delete,
.se-restore {
    min-height: 34px;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    padding: 7px 10px;
    border: 0;
    border-radius: 9px;
    text-decoration: none;
    font: 850 10px "Nunito Sans", Arial, sans-serif;
    cursor: pointer;
}

.se-read {
    color: #fff;
    background: linear-gradient(135deg,#8b6da5,#6f8fcf);
}

.se-download {
    color: #53647c;
    background: #edf2f7;
}

.se-edit {
    color: #765c7a;
    background: #f4eaf4;
}

.se-delete {
    color: #9b4040;
    background: #fae8e8;
}

.se-restore {
    color: #25684f;
    background: #e2f5eb;
}


.se-bible-directory {
    margin-top: 12px;
    padding: 12px;
    border: 1px solid #e1e7ef;
    border-radius: 14px;
    background: #fbfcfe;
}

.se-bible-directory-head {
    display:flex;
    align-items:center;
    justify-content:space-between;
    gap:10px;
    margin-bottom:9px;
}

.se-bible-directory-title {
    color:#59677d;
    font-size:11px;
    font-weight:950;
    letter-spacing:.02em;
}

.se-bible-directory-note {
    color:#8a95a6;
    font-size:9px;
    font-weight:800;
}

.se-book-grid {
    max-height: 182px;
    overflow-y: auto;
    display:flex;
    flex-wrap:wrap;
    gap:6px;
    padding:1px 2px 2px 1px;
    scrollbar-width:thin;
}

.se-book-btn {
    min-height:30px;
    padding:6px 9px;
    border:1px solid #e1e6ee;
    border-radius:999px;
    background:#fff;
    color:#647187;
    font:850 9px "Nunito Sans", Arial, sans-serif;
    cursor:pointer;
    transition:transform .12s ease, box-shadow .12s ease, background .12s ease;
}

.se-book-btn:hover {
    transform:translateY(-1px);
    box-shadow:0 4px 12px rgba(15,23,42,.08);
}

.se-book-btn.active {
    border-color:#8c78ad;
    background:linear-gradient(135deg,#9271a7,#728fc9);
    color:#fff;
    box-shadow:0 5px 14px rgba(113,91,150,.18);
}

.se-pagination {
    margin-top: 14px;
    display:flex;
    flex-wrap:wrap;
    align-items:center;
    justify-content:center;
    gap:7px;
}

.se-page-btn {
    min-width:38px;
    height:38px;
    padding:0 9px;
    border:0;
    border-radius:10px;
    background:#fff;
    color:#53637b;
    box-shadow:0 5px 18px rgba(15,23,42,.07);
    font-weight:900;
    cursor:pointer;
}
.se-page-btn:hover {
    background:#f1f4f9;
}
.se-page-btn.active {
    background:linear-gradient(135deg,#9271a7,#728fc9);
    color:#fff;
    box-shadow:0 6px 16px rgba(113,91,150,.20);
}
.se-page-btn:disabled { opacity:.45; cursor:not-allowed; }

.se-empty {
    padding:28px 18px;
    border:1px dashed #d8e0ea;
    border-radius:16px;
    background:#fff;
    color:#77849a;
    text-align:center;
    font-size:12px;
}

.se-toast {
    position:fixed;
    z-index:16000;
    left:12px;
    right:12px;
    bottom:18px;
    display:none;
    padding:12px 14px;
    border-radius:12px;
    background:#111827;
    color:white;
    font-size:11px;
    box-shadow:0 14px 34px rgba(0,0,0,.24);
}

.se-modal-backdrop {
    position:fixed;
    inset:0;
    z-index:15000;
    display:none;
    align-items:center;
    justify-content:center;
    padding:18px;
    background:rgba(15,23,42,.48);
}
.se-modal-backdrop.show { display:flex; }
.se-modal {
    width:min(520px,100%);
    padding:18px;
    border-radius:18px;
    background:#fff;
    box-shadow:0 24px 60px rgba(0,0,0,.22);
}
.se-modal h3 {
    margin:0 0 14px;
    color:#2a3955;
    font-family:"Lora",Georgia,serif;
}
.se-field { margin-top:10px; }
.se-field label {
    display:block;
    margin-bottom:5px;
    color:#657289;
    font-size:10px;
    font-weight:900;
}
.se-field input {
    width:100%;
    min-height:42px;
    padding:9px 10px;
    border:1px solid #dbe3ed;
    border-radius:10px;
    outline:none;
    font:700 12px "Nunito Sans",Arial,sans-serif;
}
.se-modal-actions {
    margin-top:14px;
    display:flex;
    justify-content:flex-end;
    gap:8px;
}

.se-loading {
    position: fixed;
    inset: 0;
    z-index: 14000;
    display:none;
    align-items:center;
    justify-content:center;
    padding:20px;
    background:rgba(15,23,42,.48);
}
.se-loading.show { display:flex; }
.se-loading-card {
    width:min(440px,100%);
    padding:20px;
    border-radius:18px;
    background:#fff;
    text-align:center;
    box-shadow:0 24px 60px rgba(0,0,0,.2);
}
.se-loading-title { font-weight:950; color:#263650; }
.se-loading-sub { margin-top:6px; color:#748096; font-size:11px; line-height:1.5; }
.se-loading-track {
    height:10px;
    margin-top:13px;
    overflow:hidden;
    border-radius:999px;
    background:#e8edf5;
}
.se-loading-bar {
    width:0%;
    height:100%;
    border-radius:999px;
    background:linear-gradient(90deg,#c98abc,#7694d6);
    transition:width .25s ease;
}
.se-loading-progress {
    margin-top:9px;
    color:#53617a;
    font-size:11px;
    font-weight:900;
}
.se-btn:disabled {
    opacity:.58;
    cursor:not-allowed;
}

@media(min-width:760px) {
    .se-page { padding:22px 20px 64px; }
    .se-hero { padding:24px; }
    .se-hero-row {
        flex-direction:row;
        align-items:flex-start;
        justify-content:space-between;
    }
    .se-tools-grid {
        display:grid;
        grid-template-columns:minmax(0,1fr) 180px 180px;
        gap:9px;
        align-items:start;
    }
    .se-sort-row {
        display:contents;
    }
    .se-status {
        grid-column:1/-1;
        margin-top:0;
    }
    .se-item { padding:16px 17px; }
    .se-item-top {
        flex-direction:row;
        align-items:flex-start;
        justify-content:space-between;
    }
    .se-item-actions {
        margin-top:0;
        flex:0 0 auto;
        max-width:430px;
        justify-content:flex-end;
    }
    .se-toast {
        left:auto;
        right:20px;
        width:360px;
    }
}
</style>

<div class="se-page">
  <div class="se-shell">
    <section class="se-hero">
      <div class="se-hero-row">
        <div>
          <div class="se-kicker">District 4 Sermon Library</div>
          <h1 class="se-title">Sermon eBooks</h1>
          <p class="se-subtitle">
            Sermons are identified from the <strong>Text</strong> and <strong>Theme</strong> written inside each PDF.
            The default arrangement follows the canonical order of the Bible.
          </p>
        </div>
        <div class="se-actions">
          <a class="se-link-btn" href="{{ url_for('pastor_resources') }}">← Pastor's Resources</a>
          <button class="se-btn primary" id="syncSermonButton" type="button" onclick="syncSermons()">Sync Sermon PDFs</button>
        </div>
      </div>

      <div class="se-count">
        <div class="se-count-main" id="sermonCount">{{ initial_total }} Sermon PDFs</div>
        <div class="se-count-note" id="sermonCountNote"></div>
      </div>
    </section>

    <section class="se-tools">
      <div class="se-view-row">
        <button class="se-view-btn active" id="viewActiveButton" type="button" onclick="switchView('active')">
          Library
        </button>
        <button class="se-view-btn" id="viewDeletedButton" type="button" onclick="switchView('deleted')">
          Deleted ({{ hidden_total }})
        </button>
      </div>

      <div class="se-tools-grid">
        <input
          id="searchInput"
          class="se-search"
          type="search"
          placeholder="Search Text, Theme, filename, or anything inside the sermon..."
          autocomplete="off"
        >

        <div class="se-sort-row">
          <select id="sortSelect" class="se-select" onchange="sortChanged()">
            <option value="text" selected>Sort: Bible Text</option>
            <option value="theme">Sort: Theme</option>
            <option value="date">Sort: Date Created</option>
          </select>

          <select id="directionSelect" class="se-select" onchange="loadSermons(1)">
            <option value="asc" selected>Canonical Order</option>
            <option value="desc">Reverse Canonical</option>
          </select>
        </div>

        <div class="se-status" id="statusText">
          {% if sync_status.get('last_sync_at') %}
            Last sync: {{ sync_status.get('last_sync_at') }}
          {% else %}
            No sermon sync has been completed yet.
          {% endif %}
        </div>
      </div>

      <div class="se-bible-directory">
        <div class="se-bible-directory-head">
          <div class="se-bible-directory-title">Bible Book Directory</div>
          <div class="se-bible-directory-note">Choose a book to show sermons from that Bible book</div>
        </div>
        <div class="se-book-grid" id="bibleDirectory">
          <button
            class="se-book-btn active"
            type="button"
            data-book-index="0"
            onclick="selectBibleBook(0)"
          >All Books</button>
          {% for book in bible_books %}
          <button
            class="se-book-btn"
            type="button"
            data-book-index="{{ loop.index }}"
            onclick="selectBibleBook({{ loop.index }})"
          >{{ book }}</button>
          {% endfor %}
        </div>
      </div>
    </section>

    <section class="se-list" id="sermonList"></section>
    <div class="se-pagination" id="pagination"></div>
  </div>
</div>

<div class="se-toast" id="toast"></div>

<div class="se-modal-backdrop" id="editModal">
  <div class="se-modal">
    <h3>Edit Sermon Details</h3>
    <input type="hidden" id="editId">
    <div class="se-field">
      <label for="editText">Bible Text</label>
      <input id="editText" placeholder="e.g. Habakkuk 3:17–19">
    </div>
    <div class="se-field">
      <label for="editTheme">Theme</label>
      <input id="editTheme" placeholder="e.g. Gratitude Beyond the Outcome">
    </div>
    <div class="se-field">
      <label for="editDate">PDF Creation Date Override (optional)</label>
      <input id="editDate" type="date">
    </div>
    <div class="se-modal-actions">
      <button class="se-link-btn" type="button" onclick="closeEdit()">Cancel</button>
      <button class="se-btn primary" type="button" onclick="saveEdit()">Save Changes</button>
    </div>
  </div>
</div>

<div class="se-loading" id="loadingOverlay">
  <div class="se-loading-card">
    <div class="se-loading-title" id="loadingTitle">Syncing sermon PDFs...</div>
    <div class="se-loading-sub" id="loadingSub">
      New or changed PDFs are being downloaded, read and indexed. Please keep this page open.
    </div>
    <div class="se-loading-track"><div class="se-loading-bar" id="loadingBar"></div></div>
    <div class="se-loading-progress" id="loadingProgress">Preparing...</div>
  </div>
</div>

<script>
const SERMON_LIBRARY_STATE_KEY = "sermonEbooksLibraryStateV2";
const SERMON_LIBRARY_RESTORE_KEY = "sermonEbooksRestoreRequestedV2";
const SERMON_READER_RETURN_URL_KEY = "sermonReaderReturnUrlV2";
const BIBLE_BOOKS = {{ bible_books|tojson }};

let currentPage = 1;
let totalPages = 1;
let currentView = "active";
let activeBookIndex = 0;
let searchTimer = null;
let currentItems = [];

function escapeHtml(value) {
    const div = document.createElement("div");
    div.textContent = value == null ? "" : String(value);
    return div.innerHTML;
}

function showToast(message) {
    const el = document.getElementById("toast");
    el.textContent = message;
    el.style.display = "block";
    clearTimeout(el.hideTimer);
    el.hideTimer = setTimeout(() => {
        el.style.display = "none";
    }, 4200);
}

function captureLibraryState() {
    return {
        page: currentPage,
        view: currentView,
        query: document.getElementById("searchInput").value || "",
        sort: document.getElementById("sortSelect").value || "text",
        direction: document.getElementById("directionSelect").value || "asc",
        book: activeBookIndex,
        scrollY: Math.max(0, window.scrollY || 0)
    };
}

function rememberLibraryState() {
    try {
        sessionStorage.setItem(
            SERMON_LIBRARY_STATE_KEY,
            JSON.stringify(captureLibraryState())
        );
        sessionStorage.setItem(
            SERMON_READER_RETURN_URL_KEY,
            window.location.pathname + window.location.search
        );
        sessionStorage.setItem(
            SERMON_LIBRARY_RESTORE_KEY,
            "1"
        );
    } catch (error) {
        console.warn("Unable to save sermon library position", error);
    }
}

function requestedRestoreState() {
    try {
        if (sessionStorage.getItem(SERMON_LIBRARY_RESTORE_KEY) !== "1") {
            return null;
        }
        sessionStorage.removeItem(SERMON_LIBRARY_RESTORE_KEY);
        const raw = sessionStorage.getItem(SERMON_LIBRARY_STATE_KEY);
        return raw ? JSON.parse(raw) : null;
    } catch (error) {
        return null;
    }
}

function configureDirection(sort, preferredValue=null) {
    const direction = document.getElementById("directionSelect");

    if (sort === "text") {
        direction.innerHTML = `
            <option value="asc">Canonical Order</option>
            <option value="desc">Reverse Canonical</option>
        `;
    } else if (sort === "theme") {
        direction.innerHTML = `
            <option value="asc">A–Z</option>
            <option value="desc">Z–A</option>
        `;
    } else {
        direction.innerHTML = `
            <option value="desc">Newest First</option>
            <option value="asc">Oldest First</option>
        `;
    }

    if (
        preferredValue &&
        Array.from(direction.options).some(option => option.value === preferredValue)
    ) {
        direction.value = preferredValue;
    } else {
        direction.value = sort === "date" ? "desc" : "asc";
    }
}

function sortChanged() {
    const sort = document.getElementById("sortSelect").value;
    configureDirection(sort);
    loadSermons(1);
}

function switchView(view) {
    currentView = view === "deleted" ? "deleted" : "active";
    document.getElementById("viewActiveButton").classList.toggle(
        "active",
        currentView === "active"
    );
    document.getElementById("viewDeletedButton").classList.toggle(
        "active",
        currentView === "deleted"
    );
    loadSermons(1);
}

function updateBibleDirectory() {
    document.querySelectorAll(".se-book-btn").forEach(button => {
        const index = Number(button.dataset.bookIndex || 0);
        button.classList.toggle(
            "active",
            index === Number(activeBookIndex || 0)
        );
    });
}

function selectBibleBook(index) {
    const parsed = Number(index || 0);

    activeBookIndex = (
        Number.isInteger(parsed)
        && parsed >= 1
        && parsed <= BIBLE_BOOKS.length
    )
        ? parsed
        : 0;

    updateBibleDirectory();
    loadSermons(1);
}

function selectedBibleBookName() {
    if (
        activeBookIndex >= 1
        && activeBookIndex <= BIBLE_BOOKS.length
    ) {
        return BIBLE_BOOKS[activeBookIndex - 1];
    }
    return "";
}

function safeSnippet(value) {
    return escapeHtml(value || "")
        .replaceAll("&lt;mark&gt;", "<mark>")
        .replaceAll("&lt;/mark&gt;", "</mark>");
}

function openSermonReader(id, pageNumber=null) {
    rememberLibraryState();
    let url = "/pastor-resources/sermon-ebooks/read/" + Number(id);
    if (pageNumber) {
        url += "?page=" + encodeURIComponent(pageNumber);
    }
    window.location.href = url;
}

function renderList(data) {
    currentItems = data.items || [];
    currentView = data.view === "deleted" ? "deleted" : "active";

    document.getElementById("viewActiveButton").classList.toggle(
        "active",
        currentView === "active"
    );
    document.getElementById("viewDeletedButton").classList.toggle(
        "active",
        currentView === "deleted"
    );
    document.getElementById("viewDeletedButton").textContent =
        "Deleted (" + Number(data.hidden_total || 0) + ")";

    const list = document.getElementById("sermonList");
    const query = document.getElementById("searchInput").value.trim();
    const bookName = selectedBibleBookName();

    if (currentView === "deleted") {
        document.getElementById("sermonCount").textContent =
            Number(data.hidden_total || 0) + " Deleted Sermons";

        if (bookName && query) {
            document.getElementById("sermonCountNote").textContent =
                "Showing " + data.total + " matching deleted sermons in " + bookName;
        } else if (bookName) {
            document.getElementById("sermonCountNote").textContent =
                "Showing " + data.total + " deleted sermons in " + bookName;
        } else if (query) {
            document.getElementById("sermonCountNote").textContent =
                "Showing " + data.total + " matching deleted sermons";
        } else {
            document.getElementById("sermonCountNote").textContent =
                "Hidden from Sermon eBooks only. Google Drive files are untouched.";
        }
    } else {
        document.getElementById("sermonCount").textContent =
            Number(data.library_total || 0) + " Sermon PDFs";

        if (bookName && query) {
            document.getElementById("sermonCountNote").textContent =
                "Showing " + data.total + " matching sermon PDFs in " + bookName;
        } else if (bookName) {
            document.getElementById("sermonCountNote").textContent =
                "Showing " + data.total + " sermon PDFs in " + bookName;
        } else if (query) {
            document.getElementById("sermonCountNote").textContent =
                "Showing " + data.total + " matching sermon PDFs";
        } else {
            document.getElementById("sermonCountNote").textContent =
                "Exact number currently visible in the sermon library";
        }
    }

    if (!currentItems.length) {
        list.innerHTML = `
            <div class="se-empty">
                ${
                    currentView === "deleted"
                        ? "No deleted sermons."
                        : (
                            (query || selectedBibleBookName())
                                ? "No sermon PDF matched the current search or Bible book selection."
                                : "No sermon PDFs are cataloged yet. Use Sync Sermon PDFs."
                        )
                }
            </div>
        `;
        return;
    }

    list.innerHTML = currentItems.map(item => {
        const match = item.match_page
            ? `
                <div class="se-match">
                    <strong>Content match • Page ${item.match_page}</strong><br>
                    ${safeSnippet(item.match_snippet || "Matching text found on this page.")}
                </div>
              `
            : "";

        const activeButtons = currentView === "deleted"
            ? `
                <button
                    class="se-restore"
                    type="button"
                    onclick="restoreSermon(${item.id})"
                >Restore</button>
              `
            : `
                <button
                    class="se-edit"
                    type="button"
                    onclick="openEdit(${item.id})"
                >Edit Details</button>
                <button
                    class="se-delete"
                    type="button"
                    onclick="deleteSermon(${item.id})"
                >Delete</button>
              `;

        return `
            <article class="se-item">
                <div class="se-item-top">
                    <div>
                        <h2 class="se-theme">${escapeHtml(item.theme)}</h2>
                        <div class="se-text">${escapeHtml(item.text)}</div>
                        <div class="se-meta">
                            <span>Created: ${escapeHtml(item.created_display)}</span>
                            <span>${escapeHtml(item.page_count)} pages</span>
                            <span>${item.searchable ? "Searchable text" : "Text unavailable"}</span>
                        </div>
                    </div>
                    <div class="se-item-actions">
                        <button
                            class="se-read"
                            type="button"
                            onclick="openSermonReader(${item.id}, ${item.match_page || "null"})"
                        >${item.match_page ? "Open Match" : "Read PDF"}</button>
                        <a
                            class="se-download"
                            href="/pastor-resources/sermon-ebooks/download/${item.id}"
                        >Download</a>
                        ${activeButtons}
                    </div>
                </div>
                ${match}
            </article>
        `;
    }).join("");
}

function renderPagination(data) {
    currentPage = Number(data.page || 1);
    totalPages = Number(data.pages || 1);

    const el = document.getElementById("pagination");

    if (totalPages <= 1) {
        el.innerHTML = "";
        return;
    }

    const pageButtons = [];

    for (let pageNumber = 1; pageNumber <= totalPages; pageNumber += 1) {
        pageButtons.push(`
            <button
                class="se-page-btn ${pageNumber === currentPage ? "active" : ""}"
                type="button"
                ${pageNumber === currentPage ? 'aria-current="page"' : ""}
                onclick="goToSermonPage(${pageNumber})"
            >${pageNumber}</button>
        `);
    }

    el.innerHTML = pageButtons.join("");
}

async function goToSermonPage(pageNumber) {
    await loadSermons(pageNumber);

    const list = document.getElementById("sermonList");
    if (list) {
        list.scrollIntoView({
            behavior: "smooth",
            block: "start"
        });
    }
}

async function loadSermons(page) {
    const query = document.getElementById("searchInput").value.trim();
    const sort = document.getElementById("sortSelect").value;
    const direction = document.getElementById("directionSelect").value;

    document.getElementById("statusText").textContent =
        currentView === "deleted"
            ? "Loading deleted sermons..."
            : "Loading sermon library...";

    try {
        const params = new URLSearchParams({
            q: query,
            sort: sort,
            direction: direction,
            page: String(page || 1),
            view: currentView,
            book: String(activeBookIndex || 0)
        });

        const response = await fetch(
            "/pastor-resources/sermon-ebooks/api/list?" + params.toString()
        );
        const data = await response.json();

        if (!response.ok || !data.ok) {
            throw new Error(data.error || "Unable to load sermon library.");
        }

        activeBookIndex = Number(data.book_index || 0);
        updateBibleDirectory();

        renderList(data);
        renderPagination(data);

        document.getElementById("statusText").textContent =
            data.last_sync_at
                ? "Last sync: " + data.last_sync_at
                : "No sermon sync has been completed yet.";

        try {
            sessionStorage.setItem(
                SERMON_LIBRARY_STATE_KEY,
                JSON.stringify(captureLibraryState())
            );
        } catch (error) {}

        return data;
    } catch (error) {
        document.getElementById("statusText").textContent =
            "Unable to load sermon library.";
        showToast(error.message);
        throw error;
    }
}

let syncPollTimer = null;

function setSyncUiRunning(running) {
    const button = document.getElementById("syncSermonButton");
    const overlay = document.getElementById("loadingOverlay");

    if (button) {
        button.disabled = !!running;
        button.textContent = running
            ? "Syncing Sermon PDFs..."
            : "Sync Sermon PDFs";
    }

    if (overlay) {
        overlay.classList.toggle("show", !!running);
    }
}

function renderSyncProgress(state) {
    state = state || {};

    const total = Number(state.total || 0);
    const processed = Number(state.processed || 0);
    const indexed = Number(state.indexed || 0);
    const skipped = Number(state.skipped || 0);
    const errors = Number(state.errors || 0);
    const percent = total > 0
        ? Math.max(0, Math.min(100, Math.round((processed / total) * 100)))
        : 0;

    document.getElementById("loadingBar").style.width = percent + "%";
    document.getElementById("loadingTitle").textContent =
        state.stage === "scanning"
            ? "Scanning sermon folder..."
            : (
                state.stage === "complete"
                    ? "Sermon Library + Pij Knowledge Ready"
                    : "Syncing sermon PDFs + Pij knowledge..."
            );

    document.getElementById("loadingSub").textContent =
        state.current_file
            ? "Reading / indexing: " + state.current_file
            : (
                state.stage === "complete"
                    ? "Pij can now search the refreshed private sermon text."
                    : (state.message || "Preparing sermon library...")
            );

    document.getElementById("loadingProgress").textContent =
        total > 0
            ? processed + " / " + total + " processed • "
              + indexed + " indexed • " + skipped + " unchanged • "
              + errors + " errors"
            : (state.message || "Scanning Google Drive...");
}

async function fetchSyncStatus() {
    const response = await fetch(
        "/pastor-resources/sermon-ebooks/api/sync-status",
        {cache:"no-store"}
    );
    const data = await response.json();

    if (!response.ok || !data.ok) {
        throw new Error(data.error || "Unable to read sermon sync status.");
    }

    return data.state || {};
}

async function pollSyncProgress() {
    clearTimeout(syncPollTimer);

    try {
        const state = await fetchSyncStatus();
        renderSyncProgress(state);

        if (state.running) {
            setSyncUiRunning(true);
            syncPollTimer = setTimeout(pollSyncProgress, 900);
            return;
        }

        if (state.stage === "error") {
            setSyncUiRunning(false);
            showToast(state.last_error || "Sermon sync stopped with an error.");
            document.getElementById("statusText").textContent =
                "Sync error: " + (state.last_error || "Unknown error");
            return;
        }

        if (state.stage === "complete") {
            const stats = state.stats || {};

            // Pij reads the authorized sermon_library_pages table directly,
            // so the same sermon sync that extracts page text is also the
            // private Pij knowledge refresh. No second duplicate index is
            // required.
            renderSyncProgress({
                ...state,
                stage:"complete",
                running:false
            });

            showToast(
                "Sermon sync complete. "
                + Number(stats.pdf_files_seen || 0)
                + " sermon PDFs cataloged. Pij private sermon knowledge refreshed."
            );

            document.getElementById("statusText").textContent =
                "Sermon library and Pij private knowledge are up to date.";

            await loadSermons(currentPage);

            setTimeout(() => {
                setSyncUiRunning(false);
            }, 1800);

            return;
        }

        setSyncUiRunning(false);

    } catch (error) {
        setSyncUiRunning(false);
        showToast(error.message);
    }
}

async function syncSermons() {
    if (!confirm(
        "Sync the sermon Google Drive folder now? New or changed PDFs will be read and indexed."
    )) return;

    setSyncUiRunning(true);
    renderSyncProgress({
        running:true,
        stage:"starting",
        message:"Starting sermon synchronization..."
    });

    try {
        const response = await fetch(
            "/pastor-resources/sermon-ebooks/sync",
            {method:"POST"}
        );
        const data = await response.json();

        if (!response.ok || !data.ok) {
            throw new Error(data.error || "Sermon sync failed to start.");
        }

        if (!data.started) {
            showToast("A sermon sync is already running. Showing its progress.");
        }

        await pollSyncProgress();

    } catch (error) {
        setSyncUiRunning(false);
        showToast(error.message);
    }
}

function openEdit(id) {
    const item = currentItems.find(value => Number(value.id) === Number(id));
    if (!item) return;

    document.getElementById("editId").value = item.id;
    document.getElementById("editText").value =
        item.text === "Text not detected" ? "" : item.text;
    document.getElementById("editTheme").value =
        item.theme === "Theme not detected" ? "" : item.theme;
    document.getElementById("editDate").value =
        (item.created_date || "").slice(0,10);
    document.getElementById("editModal").classList.add("show");
}

function closeEdit() {
    document.getElementById("editModal").classList.remove("show");
}

async function saveEdit() {
    const id = Number(document.getElementById("editId").value);
    const payload = {
        text: document.getElementById("editText").value.trim(),
        theme: document.getElementById("editTheme").value.trim(),
        created_date: document.getElementById("editDate").value.trim()
    };

    try {
        const response = await fetch(
            "/pastor-resources/sermon-ebooks/api/edit/" + id,
            {
                method:"POST",
                headers:{"Content-Type":"application/json"},
                body:JSON.stringify(payload)
            }
        );
        const data = await response.json();

        if (!response.ok || !data.ok) {
            throw new Error(data.error || "Unable to save sermon details.");
        }

        closeEdit();
        showToast("Sermon details updated.");
        await loadSermons(currentPage);
    } catch (error) {
        showToast(error.message);
    }
}

async function deleteSermon(id) {
    if (!confirm(
        "Remove this sermon from Sermon eBooks? The PDF will remain safely stored in Google Drive."
    )) return;

    try {
        const response = await fetch(
            "/pastor-resources/sermon-ebooks/api/delete/" + Number(id),
            {method:"POST"}
        );
        const data = await response.json();

        if (!response.ok || !data.ok) {
            throw new Error(data.error || "Unable to remove sermon.");
        }

        showToast("Sermon removed from the library. The Google Drive PDF was not deleted.");
        await loadSermons(currentPage);
    } catch (error) {
        showToast(error.message);
    }
}

async function restoreSermon(id) {
    try {
        const response = await fetch(
            "/pastor-resources/sermon-ebooks/api/restore/" + Number(id),
            {method:"POST"}
        );
        const data = await response.json();

        if (!response.ok || !data.ok) {
            throw new Error(data.error || "Unable to restore sermon.");
        }

        showToast("Sermon restored.");
        await loadSermons(currentPage);
    } catch (error) {
        showToast(error.message);
    }
}

document.getElementById("searchInput").addEventListener("input", () => {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(() => loadSermons(1), 320);
});

document.getElementById("editModal").addEventListener("click", event => {
    if (event.target.id === "editModal") closeEdit();
});

async function initializeSermonLibrary() {
    const saved = requestedRestoreState();

    if (saved) {
        currentView = saved.view === "deleted" ? "deleted" : "active";
        document.getElementById("searchInput").value = saved.query || "";

        const sort = ["text","theme","date"].includes(saved.sort)
            ? saved.sort
            : "text";

        document.getElementById("sortSelect").value = sort;
        configureDirection(sort, saved.direction || null);

        const savedBook = Number(saved.book || 0);
        activeBookIndex = (
            Number.isInteger(savedBook)
            && savedBook >= 1
            && savedBook <= BIBLE_BOOKS.length
        )
            ? savedBook
            : 0;
        updateBibleDirectory();

        document.getElementById("viewActiveButton").classList.toggle(
            "active",
            currentView === "active"
        );
        document.getElementById("viewDeletedButton").classList.toggle(
            "active",
            currentView === "deleted"
        );

        await loadSermons(Number(saved.page || 1));

        setTimeout(() => {
            window.scrollTo({
                top: Math.max(0, Number(saved.scrollY || 0)),
                behavior: "auto"
            });
        }, 80);
    } else {
        configureDirection("text", "asc");
        activeBookIndex = 0;
        updateBibleDirectory();
        await loadSermons(1);
    }

    fetchSyncStatus()
        .then(state => {
            if (state.running) {
                setSyncUiRunning(true);
                renderSyncProgress(state);
                pollSyncProgress();
            }
        })
        .catch(() => {});
}

initializeSermonLibrary();
</script>
{% endblock %}
"""


SERMON_READER_HTML = r"""
{% extends "base.html" %}

{% block title %}{{ sermon.theme }} - Sermon Reader{% endblock %}

{% block content %}
<script src="https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.min.js"></script>

<style>
@import url('https://fonts.googleapis.com/css2?family=Lora:wght@500;600;700&family=Nunito+Sans:wght@400;600;700;800;900&display=swap');

.app-main { max-width:none; padding:0; }

.sr-root {
    --reader-bg:#eef2f7;
    --reader-panel:#fff;
    --reader-text:#17233c;
    min-height:calc(100vh - 70px);
    background:var(--reader-bg);
    color:var(--reader-text);
    font-family:"Nunito Sans",Arial,sans-serif;
}
.sr-root.theme-sepia {
    --reader-bg:#eee4cf;
    --reader-panel:#fbf4e5;
    --reader-text:#4b3b29;
}
.sr-root.theme-dark {
    --reader-bg:#171b24;
    --reader-panel:#242a36;
    --reader-text:#ecf0f7;
}

.sr-toolbar {
    position:sticky;
    top:0;
    z-index:120;
    display:flex;
    flex-direction:column;
    gap:8px;
    padding:9px 10px;
    background:rgba(255,255,255,.96);
    border-bottom:1px solid rgba(15,23,42,.10);
    box-shadow:0 5px 18px rgba(15,23,42,.08);
    backdrop-filter:blur(12px);
}
.theme-dark .sr-toolbar {
    background:rgba(28,33,44,.97);
    border-color:rgba(255,255,255,.08);
}
.theme-sepia .sr-toolbar {
    background:rgba(251,244,229,.97);
}

.sr-topline {
    display:flex;
    align-items:center;
    gap:7px;
    min-width:0;
}
.sr-info { min-width:0; flex:1; }
.sr-title {
    overflow:hidden;
    text-overflow:ellipsis;
    white-space:nowrap;
    font:700 14px "Lora",Georgia,serif;
}
.sr-text {
    overflow:hidden;
    text-overflow:ellipsis;
    white-space:nowrap;
    color:#8a648b;
    font-size:10px;
    font-weight:800;
    margin-top:2px;
}
.theme-dark .sr-text { color:#d7afd6; }

.sr-controls {
    display:flex;
    gap:6px;
    overflow-x:auto;
    padding-bottom:2px;
}

.sr-btn,
.sr-select,
.sr-search,
.sr-page-input {
    flex:0 0 auto;
    min-height:36px;
    border:1px solid #d8e0eb;
    border-radius:9px;
    padding:7px 9px;
    background:#fff;
    color:#4e5f78;
    font:800 10px "Nunito Sans",Arial,sans-serif;
}
.sr-btn { cursor:pointer; }
.sr-btn.primary {
    border:0;
    color:white;
    background:linear-gradient(135deg,#c98cc0,#789be0);
}
.sr-search { width:150px; font-weight:600; }
.sr-page-input { width:68px; }
.sr-select { max-width:120px; }

.theme-dark .sr-btn,
.theme-dark .sr-select,
.theme-dark .sr-search,
.theme-dark .sr-page-input {
    background:#303747;
    color:#e8edf5;
    border-color:#465065;
}

.sr-progress {
    height:4px;
    background:rgba(148,163,184,.25);
    overflow:hidden;
}
.sr-progress > div {
    height:100%;
    background:linear-gradient(90deg,#cc8fc1,#6f97dd);
}

.sr-main {
    position:relative;
    display:flex;
    min-height:calc(100vh - 170px);
}
.sr-canvas-area {
    flex:1;
    min-width:0;
    overflow:auto;
    padding:14px 10px 34px;
    display:flex;
    justify-content:center;
    align-items:flex-start;
    touch-action:pan-y pinch-zoom;
}

#pdfStage {
    position:relative;
    flex:0 0 auto;
    background:#fff;
    box-shadow:0 10px 35px rgba(15,23,42,.18);
}
#pdfCanvas { display:block; }

.textLayer {
    position:absolute;
    inset:0;
    overflow:hidden;
    opacity:1;
    line-height:1;
    text-size-adjust:none;
    transform-origin:0 0;
    z-index:3;
}
.textLayer span,
.textLayer br {
    color:transparent;
    position:absolute;
    white-space:pre;
    cursor:text;
    transform-origin:0% 0%;
}
.textLayer ::selection {
    background:rgba(70,115,220,.32);
}

.pdf-link-layer {
    position:absolute;
    inset:0;
    z-index:4;
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
.pdf-link-hit:hover {
    background:rgba(52,105,190,.12);
}

.pdf-annotation-layer {
    position:absolute;
    inset:0;
    z-index:2;
    pointer-events:none;
}
.pdf-annotation {
    position:absolute;
    border-radius:2px;
    pointer-events:none;
}

.theme-dark #pdfStage {
    filter:invert(.88) hue-rotate(180deg);
}
.theme-sepia #pdfStage {
    filter:sepia(.25) saturate(.9);
}

.sr-side {
    position:fixed;
    z-index:500;
    top:70px;
    right:0;
    bottom:0;
    width:min(390px,90vw);
    transform:translateX(105%);
    transition:transform .22s ease;
    background:var(--reader-panel);
    color:var(--reader-text);
    box-shadow:-12px 0 36px rgba(15,23,42,.18);
    display:flex;
    flex-direction:column;
}
.sr-side.open { transform:translateX(0); }
.sr-side-head {
    display:flex;
    align-items:center;
    justify-content:space-between;
    padding:13px;
    border-bottom:1px solid rgba(100,116,139,.18);
}
.sr-side-head h3 {
    margin:0;
    font:700 19px "Lora",Georgia,serif;
}
.sr-side-tabs {
    display:flex;
    gap:6px;
    padding:10px;
    border-bottom:1px solid rgba(100,116,139,.14);
}
.sr-side-tab {
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
.sr-side-tab.active {
    color:white;
    background:linear-gradient(135deg,#c98cc0,#789be0);
}
.sr-side-body { flex:1; overflow:auto; padding:10px; }
.sr-side-panel { display:none; }
.sr-side-panel.active { display:block; }

.sr-item {
    padding:10px;
    border-radius:12px;
    margin-bottom:8px;
    background:rgba(148,163,184,.10);
    font-size:11px;
    line-height:1.45;
}
.sr-item-quote {
    margin-top:6px;
    padding-left:8px;
    border-left:3px solid #d7a0c8;
}
.sr-item-actions {
    display:flex;
    gap:5px;
    margin-top:8px;
    flex-wrap:wrap;
}
.sr-item-actions button {
    border:0;
    border-radius:8px;
    padding:6px 7px;
    font-size:9px;
    font-weight:850;
    cursor:pointer;
}

.sr-side-backdrop {
    position:fixed;
    inset:0;
    z-index:480;
    display:none;
    background:rgba(15,23,42,.38);
}
.sr-side-backdrop.show { display:block; }

.sr-selection {
    position:fixed;
    z-index:900;
    left:8px;
    right:8px;
    bottom:10px;
    display:none;
    gap:5px;
    flex-wrap:wrap;
    justify-content:center;
    padding:8px;
    border-radius:13px;
    background:#101827;
    color:white;
    box-shadow:0 15px 35px rgba(0,0,0,.28);
}
.sr-selection.show { display:flex; }
.sr-selection-btn {
    border:0;
    border-radius:8px;
    padding:8px 9px;
    font-size:9px;
    font-weight:850;
    cursor:pointer;
    background:#fff;
    color:#334155;
}
.sr-color {
    width:29px;
    padding:0;
}

.sr-toast {
    position:fixed;
    z-index:1200;
    left:10px;
    right:10px;
    bottom:64px;
    display:none;
    padding:12px 13px;
    border-radius:12px;
    background:#111827;
    color:white;
    font-size:11px;
    box-shadow:0 12px 30px rgba(0,0,0,.24);
}

.sr-load-overlay {
    position:fixed;
    inset:0;
    z-index:30000;
    display:flex;
    align-items:center;
    justify-content:center;
    padding:18px;
    background:rgba(244,247,252,.94);
    backdrop-filter:blur(6px);
}
.theme-dark .sr-load-overlay {
    background:rgba(17,22,31,.95);
}
.sr-load-overlay.hidden { display:none; }
.sr-load-card {
    width:min(520px,100%);
    padding:22px;
    border-radius:20px;
    background:var(--reader-panel);
    color:var(--reader-text);
    box-shadow:0 24px 70px rgba(15,23,42,.20);
    border:1px solid rgba(100,116,139,.15);
}
.sr-load-title {
    font:700 22px/1.2 "Lora",Georgia,serif;
}
.sr-load-detail {
    margin-top:7px;
    color:#738097;
    font-size:12px;
    line-height:1.5;
}
.theme-dark .sr-load-detail { color:#b3bdcc; }
.sr-load-track {
    height:13px;
    margin-top:15px;
    border-radius:999px;
    overflow:hidden;
    background:rgba(148,163,184,.24);
}
.sr-load-fill {
    width:34%;
    height:100%;
    border-radius:999px;
    background:linear-gradient(90deg,#c98fc2,#789ee3);
    animation:sr-load-slide 1.15s ease-in-out infinite;
}
@keyframes sr-load-slide {
    0% { transform:translateX(-120%); }
    100% { transform:translateX(310%); }
}
.sr-load-actions {
    display:none;
    gap:8px;
    flex-wrap:wrap;
    margin-top:15px;
}
.sr-load-actions.show { display:flex; }
.sr-load-actions button,
.sr-load-actions a {
    border:0;
    border-radius:10px;
    padding:9px 11px;
    text-decoration:none;
    background:#eef2f8;
    color:#52627d;
    font:800 10px "Nunito Sans",Arial,sans-serif;
    cursor:pointer;
}
.sr-load-actions .primary {
    color:#fff;
    background:linear-gradient(135deg,#c98cc0,#789be0);
}

.sr-page-busy {
    position:absolute;
    z-index:80;
    top:12px;
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
.sr-page-busy.show { display:block; }

@media(min-width:800px) {
    .sr-toolbar { padding:9px 14px; }
    .sr-controls { flex-wrap:wrap; overflow:visible; }
    .sr-canvas-area { padding:18px 18px 38px; }
    .sr-selection {
        left:50%;
        right:auto;
        transform:translateX(-50%);
        width:auto;
        bottom:16px;
    }
    .sr-toast {
        left:auto;
        right:20px;
        width:360px;
        bottom:20px;
    }
}
</style>

<div class="sr-root theme-{{ state.theme or 'light' }}" id="readerRoot">
    <div class="sr-toolbar">
        <div class="sr-topline">
            <button class="sr-btn" type="button" onclick="returnToSermonLibrary()">← Sermon eBooks</button>

            <div class="sr-info">
                <div class="sr-title">{{ sermon.theme }}</div>
                <div class="sr-text">{{ sermon.text }}</div>
            </div>

            <button class="sr-btn" type="button" onclick="toggleSidePanel()">Notes</button>
        </div>

        <div class="sr-controls">
            <button class="sr-btn" type="button" onclick="goPrevious()">← Prev</button>
            <button class="sr-btn" type="button" onclick="goNext()">Next →</button>

            <input
                class="sr-page-input"
                id="pageInput"
                type="number"
                min="1"
                value="{{ state.pdf_page or 1 }}"
                title="Page"
            >
            <button class="sr-btn" type="button" onclick="jumpPdfPage()">Go</button>

            <button class="sr-btn" type="button" onclick="zoomPdf(-0.15)">−</button>
            <button class="sr-btn" type="button" onclick="zoomPdf(0.15)">+</button>
            <button class="sr-btn" type="button" onclick="fitPdfWidth()">Fit Width</button>
            <button class="sr-btn" type="button" onclick="fitPdfPage()">Fit Page</button>

            <select class="sr-select" id="themeSelect" onchange="setReaderTheme(this.value)">
                <option value="light">Light</option>
                <option value="sepia">Sepia</option>
                <option value="dark">Dark</option>
            </select>

            <input
                class="sr-search"
                id="readerSearchInput"
                type="search"
                placeholder="Search in sermon..."
            >
            <button class="sr-btn" type="button" onclick="findInSermon()">Find</button>

            <button class="sr-btn" type="button" onclick="addCurrentBookmark()">Bookmark</button>
            <a class="sr-btn" href="{{ download_url }}">Download</a>
            <button class="sr-btn" type="button" onclick="toggleFullscreen()">Full Screen</button>
        </div>

        <div class="sr-progress">
            <div id="readerProgressFill" style="width:{{ state.progress_percent or 0 }}%"></div>
        </div>
    </div>

    <main class="sr-main">
        <div class="sr-page-busy" id="readerPageBusy">Loading page...</div>

        <div class="sr-canvas-area" id="readerCanvasArea">
            <div id="pdfStage">
                <canvas id="pdfCanvas"></canvas>
                <div class="pdf-annotation-layer" id="pdfAnnotationLayer"></div>
                <div class="textLayer" id="pdfTextLayer"></div>
                <div class="pdf-link-layer" id="pdfLinkLayer"></div>
            </div>
        </div>
    </main>

    <aside class="sr-side" id="readerSide">
        <div class="sr-side-head">
            <h3>Sermon Notes</h3>
            <button class="sr-btn" type="button" onclick="toggleSidePanel(false)">Close</button>
        </div>

        <div class="sr-side-tabs">
            <button class="sr-side-tab active" type="button" onclick="openReaderTab('annotations',this)">
                Highlights & Notes
            </button>
            <button class="sr-side-tab" type="button" onclick="openReaderTab('bookmarks',this)">
                Bookmarks
            </button>
        </div>

        <div class="sr-side-body">
            <div class="sr-side-panel active" id="side-annotations"></div>
            <div class="sr-side-panel" id="side-bookmarks"></div>
        </div>
    </aside>

    <div class="sr-side-backdrop" id="readerSideBackdrop" onclick="toggleSidePanel(false)"></div>

    <div class="sr-selection" id="selectionBar">
        <button class="sr-selection-btn" type="button" onclick="copySelectedText()">Copy</button>
        <button class="sr-selection-btn sr-color" type="button" style="background:#ffe66d" onclick="savePendingAnnotation('highlight','#ffe66d',false)" title="Yellow highlight"></button>
        <button class="sr-selection-btn sr-color" type="button" style="background:#9ee6b8" onclick="savePendingAnnotation('highlight','#9ee6b8',false)" title="Green highlight"></button>
        <button class="sr-selection-btn sr-color" type="button" style="background:#9ed3ff" onclick="savePendingAnnotation('highlight','#9ed3ff',false)" title="Blue highlight"></button>
        <button class="sr-selection-btn" type="button" onclick="savePendingAnnotation('underline','#e5962d',false)">Underline</button>
        <button class="sr-selection-btn" type="button" onclick="savePendingAnnotation('highlight','#ffe66d',true)">Add Note</button>
        <button class="sr-selection-btn" type="button" onclick="clearPendingSelection()">Close</button>
    </div>

    <div class="sr-load-overlay" id="readerLoadOverlay">
        <div class="sr-load-card">
            <div class="sr-load-title" id="readerLoadTitle">Opening sermon PDF...</div>
            <div class="sr-load-detail" id="readerLoadDetail">Preparing the built-in reader.</div>
            <div class="sr-load-track"><div class="sr-load-fill" id="readerLoadFill"></div></div>
            <div class="sr-load-actions" id="readerLoadActions">
                <button class="primary" type="button" onclick="location.reload()">Retry</button>
                <a href="{{ download_url }}">Download PDF</a>
                <button type="button" onclick="returnToSermonLibrary()">Back to Sermon eBooks</button>
            </div>
        </div>
    </div>

    <div class="sr-toast" id="readerToast"></div>
</div>

<script>
const SERMON_ID = {{ sermon.id }};
const MEDIA_URL = {{ media_url|tojson }};
const STATE = {{ state|tojson }};
const SERMON_HOME_URL = {{ url_for('sermon_ebooks_home')|tojson }};
const SERMON_LIBRARY_RESTORE_KEY = "sermonEbooksRestoreRequestedV2";
const SERMON_READER_RETURN_URL_KEY = "sermonReaderReturnUrlV2";

let annotations = [];
let bookmarks = [];
let pendingSelection = null;
let currentTheme = STATE.theme || "light";
let pdfDoc = null;
let pdfPageNumber = Math.max(1, Number(STATE.pdf_page || 1));
let pdfScale = Math.max(.5, Number(STATE.pdf_scale || 1.15));
let pdfRenderTask = null;
let selectionTimer = null;

function escapeReaderHtml(value) {
    const div = document.createElement("div");
    div.textContent = value == null ? "" : String(value);
    return div.innerHTML;
}

function showReaderToast(message) {
    const toast = document.getElementById("readerToast");
    toast.textContent = message;
    toast.style.display = "block";
    clearTimeout(toast.hideTimer);
    toast.hideTimer = setTimeout(() => {
        toast.style.display = "none";
    }, 4200);
}

function setPageBusy(show, text="Loading page...") {
    const busy = document.getElementById("readerPageBusy");
    if (!busy) return;
    busy.textContent = text;
    busy.classList.toggle("show", Boolean(show));
}

function showReaderLoadError(message) {
    const overlay = document.getElementById("readerLoadOverlay");
    overlay.classList.remove("hidden");
    document.getElementById("readerLoadTitle").textContent =
        "Unable to open this sermon PDF";
    document.getElementById("readerLoadDetail").textContent =
        message || "The PDF reader could not load this file.";
    document.getElementById("readerLoadActions").classList.add("show");
}

function hideReaderLoading() {
    document.getElementById("readerLoadOverlay").classList.add("hidden");
}

async function apiJson(url, options={}) {
    const response = await fetch(url, options);
    const data = await response.json();

    if (!response.ok || !data.ok) {
        throw new Error(data.error || "Request failed.");
    }

    return data;
}

function returnToSermonLibrary() {
    try {
        sessionStorage.setItem(
            SERMON_LIBRARY_RESTORE_KEY,
            "1"
        );
    } catch (error) {}

    let returnUrl = "";

    try {
        returnUrl = sessionStorage.getItem(
            SERMON_READER_RETURN_URL_KEY
        ) || "";
    } catch (error) {}

    if (
        returnUrl.startsWith("/pastor-resources/sermon-ebooks") &&
        !returnUrl.includes("/read/")
    ) {
        window.location.href = returnUrl;
    } else {
        window.location.href = SERMON_HOME_URL;
    }
}

function setProgress(percent) {
    const value = Math.max(
        0,
        Math.min(
            100,
            Number(percent || 0)
        )
    );
    document.getElementById("readerProgressFill").style.width =
        value + "%";
}

function saveReaderState(payload) {
    fetch(
        "/pastor-resources/sermon-ebooks/api/state/" + SERMON_ID,
        {
            method:"POST",
            headers:{"Content-Type":"application/json"},
            body:JSON.stringify(payload),
            keepalive:true
        }
    ).catch(() => {});
}

function setReaderTheme(theme) {
    currentTheme = ["light","sepia","dark"].includes(theme)
        ? theme
        : "light";

    const root = document.getElementById("readerRoot");
    root.classList.remove(
        "theme-light",
        "theme-sepia",
        "theme-dark"
    );
    root.classList.add("theme-" + currentTheme);

    document.getElementById("themeSelect").value =
        currentTheme;

    saveReaderState({
        theme:currentTheme,
        pdf_page:pdfPageNumber,
        pdf_scale:pdfScale
    });
}

function toggleFullscreen() {
    const root = document.getElementById("readerRoot");

    if (!document.fullscreenElement) {
        root.requestFullscreen?.();
    } else {
        document.exitFullscreen?.();
    }
}

function toggleSidePanel(force) {
    const side = document.getElementById("readerSide");
    const backdrop = document.getElementById("readerSideBackdrop");
    const open = typeof force === "boolean"
        ? force
        : !side.classList.contains("open");

    side.classList.toggle("open", open);
    backdrop.classList.toggle("show", open);
}

function openReaderTab(name, button) {
    document
        .querySelectorAll(".sr-side-tab")
        .forEach(el => el.classList.remove("active"));

    document
        .querySelectorAll(".sr-side-panel")
        .forEach(el => el.classList.remove("active"));

    if (button) {
        button.classList.add("active");
    }

    document
        .getElementById("side-" + name)
        .classList.add("active");
}

async function loadNotesData() {
    const [annotationData, bookmarkData] = await Promise.all([
        apiJson(
            "/pastor-resources/sermon-ebooks/api/annotations/" + SERMON_ID
        ),
        apiJson(
            "/pastor-resources/sermon-ebooks/api/bookmarks/" + SERMON_ID
        )
    ]);

    annotations = annotationData.annotations || [];
    bookmarks = bookmarkData.bookmarks || [];

    renderSidePanel();
}

function renderSidePanel() {
    const ann = document.getElementById("side-annotations");

    ann.innerHTML = annotations.length
        ? annotations.map(item => `
            <article class="sr-item">
                <strong>${
                    item.annotation_type === "underline"
                        ? "Underline"
                        : "Highlight"
                }</strong>

                ${
                    item.selected_text
                        ? '<div class="sr-item-quote">'
                          + escapeReaderHtml(item.selected_text)
                          + '</div>'
                        : ''
                }

                ${
                    item.note
                        ? '<div style="margin-top:6px">'
                          + escapeReaderHtml(item.note)
                          + '</div>'
                        : ''
                }

                ${
                    item.tags
                        ? '<div style="margin-top:5px;color:#8b5f92">'
                          + escapeReaderHtml(item.tags)
                          + '</div>'
                        : ''
                }

                <div class="sr-item-actions">
                    <button type="button" onclick="jumpToAnnotation(${item.id})">Go</button>
                    <button type="button" onclick="editAnnotation(${item.id})">Edit Note</button>
                    <button type="button" onclick="deleteReaderAnnotation(${item.id})">Delete</button>
                </div>
            </article>
          `).join("")
        : '<div class="sr-item">Select text in the PDF to highlight, underline or add a note.</div>';

    const bm = document.getElementById("side-bookmarks");

    bm.innerHTML = bookmarks.length
        ? bookmarks.map(item => `
            <article class="sr-item">
                <strong>${escapeReaderHtml(item.label || ("Page " + item.page))}</strong>
                <div class="sr-item-actions">
                    <button type="button" onclick="jumpToBookmark(${item.id})">Go</button>
                    <button type="button" onclick="deleteReaderBookmark(${item.id})">Delete</button>
                </div>
            </article>
          `).join("")
        : '<div class="sr-item">No bookmarks in this sermon yet.</div>';
}

function clearPendingSelection() {
    pendingSelection = null;
    document.getElementById("selectionBar").classList.remove("show");

    try {
        window.getSelection()?.removeAllRanges();
    } catch (error) {}
}

async function copySelectedText() {
    if (!pendingSelection || !pendingSelection.text) {
        return;
    }

    try {
        await navigator.clipboard.writeText(
            pendingSelection.text
        );
        showReaderToast("Text copied.");
    } catch (error) {
        showReaderToast("Unable to copy text.");
    }
}

async function savePendingAnnotation(
    annotationType,
    color,
    needsNote
) {
    if (!pendingSelection) {
        return;
    }

    let note = "";
    let tags = "";

    if (needsNote) {
        const value = window.prompt(
            "Add your note:",
            ""
        );

        if (value === null) {
            return;
        }

        note = value;

        const tagValue = window.prompt(
            "Tags (optional):",
            ""
        );

        if (tagValue !== null) {
            tags = tagValue;
        }
    }

    try {
        const data = await apiJson(
            "/pastor-resources/sermon-ebooks/api/annotations/" + SERMON_ID,
            {
                method:"POST",
                headers:{"Content-Type":"application/json"},
                body:JSON.stringify({
                    annotation_type:annotationType,
                    selected_text:pendingSelection.text,
                    locator:pendingSelection.locator,
                    page:pendingSelection.page,
                    color:color,
                    note:note,
                    tags:tags
                })
            }
        );

        annotations.unshift(
            data.annotation
        );

        renderSidePanel();
        renderPdfAnnotations();
        clearPendingSelection();

        showReaderToast(
            needsNote
                ? "Highlight and note saved."
                : "Annotation saved."
        );
    } catch (error) {
        showReaderToast(error.message);
    }
}

async function editAnnotation(id) {
    const item = annotations.find(
        value => Number(value.id) === Number(id)
    );

    if (!item) {
        return;
    }

    const note = window.prompt(
        "Edit note:",
        item.note || ""
    );

    if (note === null) {
        return;
    }

    const tags = window.prompt(
        "Tags:",
        item.tags || ""
    );

    if (tags === null) {
        return;
    }

    try {
        await apiJson(
            "/pastor-resources/sermon-ebooks/api/annotation/" + Number(id),
            {
                method:"POST",
                headers:{"Content-Type":"application/json"},
                body:JSON.stringify({
                    note:note,
                    tags:tags
                })
            }
        );

        item.note = note;
        item.tags = tags;
        renderSidePanel();
        showReaderToast("Note updated.");
    } catch (error) {
        showReaderToast(error.message);
    }
}

async function deleteReaderAnnotation(id) {
    if (!confirm(
        "Delete this highlight or note?"
    )) return;

    try {
        await apiJson(
            "/pastor-resources/sermon-ebooks/api/annotation/"
            + Number(id)
            + "/delete",
            {method:"POST"}
        );

        annotations = annotations.filter(
            item => Number(item.id) !== Number(id)
        );

        renderSidePanel();
        renderPdfAnnotations();
    } catch (error) {
        showReaderToast(error.message);
    }
}

async function addCurrentBookmark() {
    const defaultLabel =
        "Page " + String(pdfPageNumber);

    const label = window.prompt(
        "Bookmark label:",
        defaultLabel
    );

    if (label === null) {
        return;
    }

    try {
        const data = await apiJson(
            "/pastor-resources/sermon-ebooks/api/bookmarks/" + SERMON_ID,
            {
                method:"POST",
                headers:{"Content-Type":"application/json"},
                body:JSON.stringify({
                    page:pdfPageNumber,
                    label:label
                })
            }
        );

        bookmarks.unshift(
            data.bookmark
        );

        renderSidePanel();
        showReaderToast("Bookmark saved.");
    } catch (error) {
        showReaderToast(error.message);
    }
}

async function deleteReaderBookmark(id) {
    if (!confirm(
        "Delete this bookmark?"
    )) return;

    try {
        await apiJson(
            "/pastor-resources/sermon-ebooks/api/bookmark/"
            + Number(id)
            + "/delete",
            {method:"POST"}
        );

        bookmarks = bookmarks.filter(
            item => Number(item.id) !== Number(id)
        );

        renderSidePanel();
    } catch (error) {
        showReaderToast(error.message);
    }
}

async function jumpToAnnotation(id) {
    const item = annotations.find(
        value => Number(value.id) === Number(id)
    );

    if (!item || !item.page) {
        return;
    }

    toggleSidePanel(false);
    pdfPageNumber = Number(item.page);
    await renderPdfPage();
}

async function jumpToBookmark(id) {
    const item = bookmarks.find(
        value => Number(value.id) === Number(id)
    );

    if (!item || !item.page) {
        return;
    }

    toggleSidePanel(false);
    pdfPageNumber = Number(item.page);
    await renderPdfPage();
}

function capturePdfSelection() {
    const selection = window.getSelection();

    if (
        !selection ||
        selection.isCollapsed ||
        !selection.rangeCount
    ) {
        return;
    }

    const text = selection.toString().trim();

    if (!text) {
        return;
    }

    const stage = document.getElementById("pdfStage");
    const range = selection.getRangeAt(0);

    if (!stage.contains(range.commonAncestorContainer)) {
        return;
    }

    const stageRect = stage.getBoundingClientRect();

    if (
        !stageRect.width ||
        !stageRect.height
    ) {
        return;
    }

    const rects = Array.from(
        range.getClientRects()
    )
        .filter(
            rect => rect.width > 1 && rect.height > 1
        )
        .map(rect => ({
            x:(rect.left - stageRect.left) / stageRect.width,
            y:(rect.top - stageRect.top) / stageRect.height,
            w:rect.width / stageRect.width,
            h:rect.height / stageRect.height
        }));

    if (!rects.length) {
        return;
    }

    pendingSelection = {
        text:text,
        locator:JSON.stringify({
            rects:rects
        }),
        page:pdfPageNumber
    };

    document.getElementById("selectionBar").classList.add("show");
}

function scheduleSelectionCapture() {
    clearTimeout(selectionTimer);
    selectionTimer = setTimeout(
        capturePdfSelection,
        180
    );
}

function renderPdfAnnotations() {
    const layer = document.getElementById("pdfAnnotationLayer");

    if (!layer) {
        return;
    }

    layer.innerHTML = "";

    annotations
        .filter(
            item => Number(item.page) === Number(pdfPageNumber)
        )
        .forEach(item => {
            let locator = {};

            try {
                locator = JSON.parse(
                    item.locator || "{}"
                );
            } catch (error) {}

            (locator.rects || []).forEach(rect => {
                const el = document.createElement("div");
                el.className = "pdf-annotation";
                el.style.left = (Number(rect.x || 0) * 100) + "%";
                el.style.width = (Number(rect.w || 0) * 100) + "%";

                if (item.annotation_type === "underline") {
                    el.style.top =
                        ((Number(rect.y || 0) + Number(rect.h || 0)) * 100 - .35)
                        + "%";
                    el.style.height = "2px";
                    el.style.background =
                        item.color || "#e5962d";
                } else {
                    el.style.top =
                        (Number(rect.y || 0) * 100) + "%";
                    el.style.height =
                        (Number(rect.h || 0) * 100) + "%";
                    el.style.background =
                        item.color || "#ffe66d";
                    el.style.opacity = ".42";
                }

                layer.appendChild(el);
            });
        });
}

async function initPdfReader() {
    if (!window.pdfjsLib) {
        showReaderLoadError(
            "PDF.js could not be loaded. Check the internet connection used to load the reader library."
        );
        return;
    }

    pdfjsLib.GlobalWorkerOptions.workerSrc =
        "https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.worker.min.js";

    try {
        const loadingTask = pdfjsLib.getDocument({
            url:MEDIA_URL,
            rangeChunkSize:65536,
            withCredentials:true
        });

        pdfDoc = await loadingTask.promise;

        pdfPageNumber = Math.max(
            1,
            Math.min(
                pdfDoc.numPages,
                pdfPageNumber
            )
        );

        await renderPdfPage();
        hideReaderLoading();
    } catch (error) {
        console.error(error);
        showReaderLoadError(
            error?.message || "Unable to open this PDF."
        );
    }
}

async function renderPdfPage() {
    if (!pdfDoc) {
        return;
    }

    setPageBusy(
        true,
        "Loading page " + pdfPageNumber + "..."
    );

    try {
        pdfPageNumber = Math.max(
            1,
            Math.min(
                pdfDoc.numPages,
                pdfPageNumber
            )
        );

        const page = await pdfDoc.getPage(
            pdfPageNumber
        );

        const viewport = page.getViewport({
            scale:pdfScale
        });

        const canvas = document.getElementById("pdfCanvas");
        const stage = document.getElementById("pdfStage");
        const textLayer = document.getElementById("pdfTextLayer");
        const linkLayer = document.getElementById("pdfLinkLayer");
        const annotationLayer = document.getElementById("pdfAnnotationLayer");
        const dpr = window.devicePixelRatio || 1;

        stage.style.width = viewport.width + "px";
        stage.style.height = viewport.height + "px";

        canvas.style.width = viewport.width + "px";
        canvas.style.height = viewport.height + "px";
        canvas.width = Math.floor(viewport.width * dpr);
        canvas.height = Math.floor(viewport.height * dpr);

        [textLayer, linkLayer, annotationLayer].forEach(layer => {
            if (!layer) return;
            layer.style.width = viewport.width + "px";
            layer.style.height = viewport.height + "px";
        });

        const context = canvas.getContext("2d");

        if (pdfRenderTask) {
            try {
                pdfRenderTask.cancel();
            } catch (error) {}
        }

        pdfRenderTask = page.render({
            canvasContext:context,
            viewport:viewport,
            transform:
                dpr !== 1
                    ? [dpr,0,0,dpr,0,0]
                    : null
        });

        await pdfRenderTask.promise;

        textLayer.innerHTML = "";
        textLayer.style.setProperty(
            "--scale-factor",
            pdfScale
        );

        const textContent = await page.getTextContent();

        const textTask = pdfjsLib.renderTextLayer({
            textContentSource:textContent,
            container:textLayer,
            viewport:viewport,
            textDivs:[]
        });

        if (textTask && textTask.promise) {
            await textTask.promise;
        }

        await renderPdfLinkLayer(
            page,
            viewport
        );

        document.getElementById("pageInput").value =
            pdfPageNumber;
        document.getElementById("pageInput").max =
            pdfDoc.numPages;

        const percent =
            (pdfPageNumber / pdfDoc.numPages) * 100;

        setProgress(percent);
        renderPdfAnnotations();

        saveReaderState({
            pdf_page:pdfPageNumber,
            pdf_scale:pdfScale,
            progress_percent:percent,
            theme:currentTheme
        });

        const area = document.getElementById("readerCanvasArea");
        area.scrollTop = 0;

    } catch (error) {
        if (
            String(error?.name || "")
            !== "RenderingCancelledException"
        ) {
            showReaderToast(
                "Unable to render this page: "
                + (error?.message || error)
            );
        }
    } finally {
        setPageBusy(false);
    }
}

async function renderPdfLinkLayer(
    page,
    viewport
) {
    const layer = document.getElementById("pdfLinkLayer");

    if (!layer) {
        return;
    }

    layer.innerHTML = "";

    let items = [];

    try {
        items = await page.getAnnotations({
            intent:"display"
        });
    } catch (error) {
        return;
    }

    for (const annotation of items) {
        if (
            annotation.subtype !== "Link" ||
            !annotation.rect
        ) {
            continue;
        }

        if (
            !annotation.url &&
            !annotation.dest &&
            !annotation.action
        ) {
            continue;
        }

        const rect = viewport.convertToViewportRectangle(
            annotation.rect
        );

        const left = Math.min(
            rect[0],
            rect[2]
        );
        const top = Math.min(
            rect[1],
            rect[3]
        );
        const width = Math.abs(
            rect[0] - rect[2]
        );
        const height = Math.abs(
            rect[1] - rect[3]
        );

        if (
            width < 2 ||
            height < 2
        ) {
            continue;
        }

        const hit = document.createElement("button");
        hit.type = "button";
        hit.className = "pdf-link-hit";
        hit.style.left = left + "px";
        hit.style.top = top + "px";
        hit.style.width = width + "px";
        hit.style.height = height + "px";

        hit.addEventListener(
            "click",
            event => {
                event.preventDefault();
                event.stopPropagation();
                followPdfLink(annotation);
            }
        );

        layer.appendChild(hit);
    }
}

async function followPdfLink(annotation) {
    try {
        if (annotation.url) {
            const url = String(
                annotation.url || ""
            );

            if (/^(https?:|mailto:)/i.test(url)) {
                window.open(
                    url,
                    "_blank",
                    "noopener,noreferrer"
                );
            }

            return;
        }

        if (annotation.dest) {
            await goToPdfDestination(
                annotation.dest
            );
            return;
        }

        const action = String(
            annotation.action || ""
        );

        if (action === "NextPage") {
            return goNext();
        }

        if (action === "PrevPage") {
            return goPrevious();
        }

        if (action === "FirstPage") {
            pdfPageNumber = 1;
            return renderPdfPage();
        }

        if (
            action === "LastPage" &&
            pdfDoc
        ) {
            pdfPageNumber = pdfDoc.numPages;
            return renderPdfPage();
        }
    } catch (error) {
        showReaderToast(
            "This PDF link could not be opened."
        );
    }
}

async function goToPdfDestination(destination) {
    if (!pdfDoc) {
        return;
    }

    let explicit = destination;

    if (typeof destination === "string") {
        explicit = await pdfDoc.getDestination(
            destination
        );
    }

    if (
        !Array.isArray(explicit) ||
        !explicit.length
    ) {
        return;
    }

    const target = explicit[0];
    let pageIndex = null;

    if (typeof target === "number") {
        pageIndex = target;
    } else if (
        target &&
        typeof target === "object"
    ) {
        pageIndex = await pdfDoc.getPageIndex(
            target
        );
    }

    if (
        pageIndex === null ||
        pageIndex === undefined
    ) {
        return;
    }

    pdfPageNumber = Math.max(
        1,
        Math.min(
            pdfDoc.numPages,
            Number(pageIndex) + 1
        )
    );

    await renderPdfPage();
}

async function goPrevious() {
    if (
        !pdfDoc ||
        pdfPageNumber <= 1
    ) {
        return;
    }

    pdfPageNumber -= 1;
    await renderPdfPage();
}

async function goNext() {
    if (
        !pdfDoc ||
        pdfPageNumber >= pdfDoc.numPages
    ) {
        return;
    }

    pdfPageNumber += 1;
    await renderPdfPage();
}

function jumpPdfPage() {
    if (!pdfDoc) {
        return;
    }

    const value = Number(
        document.getElementById("pageInput").value || 1
    );

    pdfPageNumber = Math.max(
        1,
        Math.min(
            pdfDoc.numPages,
            value
        )
    );

    renderPdfPage();
}

async function zoomPdf(delta) {
    pdfScale = Math.min(
        4,
        Math.max(
            .5,
            pdfScale + delta
        )
    );

    await renderPdfPage();
}

async function fitPdfWidth() {
    if (!pdfDoc) {
        return;
    }

    const page = await pdfDoc.getPage(
        pdfPageNumber
    );

    const base = page.getViewport({
        scale:1
    });

    const available = Math.max(
        280,
        document.getElementById(
            "readerCanvasArea"
        ).clientWidth - 24
    );

    pdfScale = Math.min(
        4,
        Math.max(
            .5,
            available / base.width
        )
    );

    await renderPdfPage();
}

async function fitPdfPage() {
    if (!pdfDoc) {
        return;
    }

    const page = await pdfDoc.getPage(
        pdfPageNumber
    );

    const base = page.getViewport({
        scale:1
    });

    const area = document.getElementById(
        "readerCanvasArea"
    );

    const widthScale = Math.max(
        .5,
        (area.clientWidth - 24) / base.width
    );

    const heightScale = Math.max(
        .5,
        (window.innerHeight - 210) / base.height
    );

    pdfScale = Math.min(
        4,
        widthScale,
        heightScale
    );

    await renderPdfPage();
}

async function findInSermon() {
    const query = document
        .getElementById("readerSearchInput")
        .value
        .trim();

    if (!query) {
        return;
    }

    try {
        const params = new URLSearchParams({
            q:query,
            after:String(pdfPageNumber)
        });

        const data = await apiJson(
            "/pastor-resources/sermon-ebooks/api/search/"
            + SERMON_ID
            + "?"
            + params.toString()
        );

        if (!data.match) {
            showReaderToast(
                'No match found for "' + query + '".'
            );
            return;
        }

        pdfPageNumber = Number(
            data.match.page_number
        );

        await renderPdfPage();

        showReaderToast(
            "Found on page "
            + pdfPageNumber
            + "."
        );
    } catch (error) {
        showReaderToast(error.message);
    }
}

let touchStartX = null;
let touchStartY = null;
let touchStartAt = 0;

const canvasArea = document.getElementById(
    "readerCanvasArea"
);

canvasArea.addEventListener(
    "touchstart",
    event => {
        if (event.touches.length !== 1) {
            touchStartX = null;
            return;
        }

        const touch = event.touches[0];
        touchStartX = touch.clientX;
        touchStartY = touch.clientY;
        touchStartAt = Date.now();
    },
    {passive:true}
);

canvasArea.addEventListener(
    "touchend",
    event => {
        if (
            touchStartX === null ||
            !event.changedTouches.length
        ) {
            return;
        }

        const touch = event.changedTouches[0];
        const dx = touch.clientX - touchStartX;
        const dy = touch.clientY - touchStartY;
        const elapsed = Date.now() - touchStartAt;

        touchStartX = null;

        const selection = window.getSelection();
        const selectingText =
            selection &&
            !selection.isCollapsed &&
            selection.toString().trim();

        if (selectingText) {
            scheduleSelectionCapture();
            return;
        }

        if (
            elapsed <= 800 &&
            Math.abs(dx) >= 70 &&
            Math.abs(dx) > Math.abs(dy) * 1.25
        ) {
            clearPendingSelection();

            if (dx < 0) {
                goNext();
            } else {
                goPrevious();
            }

            return;
        }

        scheduleSelectionCapture();
    },
    {passive:true}
);

document.addEventListener(
    "mouseup",
    event => {
        if (
            document
                .getElementById("pdfStage")
                .contains(event.target)
        ) {
            scheduleSelectionCapture();
        }
    }
);

document.addEventListener(
    "selectionchange",
    () => {
        const selection = window.getSelection();

        if (
            !selection ||
            selection.isCollapsed ||
            !selection.rangeCount
        ) {
            return;
        }

        const stage = document.getElementById("pdfStage");
        const range = selection.getRangeAt(0);

        if (
            stage &&
            stage.contains(range.commonAncestorContainer)
        ) {
            scheduleSelectionCapture();
        }
    }
);

document.getElementById(
    "readerSearchInput"
).addEventListener(
    "keydown",
    event => {
        if (event.key === "Enter") {
            event.preventDefault();
            findInSermon();
        }
    }
);

document.getElementById(
    "pageInput"
).addEventListener(
    "keydown",
    event => {
        if (event.key === "Enter") {
            event.preventDefault();
            jumpPdfPage();
        }
    }
);

async function initializeReader() {
    document.getElementById("themeSelect").value =
        currentTheme;
    setReaderTheme(currentTheme);

    try {
        await loadNotesData();
    } catch (error) {
        console.warn(
            "Unable to load annotations/bookmarks",
            error
        );
    }

    await initPdfReader();
}

initializeReader();
</script>
{% endblock %}
"""


# =========================================================
# ROUTES
# =========================================================


def register_sermon_ebooks_routes(app):
    ensure_sermon_tables()

    def require_sermon_json_access():
        if not any_user_logged_in():
            return jsonify(
                ok=False,
                error="Unauthorized",
            ), 401

        if not is_sermon_admin():
            return jsonify(
                ok=False,
                error="Forbidden",
            ), 403

        return None

    def sermon_user_key_or_error():
        user_key = current_sermon_user_key()

        if not user_key:
            raise RuntimeError(
                "Unable to determine the logged-in account."
            )

        return user_key

    @app.route(
        "/pastor-resources/sermon-ebooks"
    )
    def sermon_ebooks_home():
        if not any_user_logged_in():
            return redirect(
                url_for("splash")
            )

        if not is_sermon_admin():
            return redirect(
                url_for("pastor_resources")
            )

        db = get_db()

        try:
            counts = db.execute(
                """
                SELECT
                    SUM(
                        CASE
                        WHEN s.is_active = 1
                         AND NOT EXISTS (
                            SELECT 1
                            FROM sermon_hidden_items h
                            WHERE h.sermon_id = s.id
                         )
                        THEN 1 ELSE 0
                        END
                    ) AS active_total,
                    SUM(
                        CASE
                        WHEN s.is_active = 1
                         AND EXISTS (
                            SELECT 1
                            FROM sermon_hidden_items h
                            WHERE h.sermon_id = s.id
                         )
                        THEN 1 ELSE 0
                        END
                    ) AS hidden_total
                FROM sermon_library_files s
                """
            ).fetchone()

            initial_total = int(
                counts["active_total"] or 0
            )
            hidden_total = int(
                counts["hidden_total"] or 0
            )

        finally:
            db.close()

        return render_template_string(
            SERMON_EBOOKS_HTML,
            initial_total=initial_total,
            hidden_total=hidden_total,
            sync_status=get_sync_status(),
            bible_books=BIBLE_BOOKS,
        )

    @app.route(
        "/pastor-resources/sermon-ebooks/api/list"
    )
    def sermon_ebooks_api_list():
        blocked = require_sermon_json_access()
        if blocked:
            return blocked

        try:
            result = list_sermons(
                query=request.args.get(
                    "q",
                    "",
                ),
                sort_by=request.args.get(
                    "sort",
                    "text",
                ),
                direction=request.args.get(
                    "direction",
                    "asc",
                ),
                page=request.args.get(
                    "page",
                    1,
                ),
                view=request.args.get(
                    "view",
                    "active",
                ),
                book_index=request.args.get(
                    "book",
                    0,
                ),
            )

            status = get_sync_status()

            return jsonify(
                ok=True,
                **result,
                last_sync_at=status.get(
                    "last_sync_at"
                ),
            )

        except Exception as error:
            return jsonify(
                ok=False,
                error=safe_unicode(error),
            ), 500

    # Keep the proven working background synchronization unchanged.
    @app.route(
        "/pastor-resources/sermon-ebooks/sync",
        methods=["POST"],
    )
    def sermon_ebooks_sync():
        blocked = require_sermon_json_access()
        if blocked:
            return blocked

        try:
            started, state = start_sermon_sync(
                app
            )

            return jsonify(
                ok=True,
                started=started,
                state=state,
            )

        except Exception as error:
            return jsonify(
                ok=False,
                error=safe_unicode(error),
            ), 500

    @app.route(
        "/pastor-resources/sermon-ebooks/api/sync-status"
    )
    def sermon_ebooks_sync_status():
        blocked = require_sermon_json_access()
        if blocked:
            return blocked

        return jsonify(
            ok=True,
            state=get_live_sync_state(),
        )

    @app.route(
        "/pastor-resources/sermon-ebooks/api/edit/<int:sermon_id>",
        methods=["POST"],
    )
    def sermon_ebooks_edit(sermon_id):
        blocked = require_sermon_json_access()
        if blocked:
            return blocked

        payload = request.get_json(
            silent=True
        ) or {}

        try:
            edit_sermon_details(
                sermon_id=sermon_id,
                bible_text=payload.get(
                    "text"
                ),
                theme=payload.get(
                    "theme"
                ),
                created_date=payload.get(
                    "created_date"
                ),
            )

            return jsonify(ok=True)

        except Exception as error:
            return jsonify(
                ok=False,
                error=safe_unicode(error),
            ), 500

    @app.route(
        "/pastor-resources/sermon-ebooks/api/delete/<int:sermon_id>",
        methods=["POST"],
    )
    def sermon_ebooks_delete(sermon_id):
        blocked = require_sermon_json_access()
        if blocked:
            return blocked

        try:
            set_sermon_hidden(
                sermon_id,
                True,
                sermon_user_key_or_error(),
            )

            return jsonify(
                ok=True,
                drive_deleted=False,
            )

        except Exception as error:
            return jsonify(
                ok=False,
                error=safe_unicode(error),
            ), 500

    @app.route(
        "/pastor-resources/sermon-ebooks/api/restore/<int:sermon_id>",
        methods=["POST"],
    )
    def sermon_ebooks_restore(sermon_id):
        blocked = require_sermon_json_access()
        if blocked:
            return blocked

        try:
            set_sermon_hidden(
                sermon_id,
                False,
                sermon_user_key_or_error(),
            )

            return jsonify(ok=True)

        except Exception as error:
            return jsonify(
                ok=False,
                error=safe_unicode(error),
            ), 500

    # -----------------------------------------------------
    # SAME-TAB BUILT-IN PDF READER
    # -----------------------------------------------------

    @app.route(
        "/pastor-resources/sermon-ebooks/read/<int:sermon_id>"
    )
    def sermon_ebooks_read(sermon_id):
        if not any_user_logged_in():
            return redirect(
                url_for("splash")
            )

        if not is_sermon_admin():
            return redirect(
                url_for("pastor_resources")
            )

        row = get_sermon_row(
            sermon_id
        )

        if not row:
            return "Sermon PDF not found.", 404

        user_key = current_sermon_user_key()

        if not user_key:
            return redirect(
                url_for("splash")
            )

        state = get_sermon_reader_state(
            user_key,
            sermon_id,
        )

        requested_page = request.args.get(
            "page"
        )

        if requested_page:
            try:
                requested_page = max(
                    1,
                    int(requested_page),
                )
                state = dict(state)
                state["pdf_page"] = requested_page
            except Exception:
                pass

        sermon = {
            "id": int(
                row["id"]
            ),
            "theme": sermon_display_theme(
                row
            ),
            "text": sermon_display_text(
                row
            ),
            "filename": safe_unicode(
                row["filename"]
                or "sermon.pdf"
            ),
            "page_count": int(
                row["page_count"]
                or 0
            ),
        }

        return render_template_string(
            SERMON_READER_HTML,
            sermon=sermon,
            state=state,
            media_url=url_for(
                "sermon_ebooks_file",
                sermon_id=sermon_id,
            ),
            download_url=url_for(
                "sermon_ebooks_download",
                sermon_id=sermon_id,
            ),
        )

    @app.route(
        "/pastor-resources/sermon-ebooks/api/state/<int:sermon_id>",
        methods=["POST"],
    )
    def sermon_ebooks_reader_state(sermon_id):
        blocked = require_sermon_json_access()
        if blocked:
            return blocked

        payload = request.get_json(
            silent=True
        ) or {}

        try:
            if not get_sermon_row(
                sermon_id
            ):
                return jsonify(
                    ok=False,
                    error="Sermon PDF not found.",
                ), 404

            state = save_sermon_reader_state(
                sermon_user_key_or_error(),
                sermon_id,
                payload,
            )

            return jsonify(
                ok=True,
                state=state,
            )

        except Exception as error:
            return jsonify(
                ok=False,
                error=safe_unicode(error),
            ), 500

    @app.route(
        "/pastor-resources/sermon-ebooks/api/annotations/<int:sermon_id>",
        methods=["GET", "POST"],
    )
    def sermon_ebooks_annotations(sermon_id):
        blocked = require_sermon_json_access()
        if blocked:
            return blocked

        try:
            if not get_sermon_row(
                sermon_id
            ):
                return jsonify(
                    ok=False,
                    error="Sermon PDF not found.",
                ), 404

            user_key = sermon_user_key_or_error()

            if request.method == "GET":
                return jsonify(
                    ok=True,
                    annotations=get_sermon_annotations(
                        user_key,
                        sermon_id,
                    ),
                )

            payload = request.get_json(
                silent=True
            ) or {}

            annotation = add_sermon_annotation(
                user_key,
                sermon_id,
                payload,
            )

            return jsonify(
                ok=True,
                annotation=annotation,
            )

        except Exception as error:
            return jsonify(
                ok=False,
                error=safe_unicode(error),
            ), 500

    @app.route(
        "/pastor-resources/sermon-ebooks/api/annotation/<int:annotation_id>",
        methods=["POST"],
    )
    def sermon_ebooks_annotation_update(annotation_id):
        blocked = require_sermon_json_access()
        if blocked:
            return blocked

        payload = request.get_json(
            silent=True
        ) or {}

        try:
            update_sermon_annotation(
                sermon_user_key_or_error(),
                annotation_id,
                payload,
            )

            return jsonify(ok=True)

        except Exception as error:
            return jsonify(
                ok=False,
                error=safe_unicode(error),
            ), 500

    @app.route(
        "/pastor-resources/sermon-ebooks/api/annotation/<int:annotation_id>/delete",
        methods=["POST"],
    )
    def sermon_ebooks_annotation_delete(annotation_id):
        blocked = require_sermon_json_access()
        if blocked:
            return blocked

        try:
            delete_sermon_annotation(
                sermon_user_key_or_error(),
                annotation_id,
            )

            return jsonify(ok=True)

        except Exception as error:
            return jsonify(
                ok=False,
                error=safe_unicode(error),
            ), 500

    @app.route(
        "/pastor-resources/sermon-ebooks/api/bookmarks/<int:sermon_id>",
        methods=["GET", "POST"],
    )
    def sermon_ebooks_bookmarks(sermon_id):
        blocked = require_sermon_json_access()
        if blocked:
            return blocked

        try:
            if not get_sermon_row(
                sermon_id
            ):
                return jsonify(
                    ok=False,
                    error="Sermon PDF not found.",
                ), 404

            user_key = sermon_user_key_or_error()

            if request.method == "GET":
                return jsonify(
                    ok=True,
                    bookmarks=get_sermon_bookmarks(
                        user_key,
                        sermon_id,
                    ),
                )

            payload = request.get_json(
                silent=True
            ) or {}

            bookmark = add_sermon_bookmark(
                user_key,
                sermon_id,
                payload,
            )

            return jsonify(
                ok=True,
                bookmark=bookmark,
            )

        except Exception as error:
            return jsonify(
                ok=False,
                error=safe_unicode(error),
            ), 500

    @app.route(
        "/pastor-resources/sermon-ebooks/api/bookmark/<int:bookmark_id>/delete",
        methods=["POST"],
    )
    def sermon_ebooks_bookmark_delete(bookmark_id):
        blocked = require_sermon_json_access()
        if blocked:
            return blocked

        try:
            delete_sermon_bookmark(
                sermon_user_key_or_error(),
                bookmark_id,
            )

            return jsonify(ok=True)

        except Exception as error:
            return jsonify(
                ok=False,
                error=safe_unicode(error),
            ), 500

    @app.route(
        "/pastor-resources/sermon-ebooks/api/search/<int:sermon_id>"
    )
    def sermon_ebooks_reader_search(sermon_id):
        blocked = require_sermon_json_access()
        if blocked:
            return blocked

        try:
            if not get_sermon_row(
                sermon_id
            ):
                return jsonify(
                    ok=False,
                    error="Sermon PDF not found.",
                ), 404

            match = find_sermon_page_match(
                sermon_id,
                request.args.get(
                    "q",
                    "",
                ),
                request.args.get(
                    "after",
                    0,
                ),
            )

            return jsonify(
                ok=True,
                match=match,
            )

        except Exception as error:
            return jsonify(
                ok=False,
                error=safe_unicode(error),
            ), 500

    # -----------------------------------------------------
    # PRIVATE DRIVE PDF PROXY / DOWNLOAD
    # -----------------------------------------------------

    @app.route(
        "/pastor-resources/sermon-ebooks/file/<int:sermon_id>"
    )
    def sermon_ebooks_file(sermon_id):
        if not any_user_logged_in():
            return redirect(
                url_for("splash")
            )

        if not is_sermon_admin():
            return redirect(
                url_for("pastor_resources")
            )

        row = get_sermon_row(
            sermon_id
        )

        if not row:
            return "Sermon PDF not found.", 404

        return proxy_pdf_file(
            row,
            as_attachment=False,
        )

    @app.route(
        "/pastor-resources/sermon-ebooks/download/<int:sermon_id>"
    )
    def sermon_ebooks_download(sermon_id):
        if not any_user_logged_in():
            return redirect(
                url_for("splash")
            )

        if not is_sermon_admin():
            return redirect(
                url_for("pastor_resources")
            )

        row = get_sermon_row(
            sermon_id
        )

        if not row:
            return "Sermon PDF not found.", 404

        return proxy_pdf_file(
            row,
            as_attachment=True,
        )
