"""Pij AI retrieval layer for Pastor's Resources.

Public Pastor's Resources ebooks are searchable by every logged-in Pij user.
The separate Sermon eBooks collection is private and is included only when
sermon_ebooks.is_sermon_admin() authorizes the current account.

This module deliberately keeps authorization in Flask/Python. Private sermon
text is never sent to Gemini for an unauthorized user.
"""

import html
import io
import os
import re
import sqlite3
import threading
import time
import zipfile
from datetime import datetime, timezone
from xml.etree import ElementTree as ET

from flask import jsonify, session
from pypdf import PdfReader

import pastor_resources
import sermon_ebooks

INDEX_LOCK = threading.Lock()
INDEX_STATE_LOCK = threading.Lock()
INDEX_STATE = {
    "running": False,
    "stage": "idle",
    "message": "",
    "total": 0,
    "processed": 0,
    "indexed": 0,
    "skipped": 0,
    "errors": 0,
    "current_file": "",
    "started_at": "",
    "finished_at": "",
    "last_error": "",
    "queued": False,
    "queued_force": False,
}

MAX_FILE_BYTES = int(os.getenv("PIJ_LIBRARY_MAX_FILE_MB", "300")) * 1024 * 1024
CHUNK_TARGET_CHARS = int(os.getenv("PIJ_LIBRARY_CHUNK_CHARS", "4200"))
CHUNK_OVERLAP_CHARS = int(os.getenv("PIJ_LIBRARY_CHUNK_OVERLAP", "500"))
MAX_RESULTS = int(os.getenv("PIJ_LIBRARY_MAX_RESULTS", "6"))
MAX_CONTEXT_CHARS = int(os.getenv("PIJ_LIBRARY_MAX_CONTEXT_CHARS", "18000"))
SQLITE_BUSY_TIMEOUT_MS = int(os.getenv("PIJ_LIBRARY_SQLITE_BUSY_TIMEOUT_MS", "60000"))
SQLITE_LOCK_RETRIES = int(os.getenv("PIJ_LIBRARY_SQLITE_LOCK_RETRIES", "8"))


def _appmod():
    import app
    return app


def _db():
    # Each Flask request/background worker gets its own SQLite connection.
    # A generous busy timeout lets short writes finish instead of immediately
    # failing while the ebook indexer is inserting many chunks.
    timeout_seconds = max(1.0, SQLITE_BUSY_TIMEOUT_MS / 1000.0)
    db = sqlite3.connect(_appmod().DATABASE, timeout=timeout_seconds, check_same_thread=False)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys = ON")
    db.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS}")
    return db


def _is_database_locked(exc):
    text = str(exc or "").lower()
    return isinstance(exc, sqlite3.OperationalError) and (
        "database is locked" in text or "database table is locked" in text
    )


def _retry_locked(operation, label="SQLite operation"):
    """Retry only SQLite lock/busy failures; propagate every other error."""
    last_error = None
    for attempt in range(SQLITE_LOCK_RETRIES + 1):
        try:
            return operation()
        except Exception as exc:
            if not _is_database_locked(exc):
                raise
            last_error = exc
            if attempt >= SQLITE_LOCK_RETRIES:
                break
            delay = min(5.0, 0.35 * (attempt + 1))
            print(f"⏳ {label}: database busy; retry {attempt + 1}/{SQLITE_LOCK_RETRIES} in {delay:.2f}s")
            time.sleep(delay)
    raise last_error


def _now():
    return datetime.now(timezone.utc).isoformat()


def _clean(value):
    if value is None:
        return ""
    text = str(value).replace("\x00", " ")
    text = text.encode("utf-8", errors="replace").decode("utf-8", errors="replace")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _ensure_ai_library_tables_once():
    db = _db()
    try:
        # WAL allows normal readers (Pij/status/library pages) to continue while
        # the background indexer writes chunks. NORMAL is the recommended
        # durability/performance balance for WAL-backed application databases.
        db.execute("PRAGMA journal_mode = WAL")
        db.execute("PRAGMA synchronous = NORMAL")
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS pij_library_documents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_type TEXT NOT NULL,
                source_file_id INTEGER NOT NULL,
                drive_file_id TEXT,
                book_id INTEGER,
                title TEXT NOT NULL,
                author TEXT,
                category TEXT,
                folder_path TEXT,
                format TEXT,
                modified_time TEXT,
                checksum TEXT,
                page_count INTEGER NOT NULL DEFAULT 0,
                chunk_count INTEGER NOT NULL DEFAULT 0,
                searchable INTEGER NOT NULL DEFAULT 0,
                extract_error TEXT,
                indexed_at TEXT NOT NULL,
                UNIQUE(source_type, source_file_id)
            )
            """
        )
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS pij_library_chunks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                document_id INTEGER NOT NULL,
                chunk_number INTEGER NOT NULL,
                page_start INTEGER,
                page_end INTEGER,
                content TEXT NOT NULL,
                FOREIGN KEY(document_id) REFERENCES pij_library_documents(id) ON DELETE CASCADE,
                UNIQUE(document_id, chunk_number)
            )
            """
        )
        db.execute("CREATE INDEX IF NOT EXISTS idx_pij_library_doc_source ON pij_library_documents(source_type, source_file_id)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_pij_library_chunk_doc ON pij_library_chunks(document_id, chunk_number)")
        try:
            db.execute(
                """
                CREATE VIRTUAL TABLE IF NOT EXISTS pij_library_chunks_fts
                USING fts5(content, document_id UNINDEXED, chunk_number UNINDEXED)
                """
            )
        except sqlite3.OperationalError:
            pass
        db.commit()
    finally:
        db.close()


def ensure_ai_library_tables():
    return _retry_locked(_ensure_ai_library_tables_once, label="Preparing AI library tables")


def _fts_available(db):
    row = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='pij_library_chunks_fts'").fetchone()
    return bool(row)


def _extract_pdf(data):
    reader = PdfReader(io.BytesIO(data), strict=False)
    pages = []
    for page in reader.pages:
        try:
            pages.append(_clean(page.extract_text() or ""))
        except Exception:
            pages.append("")
    return pages


def _xml_text(data):
    try:
        root = ET.fromstring(data)
        return _clean(" ".join(t for t in root.itertext()))
    except Exception:
        text = data.decode("utf-8", errors="ignore")
        text = re.sub(r"<script\b[^>]*>.*?</script>", " ", text, flags=re.I | re.S)
        text = re.sub(r"<style\b[^>]*>.*?</style>", " ", text, flags=re.I | re.S)
        text = re.sub(r"<[^>]+>", " ", text)
        return _clean(html.unescape(text))


def _extract_epub(data):
    sections = []
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        names = zf.namelist()
        order = []
        try:
            container = ET.fromstring(zf.read("META-INF/container.xml"))
            rootfile = next((x for x in container.iter() if x.tag.split("}")[-1] == "rootfile"), None)
            opf_name = rootfile.attrib.get("full-path") if rootfile is not None else ""
            if opf_name:
                opf = ET.fromstring(zf.read(opf_name))
                base = os.path.dirname(opf_name)
                manifest = {}
                spine = []
                for node in opf.iter():
                    local = node.tag.split("}")[-1]
                    if local == "item":
                        manifest[node.attrib.get("id", "")] = node.attrib.get("href", "")
                    elif local == "itemref":
                        spine.append(node.attrib.get("idref", ""))
                for item_id in spine:
                    href = manifest.get(item_id)
                    if href:
                        order.append(os.path.normpath(os.path.join(base, href)).replace("\\", "/"))
        except Exception:
            order = []
        if not order:
            order = [n for n in names if n.lower().endswith((".xhtml", ".html", ".htm"))]
        seen = set()
        for name in order:
            if name in seen or name not in names:
                continue
            seen.add(name)
            try:
                text = _xml_text(zf.read(name))
            except Exception:
                continue
            if text:
                sections.append(text)
    return sections


def _make_chunks(sections):
    chunks = []
    chunk_no = 0
    for section_no, raw in enumerate(sections, start=1):
        text = _clean(raw)
        if not text:
            continue
        pos = 0
        length = len(text)
        while pos < length:
            end = min(length, pos + CHUNK_TARGET_CHARS)
            if end < length:
                boundary = max(text.rfind("\n", pos, end), text.rfind(". ", pos, end))
                if boundary > pos + CHUNK_TARGET_CHARS // 2:
                    end = boundary + 1
            piece = text[pos:end].strip()
            if piece:
                chunk_no += 1
                chunks.append((chunk_no, section_no, section_no, piece))
            if end >= length:
                break
            pos = max(pos + 1, end - CHUNK_OVERLAP_CHARS)
    return chunks


def _public_files_to_index():
    pastor_resources.ensure_resource_tables()
    db = _db()
    try:
        return db.execute(
            """
            SELECT f.id AS source_file_id, f.drive_file_id, f.book_id, f.name,
                   LOWER(COALESCE(f.format,'')) AS format, f.size, f.modified_time,
                   COALESCE(f.sha256_checksum, f.md5_checksum, '') AS checksum,
                   b.title, b.author, b.category, b.folder_path
            FROM pastor_library_files f
            JOIN pastor_library_books b ON b.id = f.book_id
            WHERE f.is_active = 1 AND b.is_active = 1
              AND COALESCE(b.is_hidden,0) = 0
              AND COALESCE(f.is_duplicate,0) = 0
              AND LOWER(COALESCE(f.format,'')) IN ('pdf','epub')
            ORDER BY b.title, f.id
            """
        ).fetchall()
    finally:
        db.close()


def _needs_reindex(row):
    """Return True for new/changed files AND for previously failed/empty indexes.

    Older behavior looked only at modified_time/checksum. That meant a document
    which had previously failed extraction was stored with searchable=0 and then
    skipped forever on later incremental passes because the Drive file itself had
    not changed. Healthy indexed books remain skipped.
    """
    db = _db()
    try:
        old = db.execute(
            """
            SELECT modified_time, checksum, searchable, chunk_count, extract_error
            FROM pij_library_documents
            WHERE source_type='public_ebook' AND source_file_id=?
            """,
            (int(row["source_file_id"]),),
        ).fetchone()

        if not old:
            return True

        metadata_changed = (
            _clean(old["modified_time"]) != _clean(row["modified_time"])
            or _clean(old["checksum"]) != _clean(row["checksum"])
        )
        if metadata_changed:
            return True

        # Retry only unhealthy records. This repairs failed/empty books without
        # rebuilding the already-good library index.
        if not bool(old["searchable"]):
            return True
        if int(old["chunk_count"] or 0) <= 0:
            return True
        if _clean(old["extract_error"]):
            return True

        return False
    finally:
        db.close()


def _store_public_document_once(row, chunks, page_count, error=""):
    db = _db()
    try:
        db.execute("BEGIN")
        existing = db.execute(
            "SELECT id FROM pij_library_documents WHERE source_type='public_ebook' AND source_file_id=?",
            (int(row["source_file_id"]),),
        ).fetchone()
        values = (
            row["drive_file_id"], row["book_id"], _clean(row["title"] or row["name"]), _clean(row["author"]),
            _clean(row["category"]), _clean(row["folder_path"]), _clean(row["format"]), _clean(row["modified_time"]),
            _clean(row["checksum"]), int(page_count), len(chunks), 1 if chunks else 0, _clean(error)[:4000], _now(),
        )
        if existing:
            document_id = int(existing["id"])
            db.execute(
                """UPDATE pij_library_documents SET drive_file_id=?,book_id=?,title=?,author=?,category=?,folder_path=?,format=?,modified_time=?,checksum=?,page_count=?,chunk_count=?,searchable=?,extract_error=?,indexed_at=? WHERE id=?""",
                values + (document_id,),
            )
            db.execute("DELETE FROM pij_library_chunks WHERE document_id=?", (document_id,))
            if _fts_available(db):
                db.execute("DELETE FROM pij_library_chunks_fts WHERE document_id=?", (document_id,))
        else:
            cur = db.execute(
                """INSERT INTO pij_library_documents(source_type,source_file_id,drive_file_id,book_id,title,author,category,folder_path,format,modified_time,checksum,page_count,chunk_count,searchable,extract_error,indexed_at) VALUES('public_ebook',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (int(row["source_file_id"]),) + values,
            )
            document_id = int(cur.lastrowid)
        for chunk_no, page_start, page_end, content in chunks:
            db.execute(
                "INSERT INTO pij_library_chunks(document_id,chunk_number,page_start,page_end,content) VALUES(?,?,?,?,?)",
                (document_id, chunk_no, page_start, page_end, content),
            )
            if _fts_available(db):
                db.execute(
                    "INSERT INTO pij_library_chunks_fts(content,document_id,chunk_number) VALUES(?,?,?)",
                    (content, document_id, chunk_no),
                )
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def _store_public_document(row, chunks, page_count, error=""):
    # A status request, Pastor's Resources sync, or another normal website
    # write may briefly hold SQLite. Retry the complete transaction so a
    # healthy ebook is not incorrectly recorded as a failed index.
    return _retry_locked(
        lambda: _store_public_document_once(row, chunks, page_count, error),
        label=f"Indexing {_clean(row['name'])}",
    )


def _store_index_error(row, error):
    """Best-effort error record; never let error logging kill the whole job."""
    try:
        _store_public_document(row, [], 0, str(error))
    except Exception as store_exc:
        print(f"⚠️ Could not save index error for {_clean(row['name'])}: {store_exc}")


def index_public_library(force=False):
    """Index new/changed public Pastor's Resources ebooks. Safe to run repeatedly."""
    ensure_ai_library_tables()
    if not INDEX_LOCK.acquire(blocking=False):
        return {"ok": False, "error": "An AI library indexing job is already running."}
    try:
        rows = _public_files_to_index()
        with INDEX_STATE_LOCK:
            INDEX_STATE.update(running=True, stage="indexing", message="Preparing Pastor's Resources for Pij...", total=len(rows), processed=0, indexed=0, skipped=0, errors=0, current_file="", started_at=_now(), finished_at="", last_error="")
        drive = pastor_resources.get_drive_session()
        indexed = skipped = errors = 0
        for number, row in enumerate(rows, start=1):
            with INDEX_STATE_LOCK:
                INDEX_STATE.update(processed=number - 1, current_file=_clean(row["name"]))
            if not force and not _needs_reindex(row):
                skipped += 1
                with INDEX_STATE_LOCK:
                    INDEX_STATE.update(processed=number, skipped=skipped)
                continue
            try:
                if int(row["size"] or 0) > MAX_FILE_BYTES:
                    raise ValueError(f"File exceeds AI indexing limit of {MAX_FILE_BYTES // (1024*1024)} MB")
                data = pastor_resources.download_drive_file_bytes(drive, row["drive_file_id"])
                fmt = _clean(row["format"]).lower()
                sections = _extract_pdf(data) if fmt == "pdf" else _extract_epub(data)
                chunks = _make_chunks(sections)
                _store_public_document(row, chunks, len(sections))
                indexed += 1
            except Exception as exc:
                errors += 1
                _store_index_error(row, exc)
                with INDEX_STATE_LOCK:
                    INDEX_STATE["last_error"] = f"{_clean(row['name'])}: {_clean(exc)}"
            with INDEX_STATE_LOCK:
                INDEX_STATE.update(processed=number, indexed=indexed, skipped=skipped, errors=errors)
        with INDEX_STATE_LOCK:
            INDEX_STATE.update(running=False, stage="complete", message="AI library indexing complete.", current_file="", finished_at=_now())
        return {"ok": True, "total": len(rows), "indexed": indexed, "skipped": skipped, "errors": errors}
    finally:
        with INDEX_STATE_LOCK:
            INDEX_STATE["running"] = False
        INDEX_LOCK.release()


def start_public_library_index(force=False, queue_if_running=True):
    """
    Start one background Pastor's Resources AI indexing pass.

    If a pass is already running (for example while the visible library is
    being synchronized), queue one more incremental pass. This guarantees
    that books added late in a Drive sync are not missed merely because the
    earlier AI indexing snapshot was already in progress.
    """
    with INDEX_STATE_LOCK:
        if INDEX_STATE.get("running"):
            if queue_if_running:
                INDEX_STATE["queued"] = True
                if force:
                    INDEX_STATE["queued_force"] = True
                INDEX_STATE["message"] = (
                    "AI indexing is already running; another incremental pass is queued."
                )
            return False

        INDEX_STATE.update(
            running=True,
            stage="starting",
            message="Starting AI library index...",
            started_at=_now(),
            finished_at="",
        )

    def worker():
        try:
            index_public_library(force=force)
        except Exception as exc:
            with INDEX_STATE_LOCK:
                INDEX_STATE.update(
                    running=False,
                    stage="error",
                    message="AI library indexing stopped because of an error.",
                    last_error=_clean(exc),
                    finished_at=_now(),
                )
            print(f"❌ Pij library indexing error: {exc}")
        finally:
            rerun = False
            rerun_force = False
            with INDEX_STATE_LOCK:
                rerun = bool(INDEX_STATE.get("queued"))
                rerun_force = bool(INDEX_STATE.get("queued_force"))
                INDEX_STATE["queued"] = False
                INDEX_STATE["queued_force"] = False

            if rerun:
                # index_public_library() has already released INDEX_LOCK here.
                time.sleep(0.10)
                start_public_library_index(
                    force=rerun_force,
                    queue_if_running=False,
                )

    thread = threading.Thread(
        target=worker,
        daemon=True,
        name="pij-library-index",
    )
    thread.start()
    return True


def get_index_state():
    with INDEX_STATE_LOCK:
        state = dict(INDEX_STATE)
    # Ensure schema before opening the read connection. Opening a second
    # schema-writing connection while this one is alive can create avoidable
    # lock contention during background indexing.
    ensure_ai_library_tables()
    db = _db()
    try:
        row = db.execute(
            """
            SELECT COUNT(*) AS docs,
                   COALESCE(SUM(d.chunk_count),0) AS chunks
            FROM pij_library_documents d
            JOIN pastor_library_books b ON b.id=d.book_id
            WHERE d.source_type='public_ebook'
              AND d.searchable=1
              AND b.is_active=1
              AND COALESCE(b.is_hidden,0)=0
            """
        ).fetchone()
        state["searchable_documents"] = int(row["docs"] or 0)
        state["chunks"] = int(row["chunks"] or 0)
    finally:
        db.close()
    return state


# =========================================================
# SMART RETRIEVAL
# =========================================================

_QUERY_STOPWORDS = {
    "the", "a", "an", "and", "or", "of", "to", "in", "on", "for", "is", "are",
    "was", "were", "can", "could", "would", "should", "you", "me", "my", "i", "we",
    "our", "what", "who", "how", "when", "where", "why", "which", "please", "po",
    "ba", "ang", "ng", "sa", "mga", "ako", "ko", "mo", "nga", "ug", "unsa", "pwede",
    "pila", "nako", "nimo", "paano", "ano", "saan", "mula", "gikan", "from", "about",
    "give", "show", "tell", "find", "suggest", "recommend", "recommended", "resource",
    "resources", "ebook", "ebooks", "book", "books", "library", "pastor", "according",
    "based", "using", "used", "main", "key", "some", "helpful", "topic", "subject",
    "that", "this", "these", "those", "do", "does", "did", "show", "shows",
    "teach", "teaches", "teaching", "principle", "principles", "lesson", "lessons",
    "sermon", "sermons", "illustration", "illustrations", "outline", "outlines",
}

_GENERIC_THEOLOGY_TERMS = {
    "god", "lord", "jesus", "christ", "bible", "biblical", "scripture", "scriptures",
    "christian", "christians", "church", "pastor", "ministry", "faith",
}

_LIBRARY_CUES = {
    "book", "books", "ebook", "ebooks", "library", "resource", "resources", "author",
    "chapter", "page", "pages", "pastor's resources", "pastors resources",
    "sermon", "sermons", "illustration", "illustrations", "commentary", "commentaries",
}

_STUDY_TOPIC_CUES = {
    "discipleship", "disciple", "disciples", "theology", "doctrine", "evangelism",
    "worship", "leadership", "salvation", "grace", "sanctification", "justification",
    "prayer", "fasting", "missions", "mission", "marriage", "family", "counseling",
    "counselling", "preaching", "homiletics", "devotional", "devotion", "apologetics",
}

_WEBSITE_OPERATION_CUES = {
    "attendance", "report", "reports", "approve", "approval", "approved", "submit",
    "resubmit", "schedule", "schedules", "thanksgiving", "chain prayer", "church status",
    "area progress", "pastor's tool", "pastor tool", "ao tool", "prayer request",
    "account", "accounts", "login", "button", "menu", "church finder", "bulletin",
}

_RECOMMENDATION_CUES = {
    "suggest", "recommend", "recommendation", "recommendations", "books", "book",
    "resources", "resource", "reading", "read",
}

_SYNTHESIS_CUES = {
    "principle", "principles", "theme", "themes", "teach", "teaches", "teaching",
    "lessons", "lesson", "ideas", "truths", "according", "based", "compare", "summary",
    "summarize", "explain",
}

_SERMON_METADATA_CUES = {
    "sermon", "sermons", "preaching", "preacher", "homiletic", "homiletics",
    "illustration", "illustrations", "outline", "outlines",
}


def _word_tokens(value):
    return re.findall(r"[A-Za-zÀ-ÿ0-9]+", _clean(value).lower())


def _normalize_words(value):
    return " ".join(_word_tokens(value))


def _query_terms(question):
    out = []
    for word in _word_tokens(question):
        if len(word) < 2 or word in _QUERY_STOPWORDS:
            continue
        if word not in out:
            out.append(word)
    return out[:12]


def _term_variants(term):
    """Small English morphology helper; keeps retrieval local and API-free."""
    term = _clean(term).lower()
    variants = [term] if term else []
    special = {
        "loving": ["love"],
        "loved": ["love"],
        "loves": ["love"],
        "discipleship": ["disciple", "disciples"],
        "disciples": ["disciple", "discipleship"],
        "praying": ["prayer", "pray"],
        "prayers": ["prayer", "pray"],
        "leaders": ["leader", "leadership"],
        "leadership": ["leader", "leaders"],
        "evangelism": ["evangelize", "evangelistic"],
        "missions": ["mission"],
        "sermons": ["sermon"],
        "illustrations": ["illustration"],
    }
    special_variants = special.get(term, [])
    variants.extend(special_variants)
    if not special_variants and term.endswith("ing") and len(term) > 5:
        base = term[:-3]
        variants.extend([base, base + "e"])
    elif not special_variants and term.endswith("ies") and len(term) > 4:
        variants.append(term[:-3] + "y")
    elif not special_variants and term.endswith("s") and len(term) > 4:
        variants.append(term[:-1])
    return list(dict.fromkeys(v for v in variants if len(v) >= 2))[:4]


def _contains_any_phrase(text, phrases):
    low = _clean(text).lower()
    return any(p in low for p in phrases)


def _library_mode(question):
    """Return retrieval mode without another model/API call."""
    low = _clean(question).lower()
    words = set(_word_tokens(question))

    explicit_library = any(cue in low for cue in _LIBRARY_CUES)
    study_topic = bool(words & _STUDY_TOPIC_CUES)
    website_operation = any(cue in low for cue in _WEBSITE_OPERATION_CUES)

    # Website workflow/data questions should not be polluted with unrelated ebooks
    # unless the user explicitly asks for a book/library source.
    if website_operation and not explicit_library:
        return "none"

    if not explicit_library and not study_topic:
        return "none"

    create_sermon = bool(
        re.search(r"\b(create|write|make|draft|prepare)\b.*\bsermon\b", low)
        or re.search(r"\b(give|provide)\b.*\bsermon\s+(?:outline|manuscript|message)\b", low)
    )
    if create_sermon and not any(x in low for x in ["from pastor's resources", "from pastors resources", "existing", "find"]):
        return "none"
    if ("sermon" in low or "illustration" in low) and not create_sermon:
        return "sermon_retrieval"
    if create_sermon:
        return "sermon_retrieval"

    # A request for principles/themes/teaching is synthesis even when the user
    # explicitly says "Pastor's Resources". Recommendation intent wins only
    # when the user actually asks for books/resources to choose from.
    if words & _SYNTHESIS_CUES:
        return "synthesis"

    if (
        words & {"suggest", "recommend", "recommendation", "recommendations", "books", "reading"}
        or re.search(r"\b\d+\s+(?:books?|ebooks?|resources?)\b", low)
        or re.search(r"\b(?:a|one)\s+book\b", low)
    ):
        return "recommendation"

    if "pastor's resources" in low or "pastors resources" in low:
        return "synthesis"

    return "study"


def _requested_book_count(question, default=4):
    low = _clean(question).lower()
    if re.search(r"\b(?:1|one|a)\s+(?:book|ebook|resource)\b", low):
        return 1
    m = re.search(r"\b([2-6])\s+(?:books?|ebooks?|resources?)\b", low)
    if m:
        return max(2, min(6, int(m.group(1))))
    word_numbers = {"two": 2, "three": 3, "four": 4, "five": 5, "six": 6}
    for word, number in word_numbers.items():
        if re.search(rf"\b{word}\s+(?:books?|ebooks?|resources?)\b", low):
            return number
    return default


def _expanded_terms(terms):
    out = []
    for term in terms:
        for variant in _term_variants(term):
            if variant not in out:
                out.append(variant)
    return out[:24]


def _fts_expression(terms, strict=True):
    groups = []
    for term in terms[:8]:
        variants = [v.replace('"', '') for v in _term_variants(term) if v]
        if not variants:
            continue
        if len(variants) == 1:
            groups.append(f'"{variants[0]}"')
        else:
            groups.append("(" + " OR ".join(f'"{v}"' for v in variants) + ")")
    if not groups:
        return ""
    joiner = " AND " if strict and len(groups) > 1 else " OR "
    return joiner.join(groups)


def _active_indexed_books(db):
    return db.execute(
        """
        SELECT DISTINCT b.id AS book_id, b.title, b.author, b.category, b.folder_path
        FROM pastor_library_books b
        JOIN pij_library_documents d ON d.book_id=b.id
        WHERE d.source_type='public_ebook'
          AND d.searchable=1
          AND b.is_active=1
          AND COALESCE(b.is_hidden,0)=0
        """
    ).fetchall()


def _metadata_score(row, terms, mode):
    title = _normalize_words(row["title"])
    author = _normalize_words(row["author"])
    category = _normalize_words(row["category"])
    folder = _normalize_words(row["folder_path"])
    score = 0.0

    for term in terms:
        variants = _term_variants(term)
        generic = term in _GENERIC_THEOLOGY_TERMS
        title_weight = 2.0 if generic else 8.0
        category_weight = 1.0 if generic else 3.5
        folder_weight = 0.75 if generic else 2.5
        author_weight = 0.5 if generic else 1.0
        if any(v in title for v in variants):
            score += title_weight
        if any(v in category for v in variants):
            score += category_weight
        if any(v in folder for v in variants):
            score += folder_weight
        if any(v in author for v in variants):
            score += author_weight

    metadata_blob = " ".join([title, category, folder])
    if mode == "sermon_retrieval" and any(cue in metadata_blob for cue in _SERMON_METADATA_CUES):
        score += 14.0
    return score


def _extract_title_hint(question):
    q = _clean(question)
    quoted = re.findall(r'["“](.+?)["”]', q)
    if quoted:
        return max(quoted, key=len).strip()

    patterns = [
        r"(?i)according\s+to\s+(.+?)(?:,|\?|\bwhat\b|\bhow\b|\bwhy\b|\bwho\b|$)",
        r"(?i)in\s+the\s+book\s+(.+?)(?:,|\?|\bwhat\b|\bhow\b|\bwhy\b|$)",
        r"(?i)from\s+the\s+book\s+(.+?)(?:,|\?|\bwhat\b|\bhow\b|\bwhy\b|$)",
    ]
    for pattern in patterns:
        m = re.search(pattern, q)
        if m:
            hint = m.group(1).strip(" .,:;?-")
            if len(hint) >= 4:
                return hint
    return ""


def _find_specific_book(db, question, terms):
    books = _active_indexed_books(db)
    if not books:
        return None

    hint = _extract_title_hint(question)
    hint_words = [w for w in _word_tokens(hint) if len(w) >= 2 and w not in _QUERY_STOPWORDS]
    best = None
    best_score = 0.0

    for row in books:
        title_norm = _normalize_words(row["title"])
        title_words = set(_word_tokens(row["title"]))
        score = 0.0

        if hint:
            hint_norm = _normalize_words(hint)
            if hint_norm and hint_norm in title_norm:
                score += 40.0
            overlap = sum(1 for w in hint_words if w in title_words)
            score += overlap * 8.0
            if hint_words:
                score += (overlap / len(hint_words)) * 15.0
        else:
            distinctive = [t for t in terms if t not in _GENERIC_THEOLOGY_TERMS]
            overlap = sum(1 for t in distinctive if any(v in title_norm for v in _term_variants(t)))
            if overlap >= 2:
                score += overlap * 7.0

        if score > best_score:
            best = row
            best_score = score

    # A hint can be somewhat fuzzy; without an explicit hint require stronger evidence.
    threshold = 18.0 if hint else 16.0
    return best if best is not None and best_score >= threshold else None


def _search_chunks(db, terms, limit=120, book_id=None, strict=True):
    terms = [t for t in terms if t]
    if not terms:
        return []

    book_sql = " AND d.book_id=? " if book_id is not None else ""

    if _fts_available(db):
        expr = _fts_expression(terms, strict=strict)
        if expr:
            params = [expr]
            if book_id is not None:
                params.append(int(book_id))
            params.append(int(limit))
            try:
                return db.execute(
                    f"""
                    SELECT d.id AS document_id,d.book_id,d.title,d.author,d.category,d.folder_path,d.format,
                           c.chunk_number,c.page_start,c.page_end,c.content,
                           bm25(pij_library_chunks_fts) AS rank
                    FROM pij_library_chunks_fts f
                    JOIN pij_library_chunks c
                      ON c.document_id=CAST(f.document_id AS INTEGER)
                     AND c.chunk_number=CAST(f.chunk_number AS INTEGER)
                    JOIN pij_library_documents d ON d.id=c.document_id
                    JOIN pastor_library_books b ON b.id=d.book_id
                    WHERE pij_library_chunks_fts MATCH ?
                      AND d.source_type='public_ebook'
                      AND d.searchable=1
                      AND b.is_active=1
                      AND COALESCE(b.is_hidden,0)=0
                      {book_sql}
                    ORDER BY rank
                    LIMIT ?
                    """,
                    params,
                ).fetchall()
            except sqlite3.OperationalError:
                pass

    # Fallback for SQLite builds without FTS5, or syntax edge cases.
    term_groups = []
    like_params = []
    for term in terms:
        variants = _term_variants(term)
        if not variants:
            continue
        group = "(" + " OR ".join("LOWER(c.content) LIKE ?" for _ in variants) + ")"
        term_groups.append(group)
        like_params.extend(f"%{v}%" for v in variants)
    if not term_groups:
        return []
    joiner = " AND " if strict and len(term_groups) > 1 else " OR "
    clauses = joiner.join(term_groups)
    params = ([int(book_id)] if book_id is not None else []) + like_params + [int(limit)]
    return db.execute(
        f"""
        SELECT d.id AS document_id,d.book_id,d.title,d.author,d.category,d.folder_path,d.format,
               c.chunk_number,c.page_start,c.page_end,c.content,0 AS rank
        FROM pij_library_chunks c
        JOIN pij_library_documents d ON d.id=c.document_id
        JOIN pastor_library_books b ON b.id=d.book_id
        WHERE d.source_type='public_ebook'
          AND d.searchable=1
          AND b.is_active=1
          AND COALESCE(b.is_hidden,0)=0
          {book_sql}
          AND ({clauses})
        LIMIT ?
        """,
        params,
    ).fetchall()


def _rank_books(db, question, mode, desired_books):
    terms = _query_terms(question)
    if not terms:
        return [], mode

    specific = _find_specific_book(db, question, terms)
    if specific is not None:
        title_words = set(_word_tokens(specific["title"]))
        topic_terms = [
            t for t in terms
            if not any(v in title_words for v in _term_variants(t))
            and t not in {"principle", "principles", "theme", "themes", "lesson", "lessons"}
        ]
        if not topic_terms:
            topic_terms = [t for t in terms if t not in {"design"}] or terms
        rows = _search_chunks(db, topic_terms, limit=40, book_id=specific["book_id"], strict=False)
        if not rows:
            rows = _search_chunks(db, terms, limit=40, book_id=specific["book_id"], strict=False)
        chosen = sorted(
            rows,
            key=lambda r: (float(r["rank"] or 0), int(r["page_start"] or 0)),
        )[:4]
        if chosen:
            return [{
                "book_id": int(specific["book_id"]),
                "title": specific["title"],
                "author": specific["author"],
                "category": specific["category"],
                "folder_path": specific["folder_path"],
                "score": 100.0,
                "chunks": chosen,
            }], "specific_book"

    # Prefer stricter multi-term matches first, then broaden if necessary.
    rows = _search_chunks(db, terms, limit=140, strict=True)
    if len(rows) < max(8, desired_books * 2):
        broad = _search_chunks(db, terms, limit=180, strict=False)
        seen = {(r["document_id"], r["chunk_number"]) for r in rows}
        rows = list(rows) + [r for r in broad if (r["document_id"], r["chunk_number"]) not in seen]

    by_book = {}
    for position, row in enumerate(rows):
        bid = int(row["book_id"])
        entry = by_book.setdefault(bid, {
            "book_id": bid,
            "title": row["title"],
            "author": row["author"],
            "category": row["category"],
            "folder_path": row["folder_path"],
            "score": 0.0,
            "chunks": [],
        })

        if not entry["chunks"]:
            entry["score"] += _metadata_score(row, terms, mode)

        # Do not let very large Study Bibles/commentaries win merely because
        # they have hundreds of matching chunks. Score only the first few
        # passages from each book and cap stored candidates for diversity.
        book_chunk_index = len(entry["chunks"])
        if book_chunk_index < 5:
            entry["chunks"].append(row)
        if book_chunk_index < 3:
            content_norm = _normalize_words(row["content"])
            strong_hits = 0
            generic_hits = 0
            for term in terms:
                hit = any(v in content_norm for v in _term_variants(term))
                if not hit:
                    continue
                if term in _GENERIC_THEOLOGY_TERMS:
                    generic_hits += 1
                else:
                    strong_hits += 1
            entry["score"] += strong_hits * 3.0 + generic_hits * 0.7
            entry["score"] += max(0.2, 8.0 / (1.0 + position * 0.18))

    # Add title/category candidates even if their best matching chunk was outside the FTS top window.
    for book in _active_indexed_books(db):
        bid = int(book["book_id"])
        meta = _metadata_score(book, terms, mode)
        if meta < 7.0:
            continue
        if bid not in by_book:
            topic_rows = _search_chunks(db, terms, limit=8, book_id=bid, strict=False)
            if not topic_rows:
                continue
            by_book[bid] = {
                "book_id": bid,
                "title": book["title"],
                "author": book["author"],
                "category": book["category"],
                "folder_path": book["folder_path"],
                "score": meta,
                "chunks": list(topic_rows),
            }
        else:
            by_book[bid]["score"] += meta * 0.35

    ranked = sorted(by_book.values(), key=lambda x: (-x["score"], str(x["title"] or "").lower()))
    return ranked[:max(desired_books, 6)], mode


def _private_sermon_search(question, limit):
    if not sermon_ebooks.is_sermon_admin():
        return []
    terms = _query_terms(question)
    # Sermon/illustration are intent words, not the topic itself.
    topic_terms = [t for t in terms if t not in {"sermon", "sermons", "illustration", "illustrations", "outline", "outlines"}]
    if not topic_terms:
        topic_terms = terms
    if not topic_terms:
        return []

    sermon_ebooks.ensure_sermon_tables()
    db = _db()
    try:
        expanded = _expanded_terms(topic_terms)
        clauses = " OR ".join("LOWER(p.page_text) LIKE ?" for _ in expanded)
        params = [f"%{t}%" for t in expanded] + [max(int(limit) * 6, 20)]
        rows = db.execute(
            f"""
            SELECT s.id AS sermon_id, s.filename AS title,
                   COALESCE(NULLIF(s.manual_theme,''),s.detected_theme,'') AS theme,
                   p.page_number,p.page_text
            FROM sermon_library_pages p
            JOIN sermon_library_files s ON s.id=p.sermon_id
            WHERE s.is_active=1
              AND NOT EXISTS(SELECT 1 FROM sermon_hidden_items h WHERE h.sermon_id=s.id)
              AND ({clauses})
            LIMIT ?
            """,
            params,
        ).fetchall()

        def score(row):
            blob = _normalize_words((row["title"] or "") + " " + (row["theme"] or "") + " " + (row["page_text"] or ""))
            return sum(3 if t not in _GENERIC_THEOLOGY_TERMS else 1 for t in topic_terms if any(v in blob for v in _term_variants(t)))

        return sorted(rows, key=lambda r: (-score(r), int(r["page_number"] or 0)))[:int(limit)]
    finally:
        db.close()


def _public_reader_link(row):
    """Build only server-owned relative reader links from integer IDs."""
    try:
        book_id = int(row["book_id"])
    except Exception:
        return ""

    fmt = _clean(row["format"]).upper()
    try:
        location = max(1, int(row["page_start"] or 1))
    except Exception:
        location = 1

    if fmt == "PDF":
        return f"/pastor-resources/read/{book_id}?format=PDF&page={location}"
    if fmt == "EPUB":
        return f"/pastor-resources/read/{book_id}?format=EPUB&section={location}"
    return f"/pastor-resources/read/{book_id}"


def _private_sermon_reader_link(row):
    try:
        sermon_id = int(row["sermon_id"])
        page_number = max(1, int(row["page_number"] or 1))
    except Exception:
        return ""
    return f"/pastor-resources/sermon-ebooks/read/{sermon_id}?page={page_number}"


def _format_public_evidence(book, max_chunks):
    blocks = []
    seen_locations = set()
    for row in book["chunks"]:
        location_key = (str(row["format"] or "").upper(), int(row["page_start"] or 1))
        if location_key in seen_locations:
            continue
        seen_locations.add(location_key)

        fmt = _clean(row["format"]).upper() or "EBOOK"
        try:
            location_number = max(1, int(row["page_start"] or 1))
        except Exception:
            location_number = 1
        if fmt == "PDF":
            location = f"PDF page {location_number}"
        elif fmt == "EPUB":
            location = f"EPUB section {location_number}"
        else:
            location = f"section {location_number}"

        link = _public_reader_link(row)
        blocks.append(
            f"EVIDENCE LOCATION: {location}\n"
            + (f"APPROVED LINK: {link}\n" if link else "")
            + f"PASSAGE: {_clean(row['content'])}\n"
        )
        if len(blocks) >= max_chunks:
            break
    return blocks



def _catalog_specific_book_for_question(question):
    """Find a likely named Pastor's Resources book, including unindexed books.

    This is deliberately deterministic and local: no Gemini/API call is used.
    A match must be strong enough that a broad topic question is not mistaken
    for a specific title.
    """
    pastor_resources.ensure_resource_tables()
    terms = [
        t for t in _query_terms(question)
        if t not in _GENERIC_THEOLOGY_TERMS
        and t not in {"database", "index", "indexed", "available", "availability", "information", "info"}
    ]
    hint = _extract_title_hint(question)
    hint_terms = [
        w for w in _word_tokens(hint)
        if len(w) >= 2 and w not in _QUERY_STOPWORDS
    ]

    db = _db()
    try:
        rows = db.execute(
            """
            SELECT b.id AS book_id,b.title,b.author,b.category,b.folder_path,
                   MAX(CASE WHEN d.searchable=1 THEN 1 ELSE 0 END) AS indexed,
                   MAX(COALESCE(d.page_count,0)) AS page_count,
                   MAX(COALESCE(d.chunk_count,0)) AS chunk_count,
                   MAX(COALESCE(d.extract_error,'')) AS extract_error
            FROM pastor_library_books b
            LEFT JOIN pij_library_documents d
              ON d.book_id=b.id AND d.source_type='public_ebook'
            WHERE b.is_active=1 AND COALESCE(b.is_hidden,0)=0
            GROUP BY b.id
            """
        ).fetchall()
    finally:
        db.close()

    best = None
    best_score = 0.0
    for row in rows:
        title_norm = _normalize_words(row["title"])
        title_words = set(_word_tokens(row["title"]))
        score = 0.0

        if hint:
            hint_norm = _normalize_words(hint)
            if hint_norm and hint_norm == title_norm:
                score += 100.0
            elif hint_norm and hint_norm in title_norm:
                score += 55.0
            overlap = sum(1 for w in hint_terms if w in title_words)
            score += overlap * 12.0
            if hint_terms:
                score += (overlap / len(hint_terms)) * 20.0
        else:
            distinctive = [t for t in terms if len(t) >= 3]
            overlap = sum(
                1 for t in distinctive
                if any(v in title_norm for v in _term_variants(t))
            )
            if overlap >= 2:
                score += overlap * 14.0
                score += (overlap / max(1, len(distinctive))) * 18.0

        if score > best_score:
            best = row
            best_score = score

    threshold = 24.0 if hint else 34.0
    return best if best is not None and best_score >= threshold else None


_BOOK_FOLLOWUP_PHRASES = (
    "this book", "that book", "the book", "this ebook", "that ebook",
    "this resource", "that resource", "this one", "that one",
)

_GENERIC_BOOK_INFO_TERMS = {
    "information", "info", "overview", "summary", "summarize", "details",
    "detail", "quick", "contents", "content", "availability", "available",
    "database", "index", "indexed",
}


def _recent_catalog_book_from_history(question, history):
    """Resolve 'this book' / 'it' from recent USER turns without another AI call."""
    if not history:
        return None

    low = _clean(question).lower()
    words = _word_tokens(question)
    explicit_followup = any(p in low for p in _BOOK_FOLLOWUP_PHRASES)

    # Permit a short pronoun follow-up such as "tell me more about it" only when
    # it is genuinely short. This avoids dragging an old book into a new topic.
    short_it_followup = (
        len(words) <= 12
        and re.search(r"\b(it|its)\b", low)
        and any(x in low for x in ("tell", "more", "summary", "summarize", "explain", "information", "info", "about"))
    )

    if not (explicit_followup or short_it_followup):
        return None

    for item in reversed(history[-8:]):
        if str(item.get("role") or "").lower() != "user":
            continue
        content = _clean(item.get("content"))
        if not content:
            continue
        book = _catalog_specific_book_for_question(content)
        if book is not None:
            return book

    return None


def _opening_chunks_for_book(db, book_id, limit=6):
    """Return opening searchable chunks for an exact book.

    Useful for requests such as "give me quick information about this book",
    where there is no topic term worth searching for. This never substitutes
    another book.
    """
    return db.execute(
        """
        SELECT d.id AS document_id,d.book_id,d.title,d.author,d.category,d.folder_path,d.format,
               c.chunk_number,c.page_start,c.page_end,c.content,
               0.0 AS rank
        FROM pij_library_chunks c
        JOIN pij_library_documents d ON d.id=c.document_id
        JOIN pastor_library_books b ON b.id=d.book_id
        WHERE d.source_type='public_ebook'
          AND d.searchable=1
          AND d.book_id=?
          AND b.is_active=1
          AND COALESCE(b.is_hidden,0)=0
        ORDER BY COALESCE(c.page_start, c.chunk_number), c.chunk_number
        LIMIT ?
        """,
        (int(book_id), max(1, min(12, int(limit or 6)))),
    ).fetchall()

def library_context_for_gemini(question, history=None):
    """Retrieve book-aware evidence locally, with recent-turn follow-up support."""
    history = history or []

    # Resolve the book BEFORE deciding that retrieval is unnecessary. This lets
    # normal follow-ups such as "Can you give me quick information about this
    # book?" stay attached to the book named in the preceding user turn.
    direct_catalog_book = _catalog_specific_book_for_question(question)
    followup_catalog_book = (
        None if direct_catalog_book is not None
        else _recent_catalog_book_from_history(question, history)
    )
    catalog_book = direct_catalog_book or followup_catalog_book

    mode = _library_mode(question)
    if mode == "none" and catalog_book is not None:
        mode = "synthesis"

    if mode == "none":
        return (
            "PASTOR'S RESOURCES AI RETRIEVAL\n"
            "- Library retrieval was not needed for this question. Do not force ebook content into the answer."
        )

    ensure_ai_library_tables()

    if catalog_book is not None and not bool(catalog_book["indexed"]):
        title = _clean(catalog_book["title"])
        author = _clean(catalog_book["author"])
        page_count = int(catalog_book["page_count"] or 0)
        index_error = _clean(catalog_book["extract_error"])
        return (
            "PASTOR'S RESOURCES AI RETRIEVAL\n"
            "RETRIEVAL MODE: SPECIFIC_BOOK_CATALOG_ONLY\n"
            f"CATALOG BOOK ID: {int(catalog_book['book_id'])}\n"
            f"CATALOG TITLE: {title}\n"
            + (f"CATALOG AUTHOR: {author}\n" if author else "")
            + "INDEXED/SEARCHABLE CONTENT: NO\n"
            + (f"KNOWN PAGE COUNT: {page_count}\n" if page_count else "KNOWN PAGE COUNT: unavailable\n")
            + (f"INDEX NOTE: {index_error}\n" if index_error else "")
            + "APPROVED LIBRARY LINK: /pastor-resources\n"
            + "WEB_FALLBACK_ALLOWED: YES\n"
            + "The named book is confirmed in Pastor's Resources, but its contents are not searchable in Pij's local index. "
              "If the user asks for information about this book, external web information may be used only for this exact title. "
              "Clearly warn that external information may differ from the library edition and should be checked against the actual ebook. "
              "Do not invent page numbers or quotations from the library copy."
        )

    explicit_count = _requested_book_count(question, default=0)
    desired_books = explicit_count or (3 if mode == "sermon_retrieval" else 4)

    db = _db()
    try:
        # Exact catalog identity wins completely. Do not first rank the whole
        # library and then hope to replace the result.
        if catalog_book is not None and bool(catalog_book["indexed"]):
            exact_book_id = int(catalog_book["book_id"])
            terms = _query_terms(question)
            title_words = set(_word_tokens(catalog_book["title"]))

            topic_terms = [
                t for t in terms
                if not any(v in title_words for v in _term_variants(t))
                and t not in _GENERIC_BOOK_INFO_TERMS
                and t not in {"book", "ebook", "resource"}
            ]

            exact_rows = []
            if topic_terms:
                exact_rows = _search_chunks(
                    db,
                    topic_terms,
                    limit=40,
                    book_id=exact_book_id,
                    strict=False,
                )

            # A generic exact-book request should read the book's opening
            # material/TOC, not search unrelated books for words like "info".
            if not exact_rows:
                exact_rows = _opening_chunks_for_book(
                    db,
                    exact_book_id,
                    limit=6,
                )

            ranked_books = []
            if exact_rows:
                ranked_books = [{
                    "book_id": exact_book_id,
                    "title": catalog_book["title"],
                    "author": catalog_book["author"],
                    "category": catalog_book["category"],
                    "folder_path": catalog_book["folder_path"],
                    "score": 100.0,
                    "chunks": list(exact_rows)[:6],
                }]
            effective_mode = "specific_book"

        else:
            ranked_books, effective_mode = _rank_books(
                db,
                question,
                mode,
                desired_books,
            )
    finally:
        db.close()

    # Private created sermons are a separate collection. Use them only when the
    # user is actually asking for existing sermon/illustration material.
    private_rows = []
    if mode == "sermon_retrieval":
        private_rows = _private_sermon_search(question, max(2, desired_books // 2))

    if not ranked_books and not private_rows:
        return (
            "PASTOR'S RESOURCES AI RETRIEVAL\n"
            f"- Retrieval mode: {mode}.\n"
            "- No matching indexed library passage was found.\n"
            "- Do not invent a library title, page, sermon, illustration, or claim of availability."
        )

    if effective_mode == "specific_book":
        instruction = (
            "The user appears to be asking about a specific book. Stay focused on BOOK 1 unless the user asks for comparison."
        )
    elif mode == "recommendation":
        instruction = (
            f"This is a broad book recommendation request. If enough evidence is present, recommend about {desired_books} DISTINCT books, "
            "not several passages from one book. Explain briefly why each book fits the topic."
        )
    elif mode == "sermon_retrieval":
        instruction = (
            "This is retrieval of EXISTING sermon/illustration material, which is allowed. Find and summarize actual retrieved material; "
            "do not refuse merely because the source is a sermon. Do not create a new sermon if the user asked only to find an existing one."
        )
    else:
        instruction = (
            "This is a topic synthesis request. Compare several DISTINCT relevant books when available and synthesize recurring principles/themes. "
            "Do not let one incidental passage dominate a broad topic."
        )

    parts = [
        "PASTOR'S RESOURCES AI RETRIEVAL",
        f"RETRIEVAL MODE: {effective_mode.upper()}",
        instruction,
        "Reason and synthesize from the evidence below; you are not required to repeat its wording.",
        "For claims about what a named library source contains, stay grounded in its supplied passage(s).",
        "When an APPROVED LINK is supplied, use that exact relative URL as a Markdown link when naming/citing that source.",
        "Never invent, alter, or guess a book/page URL.",
    ]

    used = 0
    selected_books = ranked_books[: (1 if effective_mode == "specific_book" else desired_books)]
    max_chunks_per_book = 3 if effective_mode == "specific_book" else (2 if mode == "synthesis" else 1)

    for index, book in enumerate(selected_books, start=1):
        header = (
            f"\nBOOK {index}: {book['title']}"
            + (f" — {book['author']}" if book.get("author") else "")
            + (f"\nCATEGORY: {book.get('category') or ''}" if book.get("category") else "")
            + (f"\nFOLDER: {book.get('folder_path') or ''}" if book.get("folder_path") else "")
            + "\n"
        )
        if used + len(header) > MAX_CONTEXT_CHARS:
            break
        parts.append(header)
        used += len(header)

        for evidence in _format_public_evidence(book, max_chunks_per_book):
            if used + len(evidence) > MAX_CONTEXT_CHARS:
                break
            parts.append(evidence)
            used += len(evidence)

    if private_rows:
        parts.append(
            "\nPRIVATE CREATED-SERMON MATERIAL BELOW IS AUTHORIZED FOR THIS LOGGED-IN ACCOUNT ONLY. "
            "Treat it as a separate private collection from public Pastor's Resources."
        )
        for row in private_rows:
            content = _clean(row["page_text"])
            link = _private_sermon_reader_link(row)
            block = (
                f"\n[PRIVATE EXISTING SERMON] {row['title']} — PDF page {row['page_number']}\n"
                + (f"THEME: {row['theme']}\n" if row["theme"] else "")
                + (f"APPROVED LINK: {link}\n" if link else "")
                + f"PASSAGE: {content}\n"
            )
            if used + len(block) > MAX_CONTEXT_CHARS:
                break
            parts.append(block)
            used += len(block)

    return "\n".join(parts)


def register_pij_library_routes(app):
    ensure_ai_library_tables()

    @app.route("/ai/library-index/status", methods=["GET"])
    def pij_library_index_status():
        if not sermon_ebooks.is_sermon_admin():
            return jsonify(ok=False, error="Forbidden"), 403
        return jsonify(ok=True, **get_index_state())

    @app.route("/ai/library-index/start", methods=["POST"])
    def pij_library_index_start():
        if not sermon_ebooks.is_sermon_admin():
            return jsonify(ok=False, error="Forbidden"), 403
        data = __import__("flask").request.get_json(silent=True) or {}
        started = start_public_library_index(force=bool(data.get("force", False)))
        return jsonify(ok=True, started=started, state=get_index_state())


# =========================================================
# GEMINI-CONTROLLED READ-ONLY LIBRARY TOOLS
# =========================================================
# Gemini decides which searches to perform. Flask remains the security boundary
# and exposes only read-only Pastor's Resources operations.

PIJ_LIBRARY_TOOLS = [
    {
        "type": "function",
        "name": "find_library_books",
        "description": (
            "Search the Pastor's Resources catalog by title, author, category, or folder. "
            "Use this first when the user names a book or asks whether a book is available. "
            "Results explicitly say whether each catalog book has searchable indexed text."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Book title, author, or catalog search phrase."},
                "limit": {"type": "integer", "description": "Maximum results, 1 to 12."},
            },
            "required": ["query"],
        },
    },
    {
        "type": "function",
        "name": "search_library_index",
        "description": (
            "Search actual indexed ebook text in Pastor's Resources. For a named book, first call "
            "find_library_books and then pass its exact book_id here so unrelated books cannot substitute."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Concept, topic, phrase, or information to find in ebook text."},
                "book_id": {"type": "integer", "description": "Optional exact Pastor's Resources book ID."},
                "limit": {"type": "integer", "description": "Maximum passages, 1 to 12."},
            },
            "required": ["query"],
        },
    },
    {
        "type": "function",
        "name": "get_library_book_status",
        "description": (
            "Get catalog metadata and AI-index status for one exact Pastor's Resources book ID, including "
            "known PDF page count when available from the index."
        ),
        "parameters": {
            "type": "object",
            "properties": {"book_id": {"type": "integer"}},
            "required": ["book_id"],
        },
    },
]


def _catalog_book_rows(query, limit=8):
    pastor_resources.ensure_resource_tables()
    q = _clean(query)
    limit = max(1, min(12, int(limit or 8)))
    like = f"%{q}%"
    db = _db()
    try:
        return db.execute(
            """
            SELECT b.id AS book_id, b.title, b.author, b.category, b.folder_path,
                   GROUP_CONCAT(DISTINCT UPPER(f.format)) AS formats,
                   MAX(CASE WHEN d.searchable=1 THEN 1 ELSE 0 END) AS indexed,
                   MAX(COALESCE(d.page_count,0)) AS page_count,
                   MAX(COALESCE(d.chunk_count,0)) AS chunk_count,
                   MAX(COALESCE(d.extract_error,'')) AS extract_error
            FROM pastor_library_books b
            LEFT JOIN pastor_library_files f
              ON f.book_id=b.id AND f.is_active=1 AND COALESCE(f.is_duplicate,0)=0
            LEFT JOIN pij_library_documents d
              ON d.book_id=b.id AND d.source_type='public_ebook'
            WHERE b.is_active=1 AND COALESCE(b.is_hidden,0)=0
              AND (b.title LIKE ? OR b.author LIKE ? OR b.category LIKE ? OR b.folder_path LIKE ?)
            GROUP BY b.id
            ORDER BY CASE WHEN LOWER(b.title)=LOWER(?) THEN 0
                          WHEN LOWER(b.title) LIKE LOWER(?) THEN 1 ELSE 2 END,
                     LOWER(b.title)
            LIMIT ?
            """,
            (like, like, like, like, q, q + "%", limit),
        ).fetchall()
    finally:
        db.close()


def find_library_books(query, limit=8):
    rows = _catalog_book_rows(query, limit)
    books = []
    for row in rows:
        book_id = int(row["book_id"])
        books.append({
            "book_id": book_id,
            "title": _clean(row["title"]),
            "author": _clean(row["author"]),
            "category": _clean(row["category"]),
            "folder_path": _clean(row["folder_path"]),
            "formats": [x for x in _clean(row["formats"]).split(",") if x],
            "indexed": bool(row["indexed"]),
            "page_count": int(row["page_count"] or 0),
            "chunk_count": int(row["chunk_count"] or 0),
            "index_error": _clean(row["extract_error"]),
            "approved_library_link": "/pastor-resources",
        })
    return {"ok": True, "query": _clean(query), "count": len(books), "books": books}


def get_library_book_status(book_id):
    db = _db()
    try:
        row = db.execute(
            """
            SELECT b.id AS book_id,b.title,b.author,b.category,b.folder_path,
                   GROUP_CONCAT(DISTINCT UPPER(f.format)) AS formats,
                   MAX(CASE WHEN d.searchable=1 THEN 1 ELSE 0 END) AS indexed,
                   MAX(COALESCE(d.page_count,0)) AS page_count,
                   SUM(COALESCE(d.chunk_count,0)) AS chunk_count,
                   MAX(COALESCE(d.extract_error,'')) AS extract_error
            FROM pastor_library_books b
            LEFT JOIN pastor_library_files f
              ON f.book_id=b.id AND f.is_active=1 AND COALESCE(f.is_duplicate,0)=0
            LEFT JOIN pij_library_documents d
              ON d.book_id=b.id AND d.source_type='public_ebook'
            WHERE b.id=? AND b.is_active=1 AND COALESCE(b.is_hidden,0)=0
            GROUP BY b.id
            """,
            (int(book_id),),
        ).fetchone()
    finally:
        db.close()
    if not row:
        return {"ok": False, "found": False, "book_id": int(book_id)}
    return {
        "ok": True, "found": True, "book_id": int(row["book_id"]),
        "title": _clean(row["title"]), "author": _clean(row["author"]),
        "category": _clean(row["category"]), "folder_path": _clean(row["folder_path"]),
        "formats": [x for x in _clean(row["formats"]).split(",") if x],
        "indexed": bool(row["indexed"]), "page_count": int(row["page_count"] or 0),
        "chunk_count": int(row["chunk_count"] or 0), "index_error": _clean(row["extract_error"]),
        "approved_library_link": "/pastor-resources",
    }


def search_library_index(query, book_id=None, limit=8):
    ensure_ai_library_tables()
    terms = _query_terms(query)
    if not terms:
        terms = [w for w in _word_tokens(query) if len(w) >= 2][:8]
    limit = max(1, min(12, int(limit or 8)))
    db = _db()
    try:
        rows = _search_chunks(db, terms, limit=max(limit * 4, 20), book_id=book_id, strict=True)
        if not rows:
            rows = _search_chunks(db, terms, limit=max(limit * 4, 20), book_id=book_id, strict=False)
    finally:
        db.close()
    results = []
    seen = set()
    for row in rows:
        key = (int(row["document_id"]), int(row["chunk_number"]))
        if key in seen:
            continue
        seen.add(key)
        fmt = _clean(row["format"]).upper()
        location = int(row["page_start"] or row["chunk_number"] or 1)
        link = _public_reader_link(row)
        results.append({
            "book_id": int(row["book_id"]), "title": _clean(row["title"]),
            "author": _clean(row["author"]), "format": fmt,
            "location_type": "PDF page" if fmt == "PDF" else "EPUB section",
            "location": location, "approved_link": link,
            "passage": _clean(row["content"])[:6500],
        })
        if len(results) >= limit:
            break
    return {"ok": True, "query": _clean(query), "book_id": int(book_id) if book_id is not None else None,
            "count": len(results), "results": results}


def execute_pij_library_tool(name, arguments=None):
    args = dict(arguments or {})
    if name == "find_library_books":
        return find_library_books(args.get("query", ""), args.get("limit", 8))
    if name == "get_library_book_status":
        return get_library_book_status(args.get("book_id"))
    if name == "search_library_index":
        return search_library_index(args.get("query", ""), args.get("book_id"), args.get("limit", 8))
    return {"ok": False, "error": "Unknown or unauthorized library tool."}
