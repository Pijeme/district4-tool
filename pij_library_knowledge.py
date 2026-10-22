"""Pij AI retrieval layer for Pastor's Resources.

Public Pastor's Resources ebooks are searchable by every logged-in Pij user.
The separate Sermon eBooks collection is private and is included only when
sermon_ebooks.is_sermon_admin() authorizes the current account.

This module deliberately keeps authorization in Flask/Python. Private sermon
text is never sent to Gemini for an unauthorized user.
"""

import html
import hashlib
import io
import json
import os
import re
import sqlite3
import threading
import time
import zipfile
from datetime import datetime, timezone
from xml.etree import ElementTree as ET
from urllib.parse import urlsplit, parse_qsl

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
    "newly_indexed": 0,
    "reindexed": 0,
    "skipped": 0,
    "errors": 0,
    "current_file": "",
    "started_at": "",
    "finished_at": "",
    "last_error": "",
    "queued": False,
    "queued_force": False,
}

# Same-process fast signal. The authoritative cross-worker cancel request is
# stored by pastor_resources in pastor_resource_sync_state.db.
INDEX_CANCEL_EVENT = threading.Event()


def request_public_library_index_cancel():
    """Request a cooperative stop without deleting any completed AI chunks."""
    INDEX_CANCEL_EVENT.set()
    with INDEX_STATE_LOCK:
        INDEX_STATE["queued"] = False
        INDEX_STATE["queued_force"] = False
        if INDEX_STATE.get("running"):
            INDEX_STATE["stage"] = "cancelling"
            INDEX_STATE["message"] = (
                "Cancellation requested. Stopping after the current ebook step..."
            )
    return True


def _public_library_index_cancel_requested():
    if INDEX_CANCEL_EVENT.is_set():
        return True
    try:
        return bool(pastor_resources.resource_sync_cancel_requested())
    except Exception:
        return False


MAX_FILE_BYTES = int(os.getenv("PIJ_LIBRARY_MAX_FILE_MB", "300")) * 1024 * 1024
CHUNK_TARGET_CHARS = int(os.getenv("PIJ_LIBRARY_CHUNK_CHARS", "4200"))
CHUNK_OVERLAP_CHARS = int(os.getenv("PIJ_LIBRARY_CHUNK_OVERLAP", "500"))
MAX_RESULTS = int(os.getenv("PIJ_LIBRARY_MAX_RESULTS", "6"))
MAX_CONTEXT_CHARS = int(os.getenv("PIJ_LIBRARY_MAX_CONTEXT_CHARS", "18000"))
MAX_RECOMMENDED_BOOKS = 10


class LibraryContext(str):
    """Prompt text plus server-owned sources kept outside untrusted ebook text."""
    def __new__(cls, text, sources=()):
        instance = super().__new__(cls, text)
        instance.approved_sources = tuple(dict(source) for source in sources)
        return instance


SQLITE_BUSY_TIMEOUT_MS = int(os.getenv("PIJ_LIBRARY_SQLITE_BUSY_TIMEOUT_MS", "60000"))
SQLITE_LOCK_RETRIES = int(os.getenv("PIJ_LIBRARY_SQLITE_LOCK_RETRIES", "8"))


def _appmod():
    import app
    return app


def _db(read_only=False):
    # Each Flask request/background worker gets its own SQLite connection.
    # A generous busy timeout lets short writes finish instead of immediately
    # failing while the ebook indexer is inserting many chunks.
    timeout_seconds = max(1.0, SQLITE_BUSY_TIMEOUT_MS / 1000.0)
    appmod = _appmod()
    if read_only:
        from pathlib import Path
        db = sqlite3.connect(Path(appmod.AI_DATABASE).resolve().as_uri()+"?mode=ro", uri=True, timeout=2)
    else:
        db = sqlite3.connect(appmod.AI_DATABASE, timeout=timeout_seconds, check_same_thread=False)
    try:
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys = ON")
        db.execute(f"PRAGMA busy_timeout = {2000 if read_only else SQLITE_BUSY_TIMEOUT_MS}")

        # The AI index now lives in ai_index.db, while the Pastor Resources
        # catalog remains in app_v2.db. Attach the main application database
        # read/write so existing catalog lookups can join against it without
        # duplicating normal website data into the AI database.
        main_db_path = str(appmod.DATABASE).replace("'", "''")
        if read_only:
            db.execute("ATTACH DATABASE ? AS appdb", (Path(appmod.DATABASE).resolve().as_uri()+"?mode=ro",))
        else:
            db.execute(f"ATTACH DATABASE '{main_db_path}' AS appdb")
        # Filter cached document joins as well as future index candidates.
        blocked = ','.join("'" + value.replace("'", "''") + "'" for value in pastor_resources.AI_EXCLUDED_DRIVE_IDS)
        predicate = " AND ".join("LOWER(COALESCE({text},'')) NOT LIKE '" + pattern + "'" for pattern in (
            '%cebocb%', '%open%cebuano%contemporary%bible%', '%open%tagalog%contemporary%bible%',
            '%open%ang%pulong%sa%dios%', '%open%ang%salita%ng%diyos%'))
        db.execute("CREATE TEMP VIEW ai_allowed_library_files AS SELECT * FROM appdb.pastor_library_files "
                   f"WHERE drive_file_id NOT IN ({blocked}) AND " + predicate.format(text="name||' '||COALESCE(folder_path,'')"))
        db.execute("CREATE TEMP VIEW ai_allowed_library_books AS SELECT * FROM appdb.pastor_library_books WHERE "
                   + predicate.format(text="title||' '||COALESCE(folder_path,'')")
                   + " AND NOT EXISTS (SELECT 1 FROM appdb.pastor_library_files blocked_file WHERE blocked_file.book_id=appdb.pastor_library_books.id"
                     " AND blocked_file.id NOT IN (SELECT id FROM ai_allowed_library_files))")
        if read_only:
            db.execute("PRAGMA query_only=ON")
            deadline = time.monotonic() + 6
            db.set_progress_handler(lambda: int(time.monotonic() > deadline), 10000)
        return db
    except Exception:
        db.close()
        raise



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
        db.execute("CREATE INDEX IF NOT EXISTS idx_pij_library_doc_drive ON pij_library_documents(source_type, drive_file_id)")
        # Legacy duplicates, if present, must keep their text. Enforce stable
        # identity when possible without deleting them to make an index fit.
        duplicates = db.execute("""SELECT 1 FROM pij_library_documents
            WHERE COALESCE(drive_file_id,'')<>'' GROUP BY source_type,drive_file_id
            HAVING COUNT(*)>1 LIMIT 1""").fetchone()
        if not duplicates:
            db.execute("""CREATE UNIQUE INDEX IF NOT EXISTS idx_pij_library_doc_drive_unique
                ON pij_library_documents(source_type,drive_file_id)
                WHERE drive_file_id IS NOT NULL AND drive_file_id<>''""")
        db.execute("CREATE INDEX IF NOT EXISTS idx_pij_library_chunk_doc ON pij_library_chunks(document_id, chunk_number)")
        db.execute('''CREATE TABLE IF NOT EXISTS pij_library_source_formats (
            document_id INTEGER PRIMARY KEY REFERENCES pij_library_documents(id) ON DELETE CASCADE,
            checksum TEXT NOT NULL, modified_time TEXT NOT NULL, version INTEGER NOT NULL,
            sections_json TEXT NOT NULL)''')
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
        db.execute("BEGIN IMMEDIATE")
        _reconcile_document_references(db)
        db.commit()
    finally:
        db.close()


def ensure_ai_library_tables():
    return _retry_locked(_ensure_ai_library_tables_once, label="Preparing AI library tables")


def _find_public_document(db, drive_file_id):
    return db.execute("""SELECT * FROM pij_library_documents
        WHERE source_type='public_ebook' AND drive_file_id=?
        ORDER BY searchable DESC, chunk_count DESC, id LIMIT 1""",
        (_clean(drive_file_id),)).fetchone()


def _reconcile_document_references(db):
    """Rebind local references atomically; never modify chunks or fingerprints.

    Negative file references reserve unmatched/legacy records under the old
    UNIQUE(source_type,source_file_id) constraint. Two phases also handle swaps.
    """
    if not db.execute("SELECT 1 FROM appdb.sqlite_master WHERE name='pastor_library_files'").fetchone():
        return 0
    catalog_ids = {r[0] for r in db.execute("SELECT id FROM ai_allowed_library_files")}
    rows = db.execute("""SELECT d.id,d.source_file_id,d.book_id,d.drive_file_id,
            f.id AS local_file_id,f.book_id AS local_book_id
        FROM pij_library_documents d LEFT JOIN ai_allowed_library_files f
          ON f.drive_file_id=d.drive_file_id AND COALESCE(d.drive_file_id,'')<>''
        WHERE d.source_type='public_ebook'
        ORDER BY d.searchable DESC,d.chunk_count DESC,d.id""").fetchall()
    owners = set()
    plans = []
    for row in rows:
        local_id = row['local_file_id']
        if local_id is not None and local_id not in owners:
            owners.add(local_id)
            plans.append((row, local_id, row['local_book_id']))
        else:
            plans.append((row, None, None))
    next_reserved = min([0] + [int(r['source_file_id']) for r in rows]) - 1
    updates = []
    for row, local_id, book_id in plans:
        target = local_id
        if target is None:
            target = int(row['source_file_id'])
            if target in catalog_ids:
                target = next_reserved
                next_reserved -= 1
        if target != row['source_file_id'] or book_id != row['book_id']:
            updates.append((target, book_id, row['id']))
    # Release all conflicting IDs first, within the caller's transaction.
    for _, _, document_id in updates:
        db.execute("UPDATE pij_library_documents SET source_file_id=? WHERE id=?",
                   (next_reserved, document_id))
        next_reserved -= 1
    db.executemany("UPDATE pij_library_documents SET source_file_id=?,book_id=? WHERE id=?", updates)
    return len(updates)


def reconcile_public_library_references():
    """Safe repeated reconciliation after an upload or catalog synchronization."""
    def operation():
        db = _db()
        try:
            db.execute("BEGIN IMMEDIATE")
            changed = _reconcile_document_references(db)
            db.commit()
            return changed
        finally:
            db.close()
    return _retry_locked(operation, label="Reconciling AI library references")


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


def _formatted_source_title(title, author='', fmt=''):
    return bool(re.search(r'moody bible commentary|pulpit commentary|sermon resources vol|spurgeon.*sermon.*outline|shepherd.?s.staff|^52\s+(?:funeral\s+)?sermons\b|^joy$', title, re.I)
        or (title.casefold().startswith('be ') and 'wiersbe' in author.casefold() and fmt.casefold()=='epub'))


def _formatted_source_sections(data, fmt):
    """Keep a small allowlist of source typography, never executable book HTML."""
    def blocks_from_runs(runs):
        blocks, current = [], []
        def flush():
            text = ''.join(run['text'] for run in current)
            if text.strip():
                # Trim only the outside; internal spaces and Unicode offsets agree.
                left, right = len(text)-len(text.lstrip()), len(text.rstrip())
                trimmed, position = [], 0
                for run in current:
                    end = position+len(run['text'])
                    part = run['text'][max(0,left-position):max(0,min(end,right)-position)]
                    if part: trimmed.append(dict(text=part,marks=run['marks']))
                    position=end
                structure=next((run.get('structure',{}) for run in current if run['text'].strip()),{})
                blocks.append(dict(type='paragraph',text=text[left:right],runs=trimmed)|structure)
            current.clear()
        for run in runs:
            for index, part in enumerate(run['text'].replace('\r','').split('\n')):
                if index: flush()
                if re.search(r'[\ud800-\udfff]',part):
                    part=part.encode('utf-16-le','surrogatepass').decode('utf-16-le','replace')
                if part: current.append(dict(text=part,marks=run['marks'],structure=run.get('structure',{})))
        flush()
        return blocks
    if fmt.casefold() == 'pdf':
        sections=[]
        for page in PdfReader(io.BytesIO(data),strict=False).pages:
            runs=[]
            def visit(text, cm, tm, font, size):
                name=str((font or {}).get('/BaseFont','')).casefold()
                marks=[]
                if any(word in name for word in ('bold','black','demi')): marks.append('strong')
                if any(word in name for word in ('italic','oblique')): marks.append('em')
                runs.append(dict(text=text,marks=marks))
            page.extract_text(visitor_text=visit)
            sections.append(blocks_from_runs(runs))
        return sections
    sections=[]
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        container=ET.fromstring(archive.read('META-INF/container.xml'))
        opf_name=next(node.attrib['full-path'] for node in container.iter() if node.tag.split('}')[-1]=='rootfile')
        opf=ET.fromstring(archive.read(opf_name));base=os.path.dirname(opf_name)
        manifest={node.attrib['id']:node.attrib['href'] for node in opf.iter() if node.tag.split('}')[-1]=='item'}
        order=[node.attrib['idref'] for node in opf.iter() if node.tag.split('}')[-1]=='itemref']
        common_styles='\n'.join(archive.read(n).decode('utf-8','replace') for n in archive.namelist() if n.endswith('.css'))
        list_sequence=0
        for item in order:
            name=os.path.normpath(os.path.join(base,manifest[item])).replace('\\','/')
            markup=archive.read(name).decode('utf-8-sig','replace')
            markup=re.sub(r'&([A-Za-z][A-Za-z0-9]+);',lambda match:match[0] if match[1] in ('amp','lt','gt','quot','apos') else html.unescape(match[0]),markup)
            root=ET.fromstring(markup);runs=[]
            # Calibre/Word EPUBs frequently express emphasis through stylesheet classes.
            styles=common_styles+'\n'+'\n'.join(node.text or '' for node in root.iter() if node.tag.split('}')[-1]=='style')
            classes={}
            for selectors,body in re.findall(r'([^{}]+)\{([^{}]*)\}',styles):
                marks=[]
                if re.search(r'font-weight\s*:\s*(?:bold|[6-9]00)',body,re.I):marks.append('strong')
                if re.search(r'font-style\s*:\s*(?:italic|oblique)',body,re.I):marks.append('em')
                for selector in selectors.split(','):
                    match=re.fullmatch(r'(?:[\w-]+)?\.([\w-]+)',selector.strip())
                    if match:classes[match[1]]=marks
            def walk(node, inherited=(), structure=None, lists=()):
                nonlocal list_sequence
                tag=node.tag.split('}')[-1].lower()
                if tag in ('script','style','head','nav'):return
                structure=dict(structure or {})
                if tag in ('ul','ol'):
                    start=node.attrib.get('start','1')
                    lists=lists+({'kind':'ordered' if tag=='ol' else 'bullet',
                        'next':int(start) if re.fullmatch(r'-?\d{1,5}',start) else 1,
                        'style':node.attrib.get('type','1')},)
                if tag=='li':
                    list_sequence+=1
                    context=lists[-1] if lists else {'kind':'bullet','next':1,'style':'1'}
                    value=node.attrib.get('value','')
                    number=int(value) if re.fullmatch(r'-?\d{1,5}',value) else context['next']
                    context['next']=number+1
                    structure.update(list_kind=context['kind'],list_level=min(6,max(1,len(lists))),
                        list_item=list_sequence,list_number=number,list_style=context['style'] if context['style'] in ('1','a','A','i','I') else '1')
                if tag=='blockquote':structure['source_quote']=True
                if re.fullmatch(r'h[1-6]',tag):structure.update(type='heading',source_heading=int(tag[1]))
                for cls in node.attrib.get('class','').split():
                    if cls in ('Chapter-Title','A-head','B-head','C-head'):
                        structure['source_heading']={'Chapter-Title':1,'A-head':2,'B-head':3,'C-head':4}[cls]
                marks=list(inherited)
                for cls in node.attrib.get('class','').split(): marks.extend(classes.get(cls,()))
                inline=node.attrib.get('style','')
                if tag in ('b','strong') or re.search(r'font-weight\s*:\s*(?:bold|[6-9]00)',inline,re.I):marks.append('strong')
                if tag in ('i','em') or re.search(r'font-style\s*:\s*(?:italic|oblique)',inline,re.I):marks.append('em')
                if tag in ('sup','sub'):marks.append(tag)
                if re.fullmatch(r'h[1-6]',tag):marks.append('strong')
                marks=list(dict.fromkeys(marks))
                block=tag in ('p','div','li','blockquote','br') or bool(re.fullmatch(r'h[1-6]',tag))
                if block:runs.append(dict(text='\n',marks=[]))
                if node.text:runs.append(dict(text=node.text.replace('\n',' '),marks=marks,structure=structure))
                for child in node:
                    walk(child,marks,structure,lists)
                    if child.tail:runs.append(dict(text=child.tail.replace('\n',' '),marks=marks,structure=structure))
                if block:runs.append(dict(text='\n',marks=[]))
            walk(root)
            blocks=blocks_from_runs(runs)
            if blocks:sections.append(blocks)
    return sections


def store_source_format(document_id, data, fmt, checksum, modified_time):
    sections=_formatted_source_sections(data,fmt)
    def operation():
        db=_db()
        try:
            # Do not attach formatting to a changed or deleted index revision.
            doc=db.execute('SELECT checksum,modified_time,title FROM pij_library_documents WHERE id=?',(document_id,)).fetchone()
            if doc and (doc['checksum'],doc['modified_time'])==(checksum,modified_time):
                db.execute('INSERT OR REPLACE INTO pij_library_source_formats VALUES (?,?,?,?,?)',
                    (document_id,checksum,modified_time,2 if 'spurgeon' in doc['title'].casefold() else 1,json.dumps(sections,ensure_ascii=False)))
                db.commit()
        finally:db.close()
    _retry_locked(operation,label='Saving original source typography')
    return len(sections)


def refresh_study_source_formats(progress=print, local_directory=None):
    """Explicit maintenance for existing sources; ordinary reads never download."""
    ensure_ai_library_tables()
    db=_db()
    try:
        documents=db.execute('''SELECT d.* FROM pij_library_documents d
            JOIN ai_allowed_library_files f ON f.drive_file_id=d.drive_file_id
            JOIN ai_allowed_library_books b ON b.id=f.book_id
            LEFT JOIN pij_library_source_formats s ON s.document_id=d.id
            WHERE d.source_type='public_ebook' AND d.searchable=1 AND f.is_active=1 AND b.is_active=1
            AND COALESCE(b.is_hidden,0)=0 AND COALESCE(f.is_duplicate,0)=0
            AND (s.document_id IS NULL OR s.checksum<>d.checksum OR s.modified_time<>d.modified_time
                 OR s.version<CASE WHEN lower(d.title) LIKE '%spurgeon%' THEN 2 ELSE 1 END)
            ORDER BY CASE lower(d.format) WHEN 'epub' THEN 0 ELSE 1 END,d.id''').fetchall()
    finally:db.close()
    drive=None;results=[]
    for doc in documents:
        if not _formatted_source_title(doc['title'],doc['author'] or '',doc['format']):continue
        # Ignore generic/duplicate Pulpit copies when a numbered volume exists.
        if 'pulpit' in doc['title'].casefold() and not re.search(r'\bvolume\s+[1-9]\b',doc['title'],re.I):continue
        progress('Preserving original typography: '+doc['title'])
        data=None
        if local_directory:
            from pathlib import Path
            for candidate in Path(local_directory).rglob('*.'+doc['format'].lower()):
                title_key=re.sub(r'[^a-z0-9]','',doc['title'].split('(',1)[0].casefold())
                if not re.sub(r'[^a-z0-9]','',candidate.stem.casefold()).startswith(title_key):continue
                if doc['checksum'] and len(doc['checksum']) in (32,40,64):
                    raw=candidate.read_bytes()
                    algorithm={32:'md5',40:'sha1',64:'sha256'}[len(doc['checksum'])]
                    if hashlib.new(algorithm,raw).hexdigest()==doc['checksum']:
                        data=raw;break
        if data is None:
            if drive is None:drive=pastor_resources.get_drive_session()
            data=pastor_resources.download_drive_file_bytes(drive,doc['drive_file_id'])
        count=store_source_format(doc['id'],data,doc['format'],doc['checksum'],doc['modified_time'])
        results.append(dict(title=doc['title'],sections=count))
        progress('Saved '+str(count)+' source sections: '+doc['title'])
    return results


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
                   COALESCE(NULLIF(f.sha256_checksum,''), NULLIF(f.md5_checksum,''), NULLIF(f.sha1_checksum,''), '') AS checksum,
                   f.sha256_checksum, f.md5_checksum, f.sha1_checksum,
                   b.title, b.author, b.category, b.folder_path
            FROM ai_allowed_library_files f
            JOIN ai_allowed_library_books b ON b.id = f.book_id
            WHERE f.is_active = 1 AND b.is_active = 1
              AND COALESCE(b.is_hidden,0) = 0
              AND COALESCE(f.is_duplicate,0) = 0
              AND LOWER(COALESCE(f.format,'')) IN ('pdf','epub')
            ORDER BY b.title, f.id
            """
        ).fetchall()
    finally:
        db.close()


def _document_needs_reindex(old, row):
    if not old or not old['searchable'] or int(old['chunk_count'] or 0) <= 0:
        return True
    if _clean(old['extract_error']):
        return True
    old_checksum = _clean(old['checksum']).lower()
    # Compare the same hash algorithm, including indexes created when Drive
    # exposed MD5 but not SHA-256. Missing hashes alone are not content changes.
    candidates = [_clean(row[key]).lower() for key in
                  ('sha256_checksum', 'md5_checksum', 'sha1_checksum') if key in row.keys()]
    candidates.append(_clean(row['checksum']).lower())
    comparable = next((value for value in candidates
                       if value and old_checksum and len(value) == len(old_checksum)), '')
    if comparable:
        return comparable != old_checksum
    old_time, new_time = _clean(old['modified_time']), _clean(row['modified_time'])
    return not (old_time and new_time and old_time == new_time)


def _needs_reindex(row):
    """Drive identity and content fingerprints determine incremental work."""
    db = _db()
    try:
        return _document_needs_reindex(_find_public_document(db, row['drive_file_id']), row)
    finally:
        db.close()


def _store_public_document_once(row, chunks, page_count, error=""):
    db = _db()
    try:
        db.execute("BEGIN IMMEDIATE")
        _reconcile_document_references(db)
        existing = _find_public_document(db, row['drive_file_id'])
        if not chunks and existing and existing['searchable'] and existing['chunk_count'] > 0:
            # A transient download/extraction failure must not erase the last
            # successful index or advance its content fingerprint.
            db.commit()
            return
        values = (
            row["drive_file_id"], row["book_id"], _clean(row["title"] or row["name"]), _clean(row["author"]),
            _clean(row["category"]), _clean(row["folder_path"]), _clean(row["format"]), _clean(row["modified_time"]),
            _clean(row["checksum"]), int(page_count), len(chunks), 1 if chunks else 0, _clean(error)[:4000], _now(),
        )
        if existing:
            document_id = int(existing["id"])
            db.execute(
                """UPDATE pij_library_documents SET source_file_id=?,drive_file_id=?,book_id=?,title=?,author=?,category=?,folder_path=?,format=?,modified_time=?,checksum=?,page_count=?,chunk_count=?,searchable=?,extract_error=?,indexed_at=? WHERE id=?""",
                (int(row['source_file_id']),) + values + (document_id,),
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
    if pastor_resources.ai_restricted_source(row):
        raise ValueError('This Bible source is restricted to Bible Study and cannot enter the AI index.')
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
    """Index new/changed public ebooks and stop safely when cancellation is requested."""
    ensure_ai_library_tables()
    if not INDEX_LOCK.acquire(blocking=False):
        return {"ok": False, "error": "An AI library indexing job is already running."}

    try:
        rows = _public_files_to_index()
        with INDEX_STATE_LOCK:
            INDEX_STATE.update(
                running=True,
                stage="indexing",
                message="Checking existing Drive IDs and content fingerprints...",
                total=len(rows),
                processed=0,
                indexed=0,
                newly_indexed=0,
                reindexed=0,
                skipped=0,
                errors=0,
                current_file="",
                started_at=_now(),
                finished_at="",
                last_error="",
            )

        # One metadata snapshot: unchanged books can be skipped without opening
        # a new DB connection or Google Drive session for every ebook.
        db = _db()
        try:
            documents = {}
            for doc in db.execute(
                """
                SELECT *
                FROM pij_library_documents
                WHERE source_type='public_ebook'
                ORDER BY searchable DESC, chunk_count DESC, id
                """
            ):
                documents.setdefault(doc["drive_file_id"], doc)
        finally:
            db.close()

        drive = None
        indexed = newly_indexed = reindexed = skipped = errors = 0

        for number, row in enumerate(rows, start=1):
            if _public_library_index_cancel_requested():
                with INDEX_STATE_LOCK:
                    INDEX_STATE.update(
                        running=False,
                        stage="cancelled",
                        message="Pij AI indexing cancelled safely.",
                        current_file="",
                        finished_at=_now(),
                        queued=False,
                        queued_force=False,
                    )
                try:
                    pastor_resources.clear_resource_sync_cancel_request()
                except Exception:
                    pass
                return {
                    "ok": True,
                    "cancelled": True,
                    "total": len(rows),
                    "checked": number - 1,
                    "indexed": indexed,
                    "newly_indexed": newly_indexed,
                    "reindexed": reindexed,
                    "skipped": skipped,
                    "errors": errors,
                }

            with INDEX_STATE_LOCK:
                INDEX_STATE.update(
                    processed=number - 1,
                    current_file=_clean(row["name"]),
                )

            old = documents.get(row["drive_file_id"])
            if not force and not _document_needs_reindex(old, row):
                skipped += 1
                with INDEX_STATE_LOCK:
                    INDEX_STATE.update(
                        processed=number,
                        skipped=skipped,
                    )
                continue

            try:
                if int(row["size"] or 0) > MAX_FILE_BYTES:
                    raise ValueError(
                        f"File exceeds AI indexing limit of "
                        f"{MAX_FILE_BYTES // (1024*1024)} MB"
                    )

                if drive is None:
                    drive = pastor_resources.get_drive_session()

                if _public_library_index_cancel_requested():
                    continue

                data = pastor_resources.download_drive_file_bytes(
                    drive,
                    row["drive_file_id"],
                )

                # A blocking Drive download cannot be interrupted mid-request,
                # but cancellation is honored before CPU-heavy extraction begins.
                if _public_library_index_cancel_requested():
                    with INDEX_STATE_LOCK:
                        INDEX_STATE.update(processed=number - 1)
                    continue

                fmt = _clean(row["format"]).lower()
                sections = _extract_pdf(data) if fmt == "pdf" else _extract_epub(data)

                if _public_library_index_cancel_requested():
                    with INDEX_STATE_LOCK:
                        INDEX_STATE.update(processed=number - 1)
                    continue

                chunks = _make_chunks(sections)
                if not chunks:
                    raise ValueError("No searchable text extracted from ebook")

                if _public_library_index_cancel_requested():
                    with INDEX_STATE_LOCK:
                        INDEX_STATE.update(processed=number - 1)
                    continue

                _store_public_document(row, chunks, len(sections))
                if _formatted_source_title(row['title'],row['author'] or '',fmt) and not _public_library_index_cancel_requested():
                    docdb=_db()
                    try: document=_find_public_document(docdb,row['drive_file_id'])
                    finally: docdb.close()
                    if document:
                        store_source_format(document['id'],data,fmt,document['checksum'],document['modified_time'])
                indexed += 1
                if old and old["searchable"] and int(old["chunk_count"] or 0) > 0:
                    reindexed += 1
                else:
                    newly_indexed += 1

            except Exception as exc:
                # Do not turn a deliberate cancellation into an index error.
                if _public_library_index_cancel_requested():
                    continue
                errors += 1
                _store_index_error(row, exc)
                with INDEX_STATE_LOCK:
                    INDEX_STATE["last_error"] = (
                        f"{_clean(row['name'])}: {_clean(exc)}"
                    )

            with INDEX_STATE_LOCK:
                INDEX_STATE.update(
                    processed=number,
                    indexed=indexed,
                    newly_indexed=newly_indexed,
                    reindexed=reindexed,
                    skipped=skipped,
                    errors=errors,
                )

        # A cancellation may have been requested during the final ebook.
        if _public_library_index_cancel_requested():
            with INDEX_STATE_LOCK:
                INDEX_STATE.update(
                    running=False,
                    stage="cancelled",
                    message="Pij AI indexing cancelled safely.",
                    current_file="",
                    finished_at=_now(),
                    queued=False,
                    queued_force=False,
                )
            try:
                pastor_resources.clear_resource_sync_cancel_request()
            except Exception:
                pass
            return {
                "ok": True,
                "cancelled": True,
                "total": len(rows),
                "checked": int(INDEX_STATE.get("processed") or 0),
                "indexed": indexed,
                "newly_indexed": newly_indexed,
                "reindexed": reindexed,
                "skipped": skipped,
                "errors": errors,
            }

        with INDEX_STATE_LOCK:
            INDEX_STATE.update(
                running=False,
                stage="complete",
                message="AI library indexing complete.",
                current_file="",
                finished_at=_now(),
            )

        return {
            "ok": True,
            "total": len(rows),
            "checked": len(rows),
            "indexed": indexed,
            "newly_indexed": newly_indexed,
            "reindexed": reindexed,
            "skipped": skipped,
            "errors": errors,
        }

    finally:
        with INDEX_STATE_LOCK:
            INDEX_STATE["running"] = False
        INDEX_LOCK.release()


def start_public_library_index(force=False, queue_if_running=True):
    """
    Start one background Pastor's Resources AI indexing pass.

    If a pass is already running, queue at most one additional incremental
    pass. A developer cancellation clears that queue and stops safely between
    ebook operations.
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

        # A new explicit/triggered run may proceed after an earlier cancellation.
        INDEX_CANCEL_EVENT.clear()
        INDEX_STATE.update(
            running=True,
            stage="starting",
            message="Starting AI library index...",
            started_at=_now(),
            finished_at="",
            queued=False,
            queued_force=False,
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
            cancelled = _public_library_index_cancel_requested()

            with INDEX_STATE_LOCK:
                if not cancelled and INDEX_STATE.get("stage") != "cancelled":
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
            else:
                INDEX_CANCEL_EVENT.clear()

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
            JOIN ai_allowed_library_files pf ON pf.drive_file_id=d.drive_file_id
              AND pf.is_active=1 AND COALESCE(pf.is_duplicate,0)=0
            JOIN ai_allowed_library_books b ON b.id=pf.book_id
            WHERE d.source_type='public_ebook'
              AND d.searchable=1 AND d.chunk_count>0
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
    "list", "maximum", "max",
}

_GENERIC_THEOLOGY_TERMS = {
    "god", "lord", "jesus", "christ", "bible", "biblical", "scripture", "scriptures",
    "christian", "christians", "church", "pastor", "ministry", "faith",
}

_LIBRARY_CUES = {
    "book", "books", "ebook", "ebooks", "library", "resource", "resources", "author",
    "chapter", "page", "pages", "pastor's resources", "pastors resources",
    "sermon", "sermons", "illustration", "illustrations", "commentary", "commentaries",
    "study bible", "study-bible", "devotional",
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


def _term_in_text(term, normalized_text):
    return bool(re.search(r'\b' + re.escape(term) + r'\b', normalized_text))


def _broad_book_request(question):
    """Topic recommendations must not become a fuzzy match to one book title."""
    return (_library_mode(question) == 'recommendation'
            and not _extract_title_hint(question)
            and bool(re.search(r'\b(?:books?|ebooks?|resources?|bibles?|commentaries|devotionals?)\b', question, re.I)))


def _query_terms(question):
    # Requested quantities are instructions, not search topics ("ten books"
    # should search giving, not passages that happen to mention the number ten).
    question = re.sub(
        r"\b(?:(?:up\s+to|at\s+most)\s+)?(?:\d+|one|two|three|four|five|six|seven|eight|nine|ten)"
        r"\s+(?:(?:study|devotional|commentary|children'?s?|ministry)\s+)*"
        r"(?:books?|ebooks?|resources?|bibles?|commentaries|devotionals)\b",
        "books", _clean(question), flags=re.I)
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
        "others": ["another", "neighbor", "neighbour"],
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
        or _requested_book_count(question, default=0)
        or (words & {"list", "show", "find"} and _requested_resource_type(question))
        or re.search(r"\b\d+\s+(?:books?|ebooks?|resources?)\b", low)
        or re.search(r"\b(?:a|one)\s+book\b", low)
    ):
        return "recommendation"

    if "pastor's resources" in low or "pastors resources" in low:
        return "synthesis"

    return "study"


def _requested_book_count(question, default=MAX_RECOMMENDED_BOOKS):
    low = _clean(question).lower()
    word_numbers = dict(zip(
        ("one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten"),
        range(1, 11),
    ))
    numbers = "|".join(word_numbers)
    m = re.search(
        rf"\b(\d+|{numbers}|a|an)\s+"
        r"(?:(?:study|devotional|commentary|children'?s?|ministry)\s+)*"
        r"(?:books?|ebooks?|resources?|bibles?|commentaries|devotionals)\b", low)
    if m:
        token = m.group(1)
        count = int(token) if token.isdigit() else word_numbers.get(token, 1)
        return max(1, min(MAX_RECOMMENDED_BOOKS, count))
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
        FROM ai_allowed_library_books b
        JOIN ai_allowed_library_files pf ON pf.book_id=b.id
          AND pf.is_active=1 AND COALESCE(pf.is_duplicate,0)=0
        JOIN pij_library_documents d ON d.drive_file_id=pf.drive_file_id
        WHERE d.source_type='public_ebook'
          AND d.searchable=1 AND d.chunk_count>0
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
        if any(_term_in_text(v, title) for v in variants):
            score += title_weight
        if any(_term_in_text(v, category) for v in variants):
            score += category_weight
        if any(_term_in_text(v, folder) for v in variants):
            score += folder_weight
        if any(_term_in_text(v, author) for v in variants):
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
    if _broad_book_request(question):
        return None
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
            overlap = sum(1 for t in distinctive if any(_term_in_text(v, title_norm) for v in _term_variants(t)))
            if overlap >= 2:
                score += overlap * 7.0

        if score > best_score:
            best = row
            best_score = score

    # A hint can be somewhat fuzzy; without an explicit hint require stronger evidence.
    threshold = 18.0 if hint else 16.0
    return best if best is not None and best_score >= threshold else None


def _resource_priority(book):
    """Use catalog metadata; study Bibles remain secondary even in devotional folders."""
    metadata = _normalize_words(" ".join(str(book[key] or "") for key in
                                         ("title", "category", "folder_path")))
    if re.search(r"\bstudy bibles?\b", metadata):
        return 2
    if re.search(r"\b(?:commentar\w*|devotion\w*)\b", metadata):
        return 0
    return 1


def _requested_resource_type(question):
    low = _normalize_words(question)
    if re.search(r"\bstudy bibles?\b", low):
        return "study_bible"
    if re.search(r"\b(?:children|childrens|child|kids|kid|sunday school)\b", low):
        return "children_ministry"
    if re.search(r"\bdevotionals?\b", low):
        return "devotional"
    if re.search(r"\bcommentar(?:y|ies)\b", low):
        return "commentary"
    return ""


def _matches_resource_type(book, kind):
    title = _normalize_words(book['title'])
    location = _normalize_words(str(book['category'] or '') + ' ' + str(book['folder_path'] or ''))
    if kind == "study_bible":
        # Classification uses both title and location: category folders can
        # contain unrelated books and commentary series with misleading names.
        if title == 'daily study bible':  # Barclay's commentary series.
            return False
        if re.search(r"\bcommentar\w*\b", title):
            return False
        if re.search(r"\b(?:study bible|chain reference bible)\b", title):
            return True
        if re.search(r"\b(?:handbook|guide|lessons)\b", title):
            return False
        return bool(
            re.search(r"\bstudy bibles?\b", location) and re.search(r"\bbible\b", title))
    if kind == "children_ministry":
        return bool(re.search(r"\b(?:children|childrens|chilldren|kids|sunday school)\b", title + ' ' + location)
                    and not re.search(r"\bcommentar\w*\b", title))
    if kind == "devotional":
        return bool(re.search(r"\bdevotion\w*\b", title + ' ' + location)
                    and _resource_priority(book) != 2)
    if kind == "commentary":
        return _resource_priority(book) == 0 and bool(
            re.search(r"\bcommentar\w*\b", title + ' ' + location))
    return False


def _resource_topic_terms(question, kind):
    type_words = {
        'study_bible': {'study', 'bible', 'bibles'},
        'children_ministry': {'children', 'childrens', 'child', 'kids', 'kid', 'ministry', 'sunday', 'school'},
        'devotional': {'devotional', 'devotionals', 'devotions', 'devotion'},
        'commentary': {'commentary', 'commentaries'},
    }
    return [term for term in _query_terms(question) if term not in type_words.get(kind, set())]


def _catalog_opening_notes(books):
    """Bounded local front-matter reads for distinguishing study-Bible features."""
    notes = {}
    db = _db(read_only=True)
    try:
        for book in books:
            candidates = _opening_chunks_for_book(db, book['book_id'], limit=8)
            ranked = []
            for row in candidates:
                text = _clean(row['content'])
                features = re.findall(
                    r'\b(?:features?|designed|notes|readers?|introduction|amplified|suffering|recovery|'
                    r'chronological|apologetics|discipleship|illustrated|new believers?|ancient faith)\b', text, re.I)
                score = len(set(word.lower() for word in features))
                if re.search(r'\b(?:copyright|all rights reserved|isbn)\b', text, re.I):
                    score -= 4
                if score > 0 and not re.search(r'font-size|div#|<style', text):
                    ranked.append((score, text))
            if ranked:
                text = max(ranked, key=lambda item: item[0])[1]
                match = re.search(r'\b(?:features|designed|this bible|study notes|introduction)\b', text, re.I)
                start = max(0, match.start() - 120) if match else 0
                excerpt = text[start:start + 850]
                notes[int(book['book_id'])] = ('…' if start else '') + excerpt + ('…' if start + 850 < len(text) else '')
    finally:
        db.close()
    return notes


def _catalog_recommendation_context(question, kind):
    """Browse verified available books; listing a resource type needs no text match."""
    db = _db(read_only=True)
    try:
        rows = db.execute('''
            SELECT b.id AS book_id,b.title,b.author,b.category,b.folder_path,
                   pf.format
            FROM ai_allowed_library_books b
            JOIN ai_allowed_library_files pf ON pf.book_id=b.id
            WHERE b.is_active=1 AND COALESCE(b.is_hidden,0)=0
              AND pf.is_active=1 AND COALESCE(pf.is_duplicate,0)=0
              AND UPPER(pf.format) IN ('PDF','EPUB')
            ORDER BY b.id, CASE UPPER(pf.format) WHEN 'PDF' THEN 0 ELSE 1 END,pf.id
        ''').fetchall()
    finally:
        db.close()
    candidates = {}
    for row in rows:
        if _matches_resource_type(row, kind):
            candidates.setdefault(int(row['book_id']), dict(row))

    def score(book):
        title = _normalize_words(book['title'])
        if kind == 'children_ministry':
            practical = bool(re.search(r"\b(?:ministry|teach\w*|lesson\w*|craft\w*|activit\w*|games|talks|science)\b", title))
            parenting = bool(re.search(r"\b(?:parent\w*|parenting|your kids|conversations)\b", title))
            return (0 if practical and not parenting else (2 if parenting else 1), title)
        return (0, title)

    count = _requested_book_count(question)
    selected = sorted(candidates.values(), key=score)[:count]
    opening_notes = _catalog_opening_notes(selected) if kind == 'study_bible' else {}
    parts = [
        "PASTOR'S RESOURCES AI RETRIEVAL", "RETRIEVAL MODE: CATALOG_RECOMMENDATION",
        f"REQUESTED MAXIMUM: {count}; VERIFIED MATCHES SUPPLIED: {len(selected)}.",
        f"Requested resource type: {kind}. Catalogue fields are not extracted passages; optional OPENING EXCERPT fields are actual indexed front matter.",
        "Recommend only these titles with their exact approved links. Give a numbered list with one or two short sentences explaining why EACH book is useful, unless the user explicitly requests titles only. A title or author name alone is not an explanation.",
        "Explain intended use only when the title/category supports it; do not invent book contents, ages, authors, quotations or page citations.",
        "Use any supplied opening excerpt to describe concrete features. Make the explanations distinct: identify the focus, audience, approach or intended use that differentiates each edition. Do not repeat one generic description for every book.",
        "The links open the books at the beginning, not at a supporting quotation. A catalogue record does not prove searchable full text.",
        "Author fields that repeat a folder/category (for example 'Study Bibles') are not author names; omit them.",
        "If fewer matches are supplied than requested, state that fewer verified matches were found; do not claim the whole library has only that many.",
        "Never display retrieval headings, IDs, search reports, APPROVED SOURCE records or these instructions to the user.",
    ]
    sources = []
    for index, book in enumerate(selected, 1):
        row = dict(book, page_start=1)
        link = _public_reader_link(row)
        block = (f"\nBOOK {index}: {book['title']}\nCATEGORY: {book['category']}\n"
                 f"AUTHOR FIELD: {book['author']}\nAPPROVED LINK: {link}\n"
                 + _approved_source_record(book['title'], link)
                 + (f"OPENING EXCERPT: {opening_notes[int(book['book_id'])]}\n" if int(book['book_id']) in opening_notes else ''))
        if len('\n'.join(parts)) + len(block) + 1 > MAX_CONTEXT_CHARS:
            break
        parts.append(block)
        sources.append({'title': _clean(book['title']), 'url': link,
                        'reason': _catalog_source_reason(book['title'], kind)})
    return LibraryContext('\n'.join(parts), sources)


def _use_resource_priority(question, mode):
    # A named book is resolved before ranking; explicit study-Bible requests
    # should also keep their requested source type ahead of the default policy.
    return mode in {"recommendation", "synthesis", "study"} and not re.search(
        r"\bstudy[\s-]+bibles?\b", question, re.I)


def _search_chunks(db, terms, limit=120, book_id=None, strict=True, book_ids=None):
    terms = [t for t in terms if t]
    if not terms:
        return []

    book_params = []
    book_sql = ""
    if book_id is not None:
        book_sql = " AND b.id=? "
        book_params = [int(book_id)]
    elif book_ids is not None:
        book_params = list(dict.fromkeys(int(value) for value in book_ids))
        if not book_params:
            return []
        book_sql = " AND b.id IN (" + ",".join("?" for _ in book_params) + ") "

    # Global searches use FTS's native rank cursor. A book-specific probe uses
    # that book's indexed chunk range below: document_id is UNINDEXED in FTS,
    # so filtering a globally ranked cursor for one book can walk every hit.
    if book_id is None and _fts_available(db):
        expr = _fts_expression(terms, strict=strict)
        if expr:
            params = [expr, *book_params, int(limit)]
            try:
                return db.execute(
                    f"""
                    SELECT d.id AS document_id,b.id AS book_id,b.title,b.author,b.category,b.folder_path,d.format,
                           c.chunk_number,c.page_start,c.page_end,c.content,
                           f.rank AS rank
                    FROM pij_library_chunks_fts f
                    CROSS JOIN pij_library_documents d ON d.id=CAST(f.document_id AS INTEGER)
                    CROSS JOIN ai_allowed_library_files pf ON pf.drive_file_id=d.drive_file_id
                      AND pf.is_active=1 AND COALESCE(pf.is_duplicate,0)=0
                    CROSS JOIN ai_allowed_library_books b ON b.id=pf.book_id
                    CROSS JOIN pij_library_chunks c
                      ON c.document_id=d.id
                     AND c.chunk_number=CAST(f.chunk_number AS INTEGER)
                    WHERE pij_library_chunks_fts MATCH ?
                      AND d.source_type='public_ebook'
                      AND d.searchable=1 AND d.chunk_count>0
                      AND b.is_active=1
                      AND COALESCE(b.is_hidden,0)=0
                      {book_sql}
                    ORDER BY f.rank
                    LIMIT ?
                    """,
                    params,
                ).fetchall()
            except sqlite3.OperationalError as exc:
                # A cancelled/busy query must not turn into a slower full-text
                # LIKE scan. Preserve the original error and its time budget.
                if getattr(exc, "sqlite_errorcode", None) in (
                    sqlite3.SQLITE_INTERRUPT, sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED
                ) or any(value in str(exc).lower() for value in ("interrupted", "locked")):
                    raise
                pass

    # Scoped lookup, or fallback for SQLite builds without FTS5/syntax support.
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
    params = book_params + like_params + [int(limit)]
    return db.execute(
        f"""
        SELECT d.id AS document_id,b.id AS book_id,b.title,b.author,b.category,b.folder_path,d.format,
               c.chunk_number,c.page_start,c.page_end,c.content,0 AS rank
        FROM ai_allowed_library_books b
        CROSS JOIN ai_allowed_library_files pf ON pf.book_id=b.id
          AND pf.is_active=1 AND COALESCE(pf.is_duplicate,0)=0
        CROSS JOIN pij_library_documents d ON d.drive_file_id=pf.drive_file_id
        CROSS JOIN pij_library_chunks c ON c.document_id=d.id
        WHERE d.source_type='public_ebook'
          AND d.searchable=1 AND d.chunk_count>0
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

    kind = _requested_resource_type(question) if mode == 'recommendation' else ''
    specific = None if kind and not _extract_title_hint(question) else _find_specific_book(db, question, terms)
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

    books = _active_indexed_books(db)
    prioritize = _use_resource_priority(question, mode)
    if prioritize or kind:
        # Search the preferred catalog subset separately so hundreds of matching
        # study-Bible pages cannot fill the entire global candidate window.
        preferred_ids = [int(book["book_id"]) for book in books
                         if (_matches_resource_type(book, kind) if kind else _resource_priority(book) == 0)]
        preferred_rows = _search_chunks(db, terms, limit=140, strict=True, book_ids=preferred_ids)
        seen = {(r["document_id"], r["chunk_number"]) for r in rows}
        rows = list(rows) + [r for r in preferred_rows if (r["document_id"], r["chunk_number"]) not in seen]

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

        # Score the best passage, rather than adding points for each matching
        # page. Retain a few excerpts for evidence without rewarding book size.
        book_chunk_index = len(entry["chunks"])
        if book_chunk_index < 5:
            entry["chunks"].append(row)
        if book_chunk_index < 3:
            content_norm = _normalize_words(row["content"])
            strong_hits = 0
            generic_hits = 0
            for term in terms:
                hit = any(_term_in_text(v, content_norm) for v in _term_variants(term))
                if not hit:
                    continue
                if term in _GENERIC_THEOLOGY_TERMS:
                    generic_hits += 1
                else:
                    strong_hits += 1
            passage_score = strong_hits * 3.0 + generic_hits * 0.7
            passage_score += max(0.2, 8.0 / (1.0 + position * 0.18))
            previous = entry.get("evidence_score", 0.0)
            entry["score"] += max(0.0, passage_score - previous)
            entry["evidence_score"] = max(previous, passage_score)

    # Add title/category candidates even if their best matching chunk was outside the FTS top window.
    for book in books:
        bid = int(book["book_id"])
        meta = _metadata_score(book, terms, mode)
        if meta < 7.0:
            continue
        if bid not in by_book:
            topic_rows = _search_chunks(db, terms, limit=8, book_id=bid, strict=mode == 'recommendation')
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

    def topic_fit(book):
        metadata = _normalize_words(' '.join(str(book[key] or '') for key in ('title', 'category', 'folder_path')))
        subject_terms = [term for term in terms if term not in {'ministry', 'bible', 'bibles', 'study'}]
        return sum(any(re.search(r'\b' + re.escape(v) + r'\b', metadata)
                       for v in _term_variants(term)) for term in subject_terms)

    eligible = list(by_book.values())
    if mode == 'recommendation' and len(terms) > 1:
        strong = [book for book in eligible if any(
            all(any(_term_in_text(v, _normalize_words(row['content'])) for v in _term_variants(term))
                for term in terms) for row in book['chunks'])]
        if strong:
            eligible = strong
    if kind and not _extract_title_hint(question):
        eligible = [book for book in eligible if _matches_resource_type(book, kind)]
    ranked = sorted(eligible, key=lambda x: (
        -topic_fit(x),
        _resource_priority(x) if prioritize else 0,
        -x["score"], str(x["title"] or "").lower()))
    unique, seen = [], set()
    for book in ranked:
        # Imported PDF/EPUB copies can have folder names in the author field.
        # Show a title once per recommendation list despite that metadata drift.
        identity = _normalize_words(book['title'])
        if identity not in seen:
            seen.add(identity)
            unique.append(book)
    return unique[:max(desired_books, 6)], mode


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

    db = _db(read_only=True)
    try:
        if not db.execute("SELECT 1 FROM appdb.sqlite_master WHERE name='sermon_library_pages'").fetchone():
            return []
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


def _approved_source_record(title, link):
    return 'APPROVED SOURCE: ' + json.dumps(
        {'title': _clean(title), 'url': link}, ensure_ascii=False, separators=(',', ':')) + '\n'


def _catalog_source_reason(title, kind):
    """Conservative intended-use notes grounded in the catalogue title/type."""
    low = _normalize_words(title)
    descriptions = {
        'study_bible': ('A study Bible option for personal Scripture reading and study.',
                        'Isang study Bible na maaaring gamitin sa personal na pagbabasa at pag-aaral ng Kasulatan.',
                        'Usa ka study Bible alang sa personal nga pagbasa ug pagtuon sa Kasulatan.'),
        'devotional': ('A devotional option for regular Bible reflection.',
                       'Isang devotional para sa regular na pagninilay sa Bibliya.',
                       'Usa ka devotional alang sa regular nga pagpamalandong sa Bibliya.'),
        'commentary': ('A commentary option for studying and explaining biblical passages.',
                       'Isang komentaryo para sa pag-aaral at pagpapaliwanag ng mga talata sa Bibliya.',
                       'Usa ka komentaryo alang sa pagtuon ug pagpasabot sa mga teksto sa Bibliya.'),
        'children_ministry': ("A children's ministry resource to consider when preparing activities or teaching.",
                              'Sanggunian para sa paghahanda ng pagtuturo o mga gawain sa ministeryo ng mga bata.',
                              'Kapanguhaan alang sa pag-andam og pagtudlo o mga kalihokan sa ministeryo sa mga bata.'),
    }
    english, tagalog, cebuano = descriptions.get(kind, descriptions['children_ministry'])
    if kind == 'children_ministry':
        for pattern, description in (
            (r'\b(?:crafts?|crafting)\b', "Useful for planning Bible-themed crafts and hands-on children's ministry activities."),
            (r'\bgames?\b', "Useful for planning Bible games and interactive children's ministry activities."),
            (r'\bscience\b', 'Useful for considering science activities as illustrations for Bible lessons.'),
            (r'\b(?:talks|lessons?|teach\w*|sunday school)\b', 'Useful when preparing Bible lessons, short talks, or Sunday school teaching.'),
            (r'\bdevotion\w*\b', 'A devotional option for Bible reading and reflection with children.'),
        ):
            if re.search(pattern, low):
                english = description
                break
    elif kind == 'study_bible':
        # Intended-use distinctions follow the actual title, without claiming
        # that unexamined editions contain particular notes or articles.
        for pattern, description in (
            (r'\bamplified\b', 'An Amplified edition to consider when comparing Bible translations during personal study.'),
            (r'\bsuffering\b', 'A title focused on suffering and hope, worth considering for Bible study alongside people facing hardship.'),
            (r'\bnew believers?\b', 'A beginner-focused Bible option for someone starting to read Scripture after coming to faith.'),
            (r'\brecovery\b', 'A recovery-focused Bible option to consider when exploring faith, restoration, and personal change.'),
            (r'\bchronological\b', 'A chronological study option for following the sequence of biblical events and their application to life.'),
            (r'\bancient faith\b', 'An option for exploring Scripture with an interest in the historic Christian faith.'),
            (r'\bapologetics\b.*\bstudents?\b', 'A student-focused option for studying questions about Christian belief and explaining the faith.'),
            (r'\bapologetics\b', 'An option for studying questions about Christian belief and discussing reasons for the faith.'),
            (r'\billustrated\b', 'An illustrated study option to consider if visual material helps your learning.'),
            (r'\bdisciples?\b', 'An option for connecting Bible study with discipleship and following Jesus.'),
            (r'\bleadership\b', 'A leadership-focused option for connecting Bible study with ministry and leading others.'),
            (r'\bchain reference\b', 'A reference-focused option for tracing related passages and themes across Scripture.'),
        ):
            if re.search(pattern, low):
                english = description
                break
    return dict(zip(('English', 'Tagalog', 'Cebuano'), (english, tagalog, cebuano)))


def _evidence_source_reason(passage, terms):
    """Use an actual short excerpt, rather than inventing a fallback summary."""
    pattern = r'\b(?:' + '|'.join(re.escape(term) for term in terms) + r')\b' if terms else ''
    for sentence in re.split(r'(?<=[.!?])\s+|\n+', passage):
        match = re.search(pattern, sentence, re.I) if pattern else None
        if not match or len(sentence.strip()) < 30 or re.search(r'<[^>]+>|font-size|div#|\{[^}]*:', sentence):
            continue
        start = max(0, match.start() - 70)
        excerpt = sentence[start:start + 220].strip()
        excerpt = ('…' if start else '') + excerpt + ('…' if start + 220 < len(sentence) else '')
        # Quoted ebook text must not introduce a new clickable link in a
        # fallback reply. Keep its visible words, not embedded Markdown URLs.
        excerpt = re.sub(r'\[([^\]]+)\]\([^)]*\)', r'\1', excerpt)
        return {
            'English': f'A relevant excerpt for your topic: “{excerpt}”',
            'Tagalog': f'Kaugnay na sipi para sa iyong paksa: “{excerpt}”',
            'Cebuano': f'Usa ka may kalabotang kinutlo alang sa imong hilisgutan: “{excerpt}”',
        }
    return {
        'English': 'Open the linked passage to check how it relates to your topic.',
        'Tagalog': 'Buksan ang naka-link na sipi upang masuri ang kaugnayan nito sa iyong paksa.',
        'Cebuano': 'Ablihi ang gi-link nga teksto aron masusi ang kalabotan niini sa imong hilisgutan.',
    }


def validate_library_answer(answer, context, language='English', include_explanations=True):
    """Check model-written reader links against this request's authorized sources.

    Never consult model-written search reports or prior conversation IDs. An
    invalid answer becomes a short verified list without another provider call.
    """
    # Text (including search reports or "APPROVED SOURCE" lines inside a book)
    # can never grant permission to link a source.
    sources = getattr(context, 'approved_sources', ())
    pattern = r'\[((?:\\.|[^\[\]\\\n]|\[(?:\\.|[^\]\\\n])*\])+)\]\\?\(([^\s)]+)\)'

    def reason(source):
        if not include_explanations:
            return ''
        notes = source.get('reason', {})
        return notes.get(language, notes.get('English', ''))

    def markdown_label(label):
        for _ in range(3):
            unescaped = re.sub(r'\\([\\\[\]])', r'\1', label)
            if unescaped == label:
                break
            label = unescaped
        return re.sub(r'([\\\[\]])', r'\\\1', label)

    def fallback(preserved=()):
        seen = set()
        lines = []
        for item in preserved:
            for match in re.finditer(pattern, item):
                source = allowed.get(url_key(match.group(2)))
                if source:
                    seen.add(source['title'])
            item = re.sub(r'^\s*\d+[.)]\s*', '', item.strip())
            lines.append(f'{len(lines) + 1}. {item}')
        for source in sources:
            if source['title'] in seen:
                continue
            seen.add(source['title'])
            label = markdown_label(source['title'])
            explanation = reason(source)
            lines.append(f"{len(lines) + 1}. [{label}]({source['url']})"
                         + (f' — {explanation}' if explanation else ''))
        return '\n'.join(lines) or "I couldn't verify matching books in the current library results."

    def url_key(url):
        parsed = urlsplit(html.unescape(url.replace('\\&', '&')))
        return parsed.path, tuple(sorted(parse_qsl(parsed.query, keep_blank_values=True)))

    allowed = {url_key(source['url']): source for source in sources}

    def add_explanations(text):
        items = re.split(r'(?m)(?=^\d+[.)]\s+)', text)
        for index, item in enumerate(items):
            link = re.search(pattern, item)
            if not link:
                continue
            source = allowed.get(url_key(link.group(2)))
            if not source or not reason(source):
                continue
            tail = item[link.end():].strip().strip('*').strip()
            author_only = tail.lower().startswith('by ') and not re.search(r'[—–;:\n]', tail)
            if not tail.strip(' .—–-') or author_only:
                items[index] = item.rstrip() + ' — ' + reason(source) + ('\n' if item.endswith('\n') else '')
        return ''.join(items)

    def title_matches(label, title):
        label = _normalize_words(re.sub(r'\\(.)', r'\1', html.unescape(label)))
        title = _normalize_words(title)
        if label == title:
            return True
        # Models may shorten long catalogue titles; require a contiguous phrase
        # of at least three words, rather than accepting a shared word like Bible.
        return len(label.split()) >= 3 and f' {label} ' in f' {title} '

    if re.search(r'CURRENT PASTOR.S RESOURCES CONTEXT|PASTOR.S RESOURCES AI RETRIEVAL|'
                 r'APPROVED (?:SOURCE|LINK):|RETRIEVAL MODE:|Retrieved \d+ distinct library records', answer, re.I):
        return fallback()

    invalid = False
    linked_sources = 0
    # Accept an unescaped bracket pair inside a book title and normalize it.
    # This common model formatting error must not discard valid explanations.

    def check_link(match):
        nonlocal invalid, linked_sources
        label, url = match.groups()
        try:
            key = url_key(url)
        except ValueError:
            invalid = True
            return match.group(0)
        if not re.match(r'^/pastor-resources/(?:sermon-ebooks/)?read/', key[0]):
            return match.group(0)
        matching_titles = [s for s in sources if title_matches(label, s['title'])]
        source = allowed.get(key)
        if source and title_matches(label, source['title']):
            linked_sources += 1
            return f'[{markdown_label(label)}]({source["url"]})'
        # Repair a wrong ID only when the supplied title unambiguously identifies
        # a retrieved source. Use its exact server-owned URL.
        if len({s['title'] for s in matching_titles}) == 1:
            source = matching_titles[0]
            linked_sources += 1
            return f'[{markdown_label(label)}]({source["url"]})'
        invalid = True
        return match.group(0)

    result = re.sub(pattern, check_link, answer)
    # Also catch bare/HTML reader URLs that escaped the Markdown validation.
    remaining = re.sub(pattern, '', result)
    if re.search(r'/pastor-resources/(?:sermon-ebooks/)?read/', remaining):
        invalid = True
    recommendation = bool(re.search(r'RETRIEVAL MODE: (?:CATALOG_RECOMMENDATION|RECOMMENDATION)', context))
    if invalid or (recommendation and not linked_sources):
        if recommendation:
            preserved = []
            for item in re.split(r'(?m)(?=^\d+[.)]\s+)', answer):
                if not re.match(r'^\d+[.)]\s+', item):
                    continue
                invalid, linked_sources = False, 0
                checked = re.sub(pattern, check_link, item)
                bare_urls = re.search(r'/pastor-resources/(?:sermon-ebooks/)?read/', re.sub(pattern, '', checked))
                if not invalid and linked_sources and not bare_urls:
                    preserved.append(add_explanations(checked))
            if preserved:
                return fallback(preserved)
        return fallback()
    if recommendation:
        # Keep the model's explanations. Fill only bare items or author-only
        # entries, using server-owned title/type notes or retrieved excerpts.
        result = add_explanations(result)
    return result


def _format_public_evidence(book, max_chunks, max_chars=None, terms=()):
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
        prefix = (
            f"EVIDENCE LOCATION: {location}\n"
            + (f"APPROVED LINK: {link}\n" if link else "")
            + (_approved_source_record(book['title'], link) if link else "")
            + "PASSAGE: "
        )
        passage = _clean(row['content'])
        if max_chars is not None:
            available = max_chars - len(prefix) - 1
            if available < 80:
                continue
            if len(passage) > available:
                # Keep the matching topic in a short excerpt, even when it occurs
                # late in a long indexed chunk. Preserve the exact source URL.
                match = re.search(r"\b(?:" + "|".join(re.escape(t) for t in terms) + r")\b",
                                  passage, re.I) if terms else None
                start = max(0, match.start() - available // 3) if match else 0
                start = min(start, len(passage) - available)
                passage = passage[start:start + available - 2]
                passage = ("…" if start else "") + passage + "…"
        blocks.append(LibraryContext(prefix + passage + "\n", [
            {'title': _clean(book['title']), 'url': link,
             'reason': _evidence_source_reason(passage, terms)}] if link else []))
        if len(blocks) >= max_chunks:
            break
    return blocks



def _catalog_specific_book_for_question(question):
    """Find a likely named Pastor's Resources book, including unindexed books.

    This is deliberately deterministic and local: no Gemini/API call is used.
    A match must be strong enough that a broad topic question is not mistaken
    for a specific title.
    """
    if _broad_book_request(question):
        return None
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

    db = _db(read_only=True)
    try:
        rows = db.execute(
            """
            SELECT b.id AS book_id,b.title,b.author,b.category,b.folder_path,
                   MAX(CASE WHEN d.searchable=1 AND d.chunk_count>0 THEN 1 ELSE 0 END) AS indexed,
                   MAX(COALESCE(d.page_count,0)) AS page_count,
                   MAX(COALESCE(d.chunk_count,0)) AS chunk_count,
                   MAX(COALESCE(d.extract_error,'')) AS extract_error
            FROM ai_allowed_library_books b
            LEFT JOIN ai_allowed_library_files f
              ON f.book_id=b.id AND f.is_active=1 AND COALESCE(f.is_duplicate,0)=0
            LEFT JOIN pij_library_documents d
              ON d.drive_file_id=f.drive_file_id AND d.source_type='public_ebook'
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
                if any(_term_in_text(v, title_norm) for v in _term_variants(t))
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
        SELECT d.id AS document_id,b.id AS book_id,b.title,b.author,b.category,b.folder_path,d.format,
               c.chunk_number,c.page_start,c.page_end,c.content,
               0.0 AS rank
        FROM pij_library_chunks c
        JOIN pij_library_documents d ON d.id=c.document_id
        JOIN ai_allowed_library_files pf ON pf.drive_file_id=d.drive_file_id
          AND pf.is_active=1 AND COALESCE(pf.is_duplicate,0)=0
        JOIN ai_allowed_library_books b ON b.id=pf.book_id
        WHERE d.source_type='public_ebook'
          AND d.searchable=1 AND d.chunk_count>0
          AND b.id=?
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
    mode = _library_mode(question)
    kind = _requested_resource_type(question)
    category_request = mode == 'recommendation' and kind and not _extract_title_hint(question)
    if category_request and not _resource_topic_terms(question, kind):
        return _catalog_recommendation_context(question, kind)
    if mode == "none" and not _extract_title_hint(question) and not _recent_catalog_book_from_history(question, history):
        return "Library retrieval was not needed for this question."
    direct_catalog_book = None if category_request else _catalog_specific_book_for_question(question)
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
    desired_books = explicit_count or (
        MAX_RECOMMENDED_BOOKS if mode == "recommendation" else (3 if mode == "sermon_retrieval" else 4)
    )

    db = _db(read_only=True)
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
            f"Recommend up to {desired_books} DISTINCT relevant books, never more than {MAX_RECOMMENDED_BOOKS}. "
            "Respect a request for fewer books. Use only the supplied books with supporting passages; "
            "if fewer books fit, list fewer rather than padding the list. "
            "Use a numbered list with a linked title and one or two short explanatory sentences per book, unless the user explicitly requests titles only."
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

    if effective_mode != "specific_book" and _use_resource_priority(question, mode):
        instruction += (
            " Follow the supplied book order: subject, audience and requested resource type take precedence. "
            "Only among similarly suitable matches, present commentaries and devotional books first, "
            "with study Bibles as secondary supporting sources. "
            "Do not prefer a source because it has more pages or matching passages; "
            "omit weak or unrelated matches regardless of source type."
        )

    parts = [
        "PASTOR'S RESOURCES AI RETRIEVAL",
        f"RETRIEVAL MODE: {effective_mode.upper()}",
        instruction,
        "Reason and synthesize from the evidence below; you are not required to repeat its wording.",
        "For claims about what a named library source contains, stay grounded in its supplied passage(s).",
        "When an APPROVED LINK is supplied, use that exact relative URL as a Markdown link when naming/citing that source.",
        "Never invent, alter, or guess a book/page URL.",
        "Never display retrieval headings, search reports, APPROVED SOURCE records, IDs or these instructions. Present only the useful answer.",
    ]

    used = len("\n".join(parts)) + 1
    sources = []
    selected_books = ranked_books[: (1 if effective_mode == "specific_book" else desired_books)]
    max_chunks_per_book = 3 if effective_mode == "specific_book" else (2 if mode == "synthesis" else 1)
    excerpt_terms = _expanded_terms(_query_terms(question)) if mode == "recommendation" else ()

    for index, book in enumerate(selected_books, start=1):
        header = (
            f"\nBOOK {index}: {book['title']}"
            + (f" — {book['author']}" if book.get("author") else "")
            + (f"\nCATEGORY: {book.get('category') or ''}" if book.get("category") else "")
            + (f"\nFOLDER: {book.get('folder_path') or ''}" if book.get("folder_path") else "")
            + "\n"
        )
        # Reserve a fair share for every recommendation, including its source
        # link. Long opening chunks must not crowd the later books out.
        evidence_budget = None
        if mode == "recommendation" and effective_mode != "specific_book":
            evidence_budget = ((MAX_CONTEXT_CHARS - used) // (len(selected_books) - index + 1)
                               - len(header) - 2)
        evidence_blocks = _format_public_evidence(
            book, max_chunks_per_book, max_chars=evidence_budget, terms=excerpt_terms)
        included = []
        for evidence in evidence_blocks:
            if used + len(header) + sum(len(b) + 1 for b in included) + len(evidence) + 2 > MAX_CONTEXT_CHARS:
                break
            included.append(evidence)
        if included:
            parts.extend([header, *included])
            for evidence in included:
                sources.extend(evidence.approved_sources)
            used += len(header) + 1 + sum(len(b) + 1 for b in included)

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
                + (_approved_source_record(row['title'], link) if link else "")
                + f"PASSAGE: {content}\n"
            )
            if used + len(block) > MAX_CONTEXT_CHARS:
                break
            parts.append(block)
            if link:
                sources.append({'title': _clean(row['title']), 'url': link,
                                'reason': _evidence_source_reason(content, _expanded_terms(_query_terms(question)))})
            used += len(block)

    return LibraryContext("\n".join(parts), sources)


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
        pastor_resources.clear_resource_sync_cancel_request()
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
    db = _db(read_only=True)
    try:
        return db.execute(
            """
            SELECT b.id AS book_id, b.title, b.author, b.category, b.folder_path,
                   GROUP_CONCAT(DISTINCT UPPER(f.format)) AS formats,
                   MAX(CASE WHEN d.searchable=1 AND d.chunk_count>0 THEN 1 ELSE 0 END) AS indexed,
                   MAX(COALESCE(d.page_count,0)) AS page_count,
                   MAX(COALESCE(d.chunk_count,0)) AS chunk_count,
                   MAX(COALESCE(d.extract_error,'')) AS extract_error
            FROM ai_allowed_library_books b
            LEFT JOIN ai_allowed_library_files f
              ON f.book_id=b.id AND f.is_active=1 AND COALESCE(f.is_duplicate,0)=0
            LEFT JOIN pij_library_documents d
              ON d.drive_file_id=f.drive_file_id AND d.source_type='public_ebook'
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
    db = _db(read_only=True)
    try:
        row = db.execute(
            """
            SELECT b.id AS book_id,b.title,b.author,b.category,b.folder_path,
                   GROUP_CONCAT(DISTINCT UPPER(f.format)) AS formats,
                   MAX(CASE WHEN d.searchable=1 AND d.chunk_count>0 THEN 1 ELSE 0 END) AS indexed,
                   MAX(COALESCE(d.page_count,0)) AS page_count,
                   SUM(COALESCE(d.chunk_count,0)) AS chunk_count,
                   MAX(COALESCE(d.extract_error,'')) AS extract_error
            FROM ai_allowed_library_books b
            LEFT JOIN ai_allowed_library_files f
              ON f.book_id=b.id AND f.is_active=1 AND COALESCE(f.is_duplicate,0)=0
            LEFT JOIN pij_library_documents d
              ON d.drive_file_id=f.drive_file_id AND d.source_type='public_ebook'
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
    terms = _query_terms(query)
    if not terms:
        terms = [w for w in _word_tokens(query) if len(w) >= 2][:8]
    limit = max(1, min(12, int(limit or 8)))
    db = _db(read_only=True)
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
